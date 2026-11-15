/**
 * Session Memory Extraction — extracts structured facts from tool results
 * and recent conversation context for cross-turn memory persistence.
 */

import type { ProviderConfig, Message } from './types';
import { streamChatCompletion, streamChatCompletionWithRetry } from './provider';
import { log as coreLog } from './logger';

export interface SessionFact {
  fact: string;
  category: 'decision' | 'file_modification' | 'user_preference' | 'error_fix' | 'convention';
  confidence: number;
}

/**
 * Tunable session-memory knobs. Every value that was previously a magic number
 * (window size, truncation lengths, token budget, confidence floor, caps,
 * timeout) is a documented, overridable field here so a consumer can tune the
 * extractor without editing core. Construct an extractor with a
 * `Partial<SessionMemoryConfig>` to override individual knobs; unspecified
 * fields fall back to `DEFAULT_SESSION_MEMORY_CONFIG`.
 */
export interface SessionMemoryConfig {
  /** Max retained memory entries before confidence/recency eviction. */
  maxEntries: number;
  /** Max characters per persisted memory entry. */
  maxEntryChars: number;
  /** Number of trailing messages used as extraction context. */
  contextWindowMessages: number;
  /** Max characters of each tool result included in the extraction prompt. */
  toolResultTruncateChars: number;
  /** Max characters of each message included in the extraction prompt. */
  messageTruncateChars: number;
  /** maxTokens budget for the extraction model call. */
  maxTokens: number;
  /** Minimum confidence for an extracted fact to be retained. */
  confidenceFloor: number;
  /** Extraction watchdog timeout (ms). */
  extractionTimeoutMs: number;
  /**
   * Jaccard token-similarity threshold (0..1) above which two facts are treated
   * as near-duplicates and collapsed in {@link SessionMemoryExtractor.mergeFacts}.
   */
  dedupSimilarityThreshold: number;
  /**
   * How many of the newest facts are protected from eviction regardless of
   * confidence, so current-work facts are not evicted by old high-confidence
   * ones. Bounded by `maxEntries`.
   */
  recencyProtectedCount: number;
  /** Token delta since the last extraction that triggers a new extraction pass. */
  extractTokenThreshold: number;
  /** Tool-call delta since the last extraction that triggers a new pass. */
  extractToolCallThreshold: number;
}

export const DEFAULT_SESSION_MEMORY_CONFIG: SessionMemoryConfig = {
  maxEntries: 100,
  maxEntryChars: 2000,
  contextWindowMessages: 4,
  toolResultTruncateChars: 500,
  messageTruncateChars: 300,
  maxTokens: 500,
  confidenceFloor: 0.7,
  extractionTimeoutMs: 30_000,
  dedupSimilarityThreshold: 0.85,
  recencyProtectedCount: 20,
  extractTokenThreshold: 8_000,
  extractToolCallThreshold: 20,
};

export const MAX_MEMORY_ENTRIES = DEFAULT_SESSION_MEMORY_CONFIG.maxEntries;
export const MAX_MEMORY_ENTRY_CHARS = DEFAULT_SESSION_MEMORY_CONFIG.maxEntryChars;

/** The incremental window of messages to run a memory-extraction pass over, plus
 *  the cursor the caller should persist AFTER a successful extraction (parity:
 *  Claude's UUID-cursor extractMemories). Advancing only on success means a failed
 *  pass is retried over the same window next turn rather than silently skipped. */
export interface IncrementalWindow {
  window: Message[];
  /** uuid of the newest message carrying one — the cursor to persist on success. */
  nextCursor?: string;
}

/**
 * Compute the incremental extraction window: every message AFTER the one whose
 * `uuid` equals `cursorUuid`. When the cursor is absent, or no longer present in
 * the transcript (compaction dropped it), fall back to the FULL transcript so no
 * context is silently skipped. `nextCursor` is the uuid of the last message that
 * has one, so a uuid-less trailing message does not reset the cursor. The caller
 * persists `nextCursor` only after the extraction succeeds (advance-only-on-success).
 */
export function selectIncrementalWindow(messages: Message[], cursorUuid?: string): IncrementalWindow {
  let lastUuid: string | undefined;
  for (const m of messages) if (m.uuid) lastUuid = m.uuid;

  let window = messages;
  if (cursorUuid !== undefined) {
    const idx = messages.findIndex(m => m.uuid === cursorUuid);
    // idx === -1 → cursor compacted away: fall back to the full transcript.
    if (idx !== -1) window = messages.slice(idx + 1);
  }
  return { window, nextCursor: lastUuid };
}

const SUSPICIOUS_PATTERNS = [
  /<script/i,
  /javascript:/i,
  /data:text\/html/i,
  /\beval\s*\(/,
  /\bFunction\s*\(/,
  /__proto__/,
  /constructor\.prototype/,
];

/**
 * Secret/credential patterns redacted before any memory content is persisted or
 * sent to the extraction model. Catches common provider API keys, cloud access
 * keys, bearer/authorization tokens, secret-bearing env assignments, and GCP
 * service-account emails so a leaked credential in a tool result does not end up
 * in long-lived memory. Each match is replaced with `[REDACTED]`.
 */
const SECRET_PATTERNS: RegExp[] = [
  /sk-ant-[A-Za-z0-9_-]{8,}/g, // Anthropic API keys
  /sk-[A-Za-z0-9]{16,}/g, // OpenAI-style secret keys
  /AKIA[0-9A-Z]{16}/g, // AWS access key id
  /AIza[0-9A-Za-z_-]{20,}/g, // Google API key
  /\bBearer\s+[A-Za-z0-9._~+/-]{12,}=*/gi, // Authorization Bearer tokens
  /\b(?:x-api-key|api[_-]?key|authorization)\b\s*[:=]\s*['"]?[A-Za-z0-9._-]{12,}/gi, // key: value headers
  /\b(?:AWS|GOOGLE|GCP|OPENAI|ANTHROPIC)[A-Z0-9_]*(?:KEY|TOKEN|SECRET|PASSWORD)\s*=\s*\S+/g, // env assignments
  /[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.iam\.gserviceaccount\.com/g, // GCP service-account emails
];

/**
 * Replace secrets/credentials in `text` with `[REDACTED]`. Safe to call on any
 * string; text without a credential is returned unchanged.
 */
export function redactSecrets(text: string): string {
  let out = text;
  for (const pattern of SECRET_PATTERNS) out = out.replace(pattern, '[REDACTED]');
  return out;
}

export function validateMemoryEntry(entry: string): { valid: boolean; reason?: string } {
  if (!entry || entry.trim().length === 0) {
    return { valid: false, reason: 'empty' };
  }
  if (entry.length > MAX_MEMORY_ENTRY_CHARS) {
    return { valid: false, reason: 'too_long' };
  }
  for (const pattern of SUSPICIOUS_PATTERNS) {
    if (pattern.test(entry)) {
      return { valid: false, reason: 'suspicious_content' };
    }
  }
  return { valid: true };
}

export function sanitizeMemoryEntry(entry: string): string {
  let sanitized = entry.trim();
  sanitized = sanitized.replace(/<[^>]*>/g, '');
  // Redact credentials BEFORE truncation so a secret near the cap cannot survive
  // by being partially sliced off mid-token.
  sanitized = redactSecrets(sanitized);
  if (sanitized.length > MAX_MEMORY_ENTRY_CHARS) {
    sanitized = sanitized.slice(0, MAX_MEMORY_ENTRY_CHARS - 15) + '... [truncated]';
  }
  return sanitized;
}

/** Normalize a fact for similarity comparison: lowercase, drop punctuation,
 *  collapse whitespace. */
function normalizeForDedup(s: string): string {
  return s
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

/** Jaccard token similarity of two fact strings in [0,1]. Two empty strings are
 *  identical (1); an empty vs. non-empty pair shares nothing (0). */
function factSimilarity(a: string, b: string): number {
  const sa = new Set(normalizeForDedup(a).split(' ').filter(Boolean));
  const sb = new Set(normalizeForDedup(b).split(' ').filter(Boolean));
  if (sa.size === 0 && sb.size === 0) return 1;
  let intersection = 0;
  for (const token of sa) if (sb.has(token)) intersection++;
  const union = sa.size + sb.size - intersection;
  return union === 0 ? 0 : intersection / union;
}

const EXTRACTION_PROMPT = `Extract key facts from these tool results that would be useful to remember across conversation turns. Return a JSON array of objects with the following shape:

[
  {
    "fact": "concise description of the fact",
    "category": "decision" | "file_modification" | "user_preference" | "error_fix" | "convention",
    "confidence": 0.0 to 1.0
  }
]

Categories:
- decision: architectural or design decisions made during the conversation
- file_modification: files created, modified, or deleted and why
- user_preference: user preferences about coding style, tools, workflows
- error_fix: errors encountered and how they were resolved
- convention: project conventions or patterns discovered

Only include facts that would be genuinely useful in future turns. Be concise. Return ONLY the JSON array, no other text.`;

export class SessionMemoryExtractor {
  private provider: ProviderConfig;
  private compactionModel?: string;
  /** Resolved, immutable tuning config (defaults merged with constructor overrides). */
  readonly config: SessionMemoryConfig;
  /** Test seam: the streaming function used to call the model. Defaults to the
   *  retry-wrapping provider stream so transient 429/5xx/529 responses during
   *  background extraction are retried with backoff instead of discarding all
   *  facts on the first error; tests may replace it to exercise failure paths. */
  _streamFn: typeof streamChatCompletion = streamChatCompletionWithRetry;
  /** Extraction watchdog timeout (ms). Seeded from `config.extractionTimeoutMs`;
   *  tests may override it to exercise the abort path without the full wait. */
  _timeoutMs: number;

  constructor(provider: ProviderConfig, compactionModel?: string, config?: Partial<SessionMemoryConfig>) {
    this.provider = provider;
    this.compactionModel = compactionModel;
    this.config = { ...DEFAULT_SESSION_MEMORY_CONFIG, ...config };
    this._timeoutMs = this.config.extractionTimeoutMs;
  }

  /**
   * Threshold/cadence gate: whether enough has happened since the last extraction
   * to warrant another pass. Mirrors Claude's dual-threshold logic — extract when
   * either the token delta or the tool-call delta crosses its threshold — plus a
   * natural-break special case: at a turn boundary, extract any pending work even
   * if neither threshold has been reached, so facts are not lost at end of turn.
   */
  shouldExtractFacts(opts: {
    tokensSinceLast: number;
    toolCallsSinceLast: number;
    naturalBreak?: boolean;
  }): boolean {
    const { tokensSinceLast, toolCallsSinceLast, naturalBreak } = opts;
    const pending = tokensSinceLast > 0 || toolCallsSinceLast > 0;
    if (naturalBreak) return pending;
    return (
      tokensSinceLast >= this.config.extractTokenThreshold ||
      toolCallsSinceLast >= this.config.extractToolCallThreshold
    );
  }

  /**
   * Extract structured facts from tool results and recent messages.
   * Uses the compaction model if provided, otherwise the primary provider model.
   * Only includes the last `config.contextWindowMessages` messages as context.
   *
   * An optional caller `signal` is combined with the internal watchdog so a user
   * interrupt cancels extraction promptly instead of blocking up to the timeout.
   */
  async extractFacts(
    toolResults: Array<{ toolName: string; output: string; isError: boolean }>,
    recentMessages: Message[],
    signal?: AbortSignal,
  ): Promise<SessionFact[]> {
    // Only use the last N messages
    const contextMessages = recentMessages.slice(-this.config.contextWindowMessages);

    // Build the user prompt with tool results and context. Redact secrets BEFORE
    // the content is sent to the extraction model so credentials in tool output
    // never leave the process inside a prompt.
    const toolResultsSummary = toolResults
      .map(r => `[${r.toolName}]${r.isError ? ' (ERROR)' : ''}: ${redactSecrets(r.output.slice(0, this.config.toolResultTruncateChars))}`)
      .join('\n\n');

    const messageSummary = contextMessages
      .map(m => {
        const content = typeof m.content === 'string' ? m.content : JSON.stringify(m.content);
        return `[${m.role}]: ${redactSecrets(content?.slice(0, this.config.messageTruncateChars) ?? '')}`;
      })
      .join('\n');

    const userContent = `## Tool Results\n${toolResultsSummary}\n\n## Recent Conversation\n${messageSummary}`;

    // Use compaction model if available, otherwise primary model
    const effectiveConfig: ProviderConfig = this.compactionModel
      ? { ...this.provider, model: this.compactionModel }
      : this.provider;

    const messages: Message[] = [
      { role: 'system', content: EXTRACTION_PROMPT },
      { role: 'user', content: userContent },
    ];

    const abortController = new AbortController();
    // Set a timeout to avoid hanging
    const timeout = setTimeout(() => abortController.abort(), this._timeoutMs);
    // Thread the caller's abort signal into the extraction stream so an interrupt
    // cancels immediately rather than waiting out the watchdog.
    const onExternalAbort = () => abortController.abort();
    if (signal) {
      if (signal.aborted) abortController.abort();
      else signal.addEventListener('abort', onExternalAbort, { once: true });
    }

    let facts: SessionFact[] = [];
    try {
      let responseText = '';

      for await (const chunk of this._streamFn(
        { ...effectiveConfig, maxTokens: this.config.maxTokens },
        messages,
        [], // no tools
        abortController.signal,
      )) {
        if (chunk.type === 'content_delta' && chunk.text) {
          responseText += chunk.text;
        }
        if (chunk.type === 'error') {
          coreLog('session-memory', 'Extraction error', { error: chunk.error ?? 'unknown' });
          return [];
        }
      }

      // Assign and fall through to the finally cleanup (rather than returning
      // from inside the try) so timeout cleanup and the success return share one
      // normal-completion path.
      facts = this.parseFactsFromResponse(responseText);
    } catch (err) {
      coreLog('session-memory', 'Failed to extract facts', { error: String(err) });
      return [];
    } finally {
      clearTimeout(timeout);
      if (signal) signal.removeEventListener('abort', onExternalAbort);
    }
    return facts;
  }

  /**
   * Merge new facts into existing facts, deduplicating by content similarity.
   * Near-duplicates (normalized-equal, or Jaccard token similarity at/above
   * `config.dedupSimilarityThreshold`) collapse so paraphrased restatements do
   * not accumulate; the higher-confidence variant wins. When the merged list
   * exceeds `config.maxEntries`, the newest `config.recencyProtectedCount` facts
   * are retained regardless of confidence and the remaining slots are filled by
   * confidence, so current-work facts are not evicted by stale high-confidence
   * ones. The returned list is ordered by descending confidence.
   */
  mergeFacts(existing: SessionFact[], newFacts: SessionFact[]): SessionFact[] {
    const merged = [...existing];

    for (const newFact of newFacts) {
      const validation = validateMemoryEntry(newFact.fact);
      if (!validation.valid) {
        coreLog('session-memory', `Skipping invalid fact: ${validation.reason}`);
        continue;
      }
      newFact.fact = sanitizeMemoryEntry(newFact.fact);

      const duplicateIndex = merged.findIndex(
        e =>
          e.fact.toLowerCase() === newFact.fact.toLowerCase() ||
          factSimilarity(e.fact, newFact.fact) >= this.config.dedupSimilarityThreshold,
      );

      if (duplicateIndex >= 0) {
        if (newFact.confidence > merged[duplicateIndex].confidence) {
          merged[duplicateIndex] = newFact;
        }
      } else {
        merged.push(newFact);
      }
    }

    if (merged.length > this.config.maxEntries) {
      // Insertion order == recency (existing first, newest appended), so the tail
      // is the most recent. Protect the newest N from eviction, then fill the
      // remaining slots from the older facts by confidence.
      const protectCount = Math.min(this.config.recencyProtectedCount, this.config.maxEntries);
      const recent = merged.slice(merged.length - protectCount);
      const older = merged.slice(0, merged.length - protectCount);
      older.sort((a, b) => b.confidence - a.confidence);
      const survivors = [...older.slice(0, this.config.maxEntries - recent.length), ...recent];
      survivors.sort((a, b) => b.confidence - a.confidence);
      return survivors;
    }

    return merged;
  }

  /**
   * Parse JSON facts from LLM response, filtering by confidence threshold.
   */
  private parseFactsFromResponse(response: string): SessionFact[] {
    try {
      // Try to extract JSON array from the response (handle markdown code blocks)
      let jsonStr = response.trim();
      const jsonMatch = jsonStr.match(/```(?:json)?\s*([\s\S]*?)```/);
      if (jsonMatch) {
        jsonStr = jsonMatch[1].trim();
      }

      const parsed = JSON.parse(jsonStr);
      if (!Array.isArray(parsed)) {
        return [];
      }

      const validCategories = new Set(['decision', 'file_modification', 'user_preference', 'error_fix', 'convention']);

      return parsed
        .filter((item: unknown): item is SessionFact => {
          if (typeof item !== 'object' || item === null) return false;
          const obj = item as Record<string, unknown>;
          return (
            typeof obj.fact === 'string' &&
            typeof obj.category === 'string' &&
            validCategories.has(obj.category) &&
            typeof obj.confidence === 'number' &&
            obj.confidence >= this.config.confidenceFloor
          );
        });
    } catch {
      coreLog('session-memory', 'Failed to parse facts JSON from response');
      return [];
    }
  }
}
