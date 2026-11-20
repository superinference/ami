/**
 * Session Memory Extraction — extracts structured facts from tool results
 * and recent conversation context for cross-turn memory persistence.
 */

import { readFile } from 'node:fs/promises';
import * as path from 'node:path';
import type { ProviderConfig, Message } from './types';
import { streamChatCompletion, streamChatCompletionWithRetry } from './provider';
import { log as coreLog } from './logger';
import { resolveConfigDir } from './reference-journal';
import { estimateTextTokens } from './token-estimator';

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
  /**
   * Aggregate token budget for ALL persisted session-memory facts combined. In
   * addition to the per-entry char cap ({@link maxEntryChars}) and the entry-count
   * cap ({@link maxEntries}), the whole notes document is bounded so a large number
   * of small-but-numerous facts cannot silently blow the compaction budget. When the
   * merged facts exceed this many tokens (counted with the same byte-aware estimate
   * as the rest of the engine), the lowest-priority facts are evicted — recency- and
   * criticality-protected first — until the total fits. Parity: the reference's
   * `MAX_TOTAL_SESSION_MEMORY_TOKENS` whole-document budget (default 12000) that
   * triggers a condense pass preserving the most critical sections
   * (SessionMemory/prompts.ts:L9,L168-L187).
   */
  maxTotalTokens: number;
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
   * Max time (ms) a consumer (e.g. pre-compaction / shutdown coordination) will
   * block in {@link SessionMemoryExtractor.waitForSessionMemoryExtraction} waiting
   * for an in-flight extraction before giving up and proceeding. Parity: the reference's
   * `EXTRACTION_WAIT_TIMEOUT_MS` (default 15000)
   * (SessionMemory/sessionMemoryUtils.ts:L12,L99-102).
   */
  extractionWaitTimeoutMs: number;
  /**
   * Age (ms) past which an in-progress extraction is treated as STALE and no longer
   * waited on, so a crashed/abandoned pass whose completion mark never fired cannot
   * wedge a waiting consumer indefinitely. Parity: the reference's
   * `EXTRACTION_STALE_THRESHOLD_MS` (default 60000)
   * (SessionMemory/sessionMemoryUtils.ts:L13,L92-96).
   */
  extractionStaleThresholdMs: number;
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
  /**
   * Minimum total context-window tokens before the FIRST extraction is allowed.
   * Until the conversation is at least this large, no extraction runs — there is
   * nothing worth persisting yet. Parity: the reference's `minimumMessageTokensToInit`
   * (default 10000), counted the same way as autocompact so the two features agree
   * (SessionMemory/sessionMemoryUtils.ts:L18-35).
   */
  minInitContextTokens: number;
  /**
   * Minimum context-window GROWTH (in tokens) since the last extraction before the
   * next one is allowed. Measures actual context growth, not cumulative API usage.
   * Parity: the reference's `minimumTokensBetweenUpdate` (default 5000)
   * (SessionMemory/sessionMemoryUtils.ts:L21-26, hasMetUpdateThreshold).
   */
  minTokensBetweenUpdate: number;
  /**
   * Number of tool calls since the last extraction before a new pass is allowed.
   * Parity: the reference's `toolCallsBetweenUpdates` (default 3)
   * (SessionMemory/sessionMemoryUtils.ts:L27).
   */
  toolCallsBetweenUpdate: number;
  /**
   * Categories treated as critical. During eviction they receive a scoring boost
   * (see {@link SessionMemoryConfig.criticalityBoost}) so important facts survive
   * pruning even at lower confidence. Parity: the reference's eviction "preserves the most
   * critical information", not confidence alone (SessionMemory/prompts.ts:L63-68).
   */
  criticalCategories: SessionFact['category'][];
  /**
   * Score boost added to a critical fact's confidence when ranking eviction
   * survivors. Does NOT mutate the stored confidence — it only affects which facts
   * are kept when the list is over `maxEntries`.
   */
  criticalityBoost: number;
}

export const DEFAULT_SESSION_MEMORY_CONFIG: SessionMemoryConfig = {
  maxEntries: 100,
  maxTotalTokens: 12_000,
  maxEntryChars: 2000,
  contextWindowMessages: 4,
  toolResultTruncateChars: 500,
  messageTruncateChars: 300,
  maxTokens: 500,
  confidenceFloor: 0.7,
  extractionTimeoutMs: 30_000,
  extractionWaitTimeoutMs: 15_000,
  extractionStaleThresholdMs: 60_000,
  dedupSimilarityThreshold: 0.85,
  recencyProtectedCount: 20,
  minInitContextTokens: 10_000,
  minTokensBetweenUpdate: 5_000,
  toolCallsBetweenUpdate: 3,
  criticalCategories: ['error_fix', 'decision', 'user_preference'],
  criticalityBoost: 0.5,
};

export const MAX_MEMORY_ENTRIES = DEFAULT_SESSION_MEMORY_CONFIG.maxEntries;
export const MAX_MEMORY_ENTRY_CHARS = DEFAULT_SESSION_MEMORY_CONFIG.maxEntryChars;
/** Aggregate token budget for the whole persisted session-memory document (parity:
 *  the reference's `MAX_TOTAL_SESSION_MEMORY_TOKENS`). See {@link SessionMemoryConfig.maxTotalTokens}. */
export const MAX_SESSION_MEMORY_TOTAL_TOKENS = DEFAULT_SESSION_MEMORY_CONFIG.maxTotalTokens;
/** Max time a consumer blocks waiting for an in-flight extraction (parity:
 *  the reference's `EXTRACTION_WAIT_TIMEOUT_MS`). See {@link SessionMemoryConfig.extractionWaitTimeoutMs}. */
export const EXTRACTION_WAIT_TIMEOUT_MS = DEFAULT_SESSION_MEMORY_CONFIG.extractionWaitTimeoutMs;
/** Age past which an in-progress extraction is treated as stale and not waited on
 *  (parity: the reference's `EXTRACTION_STALE_THRESHOLD_MS`). See {@link SessionMemoryConfig.extractionStaleThresholdMs}. */
export const EXTRACTION_STALE_THRESHOLD_MS = DEFAULT_SESSION_MEMORY_CONFIG.extractionStaleThresholdMs;

/** The incremental window of messages to run a memory-extraction pass over, plus
 *  the cursor the caller should persist AFTER a successful extraction (parity:
 *  the reference's UUID-cursor extractMemories). Advancing only on success means a failed
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

/**
 * Convert unambiguous relative date terms ("today", "yesterday", "tomorrow") in a
 * fact to absolute `YYYY-MM-DD` calendar dates, resolved against `now` (defaults to
 * the current date). Parity: the reference's project-memory guidance mandates "Always
 * convert relative dates in user messages to absolute dates when saving … so the
 * memory remains interpretable after time passes" (memdir/memoryTypes.ts:79). A
 * persisted fact like "freeze merges tomorrow" is meaningless when recalled days
 * later; normalizing it to an absolute date keeps cross-turn memory durable.
 *
 * Only unambiguous terms are rewritten deterministically; genuinely ambiguous
 * forms (bare weekday names like "Thursday") are left to the model, which is
 * instructed to resolve them via {@link EXTRACTION_PROMPT}.
 */
export function normalizeRelativeDates(text: string, now: Date = new Date()): string {
  const pad = (n: number): string => String(n).padStart(2, '0');
  const fmt = (d: Date): string => `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
  const DAY_MS = 86_400_000;
  const base = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const replacements: Array<[RegExp, Date]> = [
    [/\byesterday\b/gi, new Date(base.getTime() - DAY_MS)],
    [/\btomorrow\b/gi, new Date(base.getTime() + DAY_MS)],
    [/\btoday\b/gi, base],
  ];
  let out = text;
  for (const [pattern, date] of replacements) out = out.replace(pattern, fmt(date));
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

Only include facts that would be genuinely useful in future turns. Be concise. Always convert relative dates (e.g. "today", "yesterday", "Thursday") to absolute calendar dates (YYYY-MM-DD) so facts stay interpretable in later sessions. If an existing-memory listing is provided below, do NOT repeat facts already present — only emit facts that are new or that update one of them. Return ONLY the JSON array, no other text.`;

/**
 * Default user-content template for a fact-extraction pass. The three `{{…}}`
 * tokens are substituted by {@link SessionMemoryExtractor.extractFacts}:
 *   - `{{toolResults}}`       — the (redacted, truncated) tool-result summary
 *   - `{{recentConversation}}`— the (redacted, truncated) trailing-message summary
 *   - `{{existingMemory}}`    — the already-persisted-facts manifest (or empty)
 * A user may override this verbatim via a config file (see
 * {@link loadSessionMemoryPromptTemplate}); unknown tokens are left intact.
 */
export const DEFAULT_MEMORY_PROMPT_TEMPLATE =
  '## Tool Results\n{{toolResults}}\n\n## Recent Conversation\n{{recentConversation}}{{existingMemory}}';

/**
 * Single-pass, backreference-safe substitution of `{{name}}` tokens in a prompt
 * template. A FUNCTION replacer inserts each value LITERALLY, so a value containing
 * `$&`/`$1`/`$$` (or arbitrary user/tool text) is never reinterpreted as a regex
 * backreference, and the single pass means a value that itself contains `{{other}}`
 * is NOT re-expanded (no double-substitution / injection). Unknown tokens are left
 * untouched. Parity: the reference's SessionMemory `substituteVariables`
 * (SessionMemory/prompts.ts:L198-212).
 */
export function substituteMemoryVariables(template: string, variables: Record<string, string>): string {
  return template.replace(/\{\{(\w+)\}\}/g, (match, key: string) =>
    Object.prototype.hasOwnProperty.call(variables, key) ? variables[key] : match,
  );
}

/**
 * Load the user-overridable fact-extraction prompt template. Reads
 * `<configDir>/session-memory/config/prompt.md` (configDir defaults to the AMI
 * config home, `~/.ami`), returning its contents so a user can fully customize the
 * extraction prompt — including `{{toolResults}}` / `{{recentConversation}}` /
 * `{{existingMemory}}` substitution tokens. Falls back to
 * {@link DEFAULT_MEMORY_PROMPT_TEMPLATE} when the file is absent (ENOENT); any other
 * read error is logged and also falls back, so a malformed config can never break
 * extraction. Parity: the reference's `loadSessionMemoryPrompt`
 * (SessionMemory/prompts.ts:L106-129), rehomed under the AMI config dir.
 */
export async function loadSessionMemoryPromptTemplate(configDir?: string): Promise<string> {
  const base = configDir ?? resolveConfigDir();
  const promptPath = path.join(base, 'session-memory', 'config', 'prompt.md');
  try {
    return await readFile(promptPath, { encoding: 'utf-8' });
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== 'ENOENT') {
      coreLog('session-memory', 'Failed to read custom extraction prompt; using default', { error: String(err) });
    }
    return DEFAULT_MEMORY_PROMPT_TEMPLATE;
  }
}

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
  /** Test seam: the clock used to resolve relative dates in extracted facts.
   *  Defaults to the real wall clock; tests override it for determinism. */
  _now: () => Date = () => new Date();
  /** Config root under which the user-overridable extraction prompt is looked up
   *  (`<_configDir>/session-memory/config/prompt.md`). `undefined` → the resolved
   *  AMI config home (`~/.ami`). Tests point it at a temp dir. */
  _configDir?: string;
  /** Test seam: the sleep used by {@link waitForSessionMemoryExtraction}'s poll
   *  loop. Defaults to a real timer; tests override it to resolve instantly (and to
   *  advance a fake clock) so the wait/stale/timeout branches run without real time. */
  _sleep: (ms: number) => Promise<void> = (ms) => new Promise(resolve => setTimeout(resolve, ms));
  /** Poll interval (ms) of the extraction wait loop (parity: the reference polls every 1s). */
  _waitPollMs = 1000;

  /**
   * Wall-clock timestamp (ms, from {@link _now}) at which the CURRENT extraction
   * pass started, or `undefined` when no pass is in flight. Set by
   * {@link markExtractionStarted} (called at the top of {@link extractFacts}) and
   * cleared by {@link markExtractionCompleted} (its `finally`), so
   * {@link isExtractionInProgress} / {@link waitForSessionMemoryExtraction} always
   * reflect reality. Parity: the reference's module-level `extractionStartedAt`
   * (SessionMemory/sessionMemoryUtils.ts:L46,L74-82).
   */
  private _extractionStartedAt?: number;

  /**
   * Whether session memory has crossed its initialization threshold at least once.
   * Before this is true, {@link shouldExtractFacts} defers all extraction until the
   * context reaches `config.minInitContextTokens` — the first extraction is held off
   * until there is enough context to be worth persisting. Parity: the reference's
   * `isSessionMemoryInitialized` / `markSessionMemoryInitialized`.
   */
  private _initialized = false;
  /**
   * Total context-window token count recorded at the last extraction, used to
   * measure growth for the `minTokensBetweenUpdate` gate. Parity: the reference's
   * `tokensAtLastExtraction` (0 until the first extraction).
   */
  private _tokensAtLastExtraction = 0;

  constructor(provider: ProviderConfig, compactionModel?: string, config?: Partial<SessionMemoryConfig>) {
    this.provider = provider;
    this.compactionModel = compactionModel;
    this.config = { ...DEFAULT_SESSION_MEMORY_CONFIG, ...config };
    this._timeoutMs = this.config.extractionTimeoutMs;
  }

  /**
   * Threshold/cadence gate: whether enough has happened to warrant an extraction
   * pass, given the CURRENT total context-window token count. Mirrors the reference's
   * `shouldExtractMemory` exactly:
   *
   * 1. **Init gate** — before the first extraction, defer until the context reaches
   *    `config.minInitContextTokens`; there is nothing worth persisting from a tiny
   *    conversation. Once crossed, the extractor is marked initialized (a one-way
   *    latch) so later passes use only the growth gate.
   * 2. **Token (growth) gate** — context growth since the last extraction must reach
   *    `config.minTokensBetweenUpdate`. This gate is ALWAYS required: a burst of tool
   *    calls alone never triggers extraction until the context has actually grown,
   *    which prevents excessive extractions.
   * 3. **Trigger** — extract when the growth gate is met AND either the tool-call
   *    gate (`toolCallsSinceLast >= config.toolCallsBetweenUpdate`) is met, OR the
   *    turn ended at a natural break (`naturalBreak`, i.e. no tool calls in the last
   *    assistant turn) so pending work is captured at the boundary.
   *
   * This is a predicate with one documented side effect: it latches `initialized`
   * when the init threshold is first met (matching the reference). Call
   * {@link recordExtraction} after a pass actually runs to advance the growth
   * baseline, and {@link resetExtractionState} to clear the latch (e.g. a new session).
   */
  shouldExtractFacts(opts: {
    currentContextTokens: number;
    toolCallsSinceLast: number;
    naturalBreak?: boolean;
  }): boolean {
    const { currentContextTokens, toolCallsSinceLast, naturalBreak } = opts;

    // 1. Init gate (latches on first threshold-met).
    if (!this._initialized) {
      if (currentContextTokens < this.config.minInitContextTokens) return false;
      this._initialized = true;
    }

    // 2. Token-growth gate — measured against the context size at the last pass.
    const growth = currentContextTokens - this._tokensAtLastExtraction;
    const hasMetTokenThreshold = growth >= this.config.minTokensBetweenUpdate;

    // 3. Tool-call gate + natural-break special case. The token gate is always required.
    const hasMetToolCallThreshold = toolCallsSinceLast >= this.config.toolCallsBetweenUpdate;
    return (
      (hasMetTokenThreshold && hasMetToolCallThreshold) ||
      (hasMetTokenThreshold && naturalBreak === true)
    );
  }

  /**
   * Record that an extraction pass ran at `currentContextTokens`, advancing the
   * growth baseline for the next `minTokensBetweenUpdate` check and latching the
   * initialized state. Parity: the reference's `recordExtractionTokenCount` +
   * `markSessionMemoryInitialized`. Call this only after a pass actually completes.
   */
  recordExtraction(currentContextTokens: number): void {
    this._initialized = true;
    this._tokensAtLastExtraction = currentContextTokens;
  }

  /**
   * Clear the extraction cadence state (initialized latch + growth baseline) so the
   * init gate applies again — e.g. when switching to a fresh session. Parity:
   * the reference's `resetSessionMemoryState`.
   */
  resetExtractionState(): void {
    this._initialized = false;
    this._tokensAtLastExtraction = 0;
  }

  /**
   * Mark that a fact-extraction pass has started. Records the current timestamp so a
   * concurrent consumer (e.g. an interrupt-driven shutdown, or pre-compaction) can
   * coordinate with the in-flight pass via {@link waitForSessionMemoryExtraction}.
   * Called at the top of {@link extractFacts}; idempotent-safe to call directly.
   * Parity: the reference's `markExtractionStarted` (SessionMemory/sessionMemoryUtils.ts:L74-76).
   */
  markExtractionStarted(): void {
    this._extractionStartedAt = this._now().getTime();
  }

  /**
   * Mark the in-flight extraction pass as finished (clears the start timestamp), so
   * {@link waitForSessionMemoryExtraction} stops blocking. Called from
   * {@link extractFacts}'s `finally`. Parity: the reference's `markExtractionCompleted`
   * (SessionMemory/sessionMemoryUtils.ts:L81-83).
   */
  markExtractionCompleted(): void {
    this._extractionStartedAt = undefined;
  }

  /** Whether a fact-extraction pass is currently in flight (introspection seam for
   *  consumers/tests). Parity: the `extractionStartedAt` truthiness check the reference's
   *  wait helper performs (SessionMemory/sessionMemoryUtils.ts:L89). */
  isExtractionInProgress(): boolean {
    return this._extractionStartedAt !== undefined;
  }

  /**
   * Block until any in-flight extraction completes, so a consumer (compaction /
   * shutdown) does not race a pass that is still writing session memory. Returns
   * IMMEDIATELY when no pass is in flight. Two bail-out guards keep it from ever
   * hanging a caller:
   *  - **stale** — if the in-progress pass is older than
   *    `config.extractionStaleThresholdMs`, it is assumed crashed/abandoned (its
   *    completion mark never fired) and the wait returns without blocking further.
   *  - **timeout** — after `config.extractionWaitTimeoutMs` of waiting, the wait
   *    returns and the caller proceeds anyway.
   * Parity: the reference's `waitForSessionMemoryExtraction`
   * (SessionMemory/sessionMemoryUtils.ts:L88-106).
   */
  async waitForSessionMemoryExtraction(): Promise<void> {
    const startTime = this._now().getTime();
    while (this._extractionStartedAt !== undefined) {
      const extractionAge = this._now().getTime() - this._extractionStartedAt;
      // Stale extraction — do not wait on an abandoned pass.
      if (extractionAge > this.config.extractionStaleThresholdMs) return;
      // Overall wait budget exhausted — proceed anyway.
      if (this._now().getTime() - startTime > this.config.extractionWaitTimeoutMs) return;
      await this._sleep(this._waitPollMs);
    }
  }

  /**
   * Extract structured facts from tool results and recent messages.
   * Uses the compaction model if provided, otherwise the primary provider model.
   * Only includes the last `config.contextWindowMessages` messages as context.
   *
   * An optional caller `signal` is combined with the internal watchdog so a user
   * interrupt cancels extraction promptly instead of blocking up to the timeout.
   *
   * `existingFacts` (the facts already persisted for this session) are pre-injected
   * into the prompt as a manifest so the extraction model dedupes/updates against
   * them instead of re-deriving facts it already knows — parity with the reference's
   * formatMemoryManifest, which pre-injects the memory listing "so the extraction
   * agent doesn't spend a turn on `ls`" (memdir/memoryScan.ts:L24-34).
   */
  async extractFacts(
    toolResults: Array<{ toolName: string; output: string; isError: boolean }>,
    recentMessages: Message[],
    signal?: AbortSignal,
    existingFacts?: SessionFact[],
  ): Promise<SessionFact[]> {
    // Record the in-flight start so a concurrent consumer (shutdown/compaction) can
    // coordinate via waitForSessionMemoryExtraction instead of racing this pass.
    this.markExtractionStarted();
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

    // Pre-inject a manifest of already-persisted facts so the model can dedupe /
    // update against them instead of re-emitting known facts. Capped at maxEntries
    // and category-tagged, mirroring the reference's formatMemoryManifest affordance hint.
    const existingManifest = existingFacts && existingFacts.length > 0
      ? `\n\n## Existing Memory (already saved — do NOT repeat; only add new or updated facts)\n${existingFacts
          .slice(0, this.config.maxEntries)
          .map(f => `- [${f.category}] ${f.fact}`)
          .join('\n')}`
      : '';

    // Build the user content from the (possibly user-overridden) prompt template,
    // substituting the three content variables. The default template reproduces the
    // original fixed layout exactly; a user prompt.md can reorder/reword it.
    const promptTemplate = await loadSessionMemoryPromptTemplate(this._configDir);
    const userContent = substituteMemoryVariables(promptTemplate, {
      toolResults: toolResultsSummary,
      recentConversation: messageSummary,
      existingMemory: existingManifest,
    });

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
        // Background fact extraction: tag the request so a 529 overload bails
        // immediately instead of amplifying a capacity cascade with retries the
        // user never sees (shouldRetry529). All other statuses still retry.
        { querySource: 'session_memory' },
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
      // Clear the in-flight mark on EVERY exit (success, error-chunk early return,
      // thrown/aborted) so waitForSessionMemoryExtraction never blocks past the pass.
      this.markExtractionCompleted();
    }
    return facts;
  }

  /**
   * Merge new facts into existing facts, deduplicating by content similarity.
   * Near-duplicates (normalized-equal, or Jaccard token similarity at/above
   * `config.dedupSimilarityThreshold`) collapse so paraphrased restatements do
   * not accumulate; the higher-confidence variant wins. When the merged list
   * exceeds `config.maxEntries` OR its combined token count exceeds
   * `config.maxTotalTokens`, the newest `config.recencyProtectedCount` facts are
   * retained regardless of confidence and the remaining slots are filled by a
   * criticality-aware score until BOTH the entry-count and token budgets are
   * satisfied, so current-work and critical facts are not evicted by stale
   * high-confidence noise. The returned list is ordered by descending confidence.
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

    const overEntries = merged.length > this.config.maxEntries;
    const overTokens = this.totalFactTokens(merged) > this.config.maxTotalTokens;
    if (overEntries || overTokens) {
      // Insertion order == recency (existing first, newest appended), so the tail
      // is the most recent. Protect the newest N from eviction, then fill the
      // remaining slots from the older facts by a criticality-aware score, honoring
      // BOTH the entry-count cap and the aggregate token budget.
      const protectCount = Math.min(this.config.recencyProtectedCount, this.config.maxEntries);
      const recent = merged.slice(merged.length - protectCount);
      const older = merged.slice(0, merged.length - protectCount);
      // Rank eviction survivors by a criticality-aware score (confidence plus a
      // boost for critical categories) rather than confidence alone, so important
      // facts (errors, decisions, user preferences) are not crowded out by stale
      // high-confidence noise. The stored confidence is never mutated.
      older.sort((a, b) => this.evictionScore(b) - this.evictionScore(a));
      // Recency-protected facts are always kept (even if they alone exceed the token
      // budget — current work must survive). Then add older facts, highest-score
      // first, skipping any that would push the running total over the token budget,
      // and stopping once the entry-count cap is reached.
      const survivors = [...recent];
      let tokens = this.totalFactTokens(recent);
      for (const fact of older) {
        if (survivors.length >= this.config.maxEntries) break;
        const factTokens = estimateTextTokens(fact.fact);
        if (tokens + factTokens > this.config.maxTotalTokens) continue;
        survivors.push(fact);
        tokens += factTokens;
      }
      survivors.sort((a, b) => b.confidence - a.confidence);
      return survivors;
    }

    return merged;
  }

  /** Combined byte-aware token estimate of a set of facts' text, used to enforce the
   *  aggregate {@link SessionMemoryConfig.maxTotalTokens} budget in {@link mergeFacts}. */
  private totalFactTokens(facts: SessionFact[]): number {
    let total = 0;
    for (const fact of facts) total += estimateTextTokens(fact.fact);
    return total;
  }

  /**
   * Eviction ranking score for a fact: its confidence plus `criticalityBoost` when
   * its category is in `criticalCategories`. Used only to choose survivors when the
   * list exceeds `maxEntries`; it never mutates the fact's stored confidence.
   */
  private evictionScore(fact: SessionFact): number {
    const critical = this.config.criticalCategories.includes(fact.category);
    return fact.confidence + (critical ? this.config.criticalityBoost : 0);
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
        })
        // Normalize any unambiguous relative dates the model left in the fact text,
        // so facts stay interpretable when recalled in a later session.
        .map(f => ({ ...f, fact: normalizeRelativeDates(f.fact, this._now()) }));
    } catch {
      coreLog('session-memory', 'Failed to parse facts JSON from response');
      return [];
    }
  }
}
