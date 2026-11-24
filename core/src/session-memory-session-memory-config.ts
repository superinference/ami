// Split from session-memory.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { readFile } from 'node:fs/promises';
import * as path from 'node:path';
import type { Message } from './types';
import { log as coreLog } from './logger';
import { resolveConfigDir } from './reference-journal';

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

export const SUSPICIOUS_PATTERNS = [
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
export const SECRET_PATTERNS: RegExp[] = [
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

/** Normalize a fact for similarity comparison: lowercase, drop punctuation,
 *  collapse whitespace. */
export function normalizeForDedup(s: string): string {
  return s
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

/** Jaccard token similarity of two fact strings in [0,1]. Two empty strings are
 *  identical (1); an empty vs. non-empty pair shares nothing (0). */
export function factSimilarity(a: string, b: string): number {
  const sa = new Set(normalizeForDedup(a).split(' ').filter(Boolean));
  const sb = new Set(normalizeForDedup(b).split(' ').filter(Boolean));
  if (sa.size === 0 && sb.size === 0) return 1;
  let intersection = 0;
  for (const token of sa) if (sb.has(token)) intersection++;
  const union = sa.size + sb.size - intersection;
  return union === 0 ? 0 : intersection / union;
}

export const EXTRACTION_PROMPT = `Extract key facts from these tool results that would be useful to remember across conversation turns. Return a JSON array of objects with the following shape:

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
