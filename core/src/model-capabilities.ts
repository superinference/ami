import * as fs from 'fs';
import * as path from 'path';
import { formatTokenCount } from './token-format';
import { capitalize } from './utils/string';

export const THINKING_LEVELS = ['off', 'low', 'medium', 'high', 'max'] as const;
export type ThinkingLevel = (typeof THINKING_LEVELS)[number];

/**
 * Human-readable, per-level descriptions of what each reasoning/thinking tier
 * means, surfaced by `/thinking` (no-arg state, `help`/info args, and invalid
 * input) so a user can pick a level deliberately instead of guessing. Mirrors
 * Claude's `/effort help` per-level copy (commands/effort/effort.tsx:L173-L176)
 * and `getEffortValueDescription` (utils/effort.ts); AMI additionally documents
 * `off` since its scale includes a disabled tier that Claude's does not.
 */
export const THINKING_LEVEL_DESCRIPTIONS: Record<ThinkingLevel, string> = {
  off: 'Thinking disabled — fastest, no extended reasoning',
  low: 'Quick, straightforward reasoning',
  medium: 'Balanced reasoning for most tasks',
  high: 'Comprehensive reasoning for hard problems',
  max: 'Maximum capability with the deepest reasoning',
};

/**
 * The human-readable description for a thinking level, or undefined when the
 * level is not a recognized {@link ThinkingLevel}. Keeps callers (slash-command
 * handlers, help text) from re-deriving the copy and drifting out of sync.
 */
export function getThinkingLevelDescription(level: string): string | undefined {
  return (THINKING_LEVEL_DESCRIPTIONS as Record<string, string>)[level];
}

// The reasoning-EFFORT scale for code review / subagents (distinct from THINKING_LEVELS:
// no 'off', adds 'xhigh'). Canonical source for /review --effort, the band's effort chip,
// and the --introspect manifest, so a hardcoded subset can't drift (audit #14).
export const REVIEW_EFFORT_LEVELS = ['low', 'medium', 'high', 'xhigh', 'max'] as const;
export type ReviewEffortLevel = (typeof REVIEW_EFFORT_LEVELS)[number];

export interface ModelCapabilities {
  reasoning: boolean;
  defaultThinkingLevel: ThinkingLevel;
  supportsAdaptiveThinking: boolean;
  requiresTemperatureOne: boolean;
  temperatureMustBeUnset: boolean;
  maxThinkingBudget: number;
  maxContextTokens?: number;
  maxOutputTokens?: number;
  supportsVision?: boolean;
  /** Whether the model accepts native `document` (PDF) content blocks. Checked by
   *  {@link modelSupportsDocuments}; distinct from {@link supportsVision}. */
  supportsDocuments?: boolean;
  supportsToolUse?: boolean;
  /**
   * The model defaults to adaptive thinking and REJECTS a request to disable it
   * (`thinking: { type:'disabled' }`) — reasoning cannot be turned off. Mirrors
   * Claude Code's AntModel `alwaysOnThinking` knob (utils/model/antModels.ts:L14-15).
   * When true, callers that build the per-provider thinking request must still send
   * a thinking config even on the `off` level (the model reasons regardless), and
   * UIs should not advertise `off` as an effective choice for such a model.
   */
  alwaysOnThinking?: boolean;
}

/** Used when the server omits a length and the model id is not in the table. */
export const DEFAULT_CONTEXT_WINDOW = 128_000;

/**
 * Published context windows. Longer ids come first so `grok-4.20` is not
 * claimed by a shorter `grok-4` prefix. A live `/models` payload overrides
 * this table.
 *
 * Proprietary lengths checked 2026-10-06 against Anthropic's models overview
 * (Fable 5.1, Opus 5.5, Sonnet 5.5 at 1M; Haiku 4.5 stays on the 200k key),
 * OpenAI's models page (GPT-6 Astra, 6.1 Sol, and Luna at 1.05M), and xAI's
 * model pricing (Grok 4.5–4.7 at 500k, Grok 4.3 and 4.20 at 1M).
 */
const CONTEXT_WINDOWS: Record<string, number> = {
  'claude-fable-5': 1_000_000,
  'claude-opus-5': 1_000_000,
  'claude-sonnet-5': 1_000_000,
  'gpt-5.6-luna': 1_000_000,
  'gpt-6-astra': 1_050_000,
  'gpt-6.1-sol': 1_050_000,
  'gpt-6-luna': 1_050_000,
  'gpt-6': 1_050_000,
  'grok-4.20': 1_000_000,
  'grok-4.7': 500_000,
  'grok-4.6': 500_000,
  'grok-4.5': 500_000,
  'grok-4.3': 1_000_000,
  'grok-build-0.1': 256_000,
  'claude-opus-4': 200000,
  'claude-sonnet-4': 200000,
  'claude-haiku-3.5': 200000,
  'claude-haiku-4': 200000,
  'claude-3-5-sonnet': 200000,
  'claude-3-opus': 200000,
  'gpt-4o-mini': 128000,
  'gpt-4o': 128000,
  'gpt-4-turbo': 128000,
  'gpt-4.1': 1048576,
  'o1-mini': 128000,
  'o1': 200000,
  'o3-mini': 200000,
  'o3': 200000,
  'o4-mini': 200000,
  'gemini-2.5-pro': 1048576,
  'gemini-2.5-flash': 1048576,
  'gemini-2.0-flash': 1048576,
  'gemini-3.6': 2097152,
  'gemini-3.1': 2097152,
  'gemini-3': 2097152,
  'gemini-1.5-pro': 1048576,
  'deepseek-chat': 64000,
  'deepseek-reasoner': 64000,
  // Qwen3.x — 256k native context
  'qwen3.8': 262144,
  'qwen3': 262144,
  // Qwen2.5 — size-specific context windows (most specific first)
  'qwen2.5-0.5b': 32768,
  'qwen2.5-1.5b': 32768,
  'qwen2.5-3b': 32768,
  'qwen2.5-7b': 131072,
  'qwen2.5-14b': 131072,
  'qwen2.5-32b': 131072,
  'qwen2.5-72b': 131072,
  'qwen2.5': 32768,
  'qwen2': 32768,
  // Small open-weight models
  'llama-3.2-1b': 131072,
  'llama-3.2-3b': 131072,
  'phi-3-mini': 4096,
  'phi-3.5-mini': 131072,
  'phi-4-mini': 131072,
};

/**
 * Detect the Anthropic 1M-context beta marker on a model id: the bracketed
 * `[1m]` form or a trailing `-1m`/`_1m`. Such a variant has a 1M window even
 * when its base id is a 200k key (U4) — otherwise the substring match below
 * would under-report it as 200k and trip compaction far too early.
 */
function hasOneMillionMarker(modelId: string): boolean {
  return /\[1m\]|[-_]1m\b/i.test(modelId);
}

/**
 * Resolve a skill's `model:` override against the session's current model,
 * carrying the `[1m]` 1M-context marker over when the target family supports it.
 *
 * A skill author writing `model: <id>` means "use that model class" — not
 * "downgrade the context window". If the user is on an explicit `<id>[1m]`
 * session and invokes a skill whose `model:` names the same (1M-capable) family
 * WITHOUT the marker, a raw overwrite silently drops the effective window from
 * 1M to the base (200k) key, which trips compaction/autocompact far too early.
 *
 * Rules (mirrors Claude Code's `resolveSkillModelOverride`):
 * - The skill model already carries a 1M marker → leave it untouched.
 * - The current session model has no 1M marker → nothing to carry, return as-is.
 * - Otherwise carry `[1m]` ONLY when the skill's target family actually supports
 *   the 1M beta (Sonnet 4+/Opus 5+); a `model: haiku` on a 1M session still
 *   downgrades, because haiku has no 1M variant and the smaller window is correct.
 */
export function resolveSkillModelOverride(skillModel: string, currentModel: string): string {
  if (hasOneMillionMarker(skillModel) || !hasOneMillionMarker(currentModel)) {
    return skillModel;
  }
  if (supportsOneMillionContextBeta(skillModel)) {
    return `${skillModel}[1m]`;
  }
  return skillModel;
}

/**
 * Anthropic's 1M-context window is gated behind the `context-1m-2025-08-07`
 * beta header. Per Anthropic's docs it is available on the Sonnet 4+ family AND
 * the Opus 5+ family — Claude Code ships a dedicated `checkOpus1mAccess()`
 * parallel to `checkSonnet1mAccess()`, i.e. Opus is a gated 1M-beta family too
 * (ref utils/model/check1mAccess.ts:L45-L58). We detect conservatively by
 * model-id substring: Sonnet major version 4+ and Opus major version 5+ (Opus 4
 * and earlier stay on their 200k window), so the header is never sent to a model
 * that would reject it. The id may carry a gateway prefix (e.g.
 * `openrouter/anthropic/claude-sonnet-4-5`), so the match is unanchored.
 */
/**
 * Operator kill-switch to force-disable the Anthropic 1M-context window
 * (compliance / cost control), independent of model detection. When set, the
 * `context-1m-2025-08-07` beta is never requested AND the local context budget
 * falls back to the model's base (200k) window, so compaction timing matches
 * what the endpoint will actually serve. Read at call time so it can be toggled
 * per-process/test. Mirrors Claude's is1mContextDisabled env gate.
 */
export function is1mContextDisabled(): boolean {
  const v = process.env.SUPERINFERENCE_DISABLE_1M_CONTEXT;
  if (!v) return false;
  return /^(1|true|yes)$/i.test(v.trim());
}

export function supportsOneMillionContextBeta(modelId: string): boolean {
  if (typeof modelId !== 'string' || !modelId) return false;
  if (is1mContextDisabled()) return false;
  // Sonnet 4+ or Opus 5+ — the two Anthropic families gated behind the 1M beta
  // (checkSonnet1mAccess / checkOpus1mAccess). Opus 4 and earlier are excluded.
  return /claude-sonnet-(?:[4-9]|\d{2,})|claude-opus-(?:[5-9]|\d{2,})/i.test(modelId);
}

/**
 * Strip the synthetic context-window marker from a model id before it is sent to
 * the provider API. The `[1m]`/`[2m]` bracket suffix (and AMI's trailing
 * `-1m`/`_1m` forms recognized by {@link hasOneMillionMarker}) is a LOCAL knob
 * that only sizes the context budget / selects the beta header — the endpoint's
 * model id has no such marker and rejects it (an unknown model) when it leaks
 * through. Callers resolve betas/window sizing from the marker-carrying id FIRST,
 * then normalize right before the request. Mirrors Claude Code's
 * `normalizeModelStringForAPI` (utils/model/model.ts:L616-L618), extended to the
 * `-1m`/`_1m` suffix forms AMI also treats as markers. A non-string id passes
 * through unchanged.
 */
export function normalizeModelStringForAPI(modelId: string): string {
  if (typeof modelId !== 'string') return modelId;
  return modelId.replace(/\[(?:1|2)m\]/gi, '').replace(/[-_]1m\b/gi, '');
}

/**
 * Operator env-var override that caps the EFFECTIVE context window used for local
 * decisions (compaction timing, output fitting, model-switch warnings), regardless
 * of the model's published window or any 1M-beta detection. Set
 * `SUPERINFERENCE_MAX_CONTEXT_TOKENS` to a positive integer to clamp the window —
 * useful when the endpoint serves a 1M window but a smaller budget is wanted for
 * cost/latency control. Returns `undefined` when unset, non-numeric, or <= 0 so the
 * normal resolution path runs. Read at call time so it can be toggled per
 * process/test. Mirrors Claude's `CLAUDE_CODE_MAX_CONTEXT_TOKENS` override.
 */
export function getContextWindowOverride(): number | undefined {
  const raw = process.env.SUPERINFERENCE_MAX_CONTEXT_TOKENS;
  if (!raw) return undefined;
  const parsed = parseInt(raw.trim(), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return undefined;
  return parsed;
}

/**
 * Env vars naming a dedicated small/fast model for cheap, throwaway auxiliary LLM
 * calls (prompt-hook verification verdicts, away/session recaps, title
 * generation) so they do not burn the main model's budget/latency. Checked in
 * order; the first non-empty value wins. Mirrors Claude Code's
 * `getSmallFastModel` (utils/model/model.ts:L36-L38), which keys off
 * `ANTHROPIC_SMALL_FAST_MODEL`; AMI also honors a provider-neutral `AMI_SMALL_FAST_MODEL`.
 */
export const SMALL_FAST_MODEL_ENV_VARS = ['ANTHROPIC_SMALL_FAST_MODEL', 'AMI_SMALL_FAST_MODEL'] as const;

/**
 * The configured small/fast model id for cheap auxiliary calls, or `fallback`
 * when none is set. Unlike Claude (which hardcodes a Haiku default), AMI is
 * multi-provider, so the knob only activates when an operator sets it — callers
 * pass the session model as `fallback` to preserve current behavior (aux calls
 * reuse the main model) when the env is unset. Read at call time so it can be
 * toggled per process/test.
 */
export function getSmallFastModel(
  fallback?: string,
  env: NodeJS.ProcessEnv = process.env,
): string | undefined {
  for (const key of SMALL_FAST_MODEL_ENV_VARS) {
    const value = env[key];
    if (typeof value === 'string' && value.trim() !== '') return value.trim();
  }
  return fallback;
}

export function getContextWindow(modelId: string): number {
  // Operator override takes precedence over all detection (table, 1M marker/beta),
  // so a smaller local budget can be enforced even on a 1M-capable endpoint.
  const override = getContextWindowOverride();
  if (override !== undefined) return override;
  const lower = modelId.toLowerCase();
  // Grant the 1M window either when the id carries an explicit [1m]/-1m marker,
  // OR when the model is one for which the provider unconditionally sends the
  // `context-1m-2025-08-07` beta (Sonnet 4+). In the latter case the endpoint
  // actually serves a 1M window with no id marker, so the local budget must match
  // or compaction fires far too early. Mirrors Claude's betas+modelSupports1M path.
  const oneMillion = !is1mContextDisabled() && (hasOneMillionMarker(modelId) || supportsOneMillionContextBeta(modelId));
  const applyBeta = (tokens: number) => (oneMillion ? Math.max(tokens, 1_000_000) : tokens);
  const entries = Object.entries(CONTEXT_WINDOWS);
  // 1. Exact id match (case-insensitive) always wins — the most-specific resolution.
  //    This matters for runtime-discovered ids appended by discoverFromModelList in
  //    arbitrary order, where a shorter family key could otherwise shadow the full id.
  //    Mirrors Claude's getModelCapability (exact-match before substring, utils/model/modelCapabilities.ts:L75-L83).
  for (const [key, tokens] of entries) {
    if (key.toLowerCase() === lower) return applyBeta(tokens);
  }
  // 2. Substring match, most-specific (longest key) first with a stable localeCompare
  //    tiebreak, so most-specific-wins holds regardless of table/insertion order
  //    (ref sortForMatching, utils/model/modelCapabilities.ts:L53-L58).
  const byLength = entries.slice().sort((a, b) => b[0].length - a[0].length || a[0].localeCompare(b[0]));
  for (const [key, tokens] of byLength) {
    const keyLower = key.toLowerCase();
    const idx = lower.indexOf(keyLower);
    if (idx === -1) continue;
    if (keyLower.length <= 3 && idx > 0 && /[a-zA-Z0-9]/.test(lower[idx - 1])) continue;
    return applyBeta(tokens);
  }
  return applyBeta(DEFAULT_CONTEXT_WINDOW);
}

const ADVERTISED_CONTEXT_KEYS = [
  'max_input_tokens',
  'max_model_len',
  'context_length',
  'context_window',
  'max_context_length',
  'input_token_limit',
  'inputTokenLimit',
  'max_position_embeddings',
] as const;

function positiveTokenCount(value: unknown): number | undefined {
  if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0) return undefined;
  return Math.floor(value);
}

/**
 * Read a context length from one `/models` record.
 * Anthropic uses `max_input_tokens`, vLLM uses `max_model_len`, OpenRouter
 * uses `context_length`, and Gemini uses `inputTokenLimit`. `max_tokens` is
 * the completion cap and is ignored. Zero means the field was withheld.
 */
export function readAdvertisedContextWindow(record: Record<string, unknown>): number | undefined {
  const nested = record.model_info;
  const sources = [record, ...(nested && typeof nested === 'object' && !Array.isArray(nested) ? [nested as Record<string, unknown>] : [])];
  for (const source of sources) {
    for (const key of ADVERTISED_CONTEXT_KEYS) {
      const found = positiveTokenCount(source[key]);
      if (found !== undefined) return found;
    }
  }
  return undefined;
}

/**
 * Field names a `/models` record may use for the completion (max-output) cap.
 * Deliberately disjoint from ADVERTISED_CONTEXT_KEYS: `max_tokens` is the output
 * ceiling (Anthropic/OpenAI), never the context window. OpenRouter/Gemini-style
 * payloads use `max_output_tokens` / `max_completion_tokens` / `output_token_limit`.
 */
const ADVERTISED_OUTPUT_KEYS = [
  'max_tokens',
  'max_output_tokens',
  'max_completion_tokens',
  'output_token_limit',
  'outputTokenLimit',
  'maxOutputTokens',
] as const;

/**
 * Read a max-output-token cap from one `/models` record (top-level or nested
 * `model_info`). Returns undefined when no positive value is advertised.
 */
export function readAdvertisedMaxOutputTokens(record: Record<string, unknown>): number | undefined {
  const nested = record.model_info;
  const sources = [record, ...(nested && typeof nested === 'object' && !Array.isArray(nested) ? [nested as Record<string, unknown>] : [])];
  for (const source of sources) {
    for (const key of ADVERTISED_OUTPUT_KEYS) {
      const found = positiveTokenCount(source[key]);
      if (found !== undefined) return found;
    }
  }
  return undefined;
}

function modelIdsMatch(requested: string, advertised: string): boolean {
  const normalize = (id: string) => id.toLowerCase().replace(/^models\//, '');
  const want = normalize(requested);
  const got = normalize(advertised);
  if (!want || !got) return false;
  if (want === got) return true;
  const tail = (id: string) => {
    const parts = id.split('/');
    return parts[parts.length - 1];
  };
  return tail(want) === tail(got);
}

/** Pull model records out of OpenAI, Anthropic, and Gemini list payloads. */
export function collectModelRecords(payload: unknown): Record<string, unknown>[] {
  if (!payload || typeof payload !== 'object') return [];
  const body = payload as Record<string, unknown>;
  for (const key of ['data', 'models'] as const) {
    const list = body[key];
    if (Array.isArray(list)) {
      return list.filter((item): item is Record<string, unknown> => !!item && typeof item === 'object' && !Array.isArray(item));
    }
  }
  if (typeof body.id === 'string' || typeof body.name === 'string') return [body];
  return [];
}

/**
 * Choose a window from a models list. An exact id wins. A single-model
 * server (typical vLLM) is used when the ids differ. No length means the
 * caller should try a Hugging Face model card, then the table, then the default.
 */
export function pickAdvertisedContextWindow(records: Record<string, unknown>[], modelId: string): number | undefined {
  for (const record of records) {
    const id = String(record.id ?? record.name ?? '');
    if (!modelIdsMatch(modelId, id)) continue;
    const window = readAdvertisedContextWindow(record);
    if (window !== undefined) return window;
  }
  if (records.length === 1) return readAdvertisedContextWindow(records[0]);
  return undefined;
}

/** Tokenizer configs use this as "unset". It is not a context window. */
const HUGGINGFACE_LENGTH_SENTINEL = 1_000_000_000;

const HUGGINGFACE_CONTEXT_FIELDS = [
  'max_position_embeddings',
  'max_sequence_length',
  'max_sequence_len',
  'seq_length',
  'seq_len',
  'n_positions',
  'n_ctx',
  'model_max_length',
  'context_length',
  'max_input_tokens',
  'sliding_window',
] as const;

const HUGGINGFACE_NESTED_CONFIGS = [
  'text_config',
  'llm_config',
  'language_config',
  'config',
  'cardData',
  'tokenizer_config',
] as const;

function collectHuggingFaceLengths(value: unknown, depth: number, out: number[]): void {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return;
  const record = value as Record<string, unknown>;
  for (const key of HUGGINGFACE_CONTEXT_FIELDS) {
    const found = positiveTokenCount(record[key]);
    if (found !== undefined && found < HUGGINGFACE_LENGTH_SENTINEL) out.push(found);
  }
  // One nested config (text_config) plus one more (its own config). Deeper
  // wrappers are not a context length.
  if (depth >= 2) return;
  for (const key of HUGGINGFACE_NESTED_CONFIGS) {
    collectHuggingFaceLengths(record[key], depth + 1, out);
  }
}

/**
 * Largest context length published in a Hugging Face config or model card.
 * `max_position_embeddings` is often the base window while `sliding_window`
 * holds the extended one (Qwen2.5-7B is 32,768 and 131,072). The larger
 * value is the window the model can actually take. `original_max_position_embeddings`
 * is the pre-extension length and is ignored. Tokenizer sentinels at 1e30
 * are ignored.
 */
export function readHuggingFaceContextWindow(payload: unknown): number | undefined {
  const found: number[] = [];
  collectHuggingFaceLengths(payload, 0, found);
  if (found.length === 0) return undefined;
  return Math.max(...found);
}

/** `owner/name` repo id. Bare proprietary ids and gateway prefixes are skipped. */
export function huggingfaceRepoId(modelId: string): string | undefined {
  const id = modelId.trim().replace(/^models\//i, '');
  if (!id || /\s/.test(id) || id.includes('://') || id.includes('\\')) return undefined;
  const parts = id.split('/').filter(Boolean);
  if (parts.length !== 2) return undefined;
  if (parts.some(part => part === '.' || part === '..')) return undefined;
  return parts.join('/');
}

/**
 * Hub repos worth a model-card lookup. The requested id counts when it is
 * `owner/name`. A one-model server also counts, so a vLLM alias can still
 * resolve to the repo it is serving. A long catalog is not searched.
 */
export function huggingfaceRepoCandidates(modelId: string, records: Record<string, unknown>[]): string[] {
  const out: string[] = [];
  const add = (id: unknown) => {
    if (typeof id !== 'string') return;
    const repo = huggingfaceRepoId(id);
    if (!repo) return;
    if (out.some(existing => existing.toLowerCase() === repo.toLowerCase())) return;
    out.push(repo);
  };
  add(modelId);
  if (records.length === 1) {
    add(records[0].id);
    add(records[0].name);
  }
  return out;
}

/**
 * Parse a Claude model id into family/major/minor, ignoring any trailing date snapshot
 * (…-YYYYMMDD or …@YYYYMMDD). A flat regex cannot do this reliably because an 8-digit
 * date segment looks like a very large minor version — so we split the post-major segments
 * and treat only a 1-2 digit leading segment as the minor version.
 *
 * Examples:
 *   claude-sonnet-4-5            -> { sonnet, 4, 5 }
 *   claude-sonnet-4-5-20251022  -> { sonnet, 4, 5 }   (date ignored)
 *   claude-sonnet-4-20250514    -> { sonnet, 4, 0 }   (bare 4.0, dated — NOT minor 20250514)
 *   claude-opus-4               -> { opus, 4, 0 }
 *   claude-opus-5-5             -> { opus, 5, 5 }
 *   openrouter/anthropic/claude-sonnet-4-6 -> { sonnet, 4, 6 }  (unanchored)
 */
export function parseClaudeVersion(
  modelId: string,
): { family: string; major: number; minor: number } | null {
  if (typeof modelId !== 'string') return null;
  // Simple anchor (no nested quantifier → no ReDoS); the trailing version segments are parsed by
  // splitting on '-' rather than a `(?:-\d+)*` group.
  const m = /claude-(opus|sonnet|haiku|fable|mythos)-(\d+)/i.exec(modelId);
  if (!m) return null;
  const family = m[1].toLowerCase();
  const major = parseInt(m[2], 10);
  const after = modelId.slice(m.index + m[0].length); // e.g. '-5', '-5-20251022', '-20250514', ''
  const segs = after.split('-').filter(Boolean);
  // A 1-2 digit leading segment is the minor version; a longer run (a …-YYYYMMDD date) is not.
  const minor = segs.length > 0 && /^\d{1,2}$/.test(segs[0]) ? parseInt(segs[0], 10) : 0;
  return { family, major, minor };
}

/**
 * Capabilities for a thinking-capable Claude model, classified by version. Adaptive thinking
 * (`thinking: { type:'adaptive' }`) is supported ONLY on Claude 4.6+ and the 5.x family; the
 * pre-4.6 Claude 4.x models (opus 4/4.1/4.5, sonnet 4/4.5) REJECT adaptive thinking with an
 * HTTP 400 ("adaptive thinking is not supported on this model") and must use the legacy
 * `{ type:'enabled', budget_tokens }` shape instead. This split is exactly what the
 * @ai-sdk/anthropic provider's own model metadata encodes (supportsAdaptiveThinking) — ui#24.
 * Returns null for non-reasoning Claude families (e.g. haiku), which keep thinking off.
 */
export function claudeReasoningCapabilities(modelId: string): ModelCapabilities | null {
  const v = parseClaudeVersion(modelId);
  if (!v) return null;
  // Current reasoning scope: Opus / Sonnet. Other families (haiku, …) stay non-reasoning.
  if (v.family !== 'opus' && v.family !== 'sonnet') return null;
  const supportsAdaptiveThinking = v.major >= 5 || (v.major === 4 && v.minor >= 6);
  // The 5.x Claude line defaults to adaptive thinking and rejects a disabled-thinking
  // request — reasoning cannot be turned off (Claude's AntModel.alwaysOnThinking). The
  // 4.x family (including the adaptive 4.6+) still accepts an off/disabled request.
  const alwaysOnThinking = v.major >= 5;
  return {
    reasoning: true,
    defaultThinkingLevel: 'medium',
    supportsAdaptiveThinking,
    alwaysOnThinking,
    requiresTemperatureOne: true,
    temperatureMustBeUnset: false,
    // The thinking budget must stay strictly below the model's real output cap —
    // a model whose completion ceiling is 32k (e.g. Opus 4.0) cannot spend 128k
    // on thinking. Derive it from the per-family output upperLimit (upperLimit-1,
    // mirroring Claude's getMaxThinkingTokensForModel) when the model is in the
    // output-cap table; otherwise keep the 128k default for untabled flagship ids
    // so a newer model is never under-budgeted. Uses the static lookup directly to
    // avoid recursing through getModelCapabilities (which calls this function).
    maxThinkingBudget: deriveMaxThinkingBudget(modelId),
  };
}

/**
 * The thinking-token ceiling for a Claude model, tied to its real output cap.
 * Returns the tabled output upperLimit minus one (parity with Claude's
 * getMaxThinkingTokensForModel, utils/context.ts) so thinking can never request
 * more than the model will emit; falls back to 128_000 when the id is not in the
 * static output-cap table (an untabled flagship). Reads the static table directly
 * — NOT getMaxOutputTokens — because getMaxOutputTokens → getModelCapabilities →
 * claudeReasoningCapabilities would recurse.
 */
function deriveMaxThinkingBudget(modelId: string): number {
  const lower = modelId.toLowerCase();
  // A completion cap learned from a live `/models` payload (discoverFromModelList)
  // bounds the thinking budget exactly as it bounds output — thinking can never
  // request more tokens than the model will actually emit. Prefer it over the
  // static family ladder so a server-advertised output cap dynamically resizes the
  // thinking ceiling (parity with Claude's getMaxThinkingTokensForModel, which
  // derives the thinking cap from the live output upperLimit). A plain Map read, so
  // it introduces no recursion back through getModelCapabilities.
  const dynamicCap = dynamicOutputCaps.get(lower);
  if (dynamicCap !== undefined && dynamicCap > 0) return Math.max(1_024, dynamicCap - 1);
  const upperLimit = lookupOutputTokenRange(lower)?.upperLimit;
  return upperLimit !== undefined ? upperLimit - 1 : 128000;
}

const REASONING_MODELS: Array<{
  pattern: RegExp;
  capabilities: ModelCapabilities;
}> = [
  // OpenAI reasoning models — reasoning_effort
  { pattern: /^o1($|-)/,
    capabilities: { reasoning: true, defaultThinkingLevel: 'medium',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: true, maxThinkingBudget: 0 } },
  { pattern: /^o3($|-)/,
    capabilities: { reasoning: true, defaultThinkingLevel: 'medium',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: true, maxThinkingBudget: 0 } },
  { pattern: /^o4-mini/,
    capabilities: { reasoning: true, defaultThinkingLevel: 'medium',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: true, maxThinkingBudget: 0 } },

  // OpenAI GPT-5+ — reasoning models with reasoning_effort
  { pattern: /^gpt-(?:[5-9]|\d{2,})/,
    capabilities: { reasoning: true, defaultThinkingLevel: 'medium',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: true, maxThinkingBudget: 0 } },

  // Google Gemini 2.5+ — thinkingConfig
  { pattern: /gemini-2\.5-pro/,
    capabilities: { reasoning: true, defaultThinkingLevel: 'medium',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: false, maxThinkingBudget: 32768 } },
  { pattern: /gemini-2\.5-flash/,
    capabilities: { reasoning: true, defaultThinkingLevel: 'low',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: false, maxThinkingBudget: 32768 } },

  // DeepSeek reasoning — inline <think> tags
  { pattern: /deepseek-r1|deepseek-reasoner/,
    capabilities: { reasoning: true, defaultThinkingLevel: 'high',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: false, maxThinkingBudget: 0 } },
];

// ---------------------------------------------------------------------------
// Dynamic model capability registry — populated at runtime from /v1/models
// ---------------------------------------------------------------------------

const dynamicCapabilities = new Map<string, ModelCapabilities | null>();

/**
 * Max-output-token caps learned from a live `/models` payload, keyed by the
 * lowercased full model id. Checked (exact-match) ahead of the static family
 * table in getMaxOutputTokens so a provider-advertised completion cap overrides
 * the built-in estimate. Populated by discoverFromModelList; cleared alongside
 * the dynamic capability registry.
 */
const dynamicOutputCaps = new Map<string, number>();

export function registerModelCapabilities(modelId: string, caps: ModelCapabilities | null): void {
  dynamicCapabilities.set(modelId.toLowerCase(), caps);
}

export function clearDynamicCapabilities(): void {
  dynamicCapabilities.clear();
  dynamicOutputCaps.clear();
  cacheReadMemo.clear();
}

const OPENAI_REASONING_PATTERN = /^o[1-9]($|-)/;
function parseGptMajorVersion(modelId: string): number | null {
  if (!modelId.startsWith('gpt-')) return null;
  const afterPrefix = modelId.slice(4);
  const digitEnd = afterPrefix.search(/\D/);
  const majorStr = digitEnd === -1 ? afterPrefix : afterPrefix.slice(0, digitEnd);
  if (!majorStr) return null;
  const major = parseInt(majorStr, 10);
  return Number.isFinite(major) ? major : null;
}

function classifyOpenAIModel(modelId: string): ModelCapabilities | null {
  if (OPENAI_REASONING_PATTERN.test(modelId)) {
    return {
      reasoning: true, defaultThinkingLevel: 'medium',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: true, maxThinkingBudget: 0,
    };
  }
  const major = parseGptMajorVersion(modelId);
  if (major !== null && major >= 5) {
    return {
      reasoning: true, defaultThinkingLevel: 'medium',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: true, maxThinkingBudget: 0,
    };
  }
  return null;
}

/**
 * Returns true if the model requires the /v1/responses endpoint for tool use.
 * gpt-5.6+ models do not support function tools with reasoning on /v1/chat/completions.
 */
export function requiresResponsesAPI(modelId: string): boolean {
  if (!modelId.startsWith('gpt-')) return false;
  const afterPrefix = modelId.slice(4);
  const versionMatch = afterPrefix.match(/^(\d+\.\d+|\d+)/);
  if (!versionMatch) return false;
  const version = parseFloat(versionMatch[1]);
  return version >= 5.6;
}

export function discoverFromModelList(records: Record<string, unknown>[]): {
  registered: string[];
  contextWindows: Record<string, number>;
  outputCaps: Record<string, number>;
} {
  const registered: string[] = [];
  const contextWindows: Record<string, number> = {};
  const outputCaps: Record<string, number> = {};
  for (const record of records) {
    const id = String(record.id ?? record.name ?? '');
    if (!id) continue;
    const window = readAdvertisedContextWindow(record);
    if (window !== undefined) {
      CONTEXT_WINDOWS[id] = window;
      contextWindows[id] = window;
    }
    // Learn the completion cap from the live payload so fitOutputTokens clamps to
    // what the endpoint actually accepts, overriding the static family estimate.
    const outputCap = readAdvertisedMaxOutputTokens(record);
    if (outputCap !== undefined) {
      dynamicOutputCaps.set(id.toLowerCase(), outputCap);
      outputCaps[id] = outputCap;
    }
    if (!dynamicCapabilities.has(id.toLowerCase())) {
      const caps = classifyOpenAIModel(id);
      dynamicCapabilities.set(id.toLowerCase(), caps);
      registered.push(id);
    }
  }
  return { registered, contextWindows, outputCaps };
}

// ---------------------------------------------------------------------------
// Persistent on-disk cache of provider-advertised model capabilities
// ---------------------------------------------------------------------------
//
// A provider's live /models listing advertises each model's context window and
// completion cap. Re-fetching that on every cold start is wasteful and leaves the
// FIRST turn under-sized (compaction can fire too early) until discovery lands.
// Mirroring Claude Code's refreshModelCapabilities (utils/model/modelCapabilities.ts
// :L60-118), AMI persists the discovered windows/caps to a JSON file so the next
// run can WARM them before — or in lieu of — a network fetch. The file carries a
// timestamp, is written with a sorted, stable model list, is skipped when nothing
// changed (no spurious mtime churn), and is created 0600 (owner-only) because it
// records which models an operator uses.
//
// Opt-in by directory: the cache is active only when a directory is resolved via
// SUPERINFERENCE_MODEL_CACHE_DIR (read at call time). With it unset every function
// here is a no-op, so default behavior and existing discovery are unchanged.

/** One cached model's advertised windows (either field may be absent). */
export interface ModelCapabilityCacheEntry {
  id: string;
  contextWindow?: number;
  maxOutputTokens?: number;
}

/** On-disk cache payload: the sorted model list plus the write timestamp (ms). */
export interface ModelCapabilityCacheFile {
  models: ModelCapabilityCacheEntry[];
  timestamp: number;
}

/** Basename of the cache file inside the resolved cache directory. */
export const MODEL_CAPABILITIES_CACHE_FILE = 'model-capabilities.json';

/** Memoized reads, keyed by absolute cache-file path, so repeated warm calls in
 *  one process touch the disk once. Cleared by {@link clearDynamicCapabilities}. */
const cacheReadMemo = new Map<string, ModelCapabilityCacheFile | null>();

/**
 * The directory the capabilities cache lives in, or `undefined` when caching is
 * not enabled. Resolved from `SUPERINFERENCE_MODEL_CACHE_DIR` at call time so a
 * test/process can toggle it. A blank value disables the cache (→ undefined).
 */
export function modelCapabilitiesCacheDir(env: NodeJS.ProcessEnv = process.env): string | undefined {
  const raw = env.SUPERINFERENCE_MODEL_CACHE_DIR;
  if (typeof raw !== 'string' || raw.trim() === '') return undefined;
  return raw.trim();
}

/** Absolute path to the cache file inside `dir`. */
export function modelCapabilitiesCachePath(dir: string): string {
  return path.join(dir, MODEL_CAPABILITIES_CACHE_FILE);
}

/** Parse/validate a raw JSON value into a cache payload, or null when malformed. */
function parseCacheFile(raw: unknown): ModelCapabilityCacheFile | null {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null;
  const obj = raw as Record<string, unknown>;
  if (typeof obj.timestamp !== 'number' || !Array.isArray(obj.models)) return null;
  const models: ModelCapabilityCacheEntry[] = [];
  for (const item of obj.models) {
    if (!item || typeof item !== 'object' || Array.isArray(item)) continue;
    const rec = item as Record<string, unknown>;
    if (typeof rec.id !== 'string' || !rec.id) continue;
    const entry: ModelCapabilityCacheEntry = { id: rec.id };
    const cw = positiveTokenCount(rec.contextWindow);
    if (cw !== undefined) entry.contextWindow = cw;
    const mo = positiveTokenCount(rec.maxOutputTokens);
    if (mo !== undefined) entry.maxOutputTokens = mo;
    models.push(entry);
  }
  return { models, timestamp: obj.timestamp };
}

/**
 * Read the persisted capability cache from `dir`, or null when caching is off,
 * the file is absent, or its contents are unreadable/malformed. Memoized per
 * path for the process lifetime (a fresh read happens after a write or a
 * {@link clearDynamicCapabilities}).
 */
export function readModelCapabilitiesCache(dir: string | undefined): ModelCapabilityCacheFile | null {
  if (!dir) return null;
  const file = modelCapabilitiesCachePath(dir);
  if (cacheReadMemo.has(file)) return cacheReadMemo.get(file) ?? null;
  let parsed: ModelCapabilityCacheFile | null = null;
  try {
    parsed = parseCacheFile(JSON.parse(fs.readFileSync(file, 'utf8')));
  } catch {
    parsed = null;
  }
  cacheReadMemo.set(file, parsed);
  return parsed;
}

/** Stable-sort entries by id so the written file is deterministic (and the
 *  skip-when-unchanged compare below is order-independent). */
function sortCacheEntries(models: ModelCapabilityCacheEntry[]): ModelCapabilityCacheEntry[] {
  return models.slice().sort((a, b) => a.id.localeCompare(b.id));
}

/** Compare two sorted model lists for equality, ignoring the timestamp. */
function sameCacheModels(a: ModelCapabilityCacheEntry[], b: ModelCapabilityCacheEntry[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i].id !== b[i].id || a[i].contextWindow !== b[i].contextWindow || a[i].maxOutputTokens !== b[i].maxOutputTokens) {
      return false;
    }
  }
  return true;
}

/**
 * Persist the given model windows/caps to the cache file in `dir`. No-op (returns
 * false) when caching is off. The list is sorted by id and the write is SKIPPED
 * when the model set is byte-identical to what is already on disk (only the
 * timestamp would change) — so an unchanged model list never churns the mtime.
 * Otherwise it is written atomically (temp file + rename) with owner-only perms
 * (dir 0700, file 0600). Returns true when a new file was written, false when the
 * write was skipped or failed. Invalidates the read memo on a successful write.
 */
export function writeModelCapabilitiesCache(
  dir: string | undefined,
  models: ModelCapabilityCacheEntry[],
  now: number = Date.now(),
): boolean {
  if (!dir) return false;
  const sorted = sortCacheEntries(models);
  const existing = readModelCapabilitiesCache(dir);
  if (existing && sameCacheModels(existing.models, sorted)) return false;
  const file = modelCapabilitiesCachePath(dir);
  const tmp = `${file}.${process.pid}.tmp`;
  const payload: ModelCapabilityCacheFile = { models: sorted, timestamp: now };
  try {
    fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
    fs.writeFileSync(tmp, JSON.stringify(payload), { mode: 0o600 });
    fs.renameSync(tmp, file);
    cacheReadMemo.set(file, payload);
    return true;
  } catch {
    try { fs.rmSync(tmp, { force: true }); } catch { /* best-effort cleanup */ }
    return false;
  }
}

/**
 * Seed the in-memory context-window / output-cap tables from the persisted cache
 * so a cold start (or an offline run whose /models fetch fails) is sized from the
 * last-known advertised windows instead of the static defaults. Returns the number
 * of models whose windows were warmed; 0 when caching is off or the cache is empty.
 * Only fills runtime tables (never mutates the static family table beyond the same
 * keys discoverFromModelList would add), so a subsequent live discovery still wins.
 */
export function warmModelCapabilitiesFromCache(dir: string | undefined): number {
  const cache = readModelCapabilitiesCache(dir);
  if (!cache) return 0;
  let warmed = 0;
  for (const entry of cache.models) {
    let touched = false;
    if (entry.contextWindow !== undefined) {
      CONTEXT_WINDOWS[entry.id] = entry.contextWindow;
      touched = true;
    }
    if (entry.maxOutputTokens !== undefined) {
      dynamicOutputCaps.set(entry.id.toLowerCase(), entry.maxOutputTokens);
      touched = true;
    }
    if (touched) warmed++;
  }
  return warmed;
}

/**
 * Convert a {@link discoverFromModelList} result into cache entries (merging the
 * context-window and output-cap maps by id) ready for {@link writeModelCapabilitiesCache}.
 */
export function discoveryToCacheEntries(discovery: {
  contextWindows: Record<string, number>;
  outputCaps: Record<string, number>;
}): ModelCapabilityCacheEntry[] {
  const byId = new Map<string, ModelCapabilityCacheEntry>();
  for (const [id, contextWindow] of Object.entries(discovery.contextWindows)) {
    byId.set(id, { id, contextWindow });
  }
  for (const [id, maxOutputTokens] of Object.entries(discovery.outputCaps)) {
    const entry = byId.get(id);
    if (entry) entry.maxOutputTokens = maxOutputTokens;
    else byId.set(id, { id, maxOutputTokens });
  }
  return [...byId.values()];
}

// ---------------------------------------------------------------------------
// User env-var capability override (escape hatch for pinned 3rd-party models)
// ---------------------------------------------------------------------------

/**
 * Capability dimensions a user can explicitly declare for a pinned third-party
 * model via a `*_SUPPORTED_CAPABILITIES` env var. Mirrors Claude's
 * `ModelCapabilityOverride` (utils/model/modelSupportOverrides.ts:L4-L9). AMI's
 * {@link ModelCapabilities} currently maps `effort`/`thinking` → `reasoning` and
 * `adaptive_thinking` → `supportsAdaptiveThinking`; `max_effort` /
 * `interleaved_thinking` are accepted by the generic predicate for forward parity.
 */
export type ModelCapabilityOverride =
  | 'effort'
  | 'max_effort'
  | 'thinking'
  | 'adaptive_thinking'
  | 'interleaved_thinking';

/** Custom base-URL env vars that signal a gateway/proxy (non-first-party) endpoint. */
const THIRD_PARTY_BASE_URL_ENV = [
  'ANTHROPIC_BASE_URL',
  'OPENAI_BASE_URL',
  'AI_BASE_URL',
  'PROVIDER_BASE_URL',
  'OLLAMA_BASE_URL',
] as const;

/**
 * True when no custom gateway/proxy endpoint is configured, i.e. requests go to a
 * provider's canonical first-party API where built-in capability detection is
 * reliable. The capability-override escape hatch below is INTENTIONALLY ignored on
 * a first-party endpoint: a user-declared list can only correct a misdetected
 * gateway model, never silently strip a known-correct first-party capability.
 * Mirrors Claude's `getAPIProvider() === 'firstParty'` short-circuit
 * (utils/model/modelSupportOverrides.ts:L32-L34); AMI detects it from the absence
 * of any custom `*_BASE_URL` env at call time.
 */
export function isFirstPartyProvider(env: NodeJS.ProcessEnv = process.env): boolean {
  return !THIRD_PARTY_BASE_URL_ENV.some(key => {
    const value = env[key];
    return typeof value === 'string' && value.trim() !== '';
  });
}

/** Per-tier env vars: a model pin plus its declared capability list. */
const CAPABILITY_OVERRIDE_TIERS = [
  { modelEnvVar: 'ANTHROPIC_DEFAULT_OPUS_MODEL', capabilitiesEnvVar: 'ANTHROPIC_DEFAULT_OPUS_MODEL_SUPPORTED_CAPABILITIES' },
  { modelEnvVar: 'ANTHROPIC_DEFAULT_SONNET_MODEL', capabilitiesEnvVar: 'ANTHROPIC_DEFAULT_SONNET_MODEL_SUPPORTED_CAPABILITIES' },
  { modelEnvVar: 'ANTHROPIC_DEFAULT_HAIKU_MODEL', capabilitiesEnvVar: 'ANTHROPIC_DEFAULT_HAIKU_MODEL_SUPPORTED_CAPABILITIES' },
] as const;

/**
 * Whether a capability override is declared for `modelId` via the pinned-tier env
 * vars, as an explicit boolean, or `undefined` when no override applies (so the
 * caller falls through to auto-detection). This is the escape hatch for
 * gateway/proxy setups where a custom model id matches no built-in pattern table
 * and auto-detection guesses wrong: pin the id to a tier
 * (`ANTHROPIC_DEFAULT_{OPUS,SONNET,HAIKU}_MODEL`) and declare its capabilities in
 * the parallel `*_SUPPORTED_CAPABILITIES` list (comma-separated, case-insensitive).
 *
 * Precisely mirrors Claude's `get3PModelCapabilityOverride`
 * (utils/model/modelSupportOverrides.ts:L30-L50):
 *  - ignored entirely on a first-party endpoint ({@link isFirstPartyProvider});
 *  - a tier with no pin OR an unset capability list is skipped (→ `undefined`);
 *  - an EMPTY capability list (`''`) is "supports nothing" → `false` (distinct from unset);
 *  - tiers are tried opus→sonnet→haiku; the model id is matched case-insensitively.
 * Read at call time (no memoization) so tests/processes can toggle the env.
 */
export function get3PModelCapabilityOverride(
  modelId: string,
  capability: ModelCapabilityOverride,
  env: NodeJS.ProcessEnv = process.env,
): boolean | undefined {
  if (typeof modelId !== 'string' || !modelId) return undefined;
  if (isFirstPartyProvider(env)) return undefined;
  const lower = modelId.toLowerCase();
  for (const tier of CAPABILITY_OVERRIDE_TIERS) {
    const pinned = env[tier.modelEnvVar];
    const capabilities = env[tier.capabilitiesEnvVar];
    if (!pinned || capabilities === undefined) continue;
    if (lower !== pinned.toLowerCase()) continue;
    return capabilities
      .toLowerCase()
      .split(',')
      .map(s => s.trim())
      .includes(capability);
  }
  return undefined;
}

/**
 * Apply any user-declared capability override (see {@link get3PModelCapabilityOverride})
 * on top of auto-detected capabilities. When the active model is pinned to a tier,
 * the declared list is authoritative: `effort`/`thinking`/`adaptive_thinking` tokens
 * turn the matching capability on, their absence turns it off — correcting a
 * misdetected gateway model in either direction. Returns `base` unchanged when no
 * override applies to this model.
 */
function applyCapabilityOverrides(
  modelId: string,
  base: ModelCapabilities | null,
): ModelCapabilities | null {
  const effort = get3PModelCapabilityOverride(modelId, 'effort');
  // No pinned-tier match for this model → fall through to auto-detection untouched.
  // (A matched tier always resolves every token to a concrete boolean, so an
  // `undefined` here means "no override applies", never "token absent from list".)
  if (effort === undefined) return base;
  const thinking = get3PModelCapabilityOverride(modelId, 'thinking') === true;
  const adaptive = get3PModelCapabilityOverride(modelId, 'adaptive_thinking') === true;
  if (!effort && !thinking && !adaptive) return null; // declared to support nothing
  const result: ModelCapabilities = base
    ? { ...base }
    : {
        reasoning: true,
        defaultThinkingLevel: 'medium',
        supportsAdaptiveThinking: false,
        requiresTemperatureOne: false,
        temperatureMustBeUnset: false,
        maxThinkingBudget: 128000,
      };
  result.reasoning = true;
  result.supportsAdaptiveThinking = adaptive;
  return result;
}

/** Auto-detected capabilities (dynamic registry → version-aware Claude → pattern table). */
function detectModelCapabilities(modelId: string): ModelCapabilities | null {
  const dynamic = dynamicCapabilities.get(modelId.toLowerCase());
  if (dynamic !== undefined) return dynamic;
  // Claude thinking classification is version-aware (adaptive is 4.6+/5.x; pre-4.6 uses
  // legacy budget_tokens — ui#24), which a flat regex can't do without mis-reading dated
  // snapshots. Handle it before the generic pattern table.
  const claudeCaps = claudeReasoningCapabilities(modelId);
  if (claudeCaps) return claudeCaps;
  for (const entry of REASONING_MODELS) {
    if (entry.pattern.test(modelId)) {
      return entry.capabilities;
    }
  }
  return null;
}

export function getModelCapabilities(modelId: string): ModelCapabilities | null {
  if (typeof modelId !== 'string') return null;
  // The user env-var escape hatch (pinned 3rd-party/gateway models) takes
  // precedence over auto-detection so a misdetected custom id can be corrected.
  return applyCapabilityOverrides(modelId, detectModelCapabilities(modelId));
}

export function isReasoningModel(modelId: string): boolean {
  return getModelCapabilities(modelId) !== null;
}

/**
 * Model families that accept image input. A live /models payload can override
 * this via registerModelCapabilities({ supportsVision }). The id may carry a
 * provider prefix (e.g. 'openai/gpt-4o' or 'openrouter/anthropic/claude-...'),
 * so we test every '/'-delimited suffix against the prefix table.
 */
const VISION_MODEL_PREFIXES: string[] = [
  // OpenAI
  'gpt-4o', 'gpt-4.1', 'gpt-5', 'gpt-6', 'o1', 'o3', 'o4', 'chatgpt-4o',
  // Anthropic (all Claude 3+ are vision-capable)
  'claude-3', 'claude-sonnet-', 'claude-opus-', 'claude-haiku-', 'claude-fable-',
  // Google
  'gemini-1.5', 'gemini-2', 'gemini-3', 'gemini-pro-vision',
  // xAI
  'grok-2-vision', 'grok-3', 'grok-4',
  // Open-weight vision
  'llama-3.2', 'llama-4', 'qwen2-vl', 'qwen2.5-vl', 'qwen3-vl', 'pixtral', 'llava', 'internvl',
];

/**
 * Whether the given model can accept image input. Honors an explicitly
 * registered `supportsVision` capability first, then falls back to the
 * known-vision-family prefix table. Unknown models default to false so images
 * are never sent blindly to a text-only model.
 */
export function modelSupportsVision(modelId: string): boolean {
  if (typeof modelId !== 'string' || !modelId) return false;
  const explicit = getModelCapabilities(modelId)?.supportsVision;
  if (explicit !== undefined) return explicit;
  const id = modelId.toLowerCase();
  // Text-only variants whose prefix would otherwise match the vision table.
  if (/(o1-mini|o3-mini|o4-mini)/.test(id) || /llama-3\.2-(1b|3b)/.test(id)) return false;
  const candidates = [id, ...id.split('/')];
  return candidates.some(c => VISION_MODEL_PREFIXES.some(p => c.startsWith(p)));
}

/**
 * Model families that accept native `document` (PDF) content blocks. Narrower than
 * {@link VISION_MODEL_PREFIXES}: native PDF ingestion is currently an Anthropic-Claude
 * and Google-Gemini capability, so a vision-only model (e.g. an open-weight VLM) is NOT
 * assumed to accept documents. A live /models payload can override this per-model via
 * `registerModelCapabilities({ supportsDocuments })`.
 */
const DOCUMENT_MODEL_PREFIXES: string[] = [
  // Anthropic — Claude 3+ accept PDF document blocks.
  'claude-3', 'claude-sonnet-', 'claude-opus-', 'claude-haiku-', 'claude-fable-',
  // Google — Gemini 1.5+ accept PDF document input.
  'gemini-1.5', 'gemini-2', 'gemini-3',
];

/**
 * Whether the given model can accept native `document` (e.g. PDF) input. Honors an
 * explicitly registered `supportsDocuments` capability first, then falls back to the
 * known document-capable family prefix table. Unknown models default to false so a
 * document is never sent blindly to a model that would reject it. Parity: Claude's
 * PDF `document` content block (supported on Claude models). */
export function modelSupportsDocuments(modelId: string): boolean {
  if (typeof modelId !== 'string' || !modelId) return false;
  const explicit = getModelCapabilities(modelId)?.supportsDocuments;
  if (explicit !== undefined) return explicit;
  const id = modelId.toLowerCase();
  const candidates = [id, ...id.split('/')];
  return candidates.some(c => DOCUMENT_MODEL_PREFIXES.some(p => c.startsWith(p)));
}

const BUDGET_MAP: Record<ThinkingLevel, number> = {
  off: 0,
  low: 4096,
  medium: 10240,
  high: 32768,
  max: 0,
};

/** Completion budget used when the caller omits maxTokens. */
export const DEFAULT_MAX_OUTPUT_TOKENS = 32_768;

/**
 * Global fallback output-token ceiling for a recognized-but-untabled Claude
 * model (Claude's MAX_OUTPUT_TOKENS_UPPER_LIMIT, utils/context.ts). Used only as
 * the `upperLimit` fallback in {@link getModelMaxOutputTokens}; it is NOT a cap
 * for unknown/non-Claude models (those still return undefined from
 * {@link getMaxOutputTokens} so no clamp is applied).
 */
export const MAX_OUTPUT_TOKENS_UPPER_LIMIT = 64_000;

/** A model's output-token budget split: the conservative default sent by
 *  default, and the hard ceiling a caller may escalate to. Mirrors Claude's
 *  `getModelMaxOutputTokens(): { default, upperLimit }` (utils/context.ts) and
 *  the AntModel `defaultMaxTokens` / `upperMaxTokensLimit` knobs. */
export interface MaxOutputTokenRange {
  /** Completion budget sent when the caller omits an explicit maxTokens. */
  default: number;
  /** Hard ceiling the budget may be raised to (and the API-rejection cap). */
  upperLimit: number;
}

/**
 * Per-family max-output-token budgets, each split into a conservative `default`
 * and a hard `upperLimit`. Several models pair a large context window with a much
 * smaller completion cap (e.g. claude-3-5-sonnet has a 200k window but only an
 * 8,192-token output cap), so a request that fits the window can still exceed the
 * model's output limit and be rejected by the API. Modern Claude families
 * additionally ship a default BELOW their ceiling (e.g. opus-4-6 sends 64k by
 * default but may be raised to 128k) — mirroring Claude's getModelMaxOutputTokens
 * ladder (utils/context.ts:L146-L210). Longer / more-specific ids come first so
 * `claude-3-5-sonnet` is not shadowed by `claude-3`, and `opus-4-6` is not
 * shadowed by `opus-4`. A live `/models` capability or registered
 * maxOutputTokens overrides the ceiling (see getModelMaxOutputTokens).
 */
const OUTPUT_TOKEN_CAPS: Record<string, MaxOutputTokenRange> = {
  // Anthropic — flagship families (5.x and Opus 4.7/4.8): same 128k ceiling as
  // Opus/Sonnet 4.6. These precede the bare `claude-opus-4` (4.0) entry so a
  // newer minor version is NOT captured by the 4.0 substring and under-reported
  // at a 32k cap — which would also under-budget its derived thinking ceiling.
  // `claude-opus-5` / `claude-sonnet-5` cover the whole 5.x line by substring.
  'claude-opus-5':    { default: 64_000, upperLimit: 128_000 },
  'claude-sonnet-5':  { default: 32_000, upperLimit: 128_000 },
  'claude-opus-4-8':  { default: 64_000, upperLimit: 128_000 },
  'claude-opus-4-7':  { default: 64_000, upperLimit: 128_000 },
  // Anthropic — modern families: default below the ceiling (distinct knobs)
  'claude-opus-4-6':  { default: 64_000, upperLimit: 128_000 },
  'claude-sonnet-4-6': { default: 32_000, upperLimit: 128_000 },
  'claude-opus-4-5':  { default: 32_000, upperLimit: 64_000 },
  'claude-sonnet-4':  { default: 32_000, upperLimit: 64_000 },
  'claude-haiku-4':   { default: 32_000, upperLimit: 64_000 },
  'claude-opus-4-1':  { default: 32_000, upperLimit: 32_000 },
  'claude-opus-4':    { default: 32_000, upperLimit: 32_000 },
  'claude-3-7-sonnet': { default: 32_000, upperLimit: 64_000 },
  // Anthropic — older small-output families (default == ceiling)
  'claude-3-5-sonnet': { default: 8192, upperLimit: 8192 },
  'claude-3.5-sonnet': { default: 8192, upperLimit: 8192 },
  'claude-3-5-haiku':  { default: 8192, upperLimit: 8192 },
  'claude-3.5-haiku':  { default: 8192, upperLimit: 8192 },
  'claude-3-opus':     { default: 4096, upperLimit: 4096 },
  'claude-3-sonnet':   { default: 8192, upperLimit: 8192 },
  'claude-3-haiku':    { default: 4096, upperLimit: 4096 },
  // OpenAI (single-knob families: default == ceiling)
  'gpt-4-turbo': { default: 4096, upperLimit: 4096 },
  'gpt-4o-mini': { default: 16384, upperLimit: 16384 },
  'gpt-4o':      { default: 16384, upperLimit: 16384 },
  'o1-mini':     { default: 65536, upperLimit: 65536 },
};

/**
 * Output-cap entries pre-sorted most-specific (longest key) first, with a stable
 * localeCompare tiebreak, so a substring lookup structurally prefers the most
 * specific id (e.g. `claude-opus-4-8` before `claude-opus-4`) regardless of the
 * table's hand-maintained insertion order. Mirrors the same specificity guarantee
 * getContextWindow already makes for CONTEXT_WINDOWS (ref sortForMatching,
 * utils/model/modelCapabilities.ts:L53-L58) so a future edit that reorders the
 * table can't silently under-report a newer minor version's output cap.
 */
const OUTPUT_TOKEN_CAP_ENTRIES: Array<[string, MaxOutputTokenRange]> = Object.entries(OUTPUT_TOKEN_CAPS)
  .sort((a, b) => b[0].length - a[0].length || a[0].localeCompare(b[0]));

/** Most-specific (longest-key) static-table range whose key is a substring of the (lowercased) id. */
function lookupOutputTokenRange(lowerId: string): MaxOutputTokenRange | undefined {
  for (const [key, range] of OUTPUT_TOKEN_CAP_ENTRIES) {
    if (lowerId.includes(key)) return range;
  }
  return undefined;
}

/** A capability-advertised completion cap (registered capability, then a cap
 *  learned from a live /models payload), or undefined when none is known. */
function capabilityOutputCap(lowerId: string, modelId: string): number | undefined {
  const registered = getModelCapabilities(modelId)?.maxOutputTokens;
  if (registered !== undefined && registered > 0) return registered;
  return dynamicOutputCaps.get(lowerId);
}

/**
 * The model's output-token budget as a {@link MaxOutputTokenRange} — the
 * conservative `default` sent by default and the hard `upperLimit` a caller may
 * escalate to. Mirrors Claude's getModelMaxOutputTokens (utils/context.ts):
 * start from the per-family ladder (or the global default/upper for an unlisted
 * model), then let a capability-advertised cap (>= 4096) raise the ceiling and
 * clamp the default not to exceed it. Always returns a defined pair.
 */
export function getModelMaxOutputTokens(modelId: string): MaxOutputTokenRange {
  const lower = typeof modelId === 'string' ? modelId.toLowerCase() : '';
  const base = lookupOutputTokenRange(lower)
    ?? { default: DEFAULT_MAX_OUTPUT_TOKENS, upperLimit: MAX_OUTPUT_TOKENS_UPPER_LIMIT };
  let { default: defaultTokens, upperLimit } = base;
  const cap = capabilityOutputCap(lower, modelId);
  if (cap !== undefined && cap >= 4_096) {
    upperLimit = cap;
    defaultTokens = Math.min(defaultTokens, upperLimit);
  }
  return { default: defaultTokens, upperLimit };
}

/**
 * The model's own max-output-token ceiling, or undefined when unknown (callers
 * then clamp only against the context window). Honors a dynamically-registered
 * capability first, then a cap learned from a live /models payload, then the
 * static family ladder's `upperLimit`. Unknown/non-Claude models return
 * undefined so no clamp is applied.
 */
export function getMaxOutputTokens(modelId: string): number | undefined {
  if (typeof modelId !== 'string' || !modelId) return undefined;
  const registered = getModelCapabilities(modelId)?.maxOutputTokens;
  if (registered !== undefined && registered > 0) return registered;
  const lower = modelId.toLowerCase();
  // A cap learned from the live /models payload overrides the static family table.
  const discovered = dynamicOutputCaps.get(lower);
  if (discovered !== undefined) return discovered;
  return lookupOutputTokenRange(lower)?.upperLimit;
}

/**
 * Tokens held back so the prompt and the completion do not land on the
 * same last token of the window.
 */
export const OUTPUT_FIT_SAFETY_TOKENS = 1_024;

/** Smallest completion we will still request when the window is nearly full. */
export const MIN_FITTED_OUTPUT_TOKENS = 256;

/** Slack subtracted before a reduced thinking budget is accepted. */
export const THINKING_HEADROOM_MARGIN = 4_096;

/** Smallest thinking budget worth sending. Below this, thinking is turned off. */
export const MIN_THINKING_BUDGET = 1_024;

/**
 * Largest completion budget that still fits beside the prompt.
 *
 * `requested` is the configured budget or an escalation step (64k / 128k /
 * 256k). Those steps are not clamped to the model, so a 32k window would
 * otherwise be asked for 256k output tokens.
 */
export function fitOutputTokens(
  requested: number,
  contextWindow: number,
  promptTokens: number,
  modelId?: string,
): number {
  let ask = Number.isFinite(requested) && requested > 0
    ? Math.floor(requested)
    : DEFAULT_MAX_OUTPUT_TOKENS;
  // Clamp against the model's own output ceiling first: a request that fits the
  // context window can still exceed the model's max-output cap (e.g. 8,192 for
  // claude-3-5-sonnet) and be rejected by the API.
  const outputCap = modelId ? getMaxOutputTokens(modelId) : undefined;
  if (outputCap !== undefined) ask = Math.min(ask, outputCap);
  if (!Number.isFinite(contextWindow) || contextWindow <= 0) return ask;
  const prompt = Number.isFinite(promptTokens) && promptTokens > 0 ? Math.floor(promptTokens) : 0;
  const cap = contextWindow - prompt - OUTPUT_FIT_SAFETY_TOKENS;
  if (cap < MIN_FITTED_OUTPUT_TOKENS) return MIN_FITTED_OUTPUT_TOKENS;
  return Math.min(ask, cap);
}

/**
 * Shrink a thinking budget so prompt + thinking + output stays under the window.
 * Returns the request unchanged when it already fits. Returns 0 when even
 * the 1024 floor would overflow — callers should disable thinking in that case.
 */
export function capThinkingBudget(
  requestedBudget: number,
  contextWindow: number,
  promptTokens: number,
  outputTokens: number,
): number {
  if (!Number.isFinite(requestedBudget) || requestedBudget <= 0) return 0;
  if (!Number.isFinite(contextWindow) || contextWindow <= 0) return Math.floor(requestedBudget);
  const prompt = Number.isFinite(promptTokens) ? Math.max(0, Math.floor(promptTokens)) : 0;
  const output = Number.isFinite(outputTokens) ? Math.max(0, Math.floor(outputTokens)) : 0;
  const headroom = contextWindow - prompt - output;
  // Keep the same safety margin fitOutputTokens reserved. Thinking must
  // not spend it, or prompt + output + thinking lands on the window edge.
  const usable = headroom - OUTPUT_FIT_SAFETY_TOKENS;
  if (usable >= requestedBudget) return Math.floor(requestedBudget);
  const withMargin = Math.floor(usable) - THINKING_HEADROOM_MARGIN;
  if (withMargin >= MIN_THINKING_BUDGET) return withMargin;
  if (usable >= MIN_THINKING_BUDGET) return MIN_THINKING_BUDGET;
  return 0;
}

/**
 * Character cap for one tool result.
 *
 * Message estimates use chars/4 and then a 4/3 pad, which is chars/3.
 * 85% of the token budget is the detached compact trigger, so a single
 * result that stays under this cap cannot fill the window by itself.
 */
export function maxToolOutputChars(tokenBudget: number): number {
  if (!Number.isFinite(tokenBudget) || tokenBudget <= 0) return 8_000;
  const tokenRoom = Math.floor(tokenBudget * 0.85);
  return Math.max(8_000, tokenRoom * 3);
}

export function resolveThinkingBudget(level: ThinkingLevel, modelId?: string): number {
  const budget = BUDGET_MAP[level];
  if (budget > 0) return budget;
  if (level === 'max') {
    const caps = getModelCapabilities(modelId ?? '');
    const maxBudget = caps?.maxThinkingBudget ?? 128000;
    return Math.max(1024, maxBudget - 8192);
  }
  return 0;
}

const EFFORT_RANK: Record<ThinkingLevel, number> = {
  off: 0, low: 1, medium: 2, high: 3, max: 4,
};

export function clampEffortLevel(requested: ThinkingLevel, maxLevel: ThinkingLevel | undefined): ThinkingLevel {
  if (!maxLevel || !(maxLevel in EFFORT_RANK)) return requested;
  if (EFFORT_RANK[requested] <= EFFORT_RANK[maxLevel]) return requested;
  return maxLevel;
}

/**
 * The thinking level a model runs at when the user asks for "auto" / the default
 * (no explicit level). Resolved PER MODEL from its detected
 * {@link ModelCapabilities} — a reasoning model yields its
 * `defaultThinkingLevel` (e.g. Gemini-flash defaults to `low`, Claude to
 * `medium`), while a non-reasoning model yields `off` because it cannot think.
 * This is the resolver behind the `/thinking auto` sentinel: it lets the UI show
 * "auto (currently <level>)" with the level that will actually be applied on the
 * active model. Mirrors Claude's per-model default-effort resolution
 * (commands/effort/effort.tsx `auto` state). An empty / unknown id → `off`.
 */
export function getDefaultThinkingLevel(modelId: string): ThinkingLevel {
  return getModelCapabilities(modelId)?.defaultThinkingLevel ?? 'off';
}

/**
 * Step a thinking level one rung in `direction` (+1 up, -1 down) with wraparound,
 * so an interactive cycle (`/thinking up`/`down`, a keybinding) never lands
 * out of range. When `includeMax` is false the top `max` rung is skipped — used
 * where a model does not accept the `max` effort tier so cycling clamps to `high`
 * instead of offering an unusable level. Mirrors Claude's ModelPicker effort
 * cycling with wraparound + clamp-to-high (components/ModelPicker.tsx). An
 * unrecognized `current` is treated as the first rung (`off`).
 */
export function cycleThinkingLevel(
  current: ThinkingLevel,
  direction: 1 | -1,
  includeMax = true,
): ThinkingLevel {
  const scale = (includeMax ? THINKING_LEVELS : THINKING_LEVELS.filter(l => l !== 'max')) as readonly ThinkingLevel[];
  const idx = scale.indexOf(current);
  // An unknown current (or a 'max' excluded by includeMax=false) starts the walk
  // from the first rung so direction still produces a valid in-range level.
  const base = idx === -1 ? 0 : idx;
  const next = (base + direction + scale.length) % scale.length;
  return scale[next];
}

/**
 * Whether the model accepts the top `max` reasoning-effort tier.
 *
 * Per Anthropic's API docs the `max` effort value is accepted ONLY by the
 * Opus 4.6+ class (public models); every other model — including Sonnet 4.6 —
 * returns an error when sent `effort: 'max'`. This is a PER-MODEL capability,
 * not a global config knob: the `max` tier can be offered/applied on one model
 * and must be downgraded on another.
 *
 * Resolution order mirrors Claude's `modelSupportsMaxEffort`
 * (utils/effort.ts:L50-65):
 *  1. An explicit 3rd-party `max_effort` capability override (pinned-tier env,
 *     gateway/proxy endpoints only) wins — see {@link get3PModelCapabilityOverride}.
 *  2. Otherwise the Opus family at version 4.6+ (and the 5.x line) supports it,
 *     matching the `supportsAdaptiveThinking` version gate's shape; all other
 *     families/versions do not.
 * An empty/non-string id supports nothing.
 */
export function modelSupportsMaxEffort(modelId: string): boolean {
  if (typeof modelId !== 'string' || !modelId) return false;
  const override = get3PModelCapabilityOverride(modelId, 'max_effort');
  if (override !== undefined) return override;
  const v = parseClaudeVersion(modelId);
  if (!v || v.family !== 'opus') return false;
  return v.major >= 5 || (v.major === 4 && v.minor >= 6);
}

/**
 * Whether the current MAIN-LOOP model is capable of driving a secondary
 * advisor/architect model. Mirrors Claude Code's `modelSupportsAdvisor`
 * (utils/advisor.ts:L88-L96), which gates the advisor tool on the Opus/Sonnet
 * 4.6+ generations (`opus-4-6`/`sonnet-4-6` and newer). AMI applies the same
 * version-aware family match — Opus or Sonnet with major≥5, or major 4 minor≥6 —
 * so a bare `claude-sonnet-4` (4.0) or a 3.x model is NOT advisor-capable and the
 * `/architect` secondary model is reported as INACTIVE rather than silently doing
 * nothing. The id may carry a gateway prefix, so the match is unanchored; Haiku,
 * non-Claude ids, and empty/non-string inputs are never advisor-capable.
 */
export function modelSupportsAdvisor(modelId: string): boolean {
  if (typeof modelId !== 'string' || !modelId) return false;
  const v = parseClaudeVersion(modelId);
  if (!v || (v.family !== 'opus' && v.family !== 'sonnet')) return false;
  return v.major >= 5 || (v.major === 4 && v.minor >= 6);
}

/**
 * Whether the model defaults to adaptive thinking and REJECTS a request to turn
 * reasoning off (`thinking: { type:'disabled' }`). Resolves from the model's
 * detected {@link ModelCapabilities} `alwaysOnThinking` flag (the 5.x Claude line);
 * an honored `thinking`/`effort` 3rd-party override that enables reasoning on a
 * pinned gateway model does NOT by itself make it always-on. Used by the provider
 * request builder to still send a thinking config on the `off` level for such a
 * model, and by UIs to avoid offering an `off` choice that the endpoint ignores.
 * An empty/unknown id is not always-on.
 */
export function modelAlwaysOnThinking(modelId: string): boolean {
  if (typeof modelId !== 'string' || !modelId) return false;
  return getModelCapabilities(modelId)?.alwaysOnThinking === true;
}

/**
 * Downgrade a requested reasoning-effort/thinking level to what the model will
 * actually accept. The top `max` tier is clamped to `high` when the model is not
 * max-effort-capable ({@link modelSupportsMaxEffort}), so the provider never
 * sends `effort: 'max'` to a model that would reject it with an API error — and
 * the displayed level stays consistent with what is sent. Every other level
 * passes through unchanged. Mirrors Claude's `resolveAppliedEffort` max→high
 * downgrade (utils/effort.ts:L162-165).
 */
export function resolveMaxEffortLevel(level: ThinkingLevel, modelId: string): ThinkingLevel {
  if (level === 'max' && !modelSupportsMaxEffort(modelId)) return 'high';
  return level;
}

const PROVIDER_SAMPLING_DEFAULTS: Record<string, { temperature?: number; topP?: number; topK?: number }> = {
  'gemini': { temperature: 1.0, topK: 64 },
  'qwen': { temperature: 0.55, topP: 1 },
  'deepseek': { temperature: 0.6 },
};

export function getProviderSamplingDefaults(modelId: string): { temperature?: number; topP?: number; topK?: number } {
  for (const [prefix, defaults] of Object.entries(PROVIDER_SAMPLING_DEFAULTS)) {
    if (modelId.toLowerCase().includes(prefix)) return defaults;
  }
  return {};
}

export function resolveTemperature(
  modelId: string,
  configTemperature: number | undefined,
  thinking: { enabled: boolean } | undefined,
): number | undefined {
  if (!thinking?.enabled) {
    // If no explicit temperature configured, use provider-specific defaults
    if (configTemperature === undefined) {
      const defaults = getProviderSamplingDefaults(modelId);
      return defaults.temperature;
    }
    return configTemperature;
  }

  const caps = getModelCapabilities(modelId);
  if (!caps) {
    if (configTemperature === undefined) {
      const defaults = getProviderSamplingDefaults(modelId);
      return defaults.temperature;
    }
    return configTemperature;
  }

  // Claude: temperature must be omitted (SDK sets it to 1 internally)
  if (caps.requiresTemperatureOne) return undefined;

  // OpenAI o-series: temperature must not be sent at all
  if (caps.temperatureMustBeUnset) return undefined;

  return configTemperature;
}

// ---------------------------------------------------------------------------
// Model switch safety
// ---------------------------------------------------------------------------

/**
 * Rough per-token pricing (USD) for compaction cost estimation.
 * Values are order-of-magnitude estimates; unknown models get zero.
 */
const TOKEN_PRICING: Record<string, { input: number; output: number }> = {
  'claude-opus':   { input: 15e-6,  output: 75e-6 },
  'claude-sonnet': { input: 3e-6,   output: 15e-6 },
  'claude-haiku':  { input: 0.25e-6, output: 1.25e-6 },
  'claude-fable':  { input: 3e-6,   output: 15e-6 },
  'gpt-5.6':       { input: 2e-6,   output: 8e-6 },
  'gpt-4o':        { input: 2.5e-6, output: 10e-6 },
  'gpt-4o-mini':   { input: 0.15e-6, output: 0.6e-6 },
  'gpt-4.1':       { input: 2e-6,   output: 8e-6 },
  'o3':            { input: 10e-6,  output: 40e-6 },
  'o4-mini':       { input: 1.1e-6, output: 4.4e-6 },
  'gemini-2.5':    { input: 1.25e-6, output: 10e-6 },
  'deepseek':      { input: 0.27e-6, output: 1.1e-6 },
};

function lookupPricing(modelId: string): { input: number; output: number } | null {
  const lower = modelId.toLowerCase();
  for (const [prefix, pricing] of Object.entries(TOKEN_PRICING)) {
    if (lower.includes(prefix)) return pricing;
  }
  return null;
}

/**
 * Whether a model switch requires compaction to avoid exceeding the target
 * model's context window.
 *
 * Returns true when the current token usage fills more than 90% of the
 * target model's context window, meaning a switch without compaction risks
 * an immediate context_length_exceeded error.
 */
export function modelSwitchNeedsCompaction(
  currentModel: string,
  targetModel: string,
  currentTokens: number,
): boolean {
  const targetWindow = getContextWindow(targetModel);
  return currentTokens > targetWindow * 0.9;
}

/**
 * Human-readable warning about a model switch when the target model has a
 * smaller context window.  Returns `null` if the switch is safe.
 */
export function getModelSwitchWarning(
  currentModel: string,
  targetModel: string,
  currentTokens: number,
): string | null {
  const currentWindow = getContextWindow(currentModel);
  const targetWindow = getContextWindow(targetModel);

  if (targetWindow >= currentWindow) return null;

  if (currentTokens > targetWindow) {
    return `Critical: conversation uses ${formatTokenCount(currentTokens)} tokens but ${targetModel} only supports ${formatTokenCount(targetWindow)}. Compaction required before switching — run /compact first.`;
  }

  if (currentTokens > targetWindow * 0.8) {
    return `Warning: conversation uses ${formatTokenCount(currentTokens)} tokens and ${targetModel} supports ${formatTokenCount(targetWindow)}. Consider compacting before switching — run /compact first.`;
  }

  return `Note: ${targetModel} has a smaller context window (${formatTokenCount(targetWindow)}) than ${currentModel} (${formatTokenCount(currentWindow)}).`;
}

/** The `[1m]` long-context suffix: a model already carrying it has no larger
 *  variant left to upgrade to. */
const ONE_MILLION_SUFFIX = /\[1m\]$/i;
/** The context window a `[1m]` upgrade grants; the baseline for the relative
 *  multiplier and the "already maxed out" short-circuit. */
const ONE_MILLION_CONTEXT = 1_000_000;

/**
 * A larger-context model variant reachable from the current model, mirroring the
 * descriptor Claude Code's `getAvailableUpgrade()` returns
 * (utils/model/contextWindowUpgradeCheck.ts:L9-L27). Unlike Claude's absolute
 * window table, AMI expresses the gain as a relative {@link multiplier} so a
 * message can say "{N}x more context" from any base window.
 */
export interface ContextUpgrade {
  /** The model id/alias to switch to, carrying the `[1m]` marker (e.g. `opus[1m]`). */
  alias: string;
  /** Human-readable name of the larger-context variant (e.g. `Opus 1M`). */
  name: string;
  /** Relative context-size gain of the variant over the current window
   *  (`1M / currentWindow`, rounded, never below 2). Claude hard-codes 5; AMI
   *  derives it from the live window so it stays accurate across base sizes. */
  multiplier: number;
}

/**
 * The larger-context sibling of `currentModel`, or `null` when none is reachable
 * (the model already carries a `[1m]` marker, is already on a ≥1M window, or is a
 * family with no 1M variant). Provider-agnostic — keyed off the `[1m]` convention
 * and the `opus`/`sonnet` families that expose a 1M variant (core's `MODEL_ALIASES`),
 * so it needs no registry import and never special-cases a vendor entitlement.
 *
 * Mirrors Claude Code's forward-upgrade detection (`getAvailableUpgrade`,
 * utils/model/contextWindowUpgradeCheck.ts) — the inverse of
 * {@link getModelSwitchWarning}, which only warns about switching DOWN to a smaller
 * window. `currentContextLimit` (the session's resolved window) lets the caller
 * suppress the suggestion once already on a ≥1M window and makes the multiplier
 * exact; when omitted the multiplier falls back to Claude's canonical 5x.
 */
export function getAvailableContextUpgrade(
  currentModel: string | null | undefined,
  currentContextLimit?: number,
): ContextUpgrade | null {
  const id = (typeof currentModel === 'string' ? currentModel : '').toLowerCase();
  if (!id) return null;
  // Already on the 1M window (by explicit suffix or by a resolved ≥1M limit) → nothing larger.
  if (ONE_MILLION_SUFFIX.test(id)) return null;
  if (typeof currentContextLimit === 'number' && currentContextLimit >= ONE_MILLION_CONTEXT) return null;
  // Only the opus/sonnet families expose a [1m] long-context variant (MODEL_ALIASES).
  const family = id.includes('opus') ? 'opus' : id.includes('sonnet') ? 'sonnet' : null;
  if (!family) return null;
  const alias = `${family}[1m]`;
  const name = `${capitalize(family)} 1M`;
  const multiplier = typeof currentContextLimit === 'number' && currentContextLimit > 0
    ? Math.max(2, Math.round(ONE_MILLION_CONTEXT / currentContextLimit))
    : 5;
  return { alias, name, multiplier };
}

/**
 * Coarse hosting class a retirement date is keyed by. The SAME logical model
 * retires on DIFFERENT dates depending on where it is served (Anthropic first
 * party vs Bedrock vs Vertex vs Foundry), so a single flat date would be wrong
 * for 3P users. Mirrors Claude's APIProvider union (utils/model/providers.ts).
 */
export type ApiProviderClass = 'firstParty' | 'bedrock' | 'vertex' | 'foundry';

/**
 * Map an AMI provider id (as returned by detectProvider) to the coarse hosting
 * class the retirement-date table is keyed by. Anything that is not a recognized
 * Bedrock/Vertex/Foundry route is treated as first party (the default).
 */
export function toApiProviderClass(provider: string | null | undefined): ApiProviderClass {
  switch (provider) {
    case 'amazon-bedrock': return 'bedrock';
    case 'anthropic-vertex':
    case 'google-vertex': return 'vertex';
    case 'azure-openai': return 'foundry';
    default: return 'firstParty';
  }
}

/**
 * Deprecated / retiring models and their retirement dates, PER hosting class.
 * Keys are matched as case-insensitive substrings of the model id (so a dated
 * snapshot or a gateway prefix still matches), newest-retiring handled by
 * first-match. A `null` date means the model is NOT retiring on that host, so no
 * warning fires there. Add an entry here to warn users off a model before the
 * endpoint starts rejecting it. Mirrors Claude's DEPRECATED_MODELS registry with
 * per-provider retirement dates (utils/model/deprecation.ts:L21-L101).
 */
const DEPRECATED_MODELS: Record<string, { modelName: string; retirementDates: Record<ApiProviderClass, string | null> }> = {
  'claude-3-opus': {
    modelName: 'Claude 3 Opus',
    retirementDates: { firstParty: 'January 5, 2026', bedrock: 'January 15, 2026', vertex: 'January 5, 2026', foundry: 'January 5, 2026' },
  },
  'claude-3-7-sonnet': {
    modelName: 'Claude 3.7 Sonnet',
    retirementDates: { firstParty: 'February 19, 2026', bedrock: 'April 28, 2026', vertex: 'May 11, 2026', foundry: 'February 19, 2026' },
  },
  'claude-3-5-haiku': {
    modelName: 'Claude 3.5 Haiku',
    retirementDates: { firstParty: 'February 19, 2026', bedrock: null, vertex: null, foundry: null },
  },
  'claude-3-5-sonnet': {
    modelName: 'Claude 3.5 Sonnet',
    retirementDates: { firstParty: 'October 22, 2025', bedrock: null, vertex: null, foundry: null },
  },
};

/**
 * A warning that the given model is deprecated and will be retired, or null when
 * the model is current (or the id is empty/non-string, or not retiring on the
 * resolved host). Returned verbatim by the `/model` slash command so switching
 * to — or viewing — a retiring model nudges the user toward a newer one. The
 * `provider` argument is an AMI provider id (defaults to first-party hosting);
 * the retirement DATE varies by host. Mirrors Claude's getModelDeprecationWarning.
 */
export function getModelDeprecationWarning(
  modelId: string | null | undefined,
  provider?: string | null,
): string | null {
  if (typeof modelId !== 'string' || !modelId) return null;
  const host = toApiProviderClass(provider);
  const lower = modelId.toLowerCase();
  for (const [key, info] of Object.entries(DEPRECATED_MODELS)) {
    if (!lower.includes(key)) continue;
    const date = info.retirementDates[host];
    if (!date) continue;
    return `${info.modelName} will be retired on ${date}. Consider switching to a newer model.`;
  }
  return null;
}

/**
 * The exact offsets of every inline `ultrathink` keyword in `text`. Each entry
 * is the matched word plus its `[start, end)` character range, so a UI can
 * highlight / underline the trigger it recognized and acknowledge the
 * escalation to the user. A fresh global regex is built per call —
 * `String.prototype.matchAll` copies `lastIndex` from the source pattern, so a
 * shared instance would leak state between calls. Mirrors Claude Code's
 * `findThinkingTriggerPositions` (utils/thinking.ts:L37-L58). Returns `[]` for a
 * non-string or keyword-free input.
 */
export function findThinkingTriggerPositions(
  text: string,
): Array<{ word: string; start: number; end: number }> {
  const positions: Array<{ word: string; start: number; end: number }> = [];
  if (typeof text !== 'string' || !text) return positions;
  for (const match of text.matchAll(/\bultrathink\b/gi)) {
    if (match.index !== undefined) {
      positions.push({ word: match[0], start: match.index, end: match.index + match[0].length });
    }
  }
  return positions;
}

/**
 * Whether `text` contains the inline `ultrathink` magic keyword — a per-message,
 * input-layer escalation knob: typing `ultrathink` anywhere in a turn bumps that
 * turn's reasoning to the deepest level, independent of the sticky `/thinking`
 * level or any env override. Word-boundary + case-insensitive, so `ultrathink`,
 * `Ultrathink` and `ULTRATHINK` all match but `ultrathinking`/`myultrathink` do
 * not. Mirrors Claude Code's `hasUltrathinkKeyword` (utils/thinking.ts:L29-L31).
 */
export function hasUltrathinkKeyword(text: string): boolean {
  if (typeof text !== 'string') return false;
  return /\bultrathink\b/i.test(text);
}

/**
 * Per-model-family fallback suggestion when a selected model is unavailable on a
 * THIRD-PARTY (non-first-party) endpoint — e.g. a brand-new version a gateway has
 * not provisioned yet. Walks a version-downgrade chain to the previous shipped
 * version of the same family so the user gets a concrete "try this instead"
 * target. Returns `undefined` on a first-party endpoint (where the canonical API
 * always serves the current model, so no downgrade is warranted — see
 * {@link isFirstPartyProvider}) and for any id outside the known chain.
 *
 * Mirrors Claude Code's `get3PModelFallbackSuggestion` (services/api/errors.ts:
 * L936-L959): Opus 4.6→4.1, Sonnet 4.6→4.5, Sonnet 4.5→4. This is DISTINCT from
 * {@link getModelDeprecationWarning} / `suggestDeprecatedReplacement` (which map a
 * removed/old id UP to a newer successor); this maps a too-new id DOWN to the
 * last version a 3P host is likely to have. Read at call time so env toggles
 * take effect. Accepts `-`/`_` version separators.
 */
export function get3PModelFallbackSuggestion(
  modelId: string | null | undefined,
  env: NodeJS.ProcessEnv = process.env,
): string | undefined {
  if (typeof modelId !== 'string' || !modelId) return undefined;
  if (isFirstPartyProvider(env)) return undefined;
  const m = modelId.toLowerCase();
  if (m.includes('opus-4-6') || m.includes('opus_4_6')) return 'claude-opus-4-1';
  if (m.includes('sonnet-4-6') || m.includes('sonnet_4_6')) return 'claude-sonnet-4-5';
  if (m.includes('sonnet-4-5') || m.includes('sonnet_4_5')) return 'claude-sonnet-4';
  return undefined;
}

/**
 * Rough cost estimate for running a compaction summary on a model.
 *
 * Assumes compaction reads all `tokens` as input and produces ~10% as output
 * (the summary). Returns `{ inputCost: 0, outputCost: 0 }` for unknown models.
 */
export function estimateCompactionCost(
  modelId: string,
  tokens: number,
): { inputCost: number; outputCost: number } {
  const pricing = lookupPricing(modelId);
  if (!pricing) return { inputCost: 0, outputCost: 0 };
  const outputTokens = Math.ceil(tokens * 0.1);
  return {
    inputCost: tokens * pricing.input,
    outputCost: outputTokens * pricing.output,
  };
}
