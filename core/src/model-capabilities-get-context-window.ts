// Split from model-capabilities.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).

export const THINKING_LEVELS = ['off', 'low', 'medium', 'high', 'max'] as const;

export type ThinkingLevel = (typeof THINKING_LEVELS)[number];

/**
 * Human-readable, per-level descriptions of what each reasoning/thinking tier
 * means, surfaced by `/thinking` (no-arg state, `help`/info args, and invalid
 * input) so a user can pick a level deliberately instead of guessing. Mirrors
 * the reference's `/effort help` per-level copy (commands/effort/effort.tsx:L173-L176)
 * and `getEffortValueDescription` (utils/effort.ts); AMI additionally documents
 * `off` since its scale includes a disabled tier that the reference's does not.
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
   * the reference's AntModel `alwaysOnThinking` knob (utils/model/antModels.ts:L14-15).
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
export const CONTEXT_WINDOWS: Record<string, number> = {
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
export function hasOneMillionMarker(modelId: string): boolean {
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
 * Rules (mirrors the reference's `resolveSkillModelOverride`):
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
 * the Opus 5+ family — the reference ships a dedicated `checkOpus1mAccess()`
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
 * per-process/test. Mirrors the reference's is1mContextDisabled env gate.
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
 * then normalize right before the request. Mirrors the reference's
 * `normalizeModelStringForAPI` (utils/model/model.ts:L616-L618), extended to the
 * `-1m`/`_1m` suffix forms AMI also treats as markers. A non-string id passes
 * through unchanged.
 */
export function normalizeModelStringForAPI(modelId: string): string {
  if (typeof modelId !== 'string') return modelId;
  // The [1m]/[2m]/_-1m suffix is AMI's LOCAL context-window marker, meaningful only
  // for first-party Claude models. A third-party id may legitimately contain "[1m]"
  // as part of its real name (e.g. DeepSeek's `deepseek-v4-pro[1m]`), so those pass
  // through verbatim instead of being mangled into a model the endpoint rejects.
  if (!/^claude[-_.]/i.test(modelId)) return modelId;
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
 * process/test. Mirrors the reference's `CLAUDE_CODE_MAX_CONTEXT_TOKENS` override.
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
 * order; the first non-empty value wins. Mirrors the reference's
 * `getSmallFastModel` (utils/model/model.ts:L36-L38), which keys off
 * `ANTHROPIC_SMALL_FAST_MODEL`; AMI also honors a provider-neutral `AMI_SMALL_FAST_MODEL`.
 */
export const SMALL_FAST_MODEL_ENV_VARS = ['ANTHROPIC_SMALL_FAST_MODEL', 'AMI_SMALL_FAST_MODEL'] as const;

/**
 * The configured small/fast model id for cheap auxiliary calls, or `fallback`
 * when none is set. Unlike the reference (which hardcodes a Haiku default), AMI is
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
  // or compaction fires far too early. Mirrors the reference's betas+modelSupports1M path.
  const oneMillion = !is1mContextDisabled() && (hasOneMillionMarker(modelId) || supportsOneMillionContextBeta(modelId));
  const applyBeta = (tokens: number) => (oneMillion ? Math.max(tokens, 1_000_000) : tokens);
  const entries = Object.entries(CONTEXT_WINDOWS);
  // 1. Exact id match (case-insensitive) always wins — the most-specific resolution.
  //    This matters for runtime-discovered ids appended by discoverFromModelList in
  //    arbitrary order, where a shorter family key could otherwise shadow the full id.
  //    Mirrors the reference's getModelCapability (exact-match before substring, utils/model/modelCapabilities.ts:L75-L83).
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

export const ADVERTISED_CONTEXT_KEYS = [
  'max_input_tokens',
  'max_model_len',
  'context_length',
  'context_window',
  'max_context_length',
  'input_token_limit',
  'inputTokenLimit',
  'max_position_embeddings',
] as const;

export function positiveTokenCount(value: unknown): number | undefined {
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
export const ADVERTISED_OUTPUT_KEYS = [
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

export function modelIdsMatch(requested: string, advertised: string): boolean {
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
