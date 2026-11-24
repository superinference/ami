// Split from model-capabilities.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { THINKING_LEVELS, getContextWindow } from './model-capabilities-get-context-window';
import type { ModelCapabilities, ThinkingLevel } from './model-capabilities-get-context-window';
import { CAPABILITY_OVERRIDE_TIERS, isFirstPartyProvider, parseClaudeVersion } from './model-capabilities-parse-claude-version';
import type { ModelCapabilityOverride } from './model-capabilities-parse-claude-version';

/**
 * Whether a capability override is declared for `modelId` via the pinned-tier env
 * vars, as an explicit boolean, or `undefined` when no override applies (so the
 * caller falls through to auto-detection). This is the escape hatch for
 * gateway/proxy setups where a custom model id matches no built-in pattern table
 * and auto-detection guesses wrong: pin the id to a tier
 * (`ANTHROPIC_DEFAULT_{OPUS,SONNET,HAIKU}_MODEL`) and declare its capabilities in
 * the parallel `*_SUPPORTED_CAPABILITIES` list (comma-separated, case-insensitive).
 *
 * Precisely mirrors the reference's `get3PModelCapabilityOverride`
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
export function applyCapabilityOverrides(
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

/**
 * Model families that accept image input. A live /models payload can override
 * this via registerModelCapabilities({ supportsVision }). The id may carry a
 * provider prefix (e.g. 'openai/gpt-4o' or 'openrouter/anthropic/claude-...'),
 * so we test every '/'-delimited suffix against the prefix table.
 */
export const VISION_MODEL_PREFIXES: string[] = [
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
 * Model families that accept native `document` (PDF) content blocks. Narrower than
 * {@link VISION_MODEL_PREFIXES}: native PDF ingestion is currently an Anthropic-the reference
 * and Google-Gemini capability, so a vision-only model (e.g. an open-weight VLM) is NOT
 * assumed to accept documents. A live /models payload can override this per-model via
 * `registerModelCapabilities({ supportsDocuments })`.
 */
export const DOCUMENT_MODEL_PREFIXES: string[] = [
  // Anthropic — Claude 3+ accept PDF document blocks.
  'claude-3', 'claude-sonnet-', 'claude-opus-', 'claude-haiku-', 'claude-fable-',
  // Google — Gemini 1.5+ accept PDF document input.
  'gemini-1.5', 'gemini-2', 'gemini-3',
];

export const BUDGET_MAP: Record<ThinkingLevel, number> = {
  off: 0,
  low: 4096,
  medium: 10240,
  high: 32768,
  max: 0,
};

/** Completion budget used when the caller omits maxTokens. */
export const DEFAULT_MAX_OUTPUT_TOKENS = 32_768;

/**
 * Global fallback output-token ceiling for a recognized-but-untabled the reference
 * model (the reference's MAX_OUTPUT_TOKENS_UPPER_LIMIT, utils/context.ts). Used only as
 * the `upperLimit` fallback in {@link getModelMaxOutputTokens}; it is NOT a cap
 * for unknown/non-Claude models (those still return undefined from
 * {@link getMaxOutputTokens} so no clamp is applied).
 */
export const MAX_OUTPUT_TOKENS_UPPER_LIMIT = 64_000;

/** A model's output-token budget split: the conservative default sent by
 *  default, and the hard ceiling a caller may escalate to. Mirrors the reference's
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
 * model's output limit and be rejected by the API. Modern the reference families
 * additionally ship a default BELOW their ceiling (e.g. opus-4-6 sends 64k by
 * default but may be raised to 128k) — mirroring the reference's getModelMaxOutputTokens
 * ladder (utils/context.ts:L146-L210). Longer / more-specific ids come first so
 * `claude-3-5-sonnet` is not shadowed by `claude-3`, and `opus-4-6` is not
 * shadowed by `opus-4`. A live `/models` capability or registered
 * maxOutputTokens overrides the ceiling (see getModelMaxOutputTokens).
 */
export const OUTPUT_TOKEN_CAPS: Record<string, MaxOutputTokenRange> = {
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

export const EFFORT_RANK: Record<ThinkingLevel, number> = {
  off: 0, low: 1, medium: 2, high: 3, max: 4,
};

export function clampEffortLevel(requested: ThinkingLevel, maxLevel: ThinkingLevel | undefined): ThinkingLevel {
  if (!maxLevel || !(maxLevel in EFFORT_RANK)) return requested;
  if (EFFORT_RANK[requested] <= EFFORT_RANK[maxLevel]) return requested;
  return maxLevel;
}

/**
 * Step a thinking level one rung in `direction` (+1 up, -1 down) with wraparound,
 * so an interactive cycle (`/thinking up`/`down`, a keybinding) never lands
 * out of range. When `includeMax` is false the top `max` rung is skipped — used
 * where a model does not accept the `max` effort tier so cycling clamps to `high`
 * instead of offering an unusable level. Mirrors the reference's ModelPicker effort
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
 * Resolution order mirrors the reference's `modelSupportsMaxEffort`
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
 * advisor/architect model. Mirrors the reference's `modelSupportsAdvisor`
 * (utils/advisor.ts:L88-L96), which gates the advisor tool on the Opus/Sonnet
 * 4.6+ generations (`opus-4-6`/`sonnet-4-6` and newer). AMI applies the same
 * version-aware family match — Opus or Sonnet with major≥5, or major 4 minor≥6 —
 * so a bare `claude-sonnet-4` (4.0) or a 3.x model is NOT advisor-capable and the
 * `/architect` secondary model is reported as INACTIVE rather than silently doing
 * nothing. The id may carry a gateway prefix, so the match is unanchored; Haiku,
 * non-the reference ids, and empty/non-string inputs are never advisor-capable.
 */
export function modelSupportsAdvisor(modelId: string): boolean {
  if (typeof modelId !== 'string' || !modelId) return false;
  const v = parseClaudeVersion(modelId);
  if (!v || (v.family !== 'opus' && v.family !== 'sonnet')) return false;
  return v.major >= 5 || (v.major === 4 && v.minor >= 6);
}

/**
 * Downgrade a requested reasoning-effort/thinking level to what the model will
 * actually accept. The top `max` tier is clamped to `high` when the model is not
 * max-effort-capable ({@link modelSupportsMaxEffort}), so the provider never
 * sends `effort: 'max'` to a model that would reject it with an API error — and
 * the displayed level stays consistent with what is sent. Every other level
 * passes through unchanged. Mirrors the reference's `resolveAppliedEffort` max→high
 * downgrade (utils/effort.ts:L162-165).
 */
export function resolveMaxEffortLevel(level: ThinkingLevel, modelId: string): ThinkingLevel {
  if (level === 'max' && !modelSupportsMaxEffort(modelId)) return 'high';
  return level;
}

export const PROVIDER_SAMPLING_DEFAULTS: Record<string, { temperature?: number; topP?: number; topK?: number }> = {
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

// ---------------------------------------------------------------------------
// Model switch safety
// ---------------------------------------------------------------------------

/**
 * Rough per-token pricing (USD) for compaction cost estimation.
 * Values are order-of-magnitude estimates; unknown models get zero.
 */
export const TOKEN_PRICING: Record<string, { input: number; output: number }> = {
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

export function lookupPricing(modelId: string): { input: number; output: number } | null {
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
