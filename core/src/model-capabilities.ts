import * as fs from 'fs';
import 'path';
import './token-format';
import './utils/string';
import { instanceScopedTempName } from './utils/runtime-namespace';
import { CONTEXT_WINDOWS, readAdvertisedContextWindow, readAdvertisedMaxOutputTokens } from './model-capabilities-get-context-window';
import type { ModelCapabilities, ThinkingLevel } from './model-capabilities-get-context-window';
import { REASONING_MODELS, classifyOpenAIModel, isFirstPartyProvider, modelCapabilitiesCachePath, parseCacheFile, parseClaudeVersion, sameCacheModels, sortCacheEntries } from './model-capabilities-parse-claude-version';
import type { ModelCapabilityCacheEntry, ModelCapabilityCacheFile } from './model-capabilities-parse-claude-version';
import { BUDGET_MAP, DEFAULT_MAX_OUTPUT_TOKENS, DOCUMENT_MODEL_PREFIXES, MAX_OUTPUT_TOKENS_UPPER_LIMIT, MIN_FITTED_OUTPUT_TOKENS, OUTPUT_FIT_SAFETY_TOKENS, OUTPUT_TOKEN_CAPS, VISION_MODEL_PREFIXES, applyCapabilityOverrides, getProviderSamplingDefaults, lookupPricing } from './model-capabilities-get3-p-model-capability-override';
import type { MaxOutputTokenRange } from './model-capabilities-get3-p-model-capability-override';
export { DEFAULT_CONTEXT_WINDOW, REVIEW_EFFORT_LEVELS, SMALL_FAST_MODEL_ENV_VARS, THINKING_LEVELS, THINKING_LEVEL_DESCRIPTIONS, collectModelRecords, getContextWindow, getContextWindowOverride, getSmallFastModel, getThinkingLevelDescription, is1mContextDisabled, normalizeModelStringForAPI, readAdvertisedContextWindow, readAdvertisedMaxOutputTokens, resolveSkillModelOverride, supportsOneMillionContextBeta } from './model-capabilities-get-context-window';
export type { ModelCapabilities, ReviewEffortLevel, ThinkingLevel } from './model-capabilities-get-context-window';
export { MODEL_CAPABILITIES_CACHE_FILE, MODEL_CAPABILITY_OVERRIDES, discoveryToCacheEntries, huggingfaceRepoCandidates, huggingfaceRepoId, isFirstPartyProvider, modelCapabilitiesCacheDir, modelCapabilitiesCachePath, parseClaudeVersion, pickAdvertisedContextWindow, readHuggingFaceContextWindow, requiresResponsesAPI } from './model-capabilities-parse-claude-version';
export type { ModelCapabilityCacheEntry, ModelCapabilityCacheFile, ModelCapabilityOverride } from './model-capabilities-parse-claude-version';
export { DEFAULT_MAX_OUTPUT_TOKENS, MAX_OUTPUT_TOKENS_UPPER_LIMIT, MIN_FITTED_OUTPUT_TOKENS, MIN_THINKING_BUDGET, OUTPUT_FIT_SAFETY_TOKENS, THINKING_HEADROOM_MARGIN, capThinkingBudget, clampEffortLevel, cycleThinkingLevel, get3PModelCapabilityOverride, getProviderSamplingDefaults, maxToolOutputChars, modelSupportsAdvisor, modelSupportsMaxEffort, modelSwitchNeedsCompaction, resolveMaxEffortLevel } from './model-capabilities-get3-p-model-capability-override';
export type { MaxOutputTokenRange } from './model-capabilities-get3-p-model-capability-override';
export { API_PROVIDER_CLASS, findThinkingTriggerPositions, getAvailableContextUpgrade, getModelDeprecationWarning, getModelSwitchWarning, hasUltrathinkKeyword, toApiProviderClass } from './model-capabilities-get-available-context-upgrade';
export type { ApiProviderClass, ContextUpgrade } from './model-capabilities-get-available-context-upgrade';

/**
 * Capabilities for a thinking-capable Claude model, classified by version. Adaptive thinking
 * (`thinking: { type:'adaptive' }`) is supported ONLY on Claude 4.6+ and the 5.x family; the
 * pre-4.6 Claude 4.x models (opus 4/4.1/4.5, sonnet 4/4.5) REJECT adaptive thinking with an
 * HTTP 400 ("adaptive thinking is not supported on this model") and must use the legacy
 * `{ type:'enabled', budget_tokens }` shape instead. This split is exactly what the
 * @ai-sdk/anthropic provider's own model metadata encodes (supportsAdaptiveThinking) — ui#24.
 * Returns null for non-reasoning the reference families (e.g. haiku), which keep thinking off.
 */
export function claudeReasoningCapabilities(modelId: string): ModelCapabilities | null {
  const v = parseClaudeVersion(modelId);
  if (!v) return null;
  // Current reasoning scope: Opus / Sonnet. Other families (haiku, …) stay non-reasoning.
  if (v.family !== 'opus' && v.family !== 'sonnet') return null;
  const supportsAdaptiveThinking = v.major >= 5 || (v.major === 4 && v.minor >= 6);
  // The 5.x the reference line defaults to adaptive thinking and rejects a disabled-thinking
  // request — reasoning cannot be turned off (the reference's AntModel.alwaysOnThinking). The
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
    // mirroring the reference's getMaxThinkingTokensForModel) when the model is in the
    // output-cap table; otherwise keep the 128k default for untabled flagship ids
    // so a newer model is never under-budgeted. Uses the static lookup directly to
    // avoid recursing through getModelCapabilities (which calls this function).
    maxThinkingBudget: deriveMaxThinkingBudget(modelId),
  };
}

/**
 * The thinking-token ceiling for a Claude model, tied to its real output cap.
 * Returns the tabled output upperLimit minus one (parity with the reference's
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
  // thinking ceiling (parity with the reference's getMaxThinkingTokensForModel, which
  // derives the thinking cap from the live output upperLimit). A plain Map read, so
  // it introduces no recursion back through getModelCapabilities.
  const dynamicCap = dynamicOutputCaps.get(lower);
  if (dynamicCap !== undefined && dynamicCap > 0) return Math.max(1_024, dynamicCap - 1);
  const upperLimit = lookupOutputTokenRange(lower)?.upperLimit;
  return upperLimit !== undefined ? upperLimit - 1 : 128000;
}

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

/** Memoized reads, keyed by absolute cache-file path, so repeated warm calls in
 *  one process touch the disk once. Cleared by {@link clearDynamicCapabilities}. */
const cacheReadMemo = new Map<string, ModelCapabilityCacheFile | null>();

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
  const tmp = instanceScopedTempName(file);
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

/** Auto-detected capabilities (dynamic registry → version-aware the reference → pattern table). */
function detectModelCapabilities(modelId: string): ModelCapabilities | null {
  const dynamic = dynamicCapabilities.get(modelId.toLowerCase());
  if (dynamic !== undefined) return dynamic;
  // the reference thinking classification is version-aware (adaptive is 4.6+/5.x; pre-4.6 uses
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
 * Whether the given model can accept native `document` (e.g. PDF) input. Honors an
 * explicitly registered `supportsDocuments` capability first, then falls back to the
 * known document-capable family prefix table. Unknown models default to false so a
 * document is never sent blindly to a model that would reject it. Parity: the reference's
 * PDF `document` content block (supported on Claude models). */
export function modelSupportsDocuments(modelId: string): boolean {
  if (typeof modelId !== 'string' || !modelId) return false;
  const explicit = getModelCapabilities(modelId)?.supportsDocuments;
  if (explicit !== undefined) return explicit;
  const id = modelId.toLowerCase();
  const candidates = [id, ...id.split('/')];
  return candidates.some(c => DOCUMENT_MODEL_PREFIXES.some(p => c.startsWith(p)));
}

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
 * escalate to. Mirrors the reference's getModelMaxOutputTokens (utils/context.ts):
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

/**
 * The thinking level a model runs at when the user asks for "auto" / the default
 * (no explicit level). Resolved PER MODEL from its detected
 * {@link ModelCapabilities} — a reasoning model yields its
 * `defaultThinkingLevel` (e.g. Gemini-flash defaults to `low`, the reference to
 * `medium`), while a non-reasoning model yields `off` because it cannot think.
 * This is the resolver behind the `/thinking auto` sentinel: it lets the UI show
 * "auto (currently <level>)" with the level that will actually be applied on the
 * active model. Mirrors the reference's per-model default-effort resolution
 * (commands/effort/effort.tsx `auto` state). An empty / unknown id → `off`.
 */
export function getDefaultThinkingLevel(modelId: string): ThinkingLevel {
  return getModelCapabilities(modelId)?.defaultThinkingLevel ?? 'off';
}

/**
 * Whether the model defaults to adaptive thinking and REJECTS a request to turn
 * reasoning off (`thinking: { type:'disabled' }`). Resolves from the model's
 * detected {@link ModelCapabilities} `alwaysOnThinking` flag (the 5.x the reference line);
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

  // the reference: temperature must be omitted (SDK sets it to 1 internally)
  if (caps.requiresTemperatureOne) return undefined;

  // OpenAI o-series: temperature must not be sent at all
  if (caps.temperatureMustBeUnset) return undefined;

  return configTemperature;
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
 * Mirrors the reference's `get3PModelFallbackSuggestion` (services/api/errors.ts:
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
