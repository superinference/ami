// Split from model-registry.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import type { ProviderConfig } from './types';
import { toApiProviderClass } from './model-capabilities';
import { resolveBedrockModelId, getBedrockInferenceProfiles, getBedrockRegionPrefix } from './bedrock';
import { FAMILY_VERSION_FALLBACKS, detectProvider } from './model-registry-detect-provider';

export function suggestFamilyVersionFallback(modelId: string, available: string[] = []): string | undefined {
  for (const { pattern, fallback } of FAMILY_VERSION_FALLBACKS) {
    if (pattern.test(modelId)) {
      // Prefer the prior version the provider actually lists; otherwise return the
      // canonical prior id so the user still has a concrete target.
      const live = available.find(id => id.toLowerCase().includes(fallback.toLowerCase()));
      return live ?? fallback;
    }
  }
  return undefined;
}

// ---------------------------------------------------------------------------
// Model aliases — short, memorable family names accepted anywhere a model id is
// expected. Mirrors the reference's canonical MODEL_ALIASES set
// (utils/model/aliases.ts): a user can type a family name (`opus`) instead of a
// full provider id. The set, the membership guard, and the alias→canonical
// resolver live here so EVERY consumer (the subagent `model` override in
// tools/task.ts, validateModel below, and — via the public barrel — the
// CLI/slash paths) resolves the SAME short names to the SAME canonical ids.
// This is the single canonical alias table: tools/task.ts previously kept its
// own inline copy and now imports these instead (consolidation — no parallel
// alias maps).
// ---------------------------------------------------------------------------

/** Canonical set of accepted model-alias inputs (case-insensitive, trimmed). */
export const MODEL_ALIASES = [
  'sonnet', 'opus', 'haiku', 'best', 'sonnet[1m]', 'opus[1m]', 'opusplan',
] as const;

export type ModelAlias = (typeof MODEL_ALIASES)[number];

/**
 * Bare family aliases (sonnet/opus/haiku). These act as wildcards for the
 * subagent anti-downgrade check: a bare family request inherits the parent's
 * exact same-family model instead of the generic family default.
 */
export const MODEL_FAMILY_ALIASES = ['sonnet', 'opus', 'haiku'] as const;

/** Each accepted alias → the concrete family-default model id it resolves to. */
export const MODEL_ALIAS_TARGETS: Record<ModelAlias, string> = {
  'sonnet': 'claude-sonnet-4',
  'opus': 'claude-opus-4',
  'haiku': 'claude-haiku-4-5',
  // "best" picks the strongest family; "opusplan" runs Opus for planning.
  'best': 'claude-opus-4',
  'opusplan': 'claude-opus-4',
  // The 1M-context suffix variants resolve to the same base model; the 1M window
  // itself is governed by model-capabilities, not the model id.
  'sonnet[1m]': 'claude-sonnet-4',
  'opus[1m]': 'claude-opus-4',
};

/**
 * Version-specific SHORT MODEL KEYS — stable, typeable selectors that name a
 * concrete model generation without the dated, provider-prefixed literal (e.g.
 * `opus45` → `claude-opus-4-5`). Mirrors the reference's ALL_MODEL_CONFIGS /
 * ModelKey set (utils/model/configs.ts:L86-L101) so a user can pin an exact
 * version with a memorable key anywhere a model id is accepted (`--model`,
 * `/model`, the subagent `model` override). This is DISTINCT from the bare
 * FAMILY aliases in {@link MODEL_ALIASES} (opus/sonnet/haiku), which resolve to
 * the family default and act as anti-downgrade wildcards; a short key always
 * pins one concrete generation. Every target id is a model AMI recognizes in
 * model-capabilities, so the context-window / thinking-budget lookup resolves.
 * (Functional change: new version-keyed short-key selector — parity gap
 * config-resolver/configs "stable short model keys as a user-facing selector".)
 */
export const MODEL_SHORT_KEYS: Record<string, string> = {
  haiku35: 'claude-3-5-haiku',
  haiku45: 'claude-haiku-4-5',
  sonnet35: 'claude-3-5-sonnet',
  sonnet37: 'claude-3-7-sonnet',
  sonnet40: 'claude-sonnet-4',
  sonnet45: 'claude-sonnet-4-5',
  sonnet46: 'claude-sonnet-4-6',
  opus40: 'claude-opus-4',
  opus41: 'claude-opus-4-1',
  opus45: 'claude-opus-4-5',
  opus46: 'claude-opus-4-6',
};

/**
 * Env-var knobs that steer model routing / legacy-id remap, surfaced in
 * `ami --introspect` (modelEnvToggles) so the feature manifest documents them —
 * the repo invariant that every new env-controlled capability is discoverable.
 * The SINGLE declaration of these toggles; the behavior lives in
 * {@link detectProvider} (routing) and {@link isLegacyModelRemapDisabled} /
 * {@link suggestDeprecatedReplacement} (remap opt-out).
 */
export const MODEL_ENV_TOGGLES = [
  {
    env: 'CLAUDE_CODE_USE_BEDROCK',
    affects: 'provider-routing',
    description: 'Route to the amazon-bedrock provider from the explicit toggle, independent of credential sniffing (parity with the reference getAPIProvider).',
  },
  {
    env: 'SUPERINFERENCE_USE_BEDROCK',
    affects: 'provider-routing',
    description: "AMI-convention alias of CLAUDE_CODE_USE_BEDROCK; either selects the amazon-bedrock provider.",
  },
  {
    env: 'CLAUDE_CODE_USE_FOUNDRY',
    affects: 'provider-routing',
    description: 'Route to the Azure/Microsoft Foundry endpoint (AMI azure-openai provider) from the explicit toggle, independent of credential sniffing (parity with the reference getAPIProvider).',
  },
  {
    env: 'SUPERINFERENCE_USE_FOUNDRY',
    affects: 'provider-routing',
    description: 'AMI-convention alias of CLAUDE_CODE_USE_FOUNDRY; either selects the azure-openai (Foundry) provider.',
  },
  {
    env: 'CLAUDE_CODE_DISABLE_LEGACY_MODEL_REMAP',
    affects: 'model-remap',
    description: 'Opt out of the deprecated→successor model remap — suppresses both the startup settings migration and the /model "did you mean" suggestion (parity with the reference useModelMigrationNotifications).',
  },
] as const;

/** Normalize an input for alias matching: trimmed + lower-cased. */
export function normalizeAlias(input: string): string {
  return typeof input === 'string' ? input.trim().toLowerCase() : '';
}

/** True when `input` (trimmed, case-insensitive) is a recognized model alias. */
export function isModelAlias(input: string): boolean {
  return (MODEL_ALIASES as readonly string[]).includes(normalizeAlias(input));
}

/**
 * True when `input` (trimmed, case-insensitive) is a recognized version-specific
 * short model key (e.g. `opus45`, `sonnet46`). Distinct from {@link isModelAlias},
 * which covers the bare family aliases.
 */
export function isModelShortKey(input: string): boolean {
  return Object.prototype.hasOwnProperty.call(MODEL_SHORT_KEYS, normalizeAlias(input));
}

/** True when `input` is a bare family alias (sonnet/opus/haiku). */
export function isModelFamilyAlias(input: string): boolean {
  return (MODEL_FAMILY_ALIASES as readonly string[]).includes(normalizeAlias(input));
}

/**
 * Resolve a model alias to its canonical family-default model id, or undefined
 * when `input` is not a recognized alias (a full/unknown model id passes through
 * for the caller to handle). Case-insensitive and whitespace-tolerant.
 */
export function resolveModelAlias(input: string): string | undefined {
  const key = normalizeAlias(input);
  if (isModelAlias(key)) return MODEL_ALIAS_TARGETS[key as ModelAlias];
  // Version-specific short keys (opus45/sonnet46/…) pin a concrete generation.
  if (isModelShortKey(key)) return MODEL_SHORT_KEYS[key];
  return undefined;
}

/**
 * Request shape for {@link getRuntimeMainLoopModel} (parity: the reference's
 * `getRuntimeMainLoopModel({ permissionMode, mainLoopModel, exceeds200kTokens })`,
 * utils/model/agent.ts:L80-88).
 */
export interface RuntimeMainLoopModelRequest {
  /** The active permission mode (`plan` switches a `opusplan` run to Opus). */
  permissionMode?: string;
  /** The configured main-loop model id/alias to re-resolve at runtime. */
  mainLoopModel: string | undefined;
}

/**
 * Re-resolve a main-loop / inherited model the same way the running main loop would
 * at spawn time, instead of copying the configured string verbatim (parity: the reference
 * Code's `getRuntimeMainLoopModel`, which the agent-inherit path runs the parent
 * model through — utils/model/agent.ts:L80-88). The behavioral difference AMI needs
 * is the DEFERRED `opusplan` alias: it resolves to Opus while the agent is in plan
 * mode (planning benefits from the strongest model) and to Sonnet during execution.
 * Every concrete model id — and every other alias — passes through unchanged, so a
 * subagent that INHERITS the parent model gets mode-conditional switching at spawn
 * without ever downgrading an explicitly-pinned model. (AMI governs the long-context
 * 1M window through model-capabilities, not a distinct model id, so unlike the reference
 * there is no separate context-size re-route here — the window is a capability of the
 * resolved model, not a different model.)
 */
export function getRuntimeMainLoopModel(req: RuntimeMainLoopModelRequest): string | undefined {
  const { permissionMode, mainLoopModel } = req;
  if (!mainLoopModel) return mainLoopModel;
  if (normalizeAlias(mainLoopModel) === 'opusplan') {
    return permissionMode === 'plan'
      ? MODEL_ALIAS_TARGETS['opus']
      : MODEL_ALIAS_TARGETS['sonnet'];
  }
  return mainLoopModel;
}

// ---------------------------------------------------------------------------
// Provider-aware default subagent model — the fast family default a brand-new
// subagent/teammate runs on when NO model is resolvable from any higher-priority
// source. Mirrors the reference's provider-aware getHardcodedTeammateModelFallback
// (utils/swarm/teammateModel.ts): a teammate the user never configured still gets
// a concrete, PROVIDER-CORRECT model id (Bedrock/Vertex/Foundry), never an empty
// string. AMI keeps ONE central send-path remap (provider.ts → resolveBedrockModelId)
// instead of the reference's N hardcoded per-provider config tables, so the per-provider
// mapping here REUSES that same canonical Bedrock resolver — no parallel id table.
// ---------------------------------------------------------------------------

/**
 * Canonical, provider-neutral fast model id a subagent falls back to when nothing
 * else selected a model. A read-only/delegated subagent does not need the main
 * model's full capability, so this is the fast/cheap family default (matches the
 * `haiku` alias target and the explore-mode fast default). {@link getDefaultSubagentModel}
 * maps it to the concrete id for the active provider.
 */
export const DEFAULT_SUBAGENT_MODEL = 'claude-haiku-4-5';

/**
 * Map a canonical first-party model id to the concrete id to INVOKE for the
 * provider that `config` detects. For Amazon Bedrock the id is resolved against
 * the account's declared inference profiles / cross-region prefix via the SINGLE
 * canonical {@link resolveBedrockModelId} — the exact resolver the provider
 * send-path (provider.ts) already uses, so a selected id lands on a provisioned
 * region-scoped profile with no parallel per-provider id table. Vertex / Foundry /
 * first-party send the canonical id unchanged (AMI's one-central-remap design).
 */
export function toProviderModelId(
  canonicalId: string,
  config: ProviderConfig,
  bedrockProfiles?: string[],
): string {
  if (toApiProviderClass(detectProvider(config)) === 'bedrock') {
    return resolveBedrockModelId(canonicalId, bedrockProfiles ?? getBedrockInferenceProfiles());
  }
  return canonicalId;
}

/**
 * Provider-aware default subagent model: the fast {@link DEFAULT_SUBAGENT_MODEL}
 * resolved to the concrete id for the provider `config` detects. Used as the FINAL
 * fallback when a subagent has no model configured anywhere (no per-call `model`,
 * no agent-definition model, no SUPERINFERENCE_SUBAGENT_MODEL override, and no
 * inheritable parent model), so a Bedrock run gets a provisioned region-scoped
 * profile id and a first-party run gets the canonical id — never an empty model
 * string. Mirrors the reference's provider-aware getHardcodedTeammateModelFallback.
 */
export function getDefaultSubagentModel(config: ProviderConfig): string {
  return toProviderModelId(DEFAULT_SUBAGENT_MODEL, config);
}

/**
 * One row of the per-provider model-string table: a logical short key, its
 * canonical first-party model id, and the concrete wire id to SEND on the active
 * provider.
 */
export interface ProviderModelString {
  /** Logical, typeable short key (e.g. `opus46`) — stable across providers. */
  key: string;
  /** The canonical first-party model id the key resolves to (`claude-opus-4-6`). */
  firstParty: string;
  /** The provider-specific wire id to invoke on the detected provider (e.g. a
   *  Bedrock region-scoped inference profile), or the canonical id unchanged for a
   *  first-party / Vertex / Foundry endpoint. */
  wireId: string;
}

/**
 * Per-provider canonical model-id table: for every logical short key in
 * {@link MODEL_SHORT_KEYS}, the canonical first-party id AND the concrete wire id
 * to invoke on the provider `config` detects. Mirrors the reference's
 * ALL_MODEL_CONFIGS / getBuiltinModelStrings(provider) — one logical ModelKey
 * mapped to a provider-specific id string — so the same logical model (`opus46`)
 * resolves to the right per-provider id without the user retyping it when they
 * switch providers.
 *
 * AMI realizes this through its SINGLE central send-path remap rather than N
 * hardcoded per-provider id tables: `wireId` is produced by the one canonical
 * {@link toProviderModelId} (which routes Bedrock ids through
 * {@link resolveBedrockModelId} against the account's inference profiles / region
 * prefix, and passes first-party / Vertex / Foundry ids through unchanged). No
 * parallel per-provider id table is introduced — this is the aggregated VIEW of the
 * existing resolution across every logical key, surfaced for selection UIs and
 * `ami --introspect`.
 */
/**
 * Build the per-provider model-string rows for `config` from the supplied Bedrock
 * inference-profile list (used verbatim when the detected provider is Bedrock; the
 * canonical first-party id passes through unchanged otherwise, via the single
 * {@link toProviderModelId} remap — no parallel per-provider id table). Factored so
 * the synchronous {@link getBuiltinModelStrings} view and the async
 * {@link ensureModelStringsInitialized} barrier produce byte-identical rows from the
 * SAME mapping — the barrier simply sources the profile list from an awaited (and
 * cacheable) resolver instead of a point-in-time env read.
 */
export function buildModelStrings(config: ProviderConfig, bedrockProfiles: string[]): ProviderModelString[] {
  return Object.entries(MODEL_SHORT_KEYS).map(([key, firstParty]) => ({
    key,
    firstParty,
    wireId: toProviderModelId(firstParty, config, bedrockProfiles),
  }));
}

// ---------------------------------------------------------------------------
// Model-string initialization barrier — parity with the reference's
// ensureModelStringsInitialized / getModelStrings pair (utils/model/modelStrings.ts
// L136-L166). Building a model-option list for a Bedrock account should reflect the
// region-scoped inference-profile strings rather than interim defaults; the reference awaits
// its profile fetch before generating options. AMI sources Bedrock profiles through
// the single canonical resolver (bedrock.ts getBedrockInferenceProfiles) behind an
// injectable async seam, caches the resolved snapshot ONCE per provider+region+
// profile signature (sequential-guarded so a concurrent burst shares one resolution),
// and serves subsequent builds from that snapshot. getBuiltinModelStrings returns the
// cached snapshot when initialized and otherwise computes the interim default inline
// (mirrors the reference's getModelStrings falling through to built-in strings while the
// fetch is pending). The SINGLE model-string init path — no parallel state module.
// ---------------------------------------------------------------------------

/**
 * Injectable async source of the account's Bedrock inference-profile ids, awaited by
 * {@link ensureModelStringsInitialized} before building the Bedrock model-string
 * snapshot. Defaults to the synchronous env-backed {@link getBedrockInferenceProfiles}
 * (AMI's one-central-remap design keeps profiles in the environment), wrapped in a
 * resolved promise so the barrier's await is uniform. A test — or a future live
 * profile fetch — can replace it to make the Bedrock readiness await observable.
 */
export const modelStringsInitHooks: { resolveBedrockProfiles: () => Promise<string[]> } = {
  resolveBedrockProfiles: async () => getBedrockInferenceProfiles(),
};

/** Signature keying a cached model-string snapshot: provider + region + profiles. */
export function modelStringsSignature(config: ProviderConfig): string {
  return [
    detectProvider(config),
    getBedrockRegionPrefix() ?? '',
    getBedrockInferenceProfiles().join(','),
  ].join(':::');
}

/**
 * True when `modelName` begins with `prefix` at a SEGMENT BOUNDARY — the match
 * must reach the end of the name or a '-' separator. So "claude-opus-4-5"
 * matches "claude-opus-4-5-20251101" but NOT "claude-opus-4-50". Mirrors the reference
 * Code's prefixMatchesModel (utils/model/modelAllowlist.ts).
 */
export function prefixMatchesModel(modelName: string, prefix: string): boolean {
  if (!modelName.startsWith(prefix)) return false;
  return modelName.length === prefix.length || modelName[prefix.length] === '-';
}

/**
 * True when `model` matches a version-prefix `entry` with segment-boundary
 * safety and 'claude-' shorthand: an entry like "opus-4-5" is auto-expanded to
 * "claude-opus-4-5" so it matches "claude-opus-4-5-20251101" while refusing
 * "claude-opus-4-50". Case-insensitive. Mirrors the reference's
 * modelMatchesVersionPrefix — the single boundary-aware matcher reused by the
 * validateModel suggestion path (and available to the engine allowlist check so
 * admins can pin a version family without accidentally matching a sibling).
 */
export function modelMatchesVersionPrefix(model: string, entry: string): boolean {
  const m = model.toLowerCase();
  const e = entry.toLowerCase();
  if (prefixMatchesModel(m, e)) return true;
  if (!e.startsWith('claude-') && prefixMatchesModel(m, `claude-${e}`)) return true;
  return false;
}

/**
 * Order candidate model ids by CLOSENESS to `target`, closest first. Closeness is
 * the absolute length delta (|id.length − target.length|) — the tightest
 * super/substring of the requested id — tie-broken by shorter id, then
 * case-insensitive lexicographic order for stable, deterministic output. Returns a
 * NEW array (never mutates the input). Used by {@link validateModel} to pick the
 * best "did you mean" suggestion from a match tier instead of the provider's
 * arbitrary list order.
 */
export function rankClosestMatches(candidates: string[], target: string): string[] {
  const t = target.length;
  return [...candidates].sort((a, b) => {
    const da = Math.abs(a.length - t);
    const db = Math.abs(b.length - t);
    if (da !== db) return da - db;
    if (a.length !== b.length) return a.length - b.length;
    return a.toLowerCase().localeCompare(b.toLowerCase());
  });
}

// ---------------------------------------------------------------------------
// Human-readable model metadata — friendly display names + one-line
// descriptions for the /model picker and status line. Mirrors the reference's
// getPublicModelDisplayName (utils/model/model.ts:L344-L415) + the per-option
// label/description copy in utils/model/modelOptions.ts (L96-L209, L431-L459).
// AMI resolves these from the RAW provider model id with pattern matching (no
// provider-specific model-string table) so a dated snapshot
// ("claude-sonnet-4-5-20251022"), a gateway-prefixed id
// ("openrouter/anthropic/claude-sonnet-4.6"), the `[1m]` long-context suffix, or
// a bare family id all resolve to the SAME friendly label. Unknown ids fall back
// to the raw id itself — the reference's renderModelName default branch (a marketing
// label for a raw model id that is always a usable string).
// ---------------------------------------------------------------------------

/** Family tier → one-line picker description (the reference's modelOptions copy). */
export const MODEL_FAMILY_DESCRIPTION = {
  opus: 'Most capable — best for complex, multi-step work',
  sonnet: 'Balanced — best for everyday coding tasks',
  haiku: 'Fastest and lowest cost — best for quick answers',
  gpt: 'OpenAI frontier model',
  gemini: 'Google Gemini model',
} as const;

export type ModelFamily = keyof typeof MODEL_FAMILY_DESCRIPTION;
