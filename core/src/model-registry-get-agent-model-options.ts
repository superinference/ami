// Split from model-registry.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { MODEL_ALIASES, MODEL_FAMILY_DESCRIPTION, resolveModelAlias } from './model-registry-model-env-toggles';
import type { ModelFamily } from './model-registry-model-env-toggles';

/**
 * Family tier → a longer, MODEL-FACING description: the capability/cost guidance an
 * agent reads when it programmatically chooses a model (e.g. the `model` override for
 * a subagent / Task). DISTINCT from {@link MODEL_FAMILY_DESCRIPTION}, which is the
 * terse one-line copy shown to the HUMAN in the picker — this copy names the
 * capability/cost trade-off explicitly so the model can reason over it when selecting
 * a model for a sub-task. Mirrors the reference's dual-audience
 * `ModelOption.descriptionForModel` (utils/model/modelOptions.ts:L38-L43).
 */
export const MODEL_FAMILY_DESCRIPTION_FOR_MODEL: Record<ModelFamily, string> = {
  opus: 'Most capable — best for complex, multi-step work and hard reasoning. Higher cost; prefer it only when a task is genuinely difficult.',
  sonnet: 'Balanced — best for everyday tasks. Generally recommended for most coding tasks.',
  haiku: 'Fastest for quick answers. Lower cost but less capable than Sonnet — use for simple, well-scoped tasks.',
  gpt: 'OpenAI frontier model — general-purpose capability for coding and reasoning.',
  gemini: 'Google Gemini model — general-purpose capability for coding and reasoning.',
};

/**
 * Ordered display-name patterns, most-specific (versioned) first so a precise
 * version wins over the bare family. Each entry also carries the family tier its
 * description comes from.
 */
export const MODEL_DISPLAY_PATTERNS: ReadonlyArray<{ pattern: RegExp; name: string; family: ModelFamily }> = [
  // the reference — Opus
  { pattern: /opus-?4[._-]?6/i, name: 'Opus 4.6', family: 'opus' },
  { pattern: /opus-?4[._-]?5/i, name: 'Opus 4.5', family: 'opus' },
  { pattern: /opus-?4[._-]?1/i, name: 'Opus 4.1', family: 'opus' },
  { pattern: /claude-?3[._-]?opus/i, name: 'Claude 3 Opus', family: 'opus' },
  { pattern: /opus-?4/i, name: 'Opus 4', family: 'opus' },
  { pattern: /\bopus\b/i, name: 'Opus', family: 'opus' },
  // the reference — Sonnet
  { pattern: /sonnet-?4[._-]?6/i, name: 'Sonnet 4.6', family: 'sonnet' },
  { pattern: /sonnet-?4[._-]?5/i, name: 'Sonnet 4.5', family: 'sonnet' },
  { pattern: /3[._-]?7[._-]?sonnet/i, name: 'Sonnet 3.7', family: 'sonnet' },
  { pattern: /3[._-]?5[._-]?sonnet/i, name: 'Sonnet 3.5', family: 'sonnet' },
  { pattern: /sonnet-?4/i, name: 'Sonnet 4', family: 'sonnet' },
  { pattern: /\bsonnet\b/i, name: 'Sonnet', family: 'sonnet' },
  // the reference — Haiku
  { pattern: /haiku-?4[._-]?5/i, name: 'Haiku 4.5', family: 'haiku' },
  { pattern: /3[._-]?5[._-]?haiku/i, name: 'Haiku 3.5', family: 'haiku' },
  { pattern: /\bhaiku\b/i, name: 'Haiku', family: 'haiku' },
  // OpenAI
  { pattern: /gpt-?6/i, name: 'GPT-6', family: 'gpt' },
  { pattern: /gpt-?5/i, name: 'GPT-5', family: 'gpt' },
  { pattern: /gpt-?4o-?mini/i, name: 'GPT-4o mini', family: 'gpt' },
  { pattern: /gpt-?4o/i, name: 'GPT-4o', family: 'gpt' },
  { pattern: /gpt-?4/i, name: 'GPT-4', family: 'gpt' },
  // Google Gemini
  { pattern: /gemini-?3/i, name: 'Gemini 3', family: 'gemini' },
  { pattern: /gemini-?2[._-]?5[._-]?pro/i, name: 'Gemini 2.5 Pro', family: 'gemini' },
  { pattern: /gemini-?2[._-]?5[._-]?flash/i, name: 'Gemini 2.5 Flash', family: 'gemini' },
];

/** Strip the optional `[1m]` long-context suffix; report whether it was present. */
export function splitOneMillionSuffix(id: string): { base: string; is1m: boolean } {
  const is1m = /\[1m\]$/i.test(id);
  return { base: is1m ? id.replace(/\[1m\]$/i, '') : id, is1m };
}

export function matchModelPattern(id: string): { name: string; family: ModelFamily } | undefined {
  return MODEL_DISPLAY_PATTERNS.find(e => e.pattern.test(id));
}

/**
 * Reverse of `applyModelOverride` (common/provider-config): given a
 * provider-specific override STRING that was swapped in from the settings
 * `modelOverrides` map (e.g. an opaque Bedrock inference-profile ARN) and that
 * same map, return the CANONICAL first-party model id it was an override for — so
 * the UI can display the clean "claude-opus-4-6" instead of the raw ARN the
 * engine actually sends. Scans the override VALUES (not keys) for an exact match
 * and returns the matching canonical key; returns the input unchanged when there
 * is no override map or no value matches. Mirrors the reference's
 * resolveOverriddenModel (utils/model/modelStrings.ts:L78-L100) — the single
 * inverse of the forward apply, so the display path and the send path stay in
 * lockstep (no parallel ARN→name table).
 */
export function resolveOverriddenModel(
  modelString: string,
  overrides: Record<string, string> | undefined,
): string {
  if (!overrides) return modelString;
  for (const [canonicalId, override] of Object.entries(overrides)) {
    if (override === modelString) return canonicalId;
  }
  return modelString;
}

/**
 * Friendly, human-readable display name for a model id ("Sonnet 4.6",
 * "GPT-4o"). Recognizes dated snapshots, gateway prefixes, and the `[1m]`
 * long-context suffix (rendered as " (1M context)"). Falls back to the raw id so
 * the return value is ALWAYS a non-empty, user-presentable string for any known
 * OR custom model — the marketing-label-for-a-raw-id affordance. When `overrides`
 * (the settings `modelOverrides` map) is supplied, a raw override string (e.g. a
 * Bedrock ARN) is first mapped back to its canonical id via
 * {@link resolveOverriddenModel} so the override's clean family name is shown.
 */
export function getModelDisplayName(modelId: string, overrides?: Record<string, string>): string {
  const id = resolveOverriddenModel((modelId ?? '').trim(), overrides);
  if (id === '') return '';
  const { base, is1m } = splitOneMillionSuffix(id);
  const hit = matchModelPattern(base);
  if (!hit) return id;
  return is1m ? `${hit.name} (1M context)` : hit.name;
}

/**
 * One-line, human-readable description for the /model picker, or undefined when
 * the model id is not recognized as a known family (an unknown/custom id has no
 * canned marketing copy). Mirrors the reference's per-option `description` field.
 */
export function getModelDescription(modelId: string): string | undefined {
  const { base } = splitOneMillionSuffix((modelId ?? '').trim());
  const hit = matchModelPattern(base);
  return hit ? MODEL_FAMILY_DESCRIPTION[hit.family] : undefined;
}

/**
 * A longer, MODEL-FACING description for a model id — the capability/cost guidance an
 * agent reads when it programmatically selects a model (e.g. the `model` override for a
 * subagent / Task), as opposed to the terse human picker copy from
 * {@link getModelDescription}. Resolves the family the SAME way (through the `[1m]`
 * suffix, gateway prefixes, and dated snapshots), and returns undefined for an
 * unknown/custom id (no canned copy). Mirrors the reference's dual-audience
 * `ModelOption.descriptionForModel` (utils/model/modelOptions.ts:L38-L43).
 */
export function getModelDescriptionForModel(modelId: string): string | undefined {
  const { base } = splitOneMillionSuffix((modelId ?? '').trim());
  const hit = matchModelPattern(base);
  return hit ? MODEL_FAMILY_DESCRIPTION_FOR_MODEL[hit.family] : undefined;
}

/**
 * The current (newest) concrete model id per the reference family. A pinned model whose
 * display name differs from its family's current model is an OLDER version, so the
 * picker can surface a "newer version available" upgrade affordance. Mirrors the reference
 * Code's getDefault{Opus,Sonnet,Haiku}Model used by getModelFamilyInfo
 * (utils/model/modelOptions.ts:L385-L459); kept in sync with the newest entries of
 * {@link MODEL_SHORT_KEYS}.
 */
export const FAMILY_CURRENT_MODEL: Record<'opus' | 'sonnet' | 'haiku', string> = {
  opus: 'claude-opus-4-6',
  sonnet: 'claude-sonnet-4-6',
  haiku: 'claude-haiku-4-5',
};

/** Capitalized family alias to name in the upgrade hint ("Opus"/"Sonnet"/"Haiku"). */
export const FAMILY_ALIAS_LABEL: Record<'opus' | 'sonnet' | 'haiku', string> = {
  opus: 'Opus', sonnet: 'Sonnet', haiku: 'Haiku',
};

/**
 * Upgrade affordance for a PINNED OLDER model: when `modelId` resolves to a known
 * the reference family (opus/sonnet/haiku) whose current version is a DIFFERENT (newer)
 * build than the pinned one, returns
 * `Newer version available · select <Alias> for <CurrentVersionName>` — otherwise
 * undefined (unknown id, non-the reference family, or already the current version).
 * Mirrors the reference's getKnownModelOption upgrade-hint description
 * (utils/model/modelOptions.ts:L431-L459). Surfaced by the /model picker via
 * {@link getModelMetadata}.
 */
export function getNewerVersionHint(modelId: string): string | undefined {
  const { base } = splitOneMillionSuffix((modelId ?? '').trim());
  const hit = matchModelPattern(base);
  if (!hit) return undefined;
  const family = hit.family;
  if (family !== 'opus' && family !== 'sonnet' && family !== 'haiku') return undefined;
  const current = FAMILY_CURRENT_MODEL[family];
  const currentName = getModelDisplayName(current);
  // Same build as the family's current version → nothing to upgrade to.
  if (hit.name === currentName) return undefined;
  return `Newer version available · select ${FAMILY_ALIAS_LABEL[family]} for ${currentName}`;
}

/**
 * Combined picker annotation for a model id: a friendly `displayName` (always a
 * usable string), an optional `description`, and an optional `upgrade` hint when a
 * newer version of the pinned model's family is available. Mirrors the reference's
 * ModelOption `{ label, description }` so a picker row can show a human label + a
 * sub-line (and an upgrade nudge) instead of a bare id.
 */
export function getModelMetadata(
  modelId: string,
  overrides?: Record<string, string>,
): { displayName: string; description?: string; upgrade?: string } {
  // Resolve a settings-override string (e.g. a Bedrock ARN) back to its canonical
  // id ONCE, so the description + upgrade hint also key off the clean id rather
  // than the opaque override the engine sends to the provider.
  const resolved = resolveOverriddenModel((modelId ?? '').trim(), overrides);
  return {
    displayName: getModelDisplayName(resolved),
    description: getModelDescription(resolved),
    upgrade: getNewerVersionHint(resolved),
  };
}

/** One selectable row in the agent/model-choice catalog. */
export interface AgentModelOption {
  /** The value a selection persists — a short family alias, or a custom full id. */
  value: string;
  /** Friendly display label ("Opus 4.6") for the row. */
  label: string;
  /** Optional one-line description shown under the label (human-facing). */
  description?: string;
  /** Optional longer, MODEL-FACING description (capability/cost guidance) an agent
   *  reads when it programmatically picks a model for a sub-task. Distinct from the
   *  human `description`. Mirrors the reference's ModelOption.descriptionForModel. */
  descriptionForModel?: string;
}

/**
 * Curated agent-model option catalog: one {value,label,description} row per
 * accepted family alias (the single {@link MODEL_ALIASES} set), with the human
 * copy resolved through {@link getModelDisplayName} / {@link getModelDescription}
 * (no parallel label table). When `currentModel` is a concrete id that is NOT one
 * of the alias values — a custom or pinned full model id — it is injected at the
 * HEAD of the list labeled "Current model (custom ID)", so an edit flow round-trips
 * that custom id instead of silently overwriting it with an alias. Mirrors the reference
 * Code's getAgentModelOptions + ModelSelector custom-id injection
 * (components/agents/ModelSelector.tsx:L21-L38). The SINGLE source of the agent
 * model-choice catalog — consumed by the config tool's model section so the model
 * sees the same alias set the picker offers (consolidation: replaces the inline
 * MODEL_ALIASES listing the config tool built by hand).
 */
export function getAgentModelOptions(currentModel?: string): AgentModelOption[] {
  const base: AgentModelOption[] = MODEL_ALIASES.map((alias) => {
    const canonical = resolveModelAlias(alias) ?? alias;
    const description = getModelDescription(canonical);
    const descriptionForModel = getModelDescriptionForModel(canonical);
    const row: AgentModelOption = { value: alias, label: getModelDisplayName(canonical) };
    if (description) row.description = description;
    // Attach the model-facing capability/cost blurb so a programmatic model choice
    // (the config tool's model section, which the agent reads) has guidance beyond
    // the raw id + human label.
    if (descriptionForModel) row.descriptionForModel = descriptionForModel;
    return row;
  });
  const current = (currentModel ?? '').trim();
  // A custom/pinned full id (not one of the alias values) rounds-trips as its own
  // head-of-list option so selecting it again preserves the exact id.
  if (current !== '' && !base.some(o => o.value === current)) {
    return [
      { value: current, label: getModelDisplayName(current), description: 'Current model (custom ID)' },
      ...base,
    ];
  }
  return base;
}

/**
 * Canonical display string for an agent / subagent model CHOICE, so every agent
 * surface (the `/agents` list + detail renderers, any model picker) formats a
 * stored `model` value the SAME way instead of printing the raw token. Mirrors
 * the reference's `getAgentModelDisplay` (utils/model/agent.ts:L124-L131): an absent
 * value is the inherit-by-default case ("Inherit from parent (default)"), the
 * explicit "inherit" sentinel is "Inherit from parent", and a concrete alias / id
 * is resolved through {@link resolveModelAlias} + {@link getModelDisplayName} to its
 * friendly name (falling back to the raw id when unrecognized). The SINGLE source
 * for rendering an agent's model choice — pairs with {@link getAgentModelOptions}
 * (the selectable catalog) so the picker and the display agree on wording.
 */
export function getAgentModelDisplay(model?: string | null): string {
  const raw = (model ?? '').trim();
  if (raw === '') return 'Inherit from parent (default)';
  if (raw.toLowerCase() === 'inherit') return 'Inherit from parent';
  const canonical = resolveModelAlias(raw) ?? raw;
  const display = getModelDisplayName(canonical);
  return display === '' ? raw : display;
}

/**
 * One row of the canonical {@link MODEL_OVERRIDES_AFFORDANCE} descriptor: the
 * settings key, the map shape, the config scopes that may declare it, whether the
 * override preserves capability resolution, and a human-readable description.
 */
export interface ModelOverridesAffordance {
  /** The settings.json key users set to declare overrides. */
  settingsKey: string;
  /** The value shape the key accepts (canonical model id → provider string). */
  valueShape: string;
  /** Config waterfall scopes that may declare the map (last-writer wins). */
  scopes: readonly string[];
  /** True: the CANONICAL id still drives every context-window / output-ceiling
   *  lookup — only the string SENT to the provider is overridden. */
  capabilityPreserving: boolean;
  /** One-paragraph human description of the affordance for the manifest. */
  description: string;
}

export function formatModelList(models: string[], provider: string): string {
  if (models.length === 0) return 'Could not retrieve model list from provider.';

  const lines = models.map(m => `  ${m}`);
  const header = `Available models (${provider}, ${models.length}):`;

  return `${header}\n${lines.join('\n')}`;
}
