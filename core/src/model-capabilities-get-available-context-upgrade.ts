// Split from model-capabilities.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { formatTokenCount } from './token-format';
import { capitalize } from './utils/string';
import { getContextWindow } from './model-capabilities-get-context-window';

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
export const ONE_MILLION_SUFFIX = /\[1m\]$/i;

/** The context window a `[1m]` upgrade grants; the baseline for the relative
 *  multiplier and the "already maxed out" short-circuit. */
export const ONE_MILLION_CONTEXT = 1_000_000;

/**
 * A larger-context model variant reachable from the current model, mirroring the
 * descriptor the reference's `getAvailableUpgrade()` returns
 * (utils/model/contextWindowUpgradeCheck.ts:L9-L27). Unlike the reference's absolute
 * window table, AMI expresses the gain as a relative {@link multiplier} so a
 * message can say "{N}x more context" from any base window.
 */
export interface ContextUpgrade {
  /** The model id/alias to switch to, carrying the `[1m]` marker (e.g. `opus[1m]`). */
  alias: string;
  /** Human-readable name of the larger-context variant (e.g. `Opus 1M`). */
  name: string;
  /** Relative context-size gain of the variant over the current window
   *  (`1M / currentWindow`, rounded, never below 2). the reference hard-codes 5; AMI
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
 * Mirrors the reference's forward-upgrade detection (`getAvailableUpgrade`,
 * utils/model/contextWindowUpgradeCheck.ts) — the inverse of
 * {@link getModelSwitchWarning}, which only warns about switching DOWN to a smaller
 * window. `currentContextLimit` (the session's resolved window) lets the caller
 * suppress the suggestion once already on a ≥1M window and makes the multiplier
 * exact; when omitted the multiplier falls back to the reference's canonical 5x.
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
 * for 3P users. Mirrors the reference's APIProvider union (utils/model/providers.ts).
 */
export const API_PROVIDER_CLASS = ['firstParty', 'bedrock', 'vertex', 'foundry'] as const;

export type ApiProviderClass = (typeof API_PROVIDER_CLASS)[number];

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
 * endpoint starts rejecting it. Mirrors the reference's DEPRECATED_MODELS registry with
 * per-provider retirement dates (utils/model/deprecation.ts:L21-L101).
 */
export const DEPRECATED_MODELS: Record<string, { modelName: string; retirementDates: Record<ApiProviderClass, string | null> }> = {
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
 * the retirement DATE varies by host. Mirrors the reference's getModelDeprecationWarning.
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
 * shared instance would leak state between calls. Mirrors the reference's
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
 * not. Mirrors the reference's `hasUltrathinkKeyword` (utils/thinking.ts:L29-L31).
 */
export function hasUltrathinkKeyword(text: string): boolean {
  if (typeof text !== 'string') return false;
  return /\bultrathink\b/i.test(text);
}
