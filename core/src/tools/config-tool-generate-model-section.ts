// Split from config-tool.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { ToolContext } from '../types';
import { canOverride, isManagedOnlyKey } from '../settings-guard';
import { listProviders, getAgentModelOptions, resolveModelAlias, validateModel } from '../model-registry';

/**
 * Metadata describing a single supported config setting. This registry is the
 * single source of truth for which keys the config tool may read/write, their
 * type, their enumerated options (if any), nested storage path, and optional
 * write-validation / read-formatting hooks. Mirrors the reference's
 * SUPPORTED_SETTINGS registry (tools/ConfigTool/supportedSettings.ts).
 */
export interface SettingConfig {
  type: 'boolean' | 'string' | 'number';
  description: string;
  /** Storage path within the config object. Defaults to key.split('.'). */
  path?: string[];
  /** Enumerated valid string values; rejection lists these back to the caller. */
  options?: readonly string[];
  /** Synchronous per-setting write validation (e.g. non-empty model). */
  validateOnWrite?: (v: unknown) => { valid: boolean; error?: string };
  /**
   * Optional ASYNC per-setting write validation, run after the synchronous
   * checks and only when the write would actually change the value. Mirrors
   * the reference's `validateOnWrite` hook (tools/ConfigTool/ConfigTool.ts): a
   * semantic/reachability check that may need I/O — e.g. confirming a `model`
   * id is actually served by the configured provider — so an invalid-but-
   * well-formed value is rejected at write time instead of only failing later
   * at inference. Receives the tool context so a validator can reach injected
   * state (e.g. the resolved provider config). When no such context is
   * available the validator should soft-allow (return valid) rather than block.
   */
  asyncValidateOnWrite?: (v: unknown, context: ToolContext) => Promise<{ valid: boolean; error?: string }>;
  /** Formats a value for display on read (e.g. undefined → "default"). */
  formatOnRead?: (v: unknown) => unknown;
}

/**
 * Async write-validator for model-valued settings (model / fallbackModel /
 * compactionModel). When the running engine has resolved provider credentials
 * (`context._providerConfig`), the requested id is checked against the provider's
 * live/known model list via the shared {@link validateModel} (the SAME check the
 * /model switch path uses — no parallel validation logic), and a bad id is
 * rejected before it is persisted, with the provider's suggestion / available
 * list surfaced so the model can self-correct. Without a provider config (bare
 * engine, offline edit, tests) it soft-allows: the synchronous non-empty guard
 * already ran, and there is no reachable list to check against. `validateModel`
 * itself also soft-allows transient network/API failures, so a blip never blocks
 * a legitimate switch.
 */
export async function validateModelOnWrite(
  v: unknown,
  context: ToolContext,
): Promise<{ valid: boolean; error?: string }> {
  const providerConfig = context._providerConfig;
  if (!providerConfig) return { valid: true };
  const modelId = String(v);
  const result = await validateModel(providerConfig, modelId);
  if (result.valid) return { valid: true };
  const parts = [`"${modelId}" is not a valid model for the configured provider.`];
  if (result.suggestion) {
    parts.push(`Did you mean "${result.suggestion}"?`);
  } else if (result.available.length > 0) {
    parts.push(`Available: ${result.available.slice(0, 20).join(', ')}.`);
  }
  if (result.reason) parts.push(result.reason);
  return { valid: false, error: parts.join(' ') };
}

export const nonEmptyString = (label: string) => (v: unknown): { valid: boolean; error?: string } =>
  typeof v === 'string' && v.trim().length > 0
    ? { valid: true }
    : { valid: false, error: `${label} must be a non-empty string.` };

export const positiveNumber = (label: string) => (v: unknown): { valid: boolean; error?: string } =>
  typeof v === 'number' && Number.isFinite(v) && v > 0
    ? { valid: true }
    : { valid: false, error: `${label} must be a positive number.` };

/**
 * Security guard against self-escalation via the model-facing config tool. The
 * config tool mutates `.ami/config.json` — i.e. it writes at the **project**
 * tier. Keys that are managed-only or tier-locked (permissionMode, maxTurns,
 * maxBudgetUsd, maxSessionMs) must not be writable from this path, or the model
 * could lift its own permission mode / budget caps. Reuses the shared
 * settings-guard tier rules (no duplicate policy). Returns a rejection reason
 * when the project tier may not set the key, else null.
 */
export function configToolWriteBlockReason(key: string): string | null {
  // canOverride(project, managed) is false for both MANAGED_ONLY and TIER_LOCKED
  // keys, which is exactly the self-escalation set we must refuse here.
  if (canOverride(key, 'project', 'managed')) return null;
  if (isManagedOnlyKey(key)) {
    return `"${key}" is a managed-only setting and cannot be changed by the config tool. It is set by your administrator's managed config.`;
  }
  // TIER_LOCKED (security-sensitive) — e.g. permissionMode self-escalation.
  return `"${key}" is a security-sensitive, tier-locked setting and cannot be changed by the config tool (it would let the session escalate its own ${key === 'permissionMode' ? 'permissions' : 'limits'}). Change it at launch (CLI flag / trusted config) instead.`;
}

/** Reads a (possibly nested) value out of the config object by path. */
export function getValueByPath(config: Record<string, unknown>, keyPath: string[]): unknown {
  let current: unknown = config;
  for (const segment of keyPath) {
    if (current && typeof current === 'object' && !Array.isArray(current) && segment in current) {
      current = (current as Record<string, unknown>)[segment];
    } else {
      return undefined;
    }
  }
  return current;
}

/** Writes a (possibly nested) value into the config object by path, in place. */
export function setValueByPath(config: Record<string, unknown>, keyPath: string[], value: unknown): void {
  let current = config;
  for (let i = 0; i < keyPath.length - 1; i++) {
    const segment = keyPath[i];
    const existing = current[segment];
    if (!existing || typeof existing !== 'object' || Array.isArray(existing)) {
      current[segment] = {};
    }
    current = current[segment] as Record<string, unknown>;
  }
  current[keyPath[keyPath.length - 1]] = value;
}

/** Deletes a (possibly nested) key, pruning now-empty parent objects. */
export function deleteValueByPath(config: Record<string, unknown>, keyPath: string[]): void {
  if (keyPath.length === 1) {
    delete config[keyPath[0]];
    return;
  }
  const parent = getValueByPath(config, keyPath.slice(0, -1));
  if (parent && typeof parent === 'object' && !Array.isArray(parent)) {
    delete (parent as Record<string, unknown>)[keyPath[keyPath.length - 1]];
    if (Object.keys(parent).length === 0) deleteValueByPath(config, keyPath.slice(0, -1));
  }
}

/** Coerce an incoming value to the setting's declared type. */
export function coerceValue(type: SettingConfig['type'], value: unknown): { value?: unknown; error?: string } {
  if (type === 'boolean') {
    if (typeof value === 'boolean') return { value };
    if (typeof value === 'string') {
      const lower = value.toLowerCase().trim();
      if (lower === 'true') return { value: true };
      if (lower === 'false') return { value: false };
    }
    return { error: 'requires true or false.' };
  }
  if (type === 'number') {
    if (typeof value === 'number' && Number.isFinite(value)) return { value };
    if (typeof value === 'string' && value.trim() !== '' && Number.isFinite(Number(value))) {
      return { value: Number(value) };
    }
    return { error: 'requires a number.' };
  }
  // string
  if (typeof value === 'string') return { value };
  if (typeof value === 'number' || typeof value === 'boolean') return { value: String(value) };
  return { error: 'requires a string.' };
}

/**
 * Builds the `## Model` section appended to the tool description, enumerating the
 * known-good model ids per provider plus the short aliases, so the model has a
 * concrete list of valid values to pick when it wants to change the `model` key
 * (mirrors the reference's generateModelSection, which special-cases the model setting
 * with a per-value listing and a try/catch static fallback). The provider lister
 * is injected so a registry failure degrades to a static fallback line rather
 * than breaking the whole tool description. Uses the SYNCHRONOUS static registry
 * (listProviders) — never a network round-trip — because the description is built
 * once at module load.
 */
export function generateModelSection(
  listFn: () => Array<{ id: string; models: string[] }> = listProviders,
): string {
  try {
    const providers = listFn();
    const providerLines = providers
      .filter((p) => p.models.length > 0)
      .map((p) => `  - ${p.id}: ${p.models.join(', ')}`);
    // Derive the alias listing from the shared agent-model catalog so the tool
    // description offers exactly the aliases (with friendly labels) the model
    // picker does — one source, no hand-maintained copy.
    const aliasLines = getAgentModelOptions().map((o) => {
      const target = resolveModelAlias(o.value);
      const head = target ? `${o.value} -> ${target} (${o.label})` : `${o.value} (${o.label})`;
      // Append the model-facing capability/cost guidance so the agent, when it sets
      // the `model` key for a sub-task, can reason over the trade-off instead of
      // choosing blind from a bare id + label (parity: ModelOption.descriptionForModel).
      return o.descriptionForModel ? `${head}: ${o.descriptionForModel}` : head;
    });
    return (
      '\n\n## Model\n' +
      'Set the "model" key to a provider model id below, or one of these aliases, ' +
      'or "default" to use the built-in default.\n' +
      providerLines.join('\n') +
      `\nAliases: ${aliasLines.join(', ')}`
    );
  } catch {
    // Registry resolution failed — degrade to a static pointer at /model so the
    // tool description still renders and the model knows where to find the list.
    return (
      '\n\n## Model\n' +
      'Set the "model" key to any provider model id (run the /model command for the live list), ' +
      'or "default" to use the built-in default.'
    );
  }
}

export function buildExamples(): string {
  return (
    '\n\nExamples:\n' +
    '  - Read all settings: action="get"\n' +
    '  - Read one setting: action="get" key="model"\n' +
    '  - Set a model: action="set" key="model" value="claude-opus-4-6"\n' +
    '  - Enable a boolean: action="set" key="safeMode" value="true"\n' +
    '  - Set a number: action="set" key="maxTurns" value="40"\n' +
    '  - Set in the global store: action="set" key="model" value="opus" scope="global"\n' +
    '  - Reset to default: action="set" key="model" value="default"'
  );
}

export const CONFIG_REL = ['.ami', 'config.json'];
