import './env-protection';
import './plugin-only-policy';
import { DANGEROUS_SHELL_SETTINGS, SETTINGS_TIERS } from './settings-guard-validate-managed-policy-values';
import type { SettingsSource, SettingsTier } from './settings-guard-validate-managed-policy-values';
import { dottedAncestors } from './settings-guard-settings-schema-property';
import type { SettingsSchemaProperty } from './settings-guard-settings-schema-property';
export { DANGEROUS_SHELL_SETTINGS, SANDBOX_POLICY_LOCK_MESSAGE, SETTINGS_TIERS, areSandboxSettingsLockedByPolicy, extractDangerousSettings, formatDangerousSettingsList, getManagedSettingsSources, getSettingsTierDisplayName, getSettingsTierDisplayNameLowercase, getSettingsTierLabels, getSettingsTierShortName, hasDangerousSettings, hasDangerousSettingsChanged, isAllHooksDisabled, isBypassPermissionsModeDisabled, isManagedHooksOnly, managedStatusSuffix, shouldAllowManagedSandboxDomainsOnly, validateManagedPolicyValues } from './settings-guard-validate-managed-policy-values';
export type { DangerousSettings, DangerousSettingsInput, DangerousShellSetting, ManagedPolicyWarning, ManagedSettingsSource, SandboxPolicyLockInput, SettingsSource, SettingsTier } from './settings-guard-validate-managed-policy-values';
export type { SettingsSchemaProperty } from './settings-guard-settings-schema-property';

export const MANAGED_ONLY_KEYS = new Set([
  'allowManagedModsOnly',
  'allowedMcpServers',
  'managedMcpServers',
  'prependPlugins',
  'appendPlugins',
  'enabledPlugins',
  'strictKnownMarketplaces',
  'processWrapper',
  'allowModsToOverrideDenyRules',
  'disableBypassPermissionsMode',
  'disableAllHooks',
  'allowManagedHooksOnly',
  // Enterprise lockdown that confines permission rules to admin/managed-sourced
  // ones only (user/project self-authorization is dropped and "always allow"
  // persistence is refused). Like its `allowManaged*Only` siblings this is an
  // admin policy, so it is managed-only — a project/local/env/cli file setting it
  // would let the restricted tier grant itself the lockdown. Parity: the reference
  // Code's `allowManagedPermissionRulesOnly` policy.
  'allowManagedPermissionRulesOnly',
  // Per-surface policy that locks customization (skills/agents/hooks/mcp) to
  // plugin-provided sources only, blocking user/project tiers. An admin lockdown,
  // so it is managed-only — a lower tier must not be able to declare (or opt out
  // of) the restriction on itself. Parity: the reference's
  // `strictPluginOnlyCustomization` policy.
  'strictPluginOnlyCustomization',
  // Org opt-in gate for the MCP "channels" notification feature. Teams/enterprise
  // orgs must explicitly enable it via `channelsEnabled: true` in managed settings
  // — it is an admin trust/availability decision, so a project/local/env/cli file
  // must not be able to self-enable the feature. Parity: the reference's
  // `channelsEnabled` managed-settings policy gate.
  'channelsEnabled',
  // Org-owned allowlist of channel plugins that, when set in managed settings,
  // REPLACES the local trust ledger (the admin owns the trust decision). A lower
  // tier setting it would let the restricted scope grant itself trust, so it is
  // managed-only. Parity: the reference's `allowedChannelPlugins` managed setting.
  'allowedChannelPlugins',
  // Admin lockdown that forces the container sandbox to permit ONLY managed/approved
  // network domains — a project/local/env/cli tier must not be able to relax the
  // egress boundary on itself, so it is managed-only. Parity: the reference's
  // `shouldAllowManagedSandboxDomainsOnly()` managed-policy knob.
  'allowManagedSandboxDomainsOnly',
  // Enterprise exclusive-control switch: when set, ONLY admin-provisioned
  // (`managedMcpServers`, enterprise scope) MCP servers may run — every user /
  // project / local / dynamic server is blocked. Like its `allowManaged*Only`
  // siblings this is an admin lockdown, so a lower tier must not be able to set it
  // on itself. Parity: the reference's `allowManagedMcpServersOnly` policy.
  'allowManagedMcpServersOnly',
]);

export const TIER_LOCKED_KEYS = new Set([
  'permissionMode',
  'maxTurns',
  'maxBudgetUsd',
  'maxSessionMs',
]);

// ---------------------------------------------------------------------------
// Nested-key (sub-key) granularity for governed settings (parity: the reference's
// settings.ts validNestedKeys — permissions/sandbox/hooks are expanded ONE level
// deep so a sub-path like `permissions.deny` or `sandbox.allowUnsandboxedCommands`
// is governed INDEPENDENTLY of its siblings). AMI's flat {@link MANAGED_ONLY_KEYS}
// / {@link TIER_LOCKED_KEYS} could only lock/manage an ENTIRE top-level key — it
// could not lock `permissions.deny` while leaving `permissions.allow` overridable.
// These dotted sets add that granularity; the {@link isManagedOnlyKey} /
// {@link isTierLockedKey} predicates below unify flat + nested + ancestor lookup so
// every guard entry point (canOverride / getEffectiveTier / filterManagedKeys /
// SettingsGuard / the config-tool write guard) honors sub-key governance.
// ---------------------------------------------------------------------------

/**
 * Dotted sub-keys that are admin/managed-only even though their top-level
 * container (`permissions`, `sandbox`) is itself overridable. Each weakens a
 * security boundary — the bypass-permissions escape hatch and the two sandbox
 * exemption toggles — so a project/local/env/cli tier must not be able to set it
 * on itself, mirroring the top-level {@link MANAGED_ONLY_KEYS}. Parity: the reference's
 * nested `permissions.disableBypassPermissionsMode` /
 * `sandbox.allowUnsandboxedCommands` managed sub-keys.
 */
export const MANAGED_ONLY_NESTED_KEYS = new Set<string>([
  'permissions.disableBypassPermissionsMode',
  'sandbox.allowUnsandboxedCommands',
  'sandbox.autoAllowBashIfSandboxed',
]);

/**
 * Dotted sub-keys that are tier-locked: a lower-priority tier cannot override the
 * value a higher tier set, but (unlike {@link MANAGED_ONLY_NESTED_KEYS}) any tier
 * may be the authoritative one. `permissions.deny` is the canonical case — a
 * higher tier's deny-list must not be shadowable by a lower tier editing only the
 * `deny` sub-key. Parity: the reference's nested-key handling for `permissions.deny`.
 */
export const TIER_LOCKED_NESTED_KEYS = new Set<string>([
  'permissions.deny',
]);

/**
 * True when `key` is governed as managed-only — directly (flat {@link
 * MANAGED_ONLY_KEYS} or nested {@link MANAGED_ONLY_NESTED_KEYS}) or by inheritance
 * from a managed-only dotted ancestor. For a flat key this is exactly
 * `MANAGED_ONLY_KEYS.has(key)`, so existing top-level behavior is unchanged.
 */
export function isManagedOnlyKey(key: string): boolean {
  if (MANAGED_ONLY_KEYS.has(key) || MANAGED_ONLY_NESTED_KEYS.has(key)) return true;
  return dottedAncestors(key).some(
    p => MANAGED_ONLY_KEYS.has(p) || MANAGED_ONLY_NESTED_KEYS.has(p),
  );
}

/**
 * True when `key` is tier-locked — directly (flat {@link TIER_LOCKED_KEYS} or
 * nested {@link TIER_LOCKED_NESTED_KEYS}) or by inheritance from a tier-locked
 * dotted ancestor. For a flat key this is exactly `TIER_LOCKED_KEYS.has(key)`, so
 * existing top-level behavior is unchanged.
 */
export function isTierLockedKey(key: string): boolean {
  if (TIER_LOCKED_KEYS.has(key) || TIER_LOCKED_NESTED_KEYS.has(key)) return true;
  return dottedAncestors(key).some(
    p => TIER_LOCKED_KEYS.has(p) || TIER_LOCKED_NESTED_KEYS.has(p),
  );
}

/** The generated settings JSON Schema document (draft-07, non-strict). */
export interface SettingsJSONSchema {
  $schema: string;
  title: string;
  type: 'object';
  properties: Record<string, SettingsSchemaProperty>;
  /** Non-strict: unknown settings keys are allowed (this is a partial, governed schema). */
  additionalProperties: boolean;
}

/** Managed-only keys whose value is a `boolean` policy kill-switch/flag. */
const BOOLEAN_MANAGED_KEYS: ReadonlySet<string> = new Set([
  'allowManagedModsOnly',
  'strictKnownMarketplaces',
  'allowModsToOverrideDenyRules',
  'disableBypassPermissionsMode',
  'disableAllHooks',
  'allowManagedHooksOnly',
  'allowManagedPermissionRulesOnly',
  'channelsEnabled',
  'allowManagedSandboxDomainsOnly',
  'allowManagedMcpServersOnly',
]);

/** Managed-only keys whose value is a list of strings. */
const STRING_ARRAY_MANAGED_KEYS: ReadonlySet<string> = new Set([
  'allowedMcpServers',
  'prependPlugins',
  'appendPlugins',
  'enabledPlugins',
]);

/** Managed-only keys whose value is a list of objects. */
const OBJECT_ARRAY_MANAGED_KEYS: ReadonlySet<string> = new Set([
  'allowedChannelPlugins',
]);

/** Managed-only keys whose value is an object/map. */
const OBJECT_MANAGED_KEYS: ReadonlySet<string> = new Set([
  'managedMcpServers',
  'processWrapper',
]);

/** Known permission modes, surfaced as the `permissionMode` enum. */
const PERMISSION_MODE_VALUES = ['ask', 'auto-allow', 'deny-all', 'plan', 'bypassPermissions'] as const;

/** Build the schema node for a single managed-only key from its classification. */
function managedKeyProperty(key: string): SettingsSchemaProperty {
  if (BOOLEAN_MANAGED_KEYS.has(key)) {
    return { type: 'boolean', description: `Managed-only policy flag "${key}" (set only in enterprise managed settings).`, managed: true };
  }
  if (STRING_ARRAY_MANAGED_KEYS.has(key)) {
    return { type: 'array', items: { type: 'string' }, description: `Managed-only policy list "${key}" (set only in enterprise managed settings).`, managed: true };
  }
  if (OBJECT_ARRAY_MANAGED_KEYS.has(key)) {
    return { type: 'array', items: { type: 'object' }, description: `Managed-only policy list "${key}" of structured entries (set only in enterprise managed settings).`, managed: true };
  }
  if (OBJECT_MANAGED_KEYS.has(key)) {
    return { type: 'object', description: `Managed-only policy map "${key}" (set only in enterprise managed settings).`, managed: true };
  }
  // Union-typed or future keys (e.g. strictPluginOnlyCustomization: boolean | string[])
  // are left permissive (no `type`) so a valid value of either shape validates and a
  // newly-added managed key never breaks the schema. Parity with the reference's
  // `unrepresentable: 'any'` fallback.
  return { description: `Managed-only policy key "${key}" (set only in enterprise managed settings).`, managed: true };
}

/** Build the schema node for a tier-locked key. */
function tierLockedKeyProperty(key: string): SettingsSchemaProperty {
  if (key === 'permissionMode') {
    return { type: 'string', enum: [...PERMISSION_MODE_VALUES], description: 'Permission mode. Tier-locked: a lower-priority tier cannot override a higher one.', tierLocked: true };
  }
  return { type: 'number', description: `Resource budget "${key}". Tier-locked: a lower-priority tier cannot override a higher one.`, tierLocked: true };
}

/**
 * Generate a (draft-07) JSON Schema for the policy keys AMI governs, derived from
 * this module's authoritative key sets so it self-updates and never drops a key.
 * Managed-only keys are flagged `managed`, tier-locked keys `tierLocked`, dangerous
 * shell-executing settings are typed `string` with a danger note. Non-strict
 * (`additionalProperties: true`) — it describes the governed subset, not every
 * possible settings key. Parity: the reference's `generateSettingsJSONSchema()`.
 */
export function getSettingsJSONSchema(): SettingsJSONSchema {
  const properties: Record<string, SettingsSchemaProperty> = {};

  // Deterministic order: sort each governed set so the emitted schema is stable.
  for (const key of [...MANAGED_ONLY_KEYS].sort()) {
    properties[key] = managedKeyProperty(key);
  }
  for (const key of [...TIER_LOCKED_KEYS].sort()) {
    // A tier-locked key already added as managed-only keeps its managed flag but
    // gains the tierLocked flag (the two policies compose).
    properties[key] = { ...tierLockedKeyProperty(key), ...(properties[key]?.managed ? { managed: true } : {}) };
  }
  for (const key of [...DANGEROUS_SHELL_SETTINGS].sort()) {
    properties[key] = {
      type: 'string',
      description: `Shell command executed by the harness for "${key}" — dangerous when sourced from managed/remote settings (arbitrary code execution).`,
    };
  }

  return {
    $schema: 'http://json-schema.org/draft-07/schema#',
    title: 'AMI settings (governed policy keys)',
    type: 'object',
    properties,
    additionalProperties: true,
  };
}

// ---------------------------------------------------------------------------
// Two precedence scales (AMI deliberately governs normal and locked keys with
// OPPOSITE authority). The original single TIER_PRIORITY map exploited the fact
// that `managed` sat at one extreme; adding the `plugin` BASE tier breaks that
// coincidence (plugin must be the weakest in BOTH senses), so the two scales are
// now explicit:
//
//   - NORMAL_PRIORITY — for ordinary (non-governed) keys the LAST-wins cascade:
//     a HIGHER number overrides a lower one. plugin(0) is the base default layer;
//     cli(7) wins. managed sits just above the plugin base (its non-policy values
//     are defaults a user file overrides) — unchanged from before.
//   - LOCK_PRIORITY — for tier-locked keys the authority order: a LOWER number is
//     MORE authoritative (managed(0) can never be overridden). plugin(7) is the
//     LEAST authoritative, so a plugin-seeded locked value yields to every tier
//     and a plugin can never lock a key against a higher tier. `flag` is the most
//     authoritative non-policy source (the reference: flagSettings sits just below policy).
//
// For the pre-existing tiers (managed/global/project/local/env/cli) the relative
// order of BOTH scales is identical to the old single map, so prior behavior is
// unchanged; plugin + flag are the only additions.
// ---------------------------------------------------------------------------

const NORMAL_PRIORITY: Record<SettingsTier, number> = {
  plugin: 0,
  managed: 1,
  global: 2,
  project: 3,
  local: 4,
  flag: 5,
  env: 6,
  cli: 7,
};

const LOCK_PRIORITY: Record<SettingsTier, number> = {
  managed: 0,
  flag: 1,
  global: 2,
  project: 3,
  local: 4,
  env: 5,
  cli: 6,
  plugin: 7,
};

/**
 * The settings tiers in explicit LOW→HIGH merge order (the base `plugin` layer
 * first, `cli` last), i.e. a later tier's ordinary value overrides an earlier
 * one's. This is the documented cascade ordering surfaced via `/settings-guard`
 * and `ami --introspect` so the precedence is machine- and human-readable rather
 * than implied by code. Parity: the reference's SETTING_SOURCES low→high ordering.
 */
export function getSettingsLayerOrder(): SettingsTier[] {
  return [...SETTINGS_TIERS].sort((a, b) => NORMAL_PRIORITY[a] - NORMAL_PRIORITY[b]);
}

export function canOverride(key: string, source: SettingsTier, existing: SettingsTier): boolean {
  // Nested-aware: a dotted sub-key (or a sub-key of a managed/locked container) is
  // governed exactly like a flat key — see {@link isManagedOnlyKey}/{@link
  // isTierLockedKey}. For a flat key these reduce to the original Set.has checks,
  // so top-level behavior is identical.
  if (isManagedOnlyKey(key) && source !== 'managed') return false;
  if (isTierLockedKey(key) && LOCK_PRIORITY[source] > LOCK_PRIORITY[existing]) return false;
  return true;
}

export function getEffectiveTier(key: string, sources: SettingsSource[]): SettingsTier | null {
  if (sources.length === 0) return null;
  if (isManagedOnlyKey(key)) {
    const managed = sources.find(s => s.tier === 'managed');
    return managed ? managed.tier : null;
  }
  // `sources` is guaranteed non-empty here (the length-0 case returned above),
  // so `sources[0]` is a safe seed and `best` is never null — no `?? null` arm.
  if (isTierLockedKey(key)) {
    // Tier-locked: the MOST authoritative (lowest LOCK_PRIORITY) source wins.
    let best = sources[0];
    for (const s of sources) {
      if (LOCK_PRIORITY[s.tier] < LOCK_PRIORITY[best.tier]) {
        best = s;
      }
    }
    return best.tier;
  }
  // Normal key: the highest-precedence (highest NORMAL_PRIORITY) source wins.
  let best = sources[0];
  for (const s of sources) {
    if (NORMAL_PRIORITY[s.tier] > NORMAL_PRIORITY[best.tier]) {
      best = s;
    }
  }
  return best.tier;
}

/** Disk-merged settings paired with the runtime-resolved values actually used. */
export interface EffectiveAppliedSettings<E extends Record<string, unknown>, A extends Record<string, unknown>> {
  /** The pure disk/tier merge (what the settings FILES resolve to). */
  effective: E;
  /** The merge with runtime overrides overlaid (model/effort after env, session, defaults). */
  applied: E & A;
}

/**
 * Split the two-phase settings resolution the reference's SDK `get_settings` exposes: the
 * disk-merged `effective` config versus the runtime-`applied` values actually sent
 * to the API after env overrides / session state / model-specific defaults are
 * layered on top. `effective` is returned untouched (not mutated); `applied` is a
 * shallow overlay of `runtime` over `effective` so a caller can show "configured
 * X, running with Y". Parity: the reference's effective≠applied distinction.
 */
export function resolveAppliedSettings<E extends Record<string, unknown>, A extends Record<string, unknown>>(
  effective: E,
  runtime: A,
): EffectiveAppliedSettings<E, A> {
  return { effective, applied: { ...effective, ...runtime } };
}

export function filterManagedKeys(config: Record<string, unknown>, sourceTier: SettingsTier): { filtered: Record<string, unknown>; rejected: string[] } {
  const filtered: Record<string, unknown> = {};
  const rejected: string[] = [];
  for (const [key, value] of Object.entries(config)) {
    if (MANAGED_ONLY_KEYS.has(key) && sourceTier !== 'managed') {
      rejected.push(key);
      continue;
    }
    // Nested granularity: a top-level CONTAINER (e.g. `permissions`, `sandbox`) is
    // not itself managed-only, but may hold managed-only SUB-keys. From a non-managed
    // source, drop just those dotted sub-keys and keep the valid siblings, rather than
    // passing the whole object through (which would let a project self-set e.g.
    // `sandbox.allowUnsandboxedCommands`). Parity: the reference's one-level nested-key
    // expansion of permissions/sandbox/hooks.
    if (
      sourceTier !== 'managed' &&
      value !== null &&
      typeof value === 'object' &&
      !Array.isArray(value)
    ) {
      const nested = filterNestedManagedKeys(key, value as Record<string, unknown>);
      if (nested.rejected.length > 0) {
        filtered[key] = nested.kept;
        rejected.push(...nested.rejected);
        continue;
      }
    }
    filtered[key] = value;
  }
  return { filtered, rejected };
}

/**
 * Drop the managed-only dotted sub-keys of a single container object, returning the
 * kept remainder and the dotted names rejected (e.g. `sandbox.allowUnsandboxedCommands`).
 * Only descends ONE level (parity with the reference's nested-key expansion); deeper
 * structure is left intact.
 */
function filterNestedManagedKeys(
  containerKey: string,
  container: Record<string, unknown>,
): { kept: Record<string, unknown>; rejected: string[] } {
  const kept: Record<string, unknown> = {};
  const rejected: string[] = [];
  for (const [subKey, subValue] of Object.entries(container)) {
    const dotted = `${containerKey}.${subKey}`;
    if (isManagedOnlyKey(dotted)) {
      rejected.push(dotted);
    } else {
      kept[subKey] = subValue;
    }
  }
  return { kept, rejected };
}

export class SettingsGuard {
  private _blockedOverrides: Array<{ key: string; source: SettingsTier; reason: string }> = [];

  checkOverride(key: string, source: SettingsTier, existing: SettingsTier): boolean {
    if (canOverride(key, source, existing)) return true;
    // Enrich the block reason with an ACTIONABLE remedy so a frontend can tell the
    // user not just that the write was refused but what to do about it. Managed-only
    // keys are org policy (remedy: contact the administrator); tier-locked keys name
    // the tier the write must target instead. The structured {key,source,reason}
    // shape is preserved so each UI can still render it however it likes. Parity:
    // the reference's managed/policy-locked rule message ("configured by managed
    // settings and cannot be modified — contact your system administrator").
    const reason = isManagedOnlyKey(key)
      ? `"${key}" is managed by your organization and cannot be set here — contact your administrator.`
      : `"${key}" is locked at tier "${existing}" and cannot be overridden by "${source}" — set it at the "${existing}" tier or higher.`;
    this._blockedOverrides.push({ key, source, reason });
    return false;
  }

  getBlockedOverrides(): ReadonlyArray<{ key: string; source: SettingsTier; reason: string }> {
    return this._blockedOverrides;
  }

  getBlockedCount(): number {
    return this._blockedOverrides.length;
  }

  clear(): void {
    this._blockedOverrides = [];
  }
}
