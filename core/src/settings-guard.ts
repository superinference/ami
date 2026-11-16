import { isDangerousSettingsEnvVar } from './env-protection';
import { CUSTOMIZATION_SURFACES } from './plugin-only-policy';

export const SETTINGS_TIERS = ['managed', 'global', 'project', 'local', 'env', 'cli'] as const;
export type SettingsTier = (typeof SETTINGS_TIERS)[number];

// ---------------------------------------------------------------------------
// Human-readable setting-source display names (parity: Claude's
// utils/settings/constants.ts getSourceDisplayName / getSettingSourceName /
// getSettingSourceDisplayNameLowercase / …Capitalized). A raw tier token
// ('managed', 'local', …) is fine for machine logic but unreadable in a UI, a
// `/doctor` finding, or a blocked-override message. These mappers turn each tier
// into prose ("Enterprise managed settings", "project local settings", …) so a
// user reads WHERE a setting came from instead of an opaque token. There is ONE
// canonical table; the three accessors are thin lenses over it.
// ---------------------------------------------------------------------------

/** Canonical prose for each {@link SettingsTier}, in three register variants. */
const SETTINGS_TIER_DISPLAY: Record<
  SettingsTier,
  { short: string; lower: string; capitalized: string }
> = {
  managed: { short: 'Managed', lower: 'enterprise managed settings', capitalized: 'Enterprise managed settings' },
  global: { short: 'User', lower: 'user settings', capitalized: 'User settings' },
  project: { short: 'Project', lower: 'shared project settings', capitalized: 'Shared project settings' },
  local: { short: 'Local', lower: 'project local settings', capitalized: 'Project local settings' },
  env: { short: 'Env', lower: 'environment variables', capitalized: 'Environment variables' },
  cli: { short: 'CLI', lower: 'command line arguments', capitalized: 'Command line arguments' },
};

/** Short capitalized label for a tier ('Managed'/'User'/'Project'/…) — compact UI. */
export function getSettingsTierShortName(tier: SettingsTier): string {
  return SETTINGS_TIER_DISPLAY[tier].short;
}

/** Full capitalized prose for a tier ('Enterprise managed settings', …) — UI labels. */
export function getSettingsTierDisplayName(tier: SettingsTier): string {
  return SETTINGS_TIER_DISPLAY[tier].capitalized;
}

/** Full lowercase prose for a tier ('enterprise managed settings', …) — inline use. */
export function getSettingsTierDisplayNameLowercase(tier: SettingsTier): string {
  return SETTINGS_TIER_DISPLAY[tier].lower;
}

/**
 * Every tier paired with its prose labels, in merge-priority order
 * (managed → global → project → local → env → cli). Surfaced in `/settings-guard`
 * introspection and `ami --introspect` so a user/admin can see the labeled,
 * priority-ordered source cascade rather than raw tokens.
 */
export function getSettingsTierLabels(): Array<{
  tier: SettingsTier;
  short: string;
  lower: string;
  capitalized: string;
}> {
  return SETTINGS_TIERS.map(tier => ({
    tier,
    short: getSettingsTierShortName(tier),
    lower: getSettingsTierDisplayNameLowercase(tier),
    capitalized: getSettingsTierDisplayName(tier),
  }));
}

// ---------------------------------------------------------------------------
// Dangerous-settings classifier (parity: Claude's ManagedSettingsSecurityDialog
// utils.ts + managedEnvConstants.ts). Managed / remote / untrusted settings can
// carry knobs that execute arbitrary shell (statusLine, auth helpers), inject
// dangerous env vars, or register hooks. Before such settings are trusted, the
// dangerous subset is extracted so the user can be prompted — and we re-prompt
// only when that subset CHANGES, not on every load.
// ---------------------------------------------------------------------------

/**
 * Settings keys whose value is a shell command the harness will execute. Coming
 * from an untrusted/managed source, each is an arbitrary-code-execution vector
 * and must be surfaced for approval.
 */
export const DANGEROUS_SHELL_SETTINGS = [
  'statusLine',
  'apiKeyHelper',
  'awsAuthRefresh',
  'awsCredentialExport',
  'gcpAuthRefresh',
  'otelHeadersHelper',
] as const;
export type DangerousShellSetting = (typeof DANGEROUS_SHELL_SETTINGS)[number];

/** Structural shape of a loaded settings object relevant to danger extraction. */
export interface DangerousSettingsInput {
  env?: Record<string, unknown>;
  hooks?: unknown;
  [key: string]: unknown;
}

export interface DangerousSettings {
  /** Dangerous shell-executing settings present, by key → command. */
  shellSettings: Partial<Record<DangerousShellSetting, string>>;
  /** Env vars present that are NOT in the SAFE_ENV_VARS allowlist, by key → value. */
  envVars: Record<string, string>;
  /** Whether the settings register any hooks. */
  hasHooks: boolean;
  /** The raw hooks value, present only when hasHooks. */
  hooks?: unknown;
}

/**
 * Extract the dangerous subset of a settings object: shell-executing knobs,
 * non-safe env vars, and hooks. Any env var not in SAFE_ENV_VARS is dangerous.
 */
export function extractDangerousSettings(
  settings: DangerousSettingsInput | null | undefined,
): DangerousSettings {
  if (!settings) {
    return { shellSettings: {}, envVars: {}, hasHooks: false };
  }

  const shellSettings: Partial<Record<DangerousShellSetting, string>> = {};
  for (const key of DANGEROUS_SHELL_SETTINGS) {
    const value = settings[key];
    if (typeof value === 'string' && value.length > 0) {
      shellSettings[key] = value;
    }
  }

  const envVars: Record<string, string> = {};
  if (settings.env && typeof settings.env === 'object') {
    for (const [key, value] of Object.entries(settings.env)) {
      if (typeof value === 'string' && value.length > 0 && isDangerousSettingsEnvVar(key)) {
        envVars[key] = value;
      }
    }
  }

  const hasHooks =
    settings.hooks != null &&
    typeof settings.hooks === 'object' &&
    Object.keys(settings.hooks as object).length > 0;

  return { shellSettings, envVars, hasHooks, hooks: hasHooks ? settings.hooks : undefined };
}

/** True when any dangerous setting is present. */
export function hasDangerousSettings(dangerous: DangerousSettings): boolean {
  return (
    Object.keys(dangerous.shellSettings).length > 0 ||
    Object.keys(dangerous.envVars).length > 0 ||
    dangerous.hasHooks
  );
}

/**
 * True when `newSettings` introduces or changes a dangerous setting versus
 * `oldSettings`. No dangerous settings in the new state → never prompt. New
 * dangerous where there were none → prompt. Otherwise a deep JSON compare of the
 * dangerous subset (shellSettings/envVars/hooks) decides.
 */
export function hasDangerousSettingsChanged(
  oldSettings: DangerousSettingsInput | null | undefined,
  newSettings: DangerousSettingsInput | null | undefined,
): boolean {
  const oldDangerous = extractDangerousSettings(oldSettings);
  const newDangerous = extractDangerousSettings(newSettings);

  if (!hasDangerousSettings(newDangerous)) return false;
  if (!hasDangerousSettings(oldDangerous)) return true;

  const serialize = (d: DangerousSettings): string =>
    JSON.stringify({ shellSettings: d.shellSettings, envVars: d.envVars, hooks: d.hooks });
  return serialize(oldDangerous) !== serialize(newDangerous);
}

/**
 * Human-readable list of dangerous setting NAMES (never values) for a UI prompt.
 */
export function formatDangerousSettingsList(dangerous: DangerousSettings): string[] {
  const items: string[] = [];
  for (const key of Object.keys(dangerous.shellSettings)) items.push(key);
  for (const key of Object.keys(dangerous.envVars)) items.push(key);
  if (dangerous.hasHooks) items.push('hooks');
  return items;
}

export interface SettingsSource {
  tier: SettingsTier;
  path?: string;
}

/** A managed-settings origin: an on-disk location paired with a prose label. */
export interface ManagedSettingsSource {
  path: string;
  label: string;
}

/**
 * The managed-settings sources AMI reads, in priority order (highest-precedence
 * first). Drop-ins under `<managedBase>/config.d/*.json` layer OVER the base file
 * (last-wins in `loadManagedConfig`), so they outrank it. Paired with prose labels
 * so `/settings-guard` / `ami --introspect` can show the labeled, ordered cascade
 * instead of a single opaque "managed" tier. Parity: Claude's getMacOSPlistPaths()
 * labeled, priority-ordered managed-source list (adapted to AMI's Linux layout).
 */
export function getManagedSettingsSources(basePath: string, dropInDir: string): ManagedSettingsSource[] {
  return [
    { path: dropInDir, label: 'managed settings drop-ins' },
    { path: basePath, label: 'system-wide managed settings' },
  ];
}

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
  // would let the restricted tier grant itself the lockdown. Parity: Claude
  // Code's `allowManagedPermissionRulesOnly` policy.
  'allowManagedPermissionRulesOnly',
  // Per-surface policy that locks customization (skills/agents/hooks/mcp) to
  // plugin-provided sources only, blocking user/project tiers. An admin lockdown,
  // so it is managed-only — a lower tier must not be able to declare (or opt out
  // of) the restriction on itself. Parity: Claude Code's
  // `strictPluginOnlyCustomization` policy.
  'strictPluginOnlyCustomization',
]);

/**
 * True when a merged config forbids the bypass/auto-allow ("YOLO") permission
 * modes via the managed `disableBypassPermissionsMode` killswitch. The flag is a
 * {@link MANAGED_ONLY_KEYS} entry, so by the time a config is merged (see
 * `mergeConfigs`, which copies it ONLY from the managed tier) a truthy value here
 * provably originated from admin/managed policy — a project/local/env/cli file
 * can never set it. Parity: Claude Code's `isBypassPermissionsModeDisabled()`.
 */
export function isBypassPermissionsModeDisabled(
  config: { disableBypassPermissionsMode?: unknown } | null | undefined,
): boolean {
  return config?.disableBypassPermissionsMode === true;
}

/**
 * True when a merged config disables ALL hook execution via the managed
 * `disableAllHooks` kill-switch (every event — preToolUse/postToolUse/
 * sessionStart/… including status-line/file-suggestion hooks). The flag is a
 * {@link MANAGED_ONLY_KEYS} entry, so a truthy value in a merged config provably
 * originated from the managed tier (`mergeConfigs` copies it ONLY from managed) —
 * a project/local/env/cli file can never set it. Parity: Claude Code's
 * `disableAllHooks` policy setting.
 */
export function isAllHooksDisabled(
  config: { disableAllHooks?: unknown } | null | undefined,
): boolean {
  return config?.disableAllHooks === true;
}

/**
 * True when a merged config restricts hook execution to admin/managed-sourced
 * hooks only via the managed `allowManagedHooksOnly` policy (user/project/plugin
 * hooks blocked). A {@link MANAGED_ONLY_KEYS} entry, so by the time a config is
 * merged a truthy value here provably came from the managed tier. Parity: Claude
 * Code's `allowManagedHooksOnly` policy setting.
 */
export function isManagedHooksOnly(
  config: { allowManagedHooksOnly?: unknown } | null | undefined,
): boolean {
  return config?.allowManagedHooksOnly === true;
}

/**
 * Managed slice relevant to the sandbox-exemption policy lock. These sub-settings
 * weaken the container sandbox, so when an admin/managed config owns ANY of them a
 * lower (project/local/env/cli) tier must not be able to add or relax its own.
 */
export interface SandboxPolicyLockInput {
  /** Top-level command-exemption list (bash commands allowed to skip the sandbox). */
  sandboxExcludedCommands?: unknown;
  sandbox?: {
    /** 'off' | 'container' — turning the sandbox off is itself an exemption. */
    mode?: unknown;
    /** Auto-approve bash inside the container (skips the permission prompt). */
    autoAllowBashIfSandboxed?: unknown;
    /** Gate on the `dangerouslyDisableSandbox` escape hatch. */
    allowUnsandboxedCommands?: unknown;
  } | null;
}

/**
 * True when sandbox-exemption settings are locked by admin/managed policy: the
 * MANAGED config pins any of `sandboxExcludedCommands`, `sandbox.mode`,
 * `sandbox.autoAllowBashIfSandboxed`, or `sandbox.allowUnsandboxedCommands`. A
 * local change (e.g. a `/sandbox exclude` write) must then be refused with
 * "overridden by a higher-priority configuration and cannot be changed locally",
 * because `mergeConfigs` makes managed sandbox settings authoritative and a local
 * edit would be silently ineffective. Pass the MANAGED config slice specifically
 * (the admin-sourced tier), mirroring Claude's check of the policy/flag sources.
 * Parity: Claude Code's `SandboxManager.areSandboxSettingsLockedByPolicy()`.
 */
export function areSandboxSettingsLockedByPolicy(
  managedConfig: SandboxPolicyLockInput | null | undefined,
): boolean {
  if (!managedConfig) return false;
  if (managedConfig.sandboxExcludedCommands !== undefined) return true;
  const s = managedConfig.sandbox;
  if (!s || typeof s !== 'object') return false;
  return (
    s.mode !== undefined ||
    s.autoAllowBashIfSandboxed !== undefined ||
    s.allowUnsandboxedCommands !== undefined
  );
}

/**
 * An admin-facing diagnostic about a managed-policy value that the loader accepted
 * but will (partly) IGNORE — a silent-drop forward-compat hazard.
 */
export interface ManagedPolicyWarning {
  /** The policy field the warning is about. */
  key: string;
  /** Human-readable description of what is wrong. */
  issue: string;
  /** How the admin should fix it. */
  fix: string;
}

/**
 * Validate the per-field VALUES of a well-formed managed-settings object and
 * surface any that this client version will silently ignore. Checks the
 * `strictPluginOnlyCustomization` policy against the known
 * {@link CUSTOMIZATION_SURFACES} enum: a wrong type (not boolean/array) is rescued
 * by the loader's degrade-safe parse and therefore NOT enforced, and array entries
 * naming surfaces this client does not recognize (or non-string entries) are
 * dropped forward-compat — in both cases an admin believes a lockdown is active
 * when part of it is not. Returns one {@link ManagedPolicyWarning} per problem
 * (empty when the policy file is clean or absent). Parity: Claude Code's doctor
 * managed-settings forward-compat diagnostic (`doctorDiagnostic.ts`).
 */
export function validateManagedPolicyValues(
  rawManagedConfig: Record<string, unknown> | null | undefined,
): ManagedPolicyWarning[] {
  const warnings: ManagedPolicyWarning[] = [];
  if (!rawManagedConfig || typeof rawManagedConfig !== 'object') return warnings;

  const known = CUSTOMIZATION_SURFACES.join(', ');
  const field = rawManagedConfig.strictPluginOnlyCustomization;
  if (field !== undefined && typeof field !== 'boolean') {
    if (!Array.isArray(field)) {
      warnings.push({
        key: 'strictPluginOnlyCustomization',
        issue: `strictPluginOnlyCustomization has an invalid value (expected true or an array, got ${typeof field})`,
        fix: `The field is silently ignored. Set it to true, or an array of: ${known}.`,
      });
    } else {
      const bad = field.filter(
        x => typeof x !== 'string' || !(CUSTOMIZATION_SURFACES as readonly string[]).includes(x),
      );
      if (bad.length > 0) {
        warnings.push({
          key: 'strictPluginOnlyCustomization',
          issue: `strictPluginOnlyCustomization has ${bad.length} value(s) this client doesn't recognize: ${bad.map(String).join(', ')}`,
          fix: `These are silently ignored (forward-compat). Known surfaces: ${known}. Remove them, or this client is older than the policy intended.`,
        });
      }
    }
  }
  return warnings;
}

export const TIER_LOCKED_KEYS = new Set([
  'permissionMode',
  'maxTurns',
  'maxBudgetUsd',
  'maxSessionMs',
]);

const TIER_PRIORITY: Record<SettingsTier, number> = {
  managed: 0,
  global: 1,
  project: 2,
  local: 3,
  env: 4,
  cli: 5,
};

export function canOverride(key: string, source: SettingsTier, existing: SettingsTier): boolean {
  if (MANAGED_ONLY_KEYS.has(key) && source !== 'managed') return false;
  if (TIER_LOCKED_KEYS.has(key) && TIER_PRIORITY[source] > TIER_PRIORITY[existing]) return false;
  return true;
}

export function getEffectiveTier(key: string, sources: SettingsSource[]): SettingsTier | null {
  if (sources.length === 0) return null;
  if (MANAGED_ONLY_KEYS.has(key)) {
    const managed = sources.find(s => s.tier === 'managed');
    return managed ? managed.tier : null;
  }
  // `sources` is guaranteed non-empty here (the length-0 case returned above),
  // so `sources[0]` is a safe seed and `best` is never null — no `?? null` arm.
  if (TIER_LOCKED_KEYS.has(key)) {
    let best = sources[0];
    for (const s of sources) {
      if (TIER_PRIORITY[s.tier] < TIER_PRIORITY[best.tier]) {
        best = s;
      }
    }
    return best.tier;
  }
  let best = sources[0];
  for (const s of sources) {
    if (TIER_PRIORITY[s.tier] > TIER_PRIORITY[best.tier]) {
      best = s;
    }
  }
  return best.tier;
}

export function filterManagedKeys(config: Record<string, unknown>, sourceTier: SettingsTier): { filtered: Record<string, unknown>; rejected: string[] } {
  const filtered: Record<string, unknown> = {};
  const rejected: string[] = [];
  for (const [key, value] of Object.entries(config)) {
    if (MANAGED_ONLY_KEYS.has(key) && sourceTier !== 'managed') {
      rejected.push(key);
    } else {
      filtered[key] = value;
    }
  }
  return { filtered, rejected };
}

export class SettingsGuard {
  private _blockedOverrides: Array<{ key: string; source: SettingsTier; reason: string }> = [];

  checkOverride(key: string, source: SettingsTier, existing: SettingsTier): boolean {
    if (canOverride(key, source, existing)) return true;
    const reason = MANAGED_ONLY_KEYS.has(key)
      ? `"${key}" can only be set by managed config`
      : `"${key}" is locked at tier "${existing}" and cannot be overridden by "${source}"`;
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
