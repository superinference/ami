import { SAFE_ENV_VARS } from './env-protection';

export const SETTINGS_TIERS = ['managed', 'global', 'project', 'local', 'env', 'cli'] as const;
export type SettingsTier = (typeof SETTINGS_TIERS)[number];

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
      if (typeof value === 'string' && value.length > 0 && !SAFE_ENV_VARS.has(key.toUpperCase())) {
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
]);

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
