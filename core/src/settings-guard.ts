export type SettingsTier = 'managed' | 'global' | 'project' | 'local' | 'env' | 'cli';

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
