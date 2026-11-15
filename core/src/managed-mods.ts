export interface ModInfo {
  name: string;
  id: string;
  tier: 'builtin' | 'managed' | 'user';
  marketplace?: string;
  path?: string;
}

export interface ManagedModsPolicy {
  allowManagedModsOnly: boolean;
  enabledPlugins?: Record<string, boolean | string[]>;
  prependPlugins?: string[];
  appendPlugins?: string[];
}

/**
 * Unconditional org force-disable predicate (Claude parity: `isPluginBlockedByPolicy`).
 * A managed `enabledPlugins[id] === false` entry force-disables that plugin/mod at
 * EVERY scope and chokepoint — install, enable, AND load — INDEPENDENT of
 * `allowManagedModsOnly`. Any other value (`true`, a `string[]` allow-array, or an
 * absent entry) is NOT a block. `enabledPlugins` is a {@link ManagedModsPolicy} field
 * sourced only from managed config, so a `false` here provably came from org policy —
 * the single source of truth consulted at the install/enable/load chokepoints.
 */
export function isModBlockedByPolicy(id: string, policy: ManagedModsPolicy | null | undefined): boolean {
  return policy?.enabledPlugins?.[id] === false;
}

/**
 * The set of plugin/mod ids LOCKED by org policy: every `enabledPlugins` entry whose
 * value is a boolean (`true` OR `false`). A pinned-enabled and a pinned-disabled mod
 * are equally immutable — the user may not enable/disable/remove either. The legacy
 * owner/repo ARRAY (allow-array) form is explicitly NOT protected (it vouches for a
 * source, it does not lock the name). Parity: Claude's `getManagedPluginNames`.
 */
export function getManagedPluginNames(policy: ManagedModsPolicy | null | undefined): Set<string> {
  const names = new Set<string>();
  if (!policy?.enabledPlugins) return names;
  for (const [id, value] of Object.entries(policy.enabledPlugins)) {
    if (typeof value === 'boolean') names.add(id);
  }
  return names;
}

export function isModAllowed(mod: ModInfo, policy: ManagedModsPolicy): { allowed: boolean; reason?: string } {
  // Unconditional org force-disable (`enabledPlugins[id] === false`) is evaluated
  // FIRST, before the allow-only short-circuit: an org that explicitly denies ONE
  // plugin blocks it at every scope even while permitting user mods generally.
  if (isModBlockedByPolicy(mod.id, policy)) {
    return {
      allowed: false,
      reason: `${mod.name} is disabled by your organization's policy (enabledPlugins["${mod.id}"] = false)`,
    };
  }

  if (!policy.allowManagedModsOnly) return { allowed: true };

  if (mod.tier === 'builtin' || mod.tier === 'managed') {
    return { allowed: true };
  }

  // User-tier mod — check if org explicitly enabled it
  if (policy.enabledPlugins) {
    const entry = policy.enabledPlugins[mod.id];
    if (entry === true || (Array.isArray(entry) && entry.length > 0)) {
      // Only local folder marketplace copies are vouched for
      if (mod.marketplace && !mod.marketplace.startsWith('inline') && !mod.marketplace.startsWith('local')) {
        return {
          allowed: false,
          reason: `mods are limited to your organization's by policy (allowManagedModsOnly); ${mod.name} from remote marketplace "${mod.marketplace}" was not loaded`,
        };
      }
      return { allowed: true };
    }
  }

  return {
    allowed: false,
    reason: `mods are limited to your organization's by policy (allowManagedModsOnly); ${mod.name} was not loaded`,
  };
}

export function parseModId(id: string): { name: string; marketplace: string } {
  const lastAt = id.lastIndexOf('@');
  if (lastAt <= 0) return { name: id, marketplace: '' };
  return {
    name: id.slice(0, lastAt),
    marketplace: id.slice(lastAt + 1),
  };
}

export function normalizeModName(name: string): string {
  return name
    .normalize('NFC')
    .toLowerCase()
    .replace(/[-_]+/g, '_')
    .replace(/[^a-z0-9_]/g, '_');
}

export function modNamesMatch(a: string, b: string): boolean {
  return normalizeModName(a) === normalizeModName(b);
}

export function validateModId(id: string): boolean {
  const DANGEROUS = ['__proto__', 'constructor', 'prototype', 'toString', 'valueOf', 'hasOwnProperty'];
  const parts = id.split(/[@/\\]/);
  return !parts.some(p => DANGEROUS.includes(p));
}

export class ManagedModsEnforcer {
  private _policy: ManagedModsPolicy;
  private _refused: Array<{ mod: ModInfo; reason: string }> = [];

  constructor(policy: ManagedModsPolicy) {
    this._policy = policy;
  }

  check(mod: ModInfo): boolean {
    const result = isModAllowed(mod, this._policy);
    if (!result.allowed && result.reason) {
      this._refused.push({ mod, reason: result.reason });
    }
    return result.allowed;
  }

  getRefused(): ReadonlyArray<{ mod: ModInfo; reason: string }> {
    return this._refused;
  }

  getRefusedCount(): number {
    return this._refused.length;
  }

  updatePolicy(policy: ManagedModsPolicy): void {
    this._policy = policy;
  }

  clear(): void {
    this._refused = [];
  }

  static shouldBlock(mod: ModInfo, policy: ManagedModsPolicy | null): { blocked: boolean; reason?: string } {
    if (!policy) return { blocked: false };
    // Force-disable (`enabledPlugins[id] === false`) blocks even when
    // allowManagedModsOnly is off — it is the org denylist, honored unconditionally.
    if (isModBlockedByPolicy(mod.id, policy)) {
      const result = isModAllowed(mod, policy);
      return { blocked: true, reason: result.reason };
    }
    if (!policy.allowManagedModsOnly) return { blocked: false };
    const result = isModAllowed(mod, policy);
    return { blocked: !result.allowed, reason: result.reason };
  }
}
