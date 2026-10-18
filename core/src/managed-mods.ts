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

export function isModAllowed(mod: ModInfo, policy: ManagedModsPolicy): { allowed: boolean; reason?: string } {
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
    if (!policy || !policy.allowManagedModsOnly) return { blocked: false };
    const result = isModAllowed(mod, policy);
    return { blocked: !result.allowed, reason: result.reason };
  }
}
