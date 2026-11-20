export interface ModInfo {
  name: string;
  id: string;
  tier: 'builtin' | 'managed' | 'user';
  marketplace?: string;
  /**
   * Typed provenance discriminant set at DISCOVERY time (non-spoofable).
   * When present it is the AUTHORITATIVE local/inline/remote decision used to
   * vouch for a mod under `allowManagedModsOnly`; the display-name `marketplace`
   * string is never consulted once this is set. This closes the spoof where a
   * REMOTE marketplace named e.g. `local-attacker` / `inline-evil` slipped past
   * a `marketplace.startsWith('local')` prefix check and was trusted as local.
   */
  source?: 'local' | 'inline' | 'remote';
  path?: string;
  /**
   * The mod's own default enabled state when NO explicit setting mentions it (the reference
   * parity: a plugin/mod definition's `defaultEnabled`). It lets a mod author ship a mod
   * that is installed-but-disabled-by-default (`defaultEnabled: false`) — or forced-on —
   * instead of every mod being unconditionally on. It is the MIDDLE tier of the
   * enablement waterfall in {@link resolveModEnabled}; an explicit `enabledPlugins` setting
   * still overrides it, and an absent value defaults to `true`.
   */
  defaultEnabled?: boolean;
}

export interface ManagedModsPolicy {
  allowManagedModsOnly: boolean;
  /**
   * The managed per-mod enablement map. BOOLEAN-AUTHORITATIVE (the reference parity:
   * managedPlugins.ts:L16-L20): a boolean value is the ONLY enablement signal —
   * `true` locks the mod enabled/vouches for it under {@link allowManagedModsOnly},
   * `false` force-disables it unconditionally ({@link isModBlockedByPolicy}). The
   * legacy `string[]` form (a version list or owner/repo allow-array) is accepted as a
   * valid config SHAPE but is NOT an enablement signal — it neither vouches for a mod
   * ({@link isModAllowed}) nor locks a name ({@link getManagedPluginNames}); such an
   * entry is treated as "not mentioned".
   */
  enabledPlugins?: Record<string, boolean | string[]>;
  prependPlugins?: string[];
  appendPlugins?: string[];
  /**
   * Operator-configured cross-marketplace trust allowlist (the reference parity:
   * `allowCrossMarketplaceDependenciesOn`). Marketplace names an org explicitly
   * trusts for user-tier mods under {@link allowManagedModsOnly}. Without this an org
   * could only vouch for the reserved `local`/`inline` sources; a specific trusted
   * REMOTE marketplace could not be approved short of editing code. Matching is exact
   * and case-insensitive (see {@link isMarketplaceAllowlisted}); there is NO transitive
   * trust — only this root allowlist is consulted.
   */
  allowedMarketplaces?: string[];
}

/**
 * The reserved marketplace tokens that identify a trusted LOCAL/INLINE copy when a mod
 * carries no typed {@link ModInfo.source}. Membership is EXACT (case-insensitive) —
 * never a prefix — mirroring the reference's `ALLOWED_OFFICIAL_MARKETPLACE_NAMES.has(...)`
 * exact-set check. An exact set cannot be spoofed by a remote marketplace that merely
 * STARTS WITH `local`/`inline` (e.g. `local-attacker`, `inline-exfil`), which the old
 * `startsWith` prefix test wrongly trusted.
 */
const RESERVED_LOCAL_MARKETPLACE_NAMES = new Set(['local', 'inline']);

/**
 * Unconditional org force-disable predicate (the reference parity: `isPluginBlockedByPolicy`).
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
 * The effective enabled state of a mod, resolved via the reference's 3-level enablement
 * waterfall (builtinPlugins.ts:72-76 `userSetting !== undefined ? userSetting === true :
 * (definition.defaultEnabled ?? true)`):
 *
 *   1. **explicit setting** — an `enabledPlugins[mod.id]` whose value is a BOOLEAN wins
 *      (`true` = on, `false` = off). The value is boolean-AUTHORITATIVE: the legacy
 *      `string[]` allow-array form is NOT an enablement setting here (it vouches for a
 *      source under {@link isModAllowed}, it does not set on/off), so it is ignored and the
 *      waterfall falls through — mirroring the reference's boolean-only `enabledPlugins` handling.
 *   2. **the mod's own default** — {@link ModInfo.defaultEnabled} (a mod may ship
 *      disabled-by-default), consulted when no boolean setting mentions the id.
 *   3. **hardcoded `true`** — a mod with neither a setting nor a declared default is on.
 *
 * This is purely the on/off DEFAULT decision and is orthogonal to the org-policy
 * allow/deny gate ({@link isModAllowed}) and the unconditional force-disable
 * ({@link isModBlockedByPolicy}); callers that enforce policy apply those separately.
 */
export function resolveModEnabled(mod: ModInfo, policy: ManagedModsPolicy | null | undefined): boolean {
  const setting = policy?.enabledPlugins?.[mod.id];
  if (typeof setting === 'boolean') return setting;
  return mod.defaultEnabled ?? true;
}

/**
 * The set of plugin/mod ids LOCKED by org policy: every `enabledPlugins` entry whose
 * value is a boolean (`true` OR `false`). A pinned-enabled and a pinned-disabled mod
 * are equally immutable — the user may not enable/disable/remove either. The legacy
 * owner/repo ARRAY (allow-array) form is explicitly NOT protected (it vouches for a
 * source, it does not lock the name). Parity: the reference's `getManagedPluginNames`.
 *
 * Each boolean-pinned id locks BOTH its full `name@marketplace` form AND the bare
 * `name` extracted from the FIRST `@` (the reference's `pluginId.split('@')[0]`). Locking the
 * bare name too means a pin such as `my-mod@local` also immobilizes a `my-mod` install
 * from any other marketplace — a strict superset of the reference's bare-name lock that also
 * keeps AMI's id-form lock, so a bare pin (no `@`) still protects its name.
 */
export function getManagedPluginNames(policy: ManagedModsPolicy | null | undefined): Set<string> {
  const names = new Set<string>();
  if (!policy?.enabledPlugins) return names;
  for (const [id, value] of Object.entries(policy.enabledPlugins)) {
    if (typeof value !== 'boolean') continue; // arrays vouch for a source, they don't lock a name
    names.add(id);
    const at = id.indexOf('@');
    if (at > 0) names.add(id.slice(0, at)); // bare name from the FIRST '@' (the reference parity)
  }
  return names;
}

/**
 * Decide whether a mod is a trusted LOCAL/INLINE copy (as opposed to a REMOTE
 * marketplace) for the `allowManagedModsOnly` vouch. The typed {@link ModInfo.source}
 * discriminant — stamped at discovery time — is authoritative and NON-SPOOFABLE: a
 * remote source carries `source: 'remote'` and can never be mistaken for local even
 * if it names its marketplace `local-attacker` or `inline-evil`. Only when `source`
 * is absent (legacy {@link ModInfo}) do we fall back to the display-name heuristic:
 * an absent marketplace is an in-repo/implicit copy (local), otherwise ONLY the EXACT
 * reserved `local`/`inline` tokens (case-insensitive) are honored — an exact-set match,
 * not a prefix, so `local-attacker`/`inline-exfil` can never masquerade as trusted.
 */
export function isLocalOrInlineSource(mod: ModInfo): boolean {
  if (mod.source) return mod.source === 'local' || mod.source === 'inline';
  if (!mod.marketplace) return true;
  return RESERVED_LOCAL_MARKETPLACE_NAMES.has(mod.marketplace.toLowerCase());
}

/**
 * Whether a marketplace is on the org's configurable cross-marketplace trust allowlist
 * ({@link ManagedModsPolicy.allowedMarketplaces}). Matching is EXACT and case-insensitive,
 * mirroring the reference's exact-set membership checks — a prefix cannot slip past. Returns
 * false when the marketplace, the policy, or the allowlist is absent.
 */
export function isMarketplaceAllowlisted(
  marketplace: string | undefined,
  policy: ManagedModsPolicy | null | undefined,
): boolean {
  if (!marketplace || !policy?.allowedMarketplaces) return false;
  const target = marketplace.toLowerCase();
  return policy.allowedMarketplaces.some((m) => m.toLowerCase() === target);
}

export function isModAllowed(mod: ModInfo, policy: ManagedModsPolicy): { allowed: boolean; reason?: string } {
  // Unconditional org force-disable (`enabledPlugins[id] === false`) is evaluated
  // FIRST, before the allow-only short-circuit: an org that explicitly denies ONE
  // plugin blocks it at every scope even while permitting user mods generally.
  if (isModBlockedByPolicy(mod.id, policy)) {
    return {
      allowed: false,
      reason: `${mod.name} is disabled by your organization's policy — contact your administrator to re-enable it`,
    };
  }

  if (!policy.allowManagedModsOnly) return { allowed: true };

  if (mod.tier === 'builtin' || mod.tier === 'managed') {
    return { allowed: true };
  }

  // User-tier mod — check if org explicitly enabled it. The managed `enabledPlugins`
  // policy is BOOLEAN-AUTHORITATIVE (the reference parity: managedPlugins.ts:L16-L20 —
  // `if (typeof value !== 'boolean' …) continue`): ONLY a boolean `true` entry vouches
  // for a user-tier mod. The ambiguous legacy `string[]` form (a version list or
  // owner/repo allow-array) is NOT an enablement signal here — an array entry falls
  // through to the deny path below rather than silently vouching for a mod whose
  // marketplace qualification it never actually asserts. A `false` entry was already
  // force-denied first by {@link isModBlockedByPolicy}.
  if (policy.enabledPlugins) {
    if (policy.enabledPlugins[mod.id] === true) {
      // Only local/inline copies — OR a mod from an org-allowlisted remote marketplace
      // ({@link ManagedModsPolicy.allowedMarketplaces}) — are vouched for. The typed
      // `source` discriminant (set at discovery time) is authoritative and non-spoofable
      // for the local/inline decision; the display-name `marketplace` is only a legacy
      // fallback there, but IS the authoritative token matched against the org allowlist.
      if (!isLocalOrInlineSource(mod) && !isMarketplaceAllowlisted(mod.marketplace, policy)) {
        return {
          allowed: false,
          reason: `${mod.name} is not part of your organization's managed mod set, so it was not loaded (it came from the remote marketplace "${mod.marketplace}") — contact your administrator to allow it`,
        };
      }
      return { allowed: true };
    }
  }

  return {
    allowed: false,
    reason: `${mod.name} is not part of your organization's managed mod set, so it was not loaded — contact your administrator to allow it`,
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
