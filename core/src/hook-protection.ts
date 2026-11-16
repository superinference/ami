export const HOOK_TIERS = ['builtin', 'prepend', 'user', 'append', 'core'] as const;
export type HookTier = (typeof HOOK_TIERS)[number];

export interface HookRegistration {
  name: string;
  event: string;
  tier: HookTier;
  source: string;
  priority: number;
}

const TIER_PRIORITY: Record<HookTier, number> = {
  builtin: 0,
  prepend: 1,
  user: 2,
  append: 3,
  core: 4,
};

export const PROTECTED_EVENTS = new Set([
  'session.compact',
  'prompt.compose',
  'prompt.section',
  'prompt.context',
  'prompt.attachment',
  'settings.read',
]);

export const PASSTHROUGH_EVENTS = new Set([
  'prompt.submit',
  'turn.start',
  'turn.end',
  'tool.call',
  'command.run',
  'command.register',
]);

export function isProtectedEvent(event: string): boolean {
  return PROTECTED_EVENTS.has(event);
}

export function canHookEvent(event: string, tier: HookTier): boolean {
  if (!isProtectedEvent(event)) return true;
  // Fail-safe: the `tier` value originates from untrusted plugin/hook
  // registration data and is only nominally typed `HookTier`. A value that is
  // not a recognized tier (e.g. a malformed/unknown tier like 'User', 'plugin',
  // or '') must NOT silently pass the protected-event guard. Deny (fail-closed)
  // any tier we do not recognize instead of allowing everything that is not the
  // single excluded 'user' literal.
  if (!(HOOK_TIERS as readonly string[]).includes(tier)) return false;
  return tier !== 'user';
}

/**
 * Defense-in-depth workspace-trust gate, orthogonal to the tier-based
 * {@link canHookEvent} check. Mirrors Claude Code's shouldSkipHookDueToTrust:
 * when the workspace trust dialog has NOT been accepted, EVERY hook of EVERY
 * tier is skipped in an interactive session (a declined-trust workspace must
 * not fire SessionEnd/SubagentStop/etc. hooks — a real CVE class). In
 * non-interactive / SDK mode trust is implicit, so hooks are allowed through.
 */
export function shouldSkipHookDueToTrust(trusted: boolean, interactive: boolean): boolean {
  if (!interactive) return false; // non-interactive / SDK → implicit trust
  return !trusted;
}

/**
 * Provenance of a hook registration, used by the managed-only policy filter.
 *
 * Mirrors Claude Code's distinction between admin/managed-settings hooks and
 * lower-trust sources (plugin bundles, session/frontmatter-derived agent/skill
 * hooks, and user/project filesystem hooks) that `allowManagedHooksOnly` must
 * strip so they cannot bypass org policy. AMI's tier model (see {@link HookTier})
 * can only reorder sources; this kind lets a loader EXCLUDE a source entirely by
 * where it came from.
 */
export type HookSourceKind = 'managed' | 'plugin' | 'project' | 'user' | 'config';

/**
 * The only provenance an `allowManagedHooksOnly` policy permits to run:
 * admin/managed-settings hooks. Everything else (plugin, project, user, config)
 * is non-managed and stripped when the policy is on.
 */
export function isManagedHookSource(kind: HookSourceKind): boolean {
  return kind === 'managed';
}

/**
 * Managed-only policy predicate: returns true iff a hook from `kind` may run.
 *
 * When `managedOnly` is OFF every provenance runs; when ON only admin/managed
 * hooks run — plugin, project, user, and config (and any session/frontmatter-
 * derived) hooks are stripped BEFORE they enter the chain, not merely reordered.
 * Parity: Claude Code's getHooksConfig → shouldAllowManagedHooksOnly branch,
 * which skips plugin matchers and session hooks entirely under the policy.
 */
export function canRunHookUnderManagedOnly(kind: HookSourceKind, managedOnly: boolean): boolean {
  return managedOnly ? isManagedHookSource(kind) : true;
}

/**
 * Filter any provenance-tagged registration list down to those permitted under
 * the managed-only policy. Identity-preserving (keeps the original objects) so
 * callers can carry their own per-source metadata on each entry.
 */
export function filterManagedOnlyHooks<T extends { kind: HookSourceKind }>(
  hooks: readonly T[],
  managedOnly: boolean,
): T[] {
  return hooks.filter((h) => canRunHookUnderManagedOnly(h.kind, managedOnly));
}

export function sortHooksByTier(hooks: HookRegistration[]): HookRegistration[] {
  return [...hooks].sort((a, b) => {
    const tierDiff = TIER_PRIORITY[a.tier] - TIER_PRIORITY[b.tier];
    if (tierDiff !== 0) return tierDiff;
    return a.priority - b.priority;
  });
}

/**
 * Hook permission verdicts, forming a precedence lattice.
 *
 * `passthrough` is a FOURTH, neutral state distinct from `allow`: a hook can
 * observe an event and decline to influence the decision (abstain) without
 * pinning the outcome to `allow`. Mirrors Claude Code's hook precedence, where
 * `allow` only wins when nothing is set yet while `passthrough` is a true no-op.
 * Precedence (strictest wins): passthrough < allow < ask < deny.
 */
export type HookVerdict = 'passthrough' | 'allow' | 'ask' | 'deny';

const VERDICT_RANK: Record<HookVerdict, number> = {
  passthrough: -1, // neutral no-op: never overrides a real verdict, is overridden by any
  allow: 0,
  ask: 1,
  deny: 2,
};

function verdictRank(verdict: string): number | undefined {
  return (VERDICT_RANK as Record<string, number | undefined>)[verdict];
}

export interface HookVerdictCheck {
  event: string;
  hookTier: HookTier;
  verdict: HookVerdict;
  isStricter: boolean;
}

/**
 * Returns true iff `current` is strictly stricter than `previous` in the verdict
 * lattice (and should therefore win the strictest-wins merge).
 *
 * Fail-safe boundary: `current`/`previous` may carry malformed values straight
 * from a plugin/hook payload. Rather than silently coercing an unrecognized
 * value to the least-strict rank (which would let a real `deny` be dropped when
 * compared against garbage), any value outside the known verdict set throws —
 * mirroring Claude Code's `processHookJSONOutput`, which rejects unknown
 * decision types instead of proceeding on malformed input.
 */
export function isStricterVerdict(current: string, previous: string): boolean {
  const currentRank = verdictRank(current);
  if (currentRank === undefined) {
    throw new Error(
      `Unknown hook verdict "${current}". Valid verdicts are: passthrough, allow, ask, deny`,
    );
  }
  const previousRank = verdictRank(previous);
  if (previousRank === undefined) {
    throw new Error(
      `Unknown hook verdict "${previous}". Valid verdicts are: passthrough, allow, ask, deny`,
    );
  }
  return currentRank > previousRank;
}

export class HookChainGuard {
  private _blocked: Array<{ event: string; tier: HookTier; reason: string }> = [];

  checkRegistration(event: string, tier: HookTier): boolean {
    if (canHookEvent(event, tier)) return true;
    this._blocked.push({
      event,
      tier,
      reason: `User-tier hook cannot intercept protected event "${event}"`,
    });
    return false;
  }

  getBlocked(): ReadonlyArray<{ event: string; tier: HookTier; reason: string }> {
    return this._blocked;
  }

  getBlockedCount(): number {
    return this._blocked.length;
  }

  clear(): void {
    this._blocked = [];
  }
}
