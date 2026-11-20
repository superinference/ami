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

/**
 * Human-readable metadata for every internal hook event the protection layer
 * governs. Replaces the former bare `PROTECTED_EVENTS` / `PASSTHROUGH_EVENTS`
 * string Sets with a single source of truth that ALSO carries a `summary` and
 * `description` — so the blocked-registration reason and any `/hooks`-style
 * introspection surface can explain WHAT each event does and WHY some are
 * protected, instead of just gating on an opaque name.
 *
 * Mirrors the reference's `getHookEventMetadata` (summary + description per event),
 * but scoped to AMI's internal prompt-assembly / protection events, which live in
 * a different namespace from the public lifecycle hook events documented in
 * `hooks.ts` (`getHookEventMetadata`). The two catalogs are disjoint by design.
 */
export interface HookEventInfo {
  /** One-line label for the event. */
  summary: string;
  /** What the event does and, for protected events, why user hooks are barred. */
  description: string;
  /**
   * `protected` events cannot be intercepted by user-tier hooks (they drive core
   * prompt/compaction/settings behavior); `passthrough` events are safe for any
   * tier to observe and transform.
   */
  category: 'protected' | 'passthrough';
}

export const HOOK_EVENT_INFO: Record<string, HookEventInfo> = {
  'session.compact': {
    summary: 'Context compaction',
    description:
      'Fires when the engine compacts or truncates the conversation to fit the context window. Protected so a user-tier hook cannot rewrite, reorder, or suppress core context-management behavior.',
    category: 'protected',
  },
  'prompt.compose': {
    summary: 'System-prompt assembly',
    description:
      'Fires while the system prompt is assembled. Protected so a user-tier hook cannot inject or strip core prompt sections.',
    category: 'protected',
  },
  'prompt.section': {
    summary: 'Prompt section emitted',
    description:
      'Fires for each section appended to the system prompt. Protected so a user-tier hook cannot tamper with individual core prompt sections.',
    category: 'protected',
  },
  'prompt.context': {
    summary: 'Prompt context assembled',
    description:
      'Fires when conversation/context material is assembled for the model. Protected so a user-tier hook cannot smuggle in or remove core context.',
    category: 'protected',
  },
  'prompt.attachment': {
    summary: 'Prompt attachment resolved',
    description:
      'Fires when an attachment is resolved into the prompt. Protected so a user-tier hook cannot inject or drop attachment content.',
    category: 'protected',
  },
  'settings.read': {
    summary: 'Settings read',
    description:
      'Fires when settings/configuration are read. Protected so a user-tier hook cannot tamper with how trusted configuration is loaded.',
    category: 'protected',
  },
  'prompt.submit': {
    summary: 'User prompt submitted',
    description: 'Fires when a user prompt is submitted; safe for any tier to observe or transform.',
    category: 'passthrough',
  },
  'turn.start': {
    summary: 'Turn started',
    description: 'Fires at the start of an agent turn; safe for any tier to observe.',
    category: 'passthrough',
  },
  'turn.end': {
    summary: 'Turn ended',
    description: 'Fires at the end of an agent turn; safe for any tier to observe.',
    category: 'passthrough',
  },
  'tool.call': {
    summary: 'Tool call',
    description: 'Fires around a tool invocation; safe for any tier to observe or gate.',
    category: 'passthrough',
  },
  'command.run': {
    summary: 'Command run',
    description: 'Fires when a slash command runs; safe for any tier to observe.',
    category: 'passthrough',
  },
  'command.register': {
    summary: 'Command registered',
    description: 'Fires when a slash command is registered; safe for any tier to observe.',
    category: 'passthrough',
  },
};

/** Human-readable metadata for an internal hook event, or `undefined` if unknown. */
export function getHookEventInfo(event: string): HookEventInfo | undefined {
  return HOOK_EVENT_INFO[event];
}

export const PROTECTED_EVENTS = new Set(
  Object.entries(HOOK_EVENT_INFO)
    .filter(([, info]) => info.category === 'protected')
    .map(([event]) => event),
);

export const PASSTHROUGH_EVENTS = new Set(
  Object.entries(HOOK_EVENT_INFO)
    .filter(([, info]) => info.category === 'passthrough')
    .map(([event]) => event),
);

export function isProtectedEvent(event: string): boolean {
  return PROTECTED_EVENTS.has(event);
}

/**
 * The hook tiers permitted to register a protected event: every tier except the
 * untrusted `user` tier (see {@link canHookEvent}). Used by
 * {@link describeProtectedEventBlock} to tell a blocked author which tiers IS
 * allowed — mirroring the reference's "what-is-allowed + what-to-do" phrasing.
 */
export function tiersAllowedForProtectedEvent(): HookTier[] {
  return HOOK_TIERS.filter((t) => t !== 'user');
}

/**
 * Build an actionable reason for a refused protected-event hook registration.
 *
 * Unlike the former terse `User-tier hook cannot intercept protected event "x"`,
 * this NAMES the offending hook (when a name/command is available), EXPLAINS why
 * the event is protected (from {@link HOOK_EVENT_INFO}), and STATES which tiers
 * may register it and how to proceed — matching the reference's richer blocked
 * messaging (which always tells the user what is allowed and what to do).
 */
export function describeProtectedEventBlock(event: string, tier: HookTier, name?: string): string {
  const subject = name ? `Hook "${name}" (${tier}-tier)` : `A ${tier}-tier hook`;
  const info = getHookEventInfo(event);
  const why = info
    ? `${info.summary}: ${info.description}`
    : `"${event}" is a protected core event.`;
  const allowed = tiersAllowedForProtectedEvent();
  return (
    `${subject} cannot intercept protected event "${event}". ${why} ` +
    `Only these tiers may register it: ${allowed.join(', ')}. ` +
    `To proceed, register the hook from a ${allowed[0]}/builtin source (e.g. a plugin or core settings) instead of a user-tier config.`
  );
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
 * {@link canHookEvent} check. Mirrors the reference's shouldSkipHookDueToTrust:
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
 * Mirrors the reference's distinction between admin/managed-settings hooks and
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
 * Parity: the reference's getHooksConfig → shouldAllowManagedHooksOnly branch,
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
 * pinning the outcome to `allow`. Mirrors the reference's hook precedence, where
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
  /**
   * Human-readable explanation of WHY this verdict fired — the reference pairs every
   * hook decision with `reason` / `permissionDecisionReason` so a deny/ask is not
   * opaque in the transcript or to the user. Optional: a `passthrough`/abstain
   * verdict needs no reason.
   */
  reason?: string;
  /**
   * Flow control (the reference `continue`): when `false`, halt the agentic loop
   * after this hook. `undefined`/`true` = keep going.
   */
  continue?: boolean;
  /** Suppress the hook's stdout from the transcript (the reference `suppressOutput`). */
  suppressOutput?: boolean;
  /** Reason shown to the user when {@link HookVerdictCheck.continue} is `false` (the reference `stopReason`). */
  stopReason?: string;
  /** System message to surface after the hook (the reference `systemMessage`). */
  systemMessage?: string;
}

/** Optional flow-control / messaging fields a hook verdict may carry. */
export interface HookVerdictFlow {
  reason?: string;
  continue?: boolean;
  suppressOutput?: boolean;
  stopReason?: string;
  systemMessage?: string;
}

/**
 * Construct a {@link HookVerdictCheck}, computing `isStricter` against the prior
 * accumulated verdict and attaching any flow-control/messaging fields the hook
 * supplied. Centralizes the `isStricterVerdict` call so callers never hand-roll
 * the lattice comparison.
 */
export function buildHookVerdictCheck(
  event: string,
  hookTier: HookTier,
  verdict: HookVerdict,
  previous: HookVerdict = 'passthrough',
  flow: HookVerdictFlow = {},
): HookVerdictCheck {
  return {
    event,
    hookTier,
    verdict,
    isStricter: isStricterVerdict(verdict, previous),
    ...(flow.reason !== undefined ? { reason: flow.reason } : {}),
    ...(flow.continue !== undefined ? { continue: flow.continue } : {}),
    ...(flow.suppressOutput !== undefined ? { suppressOutput: flow.suppressOutput } : {}),
    ...(flow.stopReason !== undefined ? { stopReason: flow.stopReason } : {}),
    ...(flow.systemMessage !== undefined ? { systemMessage: flow.systemMessage } : {}),
  };
}

/**
 * Merge a sequence of hook verdict checks under the strictest-wins lattice,
 * returning the winning check (carrying its own reason/flow fields) or
 * `undefined` for an empty list. A later check only wins when it is strictly
 * stricter than the current winner, so the FIRST strictest verdict — and its
 * reason — is preserved (a stable merge).
 */
export function mergeHookVerdictChecks(
  checks: readonly HookVerdictCheck[],
): HookVerdictCheck | undefined {
  let winner: HookVerdictCheck | undefined;
  for (const check of checks) {
    if (!winner || isStricterVerdict(check.verdict, winner.verdict)) winner = check;
  }
  return winner;
}

/**
 * Returns true iff `current` is strictly stricter than `previous` in the verdict
 * lattice (and should therefore win the strictest-wins merge).
 *
 * Fail-safe boundary: `current`/`previous` may carry malformed values straight
 * from a plugin/hook payload. Rather than silently coercing an unrecognized
 * value to the least-strict rank (which would let a real `deny` be dropped when
 * compared against garbage), any value outside the known verdict set throws —
 * mirroring the reference's `processHookJSONOutput`, which rejects unknown
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

export interface BlockedHookRegistration {
  event: string;
  tier: HookTier;
  /** The blocked hook's name/command, when the caller supplied one. */
  name?: string;
  /** Actionable explanation (see {@link describeProtectedEventBlock}). */
  reason: string;
}

export class HookChainGuard {
  private _blocked: BlockedHookRegistration[] = [];

  /**
   * Record and reject a protected-event registration by an insufficiently
   * trusted tier. `name` (the hook's name/command) is optional but, when given,
   * is woven into the actionable reason so the audit view names the blocked hook.
   */
  checkRegistration(event: string, tier: HookTier, name?: string): boolean {
    if (canHookEvent(event, tier)) return true;
    this._blocked.push({
      event,
      tier,
      ...(name ? { name } : {}),
      reason: describeProtectedEventBlock(event, tier, name),
    });
    return false;
  }

  getBlocked(): ReadonlyArray<BlockedHookRegistration> {
    return this._blocked;
  }

  getBlockedCount(): number {
    return this._blocked.length;
  }

  clear(): void {
    this._blocked = [];
  }
}

// ---------------------------------------------------------------------------
// Hidden-hook-modification defense (startup config snapshot)
// ---------------------------------------------------------------------------

/**
 * The provenance-bearing shape a hook entry must expose to be fingerprinted for
 * the startup snapshot. A {@link HookDescriptor}-like record satisfies it.
 */
export interface HookSignatureInput {
  event: string;
  /** Settings tier / provenance that contributed the hook. */
  source?: string;
  /** Matcher scoping the hook ('' / absent = all targets). */
  matcher?: string;
  /** The hook's command / prompt / URL. */
  content: string;
}

/**
 * Stable fingerprint of a hook entry: event + source + matcher + content. Two
 * entries collide iff they would run the same command from the same source on
 * the same matcher, so a snapshot of signatures captures exactly the executable
 * surface of a hook configuration. `\u0000` is used as a field separator because
 * it cannot occur in any of the component strings.
 */
export function hookEntrySignature(h: HookSignatureInput): string {
  return [h.event, h.source ?? '', h.matcher ?? '', h.content].join('\u0000');
}

/**
 * Capture a startup baseline of hook signatures.
 *
 * Mirrors the reference's `captureHooksConfigSnapshot` (taken after `setCwd`,
 * re-captured after a worktree chdir) whose explicit purpose is "to avoid hidden
 * hook modifications": a settings file edited mid-session — e.g. by a
 * prompt-injected file edit or a tool call — must not silently introduce new
 * command-executing hooks; the session is audited against the startup baseline.
 */
export function captureHookSnapshot(hooks: readonly HookSignatureInput[]): Set<string> {
  return new Set(hooks.map(hookEntrySignature));
}

/**
 * Return the subset of `current` hook entries whose signature was NOT in the
 * `baseline` snapshot — i.e. hooks that appeared (or were mutated) since startup.
 * A non-empty result means the on-disk hook config changed mid-session and is a
 * hidden-hook-modification signal (surface it via a diagnostic/introspection).
 * Identity-preserving (returns the original entry objects).
 */
export function detectHiddenHookAdditions<T extends HookSignatureInput>(
  baseline: ReadonlySet<string>,
  current: readonly T[],
): T[] {
  return current.filter((h) => !baseline.has(hookEntrySignature(h)));
}
