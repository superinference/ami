// Split from permissions.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as path from 'path';
import * as os from 'os';
import { getManagedConfigPath } from './config';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

// Each mode is a DISTINCT permission semantic (names mix AMI-native kebab and
// the reference wire-names — that is intentional, we match CC semantics, not a naming
// convention). `dontAsk` was removed: it was a semantic DUPLICATE of `deny-all` (both
// deny every tool call); normalizePermissionMode maps the legacy spelling to deny-all.
export const PERMISSION_MODES = ['ask', 'auto', 'auto-allow', 'deny-all', 'acceptEdits', 'bypassPermissions', 'plan'] as const;

export type PermissionMode = (typeof PERMISSION_MODES)[number];

/**
 * Non-canonical permission-mode spellings → the canonical {@link PermissionMode}
 * they denote. This is purely ADDITIVE acceptance: it widens the set of strings
 * {@link normalizePermissionMode} will ACCEPT, it never changes what a canonical
 * mode means and never maps anything to a MORE-permissive default (unknown stays
 * `null`). Keys are matched exactly first, then case-insensitively.
 *
 * Parity note: the reference's interactive default mode is named `default`; AMI
 * names that same semantic `ask`. A the reference settings.json (`"defaultMode":
 * "default"`) or `--permission-mode default` must therefore resolve to `ask`
 * rather than be rejected. The kebab/camel/lowercase variants let a rule file
 * authored against either naming convention load without a spelling mismatch
 * (the mode names deliberately mix AMI-kebab and CC-camel, see above).
 */
export const PERMISSION_MODE_ALIASES: Record<string, PermissionMode> = {
  // Legacy removed duplicate (both denied every tool call) — kept for back-compat.
  dontAsk: 'deny-all',
  // the reference's canonical name for the interactive default.
  default: 'ask',
  normal: 'ask',
  interactive: 'ask',
  // Case/kebab-insensitive spellings of the mixed-naming canonical modes.
  acceptedits: 'acceptEdits',
  'accept-edits': 'acceptEdits',
  bypasspermissions: 'bypassPermissions',
  'bypass-permissions': 'bypassPermissions',
  bypass: 'bypassPermissions',
  denyall: 'deny-all',
  autoallow: 'auto-allow',
  'plan-mode': 'plan',
};

/**
 * The ordered set of permission modes a user cycles through interactively (the
 * the reference shift-tab cycle: default → accept-edits → plan → default). The
 * blanket modes (`deny-all`, `auto-allow`, `bypassPermissions`) are deliberately
 * EXCLUDED so an accidental keypress can never silently drop into a mode that
 * allows or denies everything — those are reachable only via an explicit flag.
 */
export const PERMISSION_MODE_CYCLE: readonly PermissionMode[] = ['ask', 'acceptEdits', 'plan'];

/**
 * Advance the permission mode one step around {@link PERMISSION_MODE_CYCLE}
 * (parity with the reference's shift-tab mode toggle). `forward` mirrors shift-tab,
 * `backward` the reverse. If `current` is a flag-only blanket mode outside the
 * cycle, re-enter at the safe interactive default (`forward`) or the last cycle
 * entry (`backward`) — a user-initiated cycle never auto-widens a blanket deny
 * into a permissive blanket mode; it lands on a rule-evaluated mode.
 */
export function cyclePermissionMode(
  current: PermissionMode,
  direction: 'forward' | 'backward' = 'forward',
): PermissionMode {
  const n = PERMISSION_MODE_CYCLE.length;
  const idx = PERMISSION_MODE_CYCLE.indexOf(current);
  if (idx === -1) {
    return direction === 'forward' ? PERMISSION_MODE_CYCLE[0]! : PERMISSION_MODE_CYCLE[n - 1]!;
  }
  const next = direction === 'forward' ? (idx + 1) % n : (idx - 1 + n) % n;
  return PERMISSION_MODE_CYCLE[next]!;
}

/**
 * True when `mode` is the neutral/default permission mode — AMI's default is `ask`
 * — OR is unset (`undefined`). Centralizes the "is this the default?" branch so the
 * undefined-vs-default equivalence lives in ONE place instead of being re-derived as
 * scattered `?? 'ask'` coalescing at each call site (which invites drift when the
 * default changes). Mirrors the reference's `isDefaultMode` (utils/permissions/PermissionMode.ts),
 * which likewise treats both the default string and `undefined` as the default.
 */
export function isDefaultMode(mode: PermissionMode | undefined): boolean {
  return mode === undefined || mode === 'ask';
}

/**
 * The permission modes that RELAX the default prompt-on-everything gate: edits are
 * auto-accepted (`acceptEdits`), or approval is blanket (`auto-allow`, the YOLO
 * alias, and `bypassPermissions`). Centralizes the "is this a more-permissive
 * mode?" predicate so the transition-time rule-cleanup in {@link PermissionManager}
 * and any UI that warns on escalation share ONE definition. The screened-autonomy
 * `auto` mode is NOT included: it still risk-screens every call, so it is not a
 * blanket relaxation. Mirrors the reference's set of modes that trigger
 * `transitionPermissionMode` cleanup (getNextPermissionMode.ts).
 */
export function isPermissivePermissionMode(mode: PermissionMode | undefined): boolean {
  return mode === 'acceptEdits' || mode === 'auto-allow' || mode === 'bypassPermissions';
}

/**
 * How a given permission {@link PermissionMode} disposes of a tool path that
 * resolves OUTSIDE the workspace (launch cwd + user-granted roots) but is not a
 * forbidden credential/system path:
 *   - `'grant'`    — silently allow + register the containing dir for the session.
 *                    Only the blanket auto-approval modes (`auto-allow`/YOLO and
 *                    `bypassPermissions`) do this: the operator opted into waving
 *                    everything through.
 *   - `'deny'`     — hard-refuse (no mutation/escape permitted): `deny-all` and
 *                    `plan`.
 *   - `'classify'` — subject the path to normal consent: prompt the user when an
 *                    interactive channel exists, else fail-closed deny. `ask`, the
 *                    screened-autonomy `auto` mode, AND `acceptEdits`.
 *
 * The `acceptEdits` placement is the parity-critical case. `acceptEdits`
 * auto-accepts edits WITHIN the cwd (via the edit-tool fast-path in {@link
 * PermissionManager.check}), but an edit/read whose path lands OUTSIDE the cwd is
 * CLASSIFIED, not blanket-granted — mirroring the reference's acceptEdits scoping
 * ("allowed in CWD, classified outside CWD"; classifierDecision.ts safe-set doc +
 * modeValidation CWD fast-path). This is the SINGLE source of truth for the
 * out-of-workspace disposition so the executor's path-boundary guard can never
 * drift from the mode semantics. (In-workspace paths never reach this function —
 * they classify as 'allow' before the boundary check.)
 */
export const OUT_OF_WORKSPACE_PATH_DISPOSITIONS = ['grant', 'deny', 'classify'] as const;

export type OutOfWorkspacePathDisposition = (typeof OUT_OF_WORKSPACE_PATH_DISPOSITIONS)[number];

export function outOfWorkspacePathDisposition(mode: PermissionMode | undefined): OutOfWorkspacePathDisposition {
  if (mode === 'auto-allow' || mode === 'bypassPermissions') return 'grant';
  if (mode === 'deny-all' || mode === 'plan') return 'deny';
  // 'ask' / 'auto' / 'acceptEdits' (and any unset mode) → require consent.
  return 'classify';
}

/**
 * The reduced permission-mode vocabulary exposed to EXTERNAL / SDK consumers. A
 * superset consumer should not have to understand AMI's richer internal set
 * (`auto-allow`, `deny-all`); those collapse onto an external equivalent via
 * {@link toExternalPermissionMode}. Mirrors the reference's `EXTERNAL_PERMISSION_MODES`
 * (the internal set minus vendor-only modes), with AMI's neutral mode surfaced as
 * the canonical `default` label.
 */
export const EXTERNAL_PERMISSION_MODES = ['default', 'acceptEdits', 'bypassPermissions', 'plan'] as const;

export type ExternalPermissionMode = (typeof EXTERNAL_PERMISSION_MODES)[number];

/**
 * The payload delivered to a {@link PermissionModeChangeListener} whenever the
 * permission mode is mutated through {@link PermissionManager}'s single choke point.
 * Carries both the raw internal modes and their externalized forms so a listener can
 * audit the security-critical transition (e.g. into `bypassPermissions`/`auto-allow`)
 * without re-deriving the external label. `reason` names the mutation path that
 * triggered it. Parity: the reference's `notifyPermissionModeChanged` single-choke-point
 * notification (state/onChangeAppState.ts, utils/sessionState.ts) — every path that
 * changes the mode fires this, so no path can silently escalate privileges.
 */
export interface PermissionModeChange {
  /** Mode in effect before this change. */
  previousMode: PermissionMode;
  /** Mode now in effect. */
  mode: PermissionMode;
  /** {@link toExternalPermissionMode} of `previousMode`. */
  previousExternalMode: ExternalPermissionMode;
  /** {@link toExternalPermissionMode} of `mode`. */
  externalMode: ExternalPermissionMode;
  /** The mutation path that triggered the change (`setMode`, `exitPlanMode`,
   *  `deserialize`, `init`), for audit provenance. */
  reason: string;
}

/** A listener invoked on every real permission-mode transition. */
export type PermissionModeChangeListener = (change: PermissionModeChange) => void;

/**
 * Where a permission rule/update is persisted, in precedence order (later wins at
 * runtime; earlier is broader scope). Mirrors the reference's PermissionUpdateDestination:
 * - `userSettings`   — global (~/.ami), applies everywhere;
 * - `projectSettings`— shared per-project, committed (cwd/.ami);
 * - `localSettings`  — per-project but gitignored (cwd/.ami, *.local.json);
 * - `session`        — in-memory only, this run;
 * - `cliArg`         — supplied via a command-line flag.
 */
export const PERMISSION_UPDATE_DESTINATIONS = [
  'userSettings', 'projectSettings', 'localSettings', 'session', 'cliArg',
] as const;

export type PermissionUpdateDestination = (typeof PERMISSION_UPDATE_DESTINATIONS)[number];

/**
 * The full set of tiers a permission rule can ORIGINATE from — a superset of the
 * writable {@link PERMISSION_UPDATE_DESTINATIONS}. It adds three read-only sources
 * that a rule can be loaded/evaluated from but can NEVER be persisted to:
 *  - `policySettings` — managed/enterprise rules (admin-owned, highest precedence);
 *  - `flagSettings`   — rules injected from a settings file named by a launch flag;
 *  - `command`        — rules contributed at runtime by a slash command / tool.
 * Mirrors the reference's `PermissionRuleSource` (types/permissions.ts L54-63), which is
 * deliberately wider than its writable `PermissionUpdateDestination` set.
 */
export const PERMISSION_RULE_SOURCES = [
  'userSettings', 'projectSettings', 'localSettings', 'session', 'cliArg',
  'policySettings', 'flagSettings', 'command',
] as const;

export type PermissionRuleSource = (typeof PERMISSION_RULE_SOURCES)[number];

/**
 * Validate an arbitrary value against the known {@link PERMISSION_RULE_SOURCES},
 * returning the typed source or `undefined`. Lets a persisted rule carry its own
 * `source` tag (e.g. a managed/flag file that bundles rules from several origins)
 * while a bogus value falls back to the loader's default tier.
 */
export function coercePermissionRuleSource(raw: unknown): PermissionRuleSource | undefined {
  return typeof raw === 'string' && (PERMISSION_RULE_SOURCES as readonly string[]).includes(raw)
    ? (raw as PermissionRuleSource)
    : undefined;
}

/** A rule source is managed/enterprise-owned iff it is `policySettings`. Under the
 *  {@link PermissionManager}'s managed-rules-only lockdown, ONLY these are honored. */
export function isManagedPermissionSource(source: PermissionRuleSource | undefined): boolean {
  return source === 'policySettings';
}

/**
 * Is a rule's {@link PermissionRuleSource} SHARED (visible to / imposed on other
 * users) rather than PERSONAL to this machine/run? Shared tiers are committed or
 * pushed to the whole team: `projectSettings` (committed to git), `policySettings`
 * (enterprise-managed), and `command` (slash-command frontmatter). Personal tiers
 * (`userSettings`, `localSettings`, `cliArg`, `session`, `flagSettings`) only
 * affect the current user. An undefined source is treated as personal (the safe
 * default — it is never attributed to the team). Mirrors the reference's
 * `isSharedSettingSource` (utils/permissions/shadowedRuleDetection.ts L61-67); it
 * gates whether the sandbox-auto-allow shadow carve-out applies (a personal ask
 * rule a user's own sandbox would auto-allow is NOT reported as shadowing, but a
 * shared one still warns because teammates may lack the sandbox).
 */
export function isSharedSettingSource(source: PermissionRuleSource | undefined): boolean {
  return source === 'projectSettings' || source === 'policySettings' || source === 'command';
}

/** Human-readable prose for each {@link PermissionRuleSource}, for naming WHERE a
 *  rule came from in `/permissions` shadow warnings, `/doctor` findings, and fix
 *  suggestions (an opaque token like `localSettings` is unreadable). Mirrors
 *  the reference's `permissionRuleSourceDisplayString`. */
export const PERMISSION_RULE_SOURCE_DISPLAY: Record<PermissionRuleSource, string> = {
  userSettings: 'user settings',
  projectSettings: 'project settings',
  localSettings: 'local settings',
  session: 'session rules',
  cliArg: 'command-line arguments',
  policySettings: 'enterprise managed settings',
  flagSettings: 'flag settings',
  command: 'slash command',
};

/** Prose label for a rule source ('project settings', …); 'an unknown source'
 *  when the rule carries no source tag. Used in shadow-detection reasons/fixes. */
export function permissionRuleSourceDisplayString(source: PermissionRuleSource | undefined): string {
  return source ? PERMISSION_RULE_SOURCE_DISPLAY[source] : 'an unknown source';
}

export interface PermissionRule {
  tool: string;       // tool name or '*'
  pattern?: string;   // glob pattern for args (e.g., "git *")
  /** Glob match on named input params, e.g. { model: 'opus*' } for Tool(param:value). */
  paramPattern?: Record<string, string>;
  action: 'allow' | 'deny' | 'ask';
  reason?: string;    // optional reason for deny rules
  /** Tier this rule originated from (set when loaded from a source file). */
  source?: PermissionRuleSource;
}

/**
 * How a higher-priority rule makes a lower one unreachable:
 *  - `deny` — the allow is COMPLETELY blocked (a tool-wide deny always wins); the
 *    more severe case.
 *  - `ask`  — the allow is never reached because a preceding tool-wide ask always
 *    prompts first (it still "fires", just never silently allows).
 * Mirrors the reference's `ShadowType` (utils/permissions/shadowedRuleDetection.ts).
 */
export const SHADOW_TYPES = ['deny', 'ask'] as const;

export type ShadowType = (typeof SHADOW_TYPES)[number];

/**
 * A permission rule that can never fire because a higher-priority rule shadows it,
 * with a human-readable {@link reason} naming the shadowing rule + its source and a
 * concrete {@link fix} telling the user which rule to remove to make it reachable.
 * Returned by {@link PermissionManager.detectShadowedRules}. Mirrors the reference's
 * `UnreachableRule`.
 */
export interface UnreachableRule {
  /** The shadowed (unreachable) allow rule. */
  rule: PermissionRule;
  /** The higher-priority rule that shadows it. */
  shadowedBy: PermissionRule;
  /** Whether the shadow is a hard block (`deny`) or an always-prompt (`ask`). */
  shadowType: ShadowType;
  /** Human sentence: `Blocked by "bash" deny rule (from project settings)`, etc. */
  reason: string;
  /** Actionable remediation naming the exact rules + sources to change. */
  fix: string;
}

/**
 * Options for {@link PermissionManager.detectShadowedRules}. When
 * `sandboxAutoAllowEnabled` is true, a tool-wide Bash *ask* rule from PERSONAL
 * settings is NOT reported as shadowing a specific Bash allow — sandboxed commands
 * are auto-allowed so the ask never actually gates them for this user (shared-source
 * ask rules still warn, since teammates may lack the sandbox). Mirrors the reference's
 * `DetectUnreachableRulesOptions`. Omitted → derived from the manager's own sandbox
 * policy.
 */
export interface DetectUnreachableRulesOptions {
  sandboxAutoAllowEnabled?: boolean;
}

/**
 * Resolve the on-disk file a permission destination maps to, or null for the
 * in-memory tiers (`session`, `cliArg`). `userSettings` is global (~/.ami);
 * `projectSettings`/`localSettings` are per-project under `cwd/.ami` (local is
 * the gitignored `.local.json` variant). Mirrors the reference's per-destination
 * settings-file resolution.
 */
export function permissionDestinationPath(
  cwd: string,
  destination: PermissionUpdateDestination,
): string | null {
  switch (destination) {
    case 'userSettings':
      return path.join(os.homedir(), '.ami', 'permissions.json');
    case 'projectSettings':
      return path.join(cwd, '.ami', 'permissions.json');
    case 'localSettings':
      return path.join(cwd, '.ami', 'permissions.local.json');
    case 'session':
    case 'cliArg':
      return null; // in-memory only — never persisted
  }
}

/**
 * The managed/enterprise permission-rules file (`policySettings` source). It lives
 * beside the managed config (`SUPERINFERENCE_MANAGED_CONFIG` env override, else
 * `/etc/superinference/config.json`) as `permissions.json`, so one admin-owned
 * location holds both managed config and managed permission policy. Admin-owned and
 * read-only to the agent: these rules can be loaded/evaluated but never persisted to.
 * Mirrors the reference's `policySettings` managed-settings tier.
 */
export function managedPermissionsPath(): string {
  return path.join(path.dirname(getManagedConfigPath()), 'permissions.json');
}

export const BASH_CLASSIFICATIONS = ['safe', 'unsafe', 'destructive'] as const;

export type BashClassification = (typeof BASH_CLASSIFICATIONS)[number];

/**
 * Context handed to an {@link AutoModeSafetyClassifier} for a tool call the
 * deterministic engine would otherwise auto-approve in an autonomous mode.
 * `staticDecision` is always `'allow'` (the classifier is only consulted on the
 * auto-approve path), carried so a classifier can key off it and so the shape can
 * grow without a breaking change.
 */
export interface AutoModeSafetyContext {
  toolName: string;
  input: Record<string, unknown>;
  /** The autonomous mode that produced the auto-approval. */
  mode: PermissionMode;
  /** The deterministic verdict the classifier is being asked to confirm (always `allow`). */
  staticDecision: 'allow';
}

/**
 * A classifier's verdict on a would-be auto-approval. `allow` (or returning
 * `undefined` from the classifier) leaves the deterministic auto-approval intact;
 * `ask` downgrades it to an interactive prompt and `deny` blocks the call outright,
 * each carrying an optional human-readable `reason` recorded against the denial log.
 */
export interface AutoModeSafetyVerdict {
  /**
   * `allow` confirms the deterministic auto-approval; `ask` downgrades it to a
   * prompt; `deny` blocks it. `passthrough` is an explicit ABSTAIN ("no opinion —
   * defer to the deterministic decision"), the first-class spelling of what a
   * classifier previously could only express by returning `undefined`. It lets a
   * composable/layered classifier decline to weigh in without being read as a
   * positive `allow`. Mirrors the reference's `PermissionResult.behavior:
   * 'passthrough'` and consolidates with the {@link PermissionVerdict} /
   * {@link HookVerdict} `passthrough` abstain vocabulary (one canonical word for
   * "defer" across the engine). Treated identically to `allow`/`undefined` by
   * {@link PermissionManager.check}: the deterministic auto-approval stands and no
   * downgrade reason or denial is recorded.
   */
  decision: 'allow' | 'ask' | 'deny' | 'passthrough';
  /** Why the classifier downgraded the approval (recorded for the UI / denial log). */
  reason?: string;
}
