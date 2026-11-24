// Split from team.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as path from 'path';
import { TEAM_LEAD_NAME } from './send-message';
import { log as coreLog } from '../logger';
import { AgentColorName } from '../skills';

/**
 * Structured, prefixed diagnostic line for a team-state mutation (parity: the reference
 * Code's `logForDebugging('[TeammateTool] …')`). Every add/remove/mode/active/
 * cleanup/reconnect action emits one so the swarm lifecycle is observable via the
 * `team` debug component (`AMI_DEBUG_COMPONENTS=team`), satisfying AMI's
 * introspection-on-every-change rule. Routed through the shared core logger — NOT
 * a second logging path.
 */
export function teamDebug(msg: string): void {
  coreLog('team', `[TeammateTool] ${msg}`);
}

/** Env-var fallback for a teammate identity field (parity: the reference's
 *  env-var teammate identity, e.g. CLAUDE_CODE_AGENT_ID). Returns undefined for an
 *  unset/empty value so it never shadows a bound dynamic/ALS identity. */
export function envIdentity(name: string): string | undefined {
  const v = process.env[name];
  return v && v.length > 0 ? v : undefined;
}

/** Env var names a spawned teammate process inherits its identity from. */
export const ENV_AGENT_ID = 'AMI_TEAMMATE_AGENT_ID';

export const ENV_AGENT_NAME = 'AMI_TEAMMATE_AGENT_NAME';

export const ENV_TEAM_NAME = 'AMI_TEAMMATE_TEAM_NAME';

export const ENV_COLOR = 'AMI_TEAMMATE_COLOR';

/**
 * Claude-Code-compatible env knobs for seeding a SEPARATE-PROCESS teammate's
 * identity (parity: the reference's `CLAUDE_CODE_AGENT_ID` /
 * `CLAUDE_CODE_PARENT_SESSION_ID`, read in `utils/agentContext.ts`). A swarm
 * teammate launched out-of-process (tmux/iTerm2) has no in-process
 * {@link TeammateContext} (AsyncLocalStorage) or runtime {@link DynamicTeamContext}
 * to carry its id, so the reference injects the identity through the environment. AMI
 * already reads other `CLAUDE_CODE_*` knobs (`CLAUDE_CODE_MAX_*` in
 * subagent-limiter, `CLAUDE_CODE_PLAN_MODE_REQUIRED` here); these two complete the
 * set. They sit BELOW AMI's own `AMI_TEAMMATE_*` knob in precedence so AMI's
 * native variable always wins when both are set — the reference name is a compat
 * fallback, not an override.
 */
export const ENV_CC_AGENT_ID = 'CLAUDE_CODE_AGENT_ID';

export const ENV_CC_PARENT_SESSION_ID = 'CLAUDE_CODE_PARENT_SESSION_ID';

/** Env-var fallback for the per-teammate plan-mode-required gate (parity: the reference
 *  Code's `CLAUDE_CODE_PLAN_MODE_REQUIRED`). A spawned teammate process inherits
 *  the "must plan before writing code" enforcement through this when no in-process
 *  ALS / dynamic context carries it. */
export const ENV_PLAN_MODE_REQUIRED = 'AMI_TEAMMATE_PLAN_MODE_REQUIRED';

/**
 * Parse a boolean-ish env var the same way the rest of core does
 * (`1`/`true`/`yes`/`on` are truthy; everything else falsey). Returns undefined for
 * an unset/empty value so the env layer never shadows a bound dynamic/ALS flag —
 * only fills in when nothing higher in the precedence chain supplied a value.
 */
export function envBool(name: string): boolean | undefined {
  const v = envIdentity(name);
  if (v === undefined) return undefined;
  const l = v.toLowerCase();
  return l === '1' || l === 'true' || l === 'yes' || l === 'on';
}

/**
 * Format a deterministic agent id as `agentName@teamName` (mirrors the reference's
 * `formatAgentId` in utils/agentId.ts). The `@` separator makes the id
 * reproducible — the same name in the same team always yields the same id — which
 * enables reconnection after a crash/restart and lets a lead COMPUTE a teammate's
 * id without a lookup. Agent names must not contain `@` (see `sanitizeAgentName`).
 */
export function formatAgentId(agentName: string, teamName: string): string {
  return `${agentName}@${teamName}`;
}

/**
 * Parse a deterministic agent id back into its parts, splitting on the FIRST `@`
 * (mirrors the reference's `parseAgentId`). Splitting on the first `@` means a team
 * name may itself contain `@` while the agent name (which never does) is always
 * recovered intact. Returns null when there is no `@` separator (not one of our
 * ids), so callers can treat a bare name/opaque id as unparseable rather than
 * crash.
 */
export function parseAgentId(agentId: string): { agentName: string; teamName: string } | null {
  const at = agentId.indexOf('@');
  if (at === -1) return null;
  return { agentName: agentId.slice(0, at), teamName: agentId.slice(at + 1) };
}

/** Max length of a user-supplied team name (mirrors the reference's identifier bounds). */
export const MAX_TEAM_NAME_LEN = 64;

/**
 * Max length of the free-text team description persisted to disk. Mirrors the reference's
 * cap on the analogous whenToUse/description field (5000 chars): the description is
 * written verbatim into team.json, so an unbounded value is an on-disk size/DoS gap.
 */
export const MAX_TEAM_DESCRIPTION_LEN = 5000;

/**
 * Normalize a user-supplied team name into a filesystem-safe slug that becomes a
 * directory component (parity: the reference's `sanitizeName` in
 * utils/swarm/teamHelpers.ts, which replaces every unsafe char with `-` and uses
 * the result rather than rejecting it). Human-friendly names like `"My Team!"` are
 * ACCEPTED and normalized to `"My-Team"` instead of failing validation.
 *
 * AMI superset (kept, not dropped): an already-valid name is preserved VERBATIM —
 * `_`, `-`, and letter CASE survive (the reference would lowercase and collapse `_`→`-`),
 * so existing team dirs/paths are unchanged.
 *
 * Security (preserved): the slug is always a single, self-contained path component
 * — every path separator, dot, and other unsafe char is mapped to `-`, so a
 * traversal attempt like `"../../../etc/evil"` normalizes to a contained
 * `"etc-evil"` that can never escape the teams dir. Leading/trailing separators are
 * trimmed so the slug starts and ends with an alphanumeric (no flag-like `-foo`),
 * and the length is bounded. Returns the slug, or null when it normalizes to empty
 * (e.g. `""` or `"..."`) or exceeds {@link MAX_TEAM_NAME_LEN}. The three regexes are
 * linear/anchored (no nested optionals) so there is no ReDoS surface.
 */
export function sanitizeTeamName(name: string): string | null {
  const slug = name
    .replace(/[^a-zA-Z0-9_-]/g, '-') // map every unsafe char to a hyphen (parity: sanitizeName)
    .replace(/-+/g, '-')             // collapse inserted hyphen runs
    .replace(/^[-_]+|[-_]+$/g, '');  // trim leading/trailing separators (start/end alphanumeric)
  if (!slug) return null;            // empty after normalization ("" / "...") -> reject
  if (slug.length > MAX_TEAM_NAME_LEN) return null;
  return slug;
}

/**
 * Teammate execution backend (parity: the reference's swarm `BackendType`): the
 * mechanism a teammate runs under. `tmux`/`iterm2` are terminal-pane backends;
 * `in-process` runs a teammate inside the same Node process with an isolated
 * context. Persisted per-member so a resumed session knows how each teammate runs.
 */
export type BackendType = (typeof BACKEND_TYPES)[number];

export const BACKEND_TYPES = ['tmux', 'iterm2', 'in-process'] as const;

/**
 * Type guard for a valid {@link BackendType} (parity: the reference validates persisted
 * backend fields against a known set when deserializing a team file). Any value
 * outside the known set — including `undefined`, a number, or a stale enum name —
 * returns false so the caller can coerce it away rather than trust bad data.
 */
export function isBackendType(v: unknown): v is BackendType {
  return typeof v === 'string' && (BACKEND_TYPES as readonly string[]).includes(v);
}

/**
 * Type guard for a PANE-based backend only (parity: the reference's `isPaneBackend`,
 * used when projecting a teammate's status — a non-pane or unknown backend yields
 * `undefined` rather than a bogus pane backend). `in-process` is deliberately NOT a
 * pane backend.
 */
export function isPaneBackend(v: unknown): v is 'tmux' | 'iterm2' {
  return v === 'tmux' || v === 'iterm2';
}

/**
 * A rich per-member record in the team file, mirroring the reference's team config
 * member schema: a stable identity plus the operating knobs a teammate runs
 * under (model, working dir, subscriptions, per-teammate permission mode) and an
 * idle/active flag used by the delete guard.
 */
export interface TeamMember {
  name: string;
  agentId: string;
  agentType?: string;
  model?: string;
  cwd?: string;
  subscriptions?: string[];
  permissionMode?: string;
  /** Presentational display color tinting this teammate's badge/output so
   *  concurrent agents are distinguishable (parity: the reference's per-member
   *  `color`). Drawn round-robin from the shared {@link AGENT_COLORS} palette on
   *  spawn ({@link assignTeammateColor}), or set explicitly at declaration. */
  color?: AgentColorName;
  /** Absolute path of the git worktree this teammate runs in, when it was spawned
   *  into a dedicated worktree. Removed on team_delete (parity: the reference's
   *  cleanupTeamDirectories tears down per-teammate worktrees, not just dirs). */
  worktreePath?: string;
  /** Execution backend this teammate runs under (parity: the reference's per-member
   *  `backendType`). Validated against {@link BackendType} on read; an unknown
   *  persisted value is coerced to undefined. */
  backendType?: BackendType;
  /** Terminal pane id this teammate renders into for a PANE backend (parity:
   *  the reference's per-member `tmuxPaneId`). Used to derive the member's hidden
   *  state against the team's {@link TeamFile.hiddenPaneIds} set and to remove a
   *  member by its pane ({@link removeMemberByPaneId}). Absent for an in-process
   *  teammate (which has no pane). Validated to a non-empty string on read. */
  tmuxPaneId?: string;
  joinedAt: string;
  /** false => idle/finished/crashed; undefined/true => active. */
  isActive?: boolean;
  /** ISO timestamp of when the member last went idle (set when isActive flips to false). */
  idleSince?: string;
}

/**
 * The derived run-state of a teammate, mirroring the reference's `TeammateStatus`
 * (teamDiscovery.ts). Projected from a {@link TeamMember} on read: `isActive`
 * defaults to active when undefined, so an absent flag maps to `'running'`.
 */
export interface TeammateStatus {
  name: string;
  agentId: string;
  agentType?: string;
  model?: string;
  status: 'running' | 'idle';
  mode?: string;
  cwd?: string;
  /** Display color for this teammate (parity: the reference's TeammateStatus.color),
   *  surfaced so a roster/renderer can tint each agent consistently. */
  color?: AgentColorName;
  /** The teammate's execution backend, limited to a PANE backend (parity: the reference's
   *  TeammateStatus.backendType, built via isPaneBackend — a non-pane/unknown
   *  backend is surfaced as undefined). */
  backendType?: 'tmux' | 'iterm2';
  /** The teammate's terminal pane id (parity: the reference's TeammateStatus.tmuxPaneId),
   *  surfaced so a renderer can target the pane. Undefined for an in-process
   *  teammate. */
  tmuxPaneId?: string;
  /** Whether this teammate's pane is hidden in the roster view (parity: the reference's
   *  TeammateStatus.isHidden, derived from the team's `hiddenPaneIds` set). A
   *  teammate with no pane id is never hidden. */
  isHidden: boolean;
  idleSince?: string;
}

/**
 * Aggregate counts for a team, mirroring the reference's `TeamSummary`. The lead is
 * excluded from every count (membership-role boundary), so the figures describe
 * the worker roster only.
 */
export interface TeamSummary {
  name: string;
  memberCount: number;
  runningCount: number;
  idleCount: number;
}

/**
 * A declarative team-wide allowed-path entry (mirrors the reference's
 * `teamAllowedPaths` element `{ path, toolName }`). On teammate startup each entry
 * is converted into a session allow-rule that grants `toolName` under `path`
 * (see {@link teamAllowedPathsToRules}). `toolName` defaults to `'*'` (every tool)
 * when the caller supplied a bare path string.
 */
export interface TeamAllowedPath {
  path: string;
  toolName: string;
}

/**
 * The persisted team file. 1:1 with a team-scoped task list (the team name is the
 * task-list id). Carries the lead's identity/session plus declarative team-wide
 * tool permissions (teamAllowedPaths).
 */
export interface TeamFile {
  name: string;
  description?: string;
  leadSessionId?: string;
  leadAgentId: string;
  /** Declarative team-wide allowed paths inherited by every spawned teammate as
   *  session allow-rules (parity: the reference's `teamAllowedPaths`). Stored on the
   *  team file so a teammate spawned into the team is scoped the same way. */
  teamAllowedPaths?: TeamAllowedPath[];
  /** Declarative team-wide tool allowlist inherited by every teammate (parity:
   *  the reference's per-agent `tools` scoping knob). Stored on the team file so a
   *  teammate spawned into the team can be constrained the same way the lead is. */
  teamAllowedTools?: string[];
  /** Pane ids the user has collapsed/hidden in the roster view (parity: the reference
   *  Code's team-file `hiddenPaneIds` config knob). A member whose
   *  {@link TeamMember.tmuxPaneId} is in this set projects to `isHidden: true`
   *  ({@link getTeammateStatuses}). Validated to a string array on read; a member
   *  removal scrubs its pane from here ({@link removeMemberByPaneId}). */
  hiddenPaneIds?: string[];
  members: TeamMember[];
  createdAt: string;
}

/**
 * This process's runtime team-join context (parity: the reference's
 * `dynamicTeamContext` in utils/teammate.ts). It carries not just the bound
 * agentId + agentName but the richer per-session knobs a joined teammate needs:
 * the `teamName` it joined, its display `color`, whether `planModeRequired` is in
 * force, and the lead's `parentSessionId` for transcript correlation. Installed
 * when this session joins/creates a team at runtime and taking precedence over the
 * env-var fallback below. The in-process {@link TeammateContext} (AsyncLocalStorage)
 * takes precedence over THIS, so concurrent in-process teammates stay isolated.
 */
export interface DynamicTeamContext {
  agentId: string;
  agentName: string;
  teamName?: string;
  color?: AgentColorName;
  planModeRequired: boolean;
  parentSessionId?: string;
}

/**
 * The team roles a session can hold (parity: the reference's leader-vs-teammate role
 * distinction — `isTeamLead` / `isLeader = !agentId` in swarm/reconnection.ts and the
 * 'team-lead' vs teammate split surfaced in TeamStatus.tsx). Exposed as a named enum
 * (not just the separate {@link isLeader}/{@link isTeammate} booleans) so role-aware
 * diagnostics/status wording has one canonical vocabulary, and so the set is
 * registerable in the introspection surface.
 */
export const TEAM_ROLES = ['lead', 'teammate'] as const;

export type TeamRole = typeof TEAM_ROLES[number];

/**
 * A fully-resolved snapshot of THIS session's swarm identity (parity: the reference's
 * `getDynamicTeamContext` "for inspection/debugging" aggregate, combined with
 * `getAgentId`/`getAgentName`/`getTeamName`/`getTeammateColor`/`isTeammate`).
 * Unlike {@link getDynamicTeamContext} — which exposes ONLY the runtime dynamic
 * layer — this folds in the full precedence chain (in-process ALS > dynamic > env)
 * for every field and the derived role flags, so a single call answers the whole
 * "who am I in this swarm?" question: am I a teammate, which agent/team/color am I,
 * am I the lead, and am I running in-process. Always returns a record; identity
 * fields are undefined for a standalone, non-team session.
 */
export interface SelfIdentity {
  agentId?: string;
  agentName?: string;
  teamName?: string;
  color?: AgentColorName;
  planModeRequired: boolean;
  parentSessionId?: string;
  isTeammate: boolean;
  isLeader: boolean;
  /** This session's single team role (parity: the reference's leader-vs-teammate
   *  distinction), consolidating {@link isLeader}/{@link isTeammate}: `'lead'`,
   *  `'teammate'`, or `'none'` for a standalone session. */
  role: TeamRole | 'none';
  isInProcess: boolean;
  /** Whether this session may self-approve a permission request (true for a lead or
   *  solo session) or must escalate it to the team leader (false for a non-lead
   *  worker) — parity: the reference's WorkerPendingPermission gate. Resolved via
   *  {@link canSelfApprovePermission}. */
  canSelfApprovePermission: boolean;
}

/**
 * Deterministic, reproducible lead agent id derived from the team name. Uses the
 * canonical `agentName@teamName` scheme ({@link formatAgentId}) so the lead's id
 * is consistent with every other agent id and request id in the swarm subsystem
 * (send-message's structured request ids embed an `agentName@teamName` agent id).
 */
export function leadAgentIdFor(teamName: string): string {
  return formatAgentId(TEAM_LEAD_NAME, teamName);
}

/**
 * Resolve a team member by a bare name, a stored agentId, OR a deterministic
 * `agentName@teamName` id for THIS team ({@link parseAgentId}). The deterministic
 * fallback lets a lead address a teammate by its computed id even when the member
 * was registered under an opaque spawn/task id. Single canonical lookup shared by
 * every per-member mutation below (no duplicated `.find` predicates).
 */
export function findMember(team: TeamFile, nameOrId: string): TeamMember | undefined {
  const direct = team.members.find(m => m.name === nameOrId || m.agentId === nameOrId);
  if (direct) return direct;
  const parsed = parseAgentId(nameOrId);
  if (parsed && parsed.teamName === team.name) {
    return team.members.find(m => m.name === parsed.agentName);
  }
  return undefined;
}

export function teamFilePath(dir: string): string {
  return path.join(dir, 'team.json');
}

/**
 * Absolute path of the workspace teams root (`<cwd>/.ami/teams`). The single place
 * the `.ami/teams` segment is spelled, so every team-path helper derives from it
 * (parity: the reference's `getTeamsDir()` in swarm/teamHelpers.ts).
 */
export function getTeamsRoot(cwd: string): string {
  return path.join(cwd, '.ami', 'teams');
}

/**
 * THE canonical name→directory mapping for a team: sanitize the (possibly
 * untrusted) name and join it under {@link getTeamsRoot}. This is the single way a
 * team name becomes a path, so the {@link sanitizeTeamName} guard is enforced on
 * EVERY access — the write path (team_create) AND, crucially, the read/resume path
 * (a name rehydrated from a persisted transcript or team file). Returns null when
 * the name normalizes to empty / is over length (parity: the reference's
 * `getTeamDir(teamName) = join(getTeamsDir(), sanitizeName(teamName))`, whose
 * `readTeamFile`/`getTeamFilePath` apply sanitizeName on every read). Routing all
 * read/resume/reconnect code through here is defense-in-depth: a traversal name
 * read back from state (e.g. `"../../../etc/evil"`) can never be joined verbatim.
 */
export function getTeamDir(cwd: string, name: string): string | null {
  const safe = sanitizeTeamName(name);
  if (!safe) return null;
  return path.join(getTeamsRoot(cwd), safe);
}
