import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { withFileLock } from '../utils/file-lock';
import { TEAM_LEAD_NAME, purgeMailbox } from './send-message';
import { PERMISSION_MODES, normalizePermissionMode, PermissionMode, PermissionRule } from '../permissions';
import { unassignTeammateTasks, setTaskPersistPath, deleteTaskList } from './task-tracker';
import './agent-name';
import { log as coreLog } from '../logger';
import { gitExec } from '../worktree-manager';
import { isTeammatesEnabled } from './tool-gates';
import { AGENT_COLORS, AgentColorName, parseAgentColor } from '../skills';
import { getTeammateContext, isInProcessTeammate } from './teammate-context';
import { ENV_AGENT_ID, ENV_AGENT_NAME, ENV_CC_AGENT_ID, ENV_CC_PARENT_SESSION_ID, ENV_COLOR, ENV_PLAN_MODE_REQUIRED, ENV_TEAM_NAME, MAX_TEAM_DESCRIPTION_LEN, MAX_TEAM_NAME_LEN, envBool, envIdentity, findMember, formatAgentId, getTeamDir, getTeamsRoot, isPaneBackend, leadAgentIdFor, sanitizeTeamName, teamDebug, teamFilePath } from './team-team-member';
import type { DynamicTeamContext, SelfIdentity, TeamFile, TeamMember, TeamRole, TeamSummary, TeammateStatus } from './team-team-member';
import { TEAM_SCRATCHPAD_SUBDIR, findTeamMemberByName, getNextPermissionMode, loadTeamFromDisk, parseDeclaredMembers, parseTeamAllowedPaths, readTeamFile } from './team-read-team-file';
export { BACKEND_TYPES, TEAM_ROLES, formatAgentId, getTeamDir, getTeamsRoot, isBackendType, isPaneBackend, parseAgentId } from './team-team-member';
export type { BackendType, DynamicTeamContext, SelfIdentity, TeamAllowedPath, TeamFile, TeamMember, TeamRole, TeamSummary, TeammateStatus } from './team-team-member';
export { TEAM_SCRATCHPAD_SUBDIR, findTeamMemberByName, getNextPermissionMode, getTeamFilePath, loadTeamFromDisk, readTeamFile } from './team-read-team-file';

/**
 * Round-robin teammate color assignment (parity: the reference's
 * `teammateLayoutManager` assignTeammateColor/getTeammateColor/clearTeammateColors).
 * Each distinct teammate id is tinted with the next color from the shared
 * {@link AGENT_COLORS} palette (reused from skills.ts — NOT a second palette) so
 * concurrent agents are visually distinguishable in the roster/output. The
 * assignment is idempotent per id (a re-assign returns the same color) and the
 * lifecycle is reset on team teardown ({@link clearTeammateColors}).
 */
const teammateColorAssignments = new Map<string, AgentColorName>();
let teammateColorIndex = 0;

/**
 * Assign (or return the already-assigned) display color for a teammate id,
 * advancing round-robin through {@link AGENT_COLORS}. Idempotent: calling twice for
 * the same id yields the same color and does not consume a new palette slot.
 */
export function assignTeammateColor(teammateId: string): AgentColorName {
  const existing = teammateColorAssignments.get(teammateId);
  if (existing) return existing;
  const color = AGENT_COLORS[teammateColorIndex % AGENT_COLORS.length];
  teammateColorAssignments.set(teammateId, color);
  teammateColorIndex += 1;
  return color;
}

/** Look up a teammate id's assigned color, or undefined when none was assigned. */
export function getTeammateColor(teammateId: string): AgentColorName | undefined {
  return teammateColorAssignments.get(teammateId);
}

/**
 * Reset all teammate color assignments (parity: clearTeammateColors, called during
 * team cleanup so a fresh team starts the palette from the top).
 */
export function clearTeammateColors(): void {
  teammateColorAssignments.clear();
  teammateColorIndex = 0;
}

let currentTeam: TeamFile | null = null;
let currentTeamDir: string | null = null;

/**
 * In-flight {@link persistTeam} writes. The file-locked save is fire-and-forget, so
 * this set lets {@link flushTeamPersists} deterministically await the debounced
 * writes to land on disk instead of a caller racing a wall-clock sleep. Entries
 * self-remove once each write settles.
 */
const inFlightTeamPersists = new Set<Promise<void>>();

/**
 * Resolve once every currently in-flight {@link persistTeam} write has settled
 * (committed, or fallen back on a lock failure). Because the locked save is
 * otherwise fire-and-forget, a caller that must OBSERVE the persisted team on disk
 * — reading the team file back, or flushing before process exit — awaits this
 * rather than a fixed delay. A no-op (already resolved) when nothing is pending.
 */
export function flushTeamPersists(): Promise<void> {
  return Promise.allSettled([...inFlightTeamPersists]).then(() => undefined);
}
let dynamicTeamContext: DynamicTeamContext | null = null;

/**
 * Bind this process's agent identity within the active team (parity: the minimal
 * identity install). Preserves any richer context fields (color/planMode/parent)
 * already present so a plain id/name rebind does not drop them, and defaults
 * `teamName` to the active team's name.
 */
export function setSelfIdentity(agentId: string, agentName: string): void {
  dynamicTeamContext = {
    agentId,
    agentName,
    teamName: currentTeam?.name ?? dynamicTeamContext?.teamName,
    color: dynamicTeamContext?.color,
    planModeRequired: dynamicTeamContext?.planModeRequired ?? false,
    parentSessionId: dynamicTeamContext?.parentSessionId,
  };
}

/**
 * Install the FULL runtime team-join context (parity: setDynamicTeamContext),
 * replacing any existing one. Passing null clears it (equivalent to
 * {@link clearDynamicTeamContext}).
 */
export function setDynamicTeamContext(ctx: DynamicTeamContext | null): void {
  dynamicTeamContext = ctx;
}

/** Tear down the runtime team-join context (parity: clearDynamicTeamContext). */
export function clearDynamicTeamContext(): void {
  dynamicTeamContext = null;
}

/**
 * Inspect the current runtime team-join context (parity: getDynamicTeamContext —
 * "for inspection/debugging"). Returns null when this session has not joined a
 * team at runtime; the ALS/env layers are NOT folded in here (use the per-field
 * accessors for the resolved value).
 */
export function getDynamicTeamContext(): DynamicTeamContext | null {
  return dynamicTeamContext;
}

/**
 * This process's resolved agent id, following the identity precedence chain
 * (parity: getAgentId) — in-process {@link TeammateContext} (AsyncLocalStorage) >
 * runtime {@link DynamicTeamContext} > `AMI_TEAMMATE_AGENT_ID` env var >
 * `CLAUDE_CODE_AGENT_ID` env var (Claude-Code compat fallback for an
 * out-of-process teammate). Undefined when none is bound (a standalone, non-team
 * session).
 */
export function getSelfAgentId(): string | undefined {
  return getTeammateContext()?.agentId ?? dynamicTeamContext?.agentId
    ?? envIdentity(ENV_AGENT_ID) ?? envIdentity(ENV_CC_AGENT_ID);
}

/** This process's resolved agent name (same precedence as {@link getSelfAgentId}). */
export function getSelfAgentName(): string | undefined {
  return getTeammateContext()?.agentName ?? dynamicTeamContext?.agentName ?? envIdentity(ENV_AGENT_NAME);
}

/**
 * This process's resolved team name (parity: getTeamName), following ALS >
 * dynamic > env, and finally falling back to the active team's name so a leader
 * (which has no dynamic context by the teammate rule) still resolves a team name.
 */
export function getSelfTeamName(): string | undefined {
  return getTeammateContext()?.teamName ?? dynamicTeamContext?.teamName
    ?? envIdentity(ENV_TEAM_NAME) ?? currentTeam?.name;
}

/**
 * This process's resolved display color (parity: getTeammateColor), following ALS
 * > dynamic > env. Undefined when no color was assigned to this session.
 */
export function getSelfColor(): AgentColorName | undefined {
  const fromCtx = getTeammateContext()?.color ?? dynamicTeamContext?.color;
  if (fromCtx) return fromCtx;
  return parseAgentColor(envIdentity(ENV_COLOR)) ?? undefined;
}

/**
 * This process's resolved plan-mode-required flag (parity: the reference's
 * `isPlanModeRequired`), following the identity precedence chain in-process
 * {@link TeammateContext} (AsyncLocalStorage) > runtime {@link DynamicTeamContext} >
 * the `AMI_TEAMMATE_PLAN_MODE_REQUIRED` env var (parity: the reference's
 * `CLAUDE_CODE_PLAN_MODE_REQUIRED` fallback, so a spawned teammate process inherits
 * the gate). Defaults to false when nothing is bound — a standalone session is
 * never forced into plan mode.
 */
export function getSelfPlanModeRequired(): boolean {
  return getTeammateContext()?.planModeRequired
    ?? dynamicTeamContext?.planModeRequired
    ?? envBool(ENV_PLAN_MODE_REQUIRED)
    ?? false;
}

/**
 * This process's resolved parent (lead) session id (parity: the reference's
 * `getParentSessionId`), following ALS > dynamic > `CLAUDE_CODE_PARENT_SESSION_ID`
 * env var (Claude-Code compat for an out-of-process teammate) > the active team's
 * `leadSessionId`. The explicitly injected env value wins over the local team
 * fallback so a separately-spawned process correlates to the real lead that
 * launched it. Lets a teammate correlate its transcript back to the lead's
 * session. Undefined for a standalone session with no team lineage.
 */
export function getSelfParentSessionId(): string | undefined {
  return getTeammateContext()?.parentSessionId ?? dynamicTeamContext?.parentSessionId
    ?? envIdentity(ENV_CC_PARENT_SESSION_ID) ?? currentTeam?.leadSessionId;
}

/**
 * Whether THIS session is running as a (non-lead) teammate in a swarm (parity:
 * isTeammate). True when executing inside an in-process teammate scope, OR when a
 * bound id+team resolves to an agent that is NOT the team lead. The lead — which
 * also binds a self identity on create — is deliberately NOT a teammate.
 */
export function isTeammate(): boolean {
  if (isInProcessTeammate()) return true;
  const myId = dynamicTeamContext?.agentId ?? envIdentity(ENV_AGENT_ID) ?? envIdentity(ENV_CC_AGENT_ID);
  const teamName = dynamicTeamContext?.teamName ?? envIdentity(ENV_TEAM_NAME) ?? currentTeam?.name;
  if (!myId || !teamName) return false;
  if (currentTeam && myId === currentTeam.leadAgentId) return false;
  return true;
}

/** Clear the bound self identity (team teardown / reset). */
export function clearSelfIdentity(): void {
  dynamicTeamContext = null;
}

/**
 * Whether THIS process is the team's lead (parity: the reference's `isTeamLead`).
 * A session is the lead when the team has a `leadAgentId` AND either our bound
 * self id matches it, OR no self id is bound (backwards compat: the original
 * session that created the team before an agent id was assigned). Returns false
 * when there is no team / no lead id, or our id is a non-lead teammate's.
 */
export function isLeader(team: TeamFile | null = currentTeam): boolean {
  if (!team?.leadAgentId) return false;
  const myId = getSelfAgentId();
  if (myId === team.leadAgentId) return true;
  if (!myId) return true; // backwards compat: the original creator session
  return false;
}

/**
 * This session's single resolved team role (parity: the reference's leader-vs-teammate
 * role distinction). Consolidates {@link isLeader}/{@link isTeammate} into one named
 * value: `'lead'` when this session is the team lead, `'teammate'` when it is a
 * non-lead teammate, and `'none'` for a standalone session with no team lineage.
 * Reuses the existing role predicates so it never drifts from them (no duplicated
 * resolution).
 */
export function getSelfRole(): TeamRole | 'none' {
  if (isLeader()) return 'lead';
  if (isTeammate()) return 'teammate';
  return 'none';
}

/**
 * Whether THIS session should see the persistent, shared task-list panel (parity:
 * the reference's `useTasksV2` visibility gate `!teamContext || isTeamLead(teamContext)`
 * in hooks/useTasksV2.ts). Inside a team ONLY the LEAD's UI surfaces the shared
 * task list — the lead coordinates the list while a non-lead teammate acts on its
 * own assignments and should not have the whole roster's task panel pinned in its
 * view. A standalone (non-team) session always sees it. This is the model-facing
 * `task_tracker` tool's own `list`/`get` output being deliberately left untouched —
 * a teammate can still QUERY tasks; only the passive always-on panel is role-gated.
 *
 * Reuses the single resolved role ({@link getSelfRole}) so it never drifts from
 * {@link isLeader}/{@link isTeammate} (no duplicate role resolution): visible for
 * `'lead'` and `'none'`, hidden only for `'teammate'`.
 */
export function isTaskListVisibleForSelf(): boolean {
  return getSelfRole() !== 'teammate';
}

/**
 * Whether THIS session may SELF-APPROVE a permission request, or must escalate it to
 * the human-supervised team leader (parity: the reference's `WorkerPendingPermission` —
 * a worker cannot self-grant a tool; its request is withheld in a pending state and
 * routed to the lead, who decides). The privilege-separation rule is one line: a
 * non-lead teammate (worker) can NEVER self-approve, so this is true only for a
 * `'lead'` session or a standalone `'none'` session. Reuses the single resolved role
 * ({@link getSelfRole}) so it never drifts from {@link isTeammate}/{@link isLeader}
 * (no duplicate role resolution). This is the functional essence of the pending-on-
 * leader gate, surfaced on the cohesive {@link getSelfIdentity} introspection aggregate.
 */
export function canSelfApprovePermission(): boolean {
  return getSelfRole() !== 'teammate';
}

/**
 * Resolve this session's full swarm identity through the precedence chain — the
 * single cohesive introspection surface (parity: the reference's inspection aggregate).
 * Reuses the per-field accessors (no duplicated resolution logic) so it never drifts
 * from {@link getSelfAgentId} / {@link isTeammate} / {@link isLeader} et al.
 */
export function getSelfIdentity(): SelfIdentity {
  return {
    agentId: getSelfAgentId(),
    agentName: getSelfAgentName(),
    teamName: getSelfTeamName(),
    color: getSelfColor(),
    planModeRequired: getSelfPlanModeRequired(),
    parentSessionId: getSelfParentSessionId(),
    isTeammate: isTeammate(),
    isLeader: isLeader(),
    role: getSelfRole(),
    isInProcess: isInProcessTeammate(),
    canSelfApprovePermission: canSelfApprovePermission(),
  };
}

/**
 * Dependency seam for the destructive worktree teardown in {@link teamDeleteTool}
 * (mirrors `_exitWorktreeHooks`). `exec` is worktree-manager's hardened, no-shell
 * git runner, so production behaviour is identical to the exit-worktree path and a
 * teammate worktree path can never be a shell metacharacter. Tests inject a mock
 * to exercise removal + the rmSync/prune fallback without touching real git.
 */
export const _teamHooks = {
  exec: gitExec,
  existsSync: fs.existsSync as typeof fs.existsSync,
  rmSync: fs.rmSync as typeof fs.rmSync,
  mkdirSync: fs.mkdirSync as typeof fs.mkdirSync,
  writeFileSync: fs.writeFileSync as typeof fs.writeFileSync,
};

/**
 * Remove a single teammate's git worktree (parity: the reference's
 * cleanupTeamDirectories removes per-teammate worktrees). Mirrors the exit-worktree
 * teardown: `git worktree remove --force`, falling back to an fs removal + prune
 * when git's own removal fails. Best-effort — returns true when the worktree is
 * gone (by either path), false when both strategies fail.
 */
function removeMemberWorktree(worktreePath: string, repoCwd: string): boolean {
  try {
    _teamHooks.exec(['worktree', 'remove', '--force', worktreePath], { cwd: worktreePath, stdio: 'pipe' });
    return true;
  } catch {
    try {
      if (_teamHooks.existsSync(worktreePath)) {
        _teamHooks.rmSync(worktreePath, { recursive: true, force: true });
      }
      _teamHooks.exec(['worktree', 'prune'], { cwd: repoCwd, stdio: 'ignore' });
      return true;
    } catch {
      return false;
    }
  }
}

/** Persist the current team file under a cross-process lock. */
function persistTeam(): void {
  if (!currentTeam || !currentTeamDir) return;
  const file = teamFilePath(currentTeamDir);
  const snapshot = currentTeam;
  // Low-frequency mutation; lock to stay consistent with concurrent sessions.
  const write = withFileLock(file, () => {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, JSON.stringify(snapshot, null, 2));
  }).catch(() => {
    // Best-effort: fall back to a direct write so a lock failure never drops state.
    try { fs.writeFileSync(file, JSON.stringify(snapshot, null, 2)); } catch { /* ignore */ }
  });
  // Track the fire-and-forget write so {@link flushTeamPersists} can await it.
  inFlightTeamPersists.add(write);
  void write.finally(() => inFlightTeamPersists.delete(write));
}

/**
 * Rehydrate the in-memory team singleton from a loaded team file (resumed
 * session). Exposed so reconnection logic can restore state after a process
 * restart without re-creating directories.
 */
export function restoreTeam(team: TeamFile, dir: string): void {
  currentTeam = team;
  currentTeamDir = dir;
}

/**
 * Rebuild this session's team context from a resumed session (parity: the reference
 * Code's `initializeTeammateContextFromSession` / `computeInitialTeamContext` in
 * swarm/reconnection.ts). Given the `teamName` + `agentName` persisted in the
 * transcript, it reads the team file off disk, rehydrates the in-memory singleton
 * ({@link restoreTeam}), resolves this agent's deterministic id from the roster,
 * rebinds the team-scoped task list, and installs the runtime
 * {@link DynamicTeamContext} with the resolved self identity + leader flag. Emits an
 * observable `[Reconnection] …` diagnostic for success, a missing-team-file
 * failure, and the removed-member edge, so a resumed team session is never silently
 * lost. Returns the installed context, or null when the team file is missing/corrupt.
 */
export function loadTeamFromSession(cwd: string, teamName: string, agentName: string): DynamicTeamContext | null {
  const loaded = loadTeamFromDisk(cwd, teamName);
  if (!loaded) {
    teamDebug(`[Reconnection] Could not read team file for ${teamName} (agent: ${agentName})`);
    return null;
  }
  restoreTeam(loaded.team, loaded.dir);
  const member = findTeamMemberByName(loaded.team, agentName);
  if (!member) {
    teamDebug(`[Reconnection] Member ${agentName} not found in team ${teamName} - may have been removed`);
  }
  const agentId = member?.agentId ?? formatAgentId(agentName, loaded.team.name);
  const isLead = agentName === TEAM_LEAD_NAME || agentId === loaded.team.leadAgentId;
  const ctx: DynamicTeamContext = {
    agentId,
    agentName,
    teamName: loaded.team.name,
    color: member?.color,
    planModeRequired: false,
    parentSessionId: loaded.team.leadSessionId,
  };
  setDynamicTeamContext(ctx);
  // Rebind the shared, team-scoped task list (Team = Project = TaskList) so
  // task_tracker resolves to this team after the restart.
  setTaskPersistPath(cwd, loaded.team.name);
  teamDebug(
    `[Reconnection] Initialized agent context from session for ` +
    `${isLead ? `leader ${agentName}` : `teammate ${agentName}`} in team ${loaded.team.name}`,
  );
  return ctx;
}

/**
 * Teams created THIS session that were not explicitly disbanded (parity: the reference
 * Code's `sessionCreatedTeams` Set + registerTeamForSessionCleanup). On an abnormal
 * exit (SIGINT/SIGTERM/crash) these are swept by {@link cleanupSessionTeams} so a
 * `.ami/teams/<name>/` directory is never orphaned. An explicit team_delete
 * unregisters its team ({@link unregisterTeamForSessionCleanup}) to avoid a
 * double-cleanup. Reset between tests via {@link resetTeam}.
 */
const sessionCreatedTeams = new Set<string>();

/** Mark a team created this session for orphan-cleanup on exit. */
export function registerTeamForSessionCleanup(name: string): void {
  sessionCreatedTeams.add(name);
}

/** Drop a team from session-cleanup tracking (already cleaned via team_delete). */
export function unregisterTeamForSessionCleanup(name: string): void {
  sessionCreatedTeams.delete(name);
}

/** The teams currently registered for session cleanup (introspection/tests). */
export function getSessionCreatedTeams(): string[] {
  return Array.from(sessionCreatedTeams);
}

/**
 * Remove every team dir created this session that was never explicitly deleted
 * (parity: the reference's `cleanupSessionTeams`, registered with gracefulShutdown).
 * For each tracked team it tears down per-member worktrees FIRST (so teammate
 * processes are not orphaned behind a deleted dir), then removes the team directory
 * and its 1:1 task list. Best-effort and idempotent: a per-team failure is logged
 * and skipped, the tracking Set is cleared afterward, and returns the names whose
 * directory was removed. A no-op when nothing was registered.
 */
export function cleanupSessionTeams(cwd: string): string[] {
  if (sessionCreatedTeams.size === 0) return [];
  const names = Array.from(sessionCreatedTeams);
  teamDebug(`cleanupSessionTeams: removing ${names.length} orphan team dir(s): ${names.join(', ')}`);
  const removed: string[] = [];
  for (const name of names) {
    // Route through the canonical getTeamDir so a tracked name is re-sanitized
    // before it is turned into a path to remove — a corrupt entry can never cause
    // an rmSync outside the teams root. A name that cannot normalize is skipped.
    const dir = getTeamDir(cwd, name);
    if (!dir) {
      teamDebug(`cleanupSessionTeams: skipping un-sanitizable team name "${name}"`);
      continue;
    }
    // Tear down per-member worktrees before the dir (parity ordering).
    const tf = readTeamFile(dir);
    if (tf) {
      for (const m of tf.members) {
        if (m.worktreePath) removeMemberWorktree(m.worktreePath, cwd);
      }
    }
    try {
      if (_teamHooks.existsSync(dir)) {
        _teamHooks.rmSync(dir, { recursive: true, force: true });
        removed.push(name);
        teamDebug(`cleanupSessionTeams: removed orphan team dir ${dir}`);
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      teamDebug(`cleanupSessionTeams: failed to remove ${dir}: ${msg}`);
    }
    // Remove the team's 1:1 task list too (independent of dir removal).
    deleteTaskList(cwd, name);
  }
  sessionCreatedTeams.clear();
  return removed;
}

/**
 * Convert a team's declarative {@link TeamAllowedPath} entries into session
 * allow-{@link PermissionRule}s (mirrors the reference's teammateInit path→rule
 * conversion). The subtle edge is the pattern shape: an ABSOLUTE path (leading
 * `/`) becomes `/<path>/**` (the characteristic double-slash — a `//abs/**`
 * pattern), while a RELATIVE path becomes `<path>/**`. Each rule grants the
 * entry's `toolName` (`'*'` = every tool). Applied to a teammate's engine on spawn
 * so every teammate inherits a consistent, team-scoped path allowlist. Returns []
 * when the team has no allowed paths (defaults to the active team).
 */
export function teamAllowedPathsToRules(team: TeamFile | null = currentTeam): PermissionRule[] {
  if (!team || !team.teamAllowedPaths || team.teamAllowedPaths.length === 0) return [];
  return team.teamAllowedPaths.map(({ path: p, toolName }): PermissionRule => ({
    tool: toolName,
    pattern: p.startsWith('/') ? `/${p}/**` : `${p}/**`,
    action: 'allow',
  }));
}

/**
 * Absolute path of a team's shared scratchpad dir (its `memory/` subdir), derived
 * deterministically from the workspace `cwd` + team name — so a disk-loaded team
 * resolves the same path a live (in-memory) one does, independent of the module's
 * `currentTeamDir` singleton. Returns null when there is no team. This is the
 * durable location workers persist cross-teammate knowledge to.
 */
export function getTeamScratchpadDir(cwd: string, team: TeamFile | null = currentTeam): string | null {
  if (!team) return null;
  // team.name may come from a disk-loaded file; route through the canonical
  // getTeamDir so the scratchpad path is re-sanitized at this read boundary too.
  const dir = getTeamDir(cwd, team.name);
  if (!dir) return null;
  return path.join(dir, TEAM_SCRATCHPAD_SUBDIR);
}

/**
 * Session allow-{@link PermissionRule}s granting EVERY tool prompt-free access to
 * the team's shared scratchpad dir (parity: the reference's coordinator gated
 * "no permission prompt" `scratchpadDir` for durable cross-worker knowledge).
 * Applied to a teammate's engine on spawn ALONGSIDE {@link teamAllowedPathsToRules}
 * so workers can persist and share team state without interrupting the human for a
 * permission decision. The grant is scoped strictly to the team's own scratchpad
 * dir (`<scratchpad>/**`), never a broader path. Returns [] when there is no team.
 */
export function teamScratchpadRules(cwd: string, team: TeamFile | null = currentTeam): PermissionRule[] {
  const dir = getTeamScratchpadDir(cwd, team);
  if (!dir) return [];
  return [{ tool: '*', pattern: `${dir}/**`, action: 'allow' }];
}

export const teamCreateTool: ToolDefinition = {
  name: 'team_create',
  // PARITY (additive): declarative per-tool gate for the teammate subsystem.
  isEnabled: () => isTeammatesEnabled(),
  // Concept synonyms for tool_search: neither "swarm" nor "multi-agent" appears in
  // the human name, so without this hint a tool_search for those terms could not
  // surface team_create. Parity: the reference's `searchHint: 'create a multi-agent swarm team'`.
  searchHint: 'create a multi-agent swarm team collaborate parallel agents',
  // Self-declared deferral: keep team_create OUT of the default advertised set and
  // surface it on demand via tool_search (it is a specialized orchestration entry
  // point most turns never need; its schema is large). Honored by the engine's
  // per-tool deferral selection regardless of the global deferTools policy. Parity:
  // the reference's `shouldDefer: true` on TeamCreateTool.
  shouldDefer: true,
  description:
    'Create a team of agents for collaborative work. A team has a 1:1 correspondence with a task list and ' +
    'shares a memory directory; a team file records each member (name, agentId, agentType, model, per-member ' +
    'permission mode) plus the lead.\n' +
    '\n' +
    'When to use: the user asks for a team/swarm/collaboration, or a task is complex enough to benefit from ' +
    'parallel work by multiple agents (e.g. frontend + backend, refactor while keeping tests green). When in ' +
    'doubt, prefer a team.\n' +
    '\n' +
    'Choosing teammate types: spawn teammates with the task tool using a matching name and a subagent_type ' +
    'that matches the work — read-only agents (explore) only for research/search/planning, full-capability ' +
    'agents for work that edits files.\n' +
    '\n' +
    'Workflow: (1) create the team; (2) create tasks with task_tracker — they use the team task list; (3) spawn ' +
    'teammates via task({ name }); (4) assign tasks by owner; (5) teammates complete tasks and go idle between ' +
    'turns (idle is normal — an idle teammate still receives messages and wakes on one; be patient); (6) ' +
    'coordinate via send_message (teammate messages arrive automatically — do not poll); (7) gracefully shut ' +
    'teammates down with a shutdown_request via send_message, then team_delete once all have exited.\n' +
    '\n' +
    'Member discovery & addressing: the team file records every member (name, agentId, agentType) under its ' +
    '`members` list — this is how teammates discover each other. ALWAYS refer to a teammate by its human NAME ' +
    '(the name you spawned it with) — for send_message `to` and for a task `owner` — NEVER by its internal ' +
    'agentId. Pre-declare known teammates up front with the `members` input so the lead can assign them before ' +
    'they are spawned.',
  inputSchema: {
    type: 'object',
    properties: {
      team_name: { type: 'string', description: 'Name for the team. A friendly name (e.g. "My Team") is accepted and normalized to a filesystem-safe slug; it must contain at least one alphanumeric character.' },
      description: { type: 'string', description: 'Description of the team purpose.' },
      agent_type: { type: 'string', description: 'Role/type of the team lead (default "lead").' },
      model: { type: 'string', description: 'Model the lead runs under.' },
      permission_mode: {
        type: 'string',
        description: 'Permission mode the lead runs under (one of the valid permission modes).',
      },
      team_allowed_paths: {
        type: 'array',
        description:
          'Declarative team-wide allowed paths inherited by every spawned teammate as session ' +
          'allow-rules. Each item is either a path string (grants all tools under that path) or ' +
          'an object { path, tool_name } to scope the grant to a single tool.',
        items: {
          // Union item (parity with the reference's teamAllowedPaths): a bare path string
          // OR an object { path, tool_name }.
          type: ['string', 'object'],
          properties: {
            path: { type: 'string' },
            tool_name: { type: 'string' },
          },
        },
      },
      allowed_tools: {
        type: 'array',
        items: { type: 'string' },
        description: 'Declarative team-wide tool allowlist inherited by all teammates.',
      },
      members: {
        type: 'array',
        description:
          'Optional roster of teammates to pre-declare at creation (added idle, awaiting spawn via task). ' +
          'Each item is either a teammate name or an object { name, agent_type?, model?, permission_mode? }.',
        items: {
          // Union item (parity with the reference's union value params): a bare name
          // string OR an object { name, agent_type?, model?, permission_mode? }.
          type: ['string', 'object'],
          properties: {
            name: { type: 'string' },
            agent_type: { type: 'string' },
            model: { type: 'string' },
            permission_mode: { type: 'string' },
            color: { type: 'string', description: `Display color (one of: ${AGENT_COLORS.join(', ')}, or "automatic").` },
            subscriptions: {
              type: 'array',
              items: { type: 'string' },
              description: 'Topics/channels this teammate subscribes to for selective message routing.',
            },
          },
        },
      },
    },
    required: ['team_name'],
    // Strict schema (parity: the reference's team tools use z.strictObject): reject
    // unexpected keys so a typo'd/invented knob fails loudly instead of being
    // silently ignored.
    additionalProperties: false,
  },
  isReadOnly: false,
  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    if (currentTeam) return { output: `Error: Team "${currentTeam.name}" already exists. Delete it first with team_delete.`, isError: true };
    const rawName = input.team_name as string;
    const base = sanitizeTeamName(rawName);
    if (!base) return { output: `Error: team_name must contain at least one alphanumeric character (it is normalized to a filesystem-safe slug) and be at most ${MAX_TEAM_NAME_LEN} characters after normalization.`, isError: true };

    // Bound the free-text description before it is persisted verbatim to team.json.
    const description = input.description as string | undefined;
    if (typeof description === 'string' && description.length > MAX_TEAM_DESCRIPTION_LEN) {
      return { output: `Error: description must be at most ${MAX_TEAM_DESCRIPTION_LEN} characters (got ${description.length}).`, isError: true };
    }

    // Per-agent scoping knob: the lead's permission mode (parity: the reference's agent
    // `permissionMode`). Validate up-front so an invalid value fails fast instead
    // of being persisted as a dead string.
    const rawPermMode = input.permission_mode as string | undefined;
    let leadPermissionMode: PermissionMode | undefined;
    if (rawPermMode !== undefined) {
      const valid = normalizePermissionMode(rawPermMode);
      if (!valid) {
        return { output: `Error: permission_mode must be one of: ${PERMISSION_MODES.join(', ')}.`, isError: true };
      }
      leadPermissionMode = valid;
    }

    // Parse any pre-declared teammate roster (parity: the reference's team members input).
    // Each entry becomes an idle member record awaiting its task-spawn; spawning
    // later reconciles it by name via registerTeamMember. Invalid permission modes
    // abort the whole create so the caller fixes the input rather than getting a
    // silently-dropped knob.
    const parsedMembers = parseDeclaredMembers(input.members);
    if (parsedMembers.error) return { output: `Error: ${parsedMembers.error}`, isError: true };

    // Parse the declarative team-wide allowed paths (parity: the reference's
    // teamAllowedPaths). A malformed entry aborts the create so the caller fixes
    // the knob rather than getting a silently-dropped permission grant.
    const parsedPaths = parseTeamAllowedPaths(input.team_allowed_paths);
    if (parsedPaths.error) return { output: `Error: ${parsedPaths.error}`, isError: true };

    // On-disk collision detection with a unique-name fallback: if a directory for
    // this name already exists (e.g. an orphan from a prior session), pick the
    // next free "name-N" so we never clobber another team's state.
    const teamsRoot = getTeamsRoot(context.cwd);

    // Resumed-session reconnection: if a team directory for this exact name already
    // exists AND its persisted team file belongs to THIS session (same
    // leadSessionId) with an intact lead member, rehydrate it in place instead of
    // forking a renamed duplicate. This lets team state survive a process restart
    // (parity: the reference's swarm reconnection on a resumed session). A foreign or
    // orphaned dir (different session, or a corrupt/partial file whose lead member
    // is unreadable) falls through to the unique-name fallback below.
    const existing = loadTeamFromDisk(context.cwd, base);
    if (existing && existing.team.leadSessionId === context.sessionId
        && findTeamMemberByName(existing.team, TEAM_LEAD_NAME)) {
      restoreTeam(existing.team, existing.dir);
      // Rebind this process's self identity to the lead on reconnect so isLeader()
      // and the whoami introspection resolve correctly after a restart.
      setSelfIdentity(existing.team.leadAgentId, TEAM_LEAD_NAME);
      // Re-bind the shared, team-scoped task list so task_tracker resolves to this
      // team's tasks (parity: Team = Project = TaskList) after the restart too.
      setTaskPersistPath(context.cwd, existing.team.name);
      teamDebug(`Reconnected to existing team ${existing.team.name} (resumed session, ${existing.team.members.length} member(s))`);
      return {
        output: `Reconnected to existing team "${existing.team.name}" (resumed session). ` +
          `Team file: ${teamFilePath(existing.dir)}. Lead agent id: ${existing.team.leadAgentId}. ` +
          `${existing.team.members.length} member(s) restored.`,
      };
    }

    let name = base;
    let n = 1;
    while (fs.existsSync(path.join(teamsRoot, name))) {
      n += 1;
      name = `${base}-${n}`;
    }

    const teamDir = path.join(teamsRoot, name);

    const leadAgentId = leadAgentIdFor(name);
    const now = new Date().toISOString();
    const team: TeamFile = {
      name,
      description,
      leadSessionId: context.sessionId,
      leadAgentId,
      teamAllowedPaths: parsedPaths.paths,
      teamAllowedTools: Array.isArray(input.allowed_tools) ? (input.allowed_tools as string[]) : undefined,
      members: [
        {
          name: TEAM_LEAD_NAME,
          agentId: leadAgentId,
          agentType: (input.agent_type as string) || 'lead',
          model: input.model as string | undefined,
          permissionMode: leadPermissionMode,
          cwd: context.cwd,
          joinedAt: now,
          isActive: true,
        },
        // Pre-declared teammates: idle until spawned (a later task-spawn reconciles
        // the record by name). Each carries a deterministic agentId so the lead can
        // address it before it runs.
        ...parsedMembers.members.map((m): TeamMember => ({
          name: m.name,
          agentId: formatAgentId(m.name, name),
          agentType: m.agentType,
          model: m.model,
          permissionMode: m.permissionMode,
          color: m.color,
          subscriptions: m.subscriptions,
          joinedAt: now,
          isActive: false,
        })),
      ],
      createdAt: now,
    };

    // Atomicity (parity: the reference's FileWriteTool orders the disk write to preserve
    // state consistency): persist team.json to disk FIRST, and only commit the
    // in-memory singleton after a successful write. A failed write (EACCES, ENOSPC,
    // read-only cwd) returns an error WITHOUT leaving a phantom in-memory team that
    // every later team_create would reject as "already exists".
    try {
      _teamHooks.mkdirSync(teamDir, { recursive: true });
      _teamHooks.mkdirSync(path.join(teamDir, TEAM_SCRATCHPAD_SUBDIR), { recursive: true });
      _teamHooks.writeFileSync(teamFilePath(teamDir), JSON.stringify(team, null, 2));
    } catch (err) {
      // Roll back any partially-created directory so a retry starts clean.
      try { if (_teamHooks.existsSync(teamDir)) _teamHooks.rmSync(teamDir, { recursive: true, force: true }); } catch { /* best-effort */ }
      const msg = err instanceof Error ? err.message : String(err);
      return { output: `Error: failed to create team "${name}": ${msg}`, isError: true };
    }

    currentTeam = team;
    currentTeamDir = teamDir;
    // Bind this process's self identity to the lead: the creator IS the team lead
    // (parity: isTeamLead resolves true for the creating session). isLeader() and
    // the whoami introspection read this.
    setSelfIdentity(leadAgentId, TEAM_LEAD_NAME);
    // Bind the shared, team-scoped task list (parity: Team = Project = TaskList) so
    // task_tracker calls persist to this team's own tasks file.
    setTaskPersistPath(context.cwd, name);
    // Track this team for orphan-cleanup on an abnormal exit (parity:
    // registerTeamForSessionCleanup) — team_delete unregisters it on a clean disband.
    registerTeamForSessionCleanup(name);
    teamDebug(`Created team ${name} (lead ${leadAgentId}, ${team.members.length} member(s))`);

    const renamed = name !== base ? ` (renamed from "${base}" — a team by that name already existed on disk)` : '';
    const declaredNote = parsedMembers.members.length > 0
      ? ` ${parsedMembers.members.length} teammate(s) pre-declared: ${parsedMembers.members.map(m => m.name).join(', ')}.`
      : '';
    // Surface this session's resolved swarm identity via the cohesive introspection
    // aggregate (consolidates the former ad-hoc isLeader()/getSelfAgentName() reads).
    const self = getSelfIdentity();
    // Role-aware status wording driven by the single resolved role (parity: the reference's
    // leader-vs-teammate wording) — reads self.role so the three states (lead,
    // teammate, solo) are surfaced from one canonical vocabulary.
    const roleLabel = self.role === 'lead' ? 'team lead' : self.role === 'teammate' ? 'teammate' : 'solo agent';
    const roleNote = ` You are the ${roleLabel} (self: ${self.agentName}).`;
    // Advertise the shared scratchpad dir (parity: the reference's coordinator exposes the
    // scratchpadDir) so the lead knows where durable cross-teammate knowledge lives;
    // spawned teammates get prompt-free read/write there (teamScratchpadRules).
    const scratchpadDir = path.join(teamDir, TEAM_SCRATCHPAD_SUBDIR);
    teamDebug(`Team ${name} shared scratchpad: ${scratchpadDir} (prompt-free for teammates)`);
    const scratchpadNote = ` Shared scratchpad (durable cross-teammate knowledge, prompt-free for teammates): ${scratchpadDir}.`;
    return {
      output: `Team "${name}" created${renamed}. Team file: ${teamFilePath(teamDir)}. Lead agent id: ${leadAgentId}.${declaredNote}${roleNote}${scratchpadNote} ` +
        `Spawn teammates with task({ name: "<teammate>" }) to add members.`,
    };
  },
};

export const teamDeleteTool: ToolDefinition = {
  name: 'team_delete',
  // PARITY (additive): declarative per-tool gate for the teammate subsystem.
  isEnabled: () => isTeammatesEnabled(),
  description:
    'Disband the current team and clean up its resources when the swarm work is complete. This removes the ' +
    "team directory (.ami/teams/<name>/ and its shared memory dir), tears down each teammate's git worktree, " +
    'removes the team-scoped task list, flushes queued inbox messages, and clears the team context from this ' +
    'session.\n' +
    '\n' +
    'IMPORTANT: it refuses while any teammate is still active — gracefully shut teammates down first (send a ' +
    'shutdown_request via send_message), then call team_delete once all have exited. Calling it with no active ' +
    'team is a no-op that reports there was nothing to clean up.',
  // Strict, empty schema (parity: the reference's team_delete uses z.strictObject({})):
  // it takes no arguments and rejects any supplied key.
  inputSchema: { type: 'object', properties: {}, additionalProperties: false },
  isReadOnly: false,
  async execute(_input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    // No active team: graceful non-error result (parity: the reference returns
    // success=true, "nothing to clean up") so an idempotent teardown is not an error.
    if (!currentTeam) return { output: 'No active team — nothing to clean up.' };
    const name = currentTeam.name;

    // Active-member guard: never disband a team with live (non-lead) members —
    // tearing down their directories mid-run would corrupt their state.
    const activeMembers = currentTeam.members.filter(m => m.name !== TEAM_LEAD_NAME && m.isActive !== false);
    if (activeMembers.length > 0) {
      const names = activeMembers.map(m => m.name).join(', ');
      return {
        output: `Error: cannot disband team "${name}" with ${activeMembers.length} active member(s): ${names}. ` +
          `Shut them down first (send a shutdown_request via send_message), then retry team_delete.`,
        isError: true,
      };
    }

    // Tear down each teammate's git worktree before the team dir (parity: the reference
    // Code's cleanupTeamDirectories removes directories AND per-teammate worktrees).
    // Best-effort: a failed worktree removal must not block disbanding the team.
    let worktreesRemoved = 0;
    for (const m of currentTeam.members) {
      if (m.worktreePath && removeMemberWorktree(m.worktreePath, context.cwd)) {
        worktreesRemoved += 1;
      }
    }

    const teamDir = currentTeamDir || path.join(getTeamsRoot(context.cwd), name);
    // Surface cleanup failures instead of silently reporting success.
    try {
      fs.rmSync(teamDir, { recursive: true, force: true });
      teamDebug(`Cleaned up team directory: ${teamDir}`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      return { output: `Error: failed to remove team directory ${teamDir}: ${msg}`, isError: true };
    }
    // Remove the team's 1:1 task list file last (parity: the reference's cleanup
    // order is worktrees -> team dir -> tasks dir). Independent guard: a tasks-file
    // removal failure must not fail a team_delete that already removed the dir.
    const tasksRemoved = deleteTaskList(context.cwd, name);

    // Fully tear down every team member's mailbox state so a future team that
    // reuses a name never inherits stale messages, a crashed teammate's undelivered
    // queue, a leaked receive() waiter, or a stale eager-delivery notifier (parity:
    // the reference purges the inbox at the exit boundary). purgeMailbox is the superset of
    // clearMailbox — it also reclaims waiters/notifier/revision/outbound-DM history.
    for (const m of currentTeam.members) {
      purgeMailbox(m.name);
    }

    currentTeam = null;
    currentTeamDir = null;
    // Reset the round-robin color palette so the next team starts fresh (parity:
    // clearTeammateColors is called during team cleanup).
    clearTeammateColors();
    // Drop this process's bound self identity now that the team is gone (parity:
    // team context is cleared on disband).
    clearSelfIdentity();
    // Unbind the team-scoped task list: subsequent task_tracker calls fall back to
    // the default list now that the team (and its 1:1 task list) is gone.
    setTaskPersistPath(context.cwd);
    // Drop this team from session-cleanup tracking — it is already cleaned, so the
    // shutdown sweep must not try again (parity: unregisterTeamForSessionCleanup).
    unregisterTeamForSessionCleanup(name);
    teamDebug(`Disbanded team ${name} (${worktreesRemoved} worktree(s) removed)`);
    const wtNote = worktreesRemoved > 0
      ? ` and ${worktreesRemoved} worktree${worktreesRemoved === 1 ? '' : 's'}`
      : '';
    const tasksNote = tasksRemoved ? ', task list removed' : '';
    // State exactly what was removed (parity: the reference's delete result names the
    // cleaned-up team): include the removed team directory path.
    return { output: `Team "${name}" deleted: removed ${teamDir} (directories${wtNote} cleaned up${tasksNote}).` };
  },
};

export function getCurrentTeam(): TeamFile | null { return currentTeam; }

/**
 * Absolute path of the active team's on-disk directory (`.ami/teams/<name>/`), or
 * null when no team is established this session. Exposed so cross-process
 * coordination surfaces (e.g. the file-backed teammate mailbox in
 * {@link ./mailbox-fs}) can resolve the shared `<teamsRoot>/<teamName>/` location
 * other OS processes in the same swarm write to, without re-deriving it from cwd.
 */
export function getCurrentTeamDir(): string | null { return currentTeamDir; }

export function resetTeam(): void {
  currentTeam = null;
  currentTeamDir = null;
  // Reset the color palette so a fresh team starts assignment from the top.
  clearTeammateColors();
  // Drop any bound self identity so a fresh team/session starts clean.
  clearSelfIdentity();
  // Clear session-cleanup tracking so a reset (and each test) starts with no
  // registered orphan teams.
  sessionCreatedTeams.clear();
}

/**
 * Register (or update) a teammate in the current team file. Called when a named
 * agent is spawned while a team is active, so the delete guard and task-list
 * ownership can see real members. No-op when there is no active team.
 */
export function registerTeamMember(member: Omit<TeamMember, 'joinedAt' | 'isActive'> & { joinedAt?: string; isActive?: boolean }): void {
  if (!currentTeam) return;
  const existing = currentTeam.members.find(m => m.name === member.name || m.agentId === member.agentId);
  if (existing) {
    // Preserve any color already assigned; otherwise honor an explicit color or
    // assign the next palette color round-robin (parity: a spawned teammate is
    // tinted so its output is distinguishable).
    const color = existing.color ?? member.color ?? assignTeammateColor(member.agentId || existing.agentId);
    Object.assign(existing, member, { isActive: member.isActive ?? true, color });
    teamDebug(`Updated member ${member.name} in team ${currentTeam.name}`);
  } else {
    const color = member.color ?? assignTeammateColor(member.agentId);
    currentTeam.members.push({ ...member, color, joinedAt: member.joinedAt || new Date().toISOString(), isActive: member.isActive ?? true });
    teamDebug(`Added member ${member.name} (${member.agentId}) to team ${currentTeam.name}`);
  }
  persistTeam();
}

/**
 * Register (or update) a teammate in a team identified BY NAME — the on-disk target
 * of a `task({ team_name })` spawn (parity: the reference's `team_name` Agent-tool
 * parameter, which JOINs a spawned agent to a named team). When `teamName` is the
 * current in-memory team this delegates to {@link registerTeamMember} (in-memory +
 * persist); otherwise it loads that team's file off disk, appends/updates the
 * member (preserving an existing color, else assigning the next palette color), and
 * writes it back under a file lock. Returns true when the member was recorded, or
 * false when no team by that name exists (the caller surfaces the error). Does NOT
 * mutate the current in-memory team singleton when targeting a different team.
 */
export function registerTeamMemberInto(
  cwd: string,
  teamName: string,
  member: Omit<TeamMember, 'joinedAt' | 'isActive'> & { joinedAt?: string; isActive?: boolean },
): boolean {
  if (currentTeam && currentTeam.name === teamName) {
    registerTeamMember(member);
    return true;
  }
  const loaded = loadTeamFromDisk(cwd, teamName);
  if (!loaded) return false;
  const { team, dir } = loaded;
  const existing = team.members.find(m => m.name === member.name || m.agentId === member.agentId);
  if (existing) {
    const color = existing.color ?? member.color ?? assignTeammateColor(member.agentId || existing.agentId);
    Object.assign(existing, member, { isActive: member.isActive ?? true, color });
  } else {
    const color = member.color ?? assignTeammateColor(member.agentId);
    team.members.push({ ...member, color, joinedAt: member.joinedAt || new Date().toISOString(), isActive: member.isActive ?? true });
  }
  const file = teamFilePath(dir);
  withFileLock(file, () => {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, JSON.stringify(team, null, 2));
  }).catch(() => {
    try { fs.writeFileSync(file, JSON.stringify(team, null, 2)); } catch { /* best-effort */ }
  });
  teamDebug(`Added member ${member.name} (${member.agentId}) to on-disk team ${teamName}`);
  return true;
}

/**
 * Mark a teammate active/idle by name or agentId. No-op when no team or member.
 * Skips the disk write when the value is unchanged (write-if-changed guard,
 * mirroring the reference's `setMemberActive`). Stamps `idleSince` when a member
 * transitions to idle and clears it when it becomes active again.
 */
export function setTeamMemberActive(nameOrId: string, active: boolean): void {
  if (!currentTeam) return;
  const m = findMember(currentTeam, nameOrId);
  if (!m) return;
  // Write-if-changed: skip the persist entirely when the stored flag already
  // matches (mirrors the reference's `if (member.isActive === isActive) return`).
  if (m.isActive === active) return;
  m.isActive = active;
  m.idleSince = active ? undefined : new Date().toISOString();
  persistTeam();
  teamDebug(`Set member ${m.name} in team ${currentTeam.name} to ${active ? 'active' : 'idle'}`);
  // Cancellation/abort boundary (PARITY): when a teammate goes inactive — it
  // finished, errored, stalled, or was killed — release any unresolved tasks it
  // still owns back to the pool so the work is not orphaned and an idle teammate
  // can claim it. Match both identities a task can be owned under (agentId + name).
  // Best-effort and fire-and-forget: a failure to release tasks must never break
  // teammate teardown.
  if (!active) {
    void unassignTeammateTasks([m.agentId, m.name], m.name, 'shutdown')
      .then((r) => {
        if (r.unassignedTasks.length > 0) coreLog('team', r.notificationMessage);
      })
      .catch(() => { /* best-effort — never fail teardown on task release */ });
  }
}

/**
 * Set a single teammate's permission mode (by name or agentId), persisting the
 * change to the team file (mirrors the reference's setMemberMode). Returns the
 * applied mode, or null when there is no active team, no such member, or the mode
 * string is invalid.
 */
export function setMemberMode(nameOrId: string, mode: string): PermissionMode | null {
  if (!currentTeam) return null;
  const valid = normalizePermissionMode(mode);
  if (!valid) return null;
  const m = findMember(currentTeam, nameOrId);
  if (!m) return null;
  // Write-if-changed: a no-op mode set does not touch disk (mirrors the reference's
  // `if (member.mode === mode) return true`).
  if (m.permissionMode === valid) return valid;
  m.permissionMode = valid;
  persistTeam();
  teamDebug(`Set member ${m.name} in team ${currentTeam.name} to mode: ${valid}`);
  return valid;
}

/**
 * Sync THIS teammate's own permission mode into the shared team file so the lead
 * sees it (parity: the reference's `syncTeammateMode`). A no-op unless this session
 * is running as a (non-lead) teammate with a resolvable self name and an active
 * team. Delegates to {@link setMemberMode} (write-if-changed + persist). Returns
 * the applied mode, or null when it was a no-op or the mode string was invalid.
 */
export function syncTeammateMode(mode: string): PermissionMode | null {
  if (!isTeammate()) return null;
  const selfName = getSelfAgentName();
  if (!selfName || !currentTeam) return null;
  return setMemberMode(selfName, mode);
}

/**
 * Advance a single teammate's permission mode to the next in the cycle and
 * persist. Returns the new mode, or null when there is no active team / member.
 */
export function cycleMemberMode(nameOrId: string): PermissionMode | null {
  if (!currentTeam) return null;
  const m = findMember(currentTeam, nameOrId);
  if (!m) return null;
  const next = getNextPermissionMode(m.permissionMode);
  m.permissionMode = next;
  persistTeam();
  teamDebug(`Cycled member ${m.name} in team ${currentTeam.name} to mode: ${next}`);
  return next;
}

/**
 * Apply many per-teammate permission-mode changes in ONE atomic read-modify-write
 * (mirrors the reference's `setMultipleMemberModes`). Updates are keyed by member
 * name or agentId via a Map for O(1) lookup, and the team file is persisted only
 * once — and only if at least one member actually changed (the `anyChanged`
 * guard), avoiding the race of N separate read/write cycles clobbering each other.
 *
 * Returns the number of members whose mode changed (0 when nothing changed, no
 * team is active, or every supplied mode string was invalid). Invalid mode
 * strings are skipped rather than aborting the whole batch.
 */
export function setMultipleMemberModes(updates: Array<{ nameOrId: string; mode: string }>): number {
  if (!currentTeam) return 0;
  const updateMap = new Map<string, PermissionMode>();
  for (const u of updates) {
    const valid = normalizePermissionMode(u.mode);
    if (valid) updateMap.set(u.nameOrId, valid);
  }
  let changed = 0;
  for (const m of currentTeam.members) {
    const next = updateMap.get(m.name) ?? updateMap.get(m.agentId);
    if (next !== undefined && m.permissionMode !== next) {
      m.permissionMode = next;
      changed += 1;
    }
  }
  if (changed > 0) {
    persistTeam();
    teamDebug(`Set ${changed} member mode(s) in team ${currentTeam.name}`);
  }
  return changed;
}

/**
 * Cycle the permission mode of EVERY non-lead teammate in tandem (mirrors the reference
 * Code's `cycleAllTeammateModes`). Default rule: when the teammates' current modes
 * DIVERGE, reset them all to the canonical default; when they already AGREE,
 * advance them all to the next mode in the cycle. The whole change is applied in a
 * single atomic batch write.
 *
 * Returns the target mode applied to all teammates, or null when there are no
 * non-lead teammates / no active team.
 */
export function cycleAllMemberModes(): PermissionMode | null {
  if (!currentTeam) return null;
  const teammates = currentTeam.members.filter(m => m.name !== TEAM_LEAD_NAME);
  if (teammates.length === 0) return null;
  // 'default' is the reference wire name; normalizePermissionMode maps it to AMI's
  // canonical interactive default. Non-null because 'default' is a known alias.
  const resetMode = normalizePermissionMode('default')!;
  const modes = teammates.map(m => m.permissionMode ? normalizePermissionMode(m.permissionMode) : resetMode);
  const allSame = modes.every(x => x === modes[0]);
  const target: PermissionMode = !allSame ? resetMode : getNextPermissionMode(modes[0] ?? undefined);
  setMultipleMemberModes(teammates.map(m => ({ nameOrId: m.name, mode: target })));
  return target;
}

/**
 * Remove a teammate from the active team by name OR agentId (mirrors the reference's
 * `removeTeammateFromTeamFile`, which filters by either identifier). Keying on the
 * stable per-member agentId disambiguates when several in-process teammates would
 * otherwise collide on a shared value. The lead is never removable. Returns the
 * removed {@link TeamMember} (persisting the change), or null when there is no
 * active team, no matching member, or the target is the lead.
 */
export function removeTeamMember(nameOrId: string): TeamMember | null {
  if (!currentTeam) return null;
  const m = findMember(currentTeam, nameOrId);
  if (!m) return null;
  if (m.name === TEAM_LEAD_NAME) return null;
  const idx = currentTeam.members.indexOf(m);
  const [removed] = currentTeam.members.splice(idx, 1);
  if (removed.tmuxPaneId) scrubHiddenPane(removed.tmuxPaneId);
  persistTeam();
  teamDebug(`Removed member ${removed.name} (${removed.agentId}) from team ${currentTeam.name}`);
  return removed;
}

/**
 * Drop a pane id from the active team's {@link TeamFile.hiddenPaneIds} set, so a
 * disband/remove of the member owning that pane never leaves a dangling hidden-pane
 * entry (parity: the reference's `removeMemberFromTeam` scrubs the pane from
 * hiddenPaneIds). In-memory mutation only — the caller persists. Returns true when
 * the set actually changed.
 */
function scrubHiddenPane(paneId: string): boolean {
  if (!currentTeam?.hiddenPaneIds) return false;
  const next = currentTeam.hiddenPaneIds.filter(p => p !== paneId);
  if (next.length === currentTeam.hiddenPaneIds.length) return false;
  currentTeam.hiddenPaneIds = next.length > 0 ? next : undefined;
  return true;
}

/**
 * Remove a teammate from the active team keyed by its terminal pane id (parity:
 * the reference's `removeMemberFromTeam(tmuxPaneId)`), ALSO scrubbing that pane from
 * the team's {@link TeamFile.hiddenPaneIds} set so no dangling hidden entry remains.
 * Unlike {@link removeTeamMember} (name/agentId keyed), this is the pane-keyed path
 * the roster uses when a user closes a pane. The lead is never removable. Returns
 * the removed {@link TeamMember} (persisting the change), or null when there is no
 * active team or no member owns that pane.
 */
export function removeMemberByPaneId(paneId: string): TeamMember | null {
  if (!currentTeam || !paneId) return null;
  const m = currentTeam.members.find(mm => mm.tmuxPaneId === paneId);
  if (!m || m.name === TEAM_LEAD_NAME) return null;
  const idx = currentTeam.members.indexOf(m);
  const [removed] = currentTeam.members.splice(idx, 1);
  scrubHiddenPane(paneId);
  persistTeam();
  teamDebug(`Removed member ${removed.name} by pane ${paneId} from team ${currentTeam.name}`);
  return removed;
}

/**
 * Set (or clear) the hidden state of a teammate's pane in the active team (parity:
 * the reference's team-file `hiddenPaneIds` toggle driven from the roster view).
 * Adding is idempotent (a pane already hidden is not duplicated) and removing a pane
 * that is not hidden is a no-op. Persists only when the set actually changed.
 * Returns true when the hidden set changed, false otherwise (including no active
 * team or an empty pane id).
 */
export function setMemberPaneHidden(paneId: string, hidden: boolean): boolean {
  if (!currentTeam || !paneId) return false;
  const current = currentTeam.hiddenPaneIds ?? [];
  const isHidden = current.includes(paneId);
  if (hidden) {
    if (isHidden) return false;
    currentTeam.hiddenPaneIds = [...current, paneId];
  } else {
    if (!isHidden) return false;
    const next = current.filter(p => p !== paneId);
    currentTeam.hiddenPaneIds = next.length > 0 ? next : undefined;
  }
  persistTeam();
  teamDebug(`${hidden ? 'Hid' : 'Unhid'} pane ${paneId} in team ${currentTeam.name}`);
  return true;
}

/**
 * Remove every idle (non-lead) teammate from the active team in one atomic batch
 * (parity: the reference's TeamsDialog "p prune idle" action, which kills all
 * teammates whose status is `idle`). A member is idle when its run-state projects
 * to `'idle'` — i.e. `isActive === false` (same default-to-active rule as
 * {@link getTeammateStatuses}). The lead is never pruned. The team file is
 * persisted only when at least one member was removed. Returns the removed
 * members (empty when there is no active team or nothing is idle).
 */
export function pruneIdleMembers(): TeamMember[] {
  if (!currentTeam) return [];
  const removed: TeamMember[] = [];
  currentTeam.members = currentTeam.members.filter(m => {
    const idle = m.name !== TEAM_LEAD_NAME && m.isActive === false;
    if (idle) removed.push(m);
    return !idle;
  });
  if (removed.length > 0) {
    persistTeam();
    teamDebug(`Pruned ${removed.length} idle member(s) from team ${currentTeam.name}`);
  }
  return removed;
}

/**
 * Project each non-lead member of a team into a {@link TeammateStatus} record
 * (mirrors the reference's `getTeammateStatuses`). The lead is excluded. A member's
 * run-state is derived with the default-to-active rule: `isActive !== false` maps
 * to `'running'`, otherwise `'idle'`. Defaults to the active team when no team is
 * passed, so a disk-loaded team (via {@link readTeamFile}) can also be projected.
 * Returns [] when there is no team.
 */
export function getTeammateStatuses(team: TeamFile | null = currentTeam): TeammateStatus[] {
  if (!team) return [];
  // Build the hidden-pane set ONCE (parity: the reference's `new Set(hiddenPaneIds ?? [])`)
  // so each member's isHidden is an O(1) membership test rather than a per-member scan.
  const hidden = new Set(team.hiddenPaneIds ?? []);
  const out: TeammateStatus[] = [];
  for (const m of team.members) {
    if (m.name === TEAM_LEAD_NAME) continue;
    const running = m.isActive !== false;
    out.push({
      name: m.name,
      agentId: m.agentId,
      agentType: m.agentType,
      model: m.model,
      status: running ? 'running' : 'idle',
      mode: m.permissionMode,
      cwd: m.cwd,
      // Prefer the persisted member color; fall back to any live round-robin
      // assignment keyed by the member's agentId.
      color: m.color ?? getTeammateColor(m.agentId),
      // Surface a PANE backend only (parity: the reference builds status.backendType via
      // isPaneBackend — an in-process/unknown backend projects to undefined).
      backendType: isPaneBackend(m.backendType) ? m.backendType : undefined,
      tmuxPaneId: m.tmuxPaneId,
      // A member is hidden only when it HAS a pane and that pane is in the set
      // (parity: hiddenPaneIds.has(member.tmuxPaneId)); a paneless (in-process)
      // teammate is never hidden.
      isHidden: m.tmuxPaneId ? hidden.has(m.tmuxPaneId) : false,
      idleSince: running ? undefined : m.idleSince,
    });
  }
  return out;
}

/**
 * Aggregate a team's worker roster into a {@link TeamSummary} (mirrors the reference
 * Code's team summary used by the footer UI): the lead is excluded from every
 * count, and running/idle are derived with the same default-to-active rule as
 * {@link getTeammateStatuses}. Returns null when there is no team.
 */
export function getTeamSummary(team: TeamFile | null = currentTeam): TeamSummary | null {
  if (!team) return null;
  const statuses = getTeammateStatuses(team);
  const runningCount = statuses.filter(s => s.status === 'running').length;
  return {
    name: team.name,
    memberCount: statuses.length,
    runningCount,
    idleCount: statuses.length - runningCount,
  };
}
