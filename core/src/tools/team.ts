import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { withFileLock } from '../utils/file-lock';
import { TEAM_LEAD_NAME, clearMailbox } from './send-message';
import { PERMISSION_MODES, normalizePermissionMode, PermissionMode, PermissionRule } from '../permissions';
import { unassignTeammateTasks, setTaskPersistPath, deleteTaskList } from './task-tracker';
import { sanitizeAgentName } from './task';
import { log as coreLog } from '../logger';
import { gitExec } from '../worktree-manager';
import { isTeammatesEnabled } from './tool-gates';
import { AGENT_COLORS, AgentColorName, parseAgentColor } from '../skills';

/**
 * Format a deterministic agent id as `agentName@teamName` (mirrors Claude Code's
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
 * (mirrors Claude Code's `parseAgentId`). Splitting on the first `@` means a team
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

/**
 * Round-robin teammate color assignment (parity: Claude Code's
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

/** Max length of a user-supplied team name (mirrors Claude's identifier bounds). */
const MAX_TEAM_NAME_LEN = 64;

/**
 * Max length of the free-text team description persisted to disk. Mirrors Claude's
 * cap on the analogous whenToUse/description field (5000 chars): the description is
 * written verbatim into team.json, so an unbounded value is an on-disk size/DoS gap.
 */
const MAX_TEAM_DESCRIPTION_LEN = 5000;

/**
 * Validate a user-supplied team name that becomes a directory component. Mirrors
 * Claude's validateAgentType identifier bounds: a bounded length AND a start/end
 * alphanumeric requirement, so degenerate names like "-", "_", "-foo" or "bar_"
 * (which would still be valid path components) are rejected. Returns the name when
 * valid, or null otherwise.
 */
function sanitizeTeamName(name: string): string | null {
  const clean = name.replace(/[^a-zA-Z0-9_-]/g, '');
  if (!clean || clean !== name) return null;
  if (clean.length > MAX_TEAM_NAME_LEN) return null;
  // Must start and end with an alphanumeric (single-char alphanumeric is allowed).
  // clean is already [a-zA-Z0-9_-]+, so a direct first/last char test suffices
  // (avoids a ReDoS-flagged nested-optional regex).
  const isAlnum = (c: string): boolean =>
    (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');
  if (!isAlnum(clean[0]) || !isAlnum(clean[clean.length - 1])) return null;
  return clean;
}

/**
 * Teammate execution backend (parity: Claude Code's swarm `BackendType`): the
 * mechanism a teammate runs under. `tmux`/`iterm2` are terminal-pane backends;
 * `in-process` runs a teammate inside the same Node process with an isolated
 * context. Persisted per-member so a resumed session knows how each teammate runs.
 */
export type BackendType = 'tmux' | 'iterm2' | 'in-process';

const BACKEND_TYPES: readonly BackendType[] = ['tmux', 'iterm2', 'in-process'];

/**
 * Type guard for a valid {@link BackendType} (parity: Claude validates persisted
 * backend fields against a known set when deserializing a team file). Any value
 * outside the known set — including `undefined`, a number, or a stale enum name —
 * returns false so the caller can coerce it away rather than trust bad data.
 */
export function isBackendType(v: unknown): v is BackendType {
  return typeof v === 'string' && (BACKEND_TYPES as readonly string[]).includes(v);
}

/**
 * Type guard for a PANE-based backend only (parity: Claude Code's `isPaneBackend`,
 * used when projecting a teammate's status — a non-pane or unknown backend yields
 * `undefined` rather than a bogus pane backend). `in-process` is deliberately NOT a
 * pane backend.
 */
export function isPaneBackend(v: unknown): v is 'tmux' | 'iterm2' {
  return v === 'tmux' || v === 'iterm2';
}

/**
 * A rich per-member record in the team file, mirroring Claude Code's team config
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
   *  concurrent agents are distinguishable (parity: Claude Code's per-member
   *  `color`). Drawn round-robin from the shared {@link AGENT_COLORS} palette on
   *  spawn ({@link assignTeammateColor}), or set explicitly at declaration. */
  color?: AgentColorName;
  /** Absolute path of the git worktree this teammate runs in, when it was spawned
   *  into a dedicated worktree. Removed on team_delete (parity: Claude Code's
   *  cleanupTeamDirectories tears down per-teammate worktrees, not just dirs). */
  worktreePath?: string;
  /** Execution backend this teammate runs under (parity: Claude's per-member
   *  `backendType`). Validated against {@link BackendType} on read; an unknown
   *  persisted value is coerced to undefined. */
  backendType?: BackendType;
  joinedAt: string;
  /** false => idle/finished/crashed; undefined/true => active. */
  isActive?: boolean;
  /** ISO timestamp of when the member last went idle (set when isActive flips to false). */
  idleSince?: string;
}

/**
 * The derived run-state of a teammate, mirroring Claude Code's `TeammateStatus`
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
  /** Display color for this teammate (parity: Claude's TeammateStatus.color),
   *  surfaced so a roster/renderer can tint each agent consistently. */
  color?: AgentColorName;
  /** The teammate's execution backend, limited to a PANE backend (parity: Claude's
   *  TeammateStatus.backendType, built via isPaneBackend — a non-pane/unknown
   *  backend is surfaced as undefined). */
  backendType?: 'tmux' | 'iterm2';
  idleSince?: string;
}

/**
 * Aggregate counts for a team, mirroring Claude Code's `TeamSummary`. The lead is
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
 * A declarative team-wide allowed-path entry (mirrors Claude Code's
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
   *  session allow-rules (parity: Claude Code's `teamAllowedPaths`). Stored on the
   *  team file so a teammate spawned into the team is scoped the same way. */
  teamAllowedPaths?: TeamAllowedPath[];
  /** Declarative team-wide tool allowlist inherited by every teammate (parity:
   *  Claude Code's per-agent `tools` scoping knob). Stored on the team file so a
   *  teammate spawned into the team can be constrained the same way the lead is. */
  teamAllowedTools?: string[];
  members: TeamMember[];
  createdAt: string;
}

let currentTeam: TeamFile | null = null;
let currentTeamDir: string | null = null;

/**
 * This process's own agent identity within a team (parity: Claude Code's
 * per-session teammate identity — agentId + agentName — resolved from
 * AsyncLocalStorage/CLI args). The leader session binds this to the lead's id on
 * create/reconnect; a spawned teammate process binds its own. Used by
 * {@link isLeader} to tell leader from teammate and surfaced as an introspection
 * field so "who am I?" is answerable at runtime. Cleared on team teardown.
 */
interface SelfIdentity { agentId: string; agentName: string; }
let selfIdentity: SelfIdentity | null = null;

/** Bind this process's agent identity within the active team. */
export function setSelfIdentity(agentId: string, agentName: string): void {
  selfIdentity = { agentId, agentName };
}

/** This process's bound agent id, or undefined when unset (parity: getAgentId). */
export function getSelfAgentId(): string | undefined {
  return selfIdentity?.agentId;
}

/** This process's bound agent name, or undefined when unset (parity: getAgentName). */
export function getSelfAgentName(): string | undefined {
  return selfIdentity?.agentName;
}

/** Clear the bound self identity (team teardown / reset). */
export function clearSelfIdentity(): void {
  selfIdentity = null;
}

/**
 * Whether THIS process is the team's lead (parity: Claude Code's `isTeamLead`).
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
 * Deterministic, reproducible lead agent id derived from the team name. Uses the
 * canonical `agentName@teamName` scheme ({@link formatAgentId}) so the lead's id
 * is consistent with every other agent id and request id in the swarm subsystem
 * (send-message's structured request ids embed an `agentName@teamName` agent id).
 */
function leadAgentIdFor(teamName: string): string {
  return formatAgentId(TEAM_LEAD_NAME, teamName);
}

/**
 * Resolve a team member by a bare name, a stored agentId, OR a deterministic
 * `agentName@teamName` id for THIS team ({@link parseAgentId}). The deterministic
 * fallback lets a lead address a teammate by its computed id even when the member
 * was registered under an opaque spawn/task id. Single canonical lookup shared by
 * every per-member mutation below (no duplicated `.find` predicates).
 */
function findMember(team: TeamFile, nameOrId: string): TeamMember | undefined {
  const direct = team.members.find(m => m.name === nameOrId || m.agentId === nameOrId);
  if (direct) return direct;
  const parsed = parseAgentId(nameOrId);
  if (parsed && parsed.teamName === team.name) {
    return team.members.find(m => m.name === parsed.agentName);
  }
  return undefined;
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
 * Remove a single teammate's git worktree (parity: Claude Code's
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

function teamFilePath(dir: string): string {
  return path.join(dir, 'team.json');
}

/** Persist the current team file under a cross-process lock. */
function persistTeam(): void {
  if (!currentTeam || !currentTeamDir) return;
  const file = teamFilePath(currentTeamDir);
  const snapshot = currentTeam;
  // Low-frequency mutation; lock to stay consistent with concurrent sessions.
  withFileLock(file, () => {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, JSON.stringify(snapshot, null, 2));
  }).catch(() => {
    // Best-effort: fall back to a direct write so a lock failure never drops state.
    try { fs.writeFileSync(file, JSON.stringify(snapshot, null, 2)); } catch { /* ignore */ }
  });
}

/**
 * Guarded reader for a persisted team file. Returns the parsed {@link TeamFile},
 * or null when the file is missing, unreadable, not valid JSON, or structurally
 * invalid (missing name / leadAgentId / members array). Mirrors Claude Code's
 * swarm `readTeamFile` (null on ENOENT / parse failure) so a resumed session
 * degrades gracefully instead of throwing on a missing or corrupt team file.
 */
export function readTeamFile(dir: string): TeamFile | null {
  let raw: string;
  try {
    raw = fs.readFileSync(teamFilePath(dir), 'utf8');
  } catch {
    return null; // missing / unreadable
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null; // corrupt JSON
  }
  if (!parsed || typeof parsed !== 'object') return null;
  const t = parsed as Partial<TeamFile>;
  if (typeof t.name !== 'string' || typeof t.leadAgentId !== 'string' || !Array.isArray(t.members)) {
    return null; // wrong shape
  }
  // Boundary validation of persisted enum fields (parity: Claude validates
  // backendType via isPaneBackend and coerces unknown values when deserializing).
  // A stale/foreign file may carry an invalid permissionMode or backendType; coerce
  // each bad value to undefined in place rather than trusting it downstream.
  for (const raw of t.members as unknown[]) {
    if (!raw || typeof raw !== 'object') continue;
    const member = raw as TeamMember;
    if (member.permissionMode !== undefined && !normalizePermissionMode(member.permissionMode)) {
      member.permissionMode = undefined;
    }
    if (member.backendType !== undefined && !isBackendType(member.backendType)) {
      member.backendType = undefined;
    }
  }
  return t as TeamFile;
}

/**
 * Recover a member record by its (case-sensitive) name. Returns null when the
 * member is absent — e.g. it was removed from the team between sessions. Mirrors
 * Claude Code's resume-time `members.find(m => m.name === agentName)` lookup with
 * its explicit removed-member branch (callers tolerate the null rather than throw).
 */
export function findTeamMemberByName(team: TeamFile, name: string): TeamMember | null {
  return team.members.find(m => m.name === name) ?? null;
}

/**
 * Load a persisted team by name from a workspace. Returns the team plus its
 * on-disk directory, or null when no readable/valid team file exists there. Used
 * by the resumed-session reconnection path so team state survives a restart.
 */
export function loadTeamFromDisk(cwd: string, name: string): { team: TeamFile; dir: string } | null {
  const dir = path.join(cwd, '.ami', 'teams', name);
  const team = readTeamFile(dir);
  if (!team) return null;
  return { team, dir };
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

/** A teammate pre-declared in team_create's `members` input (post-validation). */
interface DeclaredMember {
  name: string;
  agentType?: string;
  model?: string;
  permissionMode?: PermissionMode;
  color?: AgentColorName;
}

/**
 * Parse and validate the optional `members` roster supplied to team_create (parity:
 * Claude Code lets the caller name the agents that participate at creation). Each
 * entry may be a bare name string or an object `{ name, agent_type?, model?,
 * permission_mode? }`. Names are sanitized with the canonical {@link sanitizeAgentName}
 * (shared helper — no duplicate reserved-char logic); the lead name and duplicates
 * are skipped. An invalid `permission_mode` on any entry is a hard error (returned
 * in `error`) so a bad knob fails the create rather than being silently dropped.
 */
function parseDeclaredMembers(raw: unknown): { members: DeclaredMember[]; error?: string } {
  if (raw === undefined || raw === null) return { members: [] };
  if (!Array.isArray(raw)) return { members: [], error: 'members must be an array of names or member objects.' };
  const out: DeclaredMember[] = [];
  const seen = new Set<string>([TEAM_LEAD_NAME]);
  for (const item of raw) {
    let rawName: string | undefined;
    let agentType: string | undefined;
    let model: string | undefined;
    let permRaw: string | undefined;
    let colorRaw: unknown;
    if (typeof item === 'string') {
      rawName = item;
    } else if (item && typeof item === 'object') {
      const o = item as Record<string, unknown>;
      rawName = typeof o.name === 'string' ? o.name : undefined;
      agentType = typeof o.agent_type === 'string' ? o.agent_type : undefined;
      model = typeof o.model === 'string' ? o.model : undefined;
      permRaw = typeof o.permission_mode === 'string' ? o.permission_mode : undefined;
      colorRaw = o.color;
    }
    if (!rawName) continue;
    const name = sanitizeAgentName(rawName);
    if (!name || seen.has(name)) continue;
    let permissionMode: PermissionMode | undefined;
    if (permRaw !== undefined) {
      const valid = normalizePermissionMode(permRaw);
      if (!valid) {
        return { members: [], error: `member "${name}" has an invalid permission_mode; must be one of: ${PERMISSION_MODES.join(', ')}.` };
      }
      permissionMode = valid;
    }
    // Explicit display color (parity: Claude's per-agent `color`). An unknown or
    // 'automatic' value normalizes to undefined so the teammate falls back to the
    // round-robin palette assignment made on spawn.
    const color = parseAgentColor(colorRaw);
    seen.add(name);
    out.push({ name, agentType, model, permissionMode, color });
  }
  return { members: out };
}

/**
 * Parse and validate the optional `team_allowed_paths` roster supplied to
 * team_create (parity: Claude Code's `teamAllowedPaths` config knob). Each entry
 * is either a bare path string (grants ALL tools — `toolName: '*'` — under that
 * path) or an object `{ path, tool_name? }` to scope the grant to a single tool.
 * An entry without a non-empty `path` is a hard error (returned in `error`) so a
 * malformed knob fails the create rather than being silently dropped. Returns the
 * normalized {@link TeamAllowedPath} list, or `undefined` when none were supplied.
 */
function parseTeamAllowedPaths(raw: unknown): { paths?: TeamAllowedPath[]; error?: string } {
  if (raw === undefined || raw === null) return {};
  if (!Array.isArray(raw)) return { error: 'team_allowed_paths must be an array of path strings or { path, tool_name } objects.' };
  const out: TeamAllowedPath[] = [];
  for (const item of raw) {
    let p: string | undefined;
    let toolName = '*';
    if (typeof item === 'string') {
      p = item;
    } else if (item && typeof item === 'object') {
      const o = item as Record<string, unknown>;
      p = typeof o.path === 'string' ? o.path : undefined;
      if (typeof o.tool_name === 'string' && o.tool_name) toolName = o.tool_name;
    }
    if (!p) return { error: 'each team_allowed_paths entry must have a non-empty path.' };
    out.push({ path: p, toolName });
  }
  return { paths: out.length > 0 ? out : undefined };
}

/**
 * Convert a team's declarative {@link TeamAllowedPath} entries into session
 * allow-{@link PermissionRule}s (mirrors Claude Code's teammateInit path→rule
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

export const teamCreateTool: ToolDefinition = {
  name: 'team_create',
  // PARITY (additive): declarative per-tool gate for the teammate subsystem.
  isEnabled: () => isTeammatesEnabled(),
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
    'teammates down with a shutdown_request via send_message, then team_delete once all have exited.',
  inputSchema: {
    type: 'object',
    properties: {
      team_name: { type: 'string', description: 'Name for the team (alphanumeric, hyphen, underscore).' },
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
          // Union item (parity with Claude's teamAllowedPaths): a bare path string
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
          // Union item (parity with Claude's union value params): a bare name
          // string OR an object { name, agent_type?, model?, permission_mode? }.
          type: ['string', 'object'],
          properties: {
            name: { type: 'string' },
            agent_type: { type: 'string' },
            model: { type: 'string' },
            permission_mode: { type: 'string' },
            color: { type: 'string', description: `Display color (one of: ${AGENT_COLORS.join(', ')}, or "automatic").` },
          },
        },
      },
    },
    required: ['team_name'],
    // Strict schema (parity: Claude Code's team tools use z.strictObject): reject
    // unexpected keys so a typo'd/invented knob fails loudly instead of being
    // silently ignored.
    additionalProperties: false,
  },
  isReadOnly: false,
  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    if (currentTeam) return { output: `Error: Team "${currentTeam.name}" already exists. Delete it first with team_delete.`, isError: true };
    const rawName = input.team_name as string;
    const base = sanitizeTeamName(rawName);
    if (!base) return { output: `Error: team_name must contain only alphanumeric characters, hyphens, and underscores, must start and end with an alphanumeric character, and be at most ${MAX_TEAM_NAME_LEN} characters.`, isError: true };

    // Bound the free-text description before it is persisted verbatim to team.json.
    const description = input.description as string | undefined;
    if (typeof description === 'string' && description.length > MAX_TEAM_DESCRIPTION_LEN) {
      return { output: `Error: description must be at most ${MAX_TEAM_DESCRIPTION_LEN} characters (got ${description.length}).`, isError: true };
    }

    // Per-agent scoping knob: the lead's permission mode (parity: Claude's agent
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

    // Parse any pre-declared teammate roster (parity: Claude's team members input).
    // Each entry becomes an idle member record awaiting its task-spawn; spawning
    // later reconciles it by name via registerTeamMember. Invalid permission modes
    // abort the whole create so the caller fixes the input rather than getting a
    // silently-dropped knob.
    const parsedMembers = parseDeclaredMembers(input.members);
    if (parsedMembers.error) return { output: `Error: ${parsedMembers.error}`, isError: true };

    // Parse the declarative team-wide allowed paths (parity: Claude's
    // teamAllowedPaths). A malformed entry aborts the create so the caller fixes
    // the knob rather than getting a silently-dropped permission grant.
    const parsedPaths = parseTeamAllowedPaths(input.team_allowed_paths);
    if (parsedPaths.error) return { output: `Error: ${parsedPaths.error}`, isError: true };

    // On-disk collision detection with a unique-name fallback: if a directory for
    // this name already exists (e.g. an orphan from a prior session), pick the
    // next free "name-N" so we never clobber another team's state.
    const teamsRoot = path.join(context.cwd, '.ami', 'teams');

    // Resumed-session reconnection: if a team directory for this exact name already
    // exists AND its persisted team file belongs to THIS session (same
    // leadSessionId) with an intact lead member, rehydrate it in place instead of
    // forking a renamed duplicate. This lets team state survive a process restart
    // (parity: Claude Code's swarm reconnection on a resumed session). A foreign or
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
          joinedAt: now,
          isActive: false,
        })),
      ],
      createdAt: now,
    };

    // Atomicity (parity: Claude's FileWriteTool orders the disk write to preserve
    // state consistency): persist team.json to disk FIRST, and only commit the
    // in-memory singleton after a successful write. A failed write (EACCES, ENOSPC,
    // read-only cwd) returns an error WITHOUT leaving a phantom in-memory team that
    // every later team_create would reject as "already exists".
    try {
      _teamHooks.mkdirSync(teamDir, { recursive: true });
      _teamHooks.mkdirSync(path.join(teamDir, 'memory'), { recursive: true });
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

    const renamed = name !== base ? ` (renamed from "${base}" — a team by that name already existed on disk)` : '';
    const declaredNote = parsedMembers.members.length > 0
      ? ` ${parsedMembers.members.length} teammate(s) pre-declared: ${parsedMembers.members.map(m => m.name).join(', ')}.`
      : '';
    const roleNote = ` You are the ${isLeader() ? 'team lead' : 'teammate'} (self: ${getSelfAgentName()}).`;
    return {
      output: `Team "${name}" created${renamed}. Team file: ${teamFilePath(teamDir)}. Lead agent id: ${leadAgentId}.${declaredNote}${roleNote} ` +
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
  // Strict, empty schema (parity: Claude's team_delete uses z.strictObject({})):
  // it takes no arguments and rejects any supplied key.
  inputSchema: { type: 'object', properties: {}, additionalProperties: false },
  isReadOnly: false,
  async execute(_input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    // No active team: graceful non-error result (parity: Claude returns
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

    // Tear down each teammate's git worktree before the team dir (parity: Claude
    // Code's cleanupTeamDirectories removes directories AND per-teammate worktrees).
    // Best-effort: a failed worktree removal must not block disbanding the team.
    let worktreesRemoved = 0;
    for (const m of currentTeam.members) {
      if (m.worktreePath && removeMemberWorktree(m.worktreePath, context.cwd)) {
        worktreesRemoved += 1;
      }
    }

    const teamDir = currentTeamDir || path.join(context.cwd, '.ami', 'teams', name);
    // Surface cleanup failures instead of silently reporting success.
    try {
      fs.rmSync(teamDir, { recursive: true, force: true });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      return { output: `Error: failed to remove team directory ${teamDir}: ${msg}`, isError: true };
    }
    // Remove the team's 1:1 task list file last (parity: Claude Code's cleanup
    // order is worktrees -> team dir -> tasks dir). Independent guard: a tasks-file
    // removal failure must not fail a team_delete that already removed the dir.
    const tasksRemoved = deleteTaskList(context.cwd, name);

    // Flush any queued inbox/relay messages for every team member so a future team
    // that reuses a name never inherits stale messages, and a crashed teammate's
    // undelivered queue does not leak (parity: Claude clears the inbox on disband).
    for (const m of currentTeam.members) {
      clearMailbox(m.name);
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
    const wtNote = worktreesRemoved > 0
      ? ` and ${worktreesRemoved} worktree${worktreesRemoved === 1 ? '' : 's'}`
      : '';
    const tasksNote = tasksRemoved ? ', task list removed' : '';
    // State exactly what was removed (parity: Claude's delete result names the
    // cleaned-up team): include the removed team directory path.
    return { output: `Team "${name}" deleted: removed ${teamDir} (directories${wtNote} cleaned up${tasksNote}).` };
  },
};

export function getCurrentTeam(): TeamFile | null { return currentTeam; }

export function resetTeam(): void {
  currentTeam = null;
  currentTeamDir = null;
  // Reset the color palette so a fresh team starts assignment from the top.
  clearTeammateColors();
  // Drop any bound self identity so a fresh team/session starts clean.
  clearSelfIdentity();
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
  } else {
    const color = member.color ?? assignTeammateColor(member.agentId);
    currentTeam.members.push({ ...member, color, joinedAt: member.joinedAt || new Date().toISOString(), isActive: member.isActive ?? true });
  }
  persistTeam();
}

/**
 * Mark a teammate active/idle by name or agentId. No-op when no team or member.
 * Skips the disk write when the value is unchanged (write-if-changed guard,
 * mirroring Claude Code's `setMemberActive`). Stamps `idleSince` when a member
 * transitions to idle and clears it when it becomes active again.
 */
export function setTeamMemberActive(nameOrId: string, active: boolean): void {
  if (!currentTeam) return;
  const m = findMember(currentTeam, nameOrId);
  if (!m) return;
  // Write-if-changed: skip the persist entirely when the stored flag already
  // matches (mirrors Claude's `if (member.isActive === isActive) return`).
  if (m.isActive === active) return;
  m.isActive = active;
  m.idleSince = active ? undefined : new Date().toISOString();
  persistTeam();
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
 * The next permission mode in a fixed cycle (mirrors Claude Code's
 * getNextPermissionMode, used to toggle a single teammate's mode). An unknown
 * current value starts the cycle at the first mode.
 */
export function getNextPermissionMode(current: string | undefined): PermissionMode {
  const normalized = current ? normalizePermissionMode(current) : null;
  const idx = normalized ? PERMISSION_MODES.indexOf(normalized) : -1;
  return PERMISSION_MODES[(idx + 1) % PERMISSION_MODES.length];
}

/**
 * Set a single teammate's permission mode (by name or agentId), persisting the
 * change to the team file (mirrors Claude Code's setMemberMode). Returns the
 * applied mode, or null when there is no active team, no such member, or the mode
 * string is invalid.
 */
export function setMemberMode(nameOrId: string, mode: string): PermissionMode | null {
  if (!currentTeam) return null;
  const valid = normalizePermissionMode(mode);
  if (!valid) return null;
  const m = findMember(currentTeam, nameOrId);
  if (!m) return null;
  // Write-if-changed: a no-op mode set does not touch disk (mirrors Claude's
  // `if (member.mode === mode) return true`).
  if (m.permissionMode === valid) return valid;
  m.permissionMode = valid;
  persistTeam();
  return valid;
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
  return next;
}

/**
 * Apply many per-teammate permission-mode changes in ONE atomic read-modify-write
 * (mirrors Claude Code's `setMultipleMemberModes`). Updates are keyed by member
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
  if (changed > 0) persistTeam();
  return changed;
}

/**
 * Cycle the permission mode of EVERY non-lead teammate in tandem (mirrors Claude
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
  // 'default' is the Claude wire name; normalizePermissionMode maps it to AMI's
  // canonical interactive default. Non-null because 'default' is a known alias.
  const resetMode = normalizePermissionMode('default')!;
  const modes = teammates.map(m => m.permissionMode ? normalizePermissionMode(m.permissionMode) : resetMode);
  const allSame = modes.every(x => x === modes[0]);
  const target: PermissionMode = !allSame ? resetMode : getNextPermissionMode(modes[0] ?? undefined);
  setMultipleMemberModes(teammates.map(m => ({ nameOrId: m.name, mode: target })));
  return target;
}

/**
 * Remove a teammate from the active team by name OR agentId (mirrors Claude Code's
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
  persistTeam();
  return removed;
}

/**
 * Remove every idle (non-lead) teammate from the active team in one atomic batch
 * (parity: Claude Code's TeamsDialog "p prune idle" action, which kills all
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
  if (removed.length > 0) persistTeam();
  return removed;
}

/**
 * Project each non-lead member of a team into a {@link TeammateStatus} record
 * (mirrors Claude Code's `getTeammateStatuses`). The lead is excluded. A member's
 * run-state is derived with the default-to-active rule: `isActive !== false` maps
 * to `'running'`, otherwise `'idle'`. Defaults to the active team when no team is
 * passed, so a disk-loaded team (via {@link readTeamFile}) can also be projected.
 * Returns [] when there is no team.
 */
export function getTeammateStatuses(team: TeamFile | null = currentTeam): TeammateStatus[] {
  if (!team) return [];
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
      // Surface a PANE backend only (parity: Claude builds status.backendType via
      // isPaneBackend — an in-process/unknown backend projects to undefined).
      backendType: isPaneBackend(m.backendType) ? m.backendType : undefined,
      idleSince: running ? undefined : m.idleSince,
    });
  }
  return out;
}

/**
 * Aggregate a team's worker roster into a {@link TeamSummary} (mirrors Claude
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
