import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { withFileLock } from '../utils/file-lock';
import { TEAM_LEAD_NAME, clearMailbox } from './send-message';
import { PERMISSION_MODES, normalizePermissionMode, PermissionMode } from '../permissions';

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
  joinedAt: string;
  /** false => idle/finished/crashed; undefined/true => active. */
  isActive?: boolean;
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
  teamAllowedPaths?: string[];
  members: TeamMember[];
  createdAt: string;
}

let currentTeam: TeamFile | null = null;
let currentTeamDir: string | null = null;

/** Deterministic, reproducible lead agent id derived from the team name. */
function leadAgentIdFor(teamName: string): string {
  return `lead-${teamName}`;
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

export const teamCreateTool: ToolDefinition = {
  name: 'team_create',
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
      team_allowed_paths: {
        type: 'array',
        items: { type: 'string' },
        description: 'Declarative team-wide allowed paths inherited by all teammates.',
      },
    },
    required: ['team_name'],
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
    fs.mkdirSync(teamDir, { recursive: true });
    fs.mkdirSync(path.join(teamDir, 'memory'), { recursive: true });

    const leadAgentId = leadAgentIdFor(name);
    currentTeam = {
      name,
      description,
      leadSessionId: context.sessionId,
      leadAgentId,
      teamAllowedPaths: Array.isArray(input.team_allowed_paths) ? (input.team_allowed_paths as string[]) : undefined,
      members: [{
        name: TEAM_LEAD_NAME,
        agentId: leadAgentId,
        agentType: (input.agent_type as string) || 'lead',
        model: input.model as string | undefined,
        cwd: context.cwd,
        joinedAt: new Date().toISOString(),
        isActive: true,
      }],
      createdAt: new Date().toISOString(),
    };
    currentTeamDir = teamDir;
    persistTeam();

    const renamed = name !== base ? ` (renamed from "${base}" — a team by that name already existed on disk)` : '';
    return {
      output: `Team "${name}" created${renamed}. Team file: ${teamFilePath(teamDir)}. Lead agent id: ${leadAgentId}. ` +
        `Spawn teammates with task({ name: "<teammate>" }) to add members.`,
    };
  },
};

export const teamDeleteTool: ToolDefinition = {
  name: 'team_delete',
  description:
    'Disband the current team and clean up its directories. Refuses while any teammate is still active — ' +
    'gracefully shut teammates down first (send a shutdown_request via send_message).',
  inputSchema: { type: 'object', properties: {} },
  isReadOnly: false,
  async execute(_input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    if (!currentTeam) return { output: 'Error: No active team.', isError: true };
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

    const teamDir = currentTeamDir || path.join(context.cwd, '.ami', 'teams', name);
    // Surface cleanup failures instead of silently reporting success.
    try {
      fs.rmSync(teamDir, { recursive: true, force: true });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      return { output: `Error: failed to remove team directory ${teamDir}: ${msg}`, isError: true };
    }
    // Flush any queued inbox/relay messages for every team member so a future team
    // that reuses a name never inherits stale messages, and a crashed teammate's
    // undelivered queue does not leak (parity: Claude clears the inbox on disband).
    for (const m of currentTeam.members) {
      clearMailbox(m.name);
    }

    currentTeam = null;
    currentTeamDir = null;
    return { output: `Team "${name}" deleted (directories cleaned up).` };
  },
};

export function getCurrentTeam(): TeamFile | null { return currentTeam; }

export function resetTeam(): void { currentTeam = null; currentTeamDir = null; }

/**
 * Register (or update) a teammate in the current team file. Called when a named
 * agent is spawned while a team is active, so the delete guard and task-list
 * ownership can see real members. No-op when there is no active team.
 */
export function registerTeamMember(member: Omit<TeamMember, 'joinedAt' | 'isActive'> & { joinedAt?: string; isActive?: boolean }): void {
  if (!currentTeam) return;
  const existing = currentTeam.members.find(m => m.name === member.name || m.agentId === member.agentId);
  if (existing) {
    Object.assign(existing, member, { isActive: member.isActive ?? true });
  } else {
    currentTeam.members.push({ ...member, joinedAt: member.joinedAt || new Date().toISOString(), isActive: member.isActive ?? true });
  }
  persistTeam();
}

/** Mark a teammate active/idle by name or agentId. No-op when no team or member. */
export function setTeamMemberActive(nameOrId: string, active: boolean): void {
  if (!currentTeam) return;
  const m = currentTeam.members.find(x => x.name === nameOrId || x.agentId === nameOrId);
  if (!m) return;
  m.isActive = active;
  persistTeam();
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
  const m = currentTeam.members.find(x => x.name === nameOrId || x.agentId === nameOrId);
  if (!m) return null;
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
  const m = currentTeam.members.find(x => x.name === nameOrId || x.agentId === nameOrId);
  if (!m) return null;
  const next = getNextPermissionMode(m.permissionMode);
  m.permissionMode = next;
  persistTeam();
  return next;
}
