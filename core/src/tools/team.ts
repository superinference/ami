import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { withFileLock } from '../utils/file-lock';
import { TEAM_LEAD_NAME } from './send-message';

/** Max length of a user-supplied team name (mirrors Claude's identifier bounds). */
const MAX_TEAM_NAME_LEN = 64;

function sanitizeTeamName(name: string): string | null {
  const clean = name.replace(/[^a-zA-Z0-9_-]/g, '');
  if (!clean || clean !== name) return null;
  if (clean.length > MAX_TEAM_NAME_LEN) return null;
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

export const teamCreateTool: ToolDefinition = {
  name: 'team_create',
  description:
    'Create a team of agents for collaborative work. The team shares a task list and memory directory, ' +
    'and a team file records each member (name, agentId, agentType, model) plus the lead. ' +
    'Spawn teammates with the task tool using a matching name to add them to the team; ' +
    'delete the team with team_delete only once all teammates have shut down.',
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
    if (!base) return { output: `Error: team_name must contain only alphanumeric characters, hyphens, and underscores, and be at most ${MAX_TEAM_NAME_LEN} characters.`, isError: true };

    // On-disk collision detection with a unique-name fallback: if a directory for
    // this name already exists (e.g. an orphan from a prior session), pick the
    // next free "name-N" so we never clobber another team's state.
    const teamsRoot = path.join(context.cwd, '.ami', 'teams');
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
      description: input.description as string | undefined,
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
