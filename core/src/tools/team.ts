import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolResult, ToolContext } from '../types';

function sanitizeTeamName(name: string): string | null {
  const clean = name.replace(/[^a-zA-Z0-9_-]/g, '');
  if (!clean || clean !== name) return null;
  return clean;
}

interface TeamContext {
  name: string;
  description?: string;
  members: string[];
  createdAt: string;
}

let currentTeam: TeamContext | null = null;

export const teamCreateTool: ToolDefinition = {
  name: 'team_create',
  description: 'Create a team of agents for collaborative work. The team shares a task list and memory directory.',
  inputSchema: {
    type: 'object',
    properties: {
      team_name: { type: 'string', description: 'Name for the team.' },
      description: { type: 'string', description: 'Description of the team purpose.' },
    },
    required: ['team_name'],
  },
  isReadOnly: false,
  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    if (currentTeam) return { output: `Error: Team "${currentTeam.name}" already exists. Delete it first.`, isError: true };
    const rawName = input.team_name as string;
    const name = sanitizeTeamName(rawName);
    if (!name) return { output: 'Error: team_name must contain only alphanumeric characters, hyphens, and underscores.', isError: true };
    const teamDir = path.join(context.cwd, '.superinference', 'teams', name);
    fs.mkdirSync(teamDir, { recursive: true });
    fs.mkdirSync(path.join(teamDir, 'memory'), { recursive: true });
    currentTeam = { name, description: input.description as string | undefined, members: [], createdAt: new Date().toISOString() };
    fs.writeFileSync(path.join(teamDir, 'team.json'), JSON.stringify(currentTeam, null, 2));
    return { output: `Team "${name}" created at ${teamDir}` };
  },
};

export const teamDeleteTool: ToolDefinition = {
  name: 'team_delete',
  description: 'Delete the current team and clean up team resources.',
  inputSchema: { type: 'object', properties: {} },
  isReadOnly: false,
  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    if (!currentTeam) return { output: 'Error: No active team.', isError: true };
    const name = currentTeam.name;
    const teamDir = path.join(context.cwd, '.superinference', 'teams', name);
    try { fs.rmSync(teamDir, { recursive: true, force: true }); } catch {}
    currentTeam = null;
    return { output: `Team "${name}" deleted.` };
  },
};

export function getCurrentTeam(): TeamContext | null { return currentTeam; }
export function resetTeam(): void { currentTeam = null; }
