import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  teamCreateTool,
  teamDeleteTool,
  getCurrentTeam,
  resetTeam,
  registerTeamMember,
  getTeammateStatuses,
  assignTeammateColor,
  getTeammateColor,
  clearTeammateColors,
} from '../src/tools/team';
import {
  taskTrackerTool,
  resetTaskState,
  setTaskPersistPath,
  deleteTaskList,
  getTaskState,
} from '../src/tools/task-tracker';
import { projectStateDir } from '../src/reference-journal';
import { AGENT_COLORS } from '../src/skills';
import type { ToolContext } from '../src/types';

let tmp: string;
let dataDir: string;
let prevDataDir: string | undefined;

function ctx(): ToolContext {
  return { cwd: tmp, sessionId: 'sess-color', abortSignal: new AbortController().signal } as ToolContext;
}

describe('U038 w66 — teammate color subsystem', () => {
  beforeEach(() => {
    resetTeam();
    clearTeammateColors();
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'team-color-'));
  });
  afterEach(() => {
    resetTeam();
    clearTeammateColors();
    try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ }
  });

  it('assigns palette colors round-robin, idempotent per teammate id', () => {
    // Round-robin order follows the shared AGENT_COLORS palette (no second palette).
    assert.equal(assignTeammateColor('a@t'), AGENT_COLORS[0]);
    assert.equal(assignTeammateColor('b@t'), AGENT_COLORS[1]);
    assert.equal(assignTeammateColor('c@t'), AGENT_COLORS[2]);
    // Idempotent: re-assigning an existing id returns the same color, no slot consumed.
    assert.equal(assignTeammateColor('a@t'), AGENT_COLORS[0]);
    assert.equal(assignTeammateColor('d@t'), AGENT_COLORS[3]);
  });

  it('wraps around the palette when more teammates than colors', () => {
    const assigned = Array.from({ length: AGENT_COLORS.length + 1 }, (_, i) => assignTeammateColor(`m${i}@t`));
    assert.equal(assigned[0], AGENT_COLORS[0]);
    assert.equal(assigned[AGENT_COLORS.length], AGENT_COLORS[0]); // wrapped
  });

  it('getTeammateColor returns the assignment or undefined; clearTeammateColors resets', () => {
    assert.equal(getTeammateColor('x@t'), undefined);
    const c = assignTeammateColor('x@t');
    assert.equal(getTeammateColor('x@t'), c);
    clearTeammateColors();
    assert.equal(getTeammateColor('x@t'), undefined);
    // After clear the palette starts from the top again.
    assert.equal(assignTeammateColor('y@t'), AGENT_COLORS[0]);
  });

  it('registerTeamMember assigns a color on spawn, persists it, and surfaces it in getTeammateStatuses', async () => {
    const r = await teamCreateTool.execute({ team_name: 'squad' }, ctx());
    assert.equal(r.isError, undefined);
    registerTeamMember({ name: 'alice', agentId: 'alice@squad', agentType: 'explore' });
    registerTeamMember({ name: 'bob', agentId: 'bob@squad' });

    const team = getCurrentTeam()!;
    const alice = team.members.find(m => m.name === 'alice')!;
    const bob = team.members.find(m => m.name === 'bob')!;
    assert.equal(alice.color, AGENT_COLORS[0]);
    assert.equal(bob.color, AGENT_COLORS[1]);

    // Persisted to disk, not just in memory.
    const file = JSON.parse(fs.readFileSync(path.join(tmp, '.ami', 'teams', 'squad', 'team.json'), 'utf-8'));
    assert.equal(file.members.find((m: { name: string }) => m.name === 'alice').color, AGENT_COLORS[0]);

    // Surfaced through the status projection consumed by the roster renderer.
    const statuses = getTeammateStatuses();
    assert.equal(statuses.find(s => s.name === 'alice')!.color, AGENT_COLORS[0]);
    assert.equal(statuses.find(s => s.name === 'bob')!.color, AGENT_COLORS[1]);
  });

  it('re-registering a teammate keeps its original color (idempotent on spawn/update)', async () => {
    await teamCreateTool.execute({ team_name: 'squad2' }, ctx());
    registerTeamMember({ name: 'alice', agentId: 'alice@squad2' });
    const first = getCurrentTeam()!.members.find(m => m.name === 'alice')!.color;
    // A later update (e.g. model learned on spawn) must not re-tint the member.
    registerTeamMember({ name: 'alice', agentId: 'alice@squad2', model: 'opus' });
    const after = getCurrentTeam()!.members.find(m => m.name === 'alice')!;
    assert.equal(after.color, first);
    assert.equal(after.model, 'opus');
  });

  it('honors an explicit per-member color at team_create and normalizes invalid/automatic to undefined', async () => {
    const r = await teamCreateTool.execute({
      team_name: 'crew',
      members: [
        { name: 'red-dev', color: 'purple' },
        { name: 'auto-dev', color: 'automatic' },
        { name: 'bad-dev', color: 'chartreuse' },
      ],
    }, ctx());
    assert.equal(r.isError, undefined);
    const team = getCurrentTeam()!;
    assert.equal(team.members.find(m => m.name === 'red-dev')!.color, 'purple');
    // 'automatic' and an unknown color both clear to undefined (await palette on spawn).
    assert.equal(team.members.find(m => m.name === 'auto-dev')!.color, undefined);
    assert.equal(team.members.find(m => m.name === 'bad-dev')!.color, undefined);
  });

  it('resetTeam clears color assignments', async () => {
    await teamCreateTool.execute({ team_name: 'squad3' }, ctx());
    registerTeamMember({ name: 'alice', agentId: 'alice@squad3' });
    assert.equal(getTeammateColor('alice@squad3'), AGENT_COLORS[0]);
    resetTeam();
    assert.equal(getTeammateColor('alice@squad3'), undefined);
  });
});

describe('U038 w66 — team task-list cleanup on disband', () => {
  beforeEach(() => {
    resetTeam();
    resetTaskState();
    clearTeammateColors();
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'team-tasks-'));
    dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'team-tasks-data-'));
    prevDataDir = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = dataDir;
  });
  afterEach(() => {
    resetTeam();
    resetTaskState();
    clearTeammateColors();
    if (prevDataDir === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
    else process.env.SUPERINFERENCE_DATA_DIR = prevDataDir;
    for (const d of [tmp, dataDir]) {
      try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
    }
  });

  it('team_delete removes the team-scoped tasks file and reports it, end-to-end', async () => {
    setTaskPersistPath(tmp);
    const c = await teamCreateTool.execute({ team_name: 'squad' }, ctx());
    assert.equal(c.isError, undefined);
    const t = await taskTrackerTool.execute({ action: 'create', subject: 'scoped task' }, ctx());
    assert.ok(!t.isError, `task create failed: ${t.output}`);
    const scoped = path.join(projectStateDir(tmp), 'tasks-squad.json');
    assert.ok(fs.existsSync(scoped), 'scoped tasks file should exist before delete');

    const d = await teamDeleteTool.execute({}, ctx());
    assert.equal(d.isError, undefined);
    assert.ok(d.output.includes('task list removed'), `delete output should note tasks cleanup: ${d.output}`);
    assert.ok(!fs.existsSync(scoped), 'scoped tasks file must be removed on team_delete');
  });

  it('deleteTaskList removes a scoped file, refuses the default list, and unbinds the persist path', async () => {
    // Arm a scoped list and write a task so the file exists on disk.
    setTaskPersistPath(tmp, 'myteam');
    await taskTrackerTool.execute({ action: 'create', subject: 'x' }, ctx());
    const scoped = path.join(projectStateDir(tmp), 'tasks-myteam.json');
    assert.ok(fs.existsSync(scoped));

    // Deleting the currently-bound scoped list removes it and unbinds persistence.
    assert.equal(deleteTaskList(tmp, 'myteam'), true);
    assert.ok(!fs.existsSync(scoped));
    // persistPath is unbound: a subsequent create stays in-memory (no file resurrected).
    await taskTrackerTool.execute({ action: 'create', subject: 'y' }, ctx());
    assert.ok(!fs.existsSync(scoped), 'a deleted scoped list must not be resurrected by a later mutation');
    assert.ok(getTaskState().size >= 1);

    // The default shared list is never removable, and a missing file is not an error.
    assert.equal(deleteTaskList(tmp, 'tasklist'), false);
    assert.equal(deleteTaskList(tmp, 'never-existed'), false);
  });
});
