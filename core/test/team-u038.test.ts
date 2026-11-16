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
  _teamHooks,
} from '../src/tools/team';
import { taskTrackerTool, resetTaskState, setTaskPersistPath } from '../src/tools/task-tracker';
import { projectStateDir } from '../src/reference-journal';
import { TEAM_LEAD_NAME } from '../src/tools/send-message';
import type { ToolContext } from '../src/types';

let tmp: string;
let dataDir: string;
let prevDataDir: string | undefined;

function ctx(): ToolContext {
  return { cwd: tmp, sessionId: 'sess-u038', abortSignal: new AbortController().signal } as ToolContext;
}

describe('U038 team_create — task-list binding, roster, scoping knobs, atomic create', () => {
  beforeEach(() => {
    resetTeam();
    resetTaskState();
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'team-u038-'));
    dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'team-u038-data-'));
    prevDataDir = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = dataDir;
  });
  afterEach(() => {
    resetTeam();
    resetTaskState();
    if (prevDataDir === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
    else process.env.SUPERINFERENCE_DATA_DIR = prevDataDir;
    for (const d of [tmp, dataDir]) {
      try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
    }
  });

  // --- Gap 5: shared task-list tied to team (Team = Project = TaskList) ---
  it('binds a team-scoped task list on create and unbinds on delete', async () => {
    // Simulate the engine arming the DEFAULT task list before any team exists.
    setTaskPersistPath(tmp);
    const r = await teamCreateTool.execute({ team_name: 'squad' }, ctx());
    assert.equal(r.isError, undefined);

    // A task created now must land in the team-scoped file, not the default one.
    const t1 = await taskTrackerTool.execute({ action: 'create', subject: 'scoped task' }, ctx());
    assert.ok(!t1.isError, `task create failed: ${t1.output}`);
    const scoped = path.join(projectStateDir(tmp), 'tasks-squad.json');
    const defaultFile = path.join(projectStateDir(tmp), 'tasks.json');
    assert.ok(fs.existsSync(scoped), 'team-scoped tasks file should exist');
    assert.ok(!fs.existsSync(defaultFile), 'default tasks file must not be written while a team is bound');

    // Deleting the team unbinds the list; subsequent tasks fall back to default.
    const d = await teamDeleteTool.execute({}, ctx());
    assert.equal(d.isError, undefined);
    const t2 = await taskTrackerTool.execute({ action: 'create', subject: 'default task' }, ctx());
    assert.ok(!t2.isError, `task create failed: ${t2.output}`);
    assert.ok(fs.existsSync(defaultFile), 'default tasks file should be written after team_delete');
  });

  // --- Gap 2: pre-declaring team members / agent types at creation ---
  it('pre-declares teammates from the members input (idle, awaiting spawn)', async () => {
    const r = await teamCreateTool.execute({
      team_name: 'crew',
      members: ['alice', { name: 'bob', agent_type: 'explore', model: 'haiku' }],
    }, ctx());
    assert.equal(r.isError, undefined);
    const team = getCurrentTeam()!;
    // lead + 2 declared
    assert.equal(team.members.length, 3);
    const alice = team.members.find(m => m.name === 'alice')!;
    const bob = team.members.find(m => m.name === 'bob')!;
    assert.ok(alice && bob);
    // Declared members are idle until spawned, and carry a deterministic agentId.
    assert.equal(alice.isActive, false);
    assert.equal(alice.agentId, 'alice@crew');
    assert.equal(bob.agentType, 'explore');
    assert.equal(bob.model, 'haiku');
    // The roster is persisted to disk, not just held in memory.
    const file = JSON.parse(fs.readFileSync(path.join(tmp, '.ami', 'teams', 'crew', 'team.json'), 'utf-8'));
    assert.equal(file.members.length, 3);
    assert.ok(r.output.includes('pre-declared'));
  });

  it('skips the lead name and de-dupes declared members, and sanitizes reserved chars', async () => {
    const r = await teamCreateTool.execute({
      team_name: 'crew2',
      members: ['alice', 'alice', TEAM_LEAD_NAME, 'a@b'],
    }, ctx());
    assert.equal(r.isError, undefined);
    const team = getCurrentTeam()!;
    const names = team.members.map(m => m.name).sort();
    // lead + alice (deduped) + a-b (sanitized) — the lead name entry is dropped.
    assert.deepEqual(names, [TEAM_LEAD_NAME, 'a-b', 'alice'].sort());
  });

  // --- Gap 3: per-agent scoping knobs (permission_mode, allowed_tools) ---
  it('applies the lead permission_mode and team-wide allowed_tools at creation', async () => {
    const r = await teamCreateTool.execute({
      team_name: 'scoped',
      permission_mode: 'plan',
      allowed_tools: ['bash', 'file_edit'],
      members: [{ name: 'dev', permission_mode: 'auto-allow' }],
    }, ctx());
    assert.equal(r.isError, undefined);
    const team = getCurrentTeam()!;
    const lead = team.members.find(m => m.name === TEAM_LEAD_NAME)!;
    assert.equal(lead.permissionMode, 'plan');
    assert.deepEqual(team.teamAllowedTools, ['bash', 'file_edit']);
    const dev = team.members.find(m => m.name === 'dev')!;
    assert.equal(dev.permissionMode, 'auto-allow');
  });

  it('rejects an invalid lead permission_mode without creating a team', async () => {
    const r = await teamCreateTool.execute({ team_name: 'bad', permission_mode: 'nope' }, ctx());
    assert.ok(r.isError && r.output.includes('permission_mode'));
    assert.equal(getCurrentTeam(), null);
    assert.ok(!fs.existsSync(path.join(tmp, '.ami', 'teams', 'bad')));
  });

  it('rejects an invalid per-member permission_mode without creating a team', async () => {
    const r = await teamCreateTool.execute({
      team_name: 'bad2',
      members: [{ name: 'dev', permission_mode: 'bogus' }],
    }, ctx());
    assert.ok(r.isError && r.output.includes('permission_mode'));
    assert.equal(getCurrentTeam(), null);
  });

  it('rejects a non-array members input', async () => {
    const r = await teamCreateTool.execute({ team_name: 'bad3', members: 'alice' }, ctx());
    assert.ok(r.isError && r.output.includes('members must be an array'));
    assert.equal(getCurrentTeam(), null);
  });

  // --- Gap 7: atomic in-memory/on-disk consistency on a failed write ---
  it('does not leave a phantom in-memory team when the disk write fails', async () => {
    const origWrite = _teamHooks.writeFileSync;
    _teamHooks.writeFileSync = (() => { throw new Error('EACCES: read-only cwd'); }) as typeof fs.writeFileSync;
    try {
      const r = await teamCreateTool.execute({ team_name: 'doomed' }, ctx());
      assert.ok(r.isError, 'create must report an error when the write fails');
      assert.ok(r.output.includes('failed to create team'));
      // The key invariant: no phantom team is committed in memory, so a retry works.
      assert.equal(getCurrentTeam(), null);
    } finally {
      _teamHooks.writeFileSync = origWrite;
    }
    // A subsequent create (write restored) must succeed — proving no stale state.
    const r2 = await teamCreateTool.execute({ team_name: 'doomed' }, ctx());
    assert.equal(r2.isError, undefined);
    assert.equal(getCurrentTeam()!.name, 'doomed');
  });
});
