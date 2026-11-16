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
  setTeamMemberActive,
  setMemberMode,
  pruneIdleMembers,
  formatAgentId,
  parseAgentId,
  _teamHooks,
} from '../src/tools/team';
import { TEAM_LEAD_NAME } from '../src/tools/send-message';
import type { ToolContext } from '../src/types';

let tmp: string;
function ctx(): ToolContext {
  return { cwd: tmp, sessionId: 'sess-1', abortSignal: new AbortController().signal } as ToolContext;
}

/** The team file is persisted under an async cross-process lock; poll until the
 *  on-disk member set matches (or time out), mirroring the existing team tests. */
async function readPersistedMembers(file: string, expected: string[]): Promise<string[]> {
  const want = [...expected].sort().join(',');
  for (let i = 0; i < 50; i++) {
    try {
      const got = (JSON.parse(fs.readFileSync(file, 'utf-8')).members as Array<{ name: string }>)
        .map(m => m.name).sort();
      if (got.join(',') === want) return got;
    } catch { /* not written yet */ }
    await new Promise(r => setTimeout(r, 20));
  }
  return (JSON.parse(fs.readFileSync(file, 'utf-8')).members as Array<{ name: string }>).map(m => m.name).sort();
}

describe('deterministic agentName@teamName agent IDs', () => {
  beforeEach(() => {
    resetTeam();
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'team-agentid-'));
  });
  afterEach(() => {
    resetTeam();
    try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ }
  });

  it('formatAgentId joins name and team with @', () => {
    assert.equal(formatAgentId('researcher', 'proj'), 'researcher@proj');
    assert.equal(formatAgentId(TEAM_LEAD_NAME, 'squad'), 'main@squad');
  });

  it('parseAgentId splits on the FIRST @ (team name may itself contain @)', () => {
    assert.deepEqual(parseAgentId('researcher@proj'), { agentName: 'researcher', teamName: 'proj' });
    assert.deepEqual(parseAgentId('a@b@c'), { agentName: 'a', teamName: 'b@c' });
  });

  it('parseAgentId returns null when there is no @ separator', () => {
    assert.equal(parseAgentId('nosep'), null);
  });

  it('round-trips: parseAgentId(formatAgentId(x,y)) recovers the parts', () => {
    const id = formatAgentId('tester', 'my-project');
    assert.deepEqual(parseAgentId(id), { agentName: 'tester', teamName: 'my-project' });
  });

  it('the lead agent id is the deterministic formatAgentId(TEAM_LEAD_NAME, team)', async () => {
    const r = await teamCreateTool.execute({ team_name: 'squad' }, ctx());
    assert.equal(r.isError, undefined);
    const team = getCurrentTeam()!;
    assert.equal(team.leadAgentId, formatAgentId(TEAM_LEAD_NAME, 'squad'));
    assert.equal(team.leadAgentId, 'main@squad');
    assert.equal(team.members[0].agentId, 'main@squad');
  });

  it('a member is addressable by its deterministic name@team id (lookup-free)', async () => {
    await teamCreateTool.execute({ team_name: 'squad' }, ctx());
    // Registered with an arbitrary spawn/task id, NOT the deterministic id.
    registerTeamMember({ name: 'bob', agentId: 'task-xyz-123' });
    // Addressing by the deterministic id still resolves the member.
    const applied = setMemberMode(formatAgentId('bob', 'squad'), 'plan');
    assert.equal(applied, 'plan');
    assert.equal(getCurrentTeam()!.members.find(m => m.name === 'bob')!.permissionMode, 'plan');
  });

  it('a deterministic id for a DIFFERENT team does not match a member', async () => {
    await teamCreateTool.execute({ team_name: 'squad' }, ctx());
    registerTeamMember({ name: 'bob', agentId: 'task-xyz-123' });
    assert.equal(setMemberMode(formatAgentId('bob', 'other-team'), 'plan'), null);
  });
});

describe('pruneIdleMembers', () => {
  beforeEach(() => {
    resetTeam();
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'team-prune-'));
  });
  afterEach(() => {
    resetTeam();
    try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ }
  });

  it('returns [] when there is no active team', () => {
    assert.deepEqual(pruneIdleMembers(), []);
  });

  it('removes only idle non-lead members, keeps lead + active, persists', async () => {
    await teamCreateTool.execute({ team_name: 'squad' }, ctx());
    registerTeamMember({ name: 'active-one', agentId: 'a1' });
    registerTeamMember({ name: 'idle-one', agentId: 'i1' });
    registerTeamMember({ name: 'idle-two', agentId: 'i2' });
    setTeamMemberActive('idle-one', false);
    setTeamMemberActive('idle-two', false);

    const removed = pruneIdleMembers();
    assert.deepEqual(removed.map(m => m.name).sort(), ['idle-one', 'idle-two']);

    const team = getCurrentTeam()!;
    assert.deepEqual(team.members.map(m => m.name).sort(), ['active-one', TEAM_LEAD_NAME].sort());

    // Persisted to disk (async lock — poll until flushed).
    const persisted = await readPersistedMembers(
      path.join(tmp, '.ami', 'teams', 'squad', 'team.json'),
      ['active-one', TEAM_LEAD_NAME],
    );
    assert.deepEqual(persisted, ['active-one', TEAM_LEAD_NAME].sort());
  });

  it('never prunes the lead and is a no-op (returns []) when nothing is idle', async () => {
    await teamCreateTool.execute({ team_name: 'squad' }, ctx());
    registerTeamMember({ name: 'active-one', agentId: 'a1' });
    assert.deepEqual(pruneIdleMembers(), []);
    assert.equal(getCurrentTeam()!.members.length, 2);
  });
});

describe('team_delete removes git worktrees', () => {
  let origExec: typeof _teamHooks.exec;
  let origRm: typeof _teamHooks.rmSync;
  let origExists: typeof _teamHooks.existsSync;
  beforeEach(() => {
    resetTeam();
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'team-wt-'));
    origExec = _teamHooks.exec;
    origRm = _teamHooks.rmSync;
    origExists = _teamHooks.existsSync;
  });
  afterEach(() => {
    resetTeam();
    _teamHooks.exec = origExec;
    _teamHooks.rmSync = origRm;
    _teamHooks.existsSync = origExists;
    try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ }
  });

  it('runs `git worktree remove --force <path>` for each member worktree and reports it', async () => {
    const calls: string[][] = [];
    _teamHooks.exec = ((args: string[]) => { calls.push(args); return ''; }) as typeof _teamHooks.exec;

    await teamCreateTool.execute({ team_name: 'squad' }, ctx());
    registerTeamMember({ name: 'worker', agentId: 'w1', worktreePath: '/tmp/fake-wt/worker', isActive: false });

    const r = await teamDeleteTool.execute({}, ctx());
    assert.equal(r.isError, undefined);
    assert.ok(r.output.includes('worktree'), `output should mention worktrees: ${r.output}`);
    const removeCall = calls.find(c => c[0] === 'worktree' && c[1] === 'remove');
    assert.ok(removeCall, 'git worktree remove must be invoked');
    assert.deepEqual(removeCall, ['worktree', 'remove', '--force', '/tmp/fake-wt/worker']);
  });

  it('falls back to rmSync + prune when `git worktree remove` fails', async () => {
    const execCalls: string[][] = [];
    let rmCalled = '';
    _teamHooks.exec = ((args: string[]) => {
      execCalls.push(args);
      if (args[0] === 'worktree' && args[1] === 'remove') throw new Error('git removal failed');
      return '';
    }) as typeof _teamHooks.exec;
    _teamHooks.existsSync = (() => true) as typeof _teamHooks.existsSync;
    _teamHooks.rmSync = ((p: string) => { rmCalled = p; }) as typeof _teamHooks.rmSync;

    await teamCreateTool.execute({ team_name: 'squad' }, ctx());
    registerTeamMember({ name: 'worker', agentId: 'w1', worktreePath: '/tmp/fake-wt/worker', isActive: false });

    const r = await teamDeleteTool.execute({}, ctx());
    assert.equal(r.isError, undefined);
    assert.equal(rmCalled, '/tmp/fake-wt/worker');
    assert.ok(execCalls.some(c => c[0] === 'worktree' && c[1] === 'prune'), 'prune fallback must run');
    assert.ok(r.output.includes('worktree'), 'output should still report worktrees cleaned');
  });

  it('members without a worktreePath do not trigger worktree removal', async () => {
    const calls: string[][] = [];
    _teamHooks.exec = ((args: string[]) => { calls.push(args); return ''; }) as typeof _teamHooks.exec;

    await teamCreateTool.execute({ team_name: 'squad' }, ctx());
    registerTeamMember({ name: 'worker', agentId: 'w1', isActive: false });

    const r = await teamDeleteTool.execute({}, ctx());
    assert.equal(r.isError, undefined);
    assert.equal(calls.length, 0, 'no git worktree commands when no member has a worktreePath');
    assert.ok(!r.output.includes('worktree'), 'no worktree mention when none were removed');
  });
});
