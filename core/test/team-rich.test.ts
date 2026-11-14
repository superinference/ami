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
} from '../src/tools/team';
import { TEAM_LEAD_NAME } from '../src/tools/send-message';
import type { ToolContext } from '../src/types';

let tmp: string;
function ctx(): ToolContext {
  return { cwd: tmp, sessionId: 'sess-1', abortSignal: new AbortController().signal } as ToolContext;
}

describe('team rich structure + guards', () => {
  beforeEach(() => {
    resetTeam();
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'team-rich-'));
  });
  afterEach(() => {
    resetTeam();
    try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ }
  });

  it('writes a rich team file with lead member + lead identity', async () => {
    const r = await teamCreateTool.execute({ team_name: 'squad', description: 'd', model: 'opus' }, ctx());
    assert.equal(r.isError, undefined);
    const team = getCurrentTeam()!;
    assert.equal(team.name, 'squad');
    assert.equal(team.leadAgentId, 'lead-squad');
    assert.equal(team.leadSessionId, 'sess-1');
    assert.equal(team.members.length, 1);
    assert.equal(team.members[0].name, TEAM_LEAD_NAME);
    assert.equal(team.members[0].agentType, 'lead');
    // persisted
    const file = JSON.parse(fs.readFileSync(path.join(tmp, '.ami', 'teams', 'squad', 'team.json'), 'utf-8'));
    assert.equal(file.leadAgentId, 'lead-squad');
    assert.ok(Array.isArray(file.members));
  });

  it('auto-renames on an on-disk name collision', async () => {
    fs.mkdirSync(path.join(tmp, '.ami', 'teams', 'dup'), { recursive: true });
    const r = await teamCreateTool.execute({ team_name: 'dup' }, ctx());
    assert.equal(r.isError, undefined);
    assert.equal(getCurrentTeam()!.name, 'dup-2');
    assert.ok(r.output.includes('renamed from'));
  });

  it('rejects an over-long team name', async () => {
    const r = await teamCreateTool.execute({ team_name: 'x'.repeat(100) }, ctx());
    assert.ok(r.isError && r.output.includes('at most'));
  });

  it('refuses to delete a team with an active member, allows after it goes idle', async () => {
    await teamCreateTool.execute({ team_name: 'squad' }, ctx());
    registerTeamMember({ name: 'worker', agentId: 'agent-1' });
    const blocked = await teamDeleteTool.execute({}, ctx());
    assert.ok(blocked.isError, 'delete blocked while member active');
    assert.ok(blocked.output.includes('worker'));
    assert.ok(blocked.output.includes('shutdown_request'));
    assert.ok(getCurrentTeam(), 'team still exists');
    // Member goes idle -> delete now succeeds.
    setTeamMemberActive('agent-1', false);
    const ok = await teamDeleteTool.execute({}, ctx());
    assert.equal(ok.isError, undefined);
    assert.equal(getCurrentTeam(), null);
    assert.ok(!fs.existsSync(path.join(tmp, '.ami', 'teams', 'squad')));
  });

  it('surfaces a cleanup failure instead of reporting success', async (t) => {
    if (process.getuid && process.getuid() === 0) {
      t.skip('running as root ignores directory permissions');
      return;
    }
    await teamCreateTool.execute({ team_name: 'squad' }, ctx());
    // Make the teams root read-only so removing the child dir fails with EACCES,
    // exercising the cleanup-failure catch arm with a real filesystem error.
    const teamsRoot = path.join(tmp, '.ami', 'teams');
    fs.chmodSync(teamsRoot, 0o500);
    try {
      const r = await teamDeleteTool.execute({}, ctx());
      assert.ok(r.isError, 'cleanup failure is reported as an error');
      assert.ok(r.output.includes('failed to remove'));
      assert.ok(getCurrentTeam(), 'team remains after failed cleanup');
    } finally {
      fs.chmodSync(teamsRoot, 0o700);
    }
  });

  it('registerTeamMember upserts and is a no-op without a team', async () => {
    registerTeamMember({ name: 'nobody', agentId: 'x' }); // no team -> no-op
    await teamCreateTool.execute({ team_name: 'squad' }, ctx());
    registerTeamMember({ name: 'worker', agentId: 'a1', model: 'haiku' });
    registerTeamMember({ name: 'worker', agentId: 'a1', model: 'opus' }); // update
    const members = getCurrentTeam()!.members.filter(m => m.name === 'worker');
    assert.equal(members.length, 1);
    assert.equal(members[0].model, 'opus');
  });
});
