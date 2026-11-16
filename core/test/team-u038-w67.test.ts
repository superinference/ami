import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  teamCreateTool,
  teamDeleteTool,
  resetTeam,
  readTeamFile,
  loadTeamFromDisk,
  getTeammateStatuses,
  registerTeamMember,
  setTeamMemberActive,
  getCurrentTeam,
  isBackendType,
  isPaneBackend,
  setSelfIdentity,
  clearSelfIdentity,
  getSelfAgentId,
  getSelfAgentName,
  isLeader,
  formatAgentId,
  type TeamFile,
} from '../src/tools/team';
import { resetTaskState } from '../src/tools/task-tracker';
import { TEAM_LEAD_NAME } from '../src/tools/send-message';
import type { ToolContext } from '../src/types';

let tmp: string;
let dataDir: string;
let prevDataDir: string | undefined;

function ctx(): ToolContext {
  return { cwd: tmp, sessionId: 'sess-w67', abortSignal: new AbortController().signal } as ToolContext;
}

describe('U038-w67 team.ts parity (backend type, self identity/leader, delete UX, strict schema)', () => {
  beforeEach(() => {
    resetTeam();
    resetTaskState();
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'team-w67-'));
    dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'team-w67-data-'));
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

  // --- Backend type validators (parity: BackendType / isPaneBackend) ---
  it('isBackendType accepts the known backends and rejects everything else', () => {
    for (const ok of ['tmux', 'iterm2', 'in-process']) assert.ok(isBackendType(ok), `${ok} should be valid`);
    for (const bad of ['TMUX', 'screen', '', undefined, null, 42, {}]) {
      assert.equal(isBackendType(bad), false, `${String(bad)} should be invalid`);
    }
  });

  it('isPaneBackend accepts only pane backends (in-process is NOT a pane backend)', () => {
    assert.ok(isPaneBackend('tmux'));
    assert.ok(isPaneBackend('iterm2'));
    assert.equal(isPaneBackend('in-process'), false);
    assert.equal(isPaneBackend('nope'), false);
    assert.equal(isPaneBackend(undefined), false);
  });

  // --- Boundary validation of persisted enum fields on read ---
  it('readTeamFile coerces an invalid persisted permissionMode and backendType to undefined', () => {
    const dir = path.join(tmp, 'teamdir');
    fs.mkdirSync(dir, { recursive: true });
    const onDisk: TeamFile = {
      name: 'squad',
      leadAgentId: formatAgentId(TEAM_LEAD_NAME, 'squad'),
      members: [
        { name: TEAM_LEAD_NAME, agentId: formatAgentId(TEAM_LEAD_NAME, 'squad'), joinedAt: 'now' },
        // bogus enum values persisted by a stale/foreign writer
        { name: 'alice', agentId: formatAgentId('alice', 'squad'), permissionMode: 'wat' as unknown as string,
          backendType: 'screen' as unknown as 'tmux', joinedAt: 'now' },
        // a valid pane backend survives
        { name: 'bob', agentId: formatAgentId('bob', 'squad'), backendType: 'tmux', joinedAt: 'now' },
      ],
      createdAt: 'now',
    };
    fs.writeFileSync(path.join(dir, 'team.json'), JSON.stringify(onDisk));
    const read = readTeamFile(dir);
    assert.ok(read);
    const alice = read.members.find(m => m.name === 'alice')!;
    assert.equal(alice.permissionMode, undefined, 'invalid permissionMode coerced to undefined');
    assert.equal(alice.backendType, undefined, 'invalid backendType coerced to undefined');
    const bob = read.members.find(m => m.name === 'bob')!;
    assert.equal(bob.backendType, 'tmux', 'valid backendType preserved');
  });

  it('getTeammateStatuses surfaces only a PANE backendType (in-process → undefined)', async () => {
    await teamCreateTool.execute({ team_name: 'squad' }, ctx());
    registerTeamMember({ name: 'paneguy', agentId: formatAgentId('paneguy', 'squad'), backendType: 'iterm2' });
    registerTeamMember({ name: 'inproc', agentId: formatAgentId('inproc', 'squad'), backendType: 'in-process' });
    const statuses = getTeammateStatuses();
    const pane = statuses.find(s => s.name === 'paneguy')!;
    const inproc = statuses.find(s => s.name === 'inproc')!;
    assert.equal(pane.backendType, 'iterm2');
    assert.equal(inproc.backendType, undefined, 'in-process is not surfaced as a pane backend');
  });

  // --- Self identity + isLeader (parity: isTeamLead) ---
  it('team_create binds self identity to the lead and isLeader() is true for the creator', async () => {
    const r = await teamCreateTool.execute({ team_name: 'squad' }, ctx());
    assert.ok(!r.isError);
    assert.ok(r.output.includes('team lead'), 'create output states the lead role');
    assert.equal(getSelfAgentName(), TEAM_LEAD_NAME);
    assert.equal(getSelfAgentId(), getCurrentTeam()!.leadAgentId);
    assert.ok(isLeader(), 'creator is the lead');
  });

  it('isLeader: a non-lead self id is NOT the lead; a cleared id is the lead (backwards compat)', async () => {
    await teamCreateTool.execute({ team_name: 'squad' }, ctx());
    const team = getCurrentTeam()!;
    // Pretend to be a teammate process
    setSelfIdentity(formatAgentId('alice', 'squad'), 'alice');
    assert.equal(isLeader(team), false, 'a teammate is not the lead');
    // Backwards compat: no self id bound but a lead id exists => original creator
    clearSelfIdentity();
    assert.ok(isLeader(team), 'no bound id defaults to lead (backwards compat)');
    // No lead id at all => not a lead
    assert.equal(isLeader({ ...team, leadAgentId: '' } as TeamFile), false);
    assert.equal(isLeader(null), false);
  });

  it('team_delete and resetTeam clear the bound self identity', async () => {
    await teamCreateTool.execute({ team_name: 'squad' }, ctx());
    assert.ok(getSelfAgentId());
    await teamDeleteTool.execute({}, ctx());
    assert.equal(getSelfAgentId(), undefined, 'delete clears self identity');

    await teamCreateTool.execute({ team_name: 'squad2' }, ctx());
    assert.ok(getSelfAgentId());
    resetTeam();
    assert.equal(getSelfAgentId(), undefined, 'reset clears self identity');
  });

  it('reconnect (same session) rebinds self identity to the lead', async () => {
    await teamCreateTool.execute({ team_name: 'squad' }, ctx());
    // Drop in-memory state but keep disk — simulate a restart with the same session.
    clearSelfIdentity();
    resetTeam();
    const loaded = loadTeamFromDisk(tmp, 'squad');
    assert.ok(loaded, 'team persisted to disk');
    const r = await teamCreateTool.execute({ team_name: 'squad' }, ctx());
    assert.ok(r.output.includes('Reconnected'), 'reconnected to existing team');
    assert.equal(getSelfAgentName(), TEAM_LEAD_NAME);
    assert.ok(isLeader());
  });

  // --- team_delete UX: graceful no-op, dir path, description ---
  it('team_delete is a graceful non-error no-op when no team is active', async () => {
    const r = await teamDeleteTool.execute({}, ctx());
    assert.equal(r.isError, undefined);
    assert.ok(r.output.includes('nothing to clean up'));
  });

  it('team_delete success output states the removed team directory path', async () => {
    await teamCreateTool.execute({ team_name: 'squad' }, ctx());
    const teamDir = path.join(tmp, '.ami', 'teams', 'squad');
    const r = await teamDeleteTool.execute({}, ctx());
    assert.ok(!r.isError);
    assert.ok(r.output.includes(teamDir), `delete output should name ${teamDir}, got: ${r.output}`);
    assert.ok(!fs.existsSync(teamDir));
  });

  it('team_delete refuses while a teammate is active, then succeeds after shutdown (regression)', async () => {
    await teamCreateTool.execute({ team_name: 'squad' }, ctx());
    registerTeamMember({ name: 'alice', agentId: formatAgentId('alice', 'squad') });
    const blocked = await teamDeleteTool.execute({}, ctx());
    assert.ok(blocked.isError, 'blocked while a member is active');
    assert.ok(blocked.output.includes('alice'));
    setTeamMemberActive('alice', false);
    const ok = await teamDeleteTool.execute({}, ctx());
    assert.ok(!ok.isError, 'succeeds once the member is idle');
  });

  it('team_delete description documents what is removed and the shutdown-first constraint', () => {
    const d = teamDeleteTool.description;
    for (const needle of ['worktree', 'task list', 'shutdown_request', 'nothing to clean up']) {
      assert.ok(d.includes(needle), `description should mention "${needle}"`);
    }
  });

  // --- Strict schemas (parity: z.strictObject) ---
  it('both team tools reject unexpected keys via additionalProperties:false', () => {
    const create = teamCreateTool.inputSchema as { additionalProperties?: boolean };
    const del = teamDeleteTool.inputSchema as { additionalProperties?: boolean };
    assert.equal(create.additionalProperties, false);
    assert.equal(del.additionalProperties, false);
  });
});
