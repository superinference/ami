import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  teamCreateTool,
  resetTeam,
  getCurrentTeam,
  setDynamicTeamContext,
  clearDynamicTeamContext,
  getSelfRole,
  getSelfIdentity,
  isLeader,
  isTeammate,
  TEAM_ROLES,
} from '../src/tools/team';
import { resetTaskState } from '../src/tools/task-tracker';
import type { ToolContext } from '../src/types';

let tmp: string;
let prevDataDir: string | undefined;

function ctx(sessionId = 'sess-w72'): ToolContext {
  return { cwd: tmp, sessionId, abortSignal: new AbortController().signal } as unknown as ToolContext;
}

describe('U038-w72 team.ts leader/teammate role distinction', () => {
  beforeEach(() => {
    resetTeam();
    resetTaskState();
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'team-w72-'));
    prevDataDir = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = path.join(tmp, '.data');
  });
  afterEach(() => {
    resetTeam();
    resetTaskState();
    clearDynamicTeamContext();
    if (prevDataDir === undefined) delete process.env.SUPERINFERENCE_DATA_DIR; else process.env.SUPERINFERENCE_DATA_DIR = prevDataDir;
    try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ }
  });

  describe('TEAM_ROLES enum', () => {
    it('is exactly the two team roles, lead then teammate', () => {
      assert.deepEqual([...TEAM_ROLES], ['lead', 'teammate']);
    });
  });

  describe('getSelfRole()', () => {
    it("is 'none' for a standalone session with no team lineage", () => {
      assert.equal(getCurrentTeam(), null);
      assert.equal(getSelfRole(), 'none');
    });

    it("is 'lead' for the session that created the team", async () => {
      const res = await teamCreateTool.execute({ team_name: 'alpha' }, ctx());
      assert.equal(res.isError, undefined);
      assert.equal(isLeader(), true);
      assert.equal(getSelfRole(), 'lead');
    });

    it("is 'teammate' for a bound non-lead dynamic identity", () => {
      // No active team; a dynamic context for a non-lead worker resolves to teammate.
      setDynamicTeamContext({ agentId: 'worker@myteam', agentName: 'worker', teamName: 'myteam', planModeRequired: false });
      assert.equal(isTeammate(), true);
      assert.equal(isLeader(), false);
      assert.equal(getSelfRole(), 'teammate');
    });

    it("stays 'teammate' when a non-lead worker is bound alongside an active team", async () => {
      await teamCreateTool.execute({ team_name: 'beta' }, ctx());
      const team = getCurrentTeam()!;
      // Rebind identity to a non-lead member of the SAME active team.
      setDynamicTeamContext({ agentId: 'worker@beta', agentName: 'worker', teamName: team.name, planModeRequired: false });
      assert.notEqual('worker@beta', team.leadAgentId);
      assert.equal(getSelfRole(), 'teammate');
    });
  });

  describe('getSelfIdentity().role', () => {
    it("matches getSelfRole() and stays consistent with isLeader/isTeammate for every state", async () => {
      // standalone
      assert.equal(getSelfIdentity().role, 'none');

      // lead
      await teamCreateTool.execute({ team_name: 'gamma' }, ctx());
      let self = getSelfIdentity();
      assert.equal(self.role, 'lead');
      assert.equal(self.role, getSelfRole());
      assert.equal(self.isLeader, true);
      assert.equal(self.isTeammate, false);

      // teammate
      setDynamicTeamContext({ agentId: 'w@gamma', agentName: 'w', teamName: getCurrentTeam()!.name, planModeRequired: false });
      self = getSelfIdentity();
      assert.equal(self.role, 'teammate');
      assert.equal(self.role, getSelfRole());
      assert.equal(self.isTeammate, true);
      assert.equal(self.isLeader, false);
    });
  });

  describe('role-aware team_create wording', () => {
    it('surfaces the resolved lead role in the create result', async () => {
      const res = await teamCreateTool.execute({ team_name: 'delta' }, ctx());
      assert.equal(res.isError, undefined);
      assert.match(res.output, /You are the team lead/);
    });
  });
});
