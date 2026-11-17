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
  formatAgentId,
  // U038-w69 additions: resolved self-identity introspection surface
  getSelfIdentity,
  getSelfPlanModeRequired,
  getSelfParentSessionId,
} from '../src/tools/team';
import {
  createTeammateContext,
  runWithTeammateContext,
} from '../src/tools/teammate-context';
import { resetTaskState } from '../src/tools/task-tracker';
import { TEAM_LEAD_NAME } from '../src/tools/send-message';
import { __resetLoggerForTest } from '../src/logger';
import type { ToolContext } from '../src/types';

let tmp: string;
let prevDataDir: string | undefined;

function ctx(sessionId = 'sess-w69'): ToolContext {
  return { cwd: tmp, sessionId, abortSignal: new AbortController().signal } as unknown as ToolContext;
}

const IDENTITY_ENV = ['AMI_TEAMMATE_AGENT_ID', 'AMI_TEAMMATE_AGENT_NAME', 'AMI_TEAMMATE_TEAM_NAME', 'AMI_TEAMMATE_COLOR'];
const savedEnv: Record<string, string | undefined> = {};

describe('U038-w69 team.ts resolved self-identity introspection surface', () => {
  beforeEach(() => {
    resetTeam();
    resetTaskState();
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'team-w69-'));
    prevDataDir = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = path.join(tmp, '.data');
    for (const k of IDENTITY_ENV) { savedEnv[k] = process.env[k]; delete process.env[k]; }
  });
  afterEach(() => {
    resetTeam();
    resetTaskState();
    if (prevDataDir === undefined) delete process.env.SUPERINFERENCE_DATA_DIR; else process.env.SUPERINFERENCE_DATA_DIR = prevDataDir;
    for (const k of IDENTITY_ENV) { if (savedEnv[k] === undefined) delete process.env[k]; else process.env[k] = savedEnv[k]; }
    __resetLoggerForTest();
    try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ }
  });

  // --- getSelfPlanModeRequired (parity: isPlanModeRequired) --------------------
  describe('getSelfPlanModeRequired', () => {
    it('defaults to false for a standalone session with no bound identity', () => {
      assert.equal(getSelfPlanModeRequired(), false);
    });

    it('reads the dynamic context flag', () => {
      setDynamicTeamContext({ agentId: 'w@t', agentName: 'w', teamName: 't', planModeRequired: true });
      assert.equal(getSelfPlanModeRequired(), true);
      clearDynamicTeamContext();
      assert.equal(getSelfPlanModeRequired(), false);
    });

    it('ALS context takes precedence over dynamic for the plan-mode flag', () => {
      setDynamicTeamContext({ agentId: 'dyn@t', agentName: 'dyn', teamName: 't', planModeRequired: false });
      const als = createTeammateContext({ agentId: 'als@t', agentName: 'als', teamName: 't', planModeRequired: true, parentSessionId: 'p' });
      runWithTeammateContext(als, () => {
        assert.equal(getSelfPlanModeRequired(), true, 'ALS true overrides dynamic false');
      });
      // Outside the ALS scope the dynamic layer wins again.
      assert.equal(getSelfPlanModeRequired(), false);
    });
  });

  // --- getSelfParentSessionId (parity: getParentSessionId) --------------------
  describe('getSelfParentSessionId', () => {
    it('is undefined for a standalone session with no team lineage', () => {
      assert.equal(getSelfParentSessionId(), undefined);
    });

    it('falls back to the active team leadSessionId when no dynamic/ALS parent is bound', async () => {
      await teamCreateTool.execute({ team_name: 'alpha' }, ctx('lead-sess-1'));
      assert.equal(getSelfParentSessionId(), 'lead-sess-1');
    });

    it('prefers the dynamic context parent over the team leadSessionId', async () => {
      await teamCreateTool.execute({ team_name: 'beta' }, ctx('lead-sess-2'));
      setDynamicTeamContext({ agentId: 'w@beta', agentName: 'w', teamName: 'beta', planModeRequired: false, parentSessionId: 'explicit-parent' });
      assert.equal(getSelfParentSessionId(), 'explicit-parent');
    });

    it('ALS parent takes precedence over dynamic', () => {
      setDynamicTeamContext({ agentId: 'dyn@t', agentName: 'dyn', teamName: 't', planModeRequired: false, parentSessionId: 'dyn-parent' });
      const als = createTeammateContext({ agentId: 'als@t', agentName: 'als', teamName: 't', parentSessionId: 'als-parent' });
      runWithTeammateContext(als, () => {
        assert.equal(getSelfParentSessionId(), 'als-parent');
      });
    });
  });

  // --- getSelfIdentity aggregate (parity: inspection aggregate) ----------------
  describe('getSelfIdentity', () => {
    it('returns an all-undefined/false record for a standalone session', () => {
      const id = getSelfIdentity();
      assert.deepEqual(id, {
        agentId: undefined,
        agentName: undefined,
        teamName: undefined,
        color: undefined,
        planModeRequired: false,
        parentSessionId: undefined,
        isTeammate: false,
        isLeader: false,
        role: 'none',
        isInProcess: false,
        // A standalone session is its own approver — it may self-approve permissions
        // (parity: WorkerPendingPermission applies only to a non-lead worker).
        canSelfApprovePermission: true,
      });
    });

    it('resolves the lead after team_create (isLeader true, isTeammate false)', async () => {
      await teamCreateTool.execute({ team_name: 'alpha' }, ctx('lead-sess'));
      const team = getCurrentTeam()!;
      const id = getSelfIdentity();
      assert.equal(id.isLeader, true);
      assert.equal(id.isTeammate, false);
      assert.equal(id.agentName, TEAM_LEAD_NAME);
      assert.equal(id.agentId, team.leadAgentId);
      assert.equal(id.teamName, team.name);
      assert.equal(id.parentSessionId, 'lead-sess');
      assert.equal(id.isInProcess, false);
    });

    it('resolves a bound non-lead member as a teammate with its full identity', async () => {
      await teamCreateTool.execute({ team_name: 'gamma' }, ctx());
      const team = getCurrentTeam()!;
      setDynamicTeamContext({
        agentId: formatAgentId('worker', team.name),
        agentName: 'worker',
        teamName: team.name,
        color: 'cyan',
        planModeRequired: true,
        parentSessionId: 'lead-sess-3',
      });
      const id = getSelfIdentity();
      assert.equal(id.isTeammate, true);
      assert.equal(id.isLeader, false);
      assert.equal(id.agentName, 'worker');
      assert.equal(id.color, 'cyan');
      assert.equal(id.planModeRequired, true);
      assert.equal(id.parentSessionId, 'lead-sess-3');
    });

    it('reflects an in-process teammate ALS scope (isInProcess true)', () => {
      const als = createTeammateContext({ agentId: 'ip@t', agentName: 'ip', teamName: 't', color: 'green', planModeRequired: true, parentSessionId: 'lead' });
      runWithTeammateContext(als, () => {
        const id = getSelfIdentity();
        assert.equal(id.isInProcess, true);
        assert.equal(id.isTeammate, true);
        assert.equal(id.agentId, 'ip@t');
        assert.equal(id.agentName, 'ip');
        assert.equal(id.color, 'green');
        assert.equal(id.planModeRequired, true);
        assert.equal(id.parentSessionId, 'lead');
      });
    });
  });

  // --- E2E: the aggregate is wired into team_create's output ------------------
  describe('team_create output surfaces the resolved identity', () => {
    it('reports the leader role + self name via getSelfIdentity', async () => {
      const res = await teamCreateTool.execute({ team_name: 'delta' }, ctx());
      assert.equal(res.isError, undefined);
      assert.match(res.output, /You are the team lead \(self: main\)\./);
    });
  });
});
