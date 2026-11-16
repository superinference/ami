import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  teamCreateTool,
  teamDeleteTool,
  resetTeam,
  getCurrentTeam,
  registerTeamMember,
  setTeamMemberActive,
  setMemberMode,
  cycleMemberMode,
  setMultipleMemberModes,
  removeTeamMember,
  pruneIdleMembers,
  // U038-w68 additions
  setSelfIdentity,
  getSelfAgentId,
  getSelfAgentName,
  getSelfTeamName,
  getSelfColor,
  isTeammate,
  setDynamicTeamContext,
  getDynamicTeamContext,
  clearDynamicTeamContext,
  syncTeammateMode,
  loadTeamFromSession,
  registerTeamForSessionCleanup,
  unregisterTeamForSessionCleanup,
  getSessionCreatedTeams,
  cleanupSessionTeams,
  formatAgentId,
} from '../src/tools/team';
import {
  createTeammateContext,
  getTeammateContext,
  isInProcessTeammate,
  runWithTeammateContext,
} from '../src/tools/teammate-context';
import { resetTaskState } from '../src/tools/task-tracker';
import { TEAM_LEAD_NAME } from '../src/tools/send-message';
import { __resetLoggerForTest } from '../src/logger';
import type { ToolContext } from '../src/types';

let tmp: string;
let prevDataDir: string | undefined;
let prevDebugFile: string | undefined;

function ctx(sessionId = 'sess-w68'): ToolContext {
  return { cwd: tmp, sessionId, abortSignal: new AbortController().signal } as unknown as ToolContext;
}

// Env vars the identity-resolution chain reads — cleared per test so a stray
// outer-env value never leaks into assertions.
const IDENTITY_ENV = ['AMI_TEAMMATE_AGENT_ID', 'AMI_TEAMMATE_AGENT_NAME', 'AMI_TEAMMATE_TEAM_NAME', 'AMI_TEAMMATE_COLOR'];
const savedEnv: Record<string, string | undefined> = {};

describe('U038-w68 team.ts parity (identity chain, diagnostics, reconnect, mode-sync, session cleanup)', () => {
  beforeEach(() => {
    resetTeam();
    resetTaskState();
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'team-w68-'));
    prevDataDir = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = path.join(tmp, '.data');
    for (const k of IDENTITY_ENV) { savedEnv[k] = process.env[k]; delete process.env[k]; }
  });
  afterEach(() => {
    resetTeam();
    resetTaskState();
    if (prevDataDir === undefined) delete process.env.SUPERINFERENCE_DATA_DIR; else process.env.SUPERINFERENCE_DATA_DIR = prevDataDir;
    for (const k of IDENTITY_ENV) { if (savedEnv[k] === undefined) delete process.env[k]; else process.env[k] = savedEnv[k]; }
    if (prevDebugFile === undefined) delete process.env.AMI_DEBUG_FILE; else process.env.AMI_DEBUG_FILE = prevDebugFile;
    __resetLoggerForTest();
    try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ }
  });

  // --- AsyncLocalStorage in-process context (gap 21) ---------------------------
  describe('teammate-context AsyncLocalStorage', () => {
    it('isolates identity per in-process teammate and is undefined outside a run scope', () => {
      assert.equal(getTeammateContext(), undefined);
      assert.equal(isInProcessTeammate(), false);
      const a = createTeammateContext({ agentId: 'a@t', agentName: 'a', teamName: 't', parentSessionId: 'lead' });
      const b = createTeammateContext({ agentId: 'b@t', agentName: 'b', teamName: 't', parentSessionId: 'lead' });
      const seen: string[] = [];
      runWithTeammateContext(a, () => {
        assert.equal(isInProcessTeammate(), true);
        seen.push(getTeammateContext()!.agentName);
        runWithTeammateContext(b, () => { seen.push(getTeammateContext()!.agentName); });
        seen.push(getTeammateContext()!.agentName); // back to a
      });
      assert.deepEqual(seen, ['a', 'b', 'a']);
      assert.equal(getTeammateContext(), undefined, 'context torn down after run');
    });

    it('createTeammateContext defaults planModeRequired false and provides an abortController', () => {
      const c = createTeammateContext({ agentId: 'x@t', agentName: 'x', teamName: 't', parentSessionId: 'p' });
      assert.equal(c.planModeRequired, false);
      assert.equal(c.isInProcess, true);
      assert.ok(c.abortController instanceof AbortController);
    });
  });

  // --- identity resolution precedence (gaps 19, 20, 23) -----------------------
  describe('identity resolution precedence (ALS > dynamic > env)', () => {
    it('getDynamicTeamContext round-trips and clears; setSelfIdentity preserves richer fields', () => {
      assert.equal(getDynamicTeamContext(), null);
      setDynamicTeamContext({ agentId: 'r@team', agentName: 'r', teamName: 'team', color: 'cyan', planModeRequired: true, parentSessionId: 'lead-sess' });
      assert.equal(getSelfAgentId(), 'r@team');
      assert.equal(getSelfColor(), 'cyan');
      // A plain id/name rebind must not drop color/planMode/parent.
      setSelfIdentity('r2@team', 'r2');
      const ctxNow = getDynamicTeamContext()!;
      assert.equal(ctxNow.agentId, 'r2@team');
      assert.equal(ctxNow.color, 'cyan');
      assert.equal(ctxNow.planModeRequired, true);
      assert.equal(ctxNow.parentSessionId, 'lead-sess');
      clearDynamicTeamContext();
      assert.equal(getSelfAgentId(), undefined);
      assert.equal(getDynamicTeamContext(), null);
    });

    it('falls back to env vars when no dynamic/ALS context is bound', () => {
      process.env.AMI_TEAMMATE_AGENT_ID = 'env-agent@env-team';
      process.env.AMI_TEAMMATE_AGENT_NAME = 'env-agent';
      process.env.AMI_TEAMMATE_TEAM_NAME = 'env-team';
      process.env.AMI_TEAMMATE_COLOR = 'pink';
      assert.equal(getSelfAgentId(), 'env-agent@env-team');
      assert.equal(getSelfAgentName(), 'env-agent');
      assert.equal(getSelfTeamName(), 'env-team');
      assert.equal(getSelfColor(), 'pink');
      assert.equal(isTeammate(), true, 'env agent+team => teammate');
    });

    it('ALS context takes precedence over dynamic and env', () => {
      process.env.AMI_TEAMMATE_AGENT_ID = 'env@t';
      setDynamicTeamContext({ agentId: 'dyn@t', agentName: 'dyn', teamName: 't', planModeRequired: false });
      const als = createTeammateContext({ agentId: 'als@t', agentName: 'als', teamName: 't', color: 'green', parentSessionId: 'p' });
      runWithTeammateContext(als, () => {
        assert.equal(getSelfAgentId(), 'als@t');
        assert.equal(getSelfAgentName(), 'als');
        assert.equal(getSelfColor(), 'green');
        assert.equal(isTeammate(), true);
      });
    });

    it('the team lead is NOT a teammate, but a bound non-lead member is', async () => {
      await teamCreateTool.execute({ team_name: 'alpha' }, ctx());
      // Creator is bound as lead.
      assert.equal(isTeammate(), false, 'lead is not a teammate');
      const team = getCurrentTeam()!;
      // Rebind self to a non-lead teammate id within the same team.
      setDynamicTeamContext({ agentId: formatAgentId('worker', team.name), agentName: 'worker', teamName: team.name, planModeRequired: false });
      assert.equal(isTeammate(), true, 'non-lead member is a teammate');
    });
  });

  // --- diagnostic / introspection trail (gap 8) -------------------------------
  describe('per-operation diagnostic trail', () => {
    function captureLogs(fn: () => void): string {
      const logFile = path.join(tmp, 'debug.log');
      prevDebugFile = process.env.AMI_DEBUG_FILE;
      process.env.AMI_DEBUG_FILE = logFile;
      __resetLoggerForTest();
      fn();
      let out = '';
      try { out = fs.readFileSync(logFile, 'utf8'); } catch { out = ''; }
      return out;
    }

    it('emits a [TeammateTool] line for every mutation', async () => {
      const logs = captureLogs(() => {
        // create -> register -> mode -> cycle -> batch -> idle -> remove
        void teamCreateTool.execute({ team_name: 'beta' }, ctx());
        registerTeamMember({ name: 'w1', agentId: 'w1@beta' });
        registerTeamMember({ name: 'w2', agentId: 'w2@beta' });
        setMemberMode('w1', 'auto-allow');
        cycleMemberMode('w1');
        setMultipleMemberModes([{ nameOrId: 'w2', mode: 'auto-allow' }]);
        setTeamMemberActive('w2', false);
        removeTeamMember('w1');
        pruneIdleMembers();
      });
      for (const needle of [
        '[TeammateTool] Created team beta',
        '[TeammateTool] Added member w1',
        '[TeammateTool] Set member w1 in team beta to mode: auto-allow',
        '[TeammateTool] Cycled member w1',
        '[TeammateTool] Set 1 member mode(s) in team beta',
        '[TeammateTool] Set member w2 in team beta to idle',
        '[TeammateTool] Removed member w1',
        '[TeammateTool] Pruned 1 idle member(s) from team beta',
      ]) {
        assert.ok(logs.includes(needle), `missing diagnostic: ${needle}\n--- logs ---\n${logs}`);
      }
    });

    it('emits create + disband lines through the tools', async () => {
      const logs = captureLogs(() => {
        void teamCreateTool.execute({ team_name: 'gamma' }, ctx());
      });
      assert.ok(logs.includes('[TeammateTool] Created team gamma'));
    });
  });

  // --- syncTeammateMode (gap 6) -----------------------------------------------
  describe('syncTeammateMode', () => {
    it('is a no-op (null) for the lead and when no team', () => {
      assert.equal(syncTeammateMode('auto-allow'), null, 'no team => null');
    });

    it('syncs the running teammate\'s own mode into the shared file', async () => {
      await teamCreateTool.execute({ team_name: 'delta' }, ctx());
      const team = getCurrentTeam()!;
      registerTeamMember({ name: 'worker', agentId: formatAgentId('worker', team.name) });
      // The lead cannot sync a teammate mode via syncTeammateMode (it's self-scoped).
      assert.equal(syncTeammateMode('auto-allow'), null, 'lead self-sync is a no-op');
      // Bind self as the worker teammate, then sync.
      setDynamicTeamContext({ agentId: formatAgentId('worker', team.name), agentName: 'worker', teamName: team.name, planModeRequired: false });
      const applied = syncTeammateMode('auto-allow');
      assert.equal(applied, 'auto-allow');
      const member = getCurrentTeam()!.members.find(m => m.name === 'worker');
      assert.equal(member?.permissionMode, 'auto-allow');
    });
  });

  // --- reconnection: loadTeamFromSession (gaps 4, 5) --------------------------
  describe('loadTeamFromSession', () => {
    it('rehydrates currentTeam + dynamic context for the lead from disk', async () => {
      await teamCreateTool.execute({ team_name: 'epsilon', members: ['scout'] }, ctx('orig-sess'));
      const created = getCurrentTeam()!;
      const leadId = created.leadAgentId;
      // Simulate a fresh process: drop all in-memory state.
      resetTeam();
      assert.equal(getCurrentTeam(), null);
      const restored = loadTeamFromSession(tmp, 'epsilon', TEAM_LEAD_NAME);
      assert.ok(restored, 'context restored');
      assert.equal(restored!.agentId, leadId);
      assert.equal(restored!.teamName, 'epsilon');
      assert.equal(getCurrentTeam()?.name, 'epsilon');
      assert.equal(getSelfTeamName(), 'epsilon');
    });

    it('returns null when the team file is missing', () => {
      assert.equal(loadTeamFromSession(tmp, 'nope', 'whoever'), null);
    });

    it('still restores (with a removed-member note) when the agent is gone from the roster', async () => {
      await teamCreateTool.execute({ team_name: 'zeta' }, ctx());
      resetTeam();
      const restored = loadTeamFromSession(tmp, 'zeta', 'ghost');
      assert.ok(restored, 'restores even when member absent');
      // Falls back to a deterministic id for the absent agent.
      assert.equal(restored!.agentId, formatAgentId('ghost', 'zeta'));
    });
  });

  // --- session-created-team cleanup (gaps 7, 9) -------------------------------
  describe('session cleanup tracking', () => {
    it('register/unregister/getSessionCreatedTeams', () => {
      assert.deepEqual(getSessionCreatedTeams(), []);
      registerTeamForSessionCleanup('t1');
      registerTeamForSessionCleanup('t2');
      assert.deepEqual(getSessionCreatedTeams().sort(), ['t1', 't2']);
      unregisterTeamForSessionCleanup('t1');
      assert.deepEqual(getSessionCreatedTeams(), ['t2']);
    });

    it('team_create registers and team_delete unregisters', async () => {
      await teamCreateTool.execute({ team_name: 'eta' }, ctx());
      assert.deepEqual(getSessionCreatedTeams(), ['eta']);
      await teamDeleteTool.execute({}, ctx());
      assert.deepEqual(getSessionCreatedTeams(), [], 'explicit delete unregisters');
    });

    it('cleanupSessionTeams removes an orphan team dir that was never disbanded', async () => {
      await teamCreateTool.execute({ team_name: 'theta' }, ctx());
      const dir = path.join(tmp, '.ami', 'teams', 'theta');
      assert.equal(fs.existsSync(dir), true);
      const removed = cleanupSessionTeams(tmp);
      assert.deepEqual(removed, ['theta']);
      assert.equal(fs.existsSync(dir), false, 'orphan team dir swept');
      assert.deepEqual(getSessionCreatedTeams(), [], 'tracking cleared');
    });

    it('cleanupSessionTeams is a no-op when nothing is registered', () => {
      assert.deepEqual(cleanupSessionTeams(tmp), []);
    });
  });
});
