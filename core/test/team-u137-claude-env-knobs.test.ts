import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  teamCreateTool,
  resetTeam,
  setDynamicTeamContext,
  clearDynamicTeamContext,
  getSelfAgentId,
  getSelfParentSessionId,
  getSelfIdentity,
  isTeammate,
} from '../src/tools/team';
import {
  createTeammateContext,
  runWithTeammateContext,
} from '../src/tools/teammate-context';
import { resetTaskState } from '../src/tools/task-tracker';
import { __resetLoggerForTest } from '../src/logger';
import type { ToolContext } from '../src/types';

let tmp: string;
let prevDataDir: string | undefined;

function ctx(sessionId = 'sess-u137'): ToolContext {
  return { cwd: tmp, sessionId, abortSignal: new AbortController().signal } as unknown as ToolContext;
}

// Every env knob that can seed out-of-process teammate identity, so each test
// starts from a clean slate regardless of the host environment.
const IDENTITY_ENV = [
  'AMI_TEAMMATE_AGENT_ID', 'AMI_TEAMMATE_AGENT_NAME', 'AMI_TEAMMATE_TEAM_NAME', 'AMI_TEAMMATE_COLOR',
  'CLAUDE_CODE_AGENT_ID', 'CLAUDE_CODE_PARENT_SESSION_ID',
];
const savedEnv: Record<string, string | undefined> = {};

describe('U137 team.ts Claude-Code env knobs: CLAUDE_CODE_AGENT_ID / CLAUDE_CODE_PARENT_SESSION_ID', () => {
  beforeEach(() => {
    resetTeam();
    resetTaskState();
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'team-u137-'));
    prevDataDir = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = path.join(tmp, '.data');
    for (const k of IDENTITY_ENV) { savedEnv[k] = process.env[k]; delete process.env[k]; }
  });
  afterEach(() => {
    resetTeam();
    resetTaskState();
    clearDynamicTeamContext();
    if (prevDataDir === undefined) delete process.env.SUPERINFERENCE_DATA_DIR; else process.env.SUPERINFERENCE_DATA_DIR = prevDataDir;
    for (const k of IDENTITY_ENV) { if (savedEnv[k] === undefined) delete process.env[k]; else process.env[k] = savedEnv[k]; }
    __resetLoggerForTest();
    try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ }
  });

  // --- getSelfAgentId: CLAUDE_CODE_AGENT_ID compat fallback -------------------
  describe('getSelfAgentId reads CLAUDE_CODE_AGENT_ID', () => {
    it('seeds the agent id for an out-of-process teammate from CLAUDE_CODE_AGENT_ID', () => {
      process.env.CLAUDE_CODE_AGENT_ID = 'cc-agent@cc-team';
      assert.equal(getSelfAgentId(), 'cc-agent@cc-team');
    });

    it('AMI_TEAMMATE_AGENT_ID takes precedence over CLAUDE_CODE_AGENT_ID', () => {
      process.env.AMI_TEAMMATE_AGENT_ID = 'ami-agent@ami-team';
      process.env.CLAUDE_CODE_AGENT_ID = 'cc-agent@cc-team';
      assert.equal(getSelfAgentId(), 'ami-agent@ami-team');
    });

    it('dynamic/ALS context takes precedence over the CLAUDE_CODE_AGENT_ID env knob', () => {
      process.env.CLAUDE_CODE_AGENT_ID = 'cc-agent@cc-team';
      setDynamicTeamContext({ agentId: 'dyn@t', agentName: 'dyn', teamName: 't', planModeRequired: false });
      assert.equal(getSelfAgentId(), 'dyn@t');
      const als = createTeammateContext({ agentId: 'als@t', agentName: 'als', teamName: 't', parentSessionId: 'p' });
      runWithTeammateContext(als, () => {
        assert.equal(getSelfAgentId(), 'als@t');
      });
    });

    it('ignores an empty CLAUDE_CODE_AGENT_ID (undefined for a standalone session)', () => {
      process.env.CLAUDE_CODE_AGENT_ID = '';
      assert.equal(getSelfAgentId(), undefined);
    });
  });

  // --- getSelfParentSessionId: CLAUDE_CODE_PARENT_SESSION_ID fallback ---------
  describe('getSelfParentSessionId reads CLAUDE_CODE_PARENT_SESSION_ID', () => {
    it('seeds the parent-session correlation id from CLAUDE_CODE_PARENT_SESSION_ID', () => {
      process.env.CLAUDE_CODE_PARENT_SESSION_ID = 'lead-sess-cc';
      assert.equal(getSelfParentSessionId(), 'lead-sess-cc');
    });

    it('prefers an explicit CLAUDE_CODE_PARENT_SESSION_ID over the active team leadSessionId', async () => {
      await teamCreateTool.execute({ team_name: 'gamma' }, ctx('lead-sess-local'));
      process.env.CLAUDE_CODE_PARENT_SESSION_ID = 'lead-sess-cc';
      assert.equal(getSelfParentSessionId(), 'lead-sess-cc');
    });

    it('dynamic/ALS parent takes precedence over the env knob', () => {
      process.env.CLAUDE_CODE_PARENT_SESSION_ID = 'env-parent';
      setDynamicTeamContext({ agentId: 'w@t', agentName: 'w', teamName: 't', planModeRequired: false, parentSessionId: 'dyn-parent' });
      assert.equal(getSelfParentSessionId(), 'dyn-parent');
      const als = createTeammateContext({ agentId: 'als@t', agentName: 'als', teamName: 't', parentSessionId: 'als-parent' });
      runWithTeammateContext(als, () => {
        assert.equal(getSelfParentSessionId(), 'als-parent');
      });
    });

    it('ignores an empty CLAUDE_CODE_PARENT_SESSION_ID', () => {
      process.env.CLAUDE_CODE_PARENT_SESSION_ID = '';
      assert.equal(getSelfParentSessionId(), undefined);
    });
  });

  // --- isTeammate honours a CLAUDE_CODE_AGENT_ID-seeded identity --------------
  describe('isTeammate with CLAUDE_CODE_AGENT_ID', () => {
    it('treats a separate process seeded via CLAUDE_CODE_AGENT_ID + AMI team name as a teammate', () => {
      process.env.CLAUDE_CODE_AGENT_ID = 'worker@delta';
      process.env.AMI_TEAMMATE_TEAM_NAME = 'delta';
      assert.equal(isTeammate(), true);
    });

    it('is not a teammate when only CLAUDE_CODE_AGENT_ID is set with no team name', () => {
      process.env.CLAUDE_CODE_AGENT_ID = 'worker@delta';
      assert.equal(isTeammate(), false);
    });
  });

  // --- aggregate introspection surface reflects the env knobs -----------------
  it('getSelfIdentity surfaces both CLAUDE_CODE_* knobs end-to-end', () => {
    process.env.CLAUDE_CODE_AGENT_ID = 'cc-agent@cc-team';
    process.env.CLAUDE_CODE_PARENT_SESSION_ID = 'lead-sess-cc';
    const self = getSelfIdentity();
    assert.equal(self.agentId, 'cc-agent@cc-team');
    assert.equal(self.parentSessionId, 'lead-sess-cc');
  });
});
