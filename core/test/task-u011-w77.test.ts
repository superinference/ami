import { describe, it, afterEach, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { taskTool, resetAgentRegistry } from '../src/tools/task';
import { resetTeam, readTeamFile, registerTeamMemberInto, loadTeamFromDisk } from '../src/tools/team';
import type { ToolContext, TeamFile } from '../src/types';

function ctx(cwd: string, overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd,
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://localhost', apiKey: 'k', model: 'm' },
    ...overrides,
  } as ToolContext;
}

function writeTeam(cwd: string, name: string, extra?: Partial<TeamFile>): void {
  const dir = path.join(cwd, '.ami', 'teams', name);
  fs.mkdirSync(dir, { recursive: true });
  const team: TeamFile = {
    name,
    leadAgentId: `lead@${name}`,
    members: [],
    createdAt: new Date().toISOString(),
    ...extra,
  };
  fs.writeFileSync(path.join(dir, 'team.json'), JSON.stringify(team, null, 2));
}

// =====================================================================
// Feature: sub-agent handoff safety review (gaps 2,3)
// =====================================================================
describe('taskTool — handoff safety review (yolo/auto handoff)', () => {
  afterEach(() => { resetAgentRegistry(); resetTeam(); });

  async function* dangerousRun(_p: string) {
    yield { type: 'tool_use_start' as const, toolName: 'bash', toolCallId: 't1', input: { command: 'rm -rf /' } };
    yield { type: 'text_delta' as const, text: 'cleaned things up' };
    yield { type: 'turn_complete' as const, content: 'cleaned things up' };
    yield { type: 'done' as const, exitReason: 'completed' as const };
  }

  it('prepends a SECURITY WARNING when an auto-allow subagent ran a dangerous command', async () => {
    const result = await taskTool.execute(
      { prompt: 'do cleanup', description: 'cleanup', mode: 'general', permission_mode: 'auto-allow' },
      ctx(process.cwd(), { _engineFactory: () => ({ submit: dangerousRun }) }),
    );
    assert.ok(result.output.includes('SECURITY WARNING'),
      `expected a handoff SECURITY WARNING, got: ${result.output}`);
    assert.ok(result.output.includes('cleaned things up'), 'original output preserved');
  });

  it('does NOT add a warning for the SAME dangerous run under an interactive (ask) mode', async () => {
    const result = await taskTool.execute(
      { prompt: 'do cleanup', description: 'cleanup', mode: 'general', permission_mode: 'ask' },
      ctx(process.cwd(), { _engineFactory: () => ({ submit: dangerousRun }) }),
    );
    assert.ok(!result.output.includes('SECURITY WARNING'),
      `ask mode must not trigger the yolo handoff review, got: ${result.output}`);
  });

  it('uses the classifier-unavailable fallback when an injected classifier throws', async () => {
    const result = await taskTool.execute(
      { prompt: 'do cleanup', description: 'cleanup', mode: 'general', permission_mode: 'auto-allow' },
      ctx(process.cwd(), {
        _engineFactory: () => ({ submit: dangerousRun }),
        _handoffClassifier: () => { throw new Error('reviewer offline'); },
      }),
    );
    assert.ok(result.output.includes('safety classifier was unavailable'),
      `expected the unavailable fallback note, got: ${result.output}`);
    assert.ok(result.output.includes('cleaned things up'), 'output still propagated, never dropped');
  });
});

// =====================================================================
// Feature: team_name parameter (gaps 6,8)
// =====================================================================
describe('taskTool — team_name parameter', () => {
  let tmp: string;
  beforeEach(() => { tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-team-')); resetTeam(); resetAgentRegistry(); });
  afterEach(() => { resetTeam(); resetAgentRegistry(); fs.rmSync(tmp, { recursive: true, force: true }); });

  async function* oneLine(_p: string) {
    yield { type: 'text_delta' as const, text: 'done' };
    yield { type: 'turn_complete' as const, content: 'done' };
    yield { type: 'done' as const, exitReason: 'completed' as const };
  }

  it('advertises a team_name property in the input schema', () => {
    assert.ok('team_name' in taskTool.inputSchema.properties);
  });

  it('rejects team_name without a name', async () => {
    writeTeam(tmp, 'alpha');
    const result = await taskTool.execute(
      { prompt: 'help', description: 'd', team_name: 'alpha' },
      ctx(tmp, { _engineFactory: () => ({ submit: oneLine }) }),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('team_name requires name'), result.output);
  });

  it('rejects a team_name that does not exist', async () => {
    const result = await taskTool.execute(
      { prompt: 'help', description: 'd', name: 'worker', team_name: 'ghost' },
      ctx(tmp, { _engineFactory: () => ({ submit: oneLine }) }),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('not found'), result.output);
  });

  it('registers the spawned agent into the named on-disk team roster', async () => {
    writeTeam(tmp, 'alpha');
    const result = await taskTool.execute(
      { prompt: 'help', description: 'd', name: 'worker', team_name: 'alpha' },
      ctx(tmp, { _engineFactory: () => ({ submit: oneLine }) }),
    );
    assert.ok(!result.isError, result.output);
    const team = readTeamFile(path.join(tmp, '.ami', 'teams', 'alpha'));
    assert.ok(team, 'team file should still exist');
    const member = team!.members.find(m => m.name === 'worker');
    assert.ok(member, `worker should be registered in team alpha, got: ${JSON.stringify(team!.members)}`);
  });

  it('tags a background agent with the resolved team name on the process manager', async () => {
    writeTeam(tmp, 'alpha');
    let captured: Record<string, unknown> | undefined;
    const pm = {
      registerAgent: (_id: string, info: Record<string, unknown>) => { captured = info; },
      runningAgentCount: () => 0,
      killTasksForAgent: () => {},
      flowManager: undefined,
    };
    await taskTool.execute(
      { prompt: 'help', description: 'd', name: 'bgworker', team_name: 'alpha', run_in_background: true },
      ctx(tmp, {
        _engineFactory: () => ({ submit: oneLine }),
        processManager: pm as unknown as ToolContext['processManager'],
      }),
    );
    assert.equal(captured?.teamName, 'alpha');
  });
});

// =====================================================================
// registerTeamMemberInto helper (unit)
// =====================================================================
describe('registerTeamMemberInto', () => {
  let tmp: string;
  beforeEach(() => { tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-rti-')); resetTeam(); });
  afterEach(() => { resetTeam(); fs.rmSync(tmp, { recursive: true, force: true }); });

  it('returns false for a nonexistent team', () => {
    assert.equal(registerTeamMemberInto(tmp, 'nope', { name: 'a', agentId: 'a@nope' }), false);
  });

  it('appends a member to an on-disk team and persists it', () => {
    const dir = path.join(tmp, '.ami', 'teams', 'beta');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'team.json'), JSON.stringify({
      name: 'beta', leadAgentId: 'lead@beta', members: [], createdAt: new Date().toISOString(),
    }));
    const ok = registerTeamMemberInto(tmp, 'beta', { name: 'w1', agentId: 'w1@beta' });
    assert.equal(ok, true);
    const loaded = loadTeamFromDisk(tmp, 'beta');
    assert.ok(loaded!.team.members.some(m => m.name === 'w1'));
  });

  it('updates an existing member rather than duplicating it', () => {
    const dir = path.join(tmp, '.ami', 'teams', 'beta');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'team.json'), JSON.stringify({
      name: 'beta', leadAgentId: 'lead@beta',
      members: [{ name: 'w1', agentId: 'old', color: 'blue', joinedAt: 'x', isActive: true }],
      createdAt: new Date().toISOString(),
    }));
    registerTeamMemberInto(tmp, 'beta', { name: 'w1', agentId: 'new' });
    const loaded = loadTeamFromDisk(tmp, 'beta');
    const w1 = loaded!.team.members.filter(m => m.name === 'w1');
    assert.equal(w1.length, 1);
    assert.equal(w1[0].agentId, 'new');
    assert.equal(w1[0].color, 'blue', 'existing color preserved');
  });
});
