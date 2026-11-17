import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  teamCreateTool,
  resetTeam,
  getCurrentTeam,
  getTeamScratchpadDir,
  teamScratchpadRules,
  TEAM_SCRATCHPAD_SUBDIR,
  type TeamFile,
} from '../src/tools/team';
import { taskTool, resetAgentRegistry } from '../src/tools/task';
import type { ToolContext, EngineConfig } from '../src/types';

let tmp: string;
let dataDir: string;
let prevDataDir: string | undefined;

function ctx(sessionId = 'sess-sp'): ToolContext {
  return { cwd: tmp, sessionId, abortSignal: new AbortController().signal } as ToolContext;
}

function teamFile(name: string, extra: Partial<TeamFile> = {}): TeamFile {
  return { name, leadAgentId: `team-lead@${name}`, members: [], createdAt: 'now', ...extra };
}

describe('U038 w78 — gated prompt-free team scratchpad dir (parity: coordinator scratchpadDir)', () => {
  beforeEach(() => {
    resetTeam();
    resetAgentRegistry();
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'team-scratch-'));
    dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'team-scratch-data-'));
    prevDataDir = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = dataDir;
  });
  afterEach(() => {
    resetTeam();
    resetAgentRegistry();
    if (prevDataDir === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
    else process.env.SUPERINFERENCE_DATA_DIR = prevDataDir;
    for (const d of [tmp, dataDir]) {
      try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
    }
  });

  // --- getTeamScratchpadDir: deterministic path resolution ---
  it('getTeamScratchpadDir returns null when there is no team', () => {
    assert.equal(getTeamScratchpadDir(tmp, null), null);
  });

  it('getTeamScratchpadDir resolves the memory subdir deterministically from cwd + name', () => {
    const dir = getTeamScratchpadDir(tmp, teamFile('crew'));
    assert.equal(dir, path.join(tmp, '.ami', 'teams', 'crew', TEAM_SCRATCHPAD_SUBDIR));
    assert.equal(TEAM_SCRATCHPAD_SUBDIR, 'memory');
  });

  it('getTeamScratchpadDir defaults to the active team', async () => {
    const c = await teamCreateTool.execute({ team_name: 'squad' }, ctx());
    assert.equal(c.isError, undefined, c.output);
    assert.equal(getTeamScratchpadDir(tmp), path.join(tmp, '.ami', 'teams', 'squad', 'memory'));
  });

  // --- teamScratchpadRules: prompt-free grant scoped to the scratchpad dir ---
  it('teamScratchpadRules returns [] when there is no team', () => {
    assert.deepEqual(teamScratchpadRules(tmp, null), []);
  });

  it('teamScratchpadRules grants every tool prompt-free access scoped to <scratchpad>/**', () => {
    const rules = teamScratchpadRules(tmp, teamFile('crew'));
    const expectedDir = path.join(tmp, '.ami', 'teams', 'crew', 'memory');
    assert.deepEqual(rules, [{ tool: '*', pattern: `${expectedDir}/**`, action: 'allow' }]);
    // The grant must be scoped to the team's own dir only — never a broader path.
    assert.ok(rules[0].pattern.startsWith(expectedDir), 'rule is scoped to the scratchpad dir');
  });

  // --- team_create provisions + advertises the scratchpad ---
  it('team_create provisions the scratchpad dir and advertises it in the result', async () => {
    const r = await teamCreateTool.execute({ team_name: 'alpha' }, ctx());
    assert.equal(r.isError, undefined, r.output);
    const dir = path.join(tmp, '.ami', 'teams', 'alpha', 'memory');
    assert.ok(fs.existsSync(dir), 'memory/ scratchpad dir created');
    assert.ok(r.output.includes(dir), `output advertises the scratchpad path: ${r.output}`);
    assert.ok(/scratchpad/i.test(r.output), 'output names it a scratchpad');
  });

  // --- E2E: a spawned teammate inherits the prompt-free scratchpad rule + advertisement ---
  it('a spawned teammate inherits the scratchpad allow-rule on its engine and is told about it', async () => {
    const c = await teamCreateTool.execute({ team_name: 'crew' }, ctx());
    assert.equal(c.isError, undefined, c.output);

    let captured: EngineConfig | null = null;
    let capturedPrompt = '';
    async function* fakeSubmit() { yield { type: 'text_delta', text: 'ok' } as never; }
    const r = await taskTool.execute(
      { prompt: 'do work', name: 'worker1' },
      {
        cwd: tmp,
        sessionId: 'sess-sp',
        abortSignal: new AbortController().signal,
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
        _engineFactory: (cfg) => {
          captured = cfg as EngineConfig;
          return { submit: (p: string) => { capturedPrompt = p; return fakeSubmit(); } };
        },
      } as unknown as ToolContext,
    );
    assert.ok(!r.isError, `teammate spawn failed: ${r.output}`);
    assert.ok(captured, 'engine factory was invoked');
    const rules = (captured as EngineConfig).permissionRules ?? [];
    const scratchDir = path.join(tmp, '.ami', 'teams', 'crew', 'memory');
    assert.ok(
      rules.some(x => x.tool === '*' && x.pattern === `${scratchDir}/**` && x.action === 'allow'),
      `teammate inherits the scratchpad allow-rule: ${JSON.stringify(rules)}`,
    );
    // The teammate is told where the shared scratchpad is (advertised in its prompt).
    assert.ok(capturedPrompt.includes(scratchDir), 'teammate prompt advertises the scratchpad dir');
    assert.ok(/scratchpad/i.test(capturedPrompt), 'teammate prompt names it a scratchpad');
  });

  it('does NOT grant a scratchpad rule for a solo (no-team) spawn', async () => {
    let captured: EngineConfig | null = null;
    async function* fakeSubmit() { yield { type: 'text_delta', text: 'ok' } as never; }
    const r = await taskTool.execute(
      { prompt: 'solo', name: 'solo1' },
      {
        cwd: tmp,
        sessionId: 'sess-sp',
        abortSignal: new AbortController().signal,
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
        _engineFactory: (cfg) => { captured = cfg as EngineConfig; return { submit: fakeSubmit }; },
      } as unknown as ToolContext,
    );
    assert.ok(!r.isError, r.output);
    const rules = (captured as unknown as EngineConfig).permissionRules ?? [];
    assert.ok(!rules.some(x => x.tool === '*' && /\/memory\/\*\*$/.test(x.pattern ?? '')), 'no scratchpad rule without a team');
    assert.equal(getCurrentTeam(), null);
  });
});
