import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  teamCreateTool,
  resetTeam,
  getCurrentTeam,
  teamAllowedPathsToRules,
} from '../src/tools/team';
import { taskTool, resetAgentRegistry } from '../src/tools/task';
import type { ToolContext, EngineConfig } from '../src/types';

let tmp: string;
let dataDir: string;
let prevDataDir: string | undefined;

function ctx(): ToolContext {
  return { cwd: tmp, sessionId: 'sess-tap', abortSignal: new AbortController().signal } as ToolContext;
}

describe('U038 teamAllowedPaths — declarative team-wide path permissions', () => {
  beforeEach(() => {
    resetTeam();
    resetAgentRegistry();
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'team-tap-'));
    dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'team-tap-data-'));
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

  // --- teamAllowedPathsToRules: path → permission-rule conversion ---
  it('returns [] when there is no team / no allowed paths', () => {
    assert.deepEqual(teamAllowedPathsToRules(null), []);
    assert.deepEqual(teamAllowedPathsToRules({
      name: 't', leadAgentId: 'team-lead@t', members: [], createdAt: 'now',
    }), []);
  });

  it('maps a RELATIVE path to `<path>/**` and an ABSOLUTE path to `/<path>/**` (double-slash)', () => {
    const rules = teamAllowedPathsToRules({
      name: 't', leadAgentId: 'team-lead@t', createdAt: 'now', members: [],
      teamAllowedPaths: [
        { path: 'src', toolName: 'file_edit' },
        { path: '/abs/dir', toolName: 'bash' },
      ],
    });
    assert.deepEqual(rules, [
      { tool: 'file_edit', pattern: 'src/**', action: 'allow' },
      // absolute → characteristic double-slash: `/` + `/abs/dir` + `/**`
      { tool: 'bash', pattern: '//abs/dir/**', action: 'allow' },
    ]);
  });

  // --- team_create parses team_allowed_paths (string + object union) ---
  it('parses bare-string entries (toolName defaults to *) and { path, tool_name } objects', async () => {
    const r = await teamCreateTool.execute({
      team_name: 'scoped',
      team_allowed_paths: ['shared', { path: 'src', tool_name: 'file_edit' }],
    }, ctx());
    assert.equal(r.isError, undefined, r.output);
    const team = getCurrentTeam()!;
    assert.deepEqual(team.teamAllowedPaths, [
      { path: 'shared', toolName: '*' },
      { path: 'src', toolName: 'file_edit' },
    ]);
    // Persisted to disk in the normalized shape.
    const file = JSON.parse(fs.readFileSync(path.join(tmp, '.ami', 'teams', 'scoped', 'team.json'), 'utf-8'));
    assert.deepEqual(file.teamAllowedPaths, team.teamAllowedPaths);
  });

  it('rejects a non-array team_allowed_paths without creating a team', async () => {
    const r = await teamCreateTool.execute({ team_name: 'bad', team_allowed_paths: 'src' }, ctx());
    assert.ok(r.isError && r.output.includes('team_allowed_paths must be an array'));
    assert.equal(getCurrentTeam(), null);
    assert.ok(!fs.existsSync(path.join(tmp, '.ami', 'teams', 'bad')));
  });

  it('rejects an entry with no usable path without creating a team', async () => {
    const r = await teamCreateTool.execute({
      team_name: 'bad2',
      team_allowed_paths: [{ tool_name: 'bash' }],
    }, ctx());
    assert.ok(r.isError && r.output.includes('non-empty path'));
    assert.equal(getCurrentTeam(), null);
  });

  // --- E2E: a spawned teammate inherits the team-wide rules on its engine ---
  it('appends the team allow-rules to a spawned teammate\'s engine permissionRules', async () => {
    const c = await teamCreateTool.execute({
      team_name: 'crew',
      team_allowed_paths: [{ path: 'src', tool_name: 'file_edit' }],
    }, ctx());
    assert.equal(c.isError, undefined, c.output);

    let captured: EngineConfig | null = null;
    async function* fakeSubmit() { yield { type: 'text_delta', text: 'ok' } as never; }
    const parentRule = { tool: 'grep', pattern: '*', action: 'allow' as const };
    const r = await taskTool.execute(
      { prompt: 'do work', name: 'worker1' },
      {
        cwd: tmp,
        sessionId: 'sess-tap',
        abortSignal: new AbortController().signal,
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
        _parentPermissionRules: [parentRule],
        _engineFactory: (cfg) => { captured = cfg as EngineConfig; return { submit: fakeSubmit }; },
      } as unknown as ToolContext,
    );
    assert.ok(!r.isError, `teammate spawn failed: ${r.output}`);
    assert.ok(captured, 'engine factory was invoked');
    const rules = (captured as EngineConfig).permissionRules ?? [];
    // Parent rule preserved AND the team rule appended.
    assert.ok(rules.some(x => x.tool === 'grep'), 'parent rule preserved');
    assert.ok(
      rules.some(x => x.tool === 'file_edit' && x.pattern === 'src/**' && x.action === 'allow'),
      `team allow-rule inherited: ${JSON.stringify(rules)}`,
    );
  });

  it('does NOT append team rules when no team is active (non-teammate spawn)', async () => {
    let captured: EngineConfig | null = null;
    async function* fakeSubmit() { yield { type: 'text_delta', text: 'ok' } as never; }
    const r = await taskTool.execute(
      { prompt: 'solo work', name: 'solo1' },
      {
        cwd: tmp,
        sessionId: 'sess-tap',
        abortSignal: new AbortController().signal,
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
        _engineFactory: (cfg) => { captured = cfg as EngineConfig; return { submit: fakeSubmit }; },
      } as unknown as ToolContext,
    );
    assert.ok(!r.isError, r.output);
    const rules = (captured as unknown as EngineConfig).permissionRules ?? [];
    assert.ok(!rules.some(x => x.tool === 'file_edit' && x.pattern === 'src/**'), 'no team rule without an active team');
  });
});
