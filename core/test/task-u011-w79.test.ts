import { describe, it, beforeEach, before, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  taskTool,
  resetAgentRegistry,
  // w79 additions under test
  FORK_SUBAGENT_ENV,
  isForkSubagentEnabled,
  resolveSpawnMode,
  TEAMMATE_ESSENTIAL_TOOLS,
  ensureTeammateCoordinationTools,
  ALL_AGENT_DISALLOWED_TOOLS,
} from '../src/tools/task';
import { teamCreateTool, resetTeam } from '../src/tools/team';
import type { AgentDefinition } from '../src/skills';
import type { ToolContext, ToolDefinition, EngineConfig } from '../src/types';

const fakeTools = (names: string[]): ToolDefinition[] =>
  names.map(name => ({ name } as unknown as ToolDefinition));

// Isolate on-disk project state into a throwaway data dir.
let dataDir: string;
before(() => {
  dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u011-w79-'));
  process.env.SUPERINFERENCE_DATA_DIR = dataDir;
});
after(() => {
  delete process.env.SUPERINFERENCE_DATA_DIR;
  delete process.env[FORK_SUBAGENT_ENV];
  try { fs.rmSync(dataDir, { recursive: true, force: true }); } catch { /* best-effort */ }
});

type Ev = Record<string, unknown>;

/** Run task.execute with a fake engine, capturing the resolved EngineConfig. */
async function runCapture(
  input: Record<string, unknown>,
  ctx: Partial<ToolContext> = {},
  events: Ev[] = [{ type: 'text_delta', text: 'ok' }],
): Promise<{ result: { isError?: boolean; output: string }; captured: EngineConfig; toolNames: string[] }> {
  let captured!: EngineConfig;
  let toolNames: string[] = [];
  async function* fakeSubmit() { for (const e of events) yield e as never; }
  const result = await taskTool.execute(input, {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
    _engineFactory: (c: EngineConfig) => {
      captured = c;
      toolNames = (c.tools as ToolDefinition[]).map(t => t.name);
      return { submit: fakeSubmit, shutdown() {} } as never;
    },
    ...ctx,
  } as unknown as ToolContext);
  return { result, captured, toolNames };
}

function skillManagerFor(agent: AgentDefinition): Partial<ToolContext>['_skillManager'] {
  return {
    getAgent: (n: string) => (n === agent.name ? agent : null),
    listAgents: () => [agent],
    getSkill: () => null,
  } as unknown as ToolContext['_skillManager'];
}

function agentDef(over: Partial<AgentDefinition>): AgentDefinition {
  return {
    name: 'a', description: 'd', systemPrompt: 'you are a', filePath: '/x', ...over,
  } as AgentDefinition;
}

// ===========================================================================
// gap: Permission-mode de-escalation at the spawn boundary + propagate clamp —
// a per-call permission_mode can NEVER escalate a subagent above the parent.
// ===========================================================================
describe('task.execute — a per-call permission_mode is clamped to the parent (de-escalation)', () => {
  beforeEach(() => resetAgentRegistry());

  it('clamps an escalating permission_mode (auto-allow under an ask parent → ask)', async () => {
    const { captured } = await runCapture(
      { prompt: 'go', permission_mode: 'auto-allow' },
      { _parentPermissionMode: 'ask' },
    );
    assert.equal(captured.permissionMode, 'ask', 'model-chosen auto-allow clamped to parent ask');
  });

  it('clamps bypassPermissions under a deny-all parent → deny-all', async () => {
    const { captured } = await runCapture(
      { prompt: 'go', permission_mode: 'bypassPermissions' },
      { _parentPermissionMode: 'deny-all' },
    );
    assert.equal(captured.permissionMode, 'deny-all');
  });

  it('does NOT tighten a de-escalating request (deny-all under an ask parent stays deny-all)', async () => {
    const { captured } = await runCapture(
      { prompt: 'go', permission_mode: 'deny-all' },
      { _parentPermissionMode: 'ask' },
    );
    assert.equal(captured.permissionMode, 'deny-all');
  });

  it('is a no-op when the parent mode is not threaded (auto-allow passes through)', async () => {
    const { captured } = await runCapture({ prompt: 'go', permission_mode: 'auto-allow' });
    assert.equal(captured.permissionMode, 'auto-allow');
  });
});

// ===========================================================================
// gap: Explicit suppression of interactive permission prompts for a
// non-interactive (background) subagent that has no handler to answer them.
// ===========================================================================
describe('task.execute — a background subagent with no prompt handler auto-denies unlisted tools', () => {
  beforeEach(() => resetAgentRegistry());

  it('a background spawn with no permission handler resolves to deny-all', async () => {
    let captured!: EngineConfig;
    async function* submit() { yield { type: 'text_delta', text: '' } as never; }
    await taskTool.execute(
      { prompt: 'bg', description: 'bg', run_in_background: true },
      {
        cwd: process.cwd(),
        abortSignal: new AbortController().signal,
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _engineFactory: (c: EngineConfig) => { captured = c; return { submit, shutdown() {} } as never; },
      } as unknown as ToolContext,
    );
    await new Promise(r => setTimeout(r, 20));
    assert.equal(captured.permissionMode, 'deny-all', 'detached background auto-denies — no prompt to answer');
  });

  it('keeps the ask default when a permission handler IS present (no YOLO regression)', async () => {
    let captured!: EngineConfig;
    async function* submit() { yield { type: 'text_delta', text: '' } as never; }
    await taskTool.execute(
      { prompt: 'bg', description: 'bg', run_in_background: true },
      {
        cwd: process.cwd(),
        abortSignal: new AbortController().signal,
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _permissionPromptHandler: (async () => ({ behavior: 'allow' })) as unknown as ToolContext['_permissionPromptHandler'],
        _engineFactory: (c: EngineConfig) => { captured = c; return { submit, shutdown() {} } as never; },
      } as unknown as ToolContext,
    );
    await new Promise(r => setTimeout(r, 20));
    assert.equal(captured.permissionMode, 'ask', 'handler present → ask default kept so it can decide');
  });

  it('a FOREGROUND spawn still defaults to ask (unchanged)', async () => {
    const { captured } = await runCapture({ prompt: 'go' });
    assert.equal(captured.permissionMode, 'ask');
  });
});

// ===========================================================================
// gap: isForkSubagentEnabled feature gate + subagent_type optional → implicit fork
// ===========================================================================
describe('isForkSubagentEnabled / resolveSpawnMode — gated implicit fork', () => {
  it('isForkSubagentEnabled reads the env knob (default off)', () => {
    assert.equal(isForkSubagentEnabled({}), false);
    assert.equal(isForkSubagentEnabled({ [FORK_SUBAGENT_ENV]: '1' }), true);
    assert.equal(isForkSubagentEnabled({ [FORK_SUBAGENT_ENV]: 'false' }), false);
  });

  it('an explicit mode always wins over the implicit default', () => {
    assert.equal(resolveSpawnMode('explore', undefined, { [FORK_SUBAGENT_ENV]: '1' }), 'explore');
    assert.equal(resolveSpawnMode('  general  ', undefined, { [FORK_SUBAGENT_ENV]: '1' }), 'general');
  });

  it('omitted mode + gate ON + no subagent_type → implicit fork', () => {
    assert.equal(resolveSpawnMode(undefined, undefined, { [FORK_SUBAGENT_ENV]: '1' }), 'fork');
    assert.equal(resolveSpawnMode('', undefined, { [FORK_SUBAGENT_ENV]: '1' }), 'fork');
  });

  it('omitted mode + gate ON + a subagent_type present → stays general (identity honored)', () => {
    assert.equal(resolveSpawnMode(undefined, 'reviewer', { [FORK_SUBAGENT_ENV]: '1' }), 'general');
  });

  it('omitted mode + gate OFF → stays general (default, additive)', () => {
    assert.equal(resolveSpawnMode(undefined, undefined, {}), 'general');
  });

  it('end-to-end: with the gate ON an omitted mode spawns a fork (no base system prompt)', async () => {
    process.env[FORK_SUBAGENT_ENV] = '1';
    try {
      const { captured } = await runCapture({ prompt: 'continue the work' });
      // A fork inherits the parent stance — the child engine gets NO string base
      // systemPrompt to replace (unlike a general mode-only subagent, which gets
      // the lean SUBAGENT_SYSTEM_PROMPT).
      assert.equal(captured.systemPrompt, undefined, 'implicit fork inherits parent stance (no base prompt)');
    } finally {
      delete process.env[FORK_SUBAGENT_ENV];
    }
  });

  it('end-to-end: with the gate OFF an omitted mode spawns a general subagent (lean base prompt)', async () => {
    const { captured } = await runCapture({ prompt: 'do one thing' });
    assert.ok(typeof captured.systemPrompt === 'string' && captured.systemPrompt.length > 0,
      'general mode-only subagent gets a lean base system prompt');
  });
});

// ===========================================================================
// gap: Force-inject team-essential coordination tools even when an agent has an
// explicit tool allowlist (so a restricted teammate stays reachable).
// ===========================================================================
describe('ensureTeammateCoordinationTools — unions coordination tools into a restricted set', () => {
  it('adds the missing essential tools from the registry', () => {
    const base = fakeTools(['file_read']);
    const all = fakeTools(['file_read', 'send_message', 'task_list', 'task_tracker', 'bash']);
    const out = ensureTeammateCoordinationTools(base, all).map(t => t.name).sort();
    assert.deepEqual(out, ['file_read', 'send_message', 'task_list', 'task_tracker']);
  });

  it('does not duplicate tools already present', () => {
    const base = fakeTools(['file_read', 'send_message']);
    const all = fakeTools(['file_read', 'send_message', 'task_list', 'task_tracker']);
    const out = ensureTeammateCoordinationTools(base, all).filter(t => t.name === 'send_message');
    assert.equal(out.length, 1);
  });

  it('never reintroduces a baseline-denied tool', () => {
    // Prove the floor respects the universal denylist even if a coordination name
    // were ever denied: add a denied name to a throwaway copy and confirm it is skipped.
    const base = fakeTools(['file_read']);
    const deniedName = [...ALL_AGENT_DISALLOWED_TOOLS][0];
    const all = fakeTools(['file_read', 'send_message', deniedName]);
    const out = ensureTeammateCoordinationTools(base, all).map(t => t.name);
    assert.ok(!out.includes(deniedName), 'denied tool not injected');
    assert.ok(out.includes('send_message'));
  });

  it('returns the SAME array when there is nothing to add', () => {
    const base = fakeTools([...TEAMMATE_ESSENTIAL_TOOLS, 'file_read']);
    const all = fakeTools([...TEAMMATE_ESSENTIAL_TOOLS, 'file_read']);
    assert.equal(ensureTeammateCoordinationTools(base, all), base);
  });
});

describe('task.execute — a named teammate with a restricted allowlist keeps coordination tools', () => {
  beforeEach(() => { resetAgentRegistry(); resetTeam(); });
  after(() => resetTeam());

  it('re-injects send_message/task_list/task_tracker for a restricted teammate in a team', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w79-team-'));
    try {
      const teamCtx = {
        cwd: tmp,
        abortSignal: new AbortController().signal,
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
      } as unknown as ToolContext;
      await teamCreateTool.execute({ team_name: 'sq' }, teamCtx);

      const def = agentDef({ name: 'scoped', tools: ['file_read'] });
      const { toolNames } = await runCapture(
        { prompt: 'help', subagent_type: 'scoped', name: 'worker' },
        { cwd: tmp, _skillManager: skillManagerFor(def) },
      );
      assert.ok(toolNames.includes('file_read'), 'declared tool kept');
      assert.ok(toolNames.includes('send_message'), 'coordination tool re-injected');
      assert.ok(toolNames.includes('task_list'), 'task coordination re-injected');
      assert.ok(toolNames.includes('task_tracker'), 'task tracker re-injected');
    } finally {
      resetTeam();
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('does NOT inject coordination tools for a restricted agent spawned WITHOUT a team/name', async () => {
    const def = agentDef({ name: 'scoped2', tools: ['file_read'] });
    const { toolNames } = await runCapture(
      { prompt: 'help', subagent_type: 'scoped2' },
      { _skillManager: skillManagerFor(def) },
    );
    assert.deepEqual(toolNames, ['file_read'], 'no team membership → no coordination floor');
  });
});
