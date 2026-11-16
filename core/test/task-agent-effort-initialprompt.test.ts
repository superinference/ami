// E2E red-green reproducer for the per-agent `effort` and `initialPrompt` defaults
// (Claude parity), driven through the real taskTool.execute path:
//
//  - `effort`: when an agent definition declares an effort level and the caller does
//    NOT pass a per-call `effort`, the spawned sub-engine config must enable thinking
//    at the agent's level. An explicit per-call effort override always wins.
//    RED on revert: without the `effortOverride ?? agentEffort` fallback in task.ts,
//    thinking stays {enabled:false, level:'off'} and the level assertion fails.
//
//  - `initialPrompt`: the agent's standing instruction must be prepended to the first
//    user turn submitted to the sub-engine; a per-call `resume` suppresses it.
//    RED on revert: without the prepend in task.ts, submit() receives the bare prompt.

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { taskTool, resetAgentRegistry } from '../src/tools/task';
import type { ToolContext } from '../src/types';

let tmpDir: string;

beforeEach(() => {
  resetAgentRegistry();
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-agent-effort-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

function gen(submittedPrompts: string[]) {
  return async function* (p: string) {
    submittedPrompts.push(p);
    yield { type: 'text_delta', text: 'ok' } as any;
  };
}

function ctx(agentDef: any, captured: { config?: any; prompts: string[] }): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
    _skillManager: {
      getAgent: () => agentDef,
      listAgents: () => [agentDef],
    },
    _engineFactory: (config: any) => {
      captured.config = config;
      return { submit: gen(captured.prompts) };
    },
  } as unknown as ToolContext;
}

describe('taskTool — per-agent effort default', () => {
  it('applies the agent effort level to the sub-engine thinking config', async () => {
    const cap: { config?: any; prompts: string[] } = { prompts: [] };
    const res = await taskTool.execute(
      { prompt: 'do the work', subagent_type: 'deepagent' },
      ctx({ name: 'deepagent', systemPrompt: 'be a deep agent', effort: 'high', tools: ['file_read'] }, cap),
    );
    assert.equal(res.isError, undefined);
    assert.deepEqual(cap.config.thinking, { enabled: true, level: 'high' });
  });

  it('lets an explicit per-call effort override the agent default', async () => {
    const cap: { config?: any; prompts: string[] } = { prompts: [] };
    await taskTool.execute(
      { prompt: 'do the work', subagent_type: 'deepagent', effort: 'low' },
      ctx({ name: 'deepagent', systemPrompt: 'be a deep agent', effort: 'high', tools: ['file_read'] }, cap),
    );
    assert.deepEqual(cap.config.thinking, { enabled: true, level: 'low' });
  });

  it('keeps thinking disabled when neither a call nor the agent sets effort', async () => {
    const cap: { config?: any; prompts: string[] } = { prompts: [] };
    await taskTool.execute(
      { prompt: 'do the work', subagent_type: 'plainagent' },
      ctx({ name: 'plainagent', systemPrompt: 'be a plain agent', tools: ['file_read'] }, cap),
    );
    assert.equal(cap.config.thinking.enabled, false);
    assert.equal(cap.config.thinking.level, 'off');
  });
});

describe('taskTool — per-agent initialPrompt', () => {
  it('prepends the agent initialPrompt to the first submitted user turn', async () => {
    const cap: { config?: any; prompts: string[] } = { prompts: [] };
    await taskTool.execute(
      { prompt: 'fix the bug', subagent_type: 'seeded' },
      ctx({ name: 'seeded', systemPrompt: 'be seeded', initialPrompt: 'Always run the tests.', tools: ['file_read'] }, cap),
    );
    assert.equal(cap.prompts.length, 1);
    assert.ok(cap.prompts[0].startsWith('Always run the tests.'), cap.prompts[0]);
    assert.ok(cap.prompts[0].includes('fix the bug'), cap.prompts[0]);
  });

  it('does not prepend initialPrompt when there is none', async () => {
    const cap: { config?: any; prompts: string[] } = { prompts: [] };
    await taskTool.execute(
      { prompt: 'fix the bug', subagent_type: 'bare' },
      ctx({ name: 'bare', systemPrompt: 'be bare', tools: ['file_read'] }, cap),
    );
    assert.ok(!cap.prompts[0].includes('Always run the tests.'), cap.prompts[0]);
    assert.ok(cap.prompts[0].includes('fix the bug'), cap.prompts[0]);
  });
});
