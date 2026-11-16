// E2E red-green reproducer for the per-agent `background: true` default (Claude
// parity): when an agent definition declares background:true and the caller does
// NOT pass run_in_background, the task tool must spawn the subagent detached
// (returning a "Background agent started: …" handle) instead of running it
// foreground. An explicit run_in_background always wins over the agent default.
//
// RED on revert: without the `agentBackgroundDefault` fallback in task.ts, the
// first test runs foreground and returns the agent's text ('ok') instead of a
// background handle, so the assertion fails.

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
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-agent-bg-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

function gen(events: any[]) {
  return async function* (_p: string) {
    for (const e of events) yield e;
  };
}

function ctx(agentDef: any): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
    _skillManager: {
      getAgent: () => agentDef,
      listAgents: () => [agentDef],
    },
    _engineFactory: () => ({ submit: gen([{ type: 'text_delta', text: 'ok' }]) }),
  } as unknown as ToolContext;
}

describe('taskTool — per-agent background:true default', () => {
  it('spawns a background:true agent detached when run_in_background is omitted', async () => {
    const res = await taskTool.execute(
      { prompt: 'do the work', subagent_type: 'bgagent' },
      ctx({ name: 'bgagent', systemPrompt: 'be a bg agent', background: true, tools: ['file_read'] }),
    );
    assert.equal(res.isError, undefined);
    assert.match(res.output, /^Background agent started:/);
  });

  it('runs foreground when the agent has no background default', async () => {
    const res = await taskTool.execute(
      { prompt: 'do the work', subagent_type: 'fgagent' },
      ctx({ name: 'fgagent', systemPrompt: 'be a fg agent', tools: ['file_read'] }),
    );
    assert.ok(!/^Background agent started:/.test(res.output), res.output);
    assert.match(res.output, /ok/);
  });

  it('explicit run_in_background:false overrides a background:true agent default', async () => {
    const res = await taskTool.execute(
      { prompt: 'do the work', subagent_type: 'bgagent2', run_in_background: false },
      ctx({ name: 'bgagent2', systemPrompt: 'be a bg agent', background: true, tools: ['file_read'] }),
    );
    assert.ok(!/^Background agent started:/.test(res.output), res.output);
    assert.match(res.output, /ok/);
  });
});
