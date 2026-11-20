import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';

import { taskTool, resetAgentRegistry } from '../src/tools/task';
import type { ToolContext, EngineEvent } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

function gen(events: unknown[]) {
  return async function* (_p: string) {
    for (const e of events) yield e;
  };
}

// U006 — subagent_start carries the spawned KIND as `taskType`
// (parity: the reference's task_started.task_type). The task tool must relay the
// resolved kind on the child event feed so a roster can classify the agent.
describe('taskTool — subagent_start taskType (the reference task_type parity)', () => {
  beforeEach(() => resetAgentRegistry());

  it('reports the named subagent_type as taskType', async () => {
    const emitted: EngineEvent[] = [];
    await taskTool.execute(
      { prompt: 'do it', subagent_type: 'code-reviewer', mode: 'general' },
      ctx({
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _emitChild: (e: EngineEvent) => emitted.push(e),
        _engineFactory: () => ({ submit: gen([{ type: 'text_delta', text: 'ok' }]) }),
      }),
    );
    const start = emitted.find(e => e.type === 'subagent_start');
    assert.ok(start, 'a subagent_start must be emitted');
    assert.equal((start as { taskType?: string }).taskType, 'code-reviewer');
  });

  it('falls back to the mode when no subagent_type is given', async () => {
    const emitted: EngineEvent[] = [];
    await taskTool.execute(
      { prompt: 'look around', mode: 'explore' },
      ctx({
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _emitChild: (e: EngineEvent) => emitted.push(e),
        _engineFactory: () => ({ submit: gen([{ type: 'text_delta', text: 'ok' }]) }),
      }),
    );
    const start = emitted.find(e => e.type === 'subagent_start');
    assert.ok(start, 'a subagent_start must be emitted');
    assert.equal((start as { taskType?: string }).taskType, 'explore');
  });
});
