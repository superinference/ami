import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { taskOutputTool } from '../src/tools/task-output';
import type { ToolContext } from '../src/types';

/**
 * U212 parity (functional change): task_output BLOCKS by default.
 *
 * Mirrors Claude Code's TaskOutputTool `block: semanticBoolean(z.boolean().default(true))`
 * + `timeout: z.number().default(30000)` — a call with no explicit `wait` blocks until
 * the task reaches a terminal state (or the 30s timeout elapses), returning the final
 * result in a single call. Passing `wait:false` takes a one-shot non-blocking snapshot.
 *
 * RED-GREEN DISCRIMINATOR: revert the block-by-default change
 *   (`const wait = input.wait === undefined || input.wait === null ? true : coerceBool(input.wait);`
 *    → `const wait = coerceBool(input.wait);`)
 * and the first test fails — with the old default, a no-`wait` call returns the FIRST
 * (running) snapshot (`calls === 1`, status "running") instead of blocking for the
 * terminal result (`calls >= 2`, status "completed").
 */

function ctx(getOutput: () => unknown, extra?: Partial<ToolContext>): ToolContext {
  return {
    cwd: '/tmp',
    abortSignal: new AbortController().signal,
    processManager: { getOutput },
    ...extra,
  } as unknown as ToolContext;
}

describe('task_output block-by-default (parity: Claude TaskOutputTool block:true)', () => {
  it('BLOCKS until terminal when no `wait` is given (default true), not a one-shot read', async () => {
    let calls = 0;
    const c = ctx(() => {
      calls++;
      return calls < 2
        ? { status: 'running', exitCode: null, elapsedMs: 1000, output: 'still going' }
        : { status: 'completed', exitCode: 0, elapsedMs: 1200, output: 'final answer' };
    });
    // No `wait` key at all — must default to blocking.
    const res = await taskOutputTool.execute!({ task_id: 'bg-1' }, c);
    assert.ok(!res.isError);
    assert.ok(res.output.includes('Status: completed'), `expected a blocked terminal result, got:\n${res.output}`);
    assert.ok(res.output.includes('Exit code: 0'));
    assert.ok(calls >= 2, 'polled internally (blocked) rather than returning the first running snapshot');
  });

  it('wait:false takes a one-shot non-blocking snapshot of a still-running task (no poll loop)', async () => {
    let calls = 0;
    const c = ctx(() => {
      calls++;
      return { status: 'running', exitCode: null, elapsedMs: 500, output: 'in progress' };
    });
    const res = await taskOutputTool.execute!({ task_id: 'bg-2', wait: false }, c);
    assert.ok(res.output.includes('Status: running'), 'returns the running snapshot immediately');
    assert.equal(calls, 1, 'did NOT poll — a single read');
  });

  it('wait:"false" (provider-serialized string) also opts out of blocking', async () => {
    let calls = 0;
    const c = ctx(() => {
      calls++;
      return { status: 'running', exitCode: null, elapsedMs: 10, output: 'x' };
    });
    await taskOutputTool.execute!({ task_id: 'bg-3', wait: 'false' as unknown as boolean }, c);
    assert.equal(calls, 1, 'the string "false" coerces to a non-blocking read');
  });

  it('default block gives up at the 30s-default timeout (clamped) for a perpetually-running task', async () => {
    let calls = 0;
    const c = ctx(() => {
      calls++;
      return { status: 'running', exitCode: null, elapsedMs: 10, output: 'loops forever' };
    });
    // A tiny explicit timeout keeps the test fast while still exercising the
    // block-by-default path: no `wait`, so it blocks, then times out quickly.
    const res = await taskOutputTool.execute!({ task_id: 'bg-4', timeout_ms: 20 }, c);
    assert.ok(res.output.includes('wait timed out'), `expected a block-then-timeout, got:\n${res.output}`);
    assert.ok(res.output.includes('Status: running'), 'still-running task reported after the timeout');
    assert.ok(calls >= 2, 'polled at least once more than the initial read before timing out');
  });
});
