import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { taskOutputTool } from '../src/tools/task-output';
import type { ToolContext } from '../src/types';

/**
 * U031 wiring: the task_output tool must read the task log with a BYTE CEILING
 * (bounded memory/IO — Claude Code's tailFile) and surface the process manager's
 * extrapolated total-line estimate so the model knows roughly how much output
 * exists above the shown tail.
 */

function ctx(overrides: Record<string, unknown>): ToolContext {
  return {
    cwd: '/tmp',
    abortSignal: new AbortController().signal,
    processManager: overrides,
  } as unknown as ToolContext;
}

describe('task_output U031: byte-bounded read wiring', () => {
  it('passes a positive byte ceiling as the 4th getOutput arg (bounded read)', async () => {
    let seenMaxBytes: unknown = 'unset';
    const c = ctx({
      getOutput: (_id: string, _tail: number, _filter: unknown, maxBytes?: number) => {
        seenMaxBytes = maxBytes;
        return { status: 'completed', exitCode: 0, elapsedMs: 10, output: 'done' };
      },
    });
    const res = await taskOutputTool.execute!({ task_id: 't' }, c);
    assert.ok(!res.isError);
    assert.equal(typeof seenMaxBytes, 'number', 'a byte ceiling was supplied to getOutput');
    assert.ok((seenMaxBytes as number) > 0, 'the byte ceiling is positive');
  });

  it('surfaces the extrapolated total-line estimate in the truncation indicator', async () => {
    const body = 'l98\nl99\nl100';
    const c = ctx({
      getOutput: () => ({
        status: 'running',
        exitCode: null,
        elapsedMs: 100,
        output: body,
        bytesTotal: 40_000,
        bytesOmitted: 39_000,
        totalLinesEstimate: 1234,
      }),
      getOutputSize: () => 40_000,
      get: () => ({ outputPath: '/var/tasks/t.output' }),
    });
    const res = await taskOutputTool.execute!({ task_id: 't', wait: false }, c);
    assert.ok(res.output.includes('~1234 lines total'), 'reports the extrapolated total line count');
    assert.ok(res.output.includes('earlier output omitted'), 'still quantifies the omission');
  });

  it('omits the line-estimate clause when no estimate is reported', async () => {
    const body = 'l1\nl2\nl3';
    const c = ctx({
      getOutput: () => ({ status: 'running', exitCode: null, elapsedMs: 5, output: body }),
      getOutputSize: () => 40_000,
      get: () => ({ outputPath: '/var/tasks/t.output' }),
    });
    const res = await taskOutputTool.execute!({ task_id: 't', wait: false }, c);
    assert.ok(res.output.includes('earlier output omitted'), 'base indicator still present');
    assert.ok(!res.output.includes('lines total'), 'no line-estimate clause without an estimate');
  });
});
