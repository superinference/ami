import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { taskOutputTool } from '../src/tools/task-output';
import type { ToolContext } from '../src/types';

describe('taskOutputTool', () => {
  it('has correct name and schema', () => {
    assert.equal(taskOutputTool.name, 'task_output');
    assert.ok(taskOutputTool.isReadOnly);
    assert.deepEqual(taskOutputTool.inputSchema.required, ['task_id']);
  });

  it('returns error when processManager is not available', async () => {
    const ctx = { cwd: '/tmp' } as ToolContext;
    const result = await taskOutputTool.execute!({ task_id: 'abc' }, ctx);
    assert.ok(result.isError);
    assert.ok(result.output.includes('not available'));
  });

  it('returns error when task not found', async () => {
    const ctx = {
      cwd: '/tmp',
      processManager: {
        getOutput: () => undefined,
      },
    } as unknown as ToolContext;
    const result = await taskOutputTool.execute!({ task_id: 'missing' }, ctx);
    assert.ok(result.isError);
    assert.ok(result.output.includes('No background task'));
  });

  it('displays running task with output', async () => {
    const ctx = {
      cwd: '/tmp',
      processManager: {
        getOutput: () => ({
          status: 'running',
          exitCode: null,
          elapsedMs: 5000,
          output: 'line 1\nline 2\n',
        }),
      },
    } as unknown as ToolContext;
    const result = await taskOutputTool.execute!({ task_id: 'run-1' }, ctx);
    assert.ok(!result.isError);
    assert.ok(result.output.includes('running'));
    assert.ok(result.output.includes('5.0s'));
    assert.ok(result.output.includes('line 1'));
  });

  it('displays completed task with exit code', async () => {
    const ctx = {
      cwd: '/tmp',
      processManager: {
        getOutput: () => ({
          status: 'completed',
          exitCode: 0,
          elapsedMs: 120000,
          output: 'done\n',
        }),
      },
    } as unknown as ToolContext;
    const result = await taskOutputTool.execute!({ task_id: 'done-1' }, ctx);
    assert.ok(!result.isError);
    assert.ok(result.output.includes('completed'));
    assert.ok(result.output.includes('Exit code: 0'));
    assert.ok(result.output.includes('2.0m'));
  });

  it('displays milliseconds for short tasks', async () => {
    const ctx = {
      cwd: '/tmp',
      processManager: {
        getOutput: () => ({
          status: 'completed',
          exitCode: 0,
          elapsedMs: 500,
          output: '',
        }),
      },
    } as unknown as ToolContext;
    const result = await taskOutputTool.execute!({ task_id: 'fast' }, ctx);
    assert.ok(result.output.includes('500ms'));
    assert.ok(result.output.includes('no output yet'));
  });

  it('wait:true blocks until the task reaches a terminal status (one call, no poll loop)', async () => {
    let calls = 0;
    const ctx = {
      cwd: '/tmp', abortSignal: new AbortController().signal,
      processManager: {
        getOutput: () => { calls++; return { status: calls < 3 ? 'running' : 'completed', exitCode: calls < 3 ? null : 0, elapsedMs: 1000, output: 'final out' }; },
      },
    } as unknown as ToolContext;
    const result = await taskOutputTool.execute!({ task_id: 'w1', wait: true }, ctx);
    assert.ok(!result.isError);
    assert.ok(result.output.includes('completed'), 'returns terminal status');
    assert.ok(result.output.includes('Exit code: 0'));
    assert.ok(calls >= 3, 'polled internally until terminal');
  });

  it('wait also accepts the string "true" (provider boolean coercion)', async () => {
    let calls = 0;
    const ctx = {
      cwd: '/tmp', abortSignal: new AbortController().signal,
      processManager: { getOutput: () => { calls++; return { status: calls < 2 ? 'running' : 'completed', exitCode: 0, elapsedMs: 1, output: '' }; } },
    } as unknown as ToolContext;
    const result = await taskOutputTool.execute!({ task_id: 'w1b', wait: 'true' as unknown as boolean }, ctx);
    assert.ok(result.output.includes('completed'));
  });

  it('wait:true stops at timeout_ms if still running', async () => {
    const ctx = {
      cwd: '/tmp', abortSignal: new AbortController().signal,
      processManager: { getOutput: () => ({ status: 'running', exitCode: null, elapsedMs: 1000, output: '' }) },
    } as unknown as ToolContext;
    const result = await taskOutputTool.execute!({ task_id: 'w2', wait: true, timeout_ms: 30 }, ctx);
    assert.ok(result.output.includes('running'), 'returns running after the wait timeout');
  });

  it('wait:true bails out immediately on abort', async () => {
    const ac = new AbortController();
    const ctx = {
      cwd: '/tmp', abortSignal: ac.signal,
      processManager: { getOutput: () => { ac.abort(); return { status: 'running', exitCode: null, elapsedMs: 1, output: '' }; } },
    } as unknown as ToolContext;
    const result = await taskOutputTool.execute!({ task_id: 'w3', wait: true }, ctx);
    assert.ok(result.output.includes('running'), 'aborts the wait loop');
  });

  it('respects custom tail parameter', async () => {
    const ctx = {
      cwd: '/tmp',
      processManager: {
        getOutput: (id: string, tail: number) => ({
          status: 'running',
          exitCode: null,
          elapsedMs: 1000,
          output: `tail=${tail}`,
        }),
      },
    } as unknown as ToolContext;
    const result = await taskOutputTool.execute!({ task_id: 't1', tail: 100 }, ctx);
    assert.ok(result.output.includes('last 100 lines'));
  });
});
