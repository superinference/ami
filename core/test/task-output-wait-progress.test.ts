import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { taskOutputTool } from '../src/tools/task-output';
import type { ToolContext } from '../src/types';

describe('task_output live wait progress (gap: live "waiting for task" progress)', () => {
  it('emits onProgress each poll while blocking, then returns the completed result', async () => {
    let calls = 0;
    const progress: string[] = [];
    const ctx = {
      cwd: '/tmp',
      abortSignal: new AbortController().signal,
      onProgress: (d: string) => progress.push(d),
      processManager: {
        getOutput: () => {
          calls++;
          // First few polls: still running; then completes.
          return calls < 3
            ? { status: 'running', exitCode: null, elapsedMs: calls * 1000, output: 'working\n' }
            : { status: 'completed', exitCode: 0, elapsedMs: 3000, output: 'done\n' };
        },
      },
    } as unknown as ToolContext;

    const result = await taskOutputTool.execute!({ task_id: 'bg-1', wait: true, timeout_ms: 5000 }, ctx);
    assert.ok(!result.isError);
    assert.match(result.output, /completed/);
    assert.ok(progress.length >= 1, 'should emit at least one progress tick while waiting');
    assert.ok(progress.every(p => p.includes('bg-1')), 'progress mentions the task id');
    assert.match(progress[0], /waiting for task/);
  });

  it('does not emit progress when the task is already terminal (no blocking)', async () => {
    const progress: string[] = [];
    const ctx = {
      cwd: '/tmp',
      abortSignal: new AbortController().signal,
      onProgress: (d: string) => progress.push(d),
      processManager: {
        getOutput: () => ({ status: 'completed', exitCode: 0, elapsedMs: 10, output: 'x\n' }),
      },
    } as unknown as ToolContext;
    await taskOutputTool.execute!({ task_id: 'done-1', wait: true }, ctx);
    assert.equal(progress.length, 0);
  });
});
