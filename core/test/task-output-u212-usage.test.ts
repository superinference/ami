import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { taskOutputTool } from '../src/tools/task-output';
import type { ToolContext } from '../src/types';

/**
 * U212 parity (additive superset) — remaining task_output gaps:
 *   - a documented structured worker-result USAGE block (duration + total_tokens +
 *     tool_uses) is surfaced for a TERMINAL agent task, mirroring Claude Code's
 *     coordinator result envelope (`usage: { total_tokens, tool_uses, duration_ms }`).
 *     Reuses the canonical `formatUsageClause` so there is one usage-string source.
 *   - the live "waiting for task" progress line carries the "esc to give additional
 *     instructions" interrupt affordance, mirroring Claude Code's
 *     renderToolUseProgressMessage.
 */

function ctx(overrides: Record<string, unknown>, extra?: Partial<ToolContext>): ToolContext {
  return {
    cwd: '/tmp',
    abortSignal: new AbortController().signal,
    processManager: overrides,
    ...extra,
  } as unknown as ToolContext;
}

describe('task_output U212: structured worker-result usage block', () => {
  it('surfaces a Usage line (duration + tokens + tool uses) for a terminal agent task', async () => {
    const c = ctx({
      getOutput: () => ({ status: 'completed', exitCode: 0, elapsedMs: 3200, output: 'the answer' }),
      get: () => ({ outputPath: '/var/tasks/a.output', resultTokenCount: 1500, resultToolCount: 4 }),
    });
    const res = await taskOutputTool.execute!({ task_id: 'a' }, c);
    assert.ok(!res.isError);
    assert.ok(res.output.includes('Usage: ran 3.2s, 1500 tokens, 4 tool uses'), `expected usage block, got:\n${res.output}`);
  });

  it('falls back to the live token/tool counters when the result totals are absent', async () => {
    const c = ctx({
      getOutput: () => ({ status: 'completed', exitCode: 0, elapsedMs: 1000, output: 'x' }),
      get: () => ({ outputPath: '/var/tasks/a.output', tokenCount: 7, toolCount: 1 }),
    });
    const res = await taskOutputTool.execute!({ task_id: 'a' }, c);
    assert.ok(res.output.includes('Usage: ran 1.0s, 7 tokens, 1 tool use'), `expected fallback usage, got:\n${res.output}`);
  });

  it('does NOT emit a usage block for a terminal bash task (no token/tool stats)', async () => {
    const c = ctx({
      getOutput: () => ({ status: 'completed', exitCode: 0, elapsedMs: 500, output: 'built' }),
      get: () => ({ outputPath: '/var/tasks/b.output' }),
    });
    const res = await taskOutputTool.execute!({ task_id: 'b' }, c);
    assert.ok(!res.output.includes('Usage:'), 'bash task has no token/tool usage block');
  });

  it('does NOT emit a usage block while the task is still running', async () => {
    const c = ctx({
      getOutput: () => ({ status: 'running', exitCode: null, elapsedMs: 100, output: 'working' }),
      get: () => ({ outputPath: '/var/tasks/a.output', tokenCount: 10, toolCount: 2 }),
    });
    const res = await taskOutputTool.execute!({ task_id: 'a' }, c);
    assert.ok(!res.output.includes('Usage:'), 'no final usage for a running task');
  });
});

describe('task_output U212: esc interrupt affordance in the wait progress line', () => {
  it('includes the "esc to give additional instructions" hint while blocking', async () => {
    let calls = 0;
    const progress: string[] = [];
    const c = ctx(
      {
        getOutput: () => {
          calls++;
          return calls < 2
            ? { status: 'running', exitCode: null, elapsedMs: 1000, output: 'working' }
            : { status: 'completed', exitCode: 0, elapsedMs: 1200, output: 'done' };
        },
      },
      { onProgress: (d: string) => progress.push(d) },
    );
    await taskOutputTool.execute!({ task_id: 'bg-1', wait: true, timeout_ms: 5000 }, c);
    assert.ok(progress.length >= 1, 'emits a progress tick while waiting');
    assert.ok(progress.every(p => p.includes('bg-1')), 'progress names the task');
    assert.ok(progress.some(p => /esc to give additional instructions/i.test(p)), `expected esc affordance, got:\n${progress.join('\n')}`);
  });
});
