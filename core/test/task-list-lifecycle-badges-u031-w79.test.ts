import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { taskListTool } from '../src/tools/task-list';
import type { ToolContext } from '../src/types';
import type { BackgroundProcess } from '../src/process-manager';

// ---------------------------------------------------------------------------
// U031 / w79: task_list surfaces the agent lifecycle badges [shutting down]
// (cooperative graceful shutdown in flight) and [idle] (agent quiescent between
// turns). Parity with Claude Code's InProcessTeammateTask shutdownRequested /
// isIdle. Red-green: reverting the badge block drops these from the output.
// ---------------------------------------------------------------------------

let idSeq = 0;
function mkAgent(over: Partial<BackgroundProcess> = {}): BackgroundProcess {
  idSeq++;
  return {
    taskId: over.taskId ?? `bg-${idSeq.toString(16).padStart(8, '0')}`,
    pid: over.pid ?? process.pid,
    command: over.command ?? '[agent] worker',
    description: over.description ?? 'Background agent: worker',
    status: over.status ?? 'running',
    kind: 'agent',
    exitCode: over.exitCode ?? null,
    outputPath: over.outputPath ?? '/tmp/out',
    startTime: over.startTime ?? Date.now(),
    ...over,
  } as BackgroundProcess;
}
function ctx(tasks: BackgroundProcess[]): ToolContext {
  return { cwd: '/tmp', abortSignal: new AbortController().signal, processManager: { list: () => tasks } as any } as ToolContext;
}

describe('task_list lifecycle badges', () => {
  it('renders [shutting down] for an agent with a graceful shutdown in flight', async () => {
    const r = await taskListTool.execute({}, ctx([mkAgent({ shutdownRequested: true })]));
    assert.ok(r.output.includes('[shutting down]'), r.output);
    assert.ok(!r.output.includes('[idle]'), r.output);
  });

  it('renders [idle] for a quiescent agent', async () => {
    const r = await taskListTool.execute({}, ctx([mkAgent({ idle: true })]));
    assert.ok(r.output.includes('[idle]'), r.output);
  });

  it('prefers [shutting down] over [idle] when both are set', async () => {
    const r = await taskListTool.execute({}, ctx([mkAgent({ idle: true, shutdownRequested: true })]));
    assert.ok(r.output.includes('[shutting down]'), r.output);
    assert.ok(!r.output.includes('[idle]'), r.output);
  });

  it('shows no lifecycle badge for a busy agent', async () => {
    const r = await taskListTool.execute({}, ctx([mkAgent({})]));
    assert.ok(!r.output.includes('[shutting down]'), r.output);
    assert.ok(!r.output.includes('[idle]'), r.output);
  });

  it('shows no badge for a terminal agent even if idle was set', async () => {
    const r = await taskListTool.execute({}, ctx([mkAgent({ status: 'completed', exitCode: 0, idle: true })]));
    assert.ok(!r.output.includes('[idle]'), r.output);
  });
});
