import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { taskTrackerTool, resetTaskState } from '../src/tools/task-tracker';
import type { ToolContext, EngineEvent } from '../src/types';

function ctx(over: Partial<ToolContext> = {}): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...over,
  } as ToolContext;
}

/** Context that captures every child event the tool emits. */
function capturingCtx(sink: EngineEvent[], over: Partial<ToolContext> = {}): ToolContext {
  return ctx({ _emitChild: (e: EngineEvent) => sink.push(e), ...over });
}

function taskEvents(sink: EngineEvent[]): Array<{ taskId: number; status: string; subject: string }> {
  return sink
    .filter((e): e is Extract<EngineEvent, { type: 'task_updated' }> => e.type === 'task_updated')
    .map((e) => ({ taskId: e.taskId, status: e.status, subject: e.subject }));
}

beforeEach(() => resetTaskState());
afterEach(() => resetTaskState());

// ---------------------------------------------------------------------------
// U050 gap: live task_updated event emitted on every mutating action.
// The EngineEvent is declared in types.ts and handled by event-processor.ts,
// repl.ts and detached-runner.ts, but NOTHING emitted it (dead wire). The tool
// must drive it so the live task panel reflects create/update/claim/release/delete.
// ---------------------------------------------------------------------------
describe('task_tracker — live task_updated event emission', () => {
  it('emits a task_updated event on create', async () => {
    const sink: EngineEvent[] = [];
    await taskTrackerTool.execute({ action: 'create', subject: 'ship it' }, capturingCtx(sink));
    assert.deepEqual(taskEvents(sink), [{ taskId: 1, status: 'pending', subject: 'ship it' }]);
  });

  it('emits a task_updated event on update (status change)', async () => {
    const sink: EngineEvent[] = [];
    await taskTrackerTool.execute({ action: 'create', subject: 'ship it' }, capturingCtx(sink));
    sink.length = 0;
    await taskTrackerTool.execute({ action: 'update', task_id: '1', status: 'in_progress' }, capturingCtx(sink));
    assert.deepEqual(taskEvents(sink), [{ taskId: 1, status: 'in_progress', subject: 'ship it' }]);
  });

  it('emits a task_updated event on claim', async () => {
    const sink: EngineEvent[] = [];
    await taskTrackerTool.execute({ action: 'create', subject: 'ship it' }, capturingCtx(sink));
    sink.length = 0;
    await taskTrackerTool.execute({ action: 'claim', task_id: '1', owner: 'alice' }, capturingCtx(sink));
    assert.deepEqual(taskEvents(sink), [{ taskId: 1, status: 'in_progress', subject: 'ship it' }]);
  });

  it('emits a task_updated event on release', async () => {
    const sink: EngineEvent[] = [];
    await taskTrackerTool.execute({ action: 'create', subject: 'ship it' }, capturingCtx(sink));
    await taskTrackerTool.execute({ action: 'claim', task_id: '1', owner: 'alice' }, capturingCtx(sink));
    sink.length = 0;
    await taskTrackerTool.execute({ action: 'release', task_id: '1' }, capturingCtx(sink));
    assert.deepEqual(taskEvents(sink), [{ taskId: 1, status: 'pending', subject: 'ship it' }]);
  });

  it('emits a task_updated event with status "deleted" on delete', async () => {
    const sink: EngineEvent[] = [];
    await taskTrackerTool.execute({ action: 'create', subject: 'ship it' }, capturingCtx(sink));
    sink.length = 0;
    await taskTrackerTool.execute({ action: 'delete', task_id: '1' }, capturingCtx(sink));
    assert.deepEqual(taskEvents(sink), [{ taskId: 1, status: 'deleted', subject: 'ship it' }]);
  });

  it('emits a task_updated event with status "deleted" on update->deleted', async () => {
    const sink: EngineEvent[] = [];
    await taskTrackerTool.execute({ action: 'create', subject: 'ship it' }, capturingCtx(sink));
    sink.length = 0;
    await taskTrackerTool.execute({ action: 'update', task_id: '1', status: 'deleted' }, capturingCtx(sink));
    assert.deepEqual(taskEvents(sink), [{ taskId: 1, status: 'deleted', subject: 'ship it' }]);
  });

  it('does NOT emit when a mutation fails (not found / blocked)', async () => {
    const sink: EngineEvent[] = [];
    // update a nonexistent task -> error, no event
    const r1 = await taskTrackerTool.execute({ action: 'update', task_id: '99', status: 'completed' }, capturingCtx(sink));
    assert.equal(r1.isError, true);
    // claim a nonexistent task -> error, no event
    const r2 = await taskTrackerTool.execute({ action: 'claim', task_id: '99' }, capturingCtx(sink));
    assert.equal(r2.isError, true);
    // delete a nonexistent task -> error, no event
    const r3 = await taskTrackerTool.execute({ action: 'delete', task_id: '99' }, capturingCtx(sink));
    assert.equal(r3.isError, true);
    assert.deepEqual(taskEvents(sink), []);
  });

  it('does not throw when no _emitChild is provided (no-op frontend)', async () => {
    const r = await taskTrackerTool.execute({ action: 'create', subject: 'ship it' }, ctx());
    assert.equal(r.isError, false);
  });

  it('read-only actions (list/get/next/status) emit no task_updated event', async () => {
    const sink: EngineEvent[] = [];
    await taskTrackerTool.execute({ action: 'create', subject: 'ship it' }, ctx());
    await taskTrackerTool.execute({ action: 'list' }, capturingCtx(sink));
    await taskTrackerTool.execute({ action: 'get', task_id: '1' }, capturingCtx(sink));
    await taskTrackerTool.execute({ action: 'next' }, capturingCtx(sink));
    await taskTrackerTool.execute({ action: 'status' }, capturingCtx(sink));
    assert.deepEqual(taskEvents(sink), []);
  });
});

// ---------------------------------------------------------------------------
// U050 gap: enumerated completion-discipline guardrail wording on the tool
// surface (never mark completed if tests failing / partial / unresolved errors
// / missing files/deps). Must be present on the always-advertised description.
// ---------------------------------------------------------------------------
describe('task_tracker — completion-discipline guardrail wording', () => {
  it('enumerates the never-complete conditions in the tool description', () => {
    const d = taskTrackerTool.description.toLowerCase();
    assert.match(d, /tests are failing|tests failing/);
    assert.match(d, /partial/);
    assert.match(d, /unresolved error/);
    assert.match(d, /files|dependencies|deps/);
    // keeps the blocked->create-a-blocker-task guidance
    assert.match(d, /blocker task/);
  });
});
