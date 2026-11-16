import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { taskTrackerTool, resetTaskState } from '../src/tools/task-tracker';
import { HookManager } from '../src/hooks';
import type { ToolContext } from '../src/types';

function ctx(over: Partial<ToolContext> = {}): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...over,
  } as ToolContext;
}

beforeEach(() => resetTaskState());
afterEach(() => resetTaskState());

// ---------------------------------------------------------------------------
// U050 gap: Blocking TaskCompleted hook can VETO a status->completed transition
// ---------------------------------------------------------------------------
describe('task_tracker — blocking TaskCompleted hook veto', () => {
  it('refuses the completion and leaves the task unchanged when a hook blocks', async () => {
    const hm = {
      executeTaskCreated: async () => {},
      executeTaskCompleted: async () => ({ blockingErrors: ['tests still failing'] }),
    } as unknown as ToolContext['_hookManager'];
    await taskTrackerTool.execute({ action: 'create', subject: 'ship it' }, ctx({ _hookManager: hm }));

    const r = await taskTrackerTool.execute(
      { action: 'update', task_id: '1', status: 'completed' },
      ctx({ _hookManager: hm }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /blocked by hook: tests still failing/);
    assert.match(r.output, /completion_blocked/);

    // The status must NOT have advanced to completed.
    const got = await taskTrackerTool.execute({ action: 'get', task_id: '1' }, ctx());
    assert.ok(got.output.includes('[pending]'));
    assert.ok(!got.output.includes('[completed]'));
  });

  it('allows the completion when the hook returns no blocking errors', async () => {
    const hm = {
      executeTaskCreated: async () => {},
      executeTaskCompleted: async () => {},
    } as unknown as ToolContext['_hookManager'];
    await taskTrackerTool.execute({ action: 'create', subject: 'ok' }, ctx({ _hookManager: hm }));
    const r = await taskTrackerTool.execute(
      { action: 'update', task_id: '1', status: 'completed' },
      ctx({ _hookManager: hm }),
    );
    assert.equal(r.isError, false);
    assert.ok(r.output.includes('[completed]'));
  });

  it('does not block when the completion hook throws (best-effort parity)', async () => {
    const hm = {
      executeTaskCreated: async () => {},
      executeTaskCompleted: () => Promise.reject(new Error('boom')),
    } as unknown as ToolContext['_hookManager'];
    await taskTrackerTool.execute({ action: 'create', subject: 't' }, ctx({ _hookManager: hm }));
    const r = await taskTrackerTool.execute(
      { action: 'update', task_id: '1', status: 'completed' },
      ctx({ _hookManager: hm }),
    );
    assert.equal(r.isError, false);
    assert.ok(r.output.includes('[completed]'));
  });

  it('E2E: a real HookManager onTaskCompleted blocking hook vetoes completion', async () => {
    const hm = new HookManager();
    let seenTaskId = '';
    hm.onTaskCompleted(async (c) => {
      seenTaskId = c.taskId;
      return { blockingError: 'verification policy: run the test suite first' };
    });
    await taskTrackerTool.execute({ action: 'create', subject: 'risky change' }, ctx({ _hookManager: hm }));
    const blocked = await taskTrackerTool.execute(
      { action: 'update', task_id: '1', status: 'completed' },
      ctx({ _hookManager: hm }),
    );
    assert.equal(blocked.isError, true);
    assert.match(blocked.output, /verification policy: run the test suite first/);
    assert.equal(seenTaskId, '1');
    const got = await taskTrackerTool.execute({ action: 'get', task_id: '1' }, ctx());
    assert.ok(got.output.includes('[pending]'));
  });

  it('does not re-run the completed hook on a no-op re-complete', async () => {
    let calls = 0;
    const hm = {
      executeTaskCreated: async () => {},
      executeTaskCompleted: async () => { calls++; },
    } as unknown as ToolContext['_hookManager'];
    await taskTrackerTool.execute({ action: 'create', subject: 't' }, ctx({ _hookManager: hm }));
    await taskTrackerTool.execute({ action: 'update', task_id: '1', status: 'completed' }, ctx({ _hookManager: hm }));
    await taskTrackerTool.execute({ action: 'update', task_id: '1', status: 'completed' }, ctx({ _hookManager: hm }));
    assert.equal(calls, 1, 'hook fires only on the real pending->completed transition');
  });
});

// ---------------------------------------------------------------------------
// U050 gap: Completion affordance hint (next / now-unblocked task)
// ---------------------------------------------------------------------------
describe('task_tracker — completion affordance hint', () => {
  it('points at a dependent task that is now unblocked on completion', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'first' }, ctx());
    await taskTrackerTool.execute({ action: 'create', subject: 'second' }, ctx());
    // #2 is blocked by #1.
    await taskTrackerTool.execute({ action: 'update', task_id: '2', addBlockedBy: ['1'] }, ctx());
    const r = await taskTrackerTool.execute({ action: 'update', task_id: '1', status: 'completed' }, ctx());
    assert.equal(r.isError, false);
    assert.match(r.output, /Now unblocked: #2/);
  });

  it('gives a generic next-task hint when nothing is newly unblocked', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'alpha' }, ctx());
    await taskTrackerTool.execute({ action: 'create', subject: 'beta' }, ctx());
    const r = await taskTrackerTool.execute({ action: 'update', task_id: '1', status: 'completed' }, ctx());
    assert.match(r.output, /find your next available task/);
  });

  it('adds no hint when the update is not a completion', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'alpha' }, ctx());
    const r = await taskTrackerTool.execute({ action: 'update', task_id: '1', status: 'in_progress' }, ctx());
    assert.ok(!r.output.includes('find your next available task'));
    assert.ok(!r.output.includes('Now unblocked'));
  });
});

// ---------------------------------------------------------------------------
// U050 gap: Structural verification nudge (all done, >=3 tasks, no /verif/i)
// ---------------------------------------------------------------------------
describe('task_tracker — verification nudge', () => {
  it('nudges when the whole 3+ task list is done and none covers verification', async () => {
    for (const s of ['build a', 'build b', 'build c']) {
      await taskTrackerTool.execute({ action: 'create', subject: s }, ctx());
    }
    await taskTrackerTool.execute({ action: 'update', task_id: '1', status: 'completed' }, ctx());
    await taskTrackerTool.execute({ action: 'update', task_id: '2', status: 'completed' }, ctx());
    const r = await taskTrackerTool.execute({ action: 'update', task_id: '3', status: 'completed' }, ctx());
    assert.match(r.output, /none covers verification/);
  });

  it('does not nudge when a task already covers verification', async () => {
    for (const s of ['build a', 'build b', 'verify the work']) {
      await taskTrackerTool.execute({ action: 'create', subject: s }, ctx());
    }
    await taskTrackerTool.execute({ action: 'update', task_id: '1', status: 'completed' }, ctx());
    await taskTrackerTool.execute({ action: 'update', task_id: '2', status: 'completed' }, ctx());
    const r = await taskTrackerTool.execute({ action: 'update', task_id: '3', status: 'completed' }, ctx());
    assert.ok(!r.output.includes('none covers verification'));
  });

  it('does not nudge when fewer than 3 tasks exist', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'a' }, ctx());
    await taskTrackerTool.execute({ action: 'create', subject: 'b' }, ctx());
    await taskTrackerTool.execute({ action: 'update', task_id: '1', status: 'completed' }, ctx());
    const r = await taskTrackerTool.execute({ action: 'update', task_id: '2', status: 'completed' }, ctx());
    assert.ok(!r.output.includes('none covers verification'));
  });
});

// ---------------------------------------------------------------------------
// U050 gap: Per-field change detection (updatedFields list + status from->to)
// ---------------------------------------------------------------------------
describe('task_tracker — updatedFields reporting', () => {
  it('lists exactly the fields that changed', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'orig' }, ctx());
    const r = await taskTrackerTool.execute(
      { action: 'update', task_id: '1', subject: 'new title', status: 'in_progress' },
      ctx(),
    );
    assert.match(r.output, /Updated task #1 \(fields: [^)]*status[^)]*\)/);
    assert.match(r.output, /Updated task #1 \(fields: [^)]*subject[^)]*\)/);
    assert.match(r.output, /Status: pending -> in_progress/);
  });

  it('omits fields that did not actually change', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'same' }, ctx());
    // Re-set the same subject + a description change only.
    const r = await taskTrackerTool.execute(
      { action: 'update', task_id: '1', subject: 'same', description: 'added' },
      ctx(),
    );
    assert.match(r.output, /fields: description/);
    assert.ok(!/fields:[^)]*subject/.test(r.output), 'no-op subject must not be reported');
    assert.ok(!/fields:[^)]*status/.test(r.output), 'unchanged status must not be reported');
  });

  it('reports blocks/blockedBy when dependencies are added', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'a' }, ctx());
    await taskTrackerTool.execute({ action: 'create', subject: 'b' }, ctx());
    const r = await taskTrackerTool.execute(
      { action: 'update', task_id: '2', addBlockedBy: ['1'] },
      ctx(),
    );
    assert.match(r.output, /fields:[^)]*blockedBy/);
  });
});

// ---------------------------------------------------------------------------
// U050 gap: Auto-assign owner when moving to in_progress without an owner
// ---------------------------------------------------------------------------
describe('task_tracker — auto-assign owner on in_progress', () => {
  it('update: defaults owner to the acting agent identity', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'work' }, ctx());
    const r = await taskTrackerTool.execute(
      { action: 'update', task_id: '1', status: 'in_progress' },
      ctx({ agentId: 'worker-7' }),
    );
    assert.equal(r.isError, false);
    assert.match(r.output, /Owner: worker-7/);
    const status = await taskTrackerTool.execute({ action: 'status' }, ctx());
    assert.match(status.output, /worker-7: busy/);
  });

  it('update: does not override an explicitly-provided owner', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'work' }, ctx());
    const r = await taskTrackerTool.execute(
      { action: 'update', task_id: '1', status: 'in_progress', owner: 'alice' },
      ctx({ agentId: 'worker-7' }),
    );
    assert.match(r.output, /Owner: alice/);
    assert.ok(!r.output.includes('worker-7'));
  });

  it('update: leaves owner unset when there is no agent identity', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'work' }, ctx());
    const r = await taskTrackerTool.execute(
      { action: 'update', task_id: '1', status: 'in_progress' },
      ctx(),
    );
    assert.ok(!r.output.includes('Owner:'));
  });

  it('claim: defaults owner to the acting agent identity when none is named', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'claimable' }, ctx());
    const r = await taskTrackerTool.execute(
      { action: 'claim', task_id: '1' },
      ctx({ agentId: 'claimer-1' }),
    );
    assert.equal(r.isError, false);
    assert.match(r.output, /\(owner: claimer-1\)/);
  });
});
