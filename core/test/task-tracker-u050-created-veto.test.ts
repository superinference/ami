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
// U050 gap: a blocking TaskCreated hook can VETO creation (with compensating
// rollback of the just-created task) — Claude Code parity.
// ---------------------------------------------------------------------------
describe('task_tracker — blocking TaskCreated hook veto + rollback', () => {
  it('rolls back the created task and surfaces the error when a hook blocks', async () => {
    const hm = {
      executeTaskCreated: async () => ({ blockingErrors: ['policy: new tasks require a ticket id'] }),
    } as unknown as ToolContext['_hookManager'];

    const r = await taskTrackerTool.execute(
      { action: 'create', subject: 'untracked work' },
      ctx({ _hookManager: hm }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /blocked by hook: policy: new tasks require a ticket id/);
    assert.match(r.output, /creation_blocked/);

    // Compensating rollback: the task must NOT remain in the list.
    const list = await taskTrackerTool.execute({ action: 'list' }, ctx());
    assert.equal(list.output, 'No tasks.');
    const got = await taskTrackerTool.execute({ action: 'get', task_id: '1' }, ctx());
    assert.match(got.output, /not found/);
  });

  it('keeps the created task when the hook returns no blocking errors', async () => {
    const hm = {
      executeTaskCreated: async () => {},
    } as unknown as ToolContext['_hookManager'];
    const r = await taskTrackerTool.execute(
      { action: 'create', subject: 'ok work' },
      ctx({ _hookManager: hm }),
    );
    assert.equal(r.isError, false);
    const got = await taskTrackerTool.execute({ action: 'get', task_id: '1' }, ctx());
    assert.ok(got.output.includes('ok work'));
    assert.ok(got.output.includes('[pending]'));
  });

  it('does not block creation when the created hook throws (best-effort parity)', async () => {
    const hm = {
      executeTaskCreated: () => Promise.reject(new Error('boom')),
    } as unknown as ToolContext['_hookManager'];
    const r = await taskTrackerTool.execute(
      { action: 'create', subject: 'resilient' },
      ctx({ _hookManager: hm }),
    );
    assert.equal(r.isError, false);
    const got = await taskTrackerTool.execute({ action: 'get', task_id: '1' }, ctx());
    assert.ok(got.output.includes('resilient'));
  });

  it('E2E: a real HookManager onTaskCreated blocking hook vetoes + rolls back', async () => {
    const hm = new HookManager();
    let seen = '';
    hm.onTaskCreated(async (c) => {
      seen = c.subject;
      return { blockingError: 'governance: creation denied' };
    });
    const blocked = await taskTrackerTool.execute(
      { action: 'create', subject: 'risky task' },
      ctx({ _hookManager: hm }),
    );
    assert.equal(blocked.isError, true);
    assert.match(blocked.output, /governance: creation denied/);
    assert.equal(seen, 'risky task');
    const list = await taskTrackerTool.execute({ action: 'list' }, ctx());
    assert.equal(list.output, 'No tasks.');
  });

  it('advances the id counter so a rolled-back id is not reused', async () => {
    const blockHm = {
      executeTaskCreated: async () => ({ blockingErrors: ['no'] }),
    } as unknown as ToolContext['_hookManager'];
    await taskTrackerTool.execute({ action: 'create', subject: 'first (blocked)' }, ctx({ _hookManager: blockHm }));
    // Next create (no hook) must get a fresh id, not reuse #1.
    const ok = await taskTrackerTool.execute({ action: 'create', subject: 'second' }, ctx());
    assert.equal(ok.isError, false);
    assert.match(ok.output, /Task #2 created/);
  });
});

// ---------------------------------------------------------------------------
// U050 gap: abort/cancellation signal propagated into task-created hook.
// ---------------------------------------------------------------------------
describe('task_tracker — TaskCreated hook abort boundary', () => {
  it('forwards the call abortSignal into the created-hook context', async () => {
    const hm = new HookManager();
    let seenSignal: AbortSignal | undefined;
    hm.onTaskCreated(async (c) => {
      seenSignal = c.signal;
    });
    const ac = new AbortController();
    await taskTrackerTool.execute(
      { action: 'create', subject: 'cancellable' },
      ctx({ _hookManager: hm, abortSignal: ac.signal }),
    );
    assert.equal(seenSignal, ac.signal);
  });

  it('short-circuits the hook loop when the signal is already aborted', async () => {
    const hm = new HookManager();
    let ran = 0;
    hm.onTaskCreated(async () => { ran++; });
    const ac = new AbortController();
    ac.abort();
    const r = await taskTrackerTool.execute(
      { action: 'create', subject: 'aborted' },
      ctx({ _hookManager: hm, abortSignal: ac.signal }),
    );
    // No hook ran, so nothing can block — creation succeeds.
    assert.equal(ran, 0);
    assert.equal(r.isError, false);
  });
});
