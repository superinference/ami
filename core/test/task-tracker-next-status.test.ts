import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  taskTrackerTool,
  resetTaskState,
  findAvailableTask,
  getAgentStatuses,
} from '../src/tools/task-tracker';
import type { ToolContext } from '../src/types';

function ctx(): ToolContext {
  return { cwd: process.cwd(), abortSignal: new AbortController().signal } as ToolContext;
}
const run = (input: Record<string, unknown>) => taskTrackerTool.execute!(input, ctx());

beforeEach(() => resetTaskState());

describe('findAvailableTask (gap: auto-select next pending/unowned/unblocked task)', () => {
  it('returns the lowest-id pending, unowned, unblocked task', async () => {
    await run({ action: 'create', subject: 'first' });   // #1
    await run({ action: 'create', subject: 'second' });  // #2
    const t = findAvailableTask();
    assert.equal(t!.id, 1);
  });

  it('skips in_progress, owned-by-others, and blocked tasks', async () => {
    await run({ action: 'create', subject: 'a' }); // #1
    await run({ action: 'create', subject: 'b' }); // #2
    await run({ action: 'create', subject: 'c' }); // #3
    await run({ action: 'claim', task_id: '1', owner: 'alice' });       // #1 in_progress/owned
    await run({ action: 'update', task_id: '2', owner: 'bob' });        // #2 owned by bob
    // #3 blocked by #1 (not completed)
    await run({ action: 'update', task_id: '3', addBlockedBy: ['1'] });
    assert.equal(findAvailableTask(), null, 'nothing is available for an anonymous claimant');
    // bob's own owned pending task #2 is available to bob
    assert.equal(findAvailableTask('bob')!.id, 2);
  });

  it('"next" action reports the next task or a no-work message', async () => {
    const empty = await run({ action: 'next' });
    assert.match(empty.output, /No available tasks/);
    await run({ action: 'create', subject: 'work it' });
    const r = await run({ action: 'next' });
    assert.match(r.output, /Next available task/);
    assert.match(r.output, /#1/);
    assert.match(r.output, /Claim it/);
  });
});

describe('getAgentStatuses (gap: per-agent idle/busy introspection)', () => {
  it('reports busy for an owner holding unresolved tasks, idle otherwise', async () => {
    await run({ action: 'create', subject: 'a' }); // #1
    await run({ action: 'create', subject: 'b' }); // #2
    await run({ action: 'claim', task_id: '1', owner: 'alice' });
    await run({ action: 'update', task_id: '2', owner: 'bob', status: 'completed' });
    const rows = getAgentStatuses();
    const alice = rows.find(r => r.owner === 'alice')!;
    const bob = rows.find(r => r.owner === 'bob')!;
    assert.equal(alice.status, 'busy');
    assert.deepEqual(alice.tasks, [1]);
    assert.equal(bob.status, 'idle');
  });

  it('"status" action renders per-owner rows / all-idle message', async () => {
    const none = await run({ action: 'status' });
    assert.match(none.output, /all agents idle/);
    await run({ action: 'create', subject: 'a' });
    await run({ action: 'claim', task_id: '1', owner: 'alice' });
    const r = await run({ action: 'status' });
    assert.match(r.output, /alice: busy/);
    assert.match(r.output, /#1/);
  });
});
