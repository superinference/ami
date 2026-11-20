import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  taskTrackerTool,
  resetTaskState,
  parseTaskId,
  normalizeOwner,
  wouldCreateCycle,
  getTaskState,
} from '../src/tools/task-tracker';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// the reference task-tracker parity hardening.
//
// Each behavioural suite below is a red-green reproducer: it is written so that
// reverting the corresponding fix in src/tools/task-tracker.ts makes the suite
// fail, and re-applying it makes the suite pass. The discriminating assertions
// are driven through the public tool (not the new helpers) so a targeted revert
// of a single fix is caught.
// ---------------------------------------------------------------------------

function ctx(): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
  };
}

const run = (input: Record<string, unknown>) => taskTrackerTool.execute(input, ctx());

beforeEach(() => {
  resetTaskState();
});

// ===========================================================================
// FIX 1 — Strict task_id parsing (no lenient parseInt truncation).
// Baseline parseInt('1abc', 10) === 1 silently retargets the call at task #1.
// ===========================================================================
describe('parity: strict task_id parsing', () => {
  it('get with a trailing-garbage id is rejected, not truncated to #1', async () => {
    await run({ action: 'create', subject: 'Real task one' });
    const r = await run({ action: 'get', task_id: '1abc' });
    // RED (lenient parseInt): would resolve to #1 and succeed.
    assert.equal(r.isError, true);
    assert.ok(r.output.includes('task_id is required'));
    assert.ok(!r.output.includes('Real task one'));
  });

  it('update with a decimal id does not mutate the truncated task', async () => {
    await run({ action: 'create', subject: 'Keep me pending' }); // #1
    await run({ action: 'create', subject: 'Second' });           // #2
    const r = await run({ action: 'update', task_id: '1.9', status: 'completed' });
    assert.equal(r.isError, true);
    // #1 must remain pending (never truncated-updated by '1.9').
    const got = await run({ action: 'get', task_id: '1' });
    assert.ok(got.output.includes('[pending]'));
  });

  it('delete with a garbage-suffixed id does not delete the truncated task', async () => {
    await run({ action: 'create', subject: 'Survivor' }); // #1
    const r = await run({ action: 'delete', task_id: '1x' });
    assert.equal(r.isError, true);
    const got = await run({ action: 'get', task_id: '1' });
    assert.equal(got.isError, false);
    assert.ok(got.output.includes('Survivor'));
  });

  it('claim with a garbage-suffixed id is rejected', async () => {
    await run({ action: 'create', subject: 'Claimable' }); // #1
    const r = await run({ action: 'claim', task_id: '1zz', owner: 'alice' });
    assert.equal(r.isError, true);
    assert.ok(r.output.includes('task_id is required'));
    // #1 stays pending (unclaimed).
    const got = await run({ action: 'get', task_id: '1' });
    assert.ok(got.output.includes('[pending]'));
  });

  it('a clean id is still accepted (no regression)', async () => {
    await run({ action: 'create', subject: 'Clean' });
    const r = await run({ action: 'get', task_id: '1' });
    assert.equal(r.isError, false);
    assert.ok(r.output.includes('Clean'));
  });

  it('parseTaskId accepts digit strings (incl. whitespace-padded) and rejects the rest', () => {
    assert.equal(parseTaskId('7'), 7);
    assert.equal(parseTaskId('  12  '), 12);
    assert.equal(parseTaskId('0'), 0);
    assert.equal(parseTaskId('3abc'), null);
    assert.equal(parseTaskId('3.5'), null);
    assert.equal(parseTaskId(''), null);
    assert.equal(parseTaskId('   '), null);
    assert.equal(parseTaskId('-4'), null);
    // number inputs: safe non-negative integers only
    assert.equal(parseTaskId(9), 9);
    assert.equal(parseTaskId(-1), null);
    assert.equal(parseTaskId(1.5), null);
    assert.equal(parseTaskId(Number.MAX_SAFE_INTEGER + 2), null);
    assert.equal(parseTaskId(null), null);
    assert.equal(parseTaskId(undefined), null);
    assert.equal(parseTaskId({}), null);
  });
});

// ===========================================================================
// FIX 2 — Owner normalization (trim + whitespace-only => unset).
// ===========================================================================
describe('parity: owner normalization', () => {
  it('create trims a padded owner so getAgentStatuses does not fragment', async () => {
    await run({ action: 'create', subject: 'Owned', owner: '  alice  ' });
    const r = await run({ action: 'status' });
    // RED (verbatim owner): row reads "  alice  : busy", so 'alice: busy' is absent.
    assert.ok(r.output.includes('alice: busy'));
    assert.ok(!r.output.includes('  alice'));
  });

  it('update trims a padded owner in the rendered task', async () => {
    await run({ action: 'create', subject: 'T' });
    await run({ action: 'update', task_id: '1', owner: '  bob  ' });
    const r = await run({ action: 'get', task_id: '1' });
    assert.ok(r.output.includes('Owner: bob'));
    assert.ok(!r.output.includes('Owner:  bob'));
  });

  it('a whitespace-only owner is treated as unowned (create + status)', async () => {
    await run({ action: 'create', subject: 'Blankowner', owner: '   ' });
    const r = await run({ action: 'status' });
    // RED: '   ' is truthy -> appears as a busy owner row.
    assert.ok(r.output.includes('all agents idle'));
  });

  it('a whitespace-only owner on claim produces no owner suffix', async () => {
    await run({ action: 'create', subject: 'C' });
    const r = await run({ action: 'claim', task_id: '1', owner: '   ' });
    assert.equal(r.isError, false);
    // RED: '   ' owner -> "(owner:    )" suffix is emitted.
    assert.ok(!r.output.includes('(owner:'));
  });

  it('a whitespace-only owner leaves a task claimable via next (unowned)', async () => {
    await run({ action: 'create', subject: 'Pullable', owner: '  ' });
    const r = await run({ action: 'next' });
    // RED: whitespace owner excludes it from the unowned candidate set.
    assert.ok(r.output.includes('Task #1'));
  });

  it('normalizeOwner unit: trims, blanks -> undefined, non-strings -> undefined', () => {
    assert.equal(normalizeOwner('  alice '), 'alice');
    assert.equal(normalizeOwner('bob'), 'bob');
    assert.equal(normalizeOwner(''), undefined);
    assert.equal(normalizeOwner('    '), undefined);
    assert.equal(normalizeOwner(42), undefined);
    assert.equal(normalizeOwner(undefined), undefined);
    assert.equal(normalizeOwner(null), undefined);
  });
});

// ===========================================================================
// FIX 3 — Dependency cycle prevention (deadlock-free task graph).
// ===========================================================================
describe('parity: dependency cycle prevention', () => {
  it('rejects a 2-node cycle created via addBlocks', async () => {
    await run({ action: 'create', subject: 'A' }); // #1
    await run({ action: 'create', subject: 'B' }); // #2
    const ok = await run({ action: 'update', task_id: '1', addBlocks: ['2'] }); // #1 before #2
    assert.equal(ok.isError, false);
    const bad = await run({ action: 'update', task_id: '2', addBlocks: ['1'] }); // would cycle
    // RED (no cycle check): edge is added -> both tasks permanently blocked.
    assert.equal(bad.isError, true);
    assert.ok(bad.output.includes('dependency_cycle'));
    // The graph stays claimable: #1 (unblocked) is still available.
    const next = await run({ action: 'next' });
    assert.ok(next.output.includes('Task #1'));
  });

  it('rejects a 2-node cycle created via addBlockedBy', async () => {
    await run({ action: 'create', subject: 'C' }); // #1
    await run({ action: 'create', subject: 'D' }); // #2
    const ok = await run({ action: 'update', task_id: '1', addBlockedBy: ['2'] }); // #1 after #2
    assert.equal(ok.isError, false);
    const bad = await run({ action: 'update', task_id: '2', addBlockedBy: ['1'] }); // would cycle
    assert.equal(bad.isError, true);
    assert.ok(bad.output.includes('dependency_cycle'));
  });

  it('rejects a longer transitive cycle (A->B->C->A)', async () => {
    await run({ action: 'create', subject: 'A' }); // #1
    await run({ action: 'create', subject: 'B' }); // #2
    await run({ action: 'create', subject: 'C' }); // #3
    await run({ action: 'update', task_id: '1', addBlocks: ['2'] }); // #1 before #2
    await run({ action: 'update', task_id: '2', addBlocks: ['3'] }); // #2 before #3
    const bad = await run({ action: 'update', task_id: '3', addBlocks: ['1'] }); // #3 before #1 -> cycle
    assert.equal(bad.isError, true);
    assert.ok(bad.output.includes('dependency_cycle'));
  });

  it('a cyclic update is atomic — no partial status change is applied', async () => {
    await run({ action: 'create', subject: 'A' }); // #1
    await run({ action: 'create', subject: 'B' }); // #2
    await run({ action: 'update', task_id: '1', addBlocks: ['2'] });
    // Try to both change the subject AND add a cyclic edge in one call.
    const bad = await run({ action: 'update', task_id: '2', subject: 'Renamed', addBlocks: ['1'] });
    assert.equal(bad.isError, true);
    const got = await run({ action: 'get', task_id: '2' });
    // Subject must be unchanged (whole update rejected).
    assert.ok(got.output.includes('Task #2: B'));
    assert.ok(!got.output.includes('Renamed'));
  });

  it('a self-edge is still silently skipped (not reported as a cycle)', async () => {
    await run({ action: 'create', subject: 'Solo' }); // #1
    const r = await run({ action: 'update', task_id: '1', addBlocks: ['1'] });
    assert.equal(r.isError, false);
    assert.ok(!r.output.includes('dependency_cycle'));
  });

  it('non-cyclic chains of edges are still accepted', async () => {
    await run({ action: 'create', subject: 'A' }); // #1
    await run({ action: 'create', subject: 'B' }); // #2
    const r = await run({ action: 'update', task_id: '2', addBlockedBy: ['1'] });
    assert.equal(r.isError, false);
  });

  it('wouldCreateCycle unit: self, reachable, and unreachable', () => {
    const tasks = getTaskState();
    tasks.set(1, { id: 1, subject: 'a', description: '', status: 'pending', blocks: [], blockedBy: [] });
    tasks.set(2, { id: 2, subject: 'b', description: '', status: 'pending', blocks: [], blockedBy: [1] });
    // #2 depends on #1. Adding "#2 before #1" (blocker=2, blocked=1) cycles.
    assert.equal(wouldCreateCycle(2, 1), true);
    // Adding "#1 before #2" is fine (no existing path #1 -> #2 via blockedBy).
    assert.equal(wouldCreateCycle(1, 2), false);
    // Self is always a cycle.
    assert.equal(wouldCreateCycle(1, 1), true);
    // Unknown node ids: no path, no cycle.
    assert.equal(wouldCreateCycle(99, 1), false);
  });
});

// ===========================================================================
// FIX 4 — Blocked-task start guard on update (parity with claim path).
// ===========================================================================
describe('parity: blocked-task start guard on update', () => {
  it('refuses to move a blocked task to in_progress via update', async () => {
    await run({ action: 'create', subject: 'Blocker' });  // #1
    await run({ action: 'create', subject: 'Dependent' }); // #2
    await run({ action: 'update', task_id: '2', addBlockedBy: ['1'] });
    const r = await run({ action: 'update', task_id: '2', status: 'in_progress' });
    // RED (no guard): update succeeds and starts the blocked task.
    assert.equal(r.isError, true);
    assert.ok(r.output.includes('blocked by: #1'));
    // #2 stays pending.
    const got = await run({ action: 'get', task_id: '2' });
    assert.ok(got.output.includes('[pending]'));
  });

  it('allows starting once the blocker is completed', async () => {
    await run({ action: 'create', subject: 'Blocker' });  // #1
    await run({ action: 'create', subject: 'Dependent' }); // #2
    await run({ action: 'update', task_id: '2', addBlockedBy: ['1'] });
    await run({ action: 'update', task_id: '1', status: 'completed' });
    const r = await run({ action: 'update', task_id: '2', status: 'in_progress' });
    assert.equal(r.isError, false);
    assert.ok(r.output.includes('[in_progress]'));
  });

  it('starting an unblocked task via update is unaffected', async () => {
    await run({ action: 'create', subject: 'Free' }); // #1
    const r = await run({ action: 'update', task_id: '1', status: 'in_progress' });
    assert.equal(r.isError, false);
    assert.ok(r.output.includes('[in_progress]'));
  });
});
