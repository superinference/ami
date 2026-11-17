import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { taskTrackerTool } from '../src/tools/task-tracker';
import { isToolReadOnlyForInput, isToolConcurrencySafe } from '../src/tools/index';

// ---------------------------------------------------------------------------
// U050 gap — read-only / concurrency-safe task-listing boundary.
//
// Claude Code ships task LISTING/GET as dedicated tools declaring isReadOnly=>true
// AND isConcurrencySafe=>true, so a pure task read joins the concurrent read batch
// and never becomes a serial write barrier. AMI consolidates read + write into one
// task_tracker tool whose STATIC isReadOnly must stay false (it can mutate), so a
// per-invocation verdict (isReadOnlyForInput) is required to recover the boundary.
// These assert the exact resolvers the tool-executor consults for segmentation.
// ---------------------------------------------------------------------------
describe('task_tracker — per-invocation read-only classification (U050)', () => {
  it('declares a per-invocation read-only hook while keeping the static flag writable', () => {
    assert.equal(typeof taskTrackerTool.isReadOnlyForInput, 'function');
    // The tool CAN mutate, so the static flag stays false.
    assert.equal(taskTrackerTool.isReadOnly, false);
  });

  it('classifies the non-mutating actions (list/get/next/status) as read-only', () => {
    for (const action of ['list', 'get', 'next', 'status']) {
      assert.equal(
        taskTrackerTool.isReadOnlyForInput!({ action }),
        true,
        `${action} should be read-only`,
      );
    }
  });

  it('classifies every mutating action as writable', () => {
    for (const action of ['create', 'update', 'delete', 'claim', 'release']) {
      assert.equal(
        taskTrackerTool.isReadOnlyForInput!({ action }),
        false,
        `${action} should be writable`,
      );
    }
    // An unknown/missing action is also treated as writable (never auto-allowed).
    assert.equal(taskTrackerTool.isReadOnlyForInput!({}), false);
    assert.equal(taskTrackerTool.isReadOnlyForInput!({ action: 'bogus' }), false);
  });

  it('resolves read-only + concurrency-safe through the shared classifiers the executor uses', () => {
    // These are the EXACT functions tool-executor calls when forming the concurrent
    // read batch (readOnlyForInputSafe && isToolConcurrencySafe). A read must be both.
    assert.equal(isToolReadOnlyForInput(taskTrackerTool, { action: 'list' }), true);
    assert.equal(isToolConcurrencySafe(taskTrackerTool, { action: 'list' }), true);
    assert.equal(isToolReadOnlyForInput(taskTrackerTool, { action: 'get' }), true);
    assert.equal(isToolConcurrencySafe(taskTrackerTool, { action: 'get' }), true);
    // A write is neither — it stays a serial barrier.
    assert.equal(isToolReadOnlyForInput(taskTrackerTool, { action: 'create' }), false);
    assert.equal(isToolConcurrencySafe(taskTrackerTool, { action: 'create' }), false);
  });
});
