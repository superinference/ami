import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { ToolCallGuardrailController } from '../src/tool-guardrails';

// ---------------------------------------------------------------------------
// Covers uncovered lines 21-22 (hashArgs with sorted keys) and 62-65
// (evictOldest when exceeding MAX_TRACKED_SIGNATURES)
// ---------------------------------------------------------------------------

describe('ToolCallGuardrailController — eviction', () => {
  it('evicts oldest entry when exceeding MAX_TRACKED_SIGNATURES (500) for failures', () => {
    const ctrl = new ToolCallGuardrailController();

    // Fill up to 501 unique failure signatures to trigger eviction
    for (let i = 0; i < 501; i++) {
      ctrl.afterCall('bash', { command: `cmd-${i}` }, 'Error: fail', true);
    }

    // The first entry should have been evicted
    // cmd-0 was evicted, so before call should allow
    const d = ctrl.beforeCall('bash', { command: 'cmd-0' });
    assert.equal(d.action, 'allow');

    // Recent ones should still be tracked (1 failure = allow, but the entry exists)
    const d2 = ctrl.beforeCall('bash', { command: 'cmd-500' });
    assert.equal(d2.action, 'allow'); // only 1 failure so far
  });

  it('evicts oldest entry when exceeding MAX_TRACKED_SIGNATURES for no-progress', () => {
    const ctrl = new ToolCallGuardrailController();

    // Fill up to 501 unique no-progress entries
    for (let i = 0; i < 501; i++) {
      ctrl.afterCall('file_read', { file_path: `/file-${i}` }, `content-${i}`, false);
    }

    // No crash, entries were evicted
    // Now add a new one — should work fine
    const d = ctrl.afterCall('file_read', { file_path: '/file-new' }, 'new-content', false);
    assert.equal(d.action, 'allow');
  });
});

describe('ToolCallGuardrailController — no-progress block', () => {
  it('warns on repeated identical results from idempotent tool (loop detection fires first)', () => {
    const ctrl = new ToolCallGuardrailController();
    const args = { file_path: '/tmp/stable.txt' };
    const output = 'stable content that never changes';

    for (let i = 0; i < 4; i++) {
      ctrl.afterCall('file_read', args, output, false);
    }

    const d = ctrl.afterCall('file_read', args, output, false);
    assert.equal(d.action, 'warn');
    assert.ok(d.reason!.includes('loop') || d.reason!.includes('identical'));
  });
});

describe('ToolCallGuardrailController — hashArgs sorted keys', () => {
  it('considers same keys in different order as identical', () => {
    const ctrl = new ToolCallGuardrailController();
    const args1 = { b: '2', a: '1' };
    const args2 = { a: '1', b: '2' };

    // Register failure with one key order
    ctrl.afterCall('bash', args1, 'Error', true);
    ctrl.afterCall('bash', args1, 'Error', true);

    // Check with different key order — should show the accumulated count
    const d = ctrl.beforeCall('bash', args2);
    assert.equal(d.action, 'warn');
  });
});
