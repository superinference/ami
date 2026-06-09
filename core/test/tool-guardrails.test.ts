import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { ToolCallGuardrailController } from '../src/tool-guardrails';

describe('ToolCallGuardrailController', () => {
  it('allows first call', () => {
    const ctrl = new ToolCallGuardrailController();
    const decision = ctrl.beforeCall('file_read', { file_path: '/tmp/test.txt' });
    assert.equal(decision.action, 'allow');
  });

  it('warns after 2 identical failures', () => {
    const ctrl = new ToolCallGuardrailController();
    const args = { file_path: '/nonexistent.txt' };

    ctrl.afterCall('file_read', args, 'Error: not found', true);
    let d = ctrl.beforeCall('file_read', args);
    assert.equal(d.action, 'allow');

    ctrl.afterCall('file_read', args, 'Error: not found', true);
    d = ctrl.beforeCall('file_read', args);
    assert.equal(d.action, 'warn');
  });

  it('blocks after 4 identical failures', () => {
    const ctrl = new ToolCallGuardrailController();
    const args = { command: 'broken-cmd' };

    for (let i = 0; i < 4; i++) {
      ctrl.afterCall('bash', args, 'Error: command not found', true);
    }

    const d = ctrl.beforeCall('bash', args);
    assert.equal(d.action, 'block');
  });

  it('resets failure count on success', () => {
    const ctrl = new ToolCallGuardrailController();
    const args = { file_path: '/tmp/test.txt' };

    ctrl.afterCall('file_read', args, 'Error: not found', true);
    ctrl.afterCall('file_read', args, 'Error: not found', true);

    // Success resets the counter
    ctrl.afterCall('file_read', args, 'file content here', false);

    const d = ctrl.beforeCall('file_read', args);
    assert.equal(d.action, 'allow');
  });

  it('detects no-progress for idempotent tools', () => {
    const ctrl = new ToolCallGuardrailController();
    const args = { file_path: '/tmp/test.txt' };
    const output = 'line 1\nline 2\nline 3';

    ctrl.afterCall('file_read', args, output, false);
    ctrl.afterCall('file_read', args, output, false);
    let d = ctrl.afterCall('file_read', args, output, false);
    assert.equal(d.action, 'warn');
  });

  it('allows different results for idempotent tools', () => {
    const ctrl = new ToolCallGuardrailController();
    const args = { query: 'test' };

    ctrl.afterCall('grep', args, 'result 1', false);
    const d = ctrl.afterCall('grep', args, 'result 2 (different)', false);
    assert.equal(d.action, 'allow');
  });

  it('does not track no-progress for non-idempotent tools', () => {
    const ctrl = new ToolCallGuardrailController();
    // Use distinct args each call to avoid loop detection
    for (let i = 0; i < 5; i++) {
      const d = ctrl.afterCall('bash', { command: `echo ${i}` }, `${i}`, false);
      assert.equal(d.action, 'allow');
    }
  });

  it('reset clears all tracking', () => {
    const ctrl = new ToolCallGuardrailController();
    const args = { file_path: '/nonexistent.txt' };

    ctrl.afterCall('file_read', args, 'Error', true);
    ctrl.afterCall('file_read', args, 'Error', true);
    ctrl.afterCall('file_read', args, 'Error', true);

    ctrl.reset();

    const d = ctrl.beforeCall('file_read', args);
    assert.equal(d.action, 'allow');
  });
});

// ---------------------------------------------------------------------------
// Content-hash loop detection
// ---------------------------------------------------------------------------

describe('ToolCallGuardrailController – loop detection', () => {
  it('detects repeating sequence of 3 identical calls', () => {
    const ctrl = new ToolCallGuardrailController();

    for (let round = 0; round < 2; round++) {
      ctrl.afterCall('bash', { command: 'step1' }, 'out1', false);
      ctrl.afterCall('bash', { command: 'step2' }, 'out2', false);
      const d = ctrl.afterCall('bash', { command: 'step3' }, 'out3', false);
      if (round === 1) {
        assert.equal(d.action, 'warn');
        assert.ok(d.reason!.includes('loop'));
      }
    }
  });

  it('does not trigger for non-repeating calls', () => {
    const ctrl = new ToolCallGuardrailController();

    ctrl.afterCall('bash', { command: 'a' }, 'out1', false);
    ctrl.afterCall('bash', { command: 'b' }, 'out2', false);
    ctrl.afterCall('bash', { command: 'c' }, 'out3', false);
    ctrl.afterCall('bash', { command: 'd' }, 'out4', false);
    ctrl.afterCall('bash', { command: 'e' }, 'out5', false);
    const d = ctrl.afterCall('bash', { command: 'f' }, 'out6', false);
    assert.equal(d.action, 'allow');
  });

  it('detects loop with 2-call repeating pattern', () => {
    const ctrl = new ToolCallGuardrailController();

    ctrl.afterCall('bash', { command: 'x' }, 'out', false);
    ctrl.afterCall('bash', { command: 'y' }, 'out2', false);
    ctrl.afterCall('bash', { command: 'x' }, 'out', false);
    const d = ctrl.afterCall('bash', { command: 'y' }, 'out2', false);
    assert.equal(d.action, 'warn');
    assert.ok(d.reason!.includes('loop'));
  });

  it('detects loop when same result changes meaning', () => {
    const ctrl = new ToolCallGuardrailController();

    for (let round = 0; round < 2; round++) {
      ctrl.afterCall('file_edit', { file_path: 'a.ts', old_string: 'x', new_string: 'y' }, 'edited', false);
      ctrl.afterCall('file_edit', { file_path: 'a.ts', old_string: 'y', new_string: 'x' }, 'edited', false);
      const d = ctrl.afterCall('bash', { command: 'npm test' }, 'FAIL', false);
      if (round === 1) {
        assert.equal(d.action, 'warn');
      }
    }
  });

  it('reset clears loop history', () => {
    const ctrl = new ToolCallGuardrailController();

    for (let i = 0; i < 3; i++) {
      ctrl.afterCall('bash', { command: 'step' }, 'out', false);
    }
    ctrl.reset();

    for (let i = 0; i < 3; i++) {
      ctrl.afterCall('bash', { command: 'step' }, 'out', false);
    }

    assert.equal(ctrl.detectLoop(), null);
  });

  it('detectLoop returns null when history is too short', () => {
    const ctrl = new ToolCallGuardrailController();
    ctrl.afterCall('bash', { command: 'a' }, 'out', false);
    assert.equal(ctrl.detectLoop(), null);
  });
});
