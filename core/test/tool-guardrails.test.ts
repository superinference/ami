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
    const args = { command: 'echo hello' };
    const output = 'hello';

    for (let i = 0; i < 5; i++) {
      const d = ctrl.afterCall('bash', args, output, false);
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
