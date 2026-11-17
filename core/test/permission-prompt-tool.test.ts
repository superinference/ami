/**
 * Delegated permission-prompt tool result parsing (parity: Claude Code's
 * --permission-prompt-tool / permissionToolOutputSchema).
 *
 * RED on revert: core/src/permission-prompt-tool.ts does not exist, so the import fails.
 * Run from core/core: node --import tsx --test test/permission-prompt-tool.test.ts
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  parsePermissionPromptToolResult,
  buildPermissionPromptToolInput,
} from '../src/permission-prompt-tool';

describe('buildPermissionPromptToolInput', () => {
  it('builds the reference {tool_name, input, tool_use_id} payload', () => {
    assert.deepEqual(
      buildPermissionPromptToolInput('bash', { command: 'ls' }, 'tu_1'),
      { tool_name: 'bash', input: { command: 'ls' }, tool_use_id: 'tu_1' },
    );
  });
  it('omits tool_use_id when absent', () => {
    assert.deepEqual(buildPermissionPromptToolInput('bash', { command: 'ls' }), {
      tool_name: 'bash',
      input: { command: 'ls' },
    });
  });
});

describe('parsePermissionPromptToolResult', () => {
  it('parses a bare allow', () => {
    const r = parsePermissionPromptToolResult({ behavior: 'allow' });
    assert.ok(r.ok);
    assert.deepEqual(r.decision, { behavior: 'allow' });
  });

  it('parses an allow with a non-empty updatedInput', () => {
    const r = parsePermissionPromptToolResult({ behavior: 'allow', updatedInput: { command: 'ls -la' } });
    assert.ok(r.ok);
    assert.deepEqual(r.decision, { behavior: 'allow', updatedInput: { command: 'ls -la' } });
  });

  it('treats an empty updatedInput {} as "use original" (reference parity)', () => {
    const r = parsePermissionPromptToolResult({ behavior: 'allow', updatedInput: {} });
    assert.ok(r.ok);
    assert.deepEqual(r.decision, { behavior: 'allow' });
  });

  it('passes updatedPermissions through on allow', () => {
    const perms = [{ type: 'addRules' }];
    const r = parsePermissionPromptToolResult({ behavior: 'allow', updatedPermissions: perms });
    assert.ok(r.ok);
    assert.ok(r.decision.behavior === 'allow');
    assert.deepEqual(r.decision.updatedPermissions, perms);
  });

  it('rejects a non-object updatedInput', () => {
    const r = parsePermissionPromptToolResult({ behavior: 'allow', updatedInput: 'nope' });
    assert.ok(!r.ok);
    assert.match(r.error, /updatedInput/);
  });

  it('rejects a non-array updatedPermissions', () => {
    const r = parsePermissionPromptToolResult({ behavior: 'allow', updatedPermissions: {} });
    assert.ok(!r.ok);
    assert.match(r.error, /updatedPermissions/);
  });

  it('parses a deny with a message', () => {
    const r = parsePermissionPromptToolResult({ behavior: 'deny', message: 'nope' });
    assert.ok(r.ok);
    assert.deepEqual(r.decision, { behavior: 'deny', message: 'nope' });
  });

  it('parses a deny with interrupt', () => {
    const r = parsePermissionPromptToolResult({ behavior: 'deny', message: 'stop', interrupt: true });
    assert.ok(r.ok);
    assert.deepEqual(r.decision, { behavior: 'deny', message: 'stop', interrupt: true });
  });

  it('rejects a deny without a string message', () => {
    const r = parsePermissionPromptToolResult({ behavior: 'deny' });
    assert.ok(!r.ok);
    assert.match(r.error, /message/);
  });

  it('rejects an unknown behavior', () => {
    const r = parsePermissionPromptToolResult({ behavior: 'maybe' });
    assert.ok(!r.ok);
    assert.match(r.error, /behavior/);
  });

  it('rejects a non-object / null / array result', () => {
    assert.ok(!parsePermissionPromptToolResult(null).ok);
    assert.ok(!parsePermissionPromptToolResult('allow').ok);
    assert.ok(!parsePermissionPromptToolResult([{ behavior: 'allow' }]).ok);
  });
});
