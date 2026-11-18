import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  PermissionManager,
  createPermissionRequestMessage,
  permissionPromptToolType,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U003 (w115): per-tool-category permission-prompt phrasing. Parity with
// Claude's PermissionRuleExplanation `toolType` ('tool' | 'command' | 'edit' |
// 'read'), which interpolates the tool category into the confirmation/denial
// message. AMI previously hardcoded "for this command" / "for this action"
// regardless of the tool.
// ---------------------------------------------------------------------------

test('permissionPromptToolType classifies tools by name', () => {
  assert.equal(permissionPromptToolType('bash'), 'command');
  assert.equal(permissionPromptToolType('Bash'), 'command'); // normalized
  assert.equal(permissionPromptToolType('file_edit'), 'edit');
  assert.equal(permissionPromptToolType('file_write'), 'edit');
  assert.equal(permissionPromptToolType('notebook_edit'), 'edit');
  assert.equal(permissionPromptToolType('file_read'), 'read');
  assert.equal(permissionPromptToolType('grep'), 'read');
  assert.equal(permissionPromptToolType('glob'), 'read');
  // Anything not read/edit/bash falls back to the generic 'tool' category.
  assert.equal(permissionPromptToolType('web_fetch'), 'tool');
  assert.equal(permissionPromptToolType('some_mcp_tool'), 'tool');
});

test('createPermissionRequestMessage keeps default wording when no toolType is given', () => {
  assert.equal(
    createPermissionRequestMessage({ type: 'rule', action: 'ask', ruleValue: 'git *' }),
    'Permission rule "git *" requires approval for this command',
  );
  assert.equal(
    createPermissionRequestMessage({ type: 'mode', mode: 'ask' }),
    'Current permission mode (ask) requires approval for this action',
  );
  assert.equal(
    createPermissionRequestMessage({ type: 'hook', hookName: 'PreToolUse' }),
    'Hook "PreToolUse" requires approval for this action',
  );
  assert.equal(
    createPermissionRequestMessage({ type: 'other' }),
    'This action requires approval',
  );
});

test('createPermissionRequestMessage interpolates the tool category when provided', () => {
  // rule arm
  assert.equal(
    createPermissionRequestMessage({ type: 'rule', action: 'deny', ruleValue: 'x' }, 'edit'),
    'Permission rule "x" requires denial for this edit',
  );
  assert.equal(
    createPermissionRequestMessage({ type: 'rule', action: 'ask' }, 'read'),
    'Permission rule requires approval for this read',
  );
  // mode arm
  assert.equal(
    createPermissionRequestMessage({ type: 'mode', mode: 'ask' }, 'read'),
    'Current permission mode (ask) requires approval for this read',
  );
  // hook arm
  assert.equal(
    createPermissionRequestMessage({ type: 'hook', hookName: 'H' }, 'edit'),
    'Hook "H" requires approval for this edit',
  );
  // other arm
  assert.equal(
    createPermissionRequestMessage({ type: 'other' }, 'edit'),
    'This edit requires approval',
  );
  // generic 'tool' category reads as "tool call"
  assert.equal(
    createPermissionRequestMessage({ type: 'mode', mode: 'ask' }, 'tool'),
    'Current permission mode (ask) requires approval for this tool call',
  );
});

test('checkWithReason phrases the message with the tool category (end-to-end)', async () => {
  // A deny rule on a READ tool → "...for this read".
  const pmRead = new PermissionManager('ask');
  pmRead.addRule({ tool: 'file_read', action: 'deny' });
  const rRead = await pmRead.checkWithReason('file_read', { file_path: '/tmp/x' });
  assert.equal(rRead.decision, 'deny');
  assert.equal(rRead.reason?.type, 'rule');
  assert.match(rRead.message ?? '', /for this read$/);
  assert.doesNotMatch(rRead.message ?? '', /for this command/);

  // A deny rule on an EDIT tool → "...for this edit".
  const pmEdit = new PermissionManager('ask');
  pmEdit.addRule({ tool: 'file_edit', action: 'deny' });
  const rEdit = await pmEdit.checkWithReason('file_edit', { file_path: '/tmp/x', old_string: 'a', new_string: 'b' });
  assert.equal(rEdit.decision, 'deny');
  assert.match(rEdit.message ?? '', /for this edit$/);

  // A deny rule on bash keeps the "command" noun.
  const pmBash = new PermissionManager('ask');
  pmBash.addRule({ tool: 'bash', action: 'deny' });
  const rBash = await pmBash.checkWithReason('bash', { command: 'echo hi' });
  assert.equal(rBash.decision, 'deny');
  assert.match(rBash.message ?? '', /for this command$/);
});
