import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  PermissionManager,
  createPermissionRequestMessage,
  type PermissionPromptResult,
} from '../src/permissions';

test('PermissionPromptResult carries feedback / acceptFeedback / scope / cancelled', () => {
  const deny: PermissionPromptResult = { action: 'deny', feedback: 'use git rm, not rm -rf' };
  const allow: PermissionPromptResult = { action: 'allow_session', scope: 'config-folder', acceptFeedback: 'ok' };
  const cancelled: PermissionPromptResult = { action: 'cancelled' };
  assert.equal(deny.feedback, 'use git rm, not rm -rf');
  assert.equal(allow.scope, 'config-folder');
  assert.equal(allow.acceptFeedback, 'ok');
  assert.equal(cancelled.action, 'cancelled');
});

test('createPermissionRequestMessage renders every reason type', () => {
  assert.match(createPermissionRequestMessage({ type: 'hardline', description: 'rm -rf /' }), /Blocked: rm -rf/);
  assert.match(createPermissionRequestMessage({ type: 'rule', action: 'deny', ruleValue: 'bash(rm *)', source: 'user' }), /rule "bash\(rm \*\)" from user requires denial/);
  assert.match(createPermissionRequestMessage({ type: 'mode', mode: 'ask' }), /permission mode \(ask\)/);
  assert.match(createPermissionRequestMessage({ type: 'safetyCheck', check: 'credential', message: 'creds!' }), /creds!/);
  assert.match(createPermissionRequestMessage({ type: 'classifier', name: 'yolo', reason: 'risky' }), /classifier \(yolo\).*risky/);
  assert.match(createPermissionRequestMessage({ type: 'other' }), /requires approval/);
});

test('checkWithReason: allow has no reason', async () => {
  const pm = new PermissionManager('auto-allow');
  const r = await pm.checkWithReason('bash', { command: 'ls' });
  assert.equal(r.decision, 'allow');
  assert.equal(r.reason, undefined);
});

test('checkWithReason attributes a hardline deny', async () => {
  const pm = new PermissionManager('ask');
  const r = await pm.checkWithReason('bash', { command: 'rm -rf /' });
  assert.equal(r.decision, 'deny');
  assert.equal(r.reason?.type, 'hardline');
  assert.ok(r.message && r.message.length > 0);
});

test('checkWithReason attributes an explicit deny rule', async () => {
  const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: 'npm *', action: 'deny', reason: 'user' }]);
  const r = await pm.checkWithReason('bash', { command: 'npm install' });
  assert.equal(r.decision, 'deny');
  assert.equal(r.reason?.type, 'rule');
  if (r.reason?.type === 'rule') assert.equal(r.reason.action, 'deny');
});

test('checkWithReason attributes a credential safety ask and a mode ask', async () => {
  const pm = new PermissionManager('ask');
  const cred = await pm.checkWithReason('bash', { command: 'cat ~/.ssh/id_rsa' });
  assert.equal(cred.decision, 'ask');
  assert.equal(cred.reason?.type, 'safetyCheck');
  if (cred.reason?.type === 'safetyCheck') assert.equal(cred.reason.check, 'credential');

  const modeAsk = await pm.checkWithReason('web_fetch', { url: 'https://x' });
  assert.equal(modeAsk.decision, 'ask');
  assert.equal(modeAsk.reason?.type, 'mode');
});

test('checkWithReason attributes an agent-config edit ask in acceptEdits', async () => {
  const pm = new PermissionManager('acceptEdits');
  const r = await pm.checkWithReason('file_edit', { file_path: '/repo/.ami/settings.json' });
  assert.equal(r.decision, 'ask');
  assert.equal(r.reason?.type, 'safetyCheck');
  if (r.reason?.type === 'safetyCheck') assert.equal(r.reason.check, 'agentConfig');
});
