import { test } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import os from 'node:os';
import { PermissionManager } from '../src/permissions';

// ---------------------------------------------------------------------------
// explainAllow — structured machine-readable decisionReason on an AUTO-ALLOW
// (parity: Claude's PowerShellTool modeValidation auto-allow decisionReason).
// The only two ways AMI auto-approves: an explicit allow RULE, or the MODE.
// ---------------------------------------------------------------------------

test('explainAllow attributes a standing allow RULE', async () => {
  const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: 'git *', action: 'allow' }]);
  assert.equal(await pm.check('bash', { command: 'git status' }), 'allow');
  const reason = pm.explainAllow('bash', { command: 'git status' });
  assert.equal(reason.type, 'rule');
  if (reason.type === 'rule') {
    assert.equal(reason.action, 'allow');
    assert.equal(reason.ruleValue, 'bash(git *)');
  }
});

test('explainAllow attributes the permission MODE when no rule matches', async () => {
  const pm = new PermissionManager('auto-allow');
  assert.equal(await pm.check('bash', { command: 'some-arbitrary-cmd' }), 'allow');
  const reason = pm.explainAllow('bash', { command: 'some-arbitrary-cmd' });
  assert.equal(reason.type, 'mode');
  if (reason.type === 'mode') assert.equal(reason.mode, 'auto-allow');
});

test('explainAllow attributes acceptEdits mode for an edit tool', async () => {
  const pm = new PermissionManager('acceptEdits');
  assert.equal(await pm.check('file_edit', { file_path: 'src/x.ts' }), 'allow');
  const reason = pm.explainAllow('file_edit', { file_path: 'src/x.ts' });
  assert.equal(reason.type, 'mode');
  if (reason.type === 'mode') assert.equal(reason.mode, 'acceptEdits');
});

test('explainAllow prefers the RULE over the mode when both would allow', async () => {
  const pm = new PermissionManager('auto-allow', [{ tool: 'bash', pattern: 'npm *', action: 'allow' }]);
  const reason = pm.explainAllow('bash', { command: 'npm test' });
  assert.equal(reason.type, 'rule');
  if (reason.type === 'rule') assert.equal(reason.ruleValue, 'bash(npm *)');
});

// ---------------------------------------------------------------------------
// blockedPath — the exact path that tripped the gate on an ask/deny verdict
// (parity: Claude's PermissionAskDecision.blockedPath / PermissionResult.blockedPath).
// ---------------------------------------------------------------------------

test('checkWithReason sets blockedPath + workingDir reason for an out-of-workspace read', async () => {
  const outside = path.join(os.tmpdir(), 'perm1-w122-outside.conf');
  const pm = new PermissionManager('ask');
  const r = await pm.checkWithReason('file_read', { file_path: outside });
  assert.equal(r.decision, 'ask');
  assert.equal(r.reason?.type, 'workingDir');
  assert.equal(r.blockedPath, outside);
});

test('checkWithReason sets blockedPath for a safety-gated file edit', async () => {
  const target = path.join(process.cwd(), 'package.json');
  const pm = new PermissionManager('acceptEdits');
  const r = await pm.checkWithReason('file_edit', { file_path: target });
  assert.equal(r.decision, 'ask');
  assert.equal(r.reason?.type, 'safetyCheck');
  assert.equal(r.blockedPath, target);
});

test('checkWithReason falls back to the `path` input key for blockedPath', async () => {
  const outside = path.join(os.tmpdir(), 'perm1-w122-pathkey.conf');
  const pm = new PermissionManager('ask');
  const r = await pm.checkWithReason('file_read', { path: outside });
  assert.equal(r.decision, 'ask');
  assert.equal(r.blockedPath, outside);
});

test('checkWithReason omits blockedPath on an allow and on a non-path bash deny', async () => {
  const allowPm = new PermissionManager('auto-allow');
  const allow = await allowPm.checkWithReason('bash', { command: 'ls' });
  assert.equal(allow.decision, 'allow');
  assert.equal(allow.blockedPath, undefined);

  const denyPm = new PermissionManager('ask');
  const bashDeny = await denyPm.checkWithReason('bash', { command: 'rm -rf /' });
  assert.equal(bashDeny.decision, 'deny');
  assert.equal(bashDeny.blockedPath, undefined);
});
