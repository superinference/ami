import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as os from 'os';
import * as path from 'path';
import {
  PermissionManager,
  createPermissionRequestMessage,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U003 (w73): extended PermissionDecisionReason taxonomy — workingDir / hook /
// sandboxOverride arms, their rendering, and the machine-readable ruleSource
// provenance on the `rule` arm. Mirrors the reference's decisionReason union
// (components/permissions/hooks.ts + PermissionDecisionDebugInfo.tsx).
// ---------------------------------------------------------------------------

test('createPermissionRequestMessage renders the new taxonomy arms', () => {
  assert.match(
    createPermissionRequestMessage({ type: 'workingDir', path: '/x/secret', reason: '"/x/secret" is outside the workspace and requires approval to access' }),
    /outside the workspace/,
  );
  assert.match(
    createPermissionRequestMessage({ type: 'hook', hookName: 'PreToolUse', reason: 'blocked by policy' }),
    /Hook "PreToolUse" requires approval.*blocked by policy/,
  );
  assert.match(
    createPermissionRequestMessage({ type: 'hook', hookName: 'PreToolUse' }),
    /Hook "PreToolUse" requires approval for this action$/,
  );
  assert.match(
    createPermissionRequestMessage({ type: 'sandboxOverride' }),
    /bypass the sandbox/,
  );
});

test('checkWithReason attributes a sandbox opt-out to sandboxOverride', async () => {
  const pm = new PermissionManager('ask');
  const r = await pm.checkWithReason('bash', { command: 'make install', dangerouslyDisableSandbox: true });
  assert.equal(r.decision, 'ask');
  assert.equal(r.reason?.type, 'sandboxOverride');
  assert.match(r.message ?? '', /bypass the sandbox/);
});

test('sandboxOverride is NOT produced for a deny-all deny (stays mode)', async () => {
  const pm = new PermissionManager('deny-all');
  const r = await pm.checkWithReason('bash', { command: 'make install', dangerouslyDisableSandbox: true });
  assert.equal(r.decision, 'deny');
  assert.equal(r.reason?.type, 'mode');
});

test('checkWithReason attributes an out-of-workspace read to workingDir', async () => {
  const pm = new PermissionManager('ask');
  const outside = path.join(os.tmpdir(), 'ami-u003-w73-outside', 'secret.txt');
  const r = await pm.checkWithReason('file_read', { file_path: outside });
  assert.equal(r.decision, 'ask');
  assert.equal(r.reason?.type, 'workingDir');
  if (r.reason?.type === 'workingDir') {
    assert.equal(r.reason.path, outside);
    assert.match(r.reason.reason, /outside the workspace/);
  }
});

test('an in-workspace read ask is NOT attributed to workingDir', async () => {
  const pm = new PermissionManager('ask');
  const inside = path.join(process.cwd(), 'notes.txt');
  const r = await pm.checkWithReason('file_read', { file_path: inside });
  // Inside the workspace → the ask is never attributed to a workingDir boundary.
  assert.equal(r.decision, 'ask');
  assert.notEqual(r.reason?.type, 'workingDir');
});

test('a matched rule carries its originating tier as ruleSource', async () => {
  const pm = new PermissionManager('ask', [
    { tool: 'bash', pattern: 'npm *', action: 'deny', reason: 'blocked by admins', source: 'policySettings' },
  ]);
  const r = await pm.checkWithReason('bash', { command: 'npm install' });
  assert.equal(r.decision, 'deny');
  assert.equal(r.reason?.type, 'rule');
  if (r.reason?.type === 'rule') {
    assert.equal(r.reason.ruleSource, 'policySettings');
    // The free-text `source` stays the rule's own label (unchanged behavior).
    assert.equal(r.reason.source, 'blocked by admins');
  }
});

test('a rule without a source tier omits ruleSource', async () => {
  const pm = new PermissionManager('ask', [
    { tool: 'bash', pattern: 'npm *', action: 'deny' },
  ]);
  const r = await pm.checkWithReason('bash', { command: 'npm install' });
  assert.equal(r.reason?.type, 'rule');
  if (r.reason?.type === 'rule') assert.equal(r.reason.ruleSource, undefined);
});
