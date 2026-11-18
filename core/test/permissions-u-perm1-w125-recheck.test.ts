import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as os from 'node:os';
import * as path from 'node:path';
import * as fs from 'node:fs';
import { PermissionManager } from '../src/permissions';

// U-perm1 / w125 — recheckPermission core primitive.
//
// Claude Code's ToolUseConfirm.recheckPermission() re-runs the decision for a
// still-open permission request when the rules change concurrently. AMI's core
// enabler is PermissionManager.onPermissionStateChanged(): a multi-subscriber
// notification fired on every decision-affecting mutation (rule add/remove, mode
// transition, read-root grant/revoke) so a pending prompt can re-evaluate itself.

test('onPermissionStateChanged fires when an allow rule is added', () => {
  const pm = new PermissionManager('ask');
  let fired = 0;
  pm.onPermissionStateChanged(() => { fired++; });
  pm.addRule({ tool: 'bash', pattern: 'echo *', action: 'allow' });
  assert.equal(fired, 1);
});

test('does NOT fire when addRule is a no-op (duplicate dedup)', () => {
  const pm = new PermissionManager('ask');
  pm.addRule({ tool: 'bash', pattern: 'echo *', action: 'allow' });
  let fired = 0;
  pm.onPermissionStateChanged(() => { fired++; });
  // Same rule again — deduped, nothing added, so no notification.
  pm.addRule({ tool: 'bash', pattern: 'echo *', action: 'allow' });
  assert.equal(fired, 0);
});

test('fires on removeRule only when a rule was actually removed', () => {
  const pm = new PermissionManager('ask');
  pm.addRule({ tool: 'bash', pattern: 'echo *', action: 'allow' });
  let fired = 0;
  pm.onPermissionStateChanged(() => { fired++; });
  // No match → nothing removed → no notification.
  const noneRemoved = pm.removeRule(r => r.pattern === 'does-not-exist');
  assert.equal(noneRemoved, 0);
  assert.equal(fired, 0);
  // Real removal → notify.
  const removed = pm.removeRule(r => r.pattern === 'echo *');
  assert.equal(removed, 1);
  assert.equal(fired, 1);
});

test('fires on a real mode transition, not on a no-op setMode', () => {
  const pm = new PermissionManager('ask');
  let fired = 0;
  pm.onPermissionStateChanged(() => { fired++; });
  pm.setMode('ask'); // no-op (same mode)
  assert.equal(fired, 0);
  pm.setMode('acceptEdits'); // real transition
  assert.equal(fired, 1);
});

test('fires on a new read-root grant but not an idempotent re-grant, and on revoke', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'perm-recheck-'));
  try {
    const pm = new PermissionManager('ask');
    let fired = 0;
    pm.onPermissionStateChanged(() => { fired++; });
    pm.registerAllowedRoot(dir, 'session');
    assert.equal(fired, 1);
    // Same path, same source → idempotent, no notification.
    pm.registerAllowedRoot(dir, 'session');
    assert.equal(fired, 1);
    // Revoke → notify.
    assert.equal(pm.removeAllowedRoot(dir), true);
    assert.equal(fired, 2);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('fires transitively through applyUpdate (addRules / setMode)', () => {
  const pm = new PermissionManager('ask');
  let fired = 0;
  pm.onPermissionStateChanged(() => { fired++; });
  pm.applyUpdate({ type: 'addRules', behavior: 'allow', rules: [{ toolName: 'bash', ruleContent: 'ls *' }], destination: 'session' });
  assert.equal(fired, 1);
  pm.applyUpdate({ type: 'setMode', mode: 'plan' });
  assert.equal(fired, 2);
});

test('unsubscribe stops further notifications', () => {
  const pm = new PermissionManager('ask');
  let fired = 0;
  const unsub = pm.onPermissionStateChanged(() => { fired++; });
  pm.addRule({ tool: 'bash', pattern: 'a *', action: 'allow' });
  assert.equal(fired, 1);
  unsub();
  pm.addRule({ tool: 'bash', pattern: 'b *', action: 'allow' });
  assert.equal(fired, 1);
});

test('all subscribers fire; a throwing subscriber never aborts the mutation', () => {
  const pm = new PermissionManager('ask');
  let a = 0;
  let b = 0;
  pm.onPermissionStateChanged(() => { a++; throw new Error('boom'); });
  pm.onPermissionStateChanged(() => { b++; });
  // The mutation must still apply and the second listener must still fire.
  assert.doesNotThrow(() => pm.addRule({ tool: 'bash', pattern: 'git status', action: 'allow' }));
  assert.equal(a, 1);
  assert.equal(b, 1);
});

test('no subscribers → mutation is a cheap no-op (no throw, nothing to notify)', () => {
  const pm = new PermissionManager('ask');
  assert.doesNotThrow(() => pm.addRule({ tool: 'bash', pattern: 'pwd', action: 'allow' }));
});
