import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { PermissionManager } from '../src/permissions';

test('removeAllowedRoot revokes a granted read root (boundary can shrink)', () => {
  const pm = new PermissionManager();
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-root-'));
  pm.registerAllowedRoot(dir);
  assert.ok(pm.getAllowedReadRoots().includes(path.normalize(dir)));
  assert.equal(pm.removeAllowedRoot(dir), true);
  assert.ok(!pm.getAllowedReadRoots().includes(path.normalize(dir)));
  // removing again / junk is a safe no-op
  assert.equal(pm.removeAllowedRoot(dir), false);
  assert.equal(pm.removeAllowedRoot(''), false);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('explicit deny rules are bypass-immune (fire in bypassPermissions mode)', async () => {
  const pm = new PermissionManager('bypassPermissions', [
    { tool: 'bash', pattern: 'rm *', action: 'deny' },
  ]);
  assert.equal(await pm.check('bash', { command: 'rm -rf build' }), 'deny');
  // non-denied command still auto-allowed under bypass
  assert.equal(await pm.check('bash', { command: 'ls' }), 'allow');
});

test('recent-denial store is newest-first, bounded, and typed by source', () => {
  const pm = new PermissionManager();
  pm.recordDenial('rm -rf /', 'hardline', 'catastrophic');
  pm.recordDenial('git push --force', 'classifier');
  const d = pm.getRecentDenials();
  assert.equal(d[0]?.display, 'git push --force');
  assert.equal(d[0]?.source, 'classifier');
  assert.equal(d[1]?.source, 'hardline');
  assert.equal(d[1]?.reason, 'catastrophic');
  for (let i = 0; i < 150; i++) pm.recordDenial('cmd' + i, 'rule');
  assert.ok(pm.getRecentDenials().length <= 100);
  pm.clearRecentDenials();
  assert.equal(pm.getRecentDenials().length, 0);
});

test('detectShadowedRules flags an allow shadowed by a tool-wide deny regardless of order', () => {
  // deny AFTER the allow — order-independent (a deny unconditionally wins)
  const pm = new PermissionManager('ask', [
    { tool: 'bash', pattern: 'ls', action: 'allow' },
    { tool: 'bash', action: 'deny' },
  ]);
  const shadows = pm.detectShadowedRules();
  assert.equal(shadows.length, 1);
  assert.equal(shadows[0]?.shadowedBy.action, 'deny');
  // ask shadower only counts when it PRECEDES the allow
  const pmAskAfter = new PermissionManager('ask', [
    { tool: 'bash', pattern: 'ls', action: 'allow' },
    { tool: 'bash', action: 'ask' },
  ]);
  assert.equal(pmAskAfter.detectShadowedRules().length, 0);
  const pmAskBefore = new PermissionManager('ask', [
    { tool: 'bash', action: 'ask' },
    { tool: 'bash', pattern: 'ls', action: 'allow' },
  ]);
  assert.equal(pmAskBefore.detectShadowedRules().length, 1);
});

test('saveRules returns a success/error signal and round-trips', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-perm-'));
  const src = new PermissionManager('ask', [{ tool: 'bash', pattern: 'ls', action: 'allow' }]);
  const res = src.saveRules(dir);
  assert.equal(res.ok, true);
  const dst = new PermissionManager();
  dst.loadRules(dir);
  assert.equal(dst.getRules().length, 1);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('saveRules returns ok:false with a message on a write failure', () => {
  const src = new PermissionManager();
  // A path whose parent is a file (not a dir) makes mkdir/write fail.
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-perm-'));
  const asFile = path.join(tmp, 'notadir');
  fs.writeFileSync(asFile, 'x');
  const res = src.saveRules(asFile);
  assert.equal(res.ok, false);
  assert.ok(res.error && res.error.length > 0);
  fs.rmSync(tmp, { recursive: true, force: true });
});

test('loadRules surfaces warnings for dropped/malformed rules', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-perm-'));
  fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
  fs.writeFileSync(path.join(dir, '.ami', 'permissions.json'), JSON.stringify({
    mode: 'not-a-mode',
    rules: [
      { tool: 'bash', action: 'allow' },
      { tool: 'bash', action: 'bogus' },
      { tool: 'bash', action: 'allow', paramPattern: { model: 123 } },
    ],
  }));
  const pm = new PermissionManager();
  const warnings = pm.loadRules(dir);
  assert.ok(warnings.some(w => w.includes('Invalid permission mode')));
  assert.ok(warnings.some(w => w.includes('was skipped')));
  assert.equal(pm.getRules().length, 1); // only the valid rule loaded
  fs.rmSync(dir, { recursive: true, force: true });
});
