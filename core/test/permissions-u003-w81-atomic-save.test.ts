import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { PermissionManager } from '../src/permissions';

// GAP U003/w81 (F-perm atomic save): saveRules()/saveRulesToDestination() must
// persist via a crash-safe temp+rename with owner-only (0o600) mode — a plain
// fs.writeFileSync left the permissions file world-readable (default 0o644) and
// could leave a truncated file on a crash that loadRules then silently discards.

const MODE_MASK = 0o777;

test('saveRules persists permissions.json owner-only (0o600) and round-trips', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-perm-w81-'));
  try {
    const src = new PermissionManager('ask', [{ tool: 'bash', pattern: 'ls', action: 'allow' }]);
    const res = src.saveRules(dir);
    assert.equal(res.ok, true);

    const file = path.join(dir, '.ami', 'permissions.json');
    assert.ok(fs.existsSync(file), 'permissions.json should exist after save');
    // Owner-only file mode — the discriminating assertion (RED on plain
    // writeFileSync, which yields 0o644 under the usual umask).
    assert.equal(fs.statSync(file).mode & MODE_MASK, 0o600, 'file must be 0o600 (owner-only)');

    // Content is intact (atomic rename never leaves a partial file).
    const dst = new PermissionManager();
    dst.loadRules(dir);
    assert.equal(dst.getRules().length, 1);

    // No temp artifact is left behind — the temp file was renamed into place.
    const leftovers = fs.readdirSync(path.join(dir, '.ami')).filter(f => f.includes('.tmp.'));
    assert.deepEqual(leftovers, [], 'no .tmp. leftover after atomic rename');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('saveRulesToDestination persists a tier file owner-only (0o600)', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-perm-w81-'));
  try {
    const pm = new PermissionManager();
    const res = pm.saveRulesToDestination(dir, 'localSettings');
    assert.equal(res.ok, true);

    const file = path.join(dir, '.ami', 'permissions.local.json');
    assert.ok(fs.existsSync(file), 'tier file should exist after save');
    assert.equal(fs.statSync(file).mode & MODE_MASK, 0o600, 'tier file must be 0o600');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('in-memory destinations (session/cliArg) persist nothing', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-perm-w81-'));
  try {
    const pm = new PermissionManager();
    assert.deepEqual(pm.saveRulesToDestination(dir, 'session'), { ok: true });
    assert.deepEqual(pm.saveRulesToDestination(dir, 'cliArg'), { ok: true });
    assert.ok(!fs.existsSync(path.join(dir, '.ami')), 'nothing written for in-memory tiers');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('saveRules still reports ok:false when the target dir cannot be created', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-perm-w81-'));
  try {
    // A cwd that is a FILE makes creating `<file>/.ami` fail (ENOTDIR) — the
    // atomic writer propagates and saveRules surfaces ok:false, never a silent
    // success and never a fallback that corrupts an unrelated path.
    const asFile = path.join(tmp, 'notadir');
    fs.writeFileSync(asFile, 'x');
    const res = new PermissionManager().saveRules(asFile);
    assert.equal(res.ok, false);
    assert.ok(res.error && res.error.length > 0);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});
