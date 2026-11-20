import { test, beforeEach, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  // Gap 1 — memoized session-stable path resolution + canonical fast-path.
  resolveSessionStableBaseRealpath,
  invalidateSessionStableBaseRealpath,
  clearSessionStableBaseRealpathCache,
  hasSessionStableBaseRealpath,
  getPathsForPermissionCheck,
  // Gap 4 — pluggable per-tool custom validation.
  getCustomValidation,
  TOOL_CONTENT_VALIDATORS,
  validatePermissionRule,
  formatPermissionRuleError,
  PermissionManager,
} from '../src/permissions';

const tmpDirs: string[] = [];
function mkTmp(prefix = 'ami-perm2-w124-'): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  tmpDirs.push(d);
  // Canonicalize so the test's base string byte-matches the memo key (macOS
  // /var -> /private/var; Linux is a no-op).
  return fs.realpathSync(d);
}

beforeEach(() => {
  clearSessionStableBaseRealpathCache();
});

after(() => {
  for (const d of tmpDirs) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ }
  }
});

// ---------------------------------------------------------------------------
// Gap 1 — session-stable base-realpath memoization
// (parity: the reference's memoize(getPathsForPermissionCheck) /
//  getResolvedWorkingDirPaths for session-stable config paths,
//  references/claude/.../utils/permissions/pathValidation.ts:L101-128,L130-146)
// ---------------------------------------------------------------------------

test('resolveSessionStableBaseRealpath returns the canonical realpath for a base', () => {
  const real = mkTmp();
  const link = path.join(os.tmpdir(), `ami-link-${process.pid}-${Date.now()}`);
  fs.symlinkSync(real, link);
  tmpDirs.push(link);
  // The resolved form of the symlink is its real target.
  assert.equal(resolveSessionStableBaseRealpath(link), real);
  assert.equal(resolveSessionStableBaseRealpath(real), real);
});

test('resolveSessionStableBaseRealpath memoizes: a resolved base survives deletion of the dir', () => {
  const real = mkTmp();
  const resolved = resolveSessionStableBaseRealpath(real);
  assert.equal(resolved, real);
  assert.ok(hasSessionStableBaseRealpath(real));
  // Delete the directory. A NON-memoized realpath would now fail (null); the
  // memoized value must still be served — this is the proof memoization is active.
  fs.rmSync(real, { recursive: true, force: true });
  assert.equal(resolveSessionStableBaseRealpath(real), resolved);
  // After an explicit clear, the (now-gone) dir resolves to null again.
  clearSessionStableBaseRealpathCache();
  assert.equal(resolveSessionStableBaseRealpath(real), null);
});

test('resolveSessionStableBaseRealpath does NOT retain a miss (base may be created later)', () => {
  const missing = path.join(os.tmpdir(), `ami-never-${process.pid}-${Date.now()}`);
  assert.equal(resolveSessionStableBaseRealpath(missing), null);
  // The miss must not be cached, so a later-created base is not pinned to null.
  assert.equal(hasSessionStableBaseRealpath(missing), false);
});

test('invalidateSessionStableBaseRealpath evicts a single cached base', () => {
  const a = mkTmp();
  const b = mkTmp();
  resolveSessionStableBaseRealpath(a);
  resolveSessionStableBaseRealpath(b);
  assert.ok(hasSessionStableBaseRealpath(a) && hasSessionStableBaseRealpath(b));
  invalidateSessionStableBaseRealpath(a);
  assert.equal(hasSessionStableBaseRealpath(a), false);
  assert.equal(hasSessionStableBaseRealpath(b), true);
});

test('getPathsForPermissionCheck returns a COPY of precomputed paths (canonical fast-path) without any syscall walk', () => {
  const precomputed = ['/ws/a', '/ws/a/b'] as const;
  const out = getPathsForPermissionCheck('/ignored/when/precomputed', 40, precomputed);
  assert.deepEqual(out, ['/ws/a', '/ws/a/b']);
  // Must be a copy, not the same reference (caller's array must not be mutated).
  assert.notEqual(out, precomputed as unknown as string[]);
});

// ---------------------------------------------------------------------------
// Gap 1 — end-to-end wiring through PermissionManager
// ---------------------------------------------------------------------------

test('isPathAllowed resolves a SYMLINKED workspace base via the memo (wiring proof)', () => {
  const realCwd = mkTmp();
  const file = path.join(realCwd, 'inside.txt');
  fs.writeFileSync(file, 'x');
  const symlinkCwd = path.join(os.tmpdir(), `ami-cwdlink-${process.pid}-${Date.now()}`);
  fs.symlinkSync(realCwd, symlinkCwd);
  tmpDirs.push(symlinkCwd);

  const pm = new PermissionManager();
  // The candidate (a real path under realCwd) only matches the workspace once the
  // symlinked cwd base is realpath-resolved — exercising resolveSessionStableBaseRealpath.
  assert.equal(pm.isPathAllowed(file, symlinkCwd), true);
  assert.ok(
    hasSessionStableBaseRealpath(path.normalize(symlinkCwd)),
    'the symlinked cwd base should be memoized after a path check',
  );
});

test('registerAllowedRoot invalidates a stale base-realpath memo entry', () => {
  const root = mkTmp();
  // Seed a cache entry for the normalized root key.
  resolveSessionStableBaseRealpath(path.normalize(root));
  assert.ok(hasSessionStableBaseRealpath(path.normalize(root)));
  const pm = new PermissionManager();
  pm.registerAllowedRoot(root);
  // Granting the root must drop its stale memo entry so it re-resolves fresh.
  assert.equal(hasSessionStableBaseRealpath(path.normalize(root)), false);
});

test('canonical fast-path still DENIES a symlink hop to a forbidden target (full walk preserved)', () => {
  const realCwd = mkTmp();
  // A symlink inside the workspace pointing at a system dir must still be denied —
  // the symlink is non-canonical, so the full hop walk (not the fast-path) runs.
  const link = path.join(realCwd, 'etc-link');
  fs.symlinkSync('/etc/passwd', link);
  const pm = new PermissionManager();
  assert.equal(pm.isPathAllowed(link, realCwd), false);
  // And a plain in-workspace file (canonical fast-path) is still allowed.
  const ok = path.join(realCwd, 'ok.txt');
  fs.writeFileSync(ok, 'y');
  assert.equal(pm.isPathAllowed(ok, realCwd), true);
});

// ---------------------------------------------------------------------------
// Gap 4 — pluggable per-tool custom validation (getCustomValidation)
// (parity: the reference's toolValidationConfig customValidation hook,
//  references/claude/.../utils/settings/permissionValidation.ts:L137-153)
// ---------------------------------------------------------------------------

test('getCustomValidation returns a validator for registry tools, normalizing the name', () => {
  assert.equal(typeof getCustomValidation('web_fetch'), 'function');
  assert.equal(typeof getCustomValidation('web_search'), 'function');
  // Claude-spelled aliases resolve to the same validator via normalizeToolName.
  assert.equal(typeof getCustomValidation('WebFetch'), 'function');
  assert.equal(typeof getCustomValidation('WebSearch'), 'function');
  // A tool with no custom validator (bash / file tools use generic checks).
  assert.equal(getCustomValidation('bash'), undefined);
  assert.equal(getCustomValidation('file_edit'), undefined);
});

test('TOOL_CONTENT_VALIDATORS is the single registry backing getCustomValidation', () => {
  const keys = Object.keys(TOOL_CONTENT_VALIDATORS).sort();
  assert.deepEqual(keys, ['web_fetch', 'web_search']);
  assert.equal(TOOL_CONTENT_VALIDATORS.web_fetch, getCustomValidation('web_fetch'));
});

test('web_fetch validator rejects a URL, rejects a path, accepts a bare/wildcard domain', () => {
  const v = getCustomValidation('web_fetch')!;
  assert.equal(v('https://example.com').valid, false);
  assert.equal(v('3http://evil').valid, false);
  // NEW (additive) behavior: a domain pattern carrying a path component is a
  // mistake because WebFetch matches the host only.
  const pathResult = v('example.com/api/*');
  assert.equal(pathResult.valid, false);
  assert.match(pathResult.error!, /domain, not a path/);
  // Valid bare + wildcard domains.
  assert.equal(v('example.com').valid, true);
  assert.equal(v('*.github.com').valid, true);
  assert.equal(v('httpbin.org').valid, true);
});

test('web_search validator rejects wildcards, accepts a plain term', () => {
  const v = getCustomValidation('web_search')!;
  assert.equal(v('weather*').valid, false);
  assert.equal(v('weather?').valid, false);
  assert.equal(v('stock prices').valid, true);
});

test('validatePermissionRule routes per-tool content checks through the registry', () => {
  // Regression lock: the behaviors that used to be hardcoded still hold after
  // consolidation into getCustomValidation.
  assert.equal(validatePermissionRule('web_fetch(https://example.com)').valid, false);
  assert.equal(validatePermissionRule('web_search(foo*)').valid, false);
  assert.equal(validatePermissionRule('web_fetch(example.com)').valid, true);
  assert.equal(validatePermissionRule('web_search(weather)').valid, true);
  // NEW: a path in a web_fetch domain pattern is rejected end-to-end, and its
  // message composes through formatPermissionRuleError.
  const res = validatePermissionRule('web_fetch(example.com/api)');
  assert.equal(res.valid, false);
  const msg = formatPermissionRuleError(res);
  assert.match(msg, /domain, not a path/);
  assert.match(msg, /Examples:/);
});

test('validatePermissionRule leaves non-registry tools to the generic checks', () => {
  // bash still validated by the bash-prefix rules, not a custom validator.
  assert.equal(validatePermissionRule('bash(git:*)').valid, true);
  assert.equal(validatePermissionRule('bash(:*)').valid, false);
  // A file tool uses glob checks, not a custom validator.
  assert.equal(validatePermissionRule('file_edit(src/**)').valid, true);
});
