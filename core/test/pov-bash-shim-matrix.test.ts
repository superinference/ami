/**
 * POV: EXHAUSTIVE guard for ui#25 (python → python3 shim).
 *
 * Tests the REAL behavior of `ensurePythonShimDir` + `_resetPythonShimCache` from
 * core/src/tools/bash.ts by building fixture `bin` dirs (via fs.mkdtempSync) that
 * contain, per case, only `python3` / both `python`+`python3` / neither, and
 * asserting the OBSERVABLE result:
 *   - python3-only  → a private shim dir containing an executable `python` that,
 *                     when run through /bin/sh, actually execs `python3` (we confirm
 *                     by pointing PATH at a fake `python3` that prints a marker).
 *   - python present → null (never shadow a real python).
 *   - no python3     → null (nothing to delegate to).
 *   - empty / garbage / undefined PATH → null.
 *   - memoization    → second call is cached regardless of a new PATH;
 *                      _resetPythonShimCache() clears the memo.
 *   - make=false     → returns a deterministic, NON-materialized dir (no shim written).
 *
 * No source-text assertions — we drive the functions and assert on results.
 */
import { test, before, after, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { execFileSync } from 'node:child_process';
import { ensurePythonShimDir, _resetPythonShimCache } from '../src/tools/bash';

// ---------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------
const tempDirs: string[] = [];

function mkTempDir(prefix: string): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  tempDirs.push(d);
  return d;
}

/** Create an executable shell script `name` in `dir` that echoes `marker`. */
function writeFakeExe(dir: string, name: string, marker: string): string {
  const p = path.join(dir, name);
  fs.writeFileSync(p, `#!/bin/sh\necho ${marker}\n`, { mode: 0o755 });
  fs.chmodSync(p, 0o755); // force exec bits past umask
  return p;
}

const PY3_MARKER = 'FAKE_PYTHON3_OK';
const PY_MARKER = 'FAKE_PYTHON_REAL';

let binPython3Only: string; // contains only python3
let binBoth: string;        // contains python + python3
let binNeither: string;     // empty
let binPythonOnly: string;  // contains only python (no python3)
let binGarbage: string;     // a path string pointing at non-existent dirs

before(() => {
  binPython3Only = mkTempDir('pov-shim-py3only-');
  writeFakeExe(binPython3Only, 'python3', PY3_MARKER);

  binBoth = mkTempDir('pov-shim-both-');
  writeFakeExe(binBoth, 'python3', PY3_MARKER);
  writeFakeExe(binBoth, 'python', PY_MARKER);

  binNeither = mkTempDir('pov-shim-neither-');

  binPythonOnly = mkTempDir('pov-shim-pyonly-');
  writeFakeExe(binPythonOnly, 'python', PY_MARKER);

  // Garbage PATH: segments that do not exist on disk.
  binGarbage = [
    path.join(os.tmpdir(), 'pov-shim-does-not-exist-abc'),
    path.join(os.tmpdir(), 'pov-shim-nope-xyz'),
  ].join(path.delimiter);
});

beforeEach(() => {
  // The shim decision is memoized module-wide; start every case from a clean slate.
  _resetPythonShimCache();
});

after(() => {
  _resetPythonShimCache();
  for (const d of tempDirs) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ }
  }
});

// ---------------------------------------------------------------------------
// python3-only → materialized shim that execs python3
// ---------------------------------------------------------------------------
test('python3-only PATH yields a shim dir with an executable python that execs python3', () => {
  const shimDir = ensurePythonShimDir(binPython3Only, true);
  assert.ok(shimDir, 'expected a non-null shim dir for python3-only PATH');
  tempDirs.push(shimDir); // ensure cleanup of the materialized dir

  // The dir is a private, randomly-named temp dir (mkdtemp), not the deterministic one.
  assert.equal(path.dirname(shimDir), os.tmpdir());
  assert.ok(path.basename(shimDir).startsWith('ami-pyshim-'),
    `shim dir should be a random ami-pyshim-* dir, got ${shimDir}`);

  const shim = path.join(shimDir, 'python');
  assert.ok(fs.existsSync(shim), 'shim `python` file must be materialized');

  // It must be executable by us.
  assert.doesNotThrow(() => fs.accessSync(shim, fs.constants.X_OK),
    'shim `python` must have the exec bit set');

  const st = fs.statSync(shim);
  assert.ok(st.isFile());
  assert.equal(st.mode & 0o111, 0o111, 'all exec bits should be set (owner/group/other)');
});

test('running the shim through /bin/sh actually execs python3 (prints python3 output)', () => {
  const shimDir = ensurePythonShimDir(binPython3Only, true);
  assert.ok(shimDir);
  tempDirs.push(shimDir);

  const shim = path.join(shimDir, 'python');
  // Run the shim with a PATH where `python3` resolves to our marker-printing fake.
  const out = execFileSync('/bin/sh', [shim], {
    env: { ...process.env, PATH: binPython3Only },
    encoding: 'utf8',
  }).trim();
  assert.equal(out, PY3_MARKER, 'the python shim must delegate to python3');
});

test('shim forwards arguments to python3 ("$@")', () => {
  // A fake python3 that echoes its args proves the shim passes "$@" through.
  const argEchoBin = mkTempDir('pov-shim-argecho-');
  const p = path.join(argEchoBin, 'python3');
  fs.writeFileSync(p, '#!/bin/sh\necho "args:$*"\n', { mode: 0o755 });
  fs.chmodSync(p, 0o755);

  _resetPythonShimCache();
  const shimDir = ensurePythonShimDir(argEchoBin, true);
  assert.ok(shimDir);
  tempDirs.push(shimDir);

  const out = execFileSync('/bin/sh', [path.join(shimDir, 'python'), 'hello', 'world'], {
    env: { ...process.env, PATH: argEchoBin },
    encoding: 'utf8',
  }).trim();
  assert.equal(out, 'args:hello world');
});

// ---------------------------------------------------------------------------
// null cases
// ---------------------------------------------------------------------------
test('PATH with both python and python3 → null (never shadow a real python)', () => {
  assert.equal(ensurePythonShimDir(binBoth, true), null);
});

test('PATH with python but no python3 → null', () => {
  assert.equal(ensurePythonShimDir(binPythonOnly, true), null);
});

test('PATH with neither python nor python3 → null', () => {
  assert.equal(ensurePythonShimDir(binNeither, true), null);
});

test('empty-string PATH → null', () => {
  assert.equal(ensurePythonShimDir('', true), null);
});

test('garbage PATH (non-existent dirs) → null', () => {
  assert.equal(ensurePythonShimDir(binGarbage, true), null);
});

test('PATH with empty segments plus a python3-only dir still yields a shim', () => {
  // Leading/trailing/double delimiters exercise the `if (!dir) continue` branch.
  const d = path.delimiter;
  const weird = `${d}${d}${binPython3Only}${d}`;
  const shimDir = ensurePythonShimDir(weird, true);
  assert.ok(shimDir, 'empty PATH segments must be skipped, not fatal');
  tempDirs.push(shimDir);
  assert.ok(fs.existsSync(path.join(shimDir, 'python')));
});

// ---------------------------------------------------------------------------
// memoization
// ---------------------------------------------------------------------------
test('result is memoized: a second call ignores a new PATH until reset', () => {
  const first = ensurePythonShimDir(binPython3Only, true);
  assert.ok(first);
  tempDirs.push(first);

  // Second call with a PATH that would otherwise return null — cached value wins.
  const second = ensurePythonShimDir(binNeither, true);
  assert.equal(second, first, 'cached shim dir must be returned regardless of new PATH');

  // A cached null is also sticky.
  _resetPythonShimCache();
  const nullCached = ensurePythonShimDir(binNeither, true);
  assert.equal(nullCached, null);
  const stillNull = ensurePythonShimDir(binPython3Only, true);
  assert.equal(stillNull, null, 'cached null must be returned regardless of new PATH');
});

test('_resetPythonShimCache clears the memo so a new PATH is re-evaluated', () => {
  const nullFirst = ensurePythonShimDir(binNeither, true);
  assert.equal(nullFirst, null);

  _resetPythonShimCache();
  const after = ensurePythonShimDir(binPython3Only, true);
  assert.ok(after, 'after reset, a python3-only PATH must re-evaluate to a shim dir');
  tempDirs.push(after);
});

// ---------------------------------------------------------------------------
// make=false → non-materialized deterministic dir
// ---------------------------------------------------------------------------
test('make=false returns a deterministic, non-materialized dir (no shim written)', () => {
  const shimDir = ensurePythonShimDir(binPython3Only, false);
  assert.ok(shimDir, 'make=false still returns the intended dir path for a python3-only PATH');

  // Deterministic name (no mkdtemp random suffix) — proves nothing was materialized.
  assert.equal(shimDir, path.join(os.tmpdir(), 'ami-pyshim'));
  assert.equal(path.basename(shimDir), 'ami-pyshim');

  // No `python` shim file was written by this call.
  assert.equal(fs.existsSync(path.join(shimDir, 'python')), false,
    'make=false must NOT write the python shim file');
});

test('make=false still returns null for the null cases', () => {
  _resetPythonShimCache();
  assert.equal(ensurePythonShimDir(binBoth, false), null);
  _resetPythonShimCache();
  assert.equal(ensurePythonShimDir(binNeither, false), null);
});
