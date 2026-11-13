/**
 * ui#25 — `python` → `python3` PATH shim.
 *
 * Modern distros ship only `python3`; an agent that runs `python foo.py` otherwise fails with
 * `bash: python: command not found` and the task result is left unverified. `ensurePythonShimDir`
 * materializes a `python` → `python3` wrapper ONLY when `python` is absent but `python3` is
 * present, in a private random temp dir (so it is not an attacker-plantable PATH entry).
 *
 * Run: cd core/core && node --import tsx --test test/bash-python-shim.test.ts
 */
import { describe, it, afterEach, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';
import { ensurePythonShimDir, _resetPythonShimCache } from '../src/tools/bash';

const tmpdirs: string[] = [];
function binDir(tools: string[]): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'pyshim-fixture-'));
  tmpdirs.push(d);
  for (const t of tools) {
    const p = path.join(d, t);
    fs.writeFileSync(p, `#!/bin/sh\necho ${t} "$@"\n`, { mode: 0o755 });
    fs.chmodSync(p, 0o755);
  }
  return d;
}

beforeEach(() => _resetPythonShimCache());
afterEach(() => {
  _resetPythonShimCache();
  for (const d of tmpdirs.splice(0)) fs.rmSync(d, { recursive: true, force: true });
});

describe('ensurePythonShimDir (ui#25)', () => {
  it('creates a python→python3 shim when only python3 is on PATH', (t) => {
    if (process.platform === 'win32') return t.skip('POSIX-only shim');
    const bin = binDir(['python3']);
    const shimDir = ensurePythonShimDir(bin);
    assert.ok(shimDir, 'a shim dir should be returned');
    const shim = path.join(shimDir!, 'python');
    assert.ok(fs.existsSync(shim), 'python shim file exists');
    tmpdirs.push(shimDir!);
    // The shim actually resolves `python` to python3's behavior.
    const out = execFileSync('/bin/sh', ['-c', `PATH=${shimDir}:${bin} python hello.py`], { encoding: 'utf8' }).trim();
    assert.equal(out, 'python3 hello.py', 'running `python` must delegate to python3');
  });

  it('returns null when a real python already exists (no shim needed)', () => {
    const bin = binDir(['python', 'python3']);
    assert.equal(ensurePythonShimDir(bin), null);
  });

  it('returns null when there is no python3 to delegate to', () => {
    const bin = binDir([]); // empty dir — neither python nor python3
    assert.equal(ensurePythonShimDir(bin), null);
  });

  it('returns null for an empty PATH or a PATH with no python3', () => {
    assert.equal(ensurePythonShimDir(''), null);
    _resetPythonShimCache();
    assert.equal(ensurePythonShimDir('/no/such/dir:/also/missing'), null);
  });

  it('memoizes the decision for the process (second call is cached)', () => {
    const bin = binDir(['python3']);
    const first = ensurePythonShimDir(bin);
    tmpdirs.push(first!);
    // A different PATH on the second call is ignored while cached.
    const second = ensurePythonShimDir('/nonexistent');
    assert.equal(second, first);
  });

  it('does not throw and reports no shim when fs materialization is skipped (make=false path)', () => {
    const bin = binDir(['python3']);
    const dir = ensurePythonShimDir(bin, false);
    // make=false still returns a (non-materialized) stable dir name; no python file written.
    assert.ok(dir);
    assert.equal(fs.existsSync(path.join(dir!, 'python')), false);
  });
});
