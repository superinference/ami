/**
 * U030 — compound-`cd` read-boundary: unit coverage for extractBashReadPathRefs.
 *
 * A relative read token resolved against the static launch cwd lands elsewhere at
 * runtime once a `cd` has moved the shell, so the ref must carry `relative` so the
 * permission layer can re-gate ONLY the cwd-relative paths (absolute tokens are
 * cwd-immune). extractBashReadPaths stays a resolved-paths-only wrapper.
 *
 * Run: cd core/core && node --import tsx --test test/bash-u030-cd-read-boundary.test.ts
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as path from 'node:path';
import { extractBashReadPathRefs, extractBashReadPaths } from '../src/tools/bash';

describe('extractBashReadPathRefs — relativeness tracking', () => {
  it('flags a relative read token as relative and resolves it against cwd', () => {
    const refs = extractBashReadPathRefs('cat secret', '/cwd');
    assert.deepEqual(refs, [{ resolved: '/cwd/secret', relative: true }]);
  });

  it('flags an absolute read token as NOT relative and leaves it untouched', () => {
    const refs = extractBashReadPathRefs('cat /etc/hostname', '/cwd');
    assert.deepEqual(refs, [{ resolved: '/etc/hostname', relative: false }]);
  });

  it('tracks relativeness per token in a compound `cd … && cat …` command', () => {
    const refs = extractBashReadPathRefs('cd ../../other && cat secret', '/cwd');
    // Only the `cat secret` segment yields a read path, and it is relative so a
    // runtime `cd` makes the static-cwd resolution untrustworthy.
    assert.equal(refs.length, 1);
    assert.equal(refs[0].relative, true);
    assert.equal(refs[0].resolved, path.resolve('/cwd', 'secret'));
  });

  it('skips the grep pattern operand but keeps the file operand (relative)', () => {
    const refs = extractBashReadPathRefs('grep -n TODO src/main.ts', '/proj');
    assert.deepEqual(refs, [{ resolved: '/proj/src/main.ts', relative: true }]);
  });

  it('extractBashReadPaths returns exactly the resolved paths of the refs', () => {
    const cmd = 'cat /a.txt b.txt';
    const resolved = extractBashReadPaths(cmd, '/cwd');
    const refs = extractBashReadPathRefs(cmd, '/cwd');
    assert.deepEqual(resolved, refs.map(r => r.resolved));
    assert.deepEqual(resolved, ['/a.txt', '/cwd/b.txt']);
  });
});
