import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as os from 'node:os';
import * as path from 'node:path';
import { pathsOverlap, _pathHooks } from '../src/utils/path';

/**
 * U081 — `pathsOverlap` symmetric, root- and case-aware path-overlap primitive
 * (core/core/src/utils/path.ts). Mirrors the reference's
 * `utils/plugins/orphanedPluginFilter.ts::pathsOverlap`: two paths overlap when
 * one is the same as, or nested inside, the other (in EITHER direction), with a
 * filesystem root overlapping everything and a case-fold on case-insensitive
 * filesystems.
 */
describe('pathsOverlap (U081)', () => {
  const origPlatform = _pathHooks.platform;
  afterEach(() => { _pathHooks.platform = origPlatform; });

  it('is true for identical paths', () => {
    assert.equal(pathsOverlap('/a/b/c', '/a/b/c'), true);
  });

  it('is true when the first path is nested inside the second', () => {
    assert.equal(pathsOverlap('/a/b/c', '/a/b'), true);
  });

  it('is true when the second path is nested inside the first (symmetric)', () => {
    assert.equal(pathsOverlap('/a/b', '/a/b/c'), true);
  });

  it('is false for disjoint sibling trees', () => {
    assert.equal(pathsOverlap('/a/b', '/a/c'), false);
  });

  it('is boundary-safe: /a/bc does NOT overlap /a/b (no shared subtree)', () => {
    assert.equal(pathsOverlap('/a/bc', '/a/b'), false);
    assert.equal(pathsOverlap('/a/b', '/a/bc'), false);
  });

  it('treats the filesystem root as overlapping any absolute path (either side)', () => {
    assert.equal(pathsOverlap('/', '/anything/deep'), true);
    assert.equal(pathsOverlap('/anything/deep', '/'), true);
  });

  it('tolerates a trailing separator on either operand', () => {
    assert.equal(pathsOverlap('/a/b/', '/a/b'), true);
    assert.equal(pathsOverlap('/a/b', '/a/b/'), true);
  });

  it('expands relative inputs against the provided cwd', () => {
    assert.equal(pathsOverlap('sub', '.', '/root'), true); // /root/sub overlaps /root
    assert.equal(pathsOverlap('sub', 'other', '/root'), false); // /root/sub vs /root/other
  });

  it('expands ~ against the home directory', () => {
    const home = os.homedir();
    assert.equal(pathsOverlap('~', path.join(home, 'x')), true);
  });

  it('is case-sensitive on linux (no fold): /A/B does NOT overlap /a/b', () => {
    _pathHooks.platform = 'linux';
    assert.equal(pathsOverlap('/A/B', '/a/b'), false);
  });

  it('folds case on darwin: /Users/Foo overlaps /users/foo/bar', () => {
    _pathHooks.platform = 'darwin';
    assert.equal(pathsOverlap('/Users/Foo', '/users/foo/bar'), true);
  });

  it('folds case on win32 (drive-letter casing subsumed by full lowercase)', () => {
    _pathHooks.platform = 'win32';
    // Compared on a POSIX host path.sep is '/', so exercise POSIX-shaped paths;
    // the win32 arm only changes the case fold, which is what we assert here.
    assert.equal(pathsOverlap('/Repo/Plugins', '/repo/plugins/cache'), true);
  });
});
