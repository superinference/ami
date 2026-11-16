import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { FileCache, fsHooks, isFsInaccessible } from '../src/file-cache';
import { fileReadTool } from '../src/tools/file-read';
import { getFileCache } from '../src/file-cache';
import type { ToolContext } from '../src/types';

const MAX = 16 * 1024 * 1024;

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'file-cache-u095-'));
}
function cleanupDir(dir: string): void {
  fs.rmSync(dir, { recursive: true, force: true });
}

afterEach(() => {
  mock.restoreAll();
});

// ---------------------------------------------------------------------------
// U095 gap #1 — mtime-hit read accelerator (getCachedRead) + encoding on FileState
// ---------------------------------------------------------------------------
describe('FileCache — getCachedRead mtime-hit accelerator', () => {
  it('returns cached content + encoding on an mtime hit WITHOUT touching disk', () => {
    // A path that does not exist on disk: a hit that touched disk would throw/miss.
    const cache = new FileCache();
    cache.set('/virtual/accel.txt', 'CACHED-BODY', 4242, 'utf16le');
    const hit = cache.getCachedRead('/virtual/accel.txt', 4242);
    assert.deepEqual(hit, { content: 'CACHED-BODY', encoding: 'utf16le' });
  });

  it('defaults encoding to utf-8 when set() was called without one', () => {
    const cache = new FileCache();
    cache.set('/virtual/def.txt', 'body', 1);
    assert.deepEqual(cache.getCachedRead('/virtual/def.txt', 1), { content: 'body', encoding: 'utf-8' });
  });

  it('returns null on an mtime MISMATCH (file considered changed)', () => {
    const cache = new FileCache();
    cache.set('/virtual/mismatch.txt', 'body', 1000);
    assert.equal(cache.getCachedRead('/virtual/mismatch.txt', 1001), null);
  });

  it('returns null for an uncached path and for a content-less (mtime-only) entry', () => {
    const cache = new FileCache();
    assert.equal(cache.getCachedRead('/virtual/absent.txt', 1), null);
    cache.trackMtime('/virtual/trackonly.txt', 5);
    assert.equal(cache.getCachedRead('/virtual/trackonly.txt', 5), null);
  });

  it('a served hit marks the entry most-recently-used (LRU bump)', () => {
    const cache = new FileCache(MAX, 2);
    cache.set('/v/a.txt', 'a', 1);
    cache.set('/v/b.txt', 'b', 2);
    // Touch a via the accelerator so it is most-recently-used; inserting c evicts b.
    assert.ok(cache.getCachedRead('/v/a.txt', 1));
    cache.set('/v/c.txt', 'c', 3);
    const paths = cache.listPaths();
    assert.ok(paths.includes('/v/a.txt'), 'recently-accessed a survives');
    assert.ok(!paths.includes('/v/b.txt'), 'untouched b was evicted');
  });
});

describe('fileReadTool — mtime-hit accelerator wiring (E2E through the real tool)', () => {
  let tmpDir: string;
  const ctx = (): ToolContext => ({ cwd: tmpDir, abortSignal: new AbortController().signal });

  it('serves an unchanged full re-read from cache WITHOUT a disk content re-read', async () => {
    tmpDir = makeTmpDir();
    try {
      getFileCache(tmpDir).clear();
      const fp = path.join(tmpDir, 'accel.txt');
      fs.writeFileSync(fp, 'line-1\nline-2\n');

      // Count the cache's disk content re-reads (the hash/freshness re-read path).
      const spy = mock.method(fsHooks, 'readFileSync');

      const r1 = await fileReadTool.execute({ file_path: fp }, ctx());
      assert.ok(r1.output.includes('line-1'));

      const r2 = await fileReadTool.execute({ file_path: fp }, ctx());
      assert.ok(r2.output.includes('unchanged'), 'second read is served as unchanged');

      // Accelerator trusts the already-statted mtime: no re-read/re-hash of bytes.
      // (Reverting the wiring to the hash-rechecking get() calls fsHooks.readFileSync.)
      assert.equal(spy.mock.callCount(), 0, 'no disk content re-read for the unchanged re-read');
    } finally {
      cleanupDir(tmpDir);
    }
  });
});

// ---------------------------------------------------------------------------
// U095 gap #5 — classify filesystem errors (isFsInaccessible; rethrow unexpected)
// ---------------------------------------------------------------------------
describe('isFsInaccessible — filesystem error classification', () => {
  it('is true for inaccessible errnos and false for ENOENT / unexpected / non-errors', () => {
    for (const code of ['EACCES', 'EPERM', 'EBUSY', 'EISDIR', 'ELOOP', 'ENOTDIR', 'EIO']) {
      assert.equal(isFsInaccessible(Object.assign(new Error(code), { code })), true, code);
    }
    assert.equal(isFsInaccessible(Object.assign(new Error('gone'), { code: 'ENOENT' })), false);
    assert.equal(isFsInaccessible(Object.assign(new Error('weird'), { code: 'EWEIRD' })), false);
    assert.equal(isFsInaccessible(new Error('no code')), false);
    assert.equal(isFsInaccessible(null), false);
    assert.equal(isFsInaccessible('string'), false);
  });
});

describe('FileCache.hasChanged — FS error classification', () => {
  it('rethrows a genuinely UNEXPECTED fs error instead of swallowing it to "unchanged"', () => {
    const tmpDir = makeTmpDir();
    try {
      const fp = path.join(tmpDir, 'oops.txt');
      fs.writeFileSync(fp, 'body');
      const cache = new FileCache();
      cache.set(fp, 'body', fs.statSync(fp).mtimeMs);
      // Force the freshness re-read to throw an unrecognized error.
      mock.method(fsHooks, 'readFileSync', () => {
        throw Object.assign(new Error('surprise'), { code: 'EWEIRD' });
      });
      assert.throws(() => cache.hasChanged(fp), /surprise/);
    } finally {
      cleanupDir(tmpDir);
    }
  });

  it('keeps the entry (returns false) on an inaccessible fs error, and reports changed on ENOENT', () => {
    const tmpDir = makeTmpDir();
    try {
      const fp = path.join(tmpDir, 'keep.txt');
      fs.writeFileSync(fp, 'body');
      const cache = new FileCache();
      cache.set(fp, 'body', fs.statSync(fp).mtimeMs);

      mock.method(fsHooks, 'readFileSync', () => {
        throw Object.assign(new Error('locked'), { code: 'EBUSY' });
      });
      assert.equal(cache.hasChanged(fp), false, 'inaccessible → keep entry');
      mock.restoreAll();

      // Genuinely deleted → changed.
      fs.unlinkSync(fp);
      assert.equal(cache.hasChanged(fp), true);
    } finally {
      cleanupDir(tmpDir);
    }
  });
});
