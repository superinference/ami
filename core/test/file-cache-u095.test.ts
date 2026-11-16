import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  FileCache,
  fsHooks,
  isFsInaccessible,
  describeFileChangeMessage,
} from '../src/file-cache';
import type { FileChangeReason } from '../src/file-cache';
import { fileReadTool } from '../src/tools/file-read';
import { fileEditTool } from '../src/tools/file-edit';
import { getFileCache } from '../src/file-cache';
import { normalizePathForComparison, isCaseInsensitiveFs, _pathCompareHooks } from '../src/tools/tool-utils';
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

// ---------------------------------------------------------------------------
// U095 — describeChange(): discriminated freshness reason (drives richer "re-read" msg)
// ---------------------------------------------------------------------------
describe('FileCache.describeChange — reason classification', () => {
  it('reports reason "unchanged" for an uncached path', () => {
    const cache = new FileCache();
    assert.deepEqual(cache.describeChange('/virtual/absent.txt'), { changed: false, reason: 'unchanged' });
  });

  it('reports "unchanged" when the file still matches the cached read', () => {
    const tmpDir = makeTmpDir();
    try {
      const fp = path.join(tmpDir, 'same.txt');
      fs.writeFileSync(fp, 'body');
      const cache = new FileCache();
      cache.set(fp, 'body', fs.statSync(fp).mtimeMs);
      assert.deepEqual(cache.describeChange(fp), { changed: false, reason: 'unchanged' });
      assert.equal(cache.hasChanged(fp), false);
    } finally {
      cleanupDir(tmpDir);
    }
  });

  it('reports reason "mtime" when the modification time moved (user/tool rewrite)', () => {
    const tmpDir = makeTmpDir();
    try {
      const fp = path.join(tmpDir, 'mtime.txt');
      fs.writeFileSync(fp, 'v1');
      const cache = new FileCache();
      // Seed with a stale mtime so the on-disk mtime no longer matches.
      cache.set(fp, 'v1', fs.statSync(fp).mtimeMs - 5000);
      const r = cache.describeChange(fp);
      assert.deepEqual(r, { changed: true, reason: 'mtime' });
      assert.equal(cache.hasChanged(fp), true);
    } finally {
      cleanupDir(tmpDir);
    }
  });

  it('reports reason "content" when mtime is stable but the bytes differ (linter/formatter signature)', () => {
    const tmpDir = makeTmpDir();
    try {
      const fp = path.join(tmpDir, 'content.txt');
      fs.writeFileSync(fp, 'original');
      const cache = new FileCache();
      cache.set(fp, 'original', fs.statSync(fp).mtimeMs);
      // Same mtime (real stat), but the freshness re-read sees DIFFERENT bytes.
      mock.method(fsHooks, 'readFileSync', () => Buffer.from('rewritten-in-place'));
      const r = cache.describeChange(fp);
      assert.deepEqual(r, { changed: true, reason: 'content' });
      assert.equal(cache.hasChanged(fp), true);
    } finally {
      cleanupDir(tmpDir);
    }
  });

  it('reports reason "content" when a mtime-stable file grew past the byte ceiling (no slurp)', () => {
    const tmpDir = makeTmpDir();
    try {
      const fp = path.join(tmpDir, 'grown.txt');
      fs.writeFileSync(fp, 'small');
      // Tiny byte ceiling so the current file size exceeds it.
      const cache = new FileCache(2);
      cache.set(fp, 'x', fs.statSync(fp).mtimeMs);
      // No readFileSync must be needed — treated as changed without hashing.
      const spy = mock.method(fsHooks, 'readFileSync');
      const r = cache.describeChange(fp);
      assert.deepEqual(r, { changed: true, reason: 'content' });
      assert.equal(spy.mock.callCount(), 0, 'over-cap file is not slurped to hash');
    } finally {
      cleanupDir(tmpDir);
    }
  });

  it('reports reason "missing" when the file was deleted (ENOENT)', () => {
    const tmpDir = makeTmpDir();
    try {
      const fp = path.join(tmpDir, 'gone.txt');
      fs.writeFileSync(fp, 'body');
      const cache = new FileCache();
      cache.set(fp, 'body', fs.statSync(fp).mtimeMs);
      fs.unlinkSync(fp);
      assert.deepEqual(cache.describeChange(fp), { changed: true, reason: 'missing' });
      assert.equal(cache.hasChanged(fp), true);
    } finally {
      cleanupDir(tmpDir);
    }
  });

  it('mtime-only entry: reason "mtime" on a whole-ms move, "unchanged" otherwise', () => {
    const tmpDir = makeTmpDir();
    try {
      const fp = path.join(tmpDir, 'trackonly.txt');
      fs.writeFileSync(fp, 'body');
      const cache = new FileCache();
      cache.trackMtime(fp, fs.statSync(fp).mtimeMs);
      assert.deepEqual(cache.describeChange(fp), { changed: false, reason: 'unchanged' });
      cache.trackMtime(fp + '.x', 1); // separate entry whose floored mtime won't match
      fs.writeFileSync(fp + '.x', 'b');
      assert.equal(cache.describeChange(fp + '.x').reason, 'mtime');
    } finally {
      cleanupDir(tmpDir);
    }
  });

  it('inaccessible fs error → reason "unchanged" (keep entry); unexpected error rethrows', () => {
    const tmpDir = makeTmpDir();
    try {
      const fp = path.join(tmpDir, 'locked.txt');
      fs.writeFileSync(fp, 'body');
      const cache = new FileCache();
      cache.set(fp, 'body', fs.statSync(fp).mtimeMs);
      mock.method(fsHooks, 'readFileSync', () => {
        throw Object.assign(new Error('busy'), { code: 'EBUSY' });
      });
      assert.deepEqual(cache.describeChange(fp), { changed: false, reason: 'unchanged' });
      mock.restoreAll();
      mock.method(fsHooks, 'readFileSync', () => {
        throw Object.assign(new Error('weird'), { code: 'EWEIRD' });
      });
      assert.throws(() => cache.describeChange(fp), /weird/);
    } finally {
      cleanupDir(tmpDir);
    }
  });
});

describe('describeFileChangeMessage — reason-aware wording', () => {
  it('names a linter/formatter for "content" and includes the action verb', () => {
    const msg = describeFileChangeMessage('content', 'editing');
    assert.match(msg, /linter or formatter/);
    assert.match(msg, /editing/);
  });
  it('names user/another tool for "mtime"', () => {
    assert.match(describeFileChangeMessage('mtime', 'overwriting'), /either by you or by another tool/);
  });
  it('names deletion for "missing"', () => {
    assert.match(describeFileChangeMessage('missing', 'editing'), /no longer exists/);
  });
  it('falls back to the mtime wording for the residual "unchanged" reason', () => {
    const reason: FileChangeReason = 'unchanged';
    assert.match(describeFileChangeMessage(reason, 'editing'), /modified since you last read it/);
  });
});

describe('fileEditTool — emits the reason-aware "re-read" message (E2E through the real tool)', () => {
  it('a mtime-changed file yields the richer "either by you or by another tool" wording', async () => {
    const tmpDir = makeTmpDir();
    const ctx = (): ToolContext => ({ cwd: tmpDir, abortSignal: new AbortController().signal });
    try {
      getFileCache(tmpDir).clear();
      const fp = path.join(tmpDir, 'edit.txt');
      fs.writeFileSync(fp, 'current-body\n');
      // Seed the cache with a STALE mtime so describeChange classifies the edit as mtime-stale.
      // (No context.filesRead, so the edit path does not auto-repopulate the entry.)
      getFileCache(tmpDir).set(fp, 'current-body\n', fs.statSync(fp).mtimeMs - 10000);

      const r = await fileEditTool.execute(
        { file_path: fp, old_string: 'current-body', new_string: 'next-body' },
        ctx(),
      );
      assert.equal(r.isError, true);
      // Reason-aware wording (reverting the describeChange wiring → flat message lacking this).
      assert.match(r.output, /either by you or by another tool/);
    } finally {
      getFileCache(tmpDir).clear();
      cleanupDir(tmpDir);
    }
  });
});

// ---------------------------------------------------------------------------
// U095 — case-insensitive-filesystem-aware path keying (normalizePathForComparison)
// ---------------------------------------------------------------------------
describe('normalizePathForComparison — platform-aware canonicalization', () => {
  afterEach(() => { _pathCompareHooks.platform = process.platform; });

  it('preserves case on a case-SENSITIVE filesystem (linux)', () => {
    _pathCompareHooks.platform = 'linux';
    assert.equal(isCaseInsensitiveFs(), false);
    assert.equal(normalizePathForComparison('/Foo/../Foo/Bar.TS'), '/Foo/Bar.TS');
  });

  it('lowercases + unifies separators on a case-INSENSITIVE filesystem (darwin)', () => {
    _pathCompareHooks.platform = 'darwin';
    assert.equal(isCaseInsensitiveFs(), true);
    assert.equal(normalizePathForComparison('/Repo/Src/A.TS'), '/repo/src/a.ts');
  });

  it('folds backslash separators on windows', () => {
    _pathCompareHooks.platform = 'win32';
    assert.equal(normalizePathForComparison('C:\\Repo\\A.TS'), 'c:/repo/a.ts');
  });
});

describe('FileCache — case-insensitive keying collapses letter-case aliases to one entry', () => {
  afterEach(() => { _pathCompareHooks.platform = process.platform; });

  it('on a case-insensitive FS, a read under one case is found under another case', () => {
    _pathCompareHooks.platform = 'darwin';
    const cache = new FileCache();
    cache.set('/Repo/Src/A.ts', 'BODY', 123);
    // Different letter-case spelling of the SAME file resolves to the same entry.
    const hit = cache.getCachedRead('/repo/src/a.ts', 123);
    assert.deepEqual(hit, { content: 'BODY', encoding: 'utf-8' });
    assert.equal(cache.getStats().files, 1, 'aliases collapsed to one entry');
  });

  it('on a case-sensitive FS, letter-case spellings stay distinct entries', () => {
    _pathCompareHooks.platform = 'linux';
    const cache = new FileCache();
    cache.set('/repo/A.ts', 'A', 1);
    cache.set('/repo/a.ts', 'a', 2);
    assert.equal(cache.getStats().files, 2);
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
