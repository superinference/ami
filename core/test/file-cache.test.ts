import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { FileCache, getFileCache, MAX_CACHE_BYTES, DEFAULT_MAX_ENTRIES, MAX_CONTENT_HASH_SIZE } from '../src/file-cache';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'file-cache-test-'));
}

function cleanupDir(dir: string): void {
  fs.rmSync(dir, { recursive: true, force: true });
}

// ---------------------------------------------------------------------------
// set / get
// ---------------------------------------------------------------------------
describe('FileCache – set/get', () => {
  it('set() stores file state', () => {
    const tmpDir = makeTmpDir();
    try {
      const filePath = path.join(tmpDir, 'test.txt');
      fs.writeFileSync(filePath, 'hello world');
      const stat = fs.statSync(filePath);

      const cache = new FileCache();
      cache.set(filePath, 'hello world', stat.mtimeMs);

      const cached = cache.get(filePath);
      assert.ok(cached);
      assert.equal(cached!.content, 'hello world');
      assert.equal(cached!.path, filePath);
      assert.equal(cached!.mtime, stat.mtimeMs);
      assert.equal(cached!.tokenEstimate, Math.ceil('hello world'.length / 4));
    } finally {
      cleanupDir(tmpDir);
    }
  });

  it('get() retrieves stored state', () => {
    const tmpDir = makeTmpDir();
    try {
      const filePath = path.join(tmpDir, 'data.txt');
      const content = 'some content here';
      fs.writeFileSync(filePath, content);
      const stat = fs.statSync(filePath);

      const cache = new FileCache();
      cache.set(filePath, content, stat.mtimeMs);

      const result = cache.get(filePath);
      assert.ok(result);
      assert.equal(result!.content, content);
    } finally {
      cleanupDir(tmpDir);
    }
  });

  it('get() returns null for uncached file', () => {
    const cache = new FileCache();
    const result = cache.get('/nonexistent/path.txt');
    assert.equal(result, null);
  });
});

// ---------------------------------------------------------------------------
// hasChanged
// ---------------------------------------------------------------------------
describe('FileCache – hasChanged', () => {
  it('returns false for unchanged file', () => {
    const tmpDir = makeTmpDir();
    try {
      const filePath = path.join(tmpDir, 'stable.txt');
      fs.writeFileSync(filePath, 'stable');
      const stat = fs.statSync(filePath);

      const cache = new FileCache();
      cache.set(filePath, 'stable', stat.mtimeMs);

      assert.equal(cache.hasChanged(filePath), false);
    } finally {
      cleanupDir(tmpDir);
    }
  });

  it('returns true when mtime differs AND content differs', () => {
    const tmpDir = makeTmpDir();
    try {
      const filePath = path.join(tmpDir, 'changing.txt');
      fs.writeFileSync(filePath, 'v1');
      const stat = fs.statSync(filePath);

      const cache = new FileCache();
      cache.set(filePath, 'v1', stat.mtimeMs);

      // Genuinely change the file's content AND its mtime.
      fs.writeFileSync(filePath, 'v2-different');
      const newMtime = stat.mtimeMs + 1000;
      fs.utimesSync(filePath, stat.atimeMs / 1000, newMtime / 1000);

      assert.equal(cache.hasChanged(filePath), true);
    } finally {
      cleanupDir(tmpDir);
    }
  });

  it('returns false for file not in cache', () => {
    const cache = new FileCache();
    assert.equal(cache.hasChanged('/not/cached.txt'), false);
  });

  it('returns true when file is deleted', () => {
    const tmpDir = makeTmpDir();
    try {
      const filePath = path.join(tmpDir, 'ephemeral.txt');
      fs.writeFileSync(filePath, 'gone soon');
      const stat = fs.statSync(filePath);

      const cache = new FileCache();
      cache.set(filePath, 'gone soon', stat.mtimeMs);

      fs.unlinkSync(filePath);
      assert.equal(cache.hasChanged(filePath), true);
    } finally {
      cleanupDir(tmpDir);
    }
  });
});

// ---------------------------------------------------------------------------
// clear
// ---------------------------------------------------------------------------
describe('FileCache – clear', () => {
  it('removes all entries', () => {
    const cache = new FileCache();
    const tmpDir = makeTmpDir();
    try {
      const f1 = path.join(tmpDir, 'a.txt');
      const f2 = path.join(tmpDir, 'b.txt');
      fs.writeFileSync(f1, 'a');
      fs.writeFileSync(f2, 'b');
      const s1 = fs.statSync(f1);
      const s2 = fs.statSync(f2);

      cache.set(f1, 'a', s1.mtimeMs);
      cache.set(f2, 'b', s2.mtimeMs);
      assert.equal(cache.getStats().files, 2);

      cache.clear();
      assert.equal(cache.getStats().files, 0);
      assert.equal(cache.get(f1), null);
      assert.equal(cache.get(f2), null);
    } finally {
      cleanupDir(tmpDir);
    }
  });
});

// ---------------------------------------------------------------------------
// getTotalTokens
// ---------------------------------------------------------------------------
describe('FileCache – getTotalTokens', () => {
  it('sums all cached token estimates', () => {
    const tmpDir = makeTmpDir();
    try {
      const f1 = path.join(tmpDir, 'a.txt');
      const f2 = path.join(tmpDir, 'b.txt');
      const c1 = 'a'.repeat(40);  // 40 chars = 10 tokens
      const c2 = 'b'.repeat(80);  // 80 chars = 20 tokens
      fs.writeFileSync(f1, c1);
      fs.writeFileSync(f2, c2);
      const s1 = fs.statSync(f1);
      const s2 = fs.statSync(f2);

      const cache = new FileCache();
      cache.set(f1, c1, s1.mtimeMs);
      cache.set(f2, c2, s2.mtimeMs);

      assert.equal(cache.getTotalTokens(), 30);
    } finally {
      cleanupDir(tmpDir);
    }
  });

  it('returns 0 for empty cache', () => {
    const cache = new FileCache();
    assert.equal(cache.getTotalTokens(), 0);
  });
});

// ---------------------------------------------------------------------------
// getStats
// ---------------------------------------------------------------------------
describe('FileCache – getStats', () => {
  it('returns file count and total tokens', () => {
    const tmpDir = makeTmpDir();
    try {
      const f1 = path.join(tmpDir, 'x.txt');
      const content = 'x'.repeat(20); // 20 chars = 5 tokens
      fs.writeFileSync(f1, content);
      const stat = fs.statSync(f1);

      const cache = new FileCache();
      cache.set(f1, content, stat.mtimeMs);

      const stats = cache.getStats();
      assert.equal(stats.files, 1);
      assert.equal(stats.tokens, 5);
    } finally {
      cleanupDir(tmpDir);
    }
  });
});

// ---------------------------------------------------------------------------
// getFileCache singleton
// ---------------------------------------------------------------------------
describe('getFileCache – singleton', () => {
  it('returns same instance for same cwd', () => {
    const a = getFileCache('/test/singleton/path');
    const b = getFileCache('/test/singleton/path');
    assert.equal(a, b);
  });

  it('returns different instance for different cwd', () => {
    const a = getFileCache('/test/singleton/alpha');
    const b = getFileCache('/test/singleton/beta');
    assert.notEqual(a, b);
  });
});

// ---------------------------------------------------------------------------
// trackMtime
// ---------------------------------------------------------------------------
describe('FileCache – trackMtime', () => {
  it('enables hasChanged() without caching content', () => {
    const tmpDir = makeTmpDir();
    try {
      const filePath = path.join(tmpDir, 'tracked.txt');
      fs.writeFileSync(filePath, 'hello');
      const stat = fs.statSync(filePath);

      const cache = new FileCache();
      cache.trackMtime(filePath, stat.mtimeMs);

      assert.equal(cache.hasChanged(filePath), false);
      assert.equal(cache.get(filePath), null);
    } finally {
      cleanupDir(tmpDir);
    }
  });

  it('detects changes after mtime-only tracking', () => {
    const tmpDir = makeTmpDir();
    try {
      const filePath = path.join(tmpDir, 'tracked2.txt');
      fs.writeFileSync(filePath, 'v1');
      const stat = fs.statSync(filePath);

      const cache = new FileCache();
      cache.trackMtime(filePath, stat.mtimeMs);

      const newMtime = stat.mtimeMs + 1000;
      fs.utimesSync(filePath, stat.atimeMs / 1000, newMtime / 1000);

      assert.equal(cache.hasChanged(filePath), true);
    } finally {
      cleanupDir(tmpDir);
    }
  });

  it('does not overwrite existing full-content entry', () => {
    const tmpDir = makeTmpDir();
    try {
      const filePath = path.join(tmpDir, 'full.txt');
      fs.writeFileSync(filePath, 'full content');
      const stat = fs.statSync(filePath);

      const cache = new FileCache();
      cache.set(filePath, 'full content', stat.mtimeMs);
      cache.trackMtime(filePath, stat.mtimeMs + 9999);

      const cached = cache.get(filePath);
      assert.ok(cached);
      assert.equal(cached!.content, 'full content');
    } finally {
      cleanupDir(tmpDir);
    }
  });
});

// ---------------------------------------------------------------------------
// delete
// ---------------------------------------------------------------------------
describe('FileCache – delete', () => {
  it('removes a cached entry', () => {
    const tmpDir = makeTmpDir();
    try {
      const filePath = path.join(tmpDir, 'del.txt');
      fs.writeFileSync(filePath, 'content');
      const stat = fs.statSync(filePath);

      const cache = new FileCache();
      cache.set(filePath, 'content', stat.mtimeMs);
      assert.ok(cache.get(filePath));

      cache.delete(filePath);
      assert.equal(cache.get(filePath), null);
      assert.equal(cache.getStats().files, 0);
    } finally {
      cleanupDir(tmpDir);
    }
  });
});

// ---------------------------------------------------------------------------
// Integration: real file lifecycle
// ---------------------------------------------------------------------------
describe('FileCache – integration with real files', () => {
  let tmpDir: string;

  afterEach(() => {
    if (tmpDir) cleanupDir(tmpDir);
  });

  it('create temp file, cache it, modify it, verify hasChanged', () => {
    tmpDir = makeTmpDir();
    const filePath = path.join(tmpDir, 'lifecycle.txt');

    // Create file
    fs.writeFileSync(filePath, 'initial content');
    const stat1 = fs.statSync(filePath);

    // Cache it
    const cache = new FileCache();
    cache.set(filePath, 'initial content', stat1.mtimeMs);

    // Verify not changed
    assert.equal(cache.hasChanged(filePath), false);
    assert.ok(cache.get(filePath));
    assert.equal(cache.get(filePath)!.content, 'initial content');

    // Modify it (set a new mtime to guarantee the change is detected)
    fs.writeFileSync(filePath, 'modified content');
    const newMtime = stat1.mtimeMs + 2000;
    fs.utimesSync(filePath, Date.now() / 1000, newMtime / 1000);

    // Verify changed
    assert.equal(cache.hasChanged(filePath), true);
    // get() should return null for changed file
    assert.equal(cache.get(filePath), null);
  });
});

// ---------------------------------------------------------------------------
// Content-hash fallback (mtime same, content changed)
// ---------------------------------------------------------------------------
describe('FileCache – content hash fallback', () => {
  it('detects content change even when mtime stays the same', () => {
    const tmpDir = makeTmpDir();
    try {
      const filePath = path.join(tmpDir, 'same-mtime.txt');
      fs.writeFileSync(filePath, 'version 1');
      const stat = fs.statSync(filePath);

      const cache = new FileCache();
      cache.set(filePath, 'version 1', stat.mtimeMs);
      assert.equal(cache.hasChanged(filePath), false);

      // Write different content but reset mtime to the same value
      fs.writeFileSync(filePath, 'version 2');
      fs.utimesSync(filePath, stat.atimeMs / 1000, stat.mtimeMs / 1000);

      // mtime matches, but content differs — hash fallback should catch it
      assert.equal(cache.hasChanged(filePath), true);
    } finally {
      cleanupDir(tmpDir);
    }
  });

  it('returns false when content is identical (hash matches)', () => {
    const tmpDir = makeTmpDir();
    try {
      const filePath = path.join(tmpDir, 'identical.txt');
      fs.writeFileSync(filePath, 'unchanged');
      const stat = fs.statSync(filePath);

      const cache = new FileCache();
      cache.set(filePath, 'unchanged', stat.mtimeMs);

      // File untouched — mtime matches, content matches
      assert.equal(cache.hasChanged(filePath), false);
    } finally {
      cleanupDir(tmpDir);
    }
  });

  it('skips hash check for mtime-only entries (no content cached)', () => {
    const tmpDir = makeTmpDir();
    try {
      const filePath = path.join(tmpDir, 'mtime-only.txt');
      fs.writeFileSync(filePath, 'tracked');
      const stat = fs.statSync(filePath);

      const cache = new FileCache();
      cache.trackMtime(filePath, stat.mtimeMs);

      // File untouched — mtime matches, no content hash to compare
      assert.equal(cache.hasChanged(filePath), false);

      // Now change content AND mtime — should detect via mtime
      fs.writeFileSync(filePath, 'changed');
      const newMtime = stat.mtimeMs + 1000;
      fs.utimesSync(filePath, stat.atimeMs / 1000, newMtime / 1000);
      assert.equal(cache.hasChanged(filePath), true);
    } finally {
      cleanupDir(tmpDir);
    }
  });

  it('contentHash is stored in FileState', () => {
    const tmpDir = makeTmpDir();
    try {
      const filePath = path.join(tmpDir, 'hash.txt');
      fs.writeFileSync(filePath, 'test content');
      const stat = fs.statSync(filePath);

      const cache = new FileCache();
      cache.set(filePath, 'test content', stat.mtimeMs);

      const cached = cache.get(filePath);
      assert.ok(cached);
      assert.ok(cached!.contentHash);
      assert.equal(cached!.contentHash.length, 64); // SHA-256 hex
    } finally {
      cleanupDir(tmpDir);
    }
  });
});

describe('FileCache – setWritten', () => {
  it('caches content after a real write so get() succeeds', () => {
    const tmpDir = makeTmpDir();
    try {
      const filePath = path.join(tmpDir, 'written.txt');
      fs.writeFileSync(filePath, 'on disk');
      const cache = new FileCache();
      cache.setWritten(filePath, 'on disk');
      const cached = cache.get(filePath);
      assert.ok(cached);
      assert.equal(cached!.content, 'on disk');
    } finally {
      cleanupDir(tmpDir);
    }
  });

  it('does not throw when stat fails after the write', () => {
    const cache = new FileCache();
    assert.doesNotThrow(() => cache.setWritten('/no/such/file/for-stat.txt', 'wrote'));
    // A fake mtime must not be stored — hasChanged() on an uncached path is false,
    // so a later file_edit would not spuriously claim the file was modified.
    assert.equal(cache.hasChanged('/no/such/file/for-stat.txt'), false);
    assert.equal(cache.get('/no/such/file/for-stat.txt'), null);
  });
});

// ---------------------------------------------------------------------------
// totalBytes tracking
// ---------------------------------------------------------------------------

describe('FileCache — totalBytes tracking', () => {
  it('tracks bytes on set', () => {
    const cache = new FileCache();
    cache.set('/a.txt', 'hello', 1);
    assert.equal(cache.getTotalBytes(), Buffer.byteLength('hello', 'utf-8'));
  });

  it('accumulates bytes across entries', () => {
    const cache = new FileCache();
    cache.set('/a.txt', 'hello', 1);
    cache.set('/b.txt', 'world!', 2);
    assert.equal(cache.getTotalBytes(), Buffer.byteLength('hello') + Buffer.byteLength('world!'));
  });

  it('adjusts bytes when updating existing entry', () => {
    const cache = new FileCache();
    cache.set('/a.txt', 'short', 1);
    const before = cache.getTotalBytes();
    cache.set('/a.txt', 'a much longer string now', 2);
    assert.equal(cache.getTotalBytes(), Buffer.byteLength('a much longer string now'));
    assert.notEqual(cache.getTotalBytes(), before);
  });

  it('subtracts bytes on delete', () => {
    const cache = new FileCache();
    cache.set('/a.txt', 'hello', 1);
    cache.set('/b.txt', 'world', 2);
    cache.delete('/a.txt');
    assert.equal(cache.getTotalBytes(), Buffer.byteLength('world'));
  });

  it('resets bytes on clear', () => {
    const cache = new FileCache();
    cache.set('/a.txt', 'hello', 1);
    cache.clear();
    assert.equal(cache.getTotalBytes(), 0);
  });

  it('includes bytes in getStats', () => {
    const cache = new FileCache();
    cache.set('/a.txt', 'hello', 1);
    const stats = cache.getStats();
    assert.equal(stats.bytes, Buffer.byteLength('hello'));
  });
});

// ---------------------------------------------------------------------------
// 16 MB eviction
// ---------------------------------------------------------------------------

describe('FileCache — 16 MB eviction', () => {
  it('MAX_CACHE_BYTES is 16 MB', () => {
    assert.equal(MAX_CACHE_BYTES, 16 * 1024 * 1024);
  });

  it('evicts oldest entry when exceeding limit', () => {
    const cache = new FileCache();
    const chunk = 'x'.repeat(MAX_CACHE_BYTES - 100);
    cache.set('/old.txt', chunk, 1);
    cache.set('/new.txt', 'y'.repeat(200), 2);
    assert.equal(cache.getStats().files, 1);
    assert.ok(cache.getTotalBytes() <= MAX_CACHE_BYTES);
  });

  it('does not evict when under limit', () => {
    const cache = new FileCache();
    cache.set('/a.txt', 'hello', 1);
    cache.set('/b.txt', 'world', 2);
    assert.equal(cache.getStats().files, 2);
  });
});

// ---------------------------------------------------------------------------
// Configurable byte ceiling (maxSizeBytes constructor param)  [gaps 30/31]
// ---------------------------------------------------------------------------
describe('FileCache — configurable maxSizeBytes', () => {
  it('evicts using the constructor byte ceiling, not the 16 MB default', () => {
    // With a 100-byte ceiling the second entry must evict the first.
    // On the pre-change code the constructor arg is ignored (16 MB default)
    // and both entries survive — this asserts the override takes effect.
    const cache = new FileCache(100);
    cache.set('/old.txt', 'x'.repeat(80), 1);
    cache.set('/new.txt', 'y'.repeat(80), 2);
    assert.equal(cache.getStats().files, 1);
    assert.ok(cache.getTotalBytes() <= 100);
    assert.ok(cache.getStats().entries.includes('/new.txt'));
    assert.ok(!cache.getStats().entries.includes('/old.txt'));
  });
});

// ---------------------------------------------------------------------------
// Entry-count ceiling (maxEntries constructor param)  [gaps 27/29/31]
// ---------------------------------------------------------------------------
describe('FileCache — entry-count ceiling', () => {
  it('DEFAULT_MAX_ENTRIES is exported and positive', () => {
    assert.ok(DEFAULT_MAX_ENTRIES > 0);
  });

  it('evicts by entry count even when far under the byte ceiling', () => {
    // Tiny entries never trip the byte budget; without an entry cap the count
    // would grow unbounded. Cap at 2 and add 3 → one eviction.
    const cache = new FileCache(MAX_CACHE_BYTES, 2);
    cache.set('/a.txt', 'a', 1);
    cache.set('/b.txt', 'b', 2);
    cache.set('/c.txt', 'c', 3);
    assert.equal(cache.getStats().files, 2);
    assert.ok(!cache.getStats().entries.includes('/a.txt')); // oldest evicted
    assert.ok(cache.getStats().entries.includes('/c.txt'));
  });

  it('mtime-only (trackMtime) entries also honor the entry cap', () => {
    const cache = new FileCache(MAX_CACHE_BYTES, 2);
    cache.trackMtime('/a.txt', 1);
    cache.trackMtime('/b.txt', 2);
    cache.trackMtime('/c.txt', 3);
    assert.equal(cache.getStats().files, 2);
  });

  it('partial-view entries also honor the entry cap', () => {
    const cache = new FileCache(MAX_CACHE_BYTES, 2);
    cache.markPartialView('/a.txt', 1);
    cache.markPartialView('/b.txt', 2);
    cache.markPartialView('/c.txt', 3);
    assert.equal(cache.getStats().files, 2);
    assert.equal(cache.isPartialView('/c.txt'), true);
  });
});

// ---------------------------------------------------------------------------
// True LRU eviction (recency-of-access, not insertion-order FIFO)  [gap 15]
// ---------------------------------------------------------------------------
describe('FileCache — LRU eviction', () => {
  it('evicts the least-recently-USED entry, keeping a re-read hot file', () => {
    const tmpDir = makeTmpDir();
    try {
      const fa = path.join(tmpDir, 'a.txt');
      const fb = path.join(tmpDir, 'b.txt');
      const fc = path.join(tmpDir, 'c.txt');
      fs.writeFileSync(fa, 'A');
      fs.writeFileSync(fb, 'B');
      fs.writeFileSync(fc, 'C');

      const cache = new FileCache(MAX_CACHE_BYTES, 2);
      cache.set(fa, 'A', fs.statSync(fa).mtimeMs);
      cache.set(fb, 'B', fs.statSync(fb).mtimeMs);

      // Access A so it becomes most-recently-used. Under FIFO A (inserted first)
      // would be evicted next; under LRU, B becomes the eviction victim.
      assert.ok(cache.get(fa));

      cache.set(fc, 'C', fs.statSync(fc).mtimeMs);

      assert.equal(cache.getStats().files, 2);
      assert.ok(cache.get(fa), 'hot (recently-read) file must survive LRU');
      assert.equal(cache.get(fb), null, 'least-recently-used file is evicted');
      assert.ok(cache.get(fc));
    } finally {
      cleanupDir(tmpDir);
    }
  });

  it('updating an existing entry refreshes its recency', () => {
    const cache = new FileCache(MAX_CACHE_BYTES, 2);
    cache.set('/a.txt', 'a', 1);
    cache.set('/b.txt', 'b', 2);
    cache.set('/a.txt', 'a2', 3); // re-set A → A is now most-recently-used
    cache.set('/c.txt', 'c', 4);  // evicts the LRU, which is now B
    assert.ok(cache.getStats().entries.includes('/a.txt'));
    assert.ok(!cache.getStats().entries.includes('/b.txt'));
  });
});

// ---------------------------------------------------------------------------
// Bounded freshness check: skip hashing files over the byte ceiling  [gap 20]
// ---------------------------------------------------------------------------
describe('FileCache — bounded freshness check', () => {
  it('treats an over-ceiling file as changed without reading it to hash', () => {
    const tmpDir = makeTmpDir();
    try {
      const filePath = path.join(tmpDir, 'grows.txt');
      fs.writeFileSync(filePath, 'x'.repeat(50));
      const stat = fs.statSync(filePath);

      // Byte ceiling of 10; the 50-byte file exceeds it. With a matching mtime
      // the pre-change code would readFileSync + hash and report unchanged
      // (false); the size guard reports changed (true) without the unbounded read.
      const cache = new FileCache(10);
      cache.set(filePath, 'x'.repeat(50), stat.mtimeMs);
      assert.equal(cache.hasChanged(filePath), true);
    } finally {
      cleanupDir(tmpDir);
    }
  });
});

// ---------------------------------------------------------------------------
// Transient vs permanent FS error in change detection  [gaps 21/36]
// ---------------------------------------------------------------------------
describe('FileCache — FS-error classification in hasChanged', () => {
  it('reports changed for a genuinely deleted file (ENOENT)', () => {
    const tmpDir = makeTmpDir();
    try {
      const filePath = path.join(tmpDir, 'deleted.txt');
      fs.writeFileSync(filePath, 'bye');
      const cache = new FileCache();
      cache.set(filePath, 'bye', fs.statSync(filePath).mtimeMs);
      fs.unlinkSync(filePath);
      assert.equal(cache.hasChanged(filePath), true);
    } finally {
      cleanupDir(tmpDir);
    }
  });

  it('keeps the cached entry on a transient (non-ENOENT) read error', () => {
    const tmpDir = makeTmpDir();
    try {
      // Cache a DIRECTORY path with content + its real mtime. statSync succeeds
      // (mtime matches) but readFileSync throws EISDIR — a non-ENOENT error.
      // Pre-change code's blanket `catch { return true }` would report changed;
      // the classification keeps the entry (returns false) on transient errors.
      const dirPath = path.join(tmpDir, 'adir');
      fs.mkdirSync(dirPath);
      const stat = fs.statSync(dirPath);
      const cache = new FileCache();
      cache.set(dirPath, 'pretend-content', stat.mtimeMs);
      assert.equal(cache.hasChanged(dirPath), false);
    } finally {
      cleanupDir(tmpDir);
    }
  });
});

// ---------------------------------------------------------------------------
// Path-key normalization on every access  [gaps 11/28/32/35]
// ---------------------------------------------------------------------------
describe('FileCache — path-key normalization', () => {
  it('collapses aliased spellings of the same path to one entry', () => {
    const tmpDir = makeTmpDir();
    try {
      const filePath = path.join(tmpDir, 'norm.txt');
      fs.writeFileSync(filePath, 'canonical');
      const stat = fs.statSync(filePath);

      const cache = new FileCache();
      // Store under the canonical path.
      cache.set(filePath, 'canonical', stat.mtimeMs);
      // Read via an aliased spelling with a redundant /../ segment. Built by
      // string concat (path.join would pre-normalize) so it stays non-canonical.
      const alias = path.join(tmpDir, 'sub') + path.sep + '..' + path.sep + 'norm.txt';
      assert.notEqual(alias, filePath); // genuinely different spelling
      assert.equal(path.normalize(alias), filePath); // ...of the same file
      const hit = cache.get(alias);
      assert.ok(hit, 'aliased spelling must hit the same cache entry');
      assert.equal(hit!.content, 'canonical');
      // Still a single entry, keyed canonically.
      assert.equal(cache.getStats().files, 1);
      assert.ok(cache.getStats().entries.includes(filePath));
      // hasChanged and delete also normalize.
      assert.equal(cache.hasChanged(alias), false);
      cache.delete(alias);
      assert.equal(cache.getStats().files, 0);
    } finally {
      cleanupDir(tmpDir);
    }
  });
});

// ---------------------------------------------------------------------------
// getStats().entries lists cached path keys  [gaps 25/26]
// ---------------------------------------------------------------------------
describe('FileCache — getStats entries list', () => {
  it('enumerates the cached path keys', () => {
    const cache = new FileCache();
    cache.set('/x.txt', 'x', 1);
    cache.trackMtime('/y.txt', 2);
    const stats = cache.getStats();
    assert.ok(Array.isArray(stats.entries));
    assert.equal(stats.entries.length, 2);
    assert.ok(stats.entries.includes('/x.txt'));
    assert.ok(stats.entries.includes('/y.txt'));
  });

  it('entries matches listPaths()', () => {
    const cache = new FileCache();
    cache.set('/a.txt', 'a', 1);
    cache.set('/b.txt', 'b', 2);
    assert.deepEqual(cache.getStats().entries, cache.listPaths());
  });
});

// ---------------------------------------------------------------------------
// Spurious "modified" suppression: mtime moved but bytes are identical
// (atomic re-save / touch / git-checkout-to-identical-content)  [gap: avoid
// spurious 'file modified since read' when only the timestamp changed]
// ---------------------------------------------------------------------------
describe('FileCache — identical-content mtime change is NOT a change', () => {
  it('returns false when only the mtime moved but the bytes are identical', () => {
    const tmpDir = makeTmpDir();
    try {
      const filePath = path.join(tmpDir, 'touched.txt');
      fs.writeFileSync(filePath, 'identical bytes');
      const stat = fs.statSync(filePath);

      const cache = new FileCache();
      cache.set(filePath, 'identical bytes', stat.mtimeMs);

      // Re-write the SAME content and bump the mtime (simulates an atomic
      // save / `touch` / a linter that rewrote identical bytes).
      fs.writeFileSync(filePath, 'identical bytes');
      const newMtime = stat.mtimeMs + 5000;
      fs.utimesSync(filePath, stat.atimeMs / 1000, newMtime / 1000);

      // The content hash is authoritative: identical bytes → not changed,
      // even though the mtime moved. Pre-change code returned true here.
      assert.equal(cache.hasChanged(filePath), false);
      // get() still serves the cached content (not invalidated).
      assert.ok(cache.get(filePath));
      assert.equal(cache.get(filePath)!.content, 'identical bytes');
    } finally {
      cleanupDir(tmpDir);
    }
  });

  it('still reports changed when both mtime and content differ', () => {
    const tmpDir = makeTmpDir();
    try {
      const filePath = path.join(tmpDir, 'realchange.txt');
      fs.writeFileSync(filePath, 'before');
      const stat = fs.statSync(filePath);

      const cache = new FileCache();
      cache.set(filePath, 'before', stat.mtimeMs);

      fs.writeFileSync(filePath, 'after - genuinely different');
      fs.utimesSync(filePath, stat.atimeMs / 1000, (stat.mtimeMs + 5000) / 1000);

      assert.equal(cache.hasChanged(filePath), true);
    } finally {
      cleanupDir(tmpDir);
    }
  });
});

// ---------------------------------------------------------------------------
// Floor mtimeMs to suppress sub-millisecond / watcher-touch false positives
// [gap: floor mtimeMs for change detection]
// ---------------------------------------------------------------------------
describe('FileCache — floored mtime comparison for mtime-only entries', () => {
  it('a sub-millisecond mtime jitter on an mtime-only entry is not a change', () => {
    const tmpDir = makeTmpDir();
    try {
      const filePath = path.join(tmpDir, 'jitter.txt');
      fs.writeFileSync(filePath, 'tracked-only');
      const stat = fs.statSync(filePath);

      const cache = new FileCache();
      // mtime-only entry (no content), cached with a sub-ms fraction added.
      cache.trackMtime(filePath, Math.floor(stat.mtimeMs) + 0.4);

      // The on-disk mtime floors to the same integer second/ms, so a sub-ms
      // difference must NOT be reported as a change. Pre-change exact-equality
      // comparison would report true here.
      assert.equal(cache.hasChanged(filePath), false);
    } finally {
      cleanupDir(tmpDir);
    }
  });

  it('a whole-millisecond mtime move on an mtime-only entry IS a change', () => {
    const tmpDir = makeTmpDir();
    try {
      const filePath = path.join(tmpDir, 'jitter2.txt');
      fs.writeFileSync(filePath, 'tracked-only');
      const stat = fs.statSync(filePath);

      const cache = new FileCache();
      cache.trackMtime(filePath, Math.floor(stat.mtimeMs) + 2000);

      assert.equal(cache.hasChanged(filePath), true);
    } finally {
      cleanupDir(tmpDir);
    }
  });
});

// ---------------------------------------------------------------------------
// MAX_CONTENT_HASH_SIZE: bound the bytes fed to SHA-256 before hashing, to
// prevent memory/CPU exhaustion on large / base64 inputs.
// [gaps: MAX_CONTENT_HASH_SIZE=100KB guard + content-size limit before hashing]
// ---------------------------------------------------------------------------
describe('FileCache — bounded content hashing (MAX_CONTENT_HASH_SIZE)', () => {
  it('exports MAX_CONTENT_HASH_SIZE = 100KB', () => {
    assert.equal(MAX_CONTENT_HASH_SIZE, 100 * 1024);
  });

  it('still produces a 64-char SHA-256 hash for over-threshold content', () => {
    const tmpDir = makeTmpDir();
    try {
      const filePath = path.join(tmpDir, 'over.txt');
      const big = 'a'.repeat(MAX_CONTENT_HASH_SIZE + 50_000);
      fs.writeFileSync(filePath, big);
      const stat = fs.statSync(filePath);

      const cache = new FileCache();
      cache.set(filePath, big, stat.mtimeMs);
      const cached = cache.get(filePath);
      assert.ok(cached);
      assert.equal(cached!.contentHash.length, 64);
    } finally {
      cleanupDir(tmpDir);
    }
  });

  it('a length change past the threshold is still detected as a different hash', () => {
    const tmpDir = makeTmpDir();
    try {
      const filePath = path.join(tmpDir, 'bighash.txt');
      const big = 'a'.repeat(MAX_CONTENT_HASH_SIZE + 10_000);
      fs.writeFileSync(filePath, big);
      const stat = fs.statSync(filePath);

      const cache = new FileCache();
      cache.set(filePath, big, stat.mtimeMs);
      assert.equal(cache.hasChanged(filePath), false);

      // Append more bytes (length changes) and keep the same mtime: the
      // length is folded into the bounded hash, so the change is detected.
      const bigger = big + 'b'.repeat(10_000);
      fs.writeFileSync(filePath, bigger);
      fs.utimesSync(filePath, stat.atimeMs / 1000, stat.mtimeMs / 1000);
      assert.equal(cache.hasChanged(filePath), true);
    } finally {
      cleanupDir(tmpDir);
    }
  });
});
