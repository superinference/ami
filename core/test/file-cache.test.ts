import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { FileCache, getFileCache, MAX_CACHE_BYTES } from '../src/file-cache';

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

  it('returns true when mtime differs', () => {
    const tmpDir = makeTmpDir();
    try {
      const filePath = path.join(tmpDir, 'changing.txt');
      fs.writeFileSync(filePath, 'v1');
      const stat = fs.statSync(filePath);

      const cache = new FileCache();
      cache.set(filePath, 'v1', stat.mtimeMs);

      // Modify the file to change its mtime
      // Ensure a different mtime by using utimesSync
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
