import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { FileCache, getFileCache } from '../src/file-cache';

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
