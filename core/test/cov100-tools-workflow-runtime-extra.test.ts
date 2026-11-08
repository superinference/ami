import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { DiskWorkflowCache } from '../src/tools/workflow-runtime';

// Covers DiskWorkflowCache.set best-effort catch branch: when the backing
// directory cannot be created (e.g. a plain file sits where the dir should be),
// set() must swallow the error silently rather than throwing, and the value
// must NOT become retrievable.

describe('DiskWorkflowCache.set best-effort failure (cov100)', () => {
  const tmps: string[] = [];
  after(() => { for (const t of tmps.splice(0)) fs.rmSync(t, { recursive: true, force: true }); });

  it('swallows mkdir/write errors when dir path is actually a file', () => {
    const base = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wfc-fail-')); tmps.push(base);
    // Create a regular FILE, then treat its path as the cache directory.
    const filePath = path.join(base, 'not-a-dir');
    fs.writeFileSync(filePath, 'i am a file');

    const cache = new DiskWorkflowCache(filePath);

    // mkdirSync(filePath, { recursive: true }) throws (EEXIST/ENOTDIR); set()
    // must catch it and return normally — no throw escapes.
    assert.doesNotThrow(() => cache.set('key', { a: 1 }));

    // Nothing was persisted, so a subsequent get is a miss (undefined).
    assert.equal(cache.get('key'), undefined);

    // The pre-existing file is untouched (write never succeeded under it).
    assert.equal(fs.readFileSync(filePath, 'utf8'), 'i am a file');
  });

  it('swallows write errors when the key file path is itself a directory', () => {
    const base = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wfc-fail2-')); tmps.push(base);
    const cache = new DiskWorkflowCache(base);
    // Pre-create a directory exactly where the key's JSON file would be written,
    // so writeFileSync throws EISDIR inside set()'s try and is swallowed.
    fs.mkdirSync(path.join(base, 'collide.json'));

    assert.doesNotThrow(() => cache.set('collide', { v: 2 }));
    // The value is not retrievable (write failed); get returns undefined.
    assert.equal(cache.get('collide'), undefined);
  });
});
