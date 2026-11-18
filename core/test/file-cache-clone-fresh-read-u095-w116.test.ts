/**
 * U095 (wave w116) — FileCache.cloneForFreshRead (core/core/src/file-cache.ts).
 *
 * The canonical "clone + per-path invalidation" dedup-busting primitive: produce a
 * fully independent snapshot that has FORGOTTEN one path, so a read of that path
 * through the clone cannot be short-circuited by a stale/"unchanged" entry, while the
 * live cache is left completely untouched. Claude parity: updateMagicDoc's
 * `cloneFileStateCache(x); x.delete(docPath)` (services/MagicDocs/magicDocs.ts L121-153).
 *
 * The cache is inspected via listPaths()/entries()/dump() (not get(), which stats the
 * real file) so these pure-cache assertions do not depend on on-disk files.
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { FileCache } from '../src/file-cache';

/** Content of a path in a cache, read straight from its entries (no disk stat). */
function contentOf(c: FileCache, key: string): string | undefined {
  return c.entries().find(([k]) => k === key)?.[1].content;
}

describe('FileCache.cloneForFreshRead — dedup-busting isolated snapshot', () => {
  it('forgets the named path in the clone but preserves every other entry', () => {
    const src = new FileCache();
    src.set('/a.ts', 'AAA', 1);
    src.set('/b.ts', 'BBB', 2);

    const fresh = src.cloneForFreshRead('/a.ts');

    // The forgotten path is gone from the clone (so a re-read returns real content,
    // not a cache hit)...
    assert.deepEqual(fresh.listPaths(), ['/b.ts']);
    // ...while every other entry survives verbatim.
    assert.equal(contentOf(fresh, '/b.ts'), 'BBB');
  });

  it('leaves the SOURCE cache completely untouched (isolation)', () => {
    const src = new FileCache();
    src.set('/a.ts', 'AAA', 1);

    const fresh = src.cloneForFreshRead('/a.ts');
    // Source still knows the path...
    assert.equal(contentOf(src, '/a.ts'), 'AAA');

    // ...and mutating the clone cannot leak back into the source.
    fresh.set('/a.ts', 'ZZZ', 9);
    fresh.set('/c.ts', 'CCC', 3);
    assert.equal(contentOf(src, '/a.ts'), 'AAA');
    assert.deepEqual(src.listPaths(), ['/a.ts']);
  });

  it('normalizes the path argument when forgetting (relative/redundant spellings)', () => {
    const src = new FileCache();
    src.set('/x/y.ts', 'YY', 1);

    // A redundant spelling of the same path must still forget the entry.
    const fresh = src.cloneForFreshRead('/x/../x/y.ts');
    assert.deepEqual(fresh.listPaths(), []);
  });

  it('preserves the size configuration of the source', () => {
    const src = new FileCache(1234, 7);
    src.set('/a.ts', 'A', 1);
    const fresh = src.cloneForFreshRead('/a.ts');
    const d = fresh.dump();
    assert.equal(d.maxSizeBytes, 1234);
    assert.equal(d.maxEntries, 7);
  });

  it('forgetting a path that is not cached is a no-op clone', () => {
    const src = new FileCache();
    src.set('/a.ts', 'AAA', 1);
    const fresh = src.cloneForFreshRead('/not-here.ts');
    assert.equal(contentOf(fresh, '/a.ts'), 'AAA');
    assert.equal(fresh.getStats().files, 1);
  });
});
