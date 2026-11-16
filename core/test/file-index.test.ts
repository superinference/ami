/**
 * Unit tests for the scalable fuzzy file-PATH index (core/core/src/file-index.ts).
 *
 * Covers the nucleo/fzf-v2 port end to end: dedupe on load, empty-query
 * top-level cache, bitmap rejection, greedy-earliest scoring with
 * boundary/camel/consecutive bonuses + gap penalties, the top-k heap, the
 * test-path 1.05× penalty, smart-case, the MAX_QUERY_LEN clamp, and the chunked
 * async build (with the config seam that forces yields deterministically).
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { FileIndex, fileIndexBuildConfig } from '../src/file-index';

function pathsOf(idx: FileIndex, q: string, limit = 20): string[] {
  return idx.search(q, limit).map(r => r.path);
}

describe('FileIndex.loadFromFileList', () => {
  it('dedupes and drops empty strings (first-seen order)', () => {
    const idx = new FileIndex();
    idx.loadFromFileList(['a.ts', '', 'a.ts', 'b.ts', '']);
    // Empty query returns the sorted top-level cache; both unique names survive.
    const top = pathsOf(idx, '');
    assert.deepEqual([...top].sort(), ['a.ts', 'b.ts']);
  });
});

describe('FileIndex.search empty query / limits', () => {
  it('returns [] for a non-positive limit', () => {
    const idx = new FileIndex();
    idx.loadFromFileList(['a.ts']);
    assert.deepEqual(idx.search('a', 0), []);
    assert.deepEqual(idx.search('a', -3), []);
  });

  it('empty query returns the top-level entries sorted by length then alpha', () => {
    const idx = new FileIndex();
    idx.loadFromFileList([
      'zeta/x.ts',
      'src/a.ts',
      'src/b.ts', // src collapses to one top-level segment
      'lib\\win.ts', // windows separator handled
      'a',
    ]);
    const res = idx.search('', 10);
    // Unique top-level segments: 'a'(1), 'lib'(3), 'src'(3), 'zeta'(4)
    assert.deepEqual(res.map(r => r.path), ['a', 'lib', 'src', 'zeta']);
    assert.ok(res.every(r => r.score === 0));
  });

  it('empty query on an empty index returns []', () => {
    const idx = new FileIndex();
    idx.loadFromFileList([]);
    assert.deepEqual(idx.search('', 5), []);
  });

  it('empty query before any load returns [] (null top-level cache)', () => {
    const idx = new FileIndex();
    assert.deepEqual(idx.search('', 5), []);
    assert.deepEqual(idx.search('abc', 5), []);
  });
});

describe('FileIndex.search ranking', () => {
  it('surfaces the best fuzzy match first (score 0.0) and finds paths anywhere', () => {
    const idx = new FileIndex();
    idx.loadFromFileList([
      'src/components/Button.tsx',
      'src/utils/helpers.ts',
      'README.md',
    ]);
    const res = idx.search('button', 5);
    assert.equal(res[0]!.path, 'src/components/Button.tsx');
    assert.equal(res[0]!.score, 0.0); // best = position 0 / count
  });

  it('rejects a path missing a needle letter via the a-z bitmap (no match)', () => {
    const idx = new FileIndex();
    idx.loadFromFileList(['abc.ts', 'def.ts']);
    // 'z' is in neither path -> bitmap reject -> no results.
    assert.deepEqual(pathsOf(idx, 'z'), []);
  });

  it('returns [] when the first needle char is absent from every path', () => {
    const idx = new FileIndex();
    idx.loadFromFileList(['abc.ts']);
    // 'q' present nowhere -> bitmap reject path.
    assert.deepEqual(pathsOf(idx, 'q'), []);
  });

  it('requires order: a needle whose chars exist out of order does not match', () => {
    const idx = new FileIndex();
    idx.loadFromFileList(['ab.ts']);
    // 'ba' -> 'b' then 'a' after it: 'a' never appears after 'b' -> no match.
    assert.deepEqual(pathsOf(idx, 'ba'), []);
    assert.deepEqual(pathsOf(idx, 'ab'), ['ab.ts']);
  });

  it('prefers a camelCase hump over a flat lowercase name for an acronym query', () => {
    const idx = new FileIndex();
    idx.loadFromFileList(['fooBar.ts', 'foodbar.ts']);
    const res = pathsOf(idx, 'fb');
    assert.ok(res.indexOf('fooBar.ts') < res.indexOf('foodbar.ts'), res.join(','));
  });

  it('rewards a boundary (separator) match', () => {
    const idx = new FileIndex();
    idx.loadFromFileList(['x/app.ts', 'xapp.ts']);
    const res = pathsOf(idx, 'app');
    // Both match; the one where 'app' starts after '/' earns BONUS_BOUNDARY.
    assert.equal(res[0], 'x/app.ts');
  });

  it('applies the 1.05x test-path penalty so a source file edges out a test', () => {
    const idx = new FileIndex();
    idx.loadFromFileList(['foo.test.ts', 'foo.ts']);
    const res = idx.search('foo', 5);
    const srcScore = res.find(r => r.path === 'foo.ts')!.score;
    const testScore = res.find(r => r.path === 'foo.test.ts')!.score;
    assert.ok(testScore >= srcScore, `${testScore} >= ${srcScore}`);
    assert.ok(testScore <= 1.0);
  });

  it('honors the top-k limit, keeping the best matches', () => {
    const idx = new FileIndex();
    idx.loadFromFileList(['abc.ts', 'axbxc.ts', 'a_b_c.ts', 'aXbXc.ts', 'abcd.ts']);
    const res = idx.search('abc', 2);
    assert.equal(res.length, 2);
    // The consecutive 'abc' matches must outrank the gapped ones.
    assert.ok(res.some(r => r.path === 'abc.ts'));
  });

  it('smart case: an uppercase query becomes case-sensitive', () => {
    const idx = new FileIndex();
    idx.loadFromFileList(['readme.md', 'README.md']);
    // Lowercase 'rea' matches both; 'REA' matches only the uppercase one.
    assert.equal(pathsOf(idx, 'rea').length, 2);
    assert.deepEqual(pathsOf(idx, 'REA'), ['README.md']);
  });

  it('clamps an over-long query to MAX_QUERY_LEN without crashing', () => {
    const idx = new FileIndex();
    idx.loadFromFileList(['abcdefabcdef.ts']);
    const q = 'abcdef'.repeat(20); // 120 chars > 64
    // Does not throw and returns an array.
    assert.ok(Array.isArray(idx.search(q, 5)));
  });

  it('gap-bound reject path: a full top-k with a weak extra candidate is skipped', () => {
    const idx = new FileIndex();
    // Two tight matches fill top-k=2; a third with a huge gap must be rejected
    // by the gap-bound early-reject (exercises that branch).
    idx.loadFromFileList(['ab.ts', 'a_b.ts', 'a' + 'z'.repeat(40) + 'b.ts']);
    const res = idx.search('ab', 2);
    assert.equal(res.length, 2);
    assert.ok(!res.some(r => r.path.includes('zzz')));
  });
});

describe('FileIndex.loadFromFileListAsync', () => {
  afterEach(() => {
    fileIndexBuildConfig.chunkMs = 4;
    fileIndexBuildConfig.chunkCheckInterval = 256;
  });

  it('builds the same index as the sync path and resolves queryable + done', async () => {
    const idx = new FileIndex();
    const { queryable, done } = idx.loadFromFileListAsync([
      'src/a.ts',
      'src/a.ts', // dup
      'src/b.ts',
    ]);
    await queryable;
    await done;
    assert.deepEqual(pathsOf(idx, 'a'), ['src/a.ts']);
  });

  it('yields per chunk when chunkMs is 0 and still indexes every path', async () => {
    fileIndexBuildConfig.chunkMs = 0; // force a yield at every checkpoint
    fileIndexBuildConfig.chunkCheckInterval = 1; // mask 0 -> check every iteration
    const idx = new FileIndex();
    const files = Array.from({ length: 10 }, (_, i) => `dir/file${i}.ts`);
    const { queryable, done } = idx.loadFromFileListAsync(files);
    await queryable;
    await done;
    assert.equal(pathsOf(idx, 'file9', 20).length, 1);
    assert.equal(idx.search('file', 20).length, 10);
  });
});
