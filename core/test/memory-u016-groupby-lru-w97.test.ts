import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { groupBy } from '../src/utils/group-by';
import { canonicalGitRoot } from '../src/reference-journal';
import { MemoryManager } from '../src/memory';

// ---------------------------------------------------------------------------
// Gap U016/groupBy — one shared, index-aware, prototype-safe grouping helper
// (parity: Claude Code utils/objectGroupBy.ts). Replaces the ad-hoc Map grouping
// re-implemented in memory.ts (session facts) and tools/lsp.ts.
// ---------------------------------------------------------------------------

describe('groupBy — shared grouping helper', () => {
  it('buckets items by a derived key, preserving first-seen key order', () => {
    const items = [
      { cat: 'b', n: 1 },
      { cat: 'a', n: 2 },
      { cat: 'b', n: 3 },
      { cat: 'a', n: 4 },
    ];
    const grouped = groupBy(items, (it) => it.cat);
    assert.deepEqual([...grouped.keys()], ['b', 'a'], 'keys in first-seen order');
    assert.deepEqual(grouped.get('b')!.map((x) => x.n), [1, 3]);
    assert.deepEqual(grouped.get('a')!.map((x) => x.n), [2, 4]);
  });

  it('passes a running index to the keySelector', () => {
    const seen: number[] = [];
    const grouped = groupBy(['x', 'y', 'z'], (_item, index) => {
      seen.push(index);
      return index % 2 === 0 ? 'even' : 'odd';
    });
    assert.deepEqual(seen, [0, 1, 2], 'index increments per item');
    assert.deepEqual(grouped.get('even'), ['x', 'z']);
    assert.deepEqual(grouped.get('odd'), ['y']);
  });

  it('returns an empty Map for empty input', () => {
    const grouped = groupBy([] as number[], (n) => n);
    assert.equal(grouped.size, 0);
  });

  it('accepts any Iterable (not just arrays)', () => {
    const set = new Set([1, 2, 3, 4]);
    const grouped = groupBy(set, (n) => (n % 2 === 0 ? 'even' : 'odd'));
    assert.deepEqual(grouped.get('even'), [2, 4]);
    assert.deepEqual(grouped.get('odd'), [1, 3]);
  });

  it('is prototype-pollution safe for hostile keys (Map, not a plain object)', () => {
    const grouped = groupBy([{ k: '__proto__' }, { k: 'constructor' }, { k: '__proto__' }], (x) => x.k);
    // A plain-object accumulator would corrupt the prototype; a Map stores these
    // as ordinary keys and the global prototype is untouched.
    assert.equal(grouped.get('__proto__')!.length, 2);
    assert.equal(grouped.get('constructor')!.length, 1);
    assert.equal(({} as Record<string, unknown>).polluted, undefined);
  });
});

// ---------------------------------------------------------------------------
// Gap U016/git-root LRU — canonicalGitRoot memoization MUST be BOUNDED (LRU)
// not an unbounded Map (parity: Claude Code findGitRoot memoizeWithLRU(…, 50)).
// ---------------------------------------------------------------------------

describe('canonicalGitRoot — bounded LRU memoization', () => {
  let base: string;
  before(() => {
    base = fs.mkdtempSync(path.join(os.tmpdir(), 'u016-gitroot-'));
  });
  after(() => {
    fs.rmSync(base, { recursive: true, force: true });
    canonicalGitRoot.cache.clear();
  });

  it('exposes an LRU cache surface and never grows past the bound', () => {
    canonicalGitRoot.cache.clear();
    assert.equal(canonicalGitRoot.cache.size(), 0);

    // Resolve 60 distinct cwds — an unbounded Map would keep all 60.
    for (let i = 0; i < 60; i++) {
      const dir = path.join(base, `d${i}`);
      fs.mkdirSync(dir, { recursive: true });
      canonicalGitRoot(dir);
    }
    assert.ok(
      canonicalGitRoot.cache.size() <= 50,
      `cache must stay bounded at 50, got ${canonicalGitRoot.cache.size()}`,
    );
    // Oldest entry evicted; most-recent retained.
    assert.equal(canonicalGitRoot.cache.has(path.join(base, 'd0')), false);
    assert.equal(canonicalGitRoot.cache.has(path.join(base, 'd59')), true);
  });

  it('memoizes repeated lookups for the same cwd (one cache entry)', () => {
    canonicalGitRoot.cache.clear();
    const dir = path.join(base, 'repeat');
    fs.mkdirSync(dir, { recursive: true });
    const a = canonicalGitRoot(dir);
    const b = canonicalGitRoot(dir);
    assert.equal(a, b);
    assert.equal(canonicalGitRoot.cache.size(), 1);
  });
});

// ---------------------------------------------------------------------------
// Integration: saveSessionMemory now groups via the shared helper; the on-disk
// file must still bucket facts by category and round-trip through load.
// ---------------------------------------------------------------------------

describe('saveSessionMemory grouping via shared groupBy (round-trip)', () => {
  let cwd: string;
  before(() => {
    cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'u016-sessmem-'));
  });
  after(() => fs.rmSync(cwd, { recursive: true, force: true }));

  it('groups facts by category and loads them back', () => {
    const mgr = new MemoryManager(cwd);
    mgr.saveSessionMemory('s1', [
      { fact: 'use pnpm', category: 'convention', confidence: 0.9 },
      { fact: 'chose esm', category: 'decision', confidence: 0.8 },
      { fact: 'prefer tabs', category: 'convention', confidence: 0.7 },
    ]);
    const loaded = mgr.loadSessionMemory('s1');
    const conventions = loaded.filter((f) => f.category === 'convention');
    const decisions = loaded.filter((f) => f.category === 'decision');
    assert.equal(conventions.length, 2);
    assert.equal(decisions.length, 1);
  });
});
