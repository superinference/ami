import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { generateWordSlug, generateShortWordSlug, randomInt, pickRandom } from '../src/words';

describe('words — generateWordSlug', () => {
  it('produces an adjective-verb-noun slug (three lowercase parts)', () => {
    const slug = generateWordSlug();
    const parts = slug.split('-');
    assert.equal(parts.length, 3, `expected 3 parts, got "${slug}"`);
    for (const p of parts) assert.match(p, /^[a-z]+$/);
  });

  it('is well-distributed — many calls yield many distinct slugs', () => {
    const seen = new Set<string>();
    for (let i = 0; i < 500; i++) seen.add(generateWordSlug());
    // With ~9.7M combinations, 500 draws should almost never collide.
    assert.ok(seen.size > 490, `expected high uniqueness, got ${seen.size}/500`);
  });
});

describe('words — generateShortWordSlug', () => {
  it('produces an adjective-noun slug (two lowercase parts)', () => {
    const slug = generateShortWordSlug();
    const parts = slug.split('-');
    assert.equal(parts.length, 2, `expected 2 parts, got "${slug}"`);
    for (const p of parts) assert.match(p, /^[a-z]+$/);
  });
});

describe('words — randomInt / pickRandom (unbiased crypto helpers)', () => {
  it('rejects a non-positive or non-integer max', () => {
    assert.throws(() => randomInt(0), RangeError);
    assert.throws(() => randomInt(-1), RangeError);
    assert.throws(() => randomInt(2.5), RangeError);
  });

  it('always returns an integer within [0, max)', () => {
    for (let i = 0; i < 1000; i++) {
      const v = randomInt(7);
      assert.ok(Number.isInteger(v) && v >= 0 && v < 7, `out of range: ${v}`);
    }
  });

  it('randomInt(1) is always 0', () => {
    for (let i = 0; i < 50; i++) assert.equal(randomInt(1), 0);
  });

  it('is approximately uniform for a non-power-of-two max (no modulo bias)', () => {
    const max = 6;
    const counts = new Array(max).fill(0);
    const draws = 60000;
    for (let i = 0; i < draws; i++) counts[randomInt(max)]++;
    const expected = draws / max;
    for (const c of counts) {
      // Within ~10% of the expected bucket count; a biased `% max` would skew.
      assert.ok(Math.abs(c - expected) < expected * 0.1, `bucket ${c} too far from ${expected}`);
    }
  });

  it('pickRandom returns an element of the array', () => {
    const arr = ['a', 'b', 'c'] as const;
    for (let i = 0; i < 100; i++) assert.ok(arr.includes(pickRandom(arr)));
  });

  it('pickRandom covers every element over many draws', () => {
    const arr = [0, 1, 2, 3, 4];
    const seen = new Set<number>();
    for (let i = 0; i < 500; i++) seen.add(pickRandom(arr));
    assert.equal(seen.size, arr.length);
  });
});
