import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { generateWordSlug, generateShortWordSlug, generateUniqueSlug, randomInt, pickRandom, ONE_TOKEN_WORDS, generateLoremIpsum } from '../src/words';

const SLUG_RE = /^[a-z]+-[a-z]+-[a-z]+$/;

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

describe('words — generateUniqueSlug (collision-aware)', () => {
  it('returns the first word slug when nothing is taken', () => {
    let calls = 0;
    const slug = generateUniqueSlug(() => { calls++; return false; });
    assert.equal(calls, 1, 'the sole candidate should be checked exactly once');
    assert.match(slug, SLUG_RE);
  });

  it('retries until it finds a free slug and never returns a rejected one', () => {
    let calls = 0;
    const slug = generateUniqueSlug(() => { calls++; return calls <= 2; });
    assert.ok(calls >= 3, `expected at least 3 predicate calls, got ${calls}`);
    assert.match(slug, SLUG_RE);
  });

  it('consults the predicate at most maxRetries times before giving up', () => {
    let calls = 0;
    const slug = generateUniqueSlug(() => { calls++; return true; }, 3);
    assert.equal(calls, 3);
    assert.match(slug, SLUG_RE);
  });

  it('does not consult the predicate when maxRetries is zero', () => {
    let calls = 0;
    const slug = generateUniqueSlug(() => { calls++; return true; }, 0);
    assert.equal(calls, 0);
    assert.match(slug, SLUG_RE);
  });
});

describe('words — ONE_TOKEN_WORDS vocabulary', () => {
  it('is a non-trivial pool of single-token words', () => {
    assert.ok(ONE_TOKEN_WORDS.length >= 150, `expected a sizeable vocabulary, got ${ONE_TOKEN_WORDS.length}`);
  });

  it('every entry is a non-empty, whitespace-free word (so it stays a single token)', () => {
    for (const w of ONE_TOKEN_WORDS) {
      assert.equal(typeof w, 'string');
      assert.ok(w.length > 0, 'no empty entries');
      assert.ok(!/\s/.test(w), `"${w}" contains whitespace`);
    }
  });
});

describe('words — generateLoremIpsum (token-budgeted filler)', () => {
  const VOCAB = new Set(ONE_TOKEN_WORDS);
  const wordCount = (s: string) => (s.trim() === '' ? 0 : s.trim().split(/\s+/).length);

  it('returns an empty string for a non-positive, NaN, or infinite budget', () => {
    assert.equal(generateLoremIpsum(0), '');
    assert.equal(generateLoremIpsum(-5), '');
    assert.equal(generateLoremIpsum(NaN), '');
    assert.equal(generateLoremIpsum(Infinity), '');
  });

  it('emits exactly the requested number of word-tokens (1 word == 1 token)', () => {
    for (const target of [1, 7, 50, 137]) {
      assert.equal(wordCount(generateLoremIpsum(target)), target, `target ${target}`);
    }
  });

  it('floors a fractional budget to a whole token count', () => {
    assert.equal(wordCount(generateLoremIpsum(10.9)), 10);
  });

  it('draws every word from the single-token vocabulary and is trimmed', () => {
    const text = generateLoremIpsum(300);
    assert.equal(text, text.trim(), 'output has no leading/trailing whitespace');
    for (const raw of text.split(/\s+/)) {
      const word = raw.replace(/\.$/, ''); // strip a sentence-ending period
      assert.ok(VOCAB.has(word), `"${word}" is not in the vocabulary`);
    }
  });

  it('shapes output into sentences and paragraphs for a large budget', () => {
    const text = generateLoremIpsum(3000);
    assert.ok(text.includes('. '), 'sentences end with a period');
    // With ~150+ sentences at a ~20% per-sentence break rate, at least one
    // paragraph break is effectively certain.
    assert.ok(text.includes('\n\n'), 'has paragraph breaks');
  });
});
