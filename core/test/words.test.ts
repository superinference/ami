import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { generateWordSlug, generateShortWordSlug } from '../src/words';

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
