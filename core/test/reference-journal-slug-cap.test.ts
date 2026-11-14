/**
 * Claude-parity: projectSlug length cap + hash suffix for deep paths (H31).
 *
 * Run: cd core/core && node --import tsx --test test/reference-journal-slug-cap.test.ts
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { projectSlug, MAX_SLUG_LENGTH } from '../src/reference-journal';

describe('projectSlug — length cap + hash suffix (H31)', () => {
  it('leaves a short cwd slug unchanged (pure replace)', () => {
    assert.equal(projectSlug('/home/u/proj'), '-home-u-proj');
  });

  it('caps a very deep cwd to MAX_SLUG_LENGTH', () => {
    const deep = '/' + Array.from({ length: 80 }, (_, i) => `segment-${i}`).join('/');
    const slug = projectSlug(deep);
    assert.ok(slug.length <= MAX_SLUG_LENGTH, `slug length ${slug.length} must be <= ${MAX_SLUG_LENGTH}`);
  });

  it('is deterministic — same cwd yields the same capped slug', () => {
    const deep = '/x' + '/aaaaaaaaaa'.repeat(40);
    assert.equal(projectSlug(deep), projectSlug(deep));
  });

  it('two different deep cwds sharing a long prefix get distinct slugs (hash disambiguates)', () => {
    const prefix = '/root' + '/zzzzzzzzzz'.repeat(40);
    const a = projectSlug(prefix + '/alpha');
    const b = projectSlug(prefix + '/beta');
    assert.notEqual(a, b, 'the hash suffix must disambiguate colliding long prefixes');
  });

  it('the capped slug contains only slug-safe characters', () => {
    const deep = '/p' + '/qqqqqqqqqq'.repeat(40);
    assert.match(projectSlug(deep), /^[a-zA-Z0-9-]+$/);
  });
});
