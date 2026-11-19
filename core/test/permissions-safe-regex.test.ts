import { test } from 'node:test';
import assert from 'node:assert/strict';
import { wildcardMatch } from '../src/permissions';
import { DEFAULT_MAX_PATTERN_LENGTH } from '../src/safe-regex';

// The permission/hook glob matcher escapes every metachar except `*` (→ `.*`),
// so its residual ReDoS risk is a pathologically long matcher that compiles
// into a huge `^.*…$` regex. The shared length cap refuses it up front.

test('wildcardMatch: a normal glob still matches', () => {
  assert.equal(wildcardMatch('git *', 'git status'), true);
  assert.equal(wildcardMatch('git *', 'git'), true);
  assert.equal(wildcardMatch('npm run *', 'npm run build'), true);
  assert.equal(wildcardMatch('foo', 'bar'), false);
  assert.equal(wildcardMatch('*', 'anything at all'), true);
});

test('wildcardMatch: an over-length matcher is refused (never matches)', () => {
  // A matcher far longer than any legitimate command — e.g. thousands of `*` —
  // would build a monster regex. It must simply not match, rather than compile.
  const huge = '*'.repeat(DEFAULT_MAX_PATTERN_LENGTH + 1);
  assert.equal(wildcardMatch(huge, 'a'.repeat(5000)), false);

  const longPrefix = 'x'.repeat(DEFAULT_MAX_PATTERN_LENGTH + 10) + '*';
  assert.equal(wildcardMatch(longPrefix, 'anything'), false);
});

test('wildcardMatch: a matcher at exactly the cap still works', () => {
  // Length == cap is allowed (strictly-greater is refused).
  const atCap = 'a'.repeat(DEFAULT_MAX_PATTERN_LENGTH);
  assert.equal(atCap.length, DEFAULT_MAX_PATTERN_LENGTH);
  assert.equal(wildcardMatch(atCap, atCap), true);
});
