/**
 * extractRetryAfterSeconds red-green: Retry-After parsing must handle BOTH a
 * numeric delta-seconds value and an HTTP-date, via the shared rate-limit parser.
 * The previous inline parseInt dropped the HTTP-date form.
 *
 * Run: node --import tsx --test test/provider-retry-after.test.ts
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { extractRetryAfterSeconds } from '../src/provider';

describe('extractRetryAfterSeconds', () => {
  it('parses a numeric delta-seconds value', () => {
    assert.equal(extractRetryAfterSeconds({ 'retry-after': '120' }), 120);
  });

  it('parses an HTTP-date value (previously dropped)', () => {
    const future = new Date(Date.now() + 90_000).toUTCString();
    const secs = extractRetryAfterSeconds({ 'retry-after': future });
    assert.ok(secs !== undefined && secs >= 80 && secs <= 95, `expected ~90s, got ${secs}`);
  });

  it('returns undefined when the header is absent', () => {
    assert.equal(extractRetryAfterSeconds({}), undefined);
    assert.equal(extractRetryAfterSeconds(undefined), undefined);
  });

  it('reads the lowercase header key (as fetch/HTTP normalize)', () => {
    assert.equal(extractRetryAfterSeconds({ 'retry-after': '30' }), 30);
  });

  it('clamps a past HTTP-date to 0', () => {
    const past = new Date(Date.now() - 60_000).toUTCString();
    assert.equal(extractRetryAfterSeconds({ 'retry-after': past }), 0);
  });
});
