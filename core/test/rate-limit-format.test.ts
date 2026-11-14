import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { formatResetTime, formatRateLimitReset } from '../src/rate-limit-parser';

describe('formatResetTime', () => {
  it('returns undefined for missing/zero timestamp', () => {
    assert.equal(formatResetTime(undefined), undefined);
    assert.equal(formatResetTime(0), undefined);
  });

  it('renders a within-24h reset as a lowercase am/pm clock time', () => {
    const inTwoHours = Math.floor(Date.now() / 1000) + 2 * 60 * 60;
    const out = formatResetTime(inTwoHours);
    assert.ok(out, 'expected a string');
    assert.match(out!, /\d(am|pm)$/i);
    // No leading space before am/pm.
    assert.ok(!/ (am|pm)$/.test(out!));
  });

  it('includes the date for resets more than 24h away', () => {
    const inThreeDays = Math.floor(Date.now() / 1000) + 3 * 24 * 60 * 60;
    const out = formatResetTime(inThreeDays)!;
    // Short month name present (e.g. "Nov 17").
    assert.match(out, /[A-Z][a-z]{2}\s+\d{1,2}/);
  });

  it('appends timezone when requested', () => {
    const inTwoHours = Math.floor(Date.now() / 1000) + 2 * 60 * 60;
    const out = formatResetTime(inTwoHours, true)!;
    assert.match(out, /\(.+\)$/);
  });
});

describe('formatRateLimitReset', () => {
  it('renders from a RateLimitInfo resetAt', () => {
    const resetAt = new Date(Date.now() + 90 * 60 * 1000);
    const out = formatRateLimitReset({
      retryAfterMs: null, remainingRequests: null, remainingTokens: null,
      resetAt, resetAtTokens: null, limitRequests: null, limitTokens: null,
    });
    assert.ok(out);
    assert.match(out!, /(am|pm)/i);
  });

  it('returns undefined when there is no reset information', () => {
    const out = formatRateLimitReset({
      retryAfterMs: null, remainingRequests: null, remainingTokens: null,
      resetAt: null, resetAtTokens: null, limitRequests: null, limitTokens: null,
    });
    assert.equal(out, undefined);
  });
});
