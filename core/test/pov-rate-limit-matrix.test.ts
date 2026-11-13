import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  parseRateLimitHeaders,
  parseRetryAfterHeader,
  shouldBackoff,
  suggestedWaitMs,
  type RateLimitInfo,
} from '../src/rate-limit-parser';
import { RateLimitTracker } from '../src/rate-limiter';

// POV: rate-limit-parser.ts + rate-limiter.ts
// All expectations are computed by calling the real code and asserting on the
// returned values — no source-text introspection.

describe('pov-rate-limit-matrix: parseRetryAfterHeader (seconds form)', () => {
  it('returns null for undefined', () => {
    assert.equal(parseRetryAfterHeader(undefined), null);
  });

  it('returns null for empty string', () => {
    assert.equal(parseRetryAfterHeader(''), null);
  });

  it('returns null for whitespace-only string', () => {
    assert.equal(parseRetryAfterHeader('   '), null);
  });

  it('converts whole seconds to milliseconds', () => {
    assert.equal(parseRetryAfterHeader('5'), 5000);
  });

  it('trims surrounding whitespace before parsing seconds', () => {
    assert.equal(parseRetryAfterHeader('  12  '), 12000);
  });

  it('ceils fractional seconds up to the next whole millisecond', () => {
    // 0.0015s = 1.5ms -> ceil -> 2ms
    assert.equal(parseRetryAfterHeader('0.0015'), 2);
  });

  it('treats zero seconds as zero milliseconds (not null)', () => {
    assert.equal(parseRetryAfterHeader('0'), 0);
  });

  it('rejects negative seconds as null', () => {
    assert.equal(parseRetryAfterHeader('-1'), null);
  });

  it('rejects seconds above the 86400 (24h) cap as null', () => {
    assert.equal(parseRetryAfterHeader('86401'), null);
  });

  it('accepts exactly the 86400 cap', () => {
    assert.equal(parseRetryAfterHeader('86400'), 86400 * 1000);
  });
});

describe('pov-rate-limit-matrix: parseRetryAfterHeader (HTTP-date form)', () => {
  it('returns a positive delay for a future HTTP-date', () => {
    const future = new Date(Date.now() + 10_000).toUTCString();
    const ms = parseRetryAfterHeader(future);
    assert.ok(ms !== null);
    // Allow a small window for clock/rounding drift.
    assert.ok((ms as number) > 8000 && (ms as number) <= 11000, `got ${ms}`);
  });

  it('clamps a past HTTP-date to zero (never negative)', () => {
    const past = new Date(Date.now() - 60_000).toUTCString();
    assert.equal(parseRetryAfterHeader(past), 0);
  });

  it('returns null for a malformed/unparseable value', () => {
    assert.equal(parseRetryAfterHeader('not-a-date'), null);
  });

  it('returns null for a numeric-with-suffix value (NaN then bad date)', () => {
    // "5s" -> Number() NaN -> new Date("5s") invalid -> null
    assert.equal(parseRetryAfterHeader('5s'), null);
  });
});

describe('pov-rate-limit-matrix: parseRateLimitHeaders (x-ratelimit-* variants)', () => {
  it('returns all-null info for an empty header map', () => {
    const info = parseRateLimitHeaders({});
    assert.deepEqual(info, {
      retryAfterMs: null,
      remainingRequests: null,
      remainingTokens: null,
      resetAt: null,
      resetAtTokens: null,
      limitRequests: null,
      limitTokens: null,
    } satisfies RateLimitInfo);
  });

  it('parses the primary x-ratelimit-* request + token integers', () => {
    const info = parseRateLimitHeaders({
      'x-ratelimit-remaining-requests': '42',
      'x-ratelimit-remaining-tokens': '1000',
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-limit-tokens': '50000',
    });
    assert.equal(info.remainingRequests, 42);
    assert.equal(info.remainingTokens, 1000);
    assert.equal(info.limitRequests, 100);
    assert.equal(info.limitTokens, 50000);
  });

  it('falls back to the ratelimit-remaining / ratelimit-limit aliases', () => {
    const info = parseRateLimitHeaders({
      'ratelimit-remaining': '7',
      'ratelimit-limit': '20',
    });
    assert.equal(info.remainingRequests, 7);
    assert.equal(info.limitRequests, 20);
  });

  it('prefers the x-ratelimit primary over the generic alias', () => {
    const info = parseRateLimitHeaders({
      'x-ratelimit-remaining-requests': '3',
      'ratelimit-remaining': '99',
      'x-ratelimit-limit-requests': '10',
      'ratelimit-limit': '1000',
    });
    assert.equal(info.remainingRequests, 3);
    assert.equal(info.limitRequests, 10);
  });

  it('parses retry-after seconds from the header map', () => {
    const info = parseRateLimitHeaders({ 'retry-after': '3' });
    assert.equal(info.retryAfterMs, 3000);
  });

  it('reads the first element when a header value is an array', () => {
    const info = parseRateLimitHeaders({
      'retry-after': ['9', '99'],
      'x-ratelimit-remaining-requests': ['11', '22'],
    });
    assert.equal(info.retryAfterMs, 9000);
    assert.equal(info.remainingRequests, 11);
  });

  it('yields null integers for non-numeric header values', () => {
    const info = parseRateLimitHeaders({
      'x-ratelimit-remaining-requests': 'nope',
      'x-ratelimit-limit-tokens': 'xyz',
    });
    assert.equal(info.remainingRequests, null);
    assert.equal(info.limitTokens, null);
  });

  it('parses a seconds-form reset header into a near-future Date', () => {
    const before = Date.now();
    const info = parseRateLimitHeaders({ 'x-ratelimit-reset-requests': '30s' });
    assert.ok(info.resetAt instanceof Date);
    const t = (info.resetAt as Date).getTime();
    assert.ok(t >= before + 30_000 - 50 && t <= Date.now() + 30_000 + 50, `got ${t}`);
  });

  it('parses a bare-numeric reset header as seconds', () => {
    const before = Date.now();
    const info = parseRateLimitHeaders({ 'x-ratelimit-reset-tokens': '15' });
    assert.ok(info.resetAtTokens instanceof Date);
    const t = (info.resetAtTokens as Date).getTime();
    assert.ok(t >= before + 15_000 - 50 && t <= Date.now() + 15_000 + 50, `got ${t}`);
  });

  it('parses a fractional seconds-form reset header', () => {
    const before = Date.now();
    const info = parseRateLimitHeaders({ 'x-ratelimit-reset-requests': '2.5s' });
    const t = (info.resetAt as Date).getTime();
    assert.ok(t >= before + 2500 - 50 && t <= Date.now() + 2500 + 50, `got ${t}`);
  });

  it('parses an absolute date reset header verbatim', () => {
    const iso = '2099-01-01T00:00:00.000Z';
    const info = parseRateLimitHeaders({ 'x-ratelimit-reset-requests': iso });
    assert.ok(info.resetAt instanceof Date);
    assert.equal((info.resetAt as Date).toISOString(), iso);
  });

  it('returns null resetAt for an unparseable reset header', () => {
    const info = parseRateLimitHeaders({ 'x-ratelimit-reset-requests': 'soon' });
    assert.equal(info.resetAt, null);
  });

  it('falls back to x-ratelimit-reset when reset-requests is absent', () => {
    const info = parseRateLimitHeaders({ 'x-ratelimit-reset': '45s' });
    assert.ok(info.resetAt instanceof Date);
  });
});

describe('pov-rate-limit-matrix: shouldBackoff', () => {
  const base: RateLimitInfo = {
    retryAfterMs: null,
    remainingRequests: null,
    remainingTokens: null,
    resetAt: null,
    resetAtTokens: null,
    limitRequests: null,
    limitTokens: null,
  };

  it('is false when all signals are null', () => {
    assert.equal(shouldBackoff({ ...base }), false);
  });

  it('is true when a positive retry-after is present', () => {
    assert.equal(shouldBackoff({ ...base, retryAfterMs: 1000 }), true);
  });

  it('is false when retry-after is exactly zero and nothing else triggers', () => {
    assert.equal(shouldBackoff({ ...base, retryAfterMs: 0 }), false);
  });

  it('is true when remaining requests is zero', () => {
    assert.equal(shouldBackoff({ ...base, remainingRequests: 0 }), true);
  });

  it('is true when remaining requests is negative', () => {
    assert.equal(shouldBackoff({ ...base, remainingRequests: -5 }), true);
  });

  it('is false when remaining requests is still positive', () => {
    assert.equal(shouldBackoff({ ...base, remainingRequests: 3 }), false);
  });

  it('is true when remaining tokens is exhausted', () => {
    assert.equal(shouldBackoff({ ...base, remainingTokens: 0 }), true);
  });

  it('is false with ample remaining tokens', () => {
    assert.equal(shouldBackoff({ ...base, remainingTokens: 500 }), false);
  });

  it('derives a true decision from real parsed headers at the limit', () => {
    const info = parseRateLimitHeaders({ 'x-ratelimit-remaining-requests': '0' });
    assert.equal(shouldBackoff(info), true);
  });
});

describe('pov-rate-limit-matrix: suggestedWaitMs', () => {
  const base: RateLimitInfo = {
    retryAfterMs: null,
    remainingRequests: null,
    remainingTokens: null,
    resetAt: null,
    resetAtTokens: null,
    limitRequests: null,
    limitTokens: null,
  };

  it('returns the default when no signal is present', () => {
    assert.equal(suggestedWaitMs({ ...base }), 1000);
  });

  it('honours a custom default', () => {
    assert.equal(suggestedWaitMs({ ...base }, 5000), 5000);
  });

  it('prefers a positive retry-after over everything else', () => {
    assert.equal(
      suggestedWaitMs({ ...base, retryAfterMs: 7000, resetAt: new Date(Date.now() + 999_999) }),
      7000,
    );
  });

  it('ignores a zero retry-after and uses the reset delta', () => {
    const resetAt = new Date(Date.now() + 20_000);
    const ms = suggestedWaitMs({ ...base, retryAfterMs: 0, resetAt });
    assert.ok(ms > 18_000 && ms <= 20_000, `got ${ms}`);
  });

  it('uses the reset delta when only resetAt is present', () => {
    const resetAt = new Date(Date.now() + 12_000);
    const ms = suggestedWaitMs({ ...base, resetAt });
    assert.ok(ms > 10_000 && ms <= 12_000, `got ${ms}`);
  });

  it('falls back to default when resetAt is in the past', () => {
    const resetAt = new Date(Date.now() - 5000);
    assert.equal(suggestedWaitMs({ ...base, resetAt }, 2500), 2500);
  });
});

describe('pov-rate-limit-matrix: RateLimitTracker accounting', () => {
  it('reports zeroed status before any update', () => {
    const t = new RateLimitTracker();
    const s = t.getStatus();
    assert.equal(s.requests, null);
    assert.equal(s.tokens, null);
    assert.equal(s.requestsPercent, 0);
    assert.equal(s.tokensPercent, 0);
    assert.equal(s.shouldWarn, false);
  });

  it('records a request bucket and computes consumed percent', () => {
    const t = new RateLimitTracker();
    t.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '75',
      'x-ratelimit-reset-requests': '30s',
    });
    const s = t.getStatus();
    assert.ok(s.requests);
    assert.equal(s.requests!.limit, 100);
    assert.equal(s.requests!.remaining, 75);
    // (100 - 75) / 100 = 25%
    assert.equal(s.requestsPercent, 25);
    assert.equal(s.shouldWarn, false);
  });

  it('records a token bucket independently of requests', () => {
    const t = new RateLimitTracker();
    t.update({
      'x-ratelimit-limit-tokens': '1000',
      'x-ratelimit-remaining-tokens': '100',
      'x-ratelimit-reset-tokens': '60s',
    });
    const s = t.getStatus();
    assert.equal(s.requests, null);
    assert.ok(s.tokens);
    assert.equal(s.tokens!.limit, 1000);
    assert.equal(s.tokens!.remaining, 100);
    // (1000 - 100) / 1000 = 90%
    assert.equal(s.tokensPercent, 90);
    assert.equal(s.shouldWarn, true); // 90% >= 80% warn threshold
  });

  it('flags shouldWarn exactly at the 80% threshold', () => {
    const t = new RateLimitTracker();
    t.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '20', // 80% consumed
    });
    const s = t.getStatus();
    assert.equal(s.requestsPercent, 80);
    assert.equal(s.shouldWarn, true);
  });

  it('does not warn just below the threshold', () => {
    const t = new RateLimitTracker();
    t.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '21', // 79% consumed
    });
    const s = t.getStatus();
    assert.equal(s.requestsPercent, 79);
    assert.equal(s.shouldWarn, false);
  });

  it('treats a zero limit as fully consumed (100%, warn)', () => {
    const t = new RateLimitTracker();
    t.update({
      'x-ratelimit-limit-requests': '0',
      'x-ratelimit-remaining-requests': '0',
    });
    const s = t.getStatus();
    assert.ok(s.requests);
    assert.equal(s.requests!.limit, 0);
    assert.equal(s.requestsPercent, 100);
    assert.equal(s.shouldWarn, true);
  });

  it('ignores an update missing the limit header (no bucket set)', () => {
    const t = new RateLimitTracker();
    t.update({ 'x-ratelimit-remaining-requests': '5' });
    const s = t.getStatus();
    assert.equal(s.requests, null);
    assert.equal(s.requestsPercent, 0);
  });

  it('ignores an update missing the remaining header (no bucket set)', () => {
    const t = new RateLimitTracker();
    t.update({ 'x-ratelimit-limit-requests': '100' });
    const s = t.getStatus();
    assert.equal(s.requests, null);
  });

  it('uses the parser resetAt (seconds form) for the bucket resetAt', () => {
    const before = Date.now();
    const t = new RateLimitTracker();
    t.update({
      'x-ratelimit-limit-requests': '10',
      'x-ratelimit-remaining-requests': '5',
      'x-ratelimit-reset-requests': '30s',
    });
    const reset = t.getStatus().requests!.resetAt;
    assert.ok(reset >= before + 30_000 - 100 && reset <= Date.now() + 30_000 + 100, `got ${reset}`);
  });

  it('falls back to parseDuration for compound reset formats the parser cannot decode', () => {
    const before = Date.now();
    const t = new RateLimitTracker();
    t.update({
      'x-ratelimit-limit-requests': '10',
      'x-ratelimit-remaining-requests': '5',
      'x-ratelimit-reset-requests': '1h30m', // parser returns null -> parseDuration
    });
    const reset = t.getStatus().requests!.resetAt;
    const expected = 60 * 60 * 1000 + 30 * 60 * 1000;
    assert.ok(reset >= before + expected - 100 && reset <= Date.now() + expected + 100, `got ${reset}`);
  });

  it('falls back to parseDuration for a millisecond reset format', () => {
    const before = Date.now();
    const t = new RateLimitTracker();
    t.update({
      'x-ratelimit-limit-tokens': '10',
      'x-ratelimit-remaining-tokens': '5',
      'x-ratelimit-reset-tokens': '500ms',
    });
    const reset = t.getStatus().tokens!.resetAt;
    assert.ok(reset >= before + 500 - 100 && reset <= Date.now() + 500 + 100, `got ${reset}`);
  });

  it('defaults parseDuration to 60s when the reset value has no digits', () => {
    const before = Date.now();
    const t = new RateLimitTracker();
    t.update({
      'x-ratelimit-limit-requests': '10',
      'x-ratelimit-remaining-requests': '5',
      'x-ratelimit-reset-requests': 'later', // no digits, not a date -> 60000 default
    });
    const reset = t.getStatus().requests!.resetAt;
    assert.ok(reset >= before + 60_000 - 100 && reset <= Date.now() + 60_000 + 100, `got ${reset}`);
  });

  it('sets resetAt to 0 when no reset header is supplied', () => {
    const t = new RateLimitTracker();
    t.update({
      'x-ratelimit-limit-requests': '10',
      'x-ratelimit-remaining-requests': '5',
    });
    assert.equal(t.getStatus().requests!.resetAt, 0);
  });

  it('overwrites an earlier bucket on a subsequent update', () => {
    const t = new RateLimitTracker();
    t.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '90',
    });
    assert.equal(t.getStatus().requestsPercent, 10);
    t.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '10',
    });
    assert.equal(t.getStatus().requestsPercent, 90);
    assert.equal(t.getStatus().shouldWarn, true);
  });

  it('rounds a fractional consumed percent to the nearest integer', () => {
    const t = new RateLimitTracker();
    t.update({
      'x-ratelimit-limit-requests': '3',
      'x-ratelimit-remaining-requests': '2', // 1/3 = 33.33% -> round 33
    });
    assert.equal(t.getStatus().requestsPercent, 33);
  });
});
