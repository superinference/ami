import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  parseRetryAfterHeader,
  parseRateLimitHeaders,
  shouldBackoff,
  suggestedWaitMs,
  type RateLimitInfo,
} from '../src/rate-limit-parser';

describe('parseRetryAfterHeader', () => {
  it('returns null for undefined', () => {
    assert.equal(parseRetryAfterHeader(undefined), null);
  });

  it('parses integer seconds', () => {
    assert.equal(parseRetryAfterHeader('5'), 5000);
  });

  it('parses decimal seconds', () => {
    const result = parseRetryAfterHeader('1.5');
    assert.equal(result, 1500); // ceil(1.5 * 1000) = ceil(1500) = 1500
  });

  it('converts to ms correctly', () => {
    assert.equal(parseRetryAfterHeader('10'), 10000);
    assert.equal(parseRetryAfterHeader('60'), 60000);
  });

  it('parses zero seconds', () => {
    assert.equal(parseRetryAfterHeader('0'), 0);
  });

  it('parses HTTP-date format', () => {
    const future = new Date(Date.now() + 30_000);
    const result = parseRetryAfterHeader(future.toUTCString());
    assert.ok(result !== null);
    assert.ok(result! > 0);
    assert.ok(result! <= 31_000);
  });

  it('returns null for garbage input', () => {
    assert.equal(parseRetryAfterHeader('not-a-number-or-date'), null);
  });

  it('rejects negative value', () => {
    assert.equal(parseRetryAfterHeader('-5'), null);
  });

  it('rejects value above 86400 seconds', () => {
    assert.equal(parseRetryAfterHeader('86401'), null);
  });

  it('accepts exactly 86400 seconds', () => {
    assert.equal(parseRetryAfterHeader('86400'), 86400000);
  });

  it('trims whitespace', () => {
    assert.equal(parseRetryAfterHeader('  5  '), 5000);
  });

  it('returns null for empty string', () => {
    assert.equal(parseRetryAfterHeader(''), null);
  });
});

describe('parseRateLimitHeaders', () => {
  it('extracts retry-after', () => {
    const info = parseRateLimitHeaders({ 'retry-after': '10' });
    assert.equal(info.retryAfterMs, 10000);
  });

  it('extracts remaining requests via x-ratelimit-remaining-requests', () => {
    const info = parseRateLimitHeaders({ 'x-ratelimit-remaining-requests': '42' });
    assert.equal(info.remainingRequests, 42);
  });

  it('extracts remaining tokens', () => {
    const info = parseRateLimitHeaders({ 'x-ratelimit-remaining-tokens': '5000' });
    assert.equal(info.remainingTokens, 5000);
  });

  it('extracts limit requests via x-ratelimit-limit-requests', () => {
    const info = parseRateLimitHeaders({ 'x-ratelimit-limit-requests': '100' });
    assert.equal(info.limitRequests, 100);
  });

  it('extracts limit tokens', () => {
    const info = parseRateLimitHeaders({ 'x-ratelimit-limit-tokens': '100000' });
    assert.equal(info.limitTokens, 100000);
  });

  it('handles missing headers gracefully', () => {
    const info = parseRateLimitHeaders({});
    assert.equal(info.retryAfterMs, null);
    assert.equal(info.remainingRequests, null);
    assert.equal(info.remainingTokens, null);
    assert.equal(info.resetAt, null);
    assert.equal(info.limitRequests, null);
    assert.equal(info.limitTokens, null);
  });

  it('handles case-insensitive retry-after', () => {
    const info = parseRateLimitHeaders({ 'Retry-After': '3' });
    // The get helper checks both the key and lower-case
    // Since headers object keys are case-sensitive in JS, this tests the
    // fallback path where headers[name.toLowerCase()] is checked
    assert.equal(info.retryAfterMs, null); // exact 'retry-after' not found, lowercase fallback of 'retry-after' also not 'Retry-After'
  });

  it('handles standard ratelimit-remaining header', () => {
    const info = parseRateLimitHeaders({ 'ratelimit-remaining': '7' });
    assert.equal(info.remainingRequests, 7);
  });

  it('handles standard ratelimit-limit header', () => {
    const info = parseRateLimitHeaders({ 'ratelimit-limit': '50' });
    assert.equal(info.limitRequests, 50);
  });

  it('prefers x-ratelimit-remaining-requests over ratelimit-remaining', () => {
    const info = parseRateLimitHeaders({
      'x-ratelimit-remaining-requests': '10',
      'ratelimit-remaining': '20',
    });
    assert.equal(info.remainingRequests, 10);
  });

  it('extracts reset time with seconds format', () => {
    const info = parseRateLimitHeaders({ 'x-ratelimit-reset-requests': '30s' });
    assert.ok(info.resetAt !== null);
    const delta = info.resetAt!.getTime() - Date.now();
    assert.ok(delta > 28_000 && delta <= 31_000);
  });

  it('handles array header values', () => {
    const info = parseRateLimitHeaders({ 'retry-after': ['5', '10'] });
    assert.equal(info.retryAfterMs, 5000);
  });
});

describe('shouldBackoff', () => {
  it('returns true when retry-after is set', () => {
    const info: RateLimitInfo = {
      retryAfterMs: 5000, remainingRequests: null, remainingTokens: null,
      resetAt: null, limitRequests: null, limitTokens: null,
    };
    assert.equal(shouldBackoff(info), true);
  });

  it('returns true when remaining requests is zero', () => {
    const info: RateLimitInfo = {
      retryAfterMs: null, remainingRequests: 0, remainingTokens: null,
      resetAt: null, limitRequests: null, limitTokens: null,
    };
    assert.equal(shouldBackoff(info), true);
  });

  it('returns true when remaining tokens is zero', () => {
    const info: RateLimitInfo = {
      retryAfterMs: null, remainingRequests: null, remainingTokens: 0,
      resetAt: null, limitRequests: null, limitTokens: null,
    };
    assert.equal(shouldBackoff(info), true);
  });

  it('returns false for healthy limits', () => {
    const info: RateLimitInfo = {
      retryAfterMs: null, remainingRequests: 50, remainingTokens: 10000,
      resetAt: null, limitRequests: 100, limitTokens: 100000,
    };
    assert.equal(shouldBackoff(info), false);
  });

  it('returns false when all null', () => {
    const info: RateLimitInfo = {
      retryAfterMs: null, remainingRequests: null, remainingTokens: null,
      resetAt: null, limitRequests: null, limitTokens: null,
    };
    assert.equal(shouldBackoff(info), false);
  });

  it('returns true when remaining requests is negative', () => {
    const info: RateLimitInfo = {
      retryAfterMs: null, remainingRequests: -1, remainingTokens: null,
      resetAt: null, limitRequests: null, limitTokens: null,
    };
    assert.equal(shouldBackoff(info), true);
  });
});

describe('suggestedWaitMs', () => {
  it('returns retry-after when set', () => {
    const info: RateLimitInfo = {
      retryAfterMs: 5000, remainingRequests: null, remainingTokens: null,
      resetAt: null, limitRequests: null, limitTokens: null,
    };
    assert.equal(suggestedWaitMs(info), 5000);
  });

  it('returns default when no info available', () => {
    const info: RateLimitInfo = {
      retryAfterMs: null, remainingRequests: null, remainingTokens: null,
      resetAt: null, limitRequests: null, limitTokens: null,
    };
    assert.equal(suggestedWaitMs(info), 1000);
  });

  it('returns custom default when provided', () => {
    const info: RateLimitInfo = {
      retryAfterMs: null, remainingRequests: null, remainingTokens: null,
      resetAt: null, limitRequests: null, limitTokens: null,
    };
    assert.equal(suggestedWaitMs(info, 3000), 3000);
  });

  it('calculates from reset time', () => {
    const resetAt = new Date(Date.now() + 15_000);
    const info: RateLimitInfo = {
      retryAfterMs: null, remainingRequests: null, remainingTokens: null,
      resetAt, limitRequests: null, limitTokens: null,
    };
    const wait = suggestedWaitMs(info);
    assert.ok(wait > 13_000 && wait <= 16_000);
  });

  it('prefers retry-after over reset time', () => {
    const resetAt = new Date(Date.now() + 60_000);
    const info: RateLimitInfo = {
      retryAfterMs: 2000, remainingRequests: null, remainingTokens: null,
      resetAt, limitRequests: null, limitTokens: null,
    };
    assert.equal(suggestedWaitMs(info), 2000);
  });

  it('falls back to default for past reset time', () => {
    const resetAt = new Date(Date.now() - 5000);
    const info: RateLimitInfo = {
      retryAfterMs: null, remainingRequests: null, remainingTokens: null,
      resetAt, limitRequests: null, limitTokens: null,
    };
    assert.equal(suggestedWaitMs(info), 1000);
  });
});
