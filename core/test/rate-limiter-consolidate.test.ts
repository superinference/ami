import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { RateLimitTracker } from '../src/rate-limiter';

// Consolidation of RateLimitTracker.update() onto parseRateLimitHeaders.
// These tests prove (a) the original x-ratelimit-* header parsing still works
// and (b) the tracker now understands the shared parser's superset of headers
// (the generic `ratelimit-limit` / `ratelimit-remaining` fallbacks).
describe('RateLimitTracker – parser consolidation', () => {
  it('still parses the canonical x-ratelimit-* headers', () => {
    const tracker = new RateLimitTracker();
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '80',
      'x-ratelimit-limit-tokens': '1000000',
      'x-ratelimit-remaining-tokens': '900000',
    });

    const status = tracker.getStatus();
    assert.equal(status.requestsPercent, 20);
    assert.equal(status.tokensPercent, 10);
    assert.ok(!status.shouldWarn);
  });

  // RED on current code: update() only reads x-ratelimit-limit-requests /
  // x-ratelimit-remaining-requests, so the generic ratelimit-* fallbacks are
  // ignored, leaving requests=null -> requestsPercent 0 and shouldWarn false.
  // GREEN after consolidation: parseRateLimitHeaders resolves these fallbacks.
  it('now handles the parser superset: generic ratelimit-limit/ratelimit-remaining', () => {
    const tracker = new RateLimitTracker();
    tracker.update({
      'ratelimit-limit': '100',
      'ratelimit-remaining': '15',
    });

    const status = tracker.getStatus();
    assert.equal(status.requestsPercent, 85);
    assert.ok(status.shouldWarn);
    assert.ok(status.requests);
    assert.equal(status.requests!.limit, 100);
    assert.equal(status.requests!.remaining, 15);
  });

  it('canonical requests headers win over the generic fallback', () => {
    const tracker = new RateLimitTracker();
    tracker.update({
      'x-ratelimit-limit-requests': '200',
      'x-ratelimit-remaining-requests': '100',
      'ratelimit-limit': '100',
      'ratelimit-remaining': '15',
    });

    const status = tracker.getStatus();
    assert.equal(status.requestsPercent, 50);
    assert.ok(!status.shouldWarn);
  });

  it('preserves the s / bare-number reset handling via parser resetAt', () => {
    const tracker = new RateLimitTracker();
    const before = Date.now();
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '50',
      'x-ratelimit-reset-requests': '30s',
    });
    const status = tracker.getStatus();
    assert.ok(status.requests);
    assert.ok(status.requests!.resetAt >= before + 30000 - 1000);
  });

  it('preserves ms/m/h/compound reset handling via parseDuration fallback', () => {
    const tracker = new RateLimitTracker();
    const before = Date.now();
    tracker.update({
      'x-ratelimit-limit-tokens': '1000000',
      'x-ratelimit-remaining-tokens': '500000',
      'x-ratelimit-reset-tokens': '2m',
    });
    const status = tracker.getStatus();
    assert.ok(status.tokens);
    const elapsed = status.tokens!.resetAt - before;
    assert.ok(elapsed >= 120000 - 100 && elapsed <= 120000 + 1000,
      `Expected token resetAt ~2m in future, got ${elapsed}ms`);
  });

  it('handles missing headers gracefully', () => {
    const tracker = new RateLimitTracker();
    tracker.update({});
    const status = tracker.getStatus();
    assert.equal(status.requestsPercent, 0);
    assert.equal(status.tokensPercent, 0);
    assert.ok(!status.shouldWarn);
  });

  it('treats limit=0 as fully consumed without NaN', () => {
    const tracker = new RateLimitTracker();
    tracker.update({
      'x-ratelimit-limit-requests': '0',
      'x-ratelimit-remaining-requests': '0',
    });
    const status = tracker.getStatus();
    assert.equal(status.requestsPercent, 100);
    assert.ok(status.shouldWarn);
    assert.ok(!Number.isNaN(status.requestsPercent));
  });
});
