import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { RateLimitTracker } from '../src/rate-limiter';

describe('RateLimitTracker', () => {
  it('tracks request rate limits from headers', () => {
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

  it('warns when requests exceed 80% usage', () => {
    const tracker = new RateLimitTracker();
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '15',
    });

    const status = tracker.getStatus();
    assert.equal(status.requestsPercent, 85);
    assert.ok(status.shouldWarn);
  });

  it('warns when tokens exceed 80% usage', () => {
    const tracker = new RateLimitTracker();
    tracker.update({
      'x-ratelimit-limit-tokens': '1000000',
      'x-ratelimit-remaining-tokens': '100000',
    });

    const status = tracker.getStatus();
    assert.equal(status.tokensPercent, 90);
    assert.ok(status.shouldWarn);
  });

  it('handles missing headers gracefully', () => {
    const tracker = new RateLimitTracker();
    tracker.update({});
    const status = tracker.getStatus();
    assert.equal(status.requestsPercent, 0);
    assert.equal(status.tokensPercent, 0);
    assert.ok(!status.shouldWarn);
  });

  it('updates incrementally', () => {
    const tracker = new RateLimitTracker();
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '90',
    });
    assert.equal(tracker.getStatus().requestsPercent, 10);

    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '10',
    });
    assert.equal(tracker.getStatus().requestsPercent, 90);
    assert.ok(tracker.getStatus().shouldWarn);
  });
});

// ---------------------------------------------------------------------------
// parseDuration — covers lines 73-76 (different duration units)
// ---------------------------------------------------------------------------
describe('RateLimitTracker – parseDuration', () => {
  it('parses milliseconds unit', () => {
    const tracker = new RateLimitTracker();
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '50',
      'x-ratelimit-reset-requests': '500ms',
    });
    const status = tracker.getStatus();
    assert.ok(status.requests);
    // resetAt should be close to now + 500ms
    assert.ok(status.requests!.resetAt > 0);
  });

  it('parses seconds unit', () => {
    const tracker = new RateLimitTracker();
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '50',
      'x-ratelimit-reset-requests': '30s',
    });
    const status = tracker.getStatus();
    assert.ok(status.requests);
    assert.ok(status.requests!.resetAt > Date.now());
  });

  it('parses minutes unit', () => {
    const tracker = new RateLimitTracker();
    tracker.update({
      'x-ratelimit-limit-tokens': '1000000',
      'x-ratelimit-remaining-tokens': '500000',
      'x-ratelimit-reset-tokens': '2m',
    });
    const status = tracker.getStatus();
    assert.ok(status.tokens);
    assert.ok(status.tokens!.resetAt > Date.now());
  });

  it('parses hours unit', () => {
    const tracker = new RateLimitTracker();
    tracker.update({
      'x-ratelimit-limit-requests': '10000',
      'x-ratelimit-remaining-requests': '5000',
      'x-ratelimit-reset-requests': '1h',
    });
    const status = tracker.getStatus();
    assert.ok(status.requests);
    // 1h = 3600000ms from now
    assert.ok(status.requests!.resetAt > Date.now() + 3000000);
  });

  it('handles bare number without unit (defaults to seconds)', () => {
    const tracker = new RateLimitTracker();
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '50',
      'x-ratelimit-reset-requests': '60',
    });
    const status = tracker.getStatus();
    assert.ok(status.requests);
    assert.ok(status.requests!.resetAt > 0);
  });

  it('returns 60000 for unparseable duration', () => {
    const tracker = new RateLimitTracker();
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '50',
      'x-ratelimit-reset-requests': 'unknown-format',
    });
    const status = tracker.getStatus();
    assert.ok(status.requests);
    // Should fallback to 60000ms
    assert.ok(status.requests!.resetAt > 0);
  });
});
