/**
 * rate-limiter-stale-state-u222 — PARITY (additive) for RateLimitTracker.getStatus().
 *
 * Two confirmed gaps against Claude's services/rateLimitMessages.ts:
 *
 *  1. Stale post-reset-window guard (rateLimitMessages.ts:68-78). getStatus()
 *     recomputed percent/shouldWarn purely from the last-seen remaining/limit and
 *     never consulted bucket.resetAt — so after a reset window elapsed the tracker
 *     kept reporting the old high percent (shouldWarn latched on forever) against
 *     data known to be invalid. A bucket whose resetAt is in the past must be
 *     treated as REFILLED (remaining===limit, 0% used) until the next update().
 *
 *  2. Multi-tier state (rateLimitMessages.ts:36-98). The single `shouldWarn`
 *     boolean cannot distinguish "approaching" (warn) from "at/over limit" (hard
 *     stop). getStatus() now also returns `state: 'ok'|'warning'|'exceeded'`.
 *
 * Run (core):
 *   cd core/core && node --import tsx --test test/rate-limiter-stale-state-u222.test.ts
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { RateLimitTracker } from '../src/rate-limiter';

// Absolute epoch-SECONDS reset far in the past (Jan 2001). The parser treats a
// value >= 1e9 as an absolute instant, so this lands resetAt in the past.
const PAST_EPOCH_SECONDS = '1000000000';

describe('RateLimitTracker.getStatus — stale post-reset-window guard (gap 1)', () => {
  it('treats a bucket whose reset window has elapsed as refilled (0% used, no warn)', () => {
    const tracker = new RateLimitTracker();
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '10', // 90% used at capture time
      'x-ratelimit-reset-requests': PAST_EPOCH_SECONDS, // window already elapsed
    });

    const status = tracker.getStatus();
    assert.equal(status.requestsPercent, 0, 'stale bucket must report 0% used');
    assert.equal(status.shouldWarn, false, 'stale bucket must not latch a warning');
    assert.equal(status.state, 'ok');
    assert.ok(status.requests);
    assert.equal(status.requests!.remaining, status.requests!.limit, 'stale bucket reads as refilled');
    assert.equal(status.warnMessage, undefined);
  });

  it('does NOT treat a FUTURE reset window as stale (regression lock)', () => {
    const tracker = new RateLimitTracker();
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '10',
      'x-ratelimit-reset-requests': '120s', // future
    });
    const status = tracker.getStatus();
    assert.equal(status.requestsPercent, 90);
    assert.ok(status.shouldWarn);
  });

  it('does NOT treat the resetAt===0 "unknown" sentinel as stale', () => {
    const tracker = new RateLimitTracker();
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '10', // 90% used, no reset header → resetAt 0
    });
    const status = tracker.getStatus();
    assert.ok(status.requests);
    assert.equal(status.requests!.resetAt, 0);
    assert.equal(status.requestsPercent, 90, 'unknown reset is not a refill');
    assert.ok(status.shouldWarn);
  });

  it('refills only the stale bucket while a fresh over-limit bucket still warns', () => {
    const tracker = new RateLimitTracker();
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '5', // 95% used but window elapsed → refilled
      'x-ratelimit-reset-requests': PAST_EPOCH_SECONDS,
      'x-ratelimit-limit-tokens': '1000000',
      'x-ratelimit-remaining-tokens': '50000', // 95% used, fresh window → warns
      'x-ratelimit-reset-tokens': '300s',
    });
    const status = tracker.getStatus();
    assert.equal(status.requestsPercent, 0, 'stale request bucket refilled');
    assert.equal(status.tokensPercent, 95, 'fresh token bucket keeps its utilization');
    assert.ok(status.shouldWarn, 'fresh bucket at 95% still warns');
    assert.equal(status.state, 'exceeded');
  });

  it('re-reports the latched warning after a fresh update() supplies new headers', () => {
    const tracker = new RateLimitTracker();
    // Stale → refilled.
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '2',
      'x-ratelimit-reset-requests': PAST_EPOCH_SECONDS,
    });
    assert.equal(tracker.getStatus().shouldWarn, false);
    // Fresh headers restore the real picture.
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '2',
      'x-ratelimit-reset-requests': '120s',
    });
    const status = tracker.getStatus();
    assert.equal(status.requestsPercent, 98);
    assert.ok(status.shouldWarn);
  });
});

describe('RateLimitTracker.getStatus — multi-tier state (gap 2)', () => {
  it('reports state "ok" when utilization is below the warn threshold', () => {
    const tracker = new RateLimitTracker();
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '70', // 30% used
    });
    const status = tracker.getStatus();
    assert.equal(status.state, 'ok');
    assert.equal(status.shouldWarn, false);
  });

  it('reports state "warning" when approaching the limit (>=warn, <error)', () => {
    const tracker = new RateLimitTracker();
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '15', // 85% used
    });
    const status = tracker.getStatus();
    assert.equal(status.state, 'warning');
    assert.equal(status.severity, 'warning');
    assert.ok(status.shouldWarn);
  });

  it('reports state "exceeded" at/over the error threshold', () => {
    const tracker = new RateLimitTracker();
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '2', // 98% used
    });
    const status = tracker.getStatus();
    assert.equal(status.state, 'exceeded');
    assert.equal(status.severity, 'error');
  });

  it('reports state "exceeded" when a bucket has no remaining budget', () => {
    const tracker = new RateLimitTracker();
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '0', // fully exhausted
    });
    const status = tracker.getStatus();
    assert.equal(status.state, 'exceeded');
  });

  it('reports state "exceeded" on a rejected unified tier even without per-bucket headers', () => {
    const tracker = new RateLimitTracker();
    tracker.update({ 'anthropic-ratelimit-unified-status': 'rejected' });
    const status = tracker.getStatus();
    assert.equal(status.state, 'exceeded', 'a rejected primary tier is a hard stop');
  });
});
