import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { RateLimitTracker } from '../src/rate-limiter';

const DAY_MS = 24 * 60 * 60 * 1000;

describe('RateLimitTracker — configurable warn/error thresholds', () => {
  it('warns below the default boundary when a lower warnThreshold is configured', () => {
    // 60% used: would NOT warn at the default 0.80, but DOES at 0.50.
    const custom = new RateLimitTracker({ warnThreshold: 0.5 });
    custom.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '40', // 60% consumed
      'x-ratelimit-reset-requests': '60s',
    });
    assert.equal(custom.getStatus().shouldWarn, true);

    const dflt = new RateLimitTracker();
    dflt.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '40',
      'x-ratelimit-reset-requests': '60s',
    });
    assert.equal(dflt.getStatus().shouldWarn, false);
  });

  it('suppresses a near-limit warning when a higher warnThreshold is configured', () => {
    // 85% used: warns at default 0.80, but NOT at 0.99.
    const t = new RateLimitTracker({ warnThreshold: 0.99 });
    t.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '15', // 85% consumed
      'x-ratelimit-reset-requests': '60s',
    });
    assert.equal(t.getStatus().shouldWarn, false);
  });

  it('escalates to error severity at a lower configured errorThreshold', () => {
    // 70% used: 'warning' at default 0.95 error cutoff, 'error' at 0.60.
    const t = new RateLimitTracker({ warnThreshold: 0.5, errorThreshold: 0.6 });
    t.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '30', // 70% consumed
      'x-ratelimit-reset-requests': '60s',
    });
    const s = t.getStatus();
    assert.equal(s.shouldWarn, true);
    assert.equal(s.severity, 'error');

    const dflt = new RateLimitTracker({ warnThreshold: 0.5 });
    dflt.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '30',
      'x-ratelimit-reset-requests': '60s',
    });
    assert.equal(dflt.getStatus().severity, 'warning');
  });

  it('clamps an out-of-range (>1) warnThreshold down to 1.0 instead of trusting it', () => {
    // No reset header → resetAt is 0, so burn-rate can't fire and the static
    // threshold is the sole warn signal. Without clamping, 1.5 would make even
    // 100% usage (reqPct 1.0 >= 1.5 === false) never warn; clamped to 1.0 it warns.
    const t = new RateLimitTracker({ warnThreshold: 1.5 });
    t.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '0', // 100% consumed
    });
    assert.equal(t.getStatus().shouldWarn, true);
  });

  it('clamps a negative warnThreshold up to 0 without crashing or inverting logic', () => {
    // Negative is clamped to 0 (exercises the lower-bound clamp); at 0% usage the
    // bucket still reports a warn — a sane, non-inverted outcome.
    const t = new RateLimitTracker({ warnThreshold: -5 });
    t.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '100', // 0% consumed
    });
    assert.equal(t.getStatus().shouldWarn, true);
  });

  it('falls back to the default when a non-finite threshold is supplied', () => {
    // NaN must fall back to 0.80, not be used directly (0.85 >= NaN === false).
    const t = new RateLimitTracker({ warnThreshold: Number.NaN });
    t.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '15', // 85% consumed
      'x-ratelimit-reset-requests': '60s',
    });
    assert.equal(t.getStatus().shouldWarn, true);
  });
});

describe('RateLimitTracker — clamps absolute (epoch) reset timestamps', () => {
  it('caps a far-future absolute request reset at the 24h ceiling', () => {
    const t = new RateLimitTracker();
    const epoch48h = Math.floor(Date.now() / 1000) + 48 * 60 * 60; // absolute, 48h out
    t.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '10',
      'x-ratelimit-reset-requests': String(epoch48h),
    });
    const s = t.getStatus();
    assert.ok(s.requests);
    const deltaMs = s.requests!.resetAt - Date.now();
    assert.ok(deltaMs <= DAY_MS + 1000, `request resetAt not clamped: ${deltaMs}ms`);
    assert.ok(deltaMs > 0);
  });

  it('caps a far-future absolute token reset at the 24h ceiling', () => {
    const t = new RateLimitTracker();
    const epoch48h = Math.floor(Date.now() / 1000) + 48 * 60 * 60;
    t.update({
      'x-ratelimit-limit-tokens': '1000',
      'x-ratelimit-remaining-tokens': '500',
      'x-ratelimit-reset-tokens': String(epoch48h),
    });
    const s = t.getStatus();
    assert.ok(s.tokens);
    const deltaMs = s.tokens!.resetAt - Date.now();
    assert.ok(deltaMs <= DAY_MS + 1000, `token resetAt not clamped: ${deltaMs}ms`);
    assert.ok(deltaMs > 0);
  });

  it('preserves an absolute reset that is within the 24h ceiling', () => {
    const t = new RateLimitTracker();
    const epoch1h = Math.floor(Date.now() / 1000) + 60 * 60; // absolute, 1h out
    t.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '10',
      'x-ratelimit-reset-requests': String(epoch1h),
    });
    const s = t.getStatus();
    assert.ok(s.requests);
    const deltaMs = s.requests!.resetAt - Date.now();
    // ~1h, well under the ceiling and not truncated to it.
    assert.ok(deltaMs > 50 * 60 * 1000 && deltaMs < 70 * 60 * 1000, `unexpected delta: ${deltaMs}ms`);
  });
});
