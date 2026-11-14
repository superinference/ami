import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { RateLimitTracker } from '../src/rate-limiter';

describe('RateLimitTracker — bounded reset duration', () => {
  it('clamps a hostile/absurd reset duration to a 24h ceiling', () => {
    const t = new RateLimitTracker();
    t.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '10',
      'x-ratelimit-reset-requests': '999999999h',
    });
    const s = t.getStatus();
    assert.ok(s.requests);
    const deltaMs = s.requests!.resetAt - Date.now();
    // Must not be an unbounded far-future timestamp.
    assert.ok(deltaMs <= 24 * 60 * 60 * 1000 + 1000, `resetAt not clamped: ${deltaMs}ms`);
    assert.ok(deltaMs > 0);
  });

  it('ignores a non-finite / garbage duration rather than producing NaN', () => {
    const t = new RateLimitTracker();
    t.update({
      'x-ratelimit-limit-tokens': '1000',
      'x-ratelimit-remaining-tokens': '500',
      'x-ratelimit-reset-tokens': 'not-a-duration',
    });
    const s = t.getStatus();
    assert.ok(s.tokens);
    assert.ok(Number.isFinite(s.tokens!.resetAt));
  });
});

describe('RateLimitTracker — burn-rate early warning', () => {
  it('warns early when usage is high but little of the window has elapsed', () => {
    const t = new RateLimitTracker();
    // First update right after a reset: full window (~5h) remaining, low usage.
    t.update({
      'x-ratelimit-limit-requests': '1000',
      'x-ratelimit-remaining-requests': '1000',
      'x-ratelimit-reset-requests': '18000s', // 5h window observed
    });
    // Now 95% consumed but still ~5h (i.e. ~0% of the window) remaining.
    t.update({
      'x-ratelimit-limit-requests': '1000',
      'x-ratelimit-remaining-requests': '50',
      'x-ratelimit-reset-requests': '18000s',
    });
    const s = t.getStatus();
    assert.equal(s.shouldWarn, true);
    assert.ok(s.warnMessage, 'expected a user-facing warn message');
    assert.equal(s.severity, 'error'); // >=95% utilization
  });

  it('does not burn-rate-warn at modest usage late in the window', () => {
    const t = new RateLimitTracker();
    t.update({
      'x-ratelimit-limit-requests': '1000',
      'x-ratelimit-remaining-requests': '1000',
      'x-ratelimit-reset-requests': '18000s',
    });
    // 30% used, window almost fully elapsed (1s left) → on pace, no early warn.
    t.update({
      'x-ratelimit-limit-requests': '1000',
      'x-ratelimit-remaining-requests': '700',
      'x-ratelimit-reset-requests': '1s',
    });
    const s = t.getStatus();
    assert.equal(s.shouldWarn, false);
    assert.equal(s.warnMessage, undefined);
  });

  it('still warns on the static 80% boundary', () => {
    const t = new RateLimitTracker();
    t.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '15',
      'x-ratelimit-reset-requests': '60s',
    });
    const s = t.getStatus();
    assert.equal(s.shouldWarn, true);
    assert.equal(s.severity, 'warning');
    assert.ok(s.warnMessage!.includes('%'));
  });
});
