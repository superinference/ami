import { describe, it, mock } from 'node:test';
import * as assert from 'node:assert/strict';

import { ProviderCoordinator } from '../src/provider-coordinator';
import { RateLimitTracker } from '../src/rate-limiter';
import type { ProviderConfig, ProviderSubsystem } from '../src/types';

// ---------------------------------------------------------------------------
// U244 — Server-directed (Retry-After) cooldown/backoff delay.
//
// Claude Code's getRetryDelay (services/api/withRetry.ts:530-548) parses the
// server `retry-after` header and uses it verbatim as the delay, only falling
// back to exponential backoff when the server did not direct one. AMI already
// parses the header (rate-limit-parser) but the coordinator did NOT feed it into
// a credential cooldown — markCredentialExhausted used a fixed escalating 60s
// default regardless of the directive the coordinator already held.
//
// This suite proves: (1) RateLimitTracker now surfaces the parsed Retry-After via
// getStatus().retryAfterMs, and (2) markCredentialExhausted defaults the cooldown
// to that server directive when the caller omits one, honoring it verbatim.
// ---------------------------------------------------------------------------

function makeSubsystem(overrides?: Partial<ProviderSubsystem>): ProviderSubsystem {
  const primary: ProviderConfig = {
    baseUrl: 'https://api.openai.com/v1',
    apiKey: 'sk-test-key',
    model: 'gpt-4o',
  };
  return { primary, ...overrides };
}

describe('RateLimitTracker — server Retry-After exposure (U244)', () => {
  it('reports null retryAfterMs before any directive', () => {
    const t = new RateLimitTracker();
    assert.equal(t.getStatus().retryAfterMs, null);
  });

  it('surfaces the parsed Retry-After (seconds → ms) from update()', () => {
    const t = new RateLimitTracker();
    t.update({ 'retry-after': '2' });
    assert.equal(t.getStatus().retryAfterMs, 2000);
  });

  it('clears a stale directive when a later response omits the header', () => {
    const t = new RateLimitTracker();
    t.update({ 'retry-after': '5' });
    assert.equal(t.getStatus().retryAfterMs, 5000);
    // A subsequent (e.g. successful) response carries no retry-after → cleared.
    t.update({ 'x-ratelimit-remaining-requests': '100', 'x-ratelimit-limit-requests': '100' });
    assert.equal(t.getStatus().retryAfterMs, null);
  });
});

describe('ProviderCoordinator — server-directed credential cooldown (U244)', () => {
  it('honors the server Retry-After verbatim when no explicit cooldown is given', () => {
    mock.timers.enable({ apis: ['Date'] });
    try {
      const coord = new ProviderCoordinator(makeSubsystem(), ['sk-second-key']);
      // Server directs a 2s cooldown; coordinator ingests it via updateRateLimits.
      coord.updateRateLimits({ 'retry-after': '2' });

      // Exhaust BOTH pooled credentials (acquire sets the active id each time).
      coord.getConfig();
      coord.markCredentialExhausted();
      coord.getConfig();
      coord.markCredentialExhausted();

      assert.equal(coord.availableCredentials, 0, 'both credentials cooling down');

      // At the server-directed instant the pool recovers — the 60s default would not.
      mock.timers.tick(2001);
      assert.equal(coord.availableCredentials, 2, 'recovered at the server-directed 2s mark');
    } finally {
      mock.timers.reset();
    }
  });

  it('falls back to the escalating default cooldown when the server directs none', () => {
    mock.timers.enable({ apis: ['Date'] });
    try {
      const coord = new ProviderCoordinator(makeSubsystem(), ['sk-second-key']);
      // No updateRateLimits → no server directive.
      coord.getConfig();
      coord.markCredentialExhausted();
      coord.getConfig();
      coord.markCredentialExhausted();

      // Server-directed window would have recovered by 2s; the 60s default has not.
      mock.timers.tick(2001);
      assert.equal(coord.availableCredentials, 0, 'still cooling under the 60s default');

      mock.timers.tick(60_000);
      assert.equal(coord.availableCredentials, 2, 'recovered after the default cooldown');
    } finally {
      mock.timers.reset();
    }
  });

  it('an explicit cooldown argument overrides the server directive', () => {
    mock.timers.enable({ apis: ['Date'] });
    try {
      const coord = new ProviderCoordinator(makeSubsystem(), ['sk-second-key']);
      coord.updateRateLimits({ 'retry-after': '2' }); // server says 2s…
      coord.getConfig();
      coord.markCredentialExhausted(30_000); // …but the caller forces 30s.
      coord.getConfig();
      coord.markCredentialExhausted(30_000);

      mock.timers.tick(2001);
      assert.equal(coord.availableCredentials, 0, 'explicit 30s cooldown wins over the 2s directive');

      mock.timers.tick(30_000);
      assert.equal(coord.availableCredentials, 2, 'recovered after the explicit cooldown');
    } finally {
      mock.timers.reset();
    }
  });

  it('is a no-op when no credential pool is configured', () => {
    // Single-key (no additionalKeys) → no pool; the call must not throw.
    const coord = new ProviderCoordinator(makeSubsystem());
    coord.updateRateLimits({ 'retry-after': '2' });
    assert.doesNotThrow(() => coord.markCredentialExhausted());
    assert.equal(coord.availableCredentials, 1);
  });
});
