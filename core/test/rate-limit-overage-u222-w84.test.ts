/**
 * rate-limit-overage-u222-w84 — two-tier (primary + overage/spillover) rate-limit
 * decision + raw per-window utilization accessor (PARITY, additive).
 *
 * Covers:
 *  - parser: unified `anthropic-ratelimit-unified-*` header parsing,
 *    {@link isHardRateLimited} two-tier decision, and {@link shouldBackoff}
 *    tolerating a soft overage (primary rejected, overage allowed).
 *  - RateLimitTracker: overage/isUsingOverage/representativeClaim on getStatus,
 *    and {@link RateLimitTracker.getRawUtilization} tracked on every response.
 *  - ProviderCoordinator: getRawUtilization passthrough.
 *
 * Run: cd core/core && node --import tsx --test test/rate-limit-overage-u222-w84.test.ts
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  parseRateLimitHeaders,
  isHardRateLimited,
  shouldBackoff,
} from '../src/rate-limit-parser';
import { RateLimitTracker } from '../src/rate-limiter';
import { ProviderCoordinator } from '../src/provider-coordinator';
import type { ProviderSubsystem } from '../src/types';

describe('parseRateLimitHeaders — unified two-tier fields', () => {
  it('parses unified status, overage status, reset and representative claim', () => {
    const info = parseRateLimitHeaders({
      'anthropic-ratelimit-unified-status': 'rejected',
      'anthropic-ratelimit-unified-overage-status': 'allowed',
      'anthropic-ratelimit-unified-overage-reset': '1800000000',
      'anthropic-ratelimit-unified-representative-claim': 'seven_day_opus',
    });
    assert.equal(info.unifiedStatus, 'rejected');
    assert.equal(info.overageStatus, 'allowed');
    assert.equal(info.representativeClaim, 'seven_day_opus');
    assert.ok(info.overageResetAt instanceof Date);
    assert.equal(Math.floor(info.overageResetAt!.getTime() / 1000), 1800000000);
  });

  it('leaves unified fields null when absent and trims surrounding whitespace', () => {
    const absent = parseRateLimitHeaders({});
    assert.equal(absent.unifiedStatus, null);
    assert.equal(absent.overageStatus, null);
    assert.equal(absent.overageResetAt, null);
    assert.equal(absent.representativeClaim, null);

    const trimmed = parseRateLimitHeaders({ 'anthropic-ratelimit-unified-status': '  rejected  ' });
    assert.equal(trimmed.unifiedStatus, 'rejected');
  });
});

describe('isHardRateLimited — two-tier decision (parity: rateLimitMocking shouldThrow429)', () => {
  const info = (unifiedStatus?: string, overageStatus?: string) =>
    parseRateLimitHeaders({
      ...(unifiedStatus ? { 'anthropic-ratelimit-unified-status': unifiedStatus } : {}),
      ...(overageStatus ? { 'anthropic-ratelimit-unified-overage-status': overageStatus } : {}),
    });

  it('is NOT hard-limited when the primary tier is not rejected', () => {
    assert.equal(isHardRateLimited(info('allowed')), false);
    assert.equal(isHardRateLimited(info()), false);
  });

  it('is hard-limited when primary rejected and there is no overage tier', () => {
    assert.equal(isHardRateLimited(info('rejected')), true);
  });

  it('is hard-limited when primary rejected and overage is also rejected', () => {
    assert.equal(isHardRateLimited(info('rejected', 'rejected')), true);
  });

  it('is NOT hard-limited when primary rejected but overage is allowed (soft spillover)', () => {
    assert.equal(isHardRateLimited(info('rejected', 'allowed')), false);
  });
});

describe('shouldBackoff — unified status is authoritative over per-bucket heuristics', () => {
  it('tolerates a soft overage even when a bucket remaining counter is zero', () => {
    const info = parseRateLimitHeaders({
      'anthropic-ratelimit-unified-status': 'rejected',
      'anthropic-ratelimit-unified-overage-status': 'allowed',
      'x-ratelimit-remaining-requests': '0',
    });
    assert.equal(shouldBackoff(info), false);
  });

  it('hard-fails a rejected primary with no overage even if remaining is positive', () => {
    const info = parseRateLimitHeaders({
      'anthropic-ratelimit-unified-status': 'rejected',
      'x-ratelimit-remaining-requests': '50',
    });
    assert.equal(shouldBackoff(info), true);
  });

  it('an explicit retry-after still wins over a soft overage', () => {
    const info = parseRateLimitHeaders({
      'retry-after': '30',
      'anthropic-ratelimit-unified-status': 'rejected',
      'anthropic-ratelimit-unified-overage-status': 'allowed',
    });
    assert.equal(shouldBackoff(info), true);
  });

  it('falls back to per-bucket remaining heuristics when no unified status is present', () => {
    assert.equal(
      shouldBackoff(parseRateLimitHeaders({ 'x-ratelimit-remaining-tokens': '0' })),
      true,
    );
    assert.equal(
      shouldBackoff(parseRateLimitHeaders({ 'x-ratelimit-remaining-requests': '5' })),
      false,
    );
  });
});

describe('RateLimitTracker — overage tier surfaced on getStatus', () => {
  it('reports isUsingOverage, overage reset and representative claim for a soft overage', () => {
    const tracker = new RateLimitTracker();
    const resetSecs = Math.floor(Date.now() / 1000) + 3600;
    tracker.update({
      'anthropic-ratelimit-unified-status': 'rejected',
      'anthropic-ratelimit-unified-overage-status': 'allowed',
      'anthropic-ratelimit-unified-overage-reset': String(resetSecs),
      'anthropic-ratelimit-unified-representative-claim': 'seven_day_opus',
    });
    const status = tracker.getStatus();
    assert.equal(status.isUsingOverage, true);
    assert.equal(status.representativeClaim, 'seven_day_opus');
    assert.ok(status.overage);
    assert.equal(status.overage!.status, 'allowed');
    assert.equal(status.overage!.resetsAt, resetSecs);
  });

  it('marks isUsingOverage false when the overage tier is also rejected', () => {
    const tracker = new RateLimitTracker();
    tracker.update({
      'anthropic-ratelimit-unified-status': 'rejected',
      'anthropic-ratelimit-unified-overage-status': 'rejected',
    });
    const status = tracker.getStatus();
    assert.equal(status.isUsingOverage, false);
    assert.equal(status.overage!.resetsAt, null);
  });

  it('clears the overage/claim state when a later response omits the headers', () => {
    const tracker = new RateLimitTracker();
    tracker.update({ 'anthropic-ratelimit-unified-overage-status': 'allowed' });
    assert.ok(tracker.getStatus().overage);
    tracker.update({ 'x-ratelimit-limit-requests': '100', 'x-ratelimit-remaining-requests': '90' });
    const status = tracker.getStatus();
    assert.equal(status.overage, undefined);
    assert.equal(status.representativeClaim, undefined);
  });
});

describe('RateLimitTracker.getRawUtilization — tracked on every response', () => {
  it('returns a per-bucket fraction and reset epoch-seconds', () => {
    const tracker = new RateLimitTracker();
    tracker.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '40',
      'x-ratelimit-reset-requests': '120s',
      'x-ratelimit-limit-tokens': '1000',
      'x-ratelimit-remaining-tokens': '250',
    });
    const raw = tracker.getRawUtilization();
    assert.ok(raw.requests);
    assert.equal(raw.requests!.utilization, 0.6);
    assert.equal(typeof raw.requests!.resetsAt, 'number');
    assert.ok(raw.tokens);
    assert.equal(raw.tokens!.utilization, 0.75);
    // No reset header for tokens → resetAt is the "unknown" sentinel → null epoch.
    assert.equal(raw.tokens!.resetsAt, null);
  });

  it('is empty before any response is observed', () => {
    assert.deepEqual(new RateLimitTracker().getRawUtilization(), {});
  });

  it('reports full utilization for a zero-limit bucket', () => {
    const tracker = new RateLimitTracker();
    tracker.update({ 'x-ratelimit-limit-requests': '0', 'x-ratelimit-remaining-requests': '0' });
    assert.equal(tracker.getRawUtilization().requests!.utilization, 1);
  });
});

describe('ProviderCoordinator.getRawUtilization — passthrough', () => {
  const subsystem: ProviderSubsystem = {
    primary: { baseUrl: 'https://api.openai.com/v1', apiKey: 'sk-test', model: 'gpt-4o' },
  };

  it('exposes the tracker raw utilization after ingesting headers', () => {
    const coord = new ProviderCoordinator(subsystem);
    assert.deepEqual(coord.getRawUtilization(), {});
    coord.updateRateLimits({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '10',
    });
    const raw = coord.getRawUtilization();
    assert.ok(raw.requests);
    assert.equal(raw.requests!.utilization, 0.9);
  });
});
