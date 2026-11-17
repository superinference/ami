import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  parseRateLimitHeaders,
  computeTimeProgress,
  computeEarlyWarning,
  getRateLimitDisplayName,
  DEFAULT_EARLY_WARNING_CONFIGS,
  type UnifiedWindow,
} from '../src/rate-limit-parser';
import { RateLimitTracker } from '../src/rate-limiter';

// Absolute epoch-seconds reset `delta` seconds from `now` (so parseResetHeader
// treats it as an absolute instant, matching Anthropic's unified reset headers).
function epochResetHeader(deltaSeconds: number, now = Date.now()): string {
  return String(Math.floor(now / 1000) + deltaSeconds);
}

describe('computeTimeProgress', () => {
  it('returns the fraction of a fixed window that has elapsed', () => {
    const now = Date.now();
    const windowSeconds = 7 * 24 * 60 * 60;
    // Reset is half the window away → half the window has elapsed.
    const resetAtSeconds = Math.floor(now / 1000) + windowSeconds / 2;
    const tp = computeTimeProgress(resetAtSeconds, windowSeconds, now);
    assert.ok(Math.abs(tp - 0.5) < 0.001, `expected ~0.5, got ${tp}`);
  });

  it('clamps to [0,1] and treats a non-positive window as 0', () => {
    const now = Date.now();
    assert.equal(computeTimeProgress(Math.floor(now / 1000), 0, now), 0);
    // Reset far in the past → fully elapsed, clamped to 1.
    assert.equal(computeTimeProgress(Math.floor(now / 1000) - 100000, 3600, now), 1);
    // Reset further out than the whole window → not yet started, clamped to 0.
    assert.equal(computeTimeProgress(Math.floor(now / 1000) + 100000, 3600, now), 0);
  });
});

describe('parseRateLimitHeaders — unified windows', () => {
  it('parses per-window utilization/reset/surpassed-threshold headers', () => {
    const resetHeader = epochResetHeader(1000);
    const info = parseRateLimitHeaders({
      'anthropic-ratelimit-unified-7d-utilization': '0.8',
      'anthropic-ratelimit-unified-7d-reset': resetHeader,
      'anthropic-ratelimit-unified-7d-surpassed-threshold': '0.75',
    });
    assert.ok(info.unifiedWindows);
    assert.equal(info.unifiedWindows!.length, 1);
    const w = info.unifiedWindows![0];
    assert.equal(w.claimAbbrev, '7d');
    assert.equal(w.utilization, 0.8);
    assert.equal(w.surpassedThreshold, 0.75);
    assert.ok(w.resetAt instanceof Date);
  });

  it('omits windows the server reported no fields for', () => {
    const info = parseRateLimitHeaders({ 'retry-after': '5' });
    assert.deepEqual(info.unifiedWindows, []);
  });

  it('records a window even when only one field is present (null for the rest)', () => {
    const info = parseRateLimitHeaders({
      'anthropic-ratelimit-unified-5h-utilization': '0.3',
    });
    assert.equal(info.unifiedWindows!.length, 1);
    assert.equal(info.unifiedWindows![0].utilization, 0.3);
    assert.equal(info.unifiedWindows![0].resetAt, null);
    assert.equal(info.unifiedWindows![0].surpassedThreshold, null);
  });
});

describe('computeEarlyWarning', () => {
  it('returns null when there are no windows', () => {
    assert.equal(computeEarlyWarning([]), null);
    assert.equal(computeEarlyWarning(undefined), null);
  });

  it('fires header-based immediately when surpassed-threshold is present', () => {
    const windows: UnifiedWindow[] = [
      { claimAbbrev: '7d', utilization: 0.4, resetAt: new Date(), surpassedThreshold: 0.25 },
    ];
    const w = computeEarlyWarning(windows);
    assert.ok(w);
    assert.equal(w!.source, 'header');
    assert.equal(w!.rateLimitType, 'seven_day');
    assert.equal(w!.surpassedThreshold, 0.25);
  });

  it('fires an aggressive 7-day tier via the FIXED window (impossible for observed-window burn-rate)', () => {
    const now = Date.now();
    const windowSeconds = 7 * 24 * 60 * 60;
    // 50% elapsed (<= 0.6 timePct), utilization 0.8 (>= 0.75) → first 7d tier fires.
    const resetAtSeconds = Math.floor(now / 1000) + windowSeconds / 2;
    const windows: UnifiedWindow[] = [
      { claimAbbrev: '7d', utilization: 0.8, resetAt: new Date(resetAtSeconds * 1000), surpassedThreshold: null },
    ];
    const w = computeEarlyWarning(windows, DEFAULT_EARLY_WARNING_CONFIGS, now);
    assert.ok(w);
    assert.equal(w!.source, 'time-relative');
    assert.equal(w!.rateLimitType, 'seven_day');
  });

  it('does NOT fire when utilization is proportionate to elapsed time', () => {
    const now = Date.now();
    const windowSeconds = 7 * 24 * 60 * 60;
    // 90% of the window elapsed, only 80% used — on pace, no warning.
    const resetAtSeconds = Math.floor(now / 1000) + windowSeconds * 0.1;
    const windows: UnifiedWindow[] = [
      { claimAbbrev: '7d', utilization: 0.8, resetAt: new Date(resetAtSeconds * 1000), surpassedThreshold: null },
    ];
    assert.equal(computeEarlyWarning(windows, DEFAULT_EARLY_WARNING_CONFIGS, now), null);
  });

  it('skips windows missing utilization or reset for the time-relative path', () => {
    const windows: UnifiedWindow[] = [
      { claimAbbrev: '7d', utilization: null, resetAt: new Date(), surpassedThreshold: null },
      { claimAbbrev: '5h', utilization: 0.95, resetAt: null, surpassedThreshold: null },
    ];
    assert.equal(computeEarlyWarning(windows), null);
  });

  it('respects a custom/empty config (empty disables proactive warnings)', () => {
    const now = Date.now();
    const windowSeconds = 7 * 24 * 60 * 60;
    const resetAtSeconds = Math.floor(now / 1000) + windowSeconds / 2;
    const windows: UnifiedWindow[] = [
      { claimAbbrev: '7d', utilization: 0.8, resetAt: new Date(resetAtSeconds * 1000), surpassedThreshold: null },
    ];
    assert.equal(computeEarlyWarning(windows, [], now), null);
  });
});

describe('getRateLimitDisplayName', () => {
  it('maps known types and falls back to the raw type', () => {
    assert.equal(getRateLimitDisplayName('five_hour'), 'session limit');
    assert.equal(getRateLimitDisplayName('seven_day'), 'weekly limit');
    assert.equal(getRateLimitDisplayName('mystery_window'), 'mystery_window');
  });
});

describe('RateLimitTracker — proactive early warning (end-to-end)', () => {
  it('raises a proactive warning from unified 7d headers with no per-bucket limit', () => {
    const now = Date.now();
    const windowSeconds = 7 * 24 * 60 * 60;
    const t = new RateLimitTracker();
    t.update({
      'anthropic-ratelimit-unified-7d-utilization': '0.8',
      'anthropic-ratelimit-unified-7d-reset': epochResetHeader(windowSeconds / 2, now),
    });
    const s = t.getStatus();
    assert.equal(s.shouldWarn, true);
    assert.equal(s.proactive, true);
    assert.equal(s.severity, 'warning');
    assert.ok(s.warnMessage && s.warnMessage.includes('weekly limit'), s.warnMessage);
  });

  it('does not set proactive when the warning is a per-bucket utilization warning', () => {
    const t = new RateLimitTracker();
    t.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '2',
      'x-ratelimit-reset-requests': '30',
    });
    const s = t.getStatus();
    assert.equal(s.shouldWarn, true);
    assert.notEqual(s.proactive, true);
    assert.ok(s.warnMessage && s.warnMessage.includes('request rate limit'), s.warnMessage);
  });

  it('a custom earlyWarningConfigs=[] disables the proactive path', () => {
    const now = Date.now();
    const windowSeconds = 7 * 24 * 60 * 60;
    const t = new RateLimitTracker({ earlyWarningConfigs: [] });
    t.update({
      'anthropic-ratelimit-unified-7d-utilization': '0.8',
      'anthropic-ratelimit-unified-7d-reset': epochResetHeader(windowSeconds / 2, now),
    });
    const s = t.getStatus();
    assert.equal(s.shouldWarn, false);
    assert.notEqual(s.proactive, true);
  });

  it('clears unified windows on a later response that omits them', () => {
    const now = Date.now();
    const windowSeconds = 7 * 24 * 60 * 60;
    const t = new RateLimitTracker();
    t.update({
      'anthropic-ratelimit-unified-7d-utilization': '0.8',
      'anthropic-ratelimit-unified-7d-reset': epochResetHeader(windowSeconds / 2, now),
    });
    assert.equal(t.getStatus().shouldWarn, true);
    t.update({ 'x-ratelimit-limit-requests': '100', 'x-ratelimit-remaining-requests': '100' });
    assert.equal(t.getStatus().shouldWarn, false);
  });
});
