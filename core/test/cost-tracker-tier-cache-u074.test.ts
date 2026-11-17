/**
 * U074 — estimateUsageCost: service-tier (batch −50%) + 1-hour cache-write (2×) re-pricing.
 *
 * Covers the parity gap (services/api/emptyUsage.ts: service_tier / cache_creation ephemeral
 * 1h-vs-5m / speed): a session Claude Code actually billed at batch (−50%) or at the 1h
 * cache-write rate must NOT be re-priced at the standard/5m rate by the cross-agent spend view.
 *
 * Run: cd core/core && node --import tsx --test test/cost-tracker-tier-cache-u074.test.ts
 */
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { estimateUsageCost, _resetPricingCache } from '../src/cost-tracker';

beforeEach(() => { _resetPricingCache(); });

describe('estimateUsageCost — service tier + 1h cache split (U074)', () => {
  it('batch service tier re-prices at HALF the standard cost', () => {
    const base = {
      uncachedPromptTokens: 1_000_000, cachedPromptTokens: 0,
      cacheCreationTokens: 0, completionTokens: 1_000_000,
    };
    const standard = estimateUsageCost('gpt-4o', base);
    const batch = estimateUsageCost('gpt-4o', { ...base, serviceTier: 'batch' });
    assert.ok(standard > 0, 'standard priced');
    assert.ok(Math.abs(batch - standard * 0.5) < 1e-9, `batch should be half of standard: ${batch} vs ${standard}`);
  });

  it('priority / standard / unknown tiers all price at the standard rate', () => {
    const base = {
      uncachedPromptTokens: 500_000, cachedPromptTokens: 0,
      cacheCreationTokens: 0, completionTokens: 0,
    };
    const std = estimateUsageCost('gpt-4o', base);
    assert.equal(estimateUsageCost('gpt-4o', { ...base, serviceTier: 'priority' }), std);
    assert.equal(estimateUsageCost('gpt-4o', { ...base, serviceTier: 'standard' }), std);
    assert.equal(estimateUsageCost('gpt-4o', base), std);
  });

  it('a 1-hour cache-write slice is priced at 2× input vs the 5-minute 1.25×', () => {
    // gpt-4o fallback input = 2.5/Mtok. 1M cache-create tokens:
    //   all-5m → 2.5 * 1.25 = 3.125 ; all-1h → 2.5 * 2 = 5.0
    const all5m = estimateUsageCost('gpt-4o', {
      uncachedPromptTokens: 0, cachedPromptTokens: 0,
      cacheCreationTokens: 1_000_000, cacheCreation1hTokens: 0, completionTokens: 0,
    });
    const all1h = estimateUsageCost('gpt-4o', {
      uncachedPromptTokens: 0, cachedPromptTokens: 0,
      cacheCreationTokens: 1_000_000, cacheCreation1hTokens: 1_000_000, completionTokens: 0,
    });
    assert.ok(Math.abs(all5m - 3.125) < 1e-6, `5m write ~3.125, got ${all5m}`);
    assert.ok(Math.abs(all1h - 5.0) < 1e-6, `1h write ~5.0, got ${all1h}`);
    assert.ok(all1h > all5m, '1h TTL must cost more than 5m');
  });

  it('clamps a 1h slice larger than the total so no negative 5m portion is priced', () => {
    const cost = estimateUsageCost('gpt-4o', {
      uncachedPromptTokens: 0, cachedPromptTokens: 0,
      cacheCreationTokens: 100_000, cacheCreation1hTokens: 999_999, completionTokens: 0,
    });
    // Entire 100k priced at the 1h rate (2.5 * 2 / 10 = 0.5), no negative 5m math.
    assert.ok(Math.abs(cost - 0.5) < 1e-6, `clamped 1h-only cost ~0.5, got ${cost}`);
  });

  it('batch discount also halves the 1h cache-write component', () => {
    const full = estimateUsageCost('gpt-4o', {
      uncachedPromptTokens: 0, cachedPromptTokens: 0,
      cacheCreationTokens: 1_000_000, cacheCreation1hTokens: 1_000_000, completionTokens: 0,
    });
    const batched = estimateUsageCost('gpt-4o', {
      uncachedPromptTokens: 0, cachedPromptTokens: 0,
      cacheCreationTokens: 1_000_000, cacheCreation1hTokens: 1_000_000, completionTokens: 0,
      serviceTier: 'batch',
    });
    assert.ok(Math.abs(batched - full * 0.5) < 1e-9, 'batch halves the 1h component too');
  });
});
