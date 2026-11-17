import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  CostTracker,
  estimateUsageCost,
  _resetPricingCache,
  _fetchPricing,
} from '../src/cost-tracker';
import { getContextWindow, getModelMaxOutputTokens } from '../src/model-capabilities';

const ZERO = { uncachedPromptTokens: 0, cachedPromptTokens: 0, cacheCreationTokens: 0, completionTokens: 0 };

// ---------------------------------------------------------------------------
// Feature A — speed / fast-mode (priority) pricing tier (U083)
// ---------------------------------------------------------------------------

describe('estimateUsageCost — speed/fast-mode (priority) pricing tier (U083)', () => {
  const savedFetch = globalThis.fetch;
  afterEach(() => {
    globalThis.fetch = savedFetch;
    _resetPricingCache();
  });

  async function loadFastModel(): Promise<void> {
    // Gateway quotes per-TOKEN prices (perTokenToPerMillion multiplies by 1e6):
    //   standard input $1/Mtok, output $2/Mtok; priority (fast) input $10/Mtok, output $20/Mtok.
    globalThis.fetch = (async () => ({
      ok: true,
      json: async () => ({
        data: [{
          id: 'vendor/fast-model',
          pricing: {
            input: '0.000001', output: '0.000002',
            input_priority: '0.00001', output_priority: '0.00002',
          },
        }],
      }),
    })) as unknown as typeof fetch;
    _resetPricingCache();
    await _fetchPricing();
  }

  it('bills a fast request at the advertised premium (priority) tier', async () => {
    await loadFastModel();
    const std = estimateUsageCost('fast-model', { ...ZERO, uncachedPromptTokens: 1_000_000, completionTokens: 1_000_000 });
    const fast = estimateUsageCost('fast-model', { ...ZERO, uncachedPromptTokens: 1_000_000, completionTokens: 1_000_000 }, undefined, 'fast');
    // standard = 1*1 + 1*2 = $3; fast = 1*10 + 1*20 = $30.
    assert.ok(Math.abs(std - 3) < 1e-9, `std expected $3, got $${std}`);
    assert.ok(Math.abs(fast - 30) < 1e-9, `fast expected $30, got $${fast}`);
  });

  it('medium/slow/absent speed bills at the standard tier even when a fast tier exists', async () => {
    await loadFastModel();
    const base = estimateUsageCost('fast-model', { ...ZERO, uncachedPromptTokens: 1_000_000 });
    for (const speed of [undefined, 'medium', 'slow'] as const) {
      const c = estimateUsageCost('fast-model', { ...ZERO, uncachedPromptTokens: 1_000_000 }, undefined, speed);
      assert.ok(Math.abs(c - base) < 1e-9, `speed=${speed} should be standard $1, got $${c}`);
    }
  });

  it('fast speed falls back to the standard rate for a model with no fast tier', () => {
    // gpt-4o (fallback table) has no priority slot → fast === standard.
    const std = estimateUsageCost('gpt-4o', { ...ZERO, uncachedPromptTokens: 1_000_000 });
    const fast = estimateUsageCost('gpt-4o', { ...ZERO, uncachedPromptTokens: 1_000_000 }, undefined, 'fast');
    assert.ok(Math.abs(std - fast) < 1e-9, 'no fast tier → fast must equal standard');
    assert.ok(std > 0);
  });

  it('derives cached + cache-write from the fast input rate when billed fast', async () => {
    await loadFastModel();
    // No explicit cached slot → cached = input*0.1, cache-write = input*1.25.
    // fast input $10/Mtok: cached $1/Mtok, cache-write $12.5/Mtok.
    const fast = estimateUsageCost(
      'fast-model',
      { ...ZERO, cachedPromptTokens: 1_000_000, cacheCreationTokens: 1_000_000 },
      undefined, 'fast',
    );
    assert.ok(Math.abs(fast - (1 + 12.5)) < 1e-9, `expected $13.5, got $${fast}`);
  });

  it('CostTracker.trackUsage threads speed into the per-model + total cost', async () => {
    await loadFastModel();
    const t = new CostTracker('fast-model');
    t.trackUsage({ promptTokens: 1_000_000, completionTokens: 0, speed: 'fast' });
    const s = t.getStats();
    // 1M uncached input at fast $10/Mtok = $10.
    assert.ok(Math.abs(s.totalCost - 10) < 1e-9, `expected $10, got $${s.totalCost}`);
    assert.ok(Math.abs(s.byModel['fast-model'].cost - 10) < 1e-9);
  });

  it('fast tier scales the cache savings estimate too', async () => {
    await loadFastModel();
    const t = new CostTracker('fast-model');
    // 1M cached tokens at fast input $10 full vs $1 cached = $9 saved (standard would be $0.9).
    t.trackUsage({ promptTokens: 1_000_000, completionTokens: 0, cachedPromptTokens: 1_000_000, speed: 'fast' });
    const s = t.getStats();
    assert.ok(Math.abs(s.cachedCostSavings - 9) < 1e-9, `expected $9 savings, got $${s.cachedCostSavings}`);
  });
});

// ---------------------------------------------------------------------------
// Feature B — per-model contextWindow / maxOutputTokens annotation (U083)
// ---------------------------------------------------------------------------

describe('CostTracker.getStats — per-model context/output limits (U083)', () => {
  afterEach(() => _resetPricingCache());

  it('annotates each per-model rollup with its context window and max-output ceiling', () => {
    const t = new CostTracker('claude-opus-4');
    t.trackUsage({ promptTokens: 100, completionTokens: 10 }, 'claude-opus-4');
    const row = t.getStats().byModel['claude-opus-4'];
    assert.equal(row.contextWindow, getContextWindow('claude-opus-4'));
    assert.equal(row.maxOutputTokens, getModelMaxOutputTokens('claude-opus-4').default);
    assert.ok((row.contextWindow ?? 0) > 0, 'a known model should resolve a positive context window');
  });

  it('attaches limits per distinct model after a mid-session switch', () => {
    const t = new CostTracker('gpt-4o');
    t.trackUsage({ promptTokens: 100, completionTokens: 10 }, 'gpt-4o');
    t.trackUsage({ promptTokens: 100, completionTokens: 10 }, 'claude-opus-4');
    const { byModel } = t.getStats();
    assert.equal(byModel['gpt-4o'].contextWindow, getContextWindow('gpt-4o'));
    assert.equal(byModel['claude-opus-4'].contextWindow, getContextWindow('claude-opus-4'));
  });
});
