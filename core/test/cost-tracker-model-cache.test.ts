import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  CostTracker,
  CacheMissDiagnostics,
  estimateUsageCost,
  diffToolHashes,
  WEB_SEARCH_COST_PER_REQUEST,
  MIN_CACHE_MISS_TOKENS,
  PARTIAL_CACHE_BREAK_RATIO,
  _resetPricingCache,
  _fetchPricing,
  type CacheDiagRequest,
} from '../src/cost-tracker';

const ZERO = { uncachedPromptTokens: 0, cachedPromptTokens: 0, cacheCreationTokens: 0, completionTokens: 0 };

describe('estimateUsageCost — web-search request component', () => {
  it('adds a flat per-request web-search charge', () => {
    const cost = estimateUsageCost('gpt-4o', { ...ZERO, webSearchRequests: 3 });
    assert.ok(Math.abs(cost - 3 * WEB_SEARCH_COST_PER_REQUEST) < 1e-9);
    assert.equal(WEB_SEARCH_COST_PER_REQUEST, 0.01);
  });
  it('defaults to no web-search cost and ignores negatives', () => {
    assert.equal(estimateUsageCost('gpt-4o', ZERO), 0);
    assert.equal(estimateUsageCost('gpt-4o', { ...ZERO, webSearchRequests: -5 }), 0);
  });
});

describe('CostTracker — per-model usage map (cost survives model switches)', () => {
  it('attributes tokens/cost per model and keeps a single running total', () => {
    const t = new CostTracker('gpt-4o');
    t.trackUsage({ promptTokens: 1_000_000, completionTokens: 0 }, 'gpt-4o');
    t.trackUsage({ promptTokens: 1_000_000, completionTokens: 0 }, 'claude-opus-4');
    const s = t.getStats();
    assert.equal(s.requestCount, 2);
    assert.ok(s.byModel['gpt-4o']);
    assert.ok(s.byModel['claude-opus-4']);
    assert.equal(s.byModel['gpt-4o'].requestCount, 1);
    assert.equal(s.byModel['claude-opus-4'].requestCount, 1);
    // opus input ($15/Mtok) costs more than gpt-4o ($2.5/Mtok)
    assert.ok(s.byModel['claude-opus-4'].cost > s.byModel['gpt-4o'].cost);
    // running total equals the sum of per-model costs
    const sum = s.byModel['gpt-4o'].cost + s.byModel['claude-opus-4'].cost;
    assert.ok(Math.abs(s.totalCost - sum) < 1e-9);
  });

  it('defaults to the constructor model when none is passed', () => {
    const t = new CostTracker('gpt-4o');
    t.trackUsage({ promptTokens: 100, completionTokens: 10 });
    assert.ok(t.getStats().byModel['gpt-4o']);
  });

  it('accumulates web-search requests into stats + cost', () => {
    const t = new CostTracker('gpt-4o');
    t.trackUsage({ promptTokens: 0, completionTokens: 0, webSearchRequests: 4 });
    const s = t.getStats();
    assert.equal(s.webSearchRequests, 4);
    assert.ok(Math.abs(s.totalCost - 4 * WEB_SEARCH_COST_PER_REQUEST) < 1e-9);
  });
});

describe('CostTracker — unknown-model accuracy flag', () => {
  it('flags estimated cost + lists unknown models', () => {
    const t = new CostTracker('definitely-not-a-real-model-xyz');
    t.trackUsage({ promptTokens: 100, completionTokens: 10 });
    const s = t.getStats();
    assert.equal(s.costAccuracy, 'estimated');
    assert.deepEqual(s.unknownModels, ['definitely-not-a-real-model-xyz']);
  });

  it('reports exact for a priced model', () => {
    const t = new CostTracker('gpt-4o');
    t.trackUsage({ promptTokens: 100, completionTokens: 10 });
    const s = t.getStats();
    assert.equal(s.costAccuracy, 'exact');
    assert.deepEqual(s.unknownModels, []);
  });

  it('reset clears per-model + unknown-model state', () => {
    const t = new CostTracker('nope-model');
    t.trackUsage({ promptTokens: 100, completionTokens: 10, webSearchRequests: 2 });
    t.reset();
    const s = t.getStats();
    assert.deepEqual(s.byModel, {});
    assert.deepEqual(s.unknownModels, []);
    assert.equal(s.webSearchRequests, 0);
    assert.equal(s.costAccuracy, 'exact');
  });
});

describe('CostTracker — retry-aware API duration tracking', () => {
  it('accumulates total duration including retries, and a without-retries figure', () => {
    const t = new CostTracker('gpt-4o');
    t.trackRequestDuration(100);                      // successful request
    t.trackRequestDuration(40, { wasRetry: true });   // a retry attempt
    t.trackRequestDuration(60, { wasRetry: false });  // another successful request
    const s = t.getStats();
    assert.equal(s.apiDurationMs, 200);               // 100 + 40 + 60
    assert.equal(s.apiDurationMsWithoutRetries, 160); // 100 + 60 (retry excluded)
    // The delta is the observable time cost of retries.
    assert.equal(s.apiDurationMs - s.apiDurationMsWithoutRetries, 40);
  });

  it('ignores non-finite, zero and negative durations', () => {
    const t = new CostTracker('gpt-4o');
    t.trackRequestDuration(NaN);
    t.trackRequestDuration(Infinity);
    t.trackRequestDuration(0);
    t.trackRequestDuration(-10);
    t.trackRequestDuration(-10, { wasRetry: true });
    const s = t.getStats();
    assert.equal(s.apiDurationMs, 0);
    assert.equal(s.apiDurationMsWithoutRetries, 0);
  });

  it('defaults to zero duration and resets it', () => {
    const t = new CostTracker('gpt-4o');
    assert.equal(t.getStats().apiDurationMs, 0);
    assert.equal(t.getStats().apiDurationMsWithoutRetries, 0);
    t.trackRequestDuration(500);
    t.reset();
    const s = t.getStats();
    assert.equal(s.apiDurationMs, 0);
    assert.equal(s.apiDurationMsWithoutRetries, 0);
  });
});

describe('estimateUsageCost — per-model cache-write price', () => {
  const savedFetch = globalThis.fetch;
  afterEach(() => {
    globalThis.fetch = savedFetch;
    _resetPricingCache();
  });

  it('honors a gateway-advertised cache-write price instead of the 1.25x ratio', async () => {
    // Gateway quotes per-TOKEN prices; perTokenToPerMillion multiplies by 1e6.
    //   input = $1/Mtok, output = $2/Mtok, cache-write = $5/Mtok (NOT 1.25x input).
    globalThis.fetch = (async () => ({
      ok: true,
      json: async () => ({
        data: [{
          id: 'vendor/cw-model',
          pricing: { input: '0.000001', output: '0.000002', input_cache_write: '0.000005' },
        }],
      }),
    })) as unknown as typeof fetch;
    _resetPricingCache();
    await _fetchPricing();

    const cost = estimateUsageCost('cw-model', { ...ZERO, cacheCreationTokens: 1_000_000 });
    // 1M cache-write at the model's own $5/Mtok = $5.00 (ratio fallback would give $1.25).
    assert.ok(Math.abs(cost - 5) < 1e-9, `expected $5 (gateway cache-write), got $${cost}`);
  });

  it('falls back to the 1.25x input ratio when the gateway omits a cache-write price', async () => {
    globalThis.fetch = (async () => ({
      ok: true,
      json: async () => ({
        data: [{ id: 'vendor/nocw-model', pricing: { input: '0.000004', output: '0.000008' } }],
      }),
    })) as unknown as typeof fetch;
    _resetPricingCache();
    await _fetchPricing();

    const cost = estimateUsageCost('nocw-model', { ...ZERO, cacheCreationTokens: 1_000_000 });
    // No cache-write slot → input ($4/Mtok) * 1.25 = $5.00.
    assert.ok(Math.abs(cost - 5) < 1e-9, `expected $5 (input*1.25 fallback), got $${cost}`);
  });
});

describe('diffToolHashes', () => {
  it('reports added/removed/changed', () => {
    const delta = diffToolHashes(
      { a: 'h1', b: 'h2', c: 'h3' },
      { a: 'h1', b: 'CHANGED', d: 'h4' },
    );
    assert.deepEqual(delta, { added: ['d'], removed: ['c'], changed: ['b'] });
  });
  it('handles undefined maps', () => {
    assert.deepEqual(diffToolHashes(undefined, { a: '1' }), { added: ['a'], removed: [], changed: [] });
    assert.deepEqual(diffToolHashes({ a: '1' }, undefined), { added: [], removed: ['a'], changed: [] });
  });
});

describe('CacheMissDiagnostics — partial cache break detection', () => {
  const req = (over: Partial<CacheDiagRequest> = {}): CacheDiagRequest => ({
    toolDefHash: 'tools-v1', systemPromptHash: 'sys-v1', thinkingMode: 'off', model: 'claude-opus-4', ...over,
  });

  it('flags a partial break when cache reads shrink past both thresholds', () => {
    const d = new CacheMissDiagnostics();
    d.recordRequest(req());
    d.recordResponse(100_000, 0); // establishes baseline (hit, not first-miss)
    d.recordRequest(req({ toolDefHash: 'tools-v2' }));
    d.recordResponse(50_000, 0); // dropped 50k (>2000) and <95% → partial break
    assert.equal(d.getPartialBreakCount(), 1);
    assert.equal(d.getLastMissCause(), 'tool_definitions_changed');
  });

  it('does NOT flag small drops below the noise floor', () => {
    const d = new CacheMissDiagnostics();
    d.recordRequest(req());
    d.recordResponse(100_000, 0);
    d.recordRequest(req());
    d.recordResponse(99_000, 0); // only 1000 drop (< MIN_CACHE_MISS_TOKENS)
    assert.equal(d.getPartialBreakCount(), 0);
  });

  it('exposes the per-tool schema delta on a break', () => {
    const d = new CacheMissDiagnostics();
    d.recordRequest(req({ perToolHashes: { bash: 'a', read: 'b' } }));
    d.recordResponse(100_000, 0);
    d.recordRequest(req({ toolDefHash: 'tools-v2', perToolHashes: { bash: 'a', read: 'CHANGED' } }));
    d.recordResponse(0, 0); // full miss
    assert.deepEqual(d.getLastToolSchemaDelta(), { added: [], removed: [], changed: ['read'] });
  });

  it('exposes threshold constants and extended TTL', () => {
    assert.equal(MIN_CACHE_MISS_TOKENS, 2000);
    assert.equal(PARTIAL_CACHE_BREAK_RATIO, 0.95);
    assert.equal(CacheMissDiagnostics.EXTENDED_TTL_MS, 60 * 60 * 1000);
  });

  it('reset clears partial-break + delta state', () => {
    const d = new CacheMissDiagnostics();
    d.recordRequest(req());
    d.recordResponse(100_000, 0);
    d.recordRequest(req({ toolDefHash: 'v2' }));
    d.recordResponse(1000, 0);
    d.reset();
    assert.equal(d.getPartialBreakCount(), 0);
    assert.deepEqual(d.getLastToolSchemaDelta(), { added: [], removed: [], changed: [] });
  });
});
