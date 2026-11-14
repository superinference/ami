import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  CostTracker,
  CacheMissDiagnostics,
  estimateUsageCost,
  diffToolHashes,
  WEB_SEARCH_COST_PER_REQUEST,
  MIN_CACHE_MISS_TOKENS,
  PARTIAL_CACHE_BREAK_RATIO,
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
