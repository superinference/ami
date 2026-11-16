import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  CostTracker,
  CacheMissDiagnostics,
  estimateUsageCost,
  isExcludedModel,
  WEB_SEARCH_COST_PER_REQUEST,
  type CacheDiagRequest,
} from '../src/cost-tracker';

const ZERO = { uncachedPromptTokens: 0, cachedPromptTokens: 0, cacheCreationTokens: 0, completionTokens: 0 };

// ---------------------------------------------------------------------------
// U083 gap: per-model web-search request count in the usage breakdown.
// Claude's per-model usage record carries webSearchRequests; AMI's byModel must
// attribute the flat per-request web_search charge to the model that issued it.
// ---------------------------------------------------------------------------
describe('CostTracker — per-model web-search request count (U083)', () => {
  it('attributes web-search requests to the model that issued them', () => {
    const t = new CostTracker('gpt-4o');
    t.trackUsage({ promptTokens: 0, completionTokens: 0, webSearchRequests: 3 }, 'gpt-4o');
    t.trackUsage({ promptTokens: 0, completionTokens: 0, webSearchRequests: 5 }, 'claude-opus-4');
    const s = t.getStats();
    assert.equal(s.byModel['gpt-4o'].webSearchRequests, 3);
    assert.equal(s.byModel['claude-opus-4'].webSearchRequests, 5);
    // Session total equals the sum of per-model counts.
    assert.equal(s.webSearchRequests, 8);
    // Each per-model cost includes its own flat web-search charge.
    assert.ok(Math.abs(s.byModel['gpt-4o'].cost - 3 * WEB_SEARCH_COST_PER_REQUEST) < 1e-9);
    assert.ok(Math.abs(s.byModel['claude-opus-4'].cost - 5 * WEB_SEARCH_COST_PER_REQUEST) < 1e-9);
  });

  it('defaults the per-model web-search count to 0 and clears it on reset', () => {
    const t = new CostTracker('gpt-4o');
    t.trackUsage({ promptTokens: 100, completionTokens: 10 }, 'gpt-4o');
    assert.equal(t.getStats().byModel['gpt-4o'].webSearchRequests, 0);
    t.trackUsage({ promptTokens: 0, completionTokens: 0, webSearchRequests: 2 }, 'gpt-4o');
    assert.equal(t.getStats().byModel['gpt-4o'].webSearchRequests, 2);
    t.reset();
    assert.deepEqual(t.getStats().byModel, {});
  });
});

// ---------------------------------------------------------------------------
// U083 gap: unknown-model fallback to the configured default (main-loop) model's
// REAL pricing tier, instead of the generic DEFAULT_PRICING constant (mirrors
// Claude's getModelCosts → MODEL_COSTS[defaultMainLoopModel]). Still flagged
// 'estimated' so the inaccuracy is surfaced.
// ---------------------------------------------------------------------------
describe('estimateUsageCost — unknown-model fallback to default model tier (U083)', () => {
  it('prices an unknown model at the supplied default model tier, not the generic constant', () => {
    // 1M uncached input tokens. Generic DEFAULT_PRICING input is $1/Mtok; opus is $15/Mtok.
    const generic = estimateUsageCost('totally-unknown-xyz', { ...ZERO, uncachedPromptTokens: 1_000_000 });
    const withFallback = estimateUsageCost('totally-unknown-xyz', { ...ZERO, uncachedPromptTokens: 1_000_000 }, 'claude-opus-4');
    assert.ok(Math.abs(generic - 1) < 1e-9, `generic should be $1, got $${generic}`);
    assert.ok(Math.abs(withFallback - 15) < 1e-9, `fallback should be $15 (opus tier), got $${withFallback}`);
  });

  it('ignores an unknown fallback model and uses the generic default', () => {
    const cost = estimateUsageCost('unknown-a', { ...ZERO, uncachedPromptTokens: 1_000_000 }, 'also-unknown-b');
    assert.ok(Math.abs(cost - 1) < 1e-9, `should fall back to generic $1, got $${cost}`);
  });

  it('prices a known model by its own tier, ignoring the fallback', () => {
    const cost = estimateUsageCost('gpt-4o', { ...ZERO, uncachedPromptTokens: 1_000_000 }, 'claude-opus-4');
    assert.ok(Math.abs(cost - 2.5) < 1e-9, `gpt-4o input is $2.5, got $${cost}`);
  });
});

describe('CostTracker — unknown per-call model uses the constructor model tier (U083)', () => {
  it('prices an unknown per-call model at the tracker default model tier but still flags estimated', () => {
    const t = new CostTracker('claude-opus-4');
    t.trackUsage({ promptTokens: 1_000_000, completionTokens: 0 }, 'unknown-side-model');
    const s = t.getStats();
    // Priced at opus $15/Mtok via the constructor-model fallback, not generic $1.
    assert.ok(Math.abs(s.byModel['unknown-side-model'].cost - 15) < 1e-9, `got $${s.byModel['unknown-side-model'].cost}`);
    // Inaccuracy is still surfaced.
    assert.equal(s.costAccuracy, 'estimated');
    assert.deepEqual(s.unknownModels, ['unknown-side-model']);
  });
});

// ---------------------------------------------------------------------------
// U083 gap: model-exclusion knob for cache-break detection. Models whose
// server-side caching semantics differ (haiku) must be skipped so cache-read
// drops don't produce false-positive break notifications.
// ---------------------------------------------------------------------------
describe('isExcludedModel (U083)', () => {
  it('excludes haiku variants, not other models', () => {
    assert.equal(isExcludedModel('claude-haiku'), true);
    assert.equal(isExcludedModel('claude-3-5-haiku-20241022'), true);
    assert.equal(isExcludedModel('anthropic/claude-haiku-4-5'), true);
    assert.equal(isExcludedModel('claude-opus-4'), false);
    assert.equal(isExcludedModel('gpt-4o'), false);
  });
});

describe('CacheMissDiagnostics — excluded models are not scored (U083)', () => {
  const req = (over: Partial<CacheDiagRequest> = {}): CacheDiagRequest => ({
    toolDefHash: 'tools-v1', systemPromptHash: 'sys-v1', thinkingMode: 'off', model: 'claude-haiku', ...over,
  });

  it('skips cache-break scoring for an excluded (haiku) model', () => {
    const d = new CacheMissDiagnostics();
    d.recordRequest(req());
    d.recordResponse(100_000, 0);        // baseline
    d.recordRequest(req({ toolDefHash: 'tools-v2' }));
    d.recordResponse(0, 0);              // full miss that WOULD be a break on a tracked model
    assert.equal(d.getTotalMisses(), 0);
    assert.equal(d.getLastMissCause(), null);
    assert.equal(d.getLastExplanation(), '');
  });

  it('DOES score a non-excluded model under the same drop', () => {
    const d = new CacheMissDiagnostics();
    d.recordRequest(req({ model: 'claude-opus-4' }));
    d.recordResponse(100_000, 0);
    d.recordRequest(req({ model: 'claude-opus-4', toolDefHash: 'tools-v2' }));
    d.recordResponse(0, 0);
    assert.equal(d.getTotalMisses(), 1);
    assert.equal(d.getLastMissCause(), 'tool_definitions_changed');
  });
});
