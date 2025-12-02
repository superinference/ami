import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { CostTracker } from '../src/cost-tracker';

// ---------------------------------------------------------------------------
// CostTracker — basic tracking
// ---------------------------------------------------------------------------
describe('CostTracker', () => {
  it('trackUsage() accumulates prompt and completion tokens', () => {
    const tracker = new CostTracker('gpt-4o');
    tracker.trackUsage({ promptTokens: 100, completionTokens: 50 });

    const stats = tracker.getStats();
    assert.equal(stats.promptTokens, 100);
    assert.equal(stats.completionTokens, 50);
    assert.equal(stats.totalTokens, 150);
    assert.equal(stats.requestCount, 1);
  });

  it('multiple trackUsage() calls accumulate correctly', () => {
    const tracker = new CostTracker('gpt-4o');
    tracker.trackUsage({ promptTokens: 100, completionTokens: 50 });
    tracker.trackUsage({ promptTokens: 200, completionTokens: 100 });
    tracker.trackUsage({ promptTokens: 300, completionTokens: 150 });

    const stats = tracker.getStats();
    assert.equal(stats.promptTokens, 600);
    assert.equal(stats.completionTokens, 300);
    assert.equal(stats.totalTokens, 900);
    assert.equal(stats.requestCount, 3);
  });

  it('trackToolCall() increments tool call count', () => {
    const tracker = new CostTracker('gpt-4o');
    tracker.trackToolCall();
    tracker.trackToolCall();
    tracker.trackToolCall();

    assert.equal(tracker.getStats().toolCallCount, 3);
  });

  it('trackTurn() increments turn count', () => {
    const tracker = new CostTracker('gpt-4o');
    tracker.trackTurn();
    tracker.trackTurn();

    assert.equal(tracker.getStats().turnCount, 2);
  });

  it('getStats() returns all accumulated stats', () => {
    const tracker = new CostTracker('gpt-4o');
    tracker.trackUsage({ promptTokens: 1000, completionTokens: 500 });
    tracker.trackToolCall();
    tracker.trackToolCall();
    tracker.trackTurn();

    const stats = tracker.getStats();
    assert.equal(stats.promptTokens, 1000);
    assert.equal(stats.completionTokens, 500);
    assert.equal(stats.totalTokens, 1500);
    assert.equal(stats.requestCount, 1);
    assert.equal(stats.toolCallCount, 2);
    assert.equal(stats.turnCount, 1);
    assert.ok(stats.totalCost > 0);
  });

  it('getStats() returns a defensive copy', () => {
    const tracker = new CostTracker('gpt-4o');
    tracker.trackUsage({ promptTokens: 100, completionTokens: 50 });

    const stats1 = tracker.getStats();
    stats1.promptTokens = 999999;
    stats1.completionTokens = 999999;
    stats1.totalCost = 999999;

    // Internal state should be unaffected
    const stats2 = tracker.getStats();
    assert.equal(stats2.promptTokens, 100);
    assert.equal(stats2.completionTokens, 50);
  });

  it('reset() clears everything', () => {
    const tracker = new CostTracker('gpt-4o');
    tracker.trackUsage({ promptTokens: 500, completionTokens: 200 });
    tracker.trackToolCall();
    tracker.trackTurn();

    tracker.reset();

    const stats = tracker.getStats();
    assert.equal(stats.promptTokens, 0);
    assert.equal(stats.completionTokens, 0);
    assert.equal(stats.totalTokens, 0);
    assert.equal(stats.totalCost, 0);
    assert.equal(stats.requestCount, 0);
    assert.equal(stats.toolCallCount, 0);
    assert.equal(stats.turnCount, 0);
  });

  it('zero-usage tracking (no calls made)', () => {
    const tracker = new CostTracker('gpt-4o');
    const stats = tracker.getStats();

    assert.equal(stats.promptTokens, 0);
    assert.equal(stats.completionTokens, 0);
    assert.equal(stats.totalTokens, 0);
    assert.equal(stats.totalCost, 0);
    assert.equal(stats.requestCount, 0);
    assert.equal(stats.toolCallCount, 0);
    assert.equal(stats.turnCount, 0);
  });
});

// ---------------------------------------------------------------------------
// CostTracker — cost estimation for different models
// ---------------------------------------------------------------------------
describe('CostTracker cost estimation', () => {
  // Helper: track a known amount and return the computed cost
  function costFor(model: string, prompt: number, completion: number): number {
    const tracker = new CostTracker(model);
    tracker.trackUsage({ promptTokens: prompt, completionTokens: completion });
    return tracker.getStats().totalCost;
  }

  it('gpt-4o pricing: $2.50 input / $10 output per million', () => {
    // 1M prompt + 1M completion = $2.50 + $10 = $12.50
    const cost = costFor('gpt-4o', 1_000_000, 1_000_000);
    assert.equal(cost, 12.5);
  });

  it('gpt-4o-mini pricing: $0.15 input / $0.60 output per million', () => {
    const cost = costFor('gpt-4o-mini', 1_000_000, 1_000_000);
    assert.equal(cost, 0.75);
  });

  it('gemini-2.0-flash pricing: $0.10 input / $0.40 output per million', () => {
    const cost = costFor('gemini-2.0-flash', 1_000_000, 1_000_000);
    assert.equal(cost, 0.5);
  });

  it('claude-sonnet-4 pricing: $3 input / $15 output per million', () => {
    const cost = costFor('claude-sonnet-4', 1_000_000, 1_000_000);
    assert.equal(cost, 18);
  });

  it('unknown model uses default pricing: $1 input / $3 output per million', () => {
    const cost = costFor('totally-unknown-model', 1_000_000, 1_000_000);
    assert.equal(cost, 4);
  });

  it('substring model matching works (e.g. gpt-4o-2024-08-06)', () => {
    const cost = costFor('gpt-4o-2024-08-06', 1_000_000, 1_000_000);
    // Should match "gpt-4o" pricing: $2.50 + $10 = $12.50
    assert.equal(cost, 12.5);
  });

  it('small token counts produce proportional costs', () => {
    // 1000 prompt tokens of gpt-4o: (1000/1M) * $2.50 = $0.0025
    const cost = costFor('gpt-4o', 1000, 0);
    assert.ok(Math.abs(cost - 0.0025) < 1e-10);
  });

  it('cost accumulates across multiple trackUsage calls', () => {
    const tracker = new CostTracker('gpt-4o');
    // gpt-4o: $2.50/M input, $10/M output
    tracker.trackUsage({ promptTokens: 500_000, completionTokens: 500_000 });
    tracker.trackUsage({ promptTokens: 500_000, completionTokens: 500_000 });

    const stats = tracker.getStats();
    // Total: 1M prompt + 1M completion = $2.50 + $10 = $12.50
    assert.ok(Math.abs(stats.totalCost - 12.5) < 1e-10);
  });
});

// ---------------------------------------------------------------------------
// Prompt caching cost tracking (lines 94-98)
// ---------------------------------------------------------------------------

describe('CostTracker – prompt caching', () => {
  it('tracks cached prompt tokens and calculates savings', () => {
    const tracker = new CostTracker('gpt-4o');
    // gpt-4o: $2.50/M input
    // 500K cached, 500K uncached, 100K completion
    tracker.trackUsage({
      promptTokens: 1_000_000,
      completionTokens: 100_000,
      cachedPromptTokens: 500_000,
    });

    const stats = tracker.getStats();
    assert.equal(stats.cachedPromptTokens, 500_000);
    assert.equal(stats.uncachedPromptTokens, 500_000);
    assert.ok(stats.cacheHitRate > 0);
    assert.ok(stats.cachedCostSavings > 0);

    // Cache hit rate should be 0.5 (500K cached out of 1M total prompt)
    assert.ok(Math.abs(stats.cacheHitRate - 0.5) < 1e-10);

    // Cached savings: 500K tokens * ($2.50/M) * (1 - 0.1) = $1.125
    assert.ok(Math.abs(stats.cachedCostSavings - 1.125) < 1e-6);
  });

  it('reports 0 cache hit rate when no prompt tokens', () => {
    const tracker = new CostTracker('gpt-4o');
    const stats = tracker.getStats();
    assert.equal(stats.cacheHitRate, 0);
    assert.equal(stats.cachedPromptTokens, 0);
    assert.equal(stats.uncachedPromptTokens, 0);
  });

  it('accumulates cache stats across multiple calls', () => {
    const tracker = new CostTracker('claude-sonnet-4');
    // $3/M input
    tracker.trackUsage({ promptTokens: 1_000_000, completionTokens: 0, cachedPromptTokens: 800_000 });
    tracker.trackUsage({ promptTokens: 500_000, completionTokens: 0, cachedPromptTokens: 200_000 });

    const stats = tracker.getStats();
    assert.equal(stats.cachedPromptTokens, 1_000_000);
    assert.equal(stats.uncachedPromptTokens, 500_000);
    // Hit rate: 1M cached / 1.5M total = 0.667
    assert.ok(Math.abs(stats.cacheHitRate - 2 / 3) < 1e-6);
    assert.ok(stats.cachedCostSavings > 0);
  });

  it('handles zero cachedPromptTokens gracefully', () => {
    const tracker = new CostTracker('gpt-4o');
    tracker.trackUsage({ promptTokens: 1_000_000, completionTokens: 0, cachedPromptTokens: 0 });

    const stats = tracker.getStats();
    assert.equal(stats.cachedPromptTokens, 0);
    assert.equal(stats.cachedCostSavings, 0);
    assert.equal(stats.uncachedPromptTokens, 1_000_000);
  });
});
