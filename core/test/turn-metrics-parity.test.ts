import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { TurnMetrics } from '../src/turn-metrics';

/**
 * Parity hardening for per-turn metrics (additive superset of the reference's
 * turn accounting). Each block is a RED-GREEN reproducer: it fails when the
 * corresponding change in turn-metrics.ts is reverted.
 */

describe('TurnMetrics parity — cost rollup', () => {
  it('accumulates per-turn USD cost into cumulative + average totals', () => {
    const tm = new TurnMetrics();
    tm.recordTurn({ toolCalls: 1, tokensUsed: 100, durationMs: 100, costUsd: 0.25 });
    tm.recordTurn({ toolCalls: 1, tokensUsed: 100, durationMs: 100, costUsd: 0.75 });
    assert.equal(tm.totalCostUsd, 1);
    assert.equal(tm.avgCostPerTurn, 0.5);
    const s = tm.getSummary();
    assert.equal(s.totalCostUsd, 1);
    assert.equal(s.avgCostPerTurn, 0.5);
  });

  it('attributes cost per model and sanitizes bad cost values to 0', () => {
    const tm = new TurnMetrics();
    tm.recordTurn({ toolCalls: 1, tokensUsed: 10, durationMs: 10, model: 'opus', costUsd: 2 });
    tm.recordTurn({ toolCalls: 1, tokensUsed: 10, durationMs: 10, model: 'opus', costUsd: 3 });
    // NaN cost must not poison the rollup
    tm.recordTurn({ toolCalls: 1, tokensUsed: 10, durationMs: 10, model: 'haiku', costUsd: NaN });
    const byModel = tm.getModelUsage();
    assert.equal(byModel.opus.costUsd, 5);
    assert.equal(byModel.haiku.costUsd, 0);
    assert.equal(tm.totalCostUsd, 5);
  });

  it('avgCostPerTurn is 0 with no turns', () => {
    const tm = new TurnMetrics();
    assert.equal(tm.avgCostPerTurn, 0);
  });
});

describe('TurnMetrics parity — token-type breakdown', () => {
  it('splits cumulative tokens by input/output/cacheRead/cacheCreation', () => {
    const tm = new TurnMetrics();
    tm.recordTurn({
      toolCalls: 1, tokensUsed: 300, durationMs: 100,
      inputTokens: 100, outputTokens: 50, cacheReadTokens: 120, cacheCreationTokens: 30,
    });
    tm.recordTurn({
      toolCalls: 1, tokensUsed: 60, durationMs: 100,
      inputTokens: 40, outputTokens: 20, cacheReadTokens: 0, cacheCreationTokens: 0,
    });
    const b = tm.getTokenBreakdown();
    assert.equal(b.input, 140);
    assert.equal(b.output, 70);
    assert.equal(b.cacheRead, 120);
    assert.equal(b.cacheCreation, 30);
    assert.deepEqual(tm.getSummary().tokens, { input: 140, output: 70, cacheRead: 120, cacheCreation: 30 });
  });

  it('coerces NaN/negative token-type fields to 0', () => {
    const tm = new TurnMetrics();
    tm.recordTurn({
      toolCalls: 1, tokensUsed: 10, durationMs: 10,
      inputTokens: NaN, outputTokens: -5, cacheReadTokens: Infinity, cacheCreationTokens: undefined as unknown as number,
    });
    assert.deepEqual(tm.getTokenBreakdown(), { input: 0, output: 0, cacheRead: 0, cacheCreation: 0 });
  });
});

describe('TurnMetrics parity — overflow / precision clamp', () => {
  it('clamps cumulative token/toolCall/duration counters at Number.MAX_SAFE_INTEGER', () => {
    const tm = new TurnMetrics();
    const MAX = Number.MAX_SAFE_INTEGER;
    tm.recordTurn({ toolCalls: MAX, tokensUsed: MAX, durationMs: MAX });
    tm.recordTurn({ toolCalls: MAX, tokensUsed: MAX, durationMs: MAX });
    // Without the clamp these become 2*MAX (> MAX_SAFE_INTEGER, lossy); with it they pin to MAX.
    assert.equal(tm.totalTokens, MAX);
    assert.equal(tm.totalToolCalls, MAX);
    assert.equal(tm.totalDurationMs, MAX);
  });

  it('clamps per-model counters too', () => {
    const tm = new TurnMetrics();
    const MAX = Number.MAX_SAFE_INTEGER;
    tm.recordTurn({ toolCalls: MAX, tokensUsed: MAX, durationMs: MAX, model: 'm' });
    tm.recordTurn({ toolCalls: MAX, tokensUsed: MAX, durationMs: MAX, model: 'm' });
    const m = tm.getModelUsage().m;
    assert.equal(m.tokens, MAX);
    assert.equal(m.toolCalls, MAX);
    assert.equal(m.durationMs, MAX);
  });
});

describe('TurnMetrics parity — snapshot / restore', () => {
  it('round-trips cumulative state through snapshot() → reset() → restore()', () => {
    const tm = new TurnMetrics();
    tm.recordTurn({
      toolCalls: 2, tokensUsed: 500, durationMs: 1000, retryDurationMs: 200, model: 'opus',
      costUsd: 0.4, inputTokens: 300, outputTokens: 200, cacheReadTokens: 10, cacheCreationTokens: 5,
    });
    tm.recordTurn({ toolCalls: 1, tokensUsed: 100, durationMs: 300, model: 'haiku', costUsd: 0.1 });
    const snap = tm.snapshot();

    const restored = new TurnMetrics();
    restored.recordTurn({ toolCalls: 99, tokensUsed: 99, durationMs: 99 }); // dirty state
    restored.restore(snap);

    assert.equal(restored.turnCount, 2);
    assert.equal(restored.totalToolCalls, 3);
    assert.equal(restored.totalTokens, 600);
    assert.equal(restored.totalDurationMs, 1300);
    assert.equal(restored.totalDurationMsWithoutRetries, 1100);
    assert.equal(restored.longestTurnMs, 1000);
    assert.equal(restored.totalCostUsd, 0.5);
    assert.deepEqual(restored.getTokenBreakdown(), snap.tokens);
    assert.deepEqual(restored.getModelUsage(), snap.byModel);
  });

  it('restore(null) and a partial/corrupt blob reset to clean, sanitized state', () => {
    const tm = new TurnMetrics();
    tm.recordTurn({ toolCalls: 5, tokensUsed: 5, durationMs: 5 });
    tm.restore(null);
    assert.equal(tm.turnCount, 0);
    assert.equal(tm.totalTokens, 0);

    tm.restore({ turnCount: 3, totalTokens: -10, totalCostUsd: NaN });
    assert.equal(tm.turnCount, 3);
    assert.equal(tm.totalTokens, 0);
    assert.equal(tm.totalCostUsd, 0);
    assert.deepEqual(tm.getModelUsage(), {});
  });

  it('snapshot does not carry transient budget/active-time state', () => {
    const tm = new TurnMetrics();
    tm.setTurnBudget(1000);
    tm.recordTurn({ toolCalls: 1, tokensUsed: 100, durationMs: 10 });
    const snap = tm.snapshot();
    assert.equal('budget' in (snap as object), false);
    assert.equal('activeTimeMs' in (snap as object), false);
  });
});
