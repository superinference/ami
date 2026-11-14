import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { TurnMetrics } from '../src/turn-metrics';

describe('TurnMetrics', () => {
  it('starts with zero turns', () => {
    const tm = new TurnMetrics();
    assert.equal(tm.turnCount, 0);
  });

  it('records a turn with tool calls', () => {
    const tm = new TurnMetrics();
    tm.recordTurn({ toolCalls: 3, tokensUsed: 5000, durationMs: 1500 });
    assert.equal(tm.turnCount, 1);
    assert.equal(tm.totalToolCalls, 3);
    assert.equal(tm.totalTokens, 5000);
  });

  it('calculates average tool calls per turn', () => {
    const tm = new TurnMetrics();
    tm.recordTurn({ toolCalls: 2, tokensUsed: 1000, durationMs: 500 });
    tm.recordTurn({ toolCalls: 4, tokensUsed: 2000, durationMs: 800 });
    assert.equal(tm.avgToolCallsPerTurn, 3);
  });

  it('calculates average tokens per turn', () => {
    const tm = new TurnMetrics();
    tm.recordTurn({ toolCalls: 1, tokensUsed: 1000, durationMs: 500 });
    tm.recordTurn({ toolCalls: 1, tokensUsed: 3000, durationMs: 800 });
    assert.equal(tm.avgTokensPerTurn, 2000);
  });

  it('tracks total duration', () => {
    const tm = new TurnMetrics();
    tm.recordTurn({ toolCalls: 1, tokensUsed: 1000, durationMs: 500 });
    tm.recordTurn({ toolCalls: 1, tokensUsed: 1000, durationMs: 700 });
    assert.equal(tm.totalDurationMs, 1200);
  });

  it('calculates tokens per second', () => {
    const tm = new TurnMetrics();
    tm.recordTurn({ toolCalls: 1, tokensUsed: 5000, durationMs: 1000 });
    assert.equal(tm.tokensPerSecond, 5000);
  });

  it('handles zero duration gracefully', () => {
    const tm = new TurnMetrics();
    tm.recordTurn({ toolCalls: 1, tokensUsed: 1000, durationMs: 0 });
    assert.equal(tm.tokensPerSecond, 0);
  });

  it('tracks longest turn', () => {
    const tm = new TurnMetrics();
    tm.recordTurn({ toolCalls: 1, tokensUsed: 1000, durationMs: 500 });
    tm.recordTurn({ toolCalls: 5, tokensUsed: 8000, durationMs: 3000 });
    tm.recordTurn({ toolCalls: 2, tokensUsed: 2000, durationMs: 1000 });
    assert.equal(tm.longestTurnMs, 3000);
  });

  it('getSummary returns all metrics', () => {
    const tm = new TurnMetrics();
    tm.recordTurn({ toolCalls: 3, tokensUsed: 5000, durationMs: 2000 });
    const summary = tm.getSummary();
    assert.equal(summary.turnCount, 1);
    assert.equal(summary.totalToolCalls, 3);
    assert.equal(summary.totalTokens, 5000);
    assert.equal(summary.totalDurationMs, 2000);
    assert.equal(summary.avgToolCallsPerTurn, 3);
    assert.equal(summary.avgTokensPerTurn, 5000);
    assert.equal(summary.tokensPerSecond, 2500);
    assert.equal(summary.longestTurnMs, 2000);
  });

  it('reset clears all metrics', () => {
    const tm = new TurnMetrics();
    tm.recordTurn({ toolCalls: 3, tokensUsed: 5000, durationMs: 2000, model: 'm', retryDurationMs: 100 });
    tm.setTurnBudget(10);
    tm.recordBudgetNudge();
    tm.beginOperation('a', 0);
    tm.endOperation('a', 50);
    tm.reset();
    assert.equal(tm.turnCount, 0);
    assert.equal(tm.totalToolCalls, 0);
    assert.equal(tm.totalDurationMsWithoutRetries, 0);
    assert.equal(tm.getBudgetStatus(), null);
    assert.deepEqual(tm.getModelUsage(), {});
    assert.equal(tm.getActiveTimeMs(100), 0);
  });
});

describe('TurnMetrics — defensive numeric accumulation', () => {
  it('coerces missing/NaN/Infinity/negative fields to 0 instead of poisoning totals', () => {
    const tm = new TurnMetrics();
    tm.recordTurn({ toolCalls: NaN, tokensUsed: undefined as unknown as number, durationMs: Infinity });
    tm.recordTurn({ toolCalls: -3, tokensUsed: -100, durationMs: -5 });
    assert.equal(tm.totalToolCalls, 0);
    assert.equal(tm.totalTokens, 0);
    assert.equal(tm.totalDurationMs, 0);
    assert.ok(Number.isFinite(tm.tokensPerSecond));
    // a valid turn afterwards still accumulates cleanly
    tm.recordTurn({ toolCalls: 2, tokensUsed: 1000, durationMs: 1000 });
    assert.equal(tm.totalTokens, 1000);
    assert.equal(tm.tokensPerSecond, 1000);
  });
});

describe('TurnMetrics — per-model attribution', () => {
  it('attributes usage per model across switches', () => {
    const tm = new TurnMetrics();
    tm.recordTurn({ toolCalls: 1, tokensUsed: 100, durationMs: 200, model: 'haiku' });
    tm.recordTurn({ toolCalls: 2, tokensUsed: 900, durationMs: 800, model: 'opus' });
    tm.recordTurn({ toolCalls: 1, tokensUsed: 50, durationMs: 100, model: 'haiku' });
    const byModel = tm.getSummary().byModel;
    assert.equal(byModel.haiku.turnCount, 2);
    assert.equal(byModel.haiku.tokens, 150);
    assert.equal(byModel.opus.tokens, 900);
  });

  it('turns without a model are not attributed', () => {
    const tm = new TurnMetrics();
    tm.recordTurn({ toolCalls: 1, tokensUsed: 100, durationMs: 200 });
    assert.deepEqual(tm.getModelUsage(), {});
  });
});

describe('TurnMetrics — retry-excluded duration', () => {
  it('separates total duration from duration-without-retries and raises the retry-excluded rate', () => {
    const tm = new TurnMetrics();
    tm.recordTurn({ toolCalls: 1, tokensUsed: 2000, durationMs: 2000, retryDurationMs: 1000 });
    assert.equal(tm.totalDurationMs, 2000);
    assert.equal(tm.totalDurationMsWithoutRetries, 1000);
    assert.equal(tm.tokensPerSecond, 1000);
    assert.equal(tm.tokensPerSecondWithoutRetries, 2000);
  });

  it('clamps retry time to the turn duration', () => {
    const tm = new TurnMetrics();
    tm.recordTurn({ toolCalls: 0, tokensUsed: 10, durationMs: 100, retryDurationMs: 999 });
    assert.equal(tm.totalDurationMsWithoutRetries, 0);
    assert.equal(tm.tokensPerSecondWithoutRetries, 0);
  });
});

describe('TurnMetrics — overlap-deduplicated active time', () => {
  it('counts N overlapping operations as a single active window', () => {
    const tm = new TurnMetrics();
    tm.beginOperation('a', 0);
    tm.beginOperation('b', 100); // overlaps a
    tm.endOperation('a', 200);
    tm.endOperation('b', 300);   // window closes here: 0..300
    assert.equal(tm.getActiveTimeMs(500), 300);
  });

  it('includes a still-open window up to now', () => {
    const tm = new TurnMetrics();
    tm.beginOperation('a', 100);
    assert.equal(tm.getActiveTimeMs(400), 300);
  });

  it('ignores end for an unknown operation id', () => {
    const tm = new TurnMetrics();
    tm.endOperation('ghost', 100);
    assert.equal(tm.getActiveTimeMs(100), 0);
  });
});

describe('TurnMetrics — per-turn token budget', () => {
  it('reports used/limit/pct and nudges, flagging exceed', () => {
    const tm = new TurnMetrics();
    assert.equal(tm.getBudgetStatus(), null);
    tm.setTurnBudget(1000);
    tm.recordTurn({ toolCalls: 1, tokensUsed: 500, durationMs: 100 });
    let b = tm.getBudgetStatus()!;
    assert.equal(b.limit, 1000);
    assert.equal(b.tokens, 500);
    assert.equal(b.pct, 50);
    assert.equal(b.exceeded, false);
    tm.recordTurn({ toolCalls: 1, tokensUsed: 1500, durationMs: 100 });
    tm.recordBudgetNudge();
    b = tm.getBudgetStatus()!;
    assert.equal(b.tokens, 1500);
    assert.equal(b.exceeded, true);
    assert.equal(b.nudges, 1);
  });
});
