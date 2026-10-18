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
    tm.recordTurn({ toolCalls: 3, tokensUsed: 5000, durationMs: 2000 });
    tm.reset();
    assert.equal(tm.turnCount, 0);
    assert.equal(tm.totalToolCalls, 0);
  });
});
