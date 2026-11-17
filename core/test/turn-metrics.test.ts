import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { TurnMetrics, formatTurnSummary } from '../src/turn-metrics';

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

describe('formatTurnSummary — human-readable metrics report', () => {
  it('renders durations via formatDuration, groups numbers, and column-pads labels', () => {
    const tm = new TurnMetrics();
    tm.recordTurn({
      toolCalls: 1200,
      tokensUsed: 45678,
      durationMs: 184000, // 3m 4s
      inputTokens: 12000,
      outputTokens: 30000,
      cacheReadTokens: 3678,
      cacheCreationTokens: 0,
      costUsd: 0.1234,
    });
    const out = formatTurnSummary(tm.getSummary());
    const lines = out.split('\n');

    // thousands separators on counts (not raw "1200"/"45678")
    assert.ok(out.includes('1,200'), `tool calls grouped: ${out}`);
    assert.ok(out.includes('45,678'), `tokens grouped: ${out}`);
    assert.ok(!/\b1200\b/.test(out), `no ungrouped tool count: ${out}`);

    // durations rendered human-readable, never raw ms
    assert.ok(out.includes('3m 4s'), `duration formatted: ${out}`);
    assert.ok(!out.includes('184000'), `no raw ms: ${out}`);

    // token breakdown with pluralized/labelled categories
    assert.ok(out.includes('12,000 input'), `input breakdown: ${out}`);
    assert.ok(out.includes('30,000 output'), `output breakdown: ${out}`);
    assert.ok(out.includes('3,678 cache read'), `cache read breakdown: ${out}`);

    // cost rendered as 4dp under $0.50
    assert.ok(out.includes('$0.1234'), `cost formatted: ${out}`);

    // labels are column-padded (aligned "Label:" prefixes, consistent value column)
    const valueCols = lines
      .filter((l) => /^\S.*:\s{2,}\S/.test(l))
      .map((l) => l.indexOf(l.trimEnd().split(/:\s{2,}/)[1]));
    assert.ok(valueCols.length >= 3, `multiple aligned rows: ${out}`);
    assert.ok(valueCols.every((c) => c === valueCols[0]), `value column aligned: ${out}`);
  });

  it('rounds tokens-per-second to an integer instead of a raw float', () => {
    const tm = new TurnMetrics();
    tm.recordTurn({ toolCalls: 1, tokensUsed: 3333, durationMs: 1000 });
    const out = formatTurnSummary(tm.getSummary());
    assert.ok(/Tokens\/sec:\s+3,333\b/.test(out), `tps rounded+grouped: ${out}`);
    assert.ok(!out.includes('3333.'), `no raw float tps: ${out}`);
  });

  it('pluralizes the turn unit (1 turn vs N turns)', () => {
    const one = new TurnMetrics();
    one.recordTurn({ toolCalls: 0, tokensUsed: 10, durationMs: 10 });
    assert.ok(/\b1 turn\b/.test(formatTurnSummary(one.getSummary())));
    const many = new TurnMetrics();
    many.recordTurn({ toolCalls: 0, tokensUsed: 10, durationMs: 10 });
    many.recordTurn({ toolCalls: 0, tokensUsed: 10, durationMs: 10 });
    assert.ok(/\b2 turns\b/.test(formatTurnSummary(many.getSummary())));
  });

  it('renders a per-model breakdown when models are attributed', () => {
    const tm = new TurnMetrics();
    tm.recordTurn({ toolCalls: 1, tokensUsed: 100, durationMs: 200, model: 'haiku', costUsd: 0.01 });
    tm.recordTurn({ toolCalls: 2, tokensUsed: 2000, durationMs: 800, model: 'opus', costUsd: 0.9 });
    const out = formatTurnSummary(tm.getSummary());
    assert.ok(out.includes('By model'), `has model section: ${out}`);
    assert.ok(out.includes('haiku'), `lists haiku: ${out}`);
    assert.ok(out.includes('opus'), `lists opus: ${out}`);
    // cost > $0.50 renders 2dp (parity with Claude formatCost)
    assert.ok(out.includes('$0.90'), `opus cost 2dp: ${out}`);
  });

  it('omits the model section when no model was attributed', () => {
    const tm = new TurnMetrics();
    tm.recordTurn({ toolCalls: 1, tokensUsed: 100, durationMs: 200 });
    assert.ok(!formatTurnSummary(tm.getSummary()).includes('By model'));
  });

  it('appends a budget line using the shared budget formatter when a budget is set', () => {
    const tm = new TurnMetrics();
    tm.setTurnBudget(50000);
    tm.recordTurn({ toolCalls: 1, tokensUsed: 12345, durationMs: 1000 });
    const out = formatTurnSummary(tm.getSummary());
    assert.ok(out.includes('Turn budget:'), `has budget row: ${out}`);
    assert.ok(out.includes('12,345 / 50,000'), `budget uses shared formatter: ${out}`);
  });

  it('handles an all-zero summary without NaN or raw ms', () => {
    const tm = new TurnMetrics();
    const out = formatTurnSummary(tm.getSummary());
    assert.ok(!out.includes('NaN'), out);
    assert.ok(out.includes('0 turns'), out);
    assert.ok(out.includes('$0.00'), out);
  });
});
