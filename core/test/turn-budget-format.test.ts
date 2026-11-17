import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { formatTurnBudget, TurnMetrics } from '../src/turn-metrics';

describe('turn-metrics: formatTurnBudget', () => {
  it('returns empty string when no budget is configured', () => {
    assert.equal(formatTurnBudget({ limit: 0, tokens: 100, pct: 0, exceeded: false, nudges: 0 }), '');
  });

  it('renders within-budget usage as tokens / limit (pct%)', () => {
    assert.equal(
      formatTurnBudget({ limit: 10000, tokens: 2500, pct: 25, exceeded: false, nudges: 0 }),
      '2,500 / 10,000 (25%)',
    );
  });

  it('renders exceeded usage as tokens used (limit min ✓)', () => {
    assert.equal(
      formatTurnBudget({ limit: 10000, tokens: 12000, pct: 120, exceeded: true, nudges: 0 }),
      '12,000 used (10,000 min ✓)',
    );
  });

  it('appends a singular nudge suffix', () => {
    assert.equal(
      formatTurnBudget({ limit: 1000, tokens: 500, pct: 50, exceeded: false, nudges: 1 }),
      '500 / 1,000 (50%) · 1 nudge',
    );
  });

  it('appends a plural nudges suffix', () => {
    assert.equal(
      formatTurnBudget({ limit: 1000, tokens: 1200, pct: 120, exceeded: true, nudges: 3 }),
      '1,200 used (1,000 min ✓) · 3 nudges',
    );
  });

  it('formats the live budget status produced by TurnMetrics end to end', () => {
    const m = new TurnMetrics();
    m.setTurnBudget(1000);
    m.recordBudgetNudge();
    m.recordTurn({ toolCalls: 1, tokensUsed: 250, durationMs: 10 });
    const status = m.getBudgetStatus();
    assert.ok(status);
    assert.equal(formatTurnBudget(status!), '250 / 1,000 (25%) · 1 nudge');
  });
});
