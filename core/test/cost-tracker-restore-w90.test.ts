/**
 * w90/U004 — unit: CostTracker.restoreTotals re-seeds a resumed session's coarse totals.
 *
 * PARITY (Claude's restoreCostStateForSession/setCostStateForRestore): resuming the SAME
 * session must continue its /cost figure instead of restarting at $0. restoreTotals folds
 * a persisted {@link CostState} (the serialize/deserialize round-trip the engine stores in
 * the session file) back into a fresh tracker's running totals.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  CostTracker,
  serializeCostState,
  deserializeCostState,
  type CostState,
} from '../src/cost-tracker';

describe('w90/U004: CostTracker.restoreTotals', () => {
  it('seeds a fresh tracker from a persisted cost state', () => {
    const t = new CostTracker('test-model');
    assert.equal(t.getStats().totalCost, 0, 'fresh tracker starts at $0');
    const state: CostState = {
      totalCost: 1.2345,
      totalInputTokens: 5000,
      totalOutputTokens: 1200,
      requestCount: 7,
    };
    t.restoreTotals(state);
    const s = t.getStats();
    assert.equal(s.totalCost, 1.2345);
    assert.equal(s.promptTokens, 5000);
    assert.equal(s.completionTokens, 1200);
    assert.equal(s.totalTokens, 6200);
    assert.equal(s.requestCount, 7);
  });

  it('round-trips through serialize/deserialize as the engine persists it', () => {
    const state: CostState = {
      totalCost: 0.5,
      totalInputTokens: 100,
      totalOutputTokens: 40,
      requestCount: 2,
    };
    const restored = deserializeCostState(serializeCostState(state));
    assert.ok(restored, 'round-trip must succeed');
    const t = new CostTracker('test-model');
    t.restoreTotals(restored!);
    const s = t.getStats();
    assert.equal(s.totalCost, 0.5);
    assert.equal(s.totalTokens, 140);
    assert.equal(s.requestCount, 2);
  });

  it('clamps negative fields to zero', () => {
    const t = new CostTracker('test-model');
    t.restoreTotals({ totalCost: -5, totalInputTokens: -10, totalOutputTokens: -3, requestCount: -1 });
    const s = t.getStats();
    assert.equal(s.totalCost, 0);
    assert.equal(s.totalTokens, 0);
    assert.equal(s.requestCount, 0);
  });

  it('is additive — restored totals accumulate on top of new usage', () => {
    const t = new CostTracker('test-model');
    t.restoreTotals({ totalCost: 1, totalInputTokens: 10, totalOutputTokens: 10, requestCount: 1 });
    t.trackUsage({ promptTokens: 5, completionTokens: 5 });
    const s = t.getStats();
    assert.equal(s.promptTokens, 15);
    assert.equal(s.completionTokens, 15);
    assert.equal(s.requestCount, 2);
  });
});
