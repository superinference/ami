import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  MAX_REASONABLE_COST_USD,
  MAX_REASONABLE_TOKENS,
  validateCostEntry,
  serializeCostState,
  deserializeCostState,
} from '../src/cost-tracker';

describe('cost tracking safety constants', () => {
  it('MAX_REASONABLE_COST_USD is 1000', () => {
    assert.equal(MAX_REASONABLE_COST_USD, 1000);
  });

  it('MAX_REASONABLE_TOKENS is 100_000_000', () => {
    assert.equal(MAX_REASONABLE_TOKENS, 100_000_000);
  });
});

describe('validateCostEntry', () => {
  it('accepts normal values', () => {
    const result = validateCostEntry(1.50, 10000, 5000);
    assert.equal(result.valid, true);
    assert.equal(result.reason, undefined);
  });

  it('accepts zero values', () => {
    const result = validateCostEntry(0, 0, 0);
    assert.equal(result.valid, true);
    assert.equal(result.reason, undefined);
  });

  it('rejects negative cost', () => {
    const result = validateCostEntry(-1, 100, 100);
    assert.equal(result.valid, false);
    assert.ok(result.reason!.includes('Negative cost'));
  });

  it('rejects negative input tokens', () => {
    const result = validateCostEntry(1, -100, 100);
    assert.equal(result.valid, false);
    assert.ok(result.reason!.includes('Negative input'));
  });

  it('rejects negative output tokens', () => {
    const result = validateCostEntry(1, 100, -100);
    assert.equal(result.valid, false);
    assert.ok(result.reason!.includes('Negative output'));
  });

  it('warns on very high cost but remains valid', () => {
    const result = validateCostEntry(1500, 1000, 1000);
    assert.equal(result.valid, true);
    assert.ok(result.reason!.includes('exceeds'));
    assert.ok(result.reason!.includes('1000'));
  });

  it('warns on very high token count but remains valid', () => {
    const result = validateCostEntry(5, 80_000_000, 30_000_000);
    assert.equal(result.valid, true);
    assert.ok(result.reason!.includes('exceeds'));
    assert.ok(result.reason!.includes('100000000'));
  });

  it('cost warning takes priority over token warning', () => {
    const result = validateCostEntry(2000, 200_000_000, 0);
    assert.equal(result.valid, true);
    assert.ok(result.reason!.includes('Cost'));
  });

  it('accepts values just below thresholds', () => {
    const result = validateCostEntry(999, 99_999_999, 0);
    assert.equal(result.valid, true);
    assert.equal(result.reason, undefined);
  });
});

describe('serializeCostState', () => {
  it('produces valid JSON', () => {
    const state = { totalCost: 5.25, totalInputTokens: 10000, totalOutputTokens: 5000, requestCount: 3 };
    const json = serializeCostState(state);
    const parsed = JSON.parse(json);
    assert.equal(typeof parsed, 'object');
  });

  it('includes all fields plus version', () => {
    const state = { totalCost: 1.5, totalInputTokens: 100, totalOutputTokens: 50, requestCount: 1 };
    const json = serializeCostState(state);
    const parsed = JSON.parse(json);
    assert.equal(parsed.totalCost, 1.5);
    assert.equal(parsed.totalInputTokens, 100);
    assert.equal(parsed.totalOutputTokens, 50);
    assert.equal(parsed.requestCount, 1);
    assert.equal(parsed.version, 1);
  });

  it('handles zero values', () => {
    const state = { totalCost: 0, totalInputTokens: 0, totalOutputTokens: 0, requestCount: 0 };
    const json = serializeCostState(state);
    const parsed = JSON.parse(json);
    assert.equal(parsed.totalCost, 0);
  });
});

describe('deserializeCostState', () => {
  it('parses valid state', () => {
    const json = JSON.stringify({ totalCost: 2.5, totalInputTokens: 200, totalOutputTokens: 100, requestCount: 2, version: 1 });
    const result = deserializeCostState(json);
    assert.ok(result !== null);
    assert.equal(result!.totalCost, 2.5);
    assert.equal(result!.totalInputTokens, 200);
    assert.equal(result!.totalOutputTokens, 100);
    assert.equal(result!.requestCount, 2);
  });

  it('returns null for invalid JSON', () => {
    assert.equal(deserializeCostState('not json'), null);
  });

  it('returns null for empty string', () => {
    assert.equal(deserializeCostState(''), null);
  });

  it('returns null for missing totalCost', () => {
    const json = JSON.stringify({ totalInputTokens: 100, totalOutputTokens: 50, requestCount: 1 });
    assert.equal(deserializeCostState(json), null);
  });

  it('returns null for missing totalInputTokens', () => {
    const json = JSON.stringify({ totalCost: 1, totalOutputTokens: 50, requestCount: 1 });
    assert.equal(deserializeCostState(json), null);
  });

  it('returns null for missing totalOutputTokens', () => {
    const json = JSON.stringify({ totalCost: 1, totalInputTokens: 100, requestCount: 1 });
    assert.equal(deserializeCostState(json), null);
  });

  it('returns null for missing requestCount', () => {
    const json = JSON.stringify({ totalCost: 1, totalInputTokens: 100, totalOutputTokens: 50 });
    assert.equal(deserializeCostState(json), null);
  });

  it('returns null for negative values', () => {
    const json = JSON.stringify({ totalCost: -1, totalInputTokens: 100, totalOutputTokens: 50, requestCount: 1 });
    assert.equal(deserializeCostState(json), null);
  });

  it('returns null for non-number fields', () => {
    const json = JSON.stringify({ totalCost: 'abc', totalInputTokens: 100, totalOutputTokens: 50, requestCount: 1 });
    assert.equal(deserializeCostState(json), null);
  });

  it('returns null for null input', () => {
    const json = JSON.stringify(null);
    assert.equal(deserializeCostState(json), null);
  });

  it('returns null for array input', () => {
    const json = JSON.stringify([1, 2, 3]);
    assert.equal(deserializeCostState(json), null);
  });
});

describe('serialize/deserialize round-trip', () => {
  it('round-trips correctly', () => {
    const original = { totalCost: 42.75, totalInputTokens: 500000, totalOutputTokens: 250000, requestCount: 15 };
    const json = serializeCostState(original);
    const restored = deserializeCostState(json);
    assert.deepStrictEqual(restored, original);
  });

  it('round-trips zero state', () => {
    const original = { totalCost: 0, totalInputTokens: 0, totalOutputTokens: 0, requestCount: 0 };
    const json = serializeCostState(original);
    const restored = deserializeCostState(json);
    assert.deepStrictEqual(restored, original);
  });

  it('round-trips large values', () => {
    const original = { totalCost: 999.99, totalInputTokens: 99_999_999, totalOutputTokens: 50_000_000, requestCount: 1000 };
    const json = serializeCostState(original);
    const restored = deserializeCostState(json);
    assert.deepStrictEqual(restored, original);
  });
});
