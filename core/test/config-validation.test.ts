import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { validateEngineConfig, sanitizeEngineConfig } from '../src/types';

describe('validateEngineConfig', () => {
  it('accepts a valid config', () => {
    const r = validateEngineConfig({ maxTurns: 100, maxSessionMs: 60_000, maxBudgetUsd: 5 });
    assert.equal(r.valid, true);
    assert.equal(r.warnings.length, 0);
  });

  it('accepts an empty config', () => {
    const r = validateEngineConfig({});
    assert.equal(r.valid, true);
    assert.equal(r.warnings.length, 0);
  });

  // maxTurns
  it('rejects negative maxTurns', () => {
    const r = validateEngineConfig({ maxTurns: -1 });
    assert.equal(r.valid, false);
    assert.ok(r.warnings.some(w => w.includes('maxTurns')));
  });

  it('rejects maxTurns = 0', () => {
    const r = validateEngineConfig({ maxTurns: 0 });
    assert.equal(r.valid, false);
  });

  it('warns on maxTurns > 10000 but stays valid', () => {
    const r = validateEngineConfig({ maxTurns: 10001 });
    assert.equal(r.valid, true);
    assert.ok(r.warnings.some(w => w.includes('10000')));
  });

  it('rejects float maxTurns', () => {
    const r = validateEngineConfig({ maxTurns: 1.5 });
    assert.equal(r.valid, false);
  });

  // maxSessionMs
  it('rejects negative maxSessionMs', () => {
    const r = validateEngineConfig({ maxSessionMs: -100 });
    assert.equal(r.valid, false);
  });

  it('rejects maxSessionMs > 24h', () => {
    const r = validateEngineConfig({ maxSessionMs: 86_400_001 });
    assert.equal(r.valid, false);
    assert.ok(r.warnings.some(w => w.includes('24 hours')));
  });

  it('warns on maxSessionMs = 2 hours', () => {
    const r = validateEngineConfig({ maxSessionMs: 7_200_000 });
    assert.equal(r.valid, true);
    assert.ok(r.warnings.some(w => w.includes('1 hour')));
  });

  // maxBudgetUsd
  it('rejects negative maxBudgetUsd', () => {
    const r = validateEngineConfig({ maxBudgetUsd: -5 });
    assert.equal(r.valid, false);
  });

  it('rejects maxBudgetUsd > 1000', () => {
    const r = validateEngineConfig({ maxBudgetUsd: 1001 });
    assert.equal(r.valid, false);
    assert.ok(r.warnings.some(w => w.includes('$1000')));
  });

  it('warns on maxBudgetUsd = 200', () => {
    const r = validateEngineConfig({ maxBudgetUsd: 200 });
    assert.equal(r.valid, true);
    assert.ok(r.warnings.some(w => w.includes('$100')));
  });

  // maxAgentDepth
  it('rejects maxAgentDepth = 0', () => {
    const r = validateEngineConfig({ maxAgentDepth: 0 });
    assert.equal(r.valid, false);
  });

  it('rejects maxAgentDepth = 21', () => {
    const r = validateEngineConfig({ maxAgentDepth: 21 });
    assert.equal(r.valid, false);
    assert.ok(r.warnings.some(w => w.includes('1-20')));
  });

  it('warns on maxAgentDepth = 15', () => {
    const r = validateEngineConfig({ maxAgentDepth: 15 });
    assert.equal(r.valid, true);
    assert.ok(r.warnings.some(w => w.includes('deep nesting')));
  });

  // maxUsageLimitWaitMs
  it('rejects negative maxUsageLimitWaitMs', () => {
    const r = validateEngineConfig({ maxUsageLimitWaitMs: -1 });
    assert.equal(r.valid, false);
  });

  it('accepts maxUsageLimitWaitMs = 0', () => {
    const r = validateEngineConfig({ maxUsageLimitWaitMs: 0 });
    assert.equal(r.valid, true);
  });

  // requestTimeoutMs
  it('rejects requestTimeoutMs = 500', () => {
    const r = validateEngineConfig({ requestTimeoutMs: 500 });
    assert.equal(r.valid, false);
  });

  it('rejects requestTimeoutMs > 600000', () => {
    const r = validateEngineConfig({ requestTimeoutMs: 600_001 });
    assert.equal(r.valid, false);
  });

  it('warns on requestTimeoutMs > 300000', () => {
    const r = validateEngineConfig({ requestTimeoutMs: 350_000 });
    assert.equal(r.valid, true);
    assert.ok(r.warnings.some(w => w.includes('5min')));
  });

  // tokenBudget
  it('rejects tokenBudget = 0', () => {
    const r = validateEngineConfig({ tokenBudget: 0 });
    assert.equal(r.valid, false);
  });

  it('rejects negative tokenBudget', () => {
    const r = validateEngineConfig({ tokenBudget: -100 });
    assert.equal(r.valid, false);
  });
});

describe('sanitizeEngineConfig', () => {
  it('clamps maxTurns to range', () => {
    const r = sanitizeEngineConfig({ maxTurns: -5 });
    assert.equal(r.maxTurns, 1);
    const r2 = sanitizeEngineConfig({ maxTurns: 99999 });
    assert.equal(r2.maxTurns, 10000);
  });

  it('rounds float maxTurns', () => {
    const r = sanitizeEngineConfig({ maxTurns: 1.7 });
    assert.equal(r.maxTurns, 2);
  });

  it('clamps maxSessionMs', () => {
    const r = sanitizeEngineConfig({ maxSessionMs: -100 });
    assert.equal(r.maxSessionMs, 1);
    const r2 = sanitizeEngineConfig({ maxSessionMs: 100_000_000 });
    assert.equal(r2.maxSessionMs, 86_400_000);
  });

  it('clamps maxBudgetUsd', () => {
    const r = sanitizeEngineConfig({ maxBudgetUsd: -5 });
    assert.equal(r.maxBudgetUsd, 0.01);
    const r2 = sanitizeEngineConfig({ maxBudgetUsd: 5000 });
    assert.equal(r2.maxBudgetUsd, 1000);
  });

  it('clamps maxAgentDepth', () => {
    const r = sanitizeEngineConfig({ maxAgentDepth: -3 });
    assert.equal(r.maxAgentDepth, 1);
    const r2 = sanitizeEngineConfig({ maxAgentDepth: 50 });
    assert.equal(r2.maxAgentDepth, 20);
  });

  it('floors maxUsageLimitWaitMs at 0', () => {
    const r = sanitizeEngineConfig({ maxUsageLimitWaitMs: -500 });
    assert.equal(r.maxUsageLimitWaitMs, 0);
  });

  it('clamps requestTimeoutMs', () => {
    const r = sanitizeEngineConfig({ requestTimeoutMs: 100 });
    assert.equal(r.requestTimeoutMs, 1000);
    const r2 = sanitizeEngineConfig({ requestTimeoutMs: 999_999 });
    assert.equal(r2.requestTimeoutMs, 600_000);
  });

  it('floors tokenBudget at 1', () => {
    const r = sanitizeEngineConfig({ tokenBudget: -100 });
    assert.equal(r.tokenBudget, 1);
  });

  it('preserves valid values unchanged', () => {
    const input = { maxTurns: 50, maxSessionMs: 30_000, maxBudgetUsd: 10, maxAgentDepth: 5 };
    const r = sanitizeEngineConfig(input);
    assert.equal(r.maxTurns, 50);
    assert.equal(r.maxSessionMs, 30_000);
    assert.equal(r.maxBudgetUsd, 10);
    assert.equal(r.maxAgentDepth, 5);
  });

  it('does not mutate the input', () => {
    const input = { maxTurns: -5 };
    sanitizeEngineConfig(input);
    assert.equal(input.maxTurns, -5);
  });
});
