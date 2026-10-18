import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  modelSwitchNeedsCompaction,
  getModelSwitchWarning,
  estimateCompactionCost,
  getContextWindow,
} from '../src/model-capabilities';

describe('modelSwitchNeedsCompaction', () => {
  it('returns false for same model with low token usage', () => {
    assert.equal(modelSwitchNeedsCompaction('claude-opus-4', 'claude-opus-4', 10_000), false);
  });

  it('returns false when tokens fit comfortably in target window', () => {
    assert.equal(modelSwitchNeedsCompaction('claude-opus-4', 'gpt-4o', 50_000), false);
  });

  it('returns true when tokens exceed 90% of target window', () => {
    const targetWindow = getContextWindow('deepseek-chat'); // 64000
    assert.equal(modelSwitchNeedsCompaction('claude-opus-4', 'deepseek-chat', targetWindow * 0.95), true);
  });

  it('returns true when tokens exceed target window entirely', () => {
    assert.equal(modelSwitchNeedsCompaction('claude-opus-4', 'deepseek-chat', 100_000), true);
  });

  it('returns false when tokens are below 90% of target', () => {
    const targetWindow = getContextWindow('gpt-4o'); // 128000
    assert.equal(modelSwitchNeedsCompaction('claude-opus-4', 'gpt-4o', targetWindow * 0.5), false);
  });

  it('handles switching from small to large model', () => {
    assert.equal(modelSwitchNeedsCompaction('deepseek-chat', 'claude-opus-4', 60_000), false);
  });
});

describe('getModelSwitchWarning', () => {
  it('returns null for same model', () => {
    assert.equal(getModelSwitchWarning('claude-opus-4', 'claude-opus-4', 10_000), null);
  });

  it('returns null when target has same or larger window', () => {
    assert.equal(getModelSwitchWarning('gpt-4o', 'claude-opus-4', 50_000), null);
  });

  it('returns critical warning when tokens exceed target window', () => {
    const warning = getModelSwitchWarning('claude-opus-4', 'deepseek-chat', 100_000);
    assert.ok(warning !== null);
    assert.ok(warning.includes('Critical'));
    assert.ok(warning.includes('Compaction required'));
  });

  it('returns warning when tokens are near target window limit', () => {
    const targetWindow = getContextWindow('deepseek-chat'); // 64000
    const warning = getModelSwitchWarning('claude-opus-4', 'deepseek-chat', targetWindow * 0.85);
    assert.ok(warning !== null);
    assert.ok(warning.includes('Warning'));
  });

  it('returns note when target is smaller but tokens are low', () => {
    const warning = getModelSwitchWarning('claude-opus-4', 'deepseek-chat', 10_000);
    assert.ok(warning !== null);
    assert.ok(warning.includes('Note'));
    assert.ok(warning.includes('smaller context window'));
  });

  it('includes model names and token counts in the message', () => {
    const warning = getModelSwitchWarning('claude-opus-4', 'deepseek-chat', 70_000);
    assert.ok(warning !== null);
    assert.ok(warning.includes('deepseek-chat'));
    assert.ok(warning.includes('70000'));
  });
});

describe('estimateCompactionCost', () => {
  it('returns non-negative values for known model', () => {
    const cost = estimateCompactionCost('claude-opus-4', 50_000);
    assert.ok(cost.inputCost >= 0);
    assert.ok(cost.outputCost >= 0);
  });

  it('returns positive costs for known model with tokens', () => {
    const cost = estimateCompactionCost('claude-opus-4', 50_000);
    assert.ok(cost.inputCost > 0);
    assert.ok(cost.outputCost > 0);
  });

  it('returns zero for unknown model', () => {
    const cost = estimateCompactionCost('totally-unknown-model-xyz', 50_000);
    assert.equal(cost.inputCost, 0);
    assert.equal(cost.outputCost, 0);
  });

  it('returns zero for zero tokens', () => {
    const cost = estimateCompactionCost('claude-opus-4', 0);
    assert.equal(cost.inputCost, 0);
    assert.equal(cost.outputCost, 0);
  });

  it('scales linearly with token count', () => {
    const cost1 = estimateCompactionCost('gpt-4o', 10_000);
    const cost2 = estimateCompactionCost('gpt-4o', 20_000);
    assert.ok(Math.abs(cost2.inputCost / cost1.inputCost - 2) < 0.01);
  });

  it('output cost is lower than input cost (summary is shorter)', () => {
    const cost = estimateCompactionCost('claude-sonnet-4', 100_000);
    assert.ok(cost.outputCost < cost.inputCost);
  });

  it('works for deepseek models', () => {
    const cost = estimateCompactionCost('deepseek-chat', 50_000);
    assert.ok(cost.inputCost > 0);
  });

  it('works for gemini models', () => {
    const cost = estimateCompactionCost('gemini-2.5-pro', 50_000);
    assert.ok(cost.inputCost > 0);
  });
});
