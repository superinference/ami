import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  getContextWindow,
  getModelCapabilities,
  isReasoningModel,
  resolveThinkingBudget,
  resolveTemperature,
  getProviderSamplingDefaults,
  type ThinkingLevel,
} from '../src/model-capabilities';

// ---------------------------------------------------------------------------
// getContextWindow
// ---------------------------------------------------------------------------
describe('getContextWindow', () => {
  it('returns 200000 for claude-opus-4', () => {
    assert.equal(getContextWindow('claude-opus-4'), 200000);
  });

  it('returns 200000 for claude-sonnet-4', () => {
    assert.equal(getContextWindow('claude-sonnet-4'), 200000);
  });

  it('returns 200000 for claude-haiku-3.5', () => {
    assert.equal(getContextWindow('claude-haiku-3.5'), 200000);
  });

  it('returns 128000 for gpt-4o', () => {
    assert.equal(getContextWindow('gpt-4o'), 128000);
  });

  it('returns 128000 for gpt-4-turbo', () => {
    assert.equal(getContextWindow('gpt-4-turbo'), 128000);
  });

  it('returns 200000 for o1', () => {
    assert.equal(getContextWindow('o1'), 200000);
  });

  it('returns 200000 for o3', () => {
    assert.equal(getContextWindow('o3'), 200000);
  });

  it('returns 200000 for o4-mini', () => {
    assert.equal(getContextWindow('o4-mini'), 200000);
  });

  it('returns 1048576 for gemini-2.0-flash', () => {
    assert.equal(getContextWindow('gemini-2.0-flash'), 1048576);
  });

  it('returns 1048576 for gemini-2.5-pro', () => {
    assert.equal(getContextWindow('gemini-2.5-pro'), 1048576);
  });

  it('returns 1048576 for gemini-2.5-flash', () => {
    assert.equal(getContextWindow('gemini-2.5-flash'), 1048576);
  });

  it('returns 2097152 for gemini-3', () => {
    assert.equal(getContextWindow('gemini-3'), 2097152);
  });

  it('returns 128000 for gpt-4o-mini', () => {
    assert.equal(getContextWindow('gpt-4o-mini'), 128000);
  });

  it('returns 1048576 for gemini-1.5-pro', () => {
    assert.equal(getContextWindow('gemini-1.5-pro'), 1048576);
  });

  it('returns 128000 for o1-mini (exact match)', () => {
    assert.equal(getContextWindow('o1-mini'), 128000);
  });

  it('returns 200000 for o3-mini', () => {
    assert.equal(getContextWindow('o3-mini'), 200000);
  });

  it('matches by prefix (e.g. gpt-4o-2024-08-06)', () => {
    assert.equal(getContextWindow('gpt-4o-2024-08-06'), 128000);
  });

  it('returns default 128000 for unknown models', () => {
    assert.equal(getContextWindow('unknown-model-xyz'), 128000);
  });
});

// ---------------------------------------------------------------------------
// getModelCapabilities
// ---------------------------------------------------------------------------
describe('getModelCapabilities', () => {
  it('returns capabilities for claude-opus-4', () => {
    const caps = getModelCapabilities('claude-opus-4');
    assert.notEqual(caps, null);
    assert.equal(caps!.reasoning, true);
    assert.equal(caps!.supportsAdaptiveThinking, true);
    assert.equal(caps!.requiresTemperatureOne, true);
    assert.equal(caps!.temperatureMustBeUnset, false);
    assert.equal(caps!.maxThinkingBudget, 128000);
    assert.equal(caps!.defaultThinkingLevel, 'medium');
  });

  it('returns capabilities for claude-sonnet-4', () => {
    const caps = getModelCapabilities('claude-sonnet-4');
    assert.notEqual(caps, null);
    assert.equal(caps!.reasoning, true);
    assert.equal(caps!.supportsAdaptiveThinking, true);
    assert.equal(caps!.requiresTemperatureOne, true);
  });

  it('returns capabilities for o1 (OpenAI reasoning)', () => {
    const caps = getModelCapabilities('o1');
    assert.notEqual(caps, null);
    assert.equal(caps!.reasoning, true);
    assert.equal(caps!.temperatureMustBeUnset, true);
    assert.equal(caps!.requiresTemperatureOne, false);
    assert.equal(caps!.maxThinkingBudget, 0);
  });

  it('returns capabilities for o1-preview (prefix match)', () => {
    const caps = getModelCapabilities('o1-preview');
    assert.notEqual(caps, null);
    assert.equal(caps!.temperatureMustBeUnset, true);
  });

  it('returns capabilities for o3', () => {
    const caps = getModelCapabilities('o3');
    assert.notEqual(caps, null);
    assert.equal(caps!.reasoning, true);
    assert.equal(caps!.temperatureMustBeUnset, true);
  });

  it('returns capabilities for o3-mini', () => {
    const caps = getModelCapabilities('o3-mini');
    assert.notEqual(caps, null);
    assert.equal(caps!.temperatureMustBeUnset, true);
  });

  it('returns capabilities for o4-mini', () => {
    const caps = getModelCapabilities('o4-mini');
    assert.notEqual(caps, null);
    assert.equal(caps!.reasoning, true);
    assert.equal(caps!.temperatureMustBeUnset, true);
  });

  it('returns capabilities for gemini-2.5-pro', () => {
    const caps = getModelCapabilities('gemini-2.5-pro');
    assert.notEqual(caps, null);
    assert.equal(caps!.reasoning, true);
    assert.equal(caps!.defaultThinkingLevel, 'medium');
    assert.equal(caps!.maxThinkingBudget, 32768);
  });

  it('returns capabilities for gemini-2.5-flash', () => {
    const caps = getModelCapabilities('gemini-2.5-flash');
    assert.notEqual(caps, null);
    assert.equal(caps!.reasoning, true);
    assert.equal(caps!.defaultThinkingLevel, 'low');
    assert.equal(caps!.maxThinkingBudget, 32768);
  });

  it('returns capabilities for deepseek-r1', () => {
    const caps = getModelCapabilities('deepseek-r1');
    assert.notEqual(caps, null);
    assert.equal(caps!.reasoning, true);
    assert.equal(caps!.defaultThinkingLevel, 'high');
    assert.equal(caps!.maxThinkingBudget, 0);
  });

  it('returns capabilities for deepseek-reasoner', () => {
    const caps = getModelCapabilities('deepseek-reasoner');
    assert.notEqual(caps, null);
    assert.equal(caps!.reasoning, true);
  });

  it('returns null for non-reasoning models', () => {
    assert.equal(getModelCapabilities('gpt-4o'), null);
    assert.equal(getModelCapabilities('gpt-4-turbo'), null);
    assert.equal(getModelCapabilities('gemini-2.0-flash'), null);
    assert.equal(getModelCapabilities('unknown-model'), null);
  });
});

// ---------------------------------------------------------------------------
// isReasoningModel
// ---------------------------------------------------------------------------
describe('isReasoningModel', () => {
  it('returns true for reasoning models', () => {
    assert.equal(isReasoningModel('claude-opus-4'), true);
    assert.equal(isReasoningModel('claude-sonnet-4'), true);
    assert.equal(isReasoningModel('o1'), true);
    assert.equal(isReasoningModel('o3'), true);
    assert.equal(isReasoningModel('o4-mini'), true);
    assert.equal(isReasoningModel('gemini-2.5-pro'), true);
    assert.equal(isReasoningModel('gemini-2.5-flash'), true);
    assert.equal(isReasoningModel('deepseek-r1'), true);
  });

  it('returns false for non-reasoning models', () => {
    assert.equal(isReasoningModel('gpt-4o'), false);
    assert.equal(isReasoningModel('gpt-4-turbo'), false);
    assert.equal(isReasoningModel('gemini-2.0-flash'), false);
    assert.equal(isReasoningModel('llama-3'), false);
  });
});

// ---------------------------------------------------------------------------
// resolveThinkingBudget
// ---------------------------------------------------------------------------
describe('resolveThinkingBudget', () => {
  it('returns 0 for off', () => {
    assert.equal(resolveThinkingBudget('off'), 0);
  });

  it('returns 4096 for low', () => {
    assert.equal(resolveThinkingBudget('low'), 4096);
  });

  it('returns 10240 for medium', () => {
    assert.equal(resolveThinkingBudget('medium'), 10240);
  });

  it('returns 32768 for high', () => {
    assert.equal(resolveThinkingBudget('high'), 32768);
  });

  it('returns 128000 for max', () => {
    assert.equal(resolveThinkingBudget('max'), 128000);
  });
});

// ---------------------------------------------------------------------------
// getProviderSamplingDefaults
// ---------------------------------------------------------------------------
describe('getProviderSamplingDefaults', () => {
  it('returns gemini defaults for gemini models', () => {
    const defaults = getProviderSamplingDefaults('gemini-2.5-pro');
    assert.equal(defaults.temperature, 1.0);
    assert.equal(defaults.topK, 64);
  });

  it('returns qwen defaults for qwen models', () => {
    const defaults = getProviderSamplingDefaults('qwen-72b-chat');
    assert.equal(defaults.temperature, 0.55);
    assert.equal(defaults.topP, 1);
  });

  it('returns deepseek defaults for deepseek models', () => {
    const defaults = getProviderSamplingDefaults('deepseek-coder-v2');
    assert.equal(defaults.temperature, 0.6);
  });

  it('returns empty object for unknown models', () => {
    const defaults = getProviderSamplingDefaults('gpt-4o');
    assert.deepEqual(defaults, {});
  });

  it('is case-insensitive', () => {
    const defaults = getProviderSamplingDefaults('Gemini-2.5-Pro');
    assert.equal(defaults.temperature, 1.0);
  });
});

// ---------------------------------------------------------------------------
// resolveTemperature
// ---------------------------------------------------------------------------
describe('resolveTemperature', () => {
  // --- thinking NOT enabled ---
  describe('thinking not enabled', () => {
    it('returns configTemperature when provided', () => {
      assert.equal(resolveTemperature('gpt-4o', 0.7, undefined), 0.7);
    });

    it('returns configTemperature when thinking is { enabled: false }', () => {
      assert.equal(resolveTemperature('gpt-4o', 0.5, { enabled: false }), 0.5);
    });

    it('returns provider default when no configTemperature and no thinking', () => {
      assert.equal(resolveTemperature('gemini-2.5-pro', undefined, undefined), 1.0);
    });

    it('returns undefined when no configTemperature and unknown model', () => {
      assert.equal(resolveTemperature('gpt-4o', undefined, undefined), undefined);
    });
  });

  // --- thinking enabled ---
  describe('thinking enabled', () => {
    it('returns undefined for Claude models (requiresTemperatureOne)', () => {
      assert.equal(resolveTemperature('claude-opus-4', 0.5, { enabled: true }), undefined);
    });

    it('returns undefined for o-series models (temperatureMustBeUnset)', () => {
      assert.equal(resolveTemperature('o1', 0.5, { enabled: true }), undefined);
      assert.equal(resolveTemperature('o3', 0.5, { enabled: true }), undefined);
      assert.equal(resolveTemperature('o4-mini', 0.5, { enabled: true }), undefined);
    });

    it('returns configTemperature for gemini thinking models', () => {
      assert.equal(resolveTemperature('gemini-2.5-pro', 0.8, { enabled: true }), 0.8);
    });

    it('returns configTemperature for deepseek reasoning models', () => {
      assert.equal(resolveTemperature('deepseek-r1', 0.6, { enabled: true }), 0.6);
    });

    it('returns configTemperature for unknown model with thinking enabled', () => {
      assert.equal(resolveTemperature('random-model', 0.3, { enabled: true }), 0.3);
    });

    it('returns provider default for unknown model when configTemperature is undefined', () => {
      // "deepseek-coder" is not a reasoning model but matches deepseek defaults
      assert.equal(resolveTemperature('deepseek-coder', undefined, { enabled: true }), 0.6);
    });

    it('returns undefined for truly unknown model with thinking enabled and no config temp', () => {
      assert.equal(resolveTemperature('totally-unknown', undefined, { enabled: true }), undefined);
    });
  });
});
