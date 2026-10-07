import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  getContextWindow,
  getModelCapabilities,
  isReasoningModel,
  resolveThinkingBudget,
  resolveTemperature,
  getProviderSamplingDefaults,
  fitOutputTokens,
  capThinkingBudget,
  maxToolOutputChars,
  registerModelCapabilities,
  clearDynamicCapabilities,
  discoverFromModelList,
  DEFAULT_MAX_OUTPUT_TOKENS,
  OUTPUT_FIT_SAFETY_TOKENS,
  MIN_FITTED_OUTPUT_TOKENS,
  MIN_THINKING_BUDGET,
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

  it('returns 262144 for Qwen3.8 SWE-bench eval ids', () => {
    assert.equal(getContextWindow('Qwen/Qwen3.8-27B'), 262144);
    assert.equal(getContextWindow('Qwen3.8-27B'), 262144);
    assert.equal(getContextWindow('Qwen/Qwen3-30B'), 262144);
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

  it('returns null for GPT-6 family (chat models, no reasoning_effort)', () => {
    assert.equal(getModelCapabilities('gpt-6-luna'), null);
    assert.equal(getModelCapabilities('gpt-6-sol'), null);
    assert.equal(getModelCapabilities('gpt-6-astra'), null);
    assert.equal(getModelCapabilities('gpt-6.1-sol'), null);
    assert.equal(getModelCapabilities('gpt-5.6-luna'), null);
    assert.equal(getModelCapabilities('gpt-5.5'), null);
    assert.equal(getModelCapabilities('gpt-5.4'), null);
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

  it('returns false for GPT-6 family', () => {
    assert.equal(isReasoningModel('gpt-6-luna'), false);
    assert.equal(isReasoningModel('gpt-6-sol'), false);
    assert.equal(isReasoningModel('gpt-6-astra'), false);
    assert.equal(isReasoningModel('gpt-6.1-sol'), false);
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

  it('returns model-aware budget for max without modelId', () => {
    const budget = resolveThinkingBudget('max');
    assert.equal(budget, 128000 - 8192);
  });

  it('returns model-aware budget for max with Claude Opus 4', () => {
    const budget = resolveThinkingBudget('max', 'claude-opus-4-6');
    assert.equal(budget, 128000 - 8192);
  });

  it('leaves room for response tokens when max', () => {
    const budget = resolveThinkingBudget('max', 'claude-opus-4-6');
    assert.ok(budget < 128000, 'budget must be less than maxOutputTokens (128000)');
    assert.ok(budget >= 1024, 'budget must be at least 1024');
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

describe('output, thinking, and tool-char budgets agree', () => {
  const windows = [32_768, 128_000, 200_000, 262_144, 1_048_576];

  it('default output is 32768 and fits beside a short prompt on every listed window', () => {
    assert.equal(DEFAULT_MAX_OUTPUT_TOKENS, 32_768);
    for (const window of windows) {
      const output = fitOutputTokens(DEFAULT_MAX_OUTPUT_TOKENS, window, 2_000);
      assert.ok(output <= DEFAULT_MAX_OUTPUT_TOKENS, `${window}: output grew past the default`);
      assert.ok(2_000 + output + OUTPUT_FIT_SAFETY_TOKENS <= window, `${window}: output does not fit`);
      if (window >= 40_000) {
        assert.equal(output, 32_768, `${window}: a roomy window must keep the full default`);
      } else {
        assert.ok(output < 32_768, `${window}: a 32k window must not be asked for 32768 output`);
        assert.ok(output > 8_192, `${window}: fell back to the old 8192 clamp`);
      }
    }
  });

  it('does not send a 256k escalation step on a 128k window', () => {
    const output = fitOutputTokens(262_144, 128_000, 20_000);
    assert.ok(output < 262_144, `raw escalation leaked: ${output}`);
    assert.equal(output, 128_000 - 20_000 - OUTPUT_FIT_SAFETY_TOKENS);
    assert.equal(20_000 + output + OUTPUT_FIT_SAFETY_TOKENS, 128_000);
  });

  it('keeps the 65536 escalation step when the prompt is small on a 128k window', () => {
    assert.equal(fitOutputTokens(65_536, 128_000, 4_000), 65_536);
  });

  it('high thinking plus default output stays under each window', () => {
    for (const window of windows) {
      const prompt = Math.floor(window * 0.5);
      const output = fitOutputTokens(DEFAULT_MAX_OUTPUT_TOKENS, window, prompt);
      const thinking = capThinkingBudget(resolveThinkingBudget('high'), window, prompt, output);
      assert.ok(
        prompt + output + thinking + OUTPUT_FIT_SAFETY_TOKENS <= window,
        `${window}: prompt ${prompt} + output ${output} + thinking ${thinking} overflows`,
      );
    }
  });

  it('max thinking on Claude does not consume the safety margin', () => {
    const window = getContextWindow('claude-opus-4');
    const prompt = 150_000;
    const output = fitOutputTokens(DEFAULT_MAX_OUTPUT_TOKENS, window, prompt);
    const thinking = capThinkingBudget(resolveThinkingBudget('max', 'claude-opus-4'), window, prompt, output);
    assert.ok(thinking > 1_024);
    assert.ok(thinking < resolveThinkingBudget('max', 'claude-opus-4'));
    assert.ok(prompt + output + thinking + OUTPUT_FIT_SAFETY_TOKENS <= window);
  });

  it('turns thinking off when the window is already full', () => {
    const thinking = capThinkingBudget(32_768, 32_768, 30_000, 4_000);
    assert.equal(thinking, 0);
  });

  it('uses the defaults when a budget is missing or the window cannot hold a completion', () => {
    assert.equal(fitOutputTokens(0, 128_000, 1_000), DEFAULT_MAX_OUTPUT_TOKENS);
    assert.equal(fitOutputTokens(Number.NaN, 128_000, 1_000), DEFAULT_MAX_OUTPUT_TOKENS);
    assert.equal(fitOutputTokens(-5, 128_000, 1_000), DEFAULT_MAX_OUTPUT_TOKENS);
    assert.equal(fitOutputTokens(4_000, 0, 1_000), 4_000);
    assert.equal(fitOutputTokens(4_000, Number.NaN, 1_000), 4_000);
    assert.equal(fitOutputTokens(4_000, -1, 1_000), 4_000);
    assert.equal(fitOutputTokens(4_000, 8_000, Number.NaN), 4_000);
    assert.equal(fitOutputTokens(8_000, 2_000, 1_500), MIN_FITTED_OUTPUT_TOKENS);

    assert.equal(capThinkingBudget(0, 128_000, 1_000, 1_000), 0);
    assert.equal(capThinkingBudget(Number.NaN, 128_000, 1_000, 1_000), 0);
    assert.equal(capThinkingBudget(-1, 128_000, 1_000, 1_000), 0);
    assert.equal(capThinkingBudget(5_000.9, 0, 1_000, 1_000), 5_000);
    assert.equal(capThinkingBudget(5_000, Number.NaN, 1_000, 1_000), 5_000);
    assert.equal(capThinkingBudget(8_000, 10_000, Number.NaN, Number.NaN), 8_000);
    assert.equal(capThinkingBudget(8_000, 10_000, -5, -5), 8_000);
    // usable is 2000: below the margin, still above the 1024 floor
    assert.equal(capThinkingBudget(8_000, 10_000, 5_000, 1_976), MIN_THINKING_BUDGET);

    assert.equal(maxToolOutputChars(0), 8_000);
    assert.equal(maxToolOutputChars(-10), 8_000);
    assert.equal(maxToolOutputChars(Number.NaN), 8_000);
  });

  it('sizes one tool result from the token budget, not a fixed 8k', () => {
    const wide = maxToolOutputChars(Math.floor(200_000 * 0.8));
    const narrow = maxToolOutputChars(Math.floor(32_768 * 0.8));
    assert.ok(wide >= 400_000, `200k window should still hold a 400k file_read, got ${wide}`);
    assert.ok(narrow < 80_000, `32k window must shrink tool output, got ${narrow}`);
    assert.ok(narrow > 8_000, 'must not collapse back to an 8k tool cap');
    assert.ok(Math.ceil(narrow / 3) <= Math.floor(32_768 * 0.8));
  });
});

// ---------------------------------------------------------------------------
// GPT-6 family context windows
// ---------------------------------------------------------------------------
describe('GPT-6 family context windows', () => {
  it('returns 1050000 for gpt-6-luna', () => {
    assert.equal(getContextWindow('gpt-6-luna'), 1_050_000);
  });

  it('returns 1050000 for gpt-6-sol', () => {
    assert.equal(getContextWindow('gpt-6-sol'), 1_050_000);
  });

  it('returns 1050000 for gpt-6-astra', () => {
    assert.equal(getContextWindow('gpt-6-astra'), 1_050_000);
  });

  it('returns 1050000 for gpt-6.1-sol', () => {
    assert.equal(getContextWindow('gpt-6.1-sol'), 1_050_000);
  });

  it('returns 1050000 for gpt-6 prefix match', () => {
    assert.equal(getContextWindow('gpt-6-something-new'), 1_050_000);
  });
});

// ---------------------------------------------------------------------------
// Dynamic model capability registry
// ---------------------------------------------------------------------------
describe('dynamic model capability registry', () => {
  afterEach(() => {
    clearDynamicCapabilities();
  });

  it('registerModelCapabilities overrides static for a known model', () => {
    assert.equal(getModelCapabilities('gpt-4o'), null);
    registerModelCapabilities('gpt-4o', {
      reasoning: true, defaultThinkingLevel: 'low',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: false, maxThinkingBudget: 0,
    });
    const caps = getModelCapabilities('gpt-4o');
    assert.notEqual(caps, null);
    assert.equal(caps!.reasoning, true);
  });

  it('registerModelCapabilities with null explicitly marks as non-reasoning', () => {
    registerModelCapabilities('custom-model', null);
    assert.equal(getModelCapabilities('custom-model'), null);
  });

  it('clearDynamicCapabilities restores static behavior', () => {
    registerModelCapabilities('o1', {
      reasoning: false, defaultThinkingLevel: 'off',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: false, maxThinkingBudget: 0,
    });
    assert.equal(getModelCapabilities('o1')!.reasoning, false);
    clearDynamicCapabilities();
    assert.equal(getModelCapabilities('o1')!.reasoning, true);
  });

  it('is case-insensitive', () => {
    registerModelCapabilities('GPT-6-Luna', null);
    assert.equal(getModelCapabilities('gpt-6-luna'), null);
  });
});

// ---------------------------------------------------------------------------
// discoverFromModelList
// ---------------------------------------------------------------------------
describe('discoverFromModelList', () => {
  afterEach(() => {
    clearDynamicCapabilities();
  });

  it('auto-classifies o-series models as reasoning', () => {
    const records = [
      { id: 'o4-mini-2025-04-16', object: 'model' },
      { id: 'o3-2025-04-16', object: 'model' },
    ];
    const result = discoverFromModelList(records);
    assert.ok(result.registered.includes('o4-mini-2025-04-16'));
    assert.ok(result.registered.includes('o3-2025-04-16'));
    const caps = getModelCapabilities('o4-mini-2025-04-16');
    assert.notEqual(caps, null);
    assert.equal(caps!.reasoning, true);
    assert.equal(caps!.temperatureMustBeUnset, true);
  });

  it('auto-classifies gpt-6 models as non-reasoning', () => {
    const records = [
      { id: 'gpt-6-luna', object: 'model' },
      { id: 'gpt-6-sol', object: 'model' },
      { id: 'gpt-6-astra', object: 'model' },
    ];
    const result = discoverFromModelList(records);
    assert.ok(result.registered.length === 3);
    assert.equal(getModelCapabilities('gpt-6-luna'), null);
    assert.equal(getModelCapabilities('gpt-6-sol'), null);
    assert.equal(getModelCapabilities('gpt-6-astra'), null);
    assert.equal(isReasoningModel('gpt-6-luna'), false);
  });

  it('picks up advertised context windows from records', () => {
    const records = [
      { id: 'new-model-x', object: 'model', max_input_tokens: 500_000 },
    ];
    discoverFromModelList(records);
    assert.equal(getContextWindow('new-model-x'), 500_000);
  });

  it('does not re-register already discovered models', () => {
    const records = [{ id: 'gpt-6-luna', object: 'model' }];
    discoverFromModelList(records);
    registerModelCapabilities('gpt-6-luna', {
      reasoning: true, defaultThinkingLevel: 'high',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: false, maxThinkingBudget: 0,
    });
    discoverFromModelList(records);
    assert.equal(getModelCapabilities('gpt-6-luna')!.reasoning, true);
  });

  it('returns registered ids and context windows', () => {
    const records = [
      { id: 'test-model-a', object: 'model', context_length: 262144 },
      { id: 'test-model-b', object: 'model' },
    ];
    const result = discoverFromModelList(records);
    assert.ok(result.registered.includes('test-model-a'));
    assert.ok(result.registered.includes('test-model-b'));
    assert.equal(result.contextWindows['test-model-a'], 262144);
    assert.equal(result.contextWindows['test-model-b'], undefined);
  });

  it('handles empty records gracefully', () => {
    const result = discoverFromModelList([]);
    assert.deepEqual(result.registered, []);
    assert.deepEqual(result.contextWindows, {});
  });

  it('handles records with missing id', () => {
    const result = discoverFromModelList([{ object: 'model' }]);
    assert.deepEqual(result.registered, []);
  });

  it('classifies o1 variant date-stamped IDs as reasoning', () => {
    discoverFromModelList([
      { id: 'o1-2025-12-17' },
      { id: 'o3-mini-2025-01-31' },
      { id: 'o4-mini-2025-04-16' },
    ]);
    assert.equal(isReasoningModel('o1-2025-12-17'), true);
    assert.equal(isReasoningModel('o3-mini-2025-01-31'), true);
    assert.equal(isReasoningModel('o4-mini-2025-04-16'), true);
  });

  it('does not classify non-o-series as reasoning', () => {
    discoverFromModelList([
      { id: 'gpt-4o' },
      { id: 'chatgpt-4o-latest' },
      { id: 'gpt-5.5' },
    ]);
    assert.equal(isReasoningModel('gpt-4o'), false);
    assert.equal(isReasoningModel('chatgpt-4o-latest'), false);
    assert.equal(isReasoningModel('gpt-5.5'), false);
  });

  it('mixed reasoning and non-reasoning models in one call', () => {
    const result = discoverFromModelList([
      { id: 'o3-2025-04-16' },
      { id: 'gpt-6-luna' },
      { id: 'o4-mini-2025-04-16' },
      { id: 'gpt-6-sol' },
    ]);
    assert.equal(result.registered.length, 4);
    assert.equal(isReasoningModel('o3-2025-04-16'), true);
    assert.equal(isReasoningModel('gpt-6-luna'), false);
    assert.equal(isReasoningModel('o4-mini-2025-04-16'), true);
    assert.equal(isReasoningModel('gpt-6-sol'), false);
  });
});

// ---------------------------------------------------------------------------
// Dynamic registry interaction with resolveTemperature
// ---------------------------------------------------------------------------
describe('dynamic registry + resolveTemperature', () => {
  afterEach(() => {
    clearDynamicCapabilities();
  });

  it('resolveTemperature returns undefined for dynamically registered reasoning model with thinking', () => {
    registerModelCapabilities('dynamic-reasoner', {
      reasoning: true, defaultThinkingLevel: 'medium',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: true, maxThinkingBudget: 0,
    });
    assert.equal(resolveTemperature('dynamic-reasoner', 0.5, { enabled: true }), undefined);
  });

  it('resolveTemperature returns configTemperature for dynamically registered non-reasoning model', () => {
    registerModelCapabilities('dynamic-chat', null);
    assert.equal(resolveTemperature('dynamic-chat', 0.7, undefined), 0.7);
  });

  it('resolveTemperature returns undefined for dynamically registered Claude-like model', () => {
    registerModelCapabilities('custom-claude', {
      reasoning: true, defaultThinkingLevel: 'medium',
      supportsAdaptiveThinking: true, requiresTemperatureOne: true,
      temperatureMustBeUnset: false, maxThinkingBudget: 128000,
    });
    assert.equal(resolveTemperature('custom-claude', 0.5, { enabled: true }), undefined);
  });
});

// ---------------------------------------------------------------------------
// Dynamic registry interaction with resolveThinkingBudget
// ---------------------------------------------------------------------------
describe('dynamic registry + resolveThinkingBudget', () => {
  afterEach(() => {
    clearDynamicCapabilities();
  });

  it('resolveThinkingBudget max uses dynamically registered maxThinkingBudget', () => {
    registerModelCapabilities('custom-thinker', {
      reasoning: true, defaultThinkingLevel: 'high',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: false, maxThinkingBudget: 65536,
    });
    const budget = resolveThinkingBudget('max', 'custom-thinker');
    assert.equal(budget, 65536 - 8192);
  });

  it('resolveThinkingBudget max falls back to 128000 for unknown dynamic model', () => {
    registerModelCapabilities('unknown-dynamic', null);
    const budget = resolveThinkingBudget('max', 'unknown-dynamic');
    assert.equal(budget, 128000 - 8192);
  });
});
