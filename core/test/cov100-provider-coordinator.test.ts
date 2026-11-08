import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';

import { ProviderCoordinator } from '../src/provider-coordinator';
import type { ProviderConfig, ProviderSubsystem } from '../src/types';

// ---------------------------------------------------------------------------
// Targeted coverage for ProviderCoordinator.setPrimaryConfig() — the state
// refresh uses `config.model || inferred?.defaultModel || 'unknown'` and
// `inferred?.provider || config.provider || 'unknown'`. The existing suite only
// exercises the inference-succeeds arms (openrouter key) and the explicit-model
// arms. The *terminal* 'unknown' fallbacks (no model, no inferable provider,
// no explicit provider) and the explicit-`config.provider` middle arm are the
// uncovered branches here. These must NOT touch the network or depend on the
// host's provider env vars, so we scrub every key the inference waterfall reads.
// ---------------------------------------------------------------------------

const PROVIDER_ENV_KEYS = [
  'OPENAI_API_KEY', 'ANTHROPIC_API_KEY', 'GOOGLE_API_KEY', 'GEMINI_API_KEY',
  'OPENROUTER_API_KEY', 'GROQ_API_KEY', 'MISTRAL_API_KEY', 'XAI_API_KEY',
  'DEEPSEEK_API_KEY', 'TOGETHER_AI_API_KEY', 'TOGETHER_API_KEY', 'COHERE_API_KEY',
  'FIREWORKS_API_KEY', 'PERPLEXITY_API_KEY', 'DEEPINFRA_API_KEY', 'CEREBRAS_API_KEY',
  'ALIBABA_API_KEY', 'DASHSCOPE_API_KEY', 'LUMA_API_KEY', 'HF_TOKEN',
  'AZURE_OPENAI_API_KEY', 'AWS_ACCESS_KEY_ID', 'ANTHROPIC_VERTEX_PROJECT_ID',
  'GOOGLE_APPLICATION_CREDENTIALS',
];

function makeSubsystem(overrides?: Partial<ProviderSubsystem>): ProviderSubsystem {
  const primary: ProviderConfig = {
    baseUrl: 'https://api.openai.com/v1',
    apiKey: 'sk-test-key',
    model: 'gpt-4o',
  };
  return { primary, ...overrides };
}

describe('ProviderCoordinator.setPrimaryConfig — unknown-inference fallback arms', () => {
  const saved: Record<string, string | undefined> = {};

  before(() => {
    for (const k of PROVIDER_ENV_KEYS) {
      saved[k] = process.env[k];
      delete process.env[k];
    }
  });

  after(() => {
    for (const k of PROVIDER_ENV_KEYS) {
      if (saved[k] !== undefined) process.env[k] = saved[k];
      else delete process.env[k];
    }
  });

  it('falls back to "unknown" for BOTH model and provider when nothing is inferable', () => {
    const coord = new ProviderCoordinator(makeSubsystem({ fallbackModel: 'gpt-3.5-turbo' }));
    // Move onto the fallback first, so we also prove setPrimaryConfig resets fallbackIndex to 0.
    coord.recordError();
    assert.equal(coord.getConfig().model, 'gpt-3.5-turbo');

    // No model, an unrecognizable key, a custom baseUrl, and no explicit provider →
    // inferProvider() returns null, so both state fields must land on the literal 'unknown'.
    coord.setPrimaryConfig({
      baseUrl: 'https://custom.nowhere.test/v1',
      apiKey: 'zzzz-unrecognizable-key-format',
      model: '',
    });

    const state = coord.getState();
    assert.equal(state.currentModel, 'unknown', 'empty model with no inferred default → "unknown"');
    assert.equal(state.currentProvider, 'unknown', 'no inference + no explicit provider → "unknown"');
    // Reset to the new primary: fallbackIndex back to 0, so getConfig returns the new primary.
    assert.equal(coord.getConfig().model, '', 'reset to new primary (fallbackIndex 0)');
    assert.equal(coord.getConfig().baseUrl, 'https://custom.nowhere.test/v1');
  });

  it('uses the explicit config.provider arm when inference fails but provider is set', () => {
    const coord = new ProviderCoordinator(makeSubsystem());

    // Unrecognizable key + custom baseUrl → inference fails → inferred?.provider is undefined,
    // so currentProvider must come from the explicit config.provider (the middle arm).
    coord.setPrimaryConfig({
      baseUrl: 'https://custom.nowhere.test/v1',
      apiKey: 'zzzz-unrecognizable-key-format',
      model: '',
      provider: 'my-private-gateway',
    });

    const state = coord.getState();
    assert.equal(state.currentProvider, 'my-private-gateway',
      'inferred provider absent → explicit config.provider wins over "unknown"');
    assert.equal(state.currentModel, 'unknown',
      'empty model with no inferred default still resolves to "unknown"');
  });

  it('exercises the empty-baseUrl arm of inferProvider via setPrimaryConfig', () => {
    const coord = new ProviderCoordinator(makeSubsystem());

    // Empty baseUrl → the `config.baseUrl ? inferFromBaseUrl : null` ternary takes the null arm;
    // unrecognizable key + cleared env → overall inference null → both fields fall through.
    coord.setPrimaryConfig({
      baseUrl: '',
      apiKey: 'zzzz-unrecognizable-key-format',
      model: 'hand-picked-model',
    });

    const state = coord.getState();
    assert.equal(state.currentModel, 'hand-picked-model', 'explicit model is preserved');
    assert.equal(state.currentProvider, 'unknown', 'empty baseUrl + unknown key → "unknown" provider');
  });
});
