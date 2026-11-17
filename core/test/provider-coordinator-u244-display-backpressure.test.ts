import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  ProviderCoordinator,
  AUTO_DETECTED_PROVIDER_DISPLAY,
  UNIDENTIFIED_MODEL_DISPLAY,
  UNKNOWN_IDENTIFIER,
} from '../src/provider-coordinator';
import type { ProviderConfig, ProviderSubsystem } from '../src/types';
import { getModelDisplayName } from '../src/model-registry';

/** Run `fn` with every provider API-key env var cleared, so provider inference
 *  cannot latch onto an ambient key in the test host and must fall through to the
 *  unknown sentinel. Restores the prior environment afterwards. */
// Every env var inferProviderFromEnv() consults (ENV_KEYS in src/provider.ts).
const PROVIDER_ENV_VARS = [
  'OPENAI_API_KEY', 'ANTHROPIC_API_KEY', 'GOOGLE_API_KEY', 'GROQ_API_KEY',
  'MISTRAL_API_KEY', 'XAI_API_KEY', 'DEEPSEEK_API_KEY', 'TOGETHER_AI_API_KEY',
  'COHERE_API_KEY', 'FIREWORKS_API_KEY', 'PERPLEXITY_API_KEY', 'DEEPINFRA_API_KEY',
  'CEREBRAS_API_KEY', 'ALIBABA_API_KEY', 'DASHSCOPE_API_KEY', 'LUMA_API_KEY',
  'OPENROUTER_API_KEY', 'HF_TOKEN', 'NVIDIA_API_KEY', 'AZURE_OPENAI_API_KEY',
  'AWS_ACCESS_KEY_ID', 'ANTHROPIC_VERTEX_PROJECT_ID', 'GOOGLE_APPLICATION_CREDENTIALS',
  'AZURE_AI_FOUNDRY_API_KEY',
];

function withNoProviderEnv<T>(fn: () => T): T {
  const saved: Record<string, string | undefined> = {};
  for (const key of PROVIDER_ENV_VARS) {
    saved[key] = process.env[key];
    delete process.env[key];
  }
  try {
    return fn();
  } finally {
    for (const [key, val] of Object.entries(saved)) {
      if (val === undefined) delete process.env[key];
      else process.env[key] = val;
    }
  }
}

function makeSubsystem(overrides?: Partial<ProviderSubsystem>, primaryOverrides?: Partial<ProviderConfig>): ProviderSubsystem {
  const primary: ProviderConfig = {
    baseUrl: 'https://api.anthropic.com',
    apiKey: 'sk-ant-primary',
    model: 'claude-sonnet-4-6',
    ...primaryOverrides,
  };
  return { primary, ...overrides };
}

// ---------------------------------------------------------------------------
// U244 gap: "Human-friendly model/provider names on introspected status surfaces"
// getState() must expose a friendly currentModelDisplay (via the canonical
// getModelDisplayName registry helper) and a clearer provider label than the
// bare 'unknown' sentinel.
// ---------------------------------------------------------------------------
describe('ProviderCoordinator — friendly display names on getState()', () => {
  it('exposes a friendly model display name via the shared registry helper', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    const st = coord.getState();
    assert.equal(st.currentModel, 'claude-sonnet-4-6', 'raw id preserved');
    assert.equal(st.currentModelDisplay, 'Sonnet 4.6', 'friendly name rendered');
    // Consolidated with the canonical helper — not a parallel table.
    assert.equal(st.currentModelDisplay, getModelDisplayName('claude-sonnet-4-6'));
  });

  it('maps the unknown model sentinel to a clear phrase, not the bare literal', () => {
    // No model, no inferable provider from key/baseurl/env -> unknown sentinel.
    const st = withNoProviderEnv(() => new ProviderCoordinator(makeSubsystem(undefined, {
      model: '',
      apiKey: 'x',
      baseUrl: 'https://example.invalid',
    })).getState());
    assert.equal(st.currentModel, UNKNOWN_IDENTIFIER, 'raw sentinel still stored');
    assert.equal(st.currentModelDisplay, UNIDENTIFIED_MODEL_DISPLAY);
    assert.notEqual(st.currentModelDisplay, UNKNOWN_IDENTIFIER);
  });

  it('maps the unknown provider sentinel to an auto-detected phrase', () => {
    const st = withNoProviderEnv(() => new ProviderCoordinator(makeSubsystem(undefined, {
      model: 'my-custom-model',
      apiKey: 'x',
      baseUrl: 'https://example.invalid',
    })).getState());
    assert.equal(st.currentProvider, UNKNOWN_IDENTIFIER, 'raw sentinel still stored');
    assert.equal(st.currentProviderDisplay, AUTO_DETECTED_PROVIDER_DISPLAY);
    // An unknown/custom model id falls back to the raw id (registry contract),
    // never the bare provider sentinel.
    assert.equal(st.currentModelDisplay, 'my-custom-model');
  });

  it('surfaces the detected provider slug verbatim when known', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    const st = coord.getState();
    assert.equal(st.currentProvider, 'anthropic');
    assert.equal(st.currentProviderDisplay, 'anthropic');
  });

  it('refreshes the display fields after a primary-config swap', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    coord.setPrimaryConfig({
      baseUrl: 'https://api.openai.com/v1',
      apiKey: 'sk-openai',
      model: 'gpt-4o',
      provider: 'openai',
    });
    const st = coord.getState();
    assert.equal(st.currentModelDisplay, 'GPT-4o');
    assert.equal(st.currentProviderDisplay, 'openai');
  });
});

// ---------------------------------------------------------------------------
// U244 gap: "Backpressure when all credentials are exhausted"
// getConfig() silently returns the throttled primary key when the pool is dry.
// getBackpressure() exposes that boundary so the engine can wait for the reset.
// ---------------------------------------------------------------------------
describe('ProviderCoordinator — credential-pool backpressure', () => {
  it('reports no backpressure when there is no pool (single credential)', () => {
    const coord = new ProviderCoordinator(makeSubsystem());
    const bp = coord.getBackpressure();
    assert.equal(bp.exhausted, false);
    assert.equal(bp.nextAvailableAt, null);
  });

  it('is not exhausted while a credential is available', () => {
    const coord = new ProviderCoordinator(makeSubsystem(), ['sk-ant-two']);
    const bp = coord.getBackpressure();
    assert.equal(bp.exhausted, false);
    // A key is available now -> nextAvailableAt is ~now (not null, not future).
    assert.ok(typeof bp.nextAvailableAt === 'number');
    assert.ok(bp.nextAvailableAt! <= Date.now());
  });

  it('flips exhausted=true and reports a future reset once every key is cooled down', () => {
    const coord = new ProviderCoordinator(makeSubsystem(), ['sk-ant-two']);
    // Exhaust both credentials with a long server-directed cooldown.
    coord.getConfig(); // acquire primary
    coord.markCredentialExhausted(60000);
    coord.getConfig(); // acquire the second
    coord.markCredentialExhausted(60000);

    const bp = coord.getBackpressure();
    assert.equal(bp.exhausted, true, 'whole pool is throttled');
    assert.ok(typeof bp.nextAvailableAt === 'number');
    assert.ok(bp.nextAvailableAt! > Date.now(), 'reset is in the future');
    assert.ok(bp.nextAvailableAt! <= Date.now() + 60000 + 10, 'bounded by the cooldown');
    // availableCredentials and getBackpressure must agree.
    assert.equal(coord.availableCredentials, 0);
  });

  it('recovers (exhausted=false) after the cooldown elapses', () => {
    const coord = new ProviderCoordinator(makeSubsystem(), ['sk-ant-two']);
    coord.getConfig();
    coord.markCredentialExhausted(0); // already elapsed
    coord.getConfig();
    coord.markCredentialExhausted(0);
    const bp = coord.getBackpressure();
    assert.equal(bp.exhausted, false);
    assert.ok(bp.nextAvailableAt! <= Date.now());
  });
});
