/**
 * U051 — user env-var capability override for pinned third-party / gateway models.
 *
 * Mirrors Claude's get3PModelCapabilityOverride
 * (references/claude/claude-code-main/src/utils/model/modelSupportOverrides.ts:L4-L50):
 *  - ignored on a first-party endpoint (no custom *_BASE_URL set);
 *  - a tier with no pin OR an unset capability list → undefined (falls through);
 *  - an EMPTY capability list ('') → false ("supports nothing"), distinct from unset;
 *  - tiers tried opus→sonnet→haiku, model id matched case-insensitively;
 *  - comma-split, trimmed, lowercased membership test.
 *
 * Also covers the end-to-end wiring into getModelCapabilities: a declared override
 * corrects a misdetected gateway model's reasoning / adaptive-thinking capability.
 *
 * Run: cd core/core && node --import tsx --test test/model-capabilities-3p-override.test.ts
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  get3PModelCapabilityOverride,
  isFirstPartyProvider,
  getModelCapabilities,
} from '../src/model-capabilities';

const OVERRIDE_ENV_KEYS = [
  'ANTHROPIC_BASE_URL',
  'OPENAI_BASE_URL',
  'AI_BASE_URL',
  'PROVIDER_BASE_URL',
  'OLLAMA_BASE_URL',
  'ANTHROPIC_DEFAULT_OPUS_MODEL',
  'ANTHROPIC_DEFAULT_OPUS_MODEL_SUPPORTED_CAPABILITIES',
  'ANTHROPIC_DEFAULT_SONNET_MODEL',
  'ANTHROPIC_DEFAULT_SONNET_MODEL_SUPPORTED_CAPABILITIES',
  'ANTHROPIC_DEFAULT_HAIKU_MODEL',
  'ANTHROPIC_DEFAULT_HAIKU_MODEL_SUPPORTED_CAPABILITIES',
];

function clearEnv(): void {
  for (const key of OVERRIDE_ENV_KEYS) delete process.env[key];
}

function thirdParty(): void {
  // Any custom base URL marks a gateway/proxy (non-first-party) endpoint.
  process.env.ANTHROPIC_BASE_URL = 'https://gateway.example.com/v1';
}

describe('isFirstPartyProvider', () => {
  afterEach(clearEnv);

  it('is first-party when no custom base URL is set', () => {
    clearEnv();
    assert.equal(isFirstPartyProvider(), true);
  });

  it('is not first-party when a custom base URL is set', () => {
    clearEnv();
    thirdParty();
    assert.equal(isFirstPartyProvider(), false);
  });

  it('treats a whitespace-only base URL as first-party', () => {
    clearEnv();
    process.env.OPENAI_BASE_URL = '   ';
    assert.equal(isFirstPartyProvider(), true);
  });

  it('accepts an injected env object', () => {
    assert.equal(isFirstPartyProvider({ PROVIDER_BASE_URL: 'http://x' } as NodeJS.ProcessEnv), false);
    assert.equal(isFirstPartyProvider({} as NodeJS.ProcessEnv), true);
  });
});

describe('get3PModelCapabilityOverride', () => {
  afterEach(clearEnv);

  it('returns undefined on a first-party endpoint even when pins are set', () => {
    clearEnv();
    process.env.ANTHROPIC_DEFAULT_OPUS_MODEL = 'my-gw-opus';
    process.env.ANTHROPIC_DEFAULT_OPUS_MODEL_SUPPORTED_CAPABILITIES = 'effort,thinking';
    assert.equal(get3PModelCapabilityOverride('my-gw-opus', 'effort'), undefined);
  });

  it('returns true when the capability is in the declared list', () => {
    clearEnv();
    thirdParty();
    process.env.ANTHROPIC_DEFAULT_OPUS_MODEL = 'my-gw-opus';
    process.env.ANTHROPIC_DEFAULT_OPUS_MODEL_SUPPORTED_CAPABILITIES = 'effort, thinking ,adaptive_thinking';
    assert.equal(get3PModelCapabilityOverride('my-gw-opus', 'effort'), true);
    assert.equal(get3PModelCapabilityOverride('my-gw-opus', 'thinking'), true);
    assert.equal(get3PModelCapabilityOverride('my-gw-opus', 'adaptive_thinking'), true);
  });

  it('returns false for a capability absent from a declared non-empty list', () => {
    clearEnv();
    thirdParty();
    process.env.ANTHROPIC_DEFAULT_SONNET_MODEL = 'gw-sonnet';
    process.env.ANTHROPIC_DEFAULT_SONNET_MODEL_SUPPORTED_CAPABILITIES = 'effort';
    assert.equal(get3PModelCapabilityOverride('gw-sonnet', 'max_effort'), false);
    assert.equal(get3PModelCapabilityOverride('gw-sonnet', 'adaptive_thinking'), false);
    assert.equal(get3PModelCapabilityOverride('gw-sonnet', 'interleaved_thinking'), false);
  });

  it('distinguishes an empty list ("supports nothing") from unset', () => {
    clearEnv();
    thirdParty();
    // Pinned with an EMPTY list -> explicit "supports nothing" (false), not undefined.
    process.env.ANTHROPIC_DEFAULT_HAIKU_MODEL = 'gw-haiku';
    process.env.ANTHROPIC_DEFAULT_HAIKU_MODEL_SUPPORTED_CAPABILITIES = '';
    assert.equal(get3PModelCapabilityOverride('gw-haiku', 'effort'), false);
  });

  it('returns undefined when the tier is pinned but the capability list is unset', () => {
    clearEnv();
    thirdParty();
    process.env.ANTHROPIC_DEFAULT_OPUS_MODEL = 'gw-opus';
    // No *_SUPPORTED_CAPABILITIES env -> undefined (fall through to auto-detection).
    assert.equal(get3PModelCapabilityOverride('gw-opus', 'effort'), undefined);
  });

  it('returns undefined when the model matches no pin', () => {
    clearEnv();
    thirdParty();
    process.env.ANTHROPIC_DEFAULT_OPUS_MODEL = 'gw-opus';
    process.env.ANTHROPIC_DEFAULT_OPUS_MODEL_SUPPORTED_CAPABILITIES = 'effort';
    assert.equal(get3PModelCapabilityOverride('some-other-model', 'effort'), undefined);
  });

  it('matches the model id case-insensitively', () => {
    clearEnv();
    thirdParty();
    process.env.ANTHROPIC_DEFAULT_OPUS_MODEL = 'My-GW-Opus';
    process.env.ANTHROPIC_DEFAULT_OPUS_MODEL_SUPPORTED_CAPABILITIES = 'EFFORT';
    assert.equal(get3PModelCapabilityOverride('my-gw-opus', 'effort'), true);
  });

  it('tries tiers opus→sonnet→haiku', () => {
    clearEnv();
    thirdParty();
    process.env.ANTHROPIC_DEFAULT_HAIKU_MODEL = 'gw-shared';
    process.env.ANTHROPIC_DEFAULT_HAIKU_MODEL_SUPPORTED_CAPABILITIES = 'thinking';
    assert.equal(get3PModelCapabilityOverride('gw-shared', 'thinking'), true);
  });

  it('returns undefined for an empty / non-string model id', () => {
    clearEnv();
    thirdParty();
    assert.equal(get3PModelCapabilityOverride('', 'effort'), undefined);
    assert.equal(get3PModelCapabilityOverride(undefined as unknown as string, 'effort'), undefined);
  });

  it('accepts an injected env object', () => {
    const env = {
      ANTHROPIC_BASE_URL: 'http://gw',
      ANTHROPIC_DEFAULT_OPUS_MODEL: 'gw-opus',
      ANTHROPIC_DEFAULT_OPUS_MODEL_SUPPORTED_CAPABILITIES: 'effort',
    } as NodeJS.ProcessEnv;
    assert.equal(get3PModelCapabilityOverride('gw-opus', 'effort', env), true);
    assert.equal(get3PModelCapabilityOverride('gw-opus', 'thinking', env), false);
  });
});

describe('getModelCapabilities — env override wiring', () => {
  afterEach(clearEnv);

  it('leaves auto-detection untouched when no override applies', () => {
    clearEnv();
    const caps = getModelCapabilities('claude-opus-5-5');
    assert.ok(caps);
    assert.equal(caps.reasoning, true);
  });

  it('synthesizes reasoning capabilities for a misdetected gateway model', () => {
    clearEnv();
    thirdParty();
    // A custom id that matches no pattern table -> normally null.
    assert.equal(getModelCapabilities('acme-ultra-7'), null);
    process.env.ANTHROPIC_DEFAULT_OPUS_MODEL = 'acme-ultra-7';
    process.env.ANTHROPIC_DEFAULT_OPUS_MODEL_SUPPORTED_CAPABILITIES = 'effort,thinking,adaptive_thinking';
    const caps = getModelCapabilities('acme-ultra-7');
    assert.ok(caps, 'override should synthesize capabilities');
    assert.equal(caps.reasoning, true);
    assert.equal(caps.supportsAdaptiveThinking, true);
  });

  it('forces reasoning OFF for a model declared to support nothing', () => {
    clearEnv();
    thirdParty();
    // This id WOULD auto-detect as a reasoning model via the gpt-5+ pattern...
    assert.ok(getModelCapabilities('gpt-7-turbo'));
    // ...but the user pins it and declares an empty capability list.
    process.env.ANTHROPIC_DEFAULT_SONNET_MODEL = 'gpt-7-turbo';
    process.env.ANTHROPIC_DEFAULT_SONNET_MODEL_SUPPORTED_CAPABILITIES = '';
    assert.equal(getModelCapabilities('gpt-7-turbo'), null);
  });

  it('corrects adaptive-thinking downward when the user declares it unsupported', () => {
    clearEnv();
    thirdParty();
    // Auto-detects adaptive=true (opus 5.x).
    assert.equal(getModelCapabilities('claude-opus-5-5')?.supportsAdaptiveThinking, true);
    process.env.ANTHROPIC_DEFAULT_OPUS_MODEL = 'claude-opus-5-5';
    process.env.ANTHROPIC_DEFAULT_OPUS_MODEL_SUPPORTED_CAPABILITIES = 'effort,thinking';
    const caps = getModelCapabilities('claude-opus-5-5');
    assert.ok(caps);
    assert.equal(caps.reasoning, true);
    assert.equal(caps.supportsAdaptiveThinking, false);
  });
});
