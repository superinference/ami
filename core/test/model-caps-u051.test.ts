// ===========================================================================
// model-caps-u051.test.ts
//
// Unit U051 — model-capabilities parity:
//   - Opus-family 1M-context beta gating (checkOpus1mAccess parity)
//   - Per-model max-output-token range: distinct `default` vs `upperLimit`
//     (Claude getModelMaxOutputTokens ladder + AntModel defaultMaxTokens /
//      upperMaxTokensLimit knobs), with a capability.max_tokens override.
//   - End-to-end: streamChatCompletion sizes maxOutputTokens from the model's
//     DEFAULT budget when the caller omits maxTokens.
//
// ESM-coverage-safe: no source-text assertions; the only seam used is the
// behavior-preserving streamTextImpl injection already present in provider.ts.
// ===========================================================================
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  supportsOneMillionContextBeta,
  getContextWindow,
  getModelMaxOutputTokens,
  getMaxOutputTokens,
  registerModelCapabilities,
  clearDynamicCapabilities,
  discoverFromModelList,
  DEFAULT_MAX_OUTPUT_TOKENS,
  MAX_OUTPUT_TOKENS_UPPER_LIMIT,
} from '../src/model-capabilities';
import { streamChatCompletion } from '../src/provider';
import type { ProviderConfig, Message, StreamChunk } from '../src/types';

// ---------------------------------------------------------------------------
// Opus-family 1M-context beta gating (checkOpus1mAccess parity)
// ---------------------------------------------------------------------------
describe('U051: Opus-family 1M-context beta gating', () => {
  afterEach(() => { delete process.env.SUPERINFERENCE_DISABLE_1M_CONTEXT; });

  it('grants the 1M beta to Opus 5+ (the gated Opus family)', () => {
    assert.equal(supportsOneMillionContextBeta('claude-opus-5'), true);
    assert.equal(supportsOneMillionContextBeta('claude-opus-5-5'), true);
    assert.equal(supportsOneMillionContextBeta('claude-opus-8'), true);
    assert.equal(supportsOneMillionContextBeta('claude-opus-10'), true);
    // Gateway/provider prefix is tolerated (unanchored).
    assert.equal(supportsOneMillionContextBeta('openrouter/anthropic/claude-opus-5-5'), true);
    assert.equal(supportsOneMillionContextBeta('CLAUDE-OPUS-5'), true);
  });

  it('still excludes Opus 4 and earlier (conservative)', () => {
    assert.equal(supportsOneMillionContextBeta('claude-opus-4'), false);
    assert.equal(supportsOneMillionContextBeta('claude-opus-4-6'), false);
    assert.equal(supportsOneMillionContextBeta('claude-opus-3'), false);
    assert.equal(supportsOneMillionContextBeta('claude-3-opus'), false);
  });

  it('keeps the existing Sonnet 4+ gating untouched', () => {
    assert.equal(supportsOneMillionContextBeta('claude-sonnet-4'), true);
    assert.equal(supportsOneMillionContextBeta('claude-sonnet-5'), true);
    assert.equal(supportsOneMillionContextBeta('claude-sonnet-3'), false);
  });

  it('honors the operator kill-switch for the Opus family too', () => {
    process.env.SUPERINFERENCE_DISABLE_1M_CONTEXT = '1';
    assert.equal(supportsOneMillionContextBeta('claude-opus-5'), false);
  });

  it('getContextWindow grants 1M to an Opus 5+ id not in the static table (via beta)', () => {
    // claude-opus-5-8 is not an explicit CONTEXT_WINDOWS key; the beta path lifts
    // it to 1M so compaction timing matches what the endpoint serves.
    assert.equal(getContextWindow('claude-opus-5-8'), 1_000_000);
    // Opus 4 stays on its base 200k window (no beta).
    assert.equal(getContextWindow('claude-opus-4'), 200_000);
  });
});

// ---------------------------------------------------------------------------
// Per-model max-output-token range (default vs upperLimit)
// ---------------------------------------------------------------------------
describe('U051: getModelMaxOutputTokens — default vs upperLimit ladder', () => {
  afterEach(() => clearDynamicCapabilities());

  it('returns a default BELOW the ceiling for modern families', () => {
    assert.deepEqual(getModelMaxOutputTokens('claude-opus-4-6'), { default: 64_000, upperLimit: 128_000 });
    assert.deepEqual(getModelMaxOutputTokens('claude-sonnet-4-6'), { default: 32_000, upperLimit: 128_000 });
    assert.deepEqual(getModelMaxOutputTokens('claude-opus-4-5'), { default: 32_000, upperLimit: 64_000 });
    assert.deepEqual(getModelMaxOutputTokens('claude-sonnet-4-5'), { default: 32_000, upperLimit: 64_000 });
    assert.deepEqual(getModelMaxOutputTokens('claude-haiku-4-5'), { default: 32_000, upperLimit: 64_000 });
    assert.deepEqual(getModelMaxOutputTokens('claude-opus-4-1'), { default: 32_000, upperLimit: 32_000 });
    assert.deepEqual(getModelMaxOutputTokens('claude-opus-4'), { default: 32_000, upperLimit: 32_000 });
  });

  it('places default == ceiling for older single-knob families', () => {
    assert.deepEqual(getModelMaxOutputTokens('claude-3-5-sonnet'), { default: 8192, upperLimit: 8192 });
    assert.deepEqual(getModelMaxOutputTokens('claude-3-opus'), { default: 4096, upperLimit: 4096 });
    assert.deepEqual(getModelMaxOutputTokens('claude-3-sonnet'), { default: 8192, upperLimit: 8192 });
  });

  it('respects ladder ordering: opus-4-6 is not shadowed by opus-4; sonnet-4-6 not by sonnet-4', () => {
    assert.equal(getModelMaxOutputTokens('claude-opus-4-6-20260101').upperLimit, 128_000);
    assert.equal(getModelMaxOutputTokens('claude-sonnet-4-6-20260101').upperLimit, 128_000);
    // A dated sonnet-4 snapshot still resolves to the sonnet-4 (not sonnet-4-6) branch.
    assert.equal(getModelMaxOutputTokens('claude-sonnet-4-20250514').upperLimit, 64_000);
  });

  it('falls back to the global default/upper for an unlisted model', () => {
    assert.deepEqual(getModelMaxOutputTokens('some-unknown-model'), {
      default: DEFAULT_MAX_OUTPUT_TOKENS,
      upperLimit: MAX_OUTPUT_TOKENS_UPPER_LIMIT,
    });
    assert.deepEqual(getModelMaxOutputTokens(''), {
      default: DEFAULT_MAX_OUTPUT_TOKENS,
      upperLimit: MAX_OUTPUT_TOKENS_UPPER_LIMIT,
    });
  });

  it('a capability-advertised cap (>=4096) raises the ceiling and clamps the default', () => {
    // Advertised 16k < the family default of 64k -> default clamps down to 16k.
    registerModelCapabilities('cap-model', {
      reasoning: false, defaultThinkingLevel: 'off', supportsAdaptiveThinking: false,
      requiresTemperatureOne: false, temperatureMustBeUnset: false, maxThinkingBudget: 0,
      maxOutputTokens: 16_000,
    });
    assert.deepEqual(getModelMaxOutputTokens('cap-model'), { default: 16_000, upperLimit: 16_000 });
  });

  it('a live /models output cap (>=4096) raises the ceiling for the pair', () => {
    discoverFromModelList([{ id: 'vllm-served', max_output_tokens: 20_000 }]);
    assert.deepEqual(getModelMaxOutputTokens('vllm-served'), { default: 20_000, upperLimit: 20_000 });
  });

  it('ignores a tiny capability cap (<4096) for the pair (Claude >=4096 guard)', () => {
    registerModelCapabilities('tiny-cap', {
      reasoning: false, defaultThinkingLevel: 'off', supportsAdaptiveThinking: false,
      requiresTemperatureOne: false, temperatureMustBeUnset: false, maxThinkingBudget: 0,
      maxOutputTokens: 2_000,
    });
    assert.deepEqual(getModelMaxOutputTokens('tiny-cap'), {
      default: DEFAULT_MAX_OUTPUT_TOKENS,
      upperLimit: MAX_OUTPUT_TOKENS_UPPER_LIMIT,
    });
  });
});

describe('U051: getMaxOutputTokens (ceiling) stays consistent with the ladder', () => {
  afterEach(() => clearDynamicCapabilities());

  it('returns the upperLimit for known families, undefined for unknown', () => {
    assert.equal(getMaxOutputTokens('claude-3-5-sonnet'), 8192);
    assert.equal(getMaxOutputTokens('claude-opus-4-6'), 128_000);
    assert.equal(getMaxOutputTokens('claude-sonnet-4-5'), 64_000);
    assert.equal(getMaxOutputTokens('some-unknown-model'), undefined);
    assert.equal(getMaxOutputTokens(''), undefined);
  });
});

// ---------------------------------------------------------------------------
// End-to-end: streamChatCompletion sizes maxOutputTokens from the model DEFAULT
// ---------------------------------------------------------------------------
describe('U051: streamChatCompletion uses the model DEFAULT budget when maxTokens omitted', () => {
  const NO_MSG: Message[] = [{ role: 'user', content: 'hi' }];

  async function captureMaxOutput(model: string): Promise<number | undefined> {
    let captured: number | undefined;
    const fakeStreamText = (cfg: { maxOutputTokens?: number }) => {
      captured = cfg.maxOutputTokens;
      return { stream: (async function* () { /* no parts */ })() };
    };
    const config: ProviderConfig = { provider: 'anthropic', apiKey: 'sk-x', model };
    const gen = streamChatCompletion(config, NO_MSG, [], new AbortController().signal, {
      streamTextImpl: fakeStreamText as never,
    });
    const out: StreamChunk[] = [];
    for await (const c of gen) out.push(c);
    return captured;
  }

  it('sends opus-4-6 its 64k default (not the flat global constant)', async () => {
    // opus-4-6: default 64k, 200k window -> fits -> 64000.
    // Before the default-knob wiring this was DEFAULT_MAX_OUTPUT_TOKENS (32768).
    const got = await captureMaxOutput('claude-opus-4-6');
    assert.equal(got, 64_000);
  });

  it('sends claude-3-5-sonnet its 8192 ceiling as the default budget', async () => {
    const got = await captureMaxOutput('claude-3-5-sonnet');
    assert.equal(got, 8192);
  });
});
