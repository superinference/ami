import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { getContextWindow, getModelCapabilities } from '../src/model-capabilities';

describe('Model context windows', () => {
  it('returns correct window for Claude models', () => {
    assert.equal(getContextWindow('claude-opus-4-20250514'), 200000);
    assert.equal(getContextWindow('claude-sonnet-4-6'), 200000);
  });

  it('returns correct window for GPT models', () => {
    assert.equal(getContextWindow('gpt-4o'), 128000);
    assert.equal(getContextWindow('gpt-4-turbo-preview'), 128000);
    assert.equal(getContextWindow('gpt-4.1'), 1048576);
  });

  it('returns correct window for Gemini models', () => {
    assert.equal(getContextWindow('gemini-2.0-flash'), 1048576);
    assert.equal(getContextWindow('gemini-2.5-pro'), 1048576);
    assert.equal(getContextWindow('gemini-3.1-pro-preview'), 2097152);
  });

  it('returns default 128K for unknown models', () => {
    assert.equal(getContextWindow('unknown-model-xyz'), 128000);
  });

  it('returns correct window for O-series models', () => {
    assert.equal(getContextWindow('o1-preview'), 200000);
    assert.equal(getContextWindow('o3-mini'), 200000);
    assert.equal(getContextWindow('o4-mini'), 200000);
  });

  it('returns 256k for Qwen3 / Qwen3.8', () => {
    assert.equal(getContextWindow('Qwen/Qwen3.8-27B'), 262144);
    assert.equal(getContextWindow('Qwen/Qwen3-235B-A22B'), 262144);
  });

  it('returns correct context for Qwen2.5 models (size-specific)', () => {
    assert.equal(getContextWindow('Qwen/Qwen2.5-0.5B-Instruct'), 32768);
    assert.equal(getContextWindow('Qwen/Qwen2.5-1.5B-Instruct'), 32768);
    assert.equal(getContextWindow('Qwen/Qwen2.5-3B-Instruct'), 32768);
    assert.equal(getContextWindow('Qwen/Qwen2.5-7B-Instruct'), 131072);
    assert.equal(getContextWindow('Qwen/Qwen2.5-14B'), 131072);
    assert.equal(getContextWindow('Qwen/Qwen2.5-32B'), 131072);
    assert.equal(getContextWindow('Qwen/Qwen2.5-72B-Instruct'), 131072);
  });

  it('case-insensitive lookup handles mixed-case HuggingFace IDs', () => {
    assert.equal(getContextWindow('QWEN2.5-0.5B'), 32768);
    assert.equal(getContextWindow('qwen2.5-72b-instruct'), 131072);
    assert.equal(getContextWindow('Qwen/QWEN3-8B'), 262144);
  });

  it('returns correct context for small open-weight models', () => {
    assert.equal(getContextWindow('meta-llama/Llama-3.2-1B-Instruct'), 131072);
    assert.equal(getContextWindow('microsoft/phi-3-mini-4k-instruct'), 4096);
    assert.equal(getContextWindow('microsoft/Phi-3.5-mini-instruct'), 131072);
  });
});

describe('Model capabilities', () => {
  it('detects Claude as reasoning model', () => {
    const caps = getModelCapabilities('claude-sonnet-4-6');
    assert.ok(caps);
    assert.ok(caps.supportsAdaptiveThinking);
  });

  it('detects Gemini 2.5 as reasoning model', () => {
    const caps = getModelCapabilities('gemini-2.5-pro');
    assert.ok(caps);
    assert.ok(caps.reasoning);
  });

  it('returns null for non-reasoning models', () => {
    assert.equal(getModelCapabilities('gpt-4o'), null);
  });
});
