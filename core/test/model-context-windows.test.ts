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
  });

  it('returns correct window for Gemini models', () => {
    assert.equal(getContextWindow('gemini-2.0-flash'), 1048576);
    assert.equal(getContextWindow('gemini-2.5-pro'), 1048576);
    assert.equal(getContextWindow('gemini-3.1-pro-preview'), 1048576);
  });

  it('returns default 128K for unknown models', () => {
    assert.equal(getContextWindow('unknown-model-xyz'), 128000);
  });

  it('returns correct window for O-series models', () => {
    assert.equal(getContextWindow('o1-preview'), 200000);
    assert.equal(getContextWindow('o3-mini'), 200000);
    assert.equal(getContextWindow('o4-mini'), 200000);
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
