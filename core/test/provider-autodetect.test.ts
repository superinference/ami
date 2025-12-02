import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { inferProviderFromApiKey } from '../src/provider';

describe('inferProviderFromApiKey', () => {
  it('detects Google API key', () => {
    const result = inferProviderFromApiKey('AIzaSyBe12345678901234567890');
    assert.ok(result);
    assert.equal(result.provider, 'google');
    assert.equal(result.defaultModel, 'gemini-2.5-pro');
  });

  it('detects Anthropic API key', () => {
    const result = inferProviderFromApiKey('sk-ant-api03-abcdefghijklmnop');
    assert.ok(result);
    assert.equal(result.provider, 'anthropic');
    assert.equal(result.defaultModel, 'claude-sonnet-4-6');
  });

  it('detects OpenAI API key', () => {
    const result = inferProviderFromApiKey('sk-proj-abcdefghijklmnop');
    assert.ok(result);
    assert.equal(result.provider, 'openai');
    assert.equal(result.defaultModel, 'o4-mini');
  });

  it('detects classic OpenAI key', () => {
    const result = inferProviderFromApiKey('sk-abcdefghijklmnop1234567890');
    assert.ok(result);
    assert.equal(result.provider, 'openai');
  });

  it('returns null for unknown key format', () => {
    const result = inferProviderFromApiKey('some-random-key-format');
    assert.equal(result, null);
  });

  it('returns null for empty string', () => {
    const result = inferProviderFromApiKey('');
    assert.equal(result, null);
  });
});
