import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { describeProviders } from '../src/provider';

describe('describeProviders', () => {
  const providers = describeProviders();

  it('returns all major providers', () => {
    const ids = providers.map(p => p.id);
    for (const expected of [
      'google', 'openai', 'anthropic', 'groq', 'mistral', 'deepseek',
      'xai', 'cohere', 'togetherai', 'fireworks', 'cerebras', 'deepinfra',
      'alibaba', 'huggingface', 'nvidia', 'openrouter', 'perplexity',
    ]) {
      assert.ok(ids.includes(expected), `missing provider: ${expected}`);
    }
    assert.ok(ids.length >= 17, `expected >=17 providers, got ${ids.length}`);
  });

  it('each provider has correct shape', () => {
    for (const p of providers) {
      assert.equal(typeof p.id, 'string', `${p.id}: id not string`);
      assert.ok(Array.isArray(p.envVars), `${p.id}: envVars not array`);
      assert.ok(Array.isArray(p.models), `${p.id}: models not array`);
      assert.ok(p.models.length > 0, `${p.id}: models empty`);
      assert.equal(typeof p.nativeSdk, 'boolean', `${p.id}: nativeSdk not boolean`);
      assert.equal(typeof p.baseUrl, 'string', `${p.id}: baseUrl not string`);
      assert.ok(Array.isArray(p.keyPrefixes), `${p.id}: keyPrefixes not array`);
    }
  });

  it('native SDK flag is correct for native providers', () => {
    const byId = new Map(providers.map(p => [p.id, p]));
    for (const id of [
      'xai', 'mistral', 'cohere', 'deepseek', 'groq', 'togetherai',
      'fireworks', 'alibaba', 'deepinfra', 'cerebras', 'huggingface',
      'google', 'anthropic',
    ]) {
      assert.equal(byId.get(id)!.nativeSdk, true, `${id} should be native`);
    }
  });

  it('native SDK flag is false for non-native providers', () => {
    const byId = new Map(providers.map(p => [p.id, p]));
    for (const id of ['openai', 'openrouter', 'nvidia', 'perplexity']) {
      assert.equal(byId.get(id)!.nativeSdk, false, `${id} should NOT be native`);
    }
  });

  it('envVars populated for providers with known keys', () => {
    const byId = new Map(providers.map(p => [p.id, p]));
    assert.ok(byId.get('google')!.envVars.includes('GOOGLE_API_KEY'));
    assert.ok(byId.get('anthropic')!.envVars.includes('ANTHROPIC_API_KEY'));
    assert.ok(byId.get('openai')!.envVars.includes('OPENAI_API_KEY'));
  });

  it('keyPrefixes populated for providers with known prefixes', () => {
    const byId = new Map(providers.map(p => [p.id, p]));
    assert.ok(byId.get('google')!.keyPrefixes.includes('AIzaSy'));
    assert.ok(byId.get('anthropic')!.keyPrefixes.includes('sk-ant-'));
    assert.ok(byId.get('groq')!.keyPrefixes.includes('gsk_'));
  });

  it('JSON round-trips cleanly', () => {
    const json = JSON.stringify(providers);
    const parsed = JSON.parse(json);
    assert.deepEqual(parsed, providers);
  });
});
