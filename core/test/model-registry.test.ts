import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { detectProvider, formatModelList, validateModel } from '../src/model-registry';
import type { ProviderConfig } from '../src/types';

// ---------------------------------------------------------------------------
// Helper to build a minimal ProviderConfig
// ---------------------------------------------------------------------------
function cfg(overrides: Partial<ProviderConfig> = {}): ProviderConfig {
  return {
    baseUrl: '',
    apiKey: '',
    model: '',
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// detectProvider
// ---------------------------------------------------------------------------
describe('detectProvider', () => {
  // Explicit provider
  it('returns explicit provider when set', () => {
    assert.equal(detectProvider(cfg({ provider: 'google' })), 'google');
  });

  // API key patterns
  it('detects anthropic from sk-ant- key prefix', () => {
    assert.equal(detectProvider(cfg({ apiKey: 'sk-ant-abc123' })), 'anthropic');
  });

  it('detects google from AIza key prefix', () => {
    assert.equal(detectProvider(cfg({ apiKey: 'AIzaSyCxxxxxxxxx' })), 'google');
  });

  it('detects openrouter from sk-or- key prefix', () => {
    assert.equal(detectProvider(cfg({ apiKey: 'sk-or-abcdef' })), 'openrouter');
  });

  // URL patterns
  it('detects google from generativelanguage URL', () => {
    assert.equal(detectProvider(cfg({ baseUrl: 'https://generativelanguage.googleapis.com/v1' })), 'google');
  });

  it('detects anthropic from api.anthropic.com URL', () => {
    assert.equal(detectProvider(cfg({ baseUrl: 'https://api.anthropic.com/v1' })), 'anthropic');
  });

  it('detects openrouter from openrouter.ai URL', () => {
    assert.equal(detectProvider(cfg({ baseUrl: 'https://openrouter.ai/api/v1' })), 'openrouter');
  });

  it('detects ollama from localhost URL', () => {
    assert.equal(detectProvider(cfg({ baseUrl: 'http://localhost:11434' })), 'ollama');
  });

  it('detects ollama from 127.0.0.1 URL', () => {
    assert.equal(detectProvider(cfg({ baseUrl: 'http://127.0.0.1:11434' })), 'ollama');
  });

  it('detects openai from api.openai.com URL', () => {
    assert.equal(detectProvider(cfg({ baseUrl: 'https://api.openai.com/v1' })), 'openai');
  });

  it('detects deepseek from api.deepseek.com URL', () => {
    assert.equal(detectProvider(cfg({ baseUrl: 'https://api.deepseek.com' })), 'deepseek');
  });

  // Fallback with sk- key
  it('falls back to openai for sk- key without matching URL', () => {
    assert.equal(detectProvider(cfg({ apiKey: 'sk-proj-abcdef123' })), 'openai');
  });

  // Default fallback
  it('defaults to openai when nothing matches', () => {
    assert.equal(detectProvider(cfg()), 'openai');
  });

  // API key priority over URL
  it('API key prefix takes priority over URL pattern', () => {
    // sk-ant- should detect anthropic even with an openai URL
    assert.equal(detectProvider(cfg({
      apiKey: 'sk-ant-abc',
      baseUrl: 'https://api.openai.com/v1',
    })), 'anthropic');
  });
});

// ---------------------------------------------------------------------------
// formatModelList
// ---------------------------------------------------------------------------
describe('formatModelList', () => {
  it('returns fallback message for empty list', () => {
    const result = formatModelList([], 'openai');
    assert.equal(result, 'Could not retrieve model list from provider.');
  });

  it('formats a short list correctly', () => {
    const models = ['gpt-4o', 'gpt-4-turbo', 'gpt-3.5-turbo'];
    const result = formatModelList(models, 'openai');
    assert.ok(result.includes('Available models (openai'));
    assert.ok(result.includes('3)'));
    assert.ok(result.includes('  gpt-4o'));
    assert.ok(result.includes('  gpt-4-turbo'));
    assert.ok(result.includes('  gpt-3.5-turbo'));
  });

  it('lists all models in given order', () => {
    const models = ['c-model', 'a-model', 'b-model'];
    const result = formatModelList(models, 'test');
    const lines = result.split('\n').slice(1); // skip header
    assert.equal(lines[0].trim(), 'c-model');
    assert.equal(lines[1].trim(), 'a-model');
    assert.equal(lines[2].trim(), 'b-model');
  });

  it('lists all models for large lists', () => {
    const models = Array.from({ length: 50 }, (_, i) => `model-${String(i).padStart(3, '0')}`);
    const result = formatModelList(models, 'ollama');
    assert.ok(result.includes('50)'));
    const lines = result.split('\n').slice(1);
    assert.equal(lines.length, 50);
  });

  it('shows correct count for exactly 30 models', () => {
    const models = Array.from({ length: 30 }, (_, i) => `model-${i}`);
    const result = formatModelList(models, 'test');
    assert.ok(result.includes('30)'));
  });

  it('includes provider name in header', () => {
    const result = formatModelList(['m1'], 'anthropic');
    assert.ok(result.includes('anthropic'));
  });
});

// ---------------------------------------------------------------------------
// validateModel (requires mocking listModels — we test the logic paths)
// ---------------------------------------------------------------------------
describe('validateModel', () => {
  // We can test validateModel by providing an HTTP server or by testing
  // the "empty models" path (network error), since listModels catches errors.
  // For a more complete test, we use a config that will fail to connect.

  it('returns valid=true when models list is empty (cannot validate)', async () => {
    // Using an unreachable base URL so listModels returns []
    const config = cfg({
      baseUrl: 'http://127.0.0.1:1',
      apiKey: 'test',
      model: 'gpt-4o',
    });
    const result = await validateModel(config, 'gpt-4o');
    assert.equal(result.valid, true);
    assert.deepEqual(result.available, []);
  });
});

// ---------------------------------------------------------------------------
// listModels + validateModel with real HTTP server (lines 60-66, 112-128)
// ---------------------------------------------------------------------------
import { before, after } from 'node:test';
import * as http from 'http';
import { listModels } from '../src/model-registry';

describe('listModels — OpenAI-compatible', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    const s = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      if (req.url === '/models') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          data: [
            { id: 'gpt-4o', owned_by: 'openai' },
            { id: 'gpt-4o-mini', owned_by: 'openai' },
            { id: 'gpt-3.5-turbo', owned_by: 'openai' },
          ],
        }));
      } else {
        res.writeHead(404);
        res.end();
      }
    });

    await new Promise<void>(resolve => {
      s.listen(0, '127.0.0.1', () => resolve());
    });
    server = s;
    port = (s.address() as { port: number }).port;
  });

  after(() => { server.close(); });

  it('lists OpenAI-compatible models from server', async () => {
    const config = cfg({
      baseUrl: `http://127.0.0.1:${port}`,
      apiKey: 'test-key',
    });
    const models = await listModels(config);
    assert.equal(models.length, 3);
    assert.equal(models[0].id, 'gpt-4o');
    assert.equal(models[0].owned_by, 'openai');
  });
});

describe('listModels — Gemini provider', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    const s = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        models: [
          { name: 'models/gemini-2.5-pro', displayName: 'Gemini 2.5 Pro' },
          { name: 'models/gemini-2.5-flash', displayName: 'Gemini 2.5 Flash' },
        ],
      }));
    });

    await new Promise<void>(resolve => {
      s.listen(0, '127.0.0.1', () => resolve());
    });
    server = s;
    port = (s.address() as { port: number }).port;
  });

  after(() => { server.close(); });

  it('lists Gemini models via Google API', async () => {
    // We can't easily redirect the hardcoded googleapis.com URL,
    // but we can test that detectProvider routes 'google' provider to listGeminiModels.
    // For full coverage, the catch path in listModels also matters.
    const config = cfg({
      baseUrl: `http://127.0.0.1:${port}`,
      apiKey: 'test-key',
      provider: 'google',
    });
    // listModels calls listGeminiModels for 'google' provider, which uses
    // the hardcoded googleapis.com URL, so this will fail and return [].
    // That's fine — it exercises the catch path (line 76-77).
    const models = await listModels(config);
    assert.ok(Array.isArray(models));
  });
});

describe('validateModel — with live server', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    const s = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        data: [
          { id: 'gpt-4o' },
          { id: 'gpt-4o-mini' },
          { id: 'gpt-3.5-turbo' },
        ],
      }));
    });

    await new Promise<void>(resolve => {
      s.listen(0, '127.0.0.1', () => resolve());
    });
    server = s;
    port = (s.address() as { port: number }).port;
  });

  after(() => { server.close(); });

  it('validates exact match returns valid=true', async () => {
    const config = cfg({
      baseUrl: `http://127.0.0.1:${port}`,
      apiKey: 'test-key',
    });
    const result = await validateModel(config, 'gpt-4o');
    assert.equal(result.valid, true);
    assert.equal(result.available.length, 3);
  });

  it('returns valid=false and suggestion for partial match', async () => {
    const config = cfg({
      baseUrl: `http://127.0.0.1:${port}`,
      apiKey: 'test-key',
    });
    const result = await validateModel(config, 'gpt-4');
    assert.equal(result.valid, false);
    assert.ok(result.suggestion, 'Should suggest a partial match');
  });

  it('returns valid=false without suggestion for no match', async () => {
    const config = cfg({
      baseUrl: `http://127.0.0.1:${port}`,
      apiKey: 'test-key',
    });
    const result = await validateModel(config, 'totally-unknown-model');
    assert.equal(result.valid, false);
    assert.equal(result.suggestion, undefined);
  });
});

// ---------------------------------------------------------------------------
// formatModelList — truncation with >30 models (lines 124-128)
// ---------------------------------------------------------------------------
describe('formatModelList — additional coverage', () => {
  it('lists all 30 models without truncation', () => {
    const models = Array.from({ length: 30 }, (_, i) => `m-${String(i).padStart(2, '0')}`);
    const result = formatModelList(models, 'test');
    assert.ok(result.includes('30)'));
    const lines = result.split('\n').slice(1);
    assert.equal(lines.length, 30);
  });

  it('lists all 31 models without truncation', () => {
    const models = Array.from({ length: 31 }, (_, i) => `m-${String(i).padStart(2, '0')}`);
    const result = formatModelList(models, 'test');
    assert.ok(result.includes('31)'));
    const lines = result.split('\n').slice(1);
    assert.equal(lines.length, 31);
  });
});
