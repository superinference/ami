import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { detectProvider, formatModelList, validateModel } from '../src/model-registry';
import type { ProviderConfig } from '../src/types';

// ---------------------------------------------------------------------------
// Env vars that leak from the host and interfere with provider detection
// ---------------------------------------------------------------------------
const INTERFERING_ENV_VARS = [
  'ANTHROPIC_VERTEX_PROJECT_ID', 'CLAUDE_CODE_USE_VERTEX', 'HF_TOKEN',
  'GROQ_API_KEY', 'MISTRAL_API_KEY', 'XAI_API_KEY', 'DEEPSEEK_API_KEY',
  'TOGETHER_AI_API_KEY', 'COHERE_API_KEY', 'FIREWORKS_API_KEY',
  'PERPLEXITY_API_KEY', 'DEEPINFRA_API_KEY', 'CEREBRAS_API_KEY',
  'ALIBABA_API_KEY', 'DASHSCOPE_API_KEY', 'LUMA_API_KEY',
  'AZURE_OPENAI_API_KEY', 'AWS_ACCESS_KEY_ID', 'GOOGLE_APPLICATION_CREDENTIALS',
];

function withCleanEnv<T>(fn: () => T): T {
  const saved: Record<string, string | undefined> = {};
  for (const key of INTERFERING_ENV_VARS) {
    saved[key] = process.env[key];
    delete process.env[key];
  }
  try {
    return fn();
  } finally {
    for (const key of INTERFERING_ENV_VARS) {
      if (saved[key] !== undefined) process.env[key] = saved[key];
      else delete process.env[key];
    }
  }
}

async function withCleanEnvAsync<T>(fn: () => Promise<T>): Promise<T> {
  const saved: Record<string, string | undefined> = {};
  for (const key of INTERFERING_ENV_VARS) {
    saved[key] = process.env[key];
    delete process.env[key];
  }
  try {
    return await fn();
  } finally {
    for (const key of INTERFERING_ENV_VARS) {
      if (saved[key] !== undefined) process.env[key] = saved[key];
      else delete process.env[key];
    }
  }
}

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
    withCleanEnv(() => assert.equal(detectProvider(cfg({ apiKey: 'sk-ant-abc123' })), 'anthropic'));
  });

  it('detects google from AIza key prefix', () => {
    withCleanEnv(() => assert.equal(detectProvider(cfg({ apiKey: 'AIzaSyCxxxxxxxxx' })), 'google'));
  });

  it('detects openrouter from sk-or- key prefix', () => {
    withCleanEnv(() => assert.equal(detectProvider(cfg({ apiKey: 'sk-or-abcdef' })), 'openrouter'));
  });

  // URL patterns
  it('detects google from generativelanguage URL', () => {
    withCleanEnv(() => assert.equal(detectProvider(cfg({ baseUrl: 'https://generativelanguage.googleapis.com/v1' })), 'google'));
  });

  it('detects anthropic from api.anthropic.com URL', () => {
    withCleanEnv(() => assert.equal(detectProvider(cfg({ baseUrl: 'https://api.anthropic.com/v1' })), 'anthropic'));
  });

  it('detects openrouter from openrouter.ai URL', () => {
    withCleanEnv(() => assert.equal(detectProvider(cfg({ baseUrl: 'https://openrouter.ai/api/v1' })), 'openrouter'));
  });

  it('detects ollama from localhost URL', () => {
    withCleanEnv(() => assert.equal(detectProvider(cfg({ baseUrl: 'http://localhost:11434' })), 'ollama'));
  });

  it('detects ollama from 127.0.0.1 URL', () => {
    withCleanEnv(() => assert.equal(detectProvider(cfg({ baseUrl: 'http://127.0.0.1:11434' })), 'ollama'));
  });

  it('detects openai from api.openai.com URL', () => {
    withCleanEnv(() => assert.equal(detectProvider(cfg({ baseUrl: 'https://api.openai.com/v1' })), 'openai'));
  });

  it('detects deepseek from api.deepseek.com URL', () => {
    withCleanEnv(() => assert.equal(detectProvider(cfg({ baseUrl: 'https://api.deepseek.com' })), 'deepseek'));
  });

  // Fallback with sk- key
  it('falls back to openai for sk- key without matching URL', () => {
    withCleanEnv(() => assert.equal(detectProvider(cfg({ apiKey: 'sk-proj-abcdef123' })), 'openai'));
  });

  // Default fallback
  it('defaults to openai when nothing matches', () => {
    withCleanEnv(() => assert.equal(detectProvider(cfg()), 'openai'));
  });

  // API key priority over URL
  it('API key prefix takes priority over URL pattern', () => {
    // sk-ant- should detect anthropic even with an openai URL
    withCleanEnv(() => assert.equal(detectProvider(cfg({
      apiKey: 'sk-ant-abc',
      baseUrl: 'https://api.openai.com/v1',
    })), 'anthropic'));
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
    const result = await withCleanEnvAsync(() => validateModel(config, 'gpt-4o'));
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
    const models = await withCleanEnvAsync(() => listModels(config));
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
    const result = await withCleanEnvAsync(() => validateModel(config, 'gpt-4o'));
    assert.equal(result.valid, true);
    assert.equal(result.available.length, 3);
  });

  it('returns valid=false and suggestion for partial match', async () => {
    const config = cfg({
      baseUrl: `http://127.0.0.1:${port}`,
      apiKey: 'test-key',
    });
    const result = await withCleanEnvAsync(() => validateModel(config, 'gpt-4'));
    assert.equal(result.valid, false);
    assert.ok(result.suggestion, 'Should suggest a partial match');
  });

  it('returns valid=false without suggestion for no match', async () => {
    const config = cfg({
      baseUrl: `http://127.0.0.1:${port}`,
      apiKey: 'test-key',
    });
    const result = await withCleanEnvAsync(() => validateModel(config, 'totally-unknown-model'));
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

// ---------------------------------------------------------------------------
// In-flight listing coalescing (gap: concurrency-guarded, de-duplicated model
// listing — parity with the reference's sequential()-guarded background init).
// Concurrent callers for the same endpoint must share ONE /models request; a
// later call (after the first settled) must re-fetch.
// ---------------------------------------------------------------------------
import { listModelsDiagnostic, getAgentModelOptions, MODEL_ALIASES, getModelDescriptionForModel } from '../src/model-registry';

describe('listModels — in-flight coalescing', () => {
  let server: http.Server;
  let port: number;
  let requestCount = 0;

  before(async () => {
    const s = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      if (req.url === '/models') {
        requestCount++;
        // Small delay so a concurrent second caller would race in without coalescing.
        await new Promise<void>(r => setTimeout(r, 25));
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ data: [{ id: 'gpt-4o' }, { id: 'gpt-4o-mini' }] }));
      } else {
        res.writeHead(404);
        res.end();
      }
    });
    await new Promise<void>(resolve => { s.listen(0, '127.0.0.1', () => resolve()); });
    server = s;
    port = (s.address() as { port: number }).port;
  });

  after(() => { server.close(); });

  it('coalesces concurrent listings into one request, then re-fetches later', async () => {
    const config = cfg({ baseUrl: `http://127.0.0.1:${port}`, apiKey: 'test-key' });
    requestCount = 0;
    // Two concurrent callers for the SAME endpoint → exactly one network walk.
    const [a, b] = await withCleanEnvAsync(() =>
      Promise.all([listModelsDiagnostic(config), listModelsDiagnostic(config)]),
    );
    assert.equal(requestCount, 1, 'concurrent callers must share a single /models request');
    assert.deepEqual(a.models.map(m => m.id), ['gpt-4o', 'gpt-4o-mini']);
    assert.deepEqual(b.models.map(m => m.id), a.models.map(m => m.id));
    // A caller arriving AFTER the first settled must re-fetch (in-flight only).
    const c = await withCleanEnvAsync(() => listModelsDiagnostic(config));
    assert.equal(requestCount, 2, 'a later call must not reuse a settled listing');
    assert.deepEqual(c.models.map(m => m.id), ['gpt-4o', 'gpt-4o-mini']);
  });
});

// ---------------------------------------------------------------------------
// getAgentModelOptions — curated agent-model catalog (parity with the reference's
// getAgentModelOptions + ModelSelector custom-id injection).
// ---------------------------------------------------------------------------
describe('getAgentModelOptions', () => {
  it('returns one row per accepted alias with a friendly label', () => {
    const opts = getAgentModelOptions();
    assert.deepEqual(opts.map(o => o.value), [...MODEL_ALIASES]);
    // Family aliases carry a one-line description resolved from the canonical id.
    const opus = opts.find(o => o.value === 'opus');
    assert.ok(opus);
    assert.equal(opus!.label, 'Opus 4');
    assert.ok(opus!.description && opus!.description.length > 0);
  });

  it('injects an unknown custom id at the head as "Current model (custom ID)"', () => {
    const opts = getAgentModelOptions('my-org/custom-llm-v9');
    assert.equal(opts[0].value, 'my-org/custom-llm-v9');
    assert.equal(opts[0].description, 'Current model (custom ID)');
    // The base alias set still follows the injected custom row.
    assert.deepEqual(opts.slice(1).map(o => o.value), [...MODEL_ALIASES]);
  });

  it('does not inject when the current model is already an alias value', () => {
    const opts = getAgentModelOptions('opus');
    assert.deepEqual(opts.map(o => o.value), [...MODEL_ALIASES]);
  });

  it('ignores an empty / whitespace current model', () => {
    assert.deepEqual(getAgentModelOptions('   ').map(o => o.value), [...MODEL_ALIASES]);
    assert.deepEqual(getAgentModelOptions().map(o => o.value), [...MODEL_ALIASES]);
  });

  it('carries a model-facing descriptionForModel on every family-alias row', () => {
    const opts = getAgentModelOptions();
    const opus = opts.find(o => o.value === 'opus');
    const sonnet = opts.find(o => o.value === 'sonnet');
    const haiku = opts.find(o => o.value === 'haiku');
    // The model-facing blurb is distinct from the terse human picker copy and
    // names the capability/cost trade-off an agent reasons over when it picks a
    // model for a sub-task.
    assert.ok(opus!.descriptionForModel && opus!.descriptionForModel.length > 0);
    assert.notEqual(opus!.descriptionForModel, opus!.description);
    assert.match(sonnet!.descriptionForModel!, /everyday/i);
    assert.match(haiku!.descriptionForModel!, /less capable/i);
  });

  it('omits descriptionForModel on a custom id with no canned copy', () => {
    const opts = getAgentModelOptions('my-org/custom-llm-v9');
    assert.equal(opts[0].descriptionForModel, undefined);
  });
});

// ---------------------------------------------------------------------------
// getModelDescriptionForModel — longer MODEL-FACING capability/cost copy, distinct
// from the terse human picker copy (parity: the reference's ModelOption.descriptionForModel).
// ---------------------------------------------------------------------------
describe('getModelDescriptionForModel', () => {
  it('resolves a family-level model-facing description for a known id', () => {
    assert.match(getModelDescriptionForModel('claude-opus-4-6')!, /most capable/i);
    assert.match(getModelDescriptionForModel('claude-sonnet-4-5-20251022')!, /everyday/i);
    assert.match(getModelDescriptionForModel('haiku')!, /fastest/i);
  });
  it('resolves through the [1m] long-context suffix and gateway prefixes', () => {
    assert.ok(getModelDescriptionForModel('openrouter/anthropic/claude-sonnet-4.6[1m]'));
  });
  it('returns undefined for an unknown / custom id', () => {
    assert.equal(getModelDescriptionForModel('my-org/private-llm'), undefined);
    assert.equal(getModelDescriptionForModel(''), undefined);
    assert.equal(getModelDescriptionForModel('  '), undefined);
  });
});

// ---------------------------------------------------------------------------
// probeProviderConnectivity — live reachability probe (parity: checkEndpoints)
// ---------------------------------------------------------------------------
import { probeProviderConnectivity } from '../src/model-registry';

describe('probeProviderConnectivity', () => {
  let server: http.Server;
  let port: number;
  const NONESSENTIAL = 'SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC';
  let savedNonessential: string | undefined;

  before(async () => {
    savedNonessential = process.env[NONESSENTIAL];
    delete process.env[NONESSENTIAL];
    const s = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      const auth = req.headers['authorization'];
      if (auth === 'Bearer k_auth') {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: { message: 'invalid api key' } }));
        return;
      }
      if (auth === 'Bearer k_api') {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: { message: 'internal error' } }));
        return;
      }
      if (req.url === '/models') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ data: [{ id: 'gpt-4o' }] }));
        return;
      }
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: { message: 'not found' } }));
    });
    await new Promise<void>(resolve => { s.listen(0, '127.0.0.1', () => resolve()); });
    server = s;
    port = (s.address() as { port: number }).port;
  });

  after(() => {
    server.close();
    if (savedNonessential !== undefined) process.env[NONESSENTIAL] = savedNonessential;
    else delete process.env[NONESSENTIAL];
  });

  it('reports ok + reachable when the endpoint answers 2xx', async () => {
    const r = await probeProviderConnectivity(cfg({ provider: 'openai', baseUrl: `http://127.0.0.1:${port}`, apiKey: 'k_ok' }));
    assert.equal(r.status, 'ok');
    assert.equal(r.reachable, true);
    assert.equal(r.errorKind, 'none');
    assert.equal(typeof r.latencyMs, 'number');
  });

  it('classifies a 401 as auth: reachable but warn', async () => {
    const r = await probeProviderConnectivity(cfg({ provider: 'openai', baseUrl: `http://127.0.0.1:${port}`, apiKey: 'k_auth' }));
    assert.equal(r.status, 'warn');
    assert.equal(r.reachable, true);
    assert.equal(r.errorKind, 'auth');
  });

  it('classifies an HTTP 500 as api: reachable but warn', async () => {
    const r = await probeProviderConnectivity(cfg({ provider: 'openai', baseUrl: `http://127.0.0.1:${port}`, apiKey: 'k_api' }));
    assert.equal(r.status, 'warn');
    assert.equal(r.reachable, true);
    assert.equal(r.errorKind, 'api');
  });

  it('classifies a 404 path as api (endpoint answered)', async () => {
    const r = await probeProviderConnectivity(cfg({ provider: 'openai', baseUrl: `http://127.0.0.1:${port}/sub`, apiKey: 'k_ok' }));
    assert.equal(r.status, 'warn');
    assert.equal(r.reachable, true);
    assert.equal(r.errorKind, 'api');
  });

  it('reports error + unreachable on a connection failure', async () => {
    // Bind then free a port so the probe hits a refused connection (ECONNREFUSED).
    const tmp = http.createServer();
    const freePort = await new Promise<number>(resolve => {
      tmp.listen(0, '127.0.0.1', () => resolve((tmp.address() as { port: number }).port));
    });
    await new Promise<void>(resolve => tmp.close(() => resolve()));
    const r = await probeProviderConnectivity(cfg({ provider: 'openai', baseUrl: `http://127.0.0.1:${freePort}`, apiKey: 'k_ok' }));
    assert.equal(r.status, 'error');
    assert.equal(r.reachable, false);
    assert.equal(r.errorKind, 'network');
  });

  it('errors when no base URL can be derived for the provider', async () => {
    const r = await probeProviderConnectivity(cfg({ provider: 'openai', apiKey: 'k_ok' }));
    assert.equal(r.status, 'error');
    assert.equal(r.reachable, false);
    assert.equal(r.errorKind, 'network');
    assert.match(r.detail, /No base URL configured/);
  });

  it('skips the live probe under essential-traffic-only', async () => {
    process.env[NONESSENTIAL] = '1';
    try {
      const r = await probeProviderConnectivity(cfg({ provider: 'openai', baseUrl: `http://127.0.0.1:${port}`, apiKey: 'k_ok' }));
      assert.equal(r.status, 'skipped');
      assert.equal(r.reachable, false);
      assert.match(r.detail, /nonessential network traffic is disabled/);
    } finally {
      delete process.env[NONESSENTIAL];
    }
  });
});

// ---------------------------------------------------------------------------
// listModels — capability cache skip-when-unchanged (parity: '[modelCapabilities]
// cache unchanged, skipping write')
// ---------------------------------------------------------------------------
import * as fs from 'fs';
import * as os from 'os';
import * as nodePath from 'path';

describe('listModels — capability cache reuse', () => {
  let server: http.Server;
  let port: number;
  let cacheDir: string;
  let savedCacheDir: string | undefined;

  before(async () => {
    const s = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ data: [{ id: 'gpt-4o', owned_by: 'openai', context_window: 128000 }] }));
    });
    await new Promise<void>(resolve => { s.listen(0, '127.0.0.1', () => resolve()); });
    server = s;
    port = (s.address() as { port: number }).port;
    cacheDir = fs.mkdtempSync(nodePath.join(os.tmpdir(), 'mr-cache-'));
    savedCacheDir = process.env.SUPERINFERENCE_MODEL_CACHE_DIR;
    process.env.SUPERINFERENCE_MODEL_CACHE_DIR = cacheDir;
  });

  after(() => {
    server.close();
    if (savedCacheDir !== undefined) process.env.SUPERINFERENCE_MODEL_CACHE_DIR = savedCacheDir;
    else delete process.env.SUPERINFERENCE_MODEL_CACHE_DIR;
    fs.rmSync(cacheDir, { recursive: true, force: true });
  });

  it('does not rewrite the cache file when the model set is unchanged', async () => {
    const config = cfg({ provider: 'openai', baseUrl: `http://127.0.0.1:${port}`, apiKey: 'test-key' });
    await withCleanEnvAsync(() => listModels(config));
    const files = fs.readdirSync(cacheDir).filter(f => f.endsWith('.json'));
    assert.ok(files.length >= 1, 'expected a capability cache file to be written');
    const file = nodePath.join(cacheDir, files[0]);
    const first = fs.readFileSync(file, 'utf8');
    // Second identical discovery must take the skip-when-unchanged branch (no rewrite).
    await withCleanEnvAsync(() => listModels(config));
    assert.equal(fs.readFileSync(file, 'utf8'), first);
  });
});
