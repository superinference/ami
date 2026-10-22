/**
 * Red-green coverage for the model/provider empty-state diagnostics
 * (overlay-model-hub). Standalone core test — no imports from common/.
 *
 *   cd core/core && node --import tsx --test test/model-diagnostics.test.ts
 *
 * RED on current source: diagnoseModelDiscovery / listModelsDiagnostic do not
 * exist, so this file fails to import.
 */
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import * as http from 'http';
import { diagnoseModelDiscovery, listModelsDiagnostic } from '../src/model-registry';
import type { ProviderConfig } from '../src/types';

describe('diagnoseModelDiscovery — typed empty-state diagnostics', () => {
  it('maps 401 to a key-rejected message with the env-var hint', () => {
    const err = Object.assign(new Error('HTTP 401'), { statusCode: 401 });
    const msg = diagnoseModelDiscovery(err, 'openai');
    assert.match(msg, /rejected/i);
    assert.match(msg, /OPENAI_API_KEY/);
  });

  it('maps 403 to a key-rejected message', () => {
    const err = Object.assign(new Error('HTTP 403'), { statusCode: 403 });
    assert.match(diagnoseModelDiscovery(err, 'openai'), /403/);
  });

  it('maps 404 to a base-URL / v1 hint', () => {
    const err = Object.assign(new Error('HTTP 404'), { statusCode: 404 });
    const msg = diagnoseModelDiscovery(err, 'openai');
    assert.match(msg, /404/);
    assert.match(msg, /v1|base URL/i);
  });

  it('maps other HTTP status codes generically', () => {
    const err = Object.assign(new Error('HTTP 500'), { statusCode: 500 });
    assert.match(diagnoseModelDiscovery(err, 'openai'), /HTTP 500/);
  });

  it('maps a missing base URL to a config hint', () => {
    const msg = diagnoseModelDiscovery(new Error('baseUrl is required for OpenAI-compatible model listing'), 'openai');
    assert.match(msg, /base URL|AI_BASE_URL/i);
  });

  it('maps a timeout to an unreachable message', () => {
    assert.match(diagnoseModelDiscovery(new Error('Timeout'), 'openai'), /unreachable|Timed out/i);
  });

  it('maps connection errors (ECONNREFUSED) to an unreachable message', () => {
    const err = Object.assign(new Error('connect ECONNREFUSED'), { code: 'ECONNREFUSED' });
    assert.match(diagnoseModelDiscovery(err, 'ollama'), /ECONNREFUSED/);
  });

  it('maps invalid JSON to a wrong-base-URL hint', () => {
    assert.match(diagnoseModelDiscovery(new Error('Invalid JSON response'), 'openai'), /non-JSON|base URL/i);
  });

  it('scrubs leaked API keys from the fallback message', () => {
    const leaked = 'request failed with key sk-abcdefghijklmnopqrstuvwxyz0123456789';
    const msg = diagnoseModelDiscovery(new Error(leaked), 'openai');
    assert.ok(!msg.includes('sk-abcdefghijklmnopqrstuvwxyz0123456789'), `must scrub key; got: ${msg}`);
  });

  it('falls back for unknown providers with a generic env-var phrase', () => {
    const err = Object.assign(new Error('HTTP 401'), { statusCode: 401 });
    assert.match(diagnoseModelDiscovery(err, 'weirdprovider'), /weirdprovider API key/i);
  });
});

describe('listModelsDiagnostic — empty vs populated', () => {
  let server: http.Server;
  let mode: 'ok' | 'empty' | 'unauthorized' = 'ok';
  let port: number;

  before(async () => {
    server = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      if (mode === 'unauthorized') {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'invalid key' }));
        return;
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ data: mode === 'ok' ? [{ id: 'gpt-4o' }] : [] }));
    });
    await new Promise<void>(resolve => server.listen(0, '127.0.0.1', () => resolve()));
    port = (server.address() as { port: number }).port;
  });

  after(() => server.close());

  const cfg = (): ProviderConfig => ({
    baseUrl: `http://127.0.0.1:${port}`, apiKey: 'test-key', provider: 'openai',
  } as ProviderConfig);

  it('returns models and no diagnostic on success', async () => {
    mode = 'ok';
    const res = await listModelsDiagnostic(cfg());
    assert.equal(res.models.length, 1);
    assert.equal(res.diagnostic, null);
  });

  it('returns an empty-list diagnostic when the provider returns zero models', async () => {
    mode = 'empty';
    const res = await listModelsDiagnostic(cfg());
    assert.equal(res.models.length, 0);
    assert.match(String(res.diagnostic), /empty model list/i);
  });

  it('returns a key-rejected diagnostic on 401', async () => {
    mode = 'unauthorized';
    const res = await listModelsDiagnostic(cfg());
    assert.equal(res.models.length, 0);
    assert.match(String(res.diagnostic), /rejected|401/i);
  });

  it('reports a null diagnostic for a static provider with a built-in list', async () => {
    const res = await listModelsDiagnostic({ provider: 'anthropic', baseUrl: '', apiKey: '' } as ProviderConfig);
    assert.ok(res.models.length > 0);
    assert.equal(res.diagnostic, null);
  });

  it('reports an unreachable diagnostic when the endpoint is down', async () => {
    const res = await listModelsDiagnostic({ provider: 'openai', baseUrl: 'http://127.0.0.1:1', apiKey: 'k' } as ProviderConfig);
    assert.equal(res.models.length, 0);
    assert.ok(res.diagnostic);
  });

  it('exercises the google provider path without crashing', async () => {
    const res = await listModelsDiagnostic({ provider: 'google', baseUrl: '', apiKey: 'test-key' } as ProviderConfig);
    assert.ok(Array.isArray(res.models));
    assert.ok(res.diagnostic === null || typeof res.diagnostic === 'string');
  });
});
