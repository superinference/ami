import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';
import { validateModel } from '../src/model-registry';
import type { ProviderConfig } from '../src/types';

function cfg(overrides: Partial<ProviderConfig>): ProviderConfig {
  return { provider: 'openai', apiKey: 'k', baseUrl: '', model: 'm', ...overrides } as ProviderConfig;
}

describe('validateModel — auth vs network distinction', () => {
  let authServer: http.Server;
  let authPort = 0;
  before(async () => {
    authServer = http.createServer((_req, res) => { res.statusCode = 401; res.end('{"error":"bad key"}'); });
    await new Promise<void>(r => authServer.listen(0, () => r()));
    authPort = (authServer.address() as import('net').AddressInfo).port;
  });
  after(async () => { await new Promise<void>(r => authServer.close(() => r())); });

  it('hard-fails on an authentication error (does not fail open)', async () => {
    const res = await validateModel(cfg({ baseUrl: `http://127.0.0.1:${authPort}`, apiKey: 'bad' }), 'gpt-4o');
    assert.equal(res.valid, false);
    assert.equal(res.errorKind, 'auth');
    assert.ok(res.reason && /auth/i.test(res.reason));
  });

  it('soft-fails open on a connection error but surfaces the reason', async () => {
    const res = await validateModel(cfg({ baseUrl: 'http://127.0.0.1:1' }), 'gpt-4o');
    assert.equal(res.valid, true); // transient — do not block the switch
    assert.equal(res.errorKind, 'network');
    assert.ok(res.reason);
  });
});

describe('validateModel — input normalization', () => {
  let server: http.Server;
  let port = 0;
  let hits = 0;
  before(async () => {
    server = http.createServer((_req, res) => {
      hits++;
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ data: [{ id: 'GPT-4O' }, { id: 'gpt-4o-mini' }] }));
    });
    await new Promise<void>(r => server.listen(0, () => r()));
    port = (server.address() as import('net').AddressInfo).port;
  });
  after(async () => { await new Promise<void>(r => server.close(() => r())); });

  it('rejects an empty/whitespace model name with a dedicated error and no listing', async () => {
    const before = hits;
    const res = await validateModel(cfg({ baseUrl: `http://127.0.0.1:${port}/v1` }), '   ');
    assert.equal(res.valid, false);
    assert.equal(res.reason, 'Model name cannot be empty');
    assert.deepEqual(res.available, []);
    assert.equal(res.suggestion, undefined);
    assert.equal(hits, before, 'empty name must short-circuit before any provider listing');
  });

  it('trims surrounding whitespace before matching (exact match succeeds)', async () => {
    const res = await validateModel(cfg({ baseUrl: `http://127.0.0.1:${port}/v1` }), '  gpt-4o-mini  ');
    assert.equal(res.valid, true);
  });

  it('matches case-insensitively when the provider id differs only in case', async () => {
    const res = await validateModel(cfg({ baseUrl: `http://127.0.0.1:${port}/v1` }), 'gpt-4o');
    assert.equal(res.valid, true, 'gpt-4o should match the listed GPT-4O case-insensitively');
  });
});

describe('validateModel — custom-model knob (SI_CUSTOM_MODEL)', () => {
  const saved = process.env.SI_CUSTOM_MODEL;
  after(() => {
    if (saved === undefined) delete process.env.SI_CUSTOM_MODEL;
    else process.env.SI_CUSTOM_MODEL = saved;
  });

  it('force-accepts a declared custom id without any provider round-trip', async () => {
    process.env.SI_CUSTOM_MODEL = 'other, My-Private-Model';
    // baseUrl points nowhere; if listing were attempted it would network-fail.
    const res = await validateModel(cfg({ baseUrl: 'http://127.0.0.1:1' }), 'my-private-model');
    assert.equal(res.valid, true);
    assert.equal(res.errorKind, 'none');
  });

  it('does not accept an id absent from SI_CUSTOM_MODEL', async () => {
    process.env.SI_CUSTOM_MODEL = 'allowed-one';
    const res = await validateModel(cfg({ baseUrl: 'http://127.0.0.1:1' }), 'not-allowed');
    // Falls through to listing (which network-fails -> soft valid), proving the
    // knob did not short-circuit on a non-matching id.
    assert.equal(res.errorKind, 'network');
  });
});

describe('validateModel — deprecated replacement suggestion', () => {
  let server: http.Server;
  let port = 0;
  before(async () => {
    server = http.createServer((_req, res) => {
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ data: [{ id: 'claude-sonnet-4-5' }, { id: 'gpt-4o' }] }));
    });
    await new Promise<void>(r => server.listen(0, () => r()));
    port = (server.address() as import('net').AddressInfo).port;
  });
  after(async () => { await new Promise<void>(r => server.close(() => r())); });

  it('suggests a live successor for a removed id with no substring overlap', async () => {
    const res = await validateModel(cfg({ baseUrl: `http://127.0.0.1:${port}/v1` }), 'claude-3-5-sonnet-20241022');
    assert.equal(res.valid, false);
    assert.equal(res.suggestion, 'claude-sonnet-4-5');
  });

  it('falls back to the canonical successor id when the provider does not list it', async () => {
    const res = await validateModel(cfg({ baseUrl: `http://127.0.0.1:${port}/v1` }), 'claude-3-opus-20240229');
    assert.equal(res.valid, false);
    assert.equal(res.suggestion, 'claude-opus-4-1');
  });

  it('leaves suggestion undefined for an unknown id matching no deprecation pattern', async () => {
    const res = await validateModel(cfg({ baseUrl: `http://127.0.0.1:${port}/v1` }), 'totally-unknown-zzz');
    assert.equal(res.valid, false);
    assert.equal(res.suggestion, undefined);
  });
});

describe('validateModel — result cache', () => {
  let server: http.Server;
  let port = 0;
  let hits = 0;
  before(async () => {
    server = http.createServer((_req, res) => {
      hits++;
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ data: [{ id: 'gpt-4o' }] }));
    });
    await new Promise<void>(r => server.listen(0, () => r()));
    port = (server.address() as import('net').AddressInfo).port;
  });
  after(async () => { await new Promise<void>(r => server.close(() => r())); });

  it('does not re-probe a model that already validated', async () => {
    const config = cfg({ baseUrl: `http://127.0.0.1:${port}/v1`, model: 'gpt-4o' });
    const r1 = await validateModel(config, 'gpt-4o');
    assert.equal(r1.valid, true);
    const after1 = hits;
    const r2 = await validateModel(config, 'gpt-4o');
    assert.equal(r2.valid, true);
    assert.equal(hits, after1, 'second validation should be served from cache');
  });
});
