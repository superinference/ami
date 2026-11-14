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
