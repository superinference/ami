/**
 * U065 — model-registry transient-retry + header-injection defense (parity).
 *
 *  - sanitizeHeaderValue: strip CR/LF/NUL from an auth header value before it is
 *    placed in the model-list request, so a mis-pasted key cannot smuggle an
 *    extra header line. Ref utils/hooks/execHttpHook.ts sanitizeHeaderValue.
 *  - httpGetJSONRetrying (via listModels): bounded retry on transient 429/503 and
 *    ECONNRESET/ETIMEDOUT, honoring Retry-After over blind exponential backoff.
 *    Ref services/api/withRetry.ts.
 *
 * RED on revert: before the fix, listOpenAIModels/listGeminiModels passed the raw
 * apiKey into the header (Node throws ERR_INVALID_CHAR on a CRLF key → the server
 * is never reached) and did not retry a 503, so the first blip fell straight
 * through to the static fallback list.
 *
 * Run: cd core/core && node --import tsx --test test/model-registry-u065-retry-sanitize.test.ts
 */
import { describe, it, before, after, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';
import type { AddressInfo } from 'net';

import {
  sanitizeHeaderValue,
  listModels,
  modelListRetryHooks,
} from '../src/model-registry';
import type { ProviderConfig } from '../src/types';

describe('sanitizeHeaderValue — CRLF/NUL header-injection defense (U065)', () => {
  it('strips CR, LF, and NUL bytes', () => {
    assert.equal(sanitizeHeaderValue('sk-test\r\nX-Evil: 1'), 'sk-testX-Evil: 1');
    assert.equal(sanitizeHeaderValue('a\rb\nc\x00d'), 'abcd');
  });

  it('passes a clean value through unchanged', () => {
    assert.equal(sanitizeHeaderValue('sk-ant-abc123'), 'sk-ant-abc123');
  });

  it('returns an empty string for a nullish value', () => {
    assert.equal(sanitizeHeaderValue(undefined), '');
    // @ts-expect-error — defensive: a non-string must not become "undefined"
    assert.equal(sanitizeHeaderValue(null), '');
  });
});

describe('listModels — header-injection neutralized end to end (U065)', () => {
  let server: http.Server;
  let port: number;
  let lastHeaders: http.IncomingHttpHeaders = {};

  before(async () => {
    server = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      lastHeaders = req.headers;
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ data: [{ id: 'served-model' }] }));
    });
    await new Promise<void>(r => server.listen(0, '127.0.0.1', () => r()));
    port = (server.address() as AddressInfo).port;
  });
  after(() => server.close());

  it('sends a sanitized Authorization header and no injected header', async () => {
    const cfg = {
      provider: 'openai',
      baseUrl: `http://127.0.0.1:${port}`,
      apiKey: 'sk-test\r\nX-Evil: pwned',
    } as ProviderConfig;
    const models = await listModels(cfg);
    // The request actually reached the server (proving no ERR_INVALID_CHAR throw).
    assert.deepEqual(models.map(m => m.id), ['served-model']);
    // The CRLF was stripped → the smuggled header never materialized.
    assert.equal(lastHeaders['x-evil'], undefined);
    assert.equal(lastHeaders['authorization'], 'Bearer sk-testX-Evil: pwned');
  });
});

describe('listModels — transient-failure retry (U065)', () => {
  let server: http.Server;
  let port: number;
  let requests = 0;
  let plan: Array<(res: http.ServerResponse) => void> = [];
  const waits: number[] = [];
  const origSleep = modelListRetryHooks.sleep;
  const origRandom = modelListRetryHooks.random;

  const ok = (res: http.ServerResponse) => {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ data: [{ id: 'gpt-4o' }] }));
  };
  const status = (code: number, headers: http.OutgoingHttpHeaders = {}) =>
    (res: http.ServerResponse) => {
      res.writeHead(code, { 'Content-Type': 'application/json', ...headers });
      res.end(JSON.stringify({ error: `HTTP ${code}` }));
    };
  const dropSocket = (res: http.ServerResponse) => { res.socket?.destroy(); };

  before(async () => {
    server = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      const step = plan[requests] ?? ok;
      requests += 1;
      step(res);
    });
    await new Promise<void>(r => server.listen(0, '127.0.0.1', () => r()));
    port = (server.address() as AddressInfo).port;
  });
  after(() => server.close());

  beforeEach(() => {
    requests = 0;
    plan = [];
    waits.length = 0;
    // No real waiting; record the backoff the loop computed, and make jitter
    // deterministic (random()=0 → 50% of the base window).
    modelListRetryHooks.sleep = async (ms: number) => { waits.push(ms); };
    modelListRetryHooks.random = () => 0;
  });
  afterEach(() => {
    modelListRetryHooks.sleep = origSleep;
    modelListRetryHooks.random = origRandom;
  });

  const cfg = (): ProviderConfig => ({
    provider: 'openai',
    baseUrl: `http://127.0.0.1:${port}`,
    apiKey: 'test-key',
  } as ProviderConfig);

  it('retries a 503 and then succeeds (blind jittered backoff)', async () => {
    plan = [status(503), status(503), ok];
    const models = await listModels(cfg());
    assert.deepEqual(models.map(m => m.id), ['gpt-4o']);
    assert.equal(requests, 3);
    // Two blind backoffs: 300*2^0*0.5=150, then 300*2^1*0.5=300.
    assert.deepEqual(waits, [150, 300]);
  });

  it('retries a dropped socket (ECONNRESET) and then succeeds', async () => {
    plan = [dropSocket, ok];
    const models = await listModels(cfg());
    assert.deepEqual(models.map(m => m.id), ['gpt-4o']);
    assert.equal(requests, 2);
    assert.equal(waits.length, 1);
  });

  it('honors Retry-After over blind backoff on a 429', async () => {
    plan = [status(429, { 'retry-after': '1' }), ok];
    const models = await listModels(cfg());
    assert.deepEqual(models.map(m => m.id), ['gpt-4o']);
    // 1s Retry-After header → 1000ms, not the ~150ms blind backoff.
    assert.deepEqual(waits, [1000]);
  });

  it('does NOT retry a non-transient 401 — one request, then static fallback', async () => {
    plan = [status(401), status(401), status(401)];
    const models = await listModels(cfg());
    // Only the first attempt was made (no retry on auth failure).
    assert.equal(requests, 1);
    assert.equal(waits.length, 0);
    // listModels swallowed the failure and fell back (openai static list).
    assert.ok(Array.isArray(models));
  });

  it('gives up after the max attempts on a persistent 503', async () => {
    plan = [status(503), status(503), status(503), ok];
    const models = await listModels(cfg());
    // 3 attempts total, 2 backoffs, then fallback (never reached the ok step).
    assert.equal(requests, 3);
    assert.equal(waits.length, 2);
    assert.ok(Array.isArray(models));
  });
});
