/**
 * U065 — honor the essential-traffic-only privacy opt-out before the live
 * model-list discovery network call. Mirrors the reference's modelCapabilities
 * skipping background discovery when *_DISABLE_NONESSENTIAL_TRAFFIC is set.
 *
 * Gap: "Honor an 'essential-traffic-only' privacy opt-out before making
 * background/non-essential network calls (model-list discovery...)".
 * Ref: references/claude/claude-code-main/src/utils/model/modelCapabilities.ts.
 *
 * RED on revert: without the guard, listModels/listModelsDiagnostic hit the
 * mock server even under the opt-out, so requestCount > 0 and these asserts fail.
 *
 * Run: cd core/core && node --import tsx --test test/model-registry-u065-traffic.test.ts
 */
import { describe, it, before, after, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';

import { listModels, listModelsDiagnostic } from '../src/model-registry';
import type { ProviderConfig } from '../src/types';

describe('model-list discovery honors essential-traffic-only (U065)', () => {
  let server: http.Server;
  let port: number;
  let requestCount = 0;
  const ENV = 'SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC';
  let prev: string | undefined;

  before(async () => {
    server = http.createServer(async (req, res) => {
      requestCount++;
      for await (const _ of req) { /* drain */ }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ data: [{ id: 'gpt-from-network' }] }));
    });
    await new Promise<void>(resolve => server.listen(0, '127.0.0.1', () => resolve()));
    port = (server.address() as { port: number }).port;
  });

  after(() => server.close());

  beforeEach(() => {
    requestCount = 0;
    prev = process.env[ENV];
  });
  afterEach(() => {
    if (prev === undefined) delete process.env[ENV];
    else process.env[ENV] = prev;
  });

  const cfg = (): ProviderConfig => ({
    baseUrl: `http://127.0.0.1:${port}`, apiKey: 'test-key', provider: 'openai',
  } as ProviderConfig);

  it('listModels skips the network and returns the static list under the opt-out', async () => {
    process.env[ENV] = '1';
    const models = await listModels(cfg());
    assert.equal(requestCount, 0, 'no discovery request should be made');
    assert.ok(models.length > 0, 'static fallback list is returned');
    assert.ok(!models.some(m => m.id === 'gpt-from-network'), 'network model must not appear');
    // Static list is the provider preference, not the mock-server response.
    assert.ok(models.some(m => m.id === 'gpt-4o'));
  });

  it('listModelsDiagnostic skips the network and returns the static list under the opt-out', async () => {
    process.env[ENV] = '1';
    const res = await listModelsDiagnostic(cfg());
    assert.equal(requestCount, 0, 'no discovery request should be made');
    assert.ok(res.models.length > 0);
    assert.equal(res.diagnostic, null, 'null diagnostic when a static list exists');
    assert.ok(!res.models.some(m => m.id === 'gpt-from-network'));
  });

  it('listModelsDiagnostic reports a skip diagnostic when no static list exists for the provider', async () => {
    process.env[ENV] = '1';
    // nvidia-style: a dynamic provider with no detect match here; use a provider
    // id with no MODEL_PREFERENCE entry so the static fallback is empty.
    const res = await listModelsDiagnostic({
      baseUrl: `http://127.0.0.1:${port}`, apiKey: 'k', provider: 'unknownprov',
    } as ProviderConfig);
    assert.equal(requestCount, 0);
    assert.equal(res.models.length, 0);
    assert.match(String(res.diagnostic), /nonessential network traffic is disabled/i);
    assert.match(String(res.diagnostic), /SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC/);
  });

  it('still hits the network when the opt-out is NOT set', async () => {
    delete process.env[ENV];
    const res = await listModelsDiagnostic(cfg());
    assert.ok(requestCount > 0, 'discovery request should be made without the opt-out');
    assert.ok(res.models.some(m => m.id === 'gpt-from-network'));
  });

  it('also honors the Claude-compatible alias env var', async () => {
    delete process.env[ENV];
    const alias = 'CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC';
    const prevAlias = process.env[alias];
    process.env[alias] = 'true';
    try {
      const models = await listModels(cfg());
      assert.equal(requestCount, 0);
      assert.ok(models.length > 0);
    } finally {
      if (prevAlias === undefined) delete process.env[alias];
      else process.env[alias] = prevAlias;
    }
  });
});
