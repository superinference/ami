/**
 * U065 — model-registry parity additions:
 *  - extractErrorDetail + server error-body surfacing in diagnoseModelDiscovery
 *    (ref bridge/debugUtils.ts:L60-L121): an HTTP failure surfaces the server's
 *    own human-readable message instead of a bare status code.
 *  - modelMatchesVersionPrefix: segment-boundary version-prefix matching with
 *    'claude-' shorthand (ref utils/model/modelAllowlist.ts:L22-L57), used by
 *    the validateModel suggestion path so "claude-opus-4-5" does NOT over-match
 *    a sibling "claude-opus-4-50-preview".
 *  - [modelCapabilities] debug/introspection logging of discovery outcome
 *    (ref utils/model/modelCapabilities.ts:L101-L117).
 *
 * Run: cd core/core && node --import tsx --test test/model-registry-u065-parity.test.ts
 */
import { describe, it, before, after, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';

import {
  extractErrorDetail,
  modelMatchesVersionPrefix,
  diagnoseModelDiscovery,
  listModelsDiagnostic,
  validateModel,
  listModels,
} from '../src/model-registry';
import { fsHooks } from '../src/logger';
import type { ProviderConfig } from '../src/types';

function cfg(overrides: Partial<ProviderConfig>): ProviderConfig {
  return { provider: 'openai', apiKey: 'k', baseUrl: '', model: 'm', ...overrides } as ProviderConfig;
}

describe('extractErrorDetail — server error-body message', () => {
  it('reads data.message first', () => {
    assert.equal(extractErrorDetail({ message: 'model X not found' }), 'model X not found');
  });
  it('falls back to data.error.message', () => {
    assert.equal(extractErrorDetail({ error: { message: 'bad model id' } }), 'bad model id');
  });
  it('returns undefined for non-objects, empty, or string error fields', () => {
    assert.equal(extractErrorDetail(undefined), undefined);
    assert.equal(extractErrorDetail('plain'), undefined);
    assert.equal(extractErrorDetail({ message: '   ' }), undefined);
    assert.equal(extractErrorDetail({ error: 'bad key' }), undefined);
    assert.equal(extractErrorDetail({ error: { code: 1 } }), undefined);
  });
});

describe('diagnoseModelDiscovery — appends server detail', () => {
  it('appends the body detail to a 404 status line', () => {
    const err = Object.assign(new Error('HTTP 404'), { statusCode: 404, detail: 'model gpt-foo not found' });
    const out = diagnoseModelDiscovery(err, 'openai');
    assert.match(out, /HTTP 404/);
    assert.match(out, /model gpt-foo not found$/);
  });
  it('omits the suffix when there is no detail', () => {
    const err = Object.assign(new Error('HTTP 500'), { statusCode: 500 });
    const out = diagnoseModelDiscovery(err, 'openai');
    assert.ok(!out.includes(' — '), 'no detail suffix when body carried none');
  });
});

describe('listModelsDiagnostic — surfaces the server body on HTTP error', () => {
  let server: http.Server;
  let port = 0;
  before(async () => {
    server = http.createServer((_req, res) => {
      res.statusCode = 404;
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ error: { message: 'no such model: gpt-foo' } }));
    });
    await new Promise<void>(r => server.listen(0, () => r()));
    port = (server.address() as import('net').AddressInfo).port;
  });
  after(async () => { await new Promise<void>(r => server.close(() => r())); });

  it('includes the server-reported reason in the diagnostic', async () => {
    const { models, diagnostic } = await listModelsDiagnostic(cfg({ baseUrl: `http://127.0.0.1:${port}/v1` }));
    assert.deepEqual(models, []);
    assert.ok(diagnostic && diagnostic.includes('no such model: gpt-foo'),
      `diagnostic should surface server body, got: ${diagnostic}`);
  });
});

describe('modelMatchesVersionPrefix — segment-boundary + claude- shorthand', () => {
  it('matches at a segment boundary', () => {
    assert.equal(modelMatchesVersionPrefix('claude-opus-4-5-20251101', 'claude-opus-4-5'), true);
    assert.equal(modelMatchesVersionPrefix('claude-opus-4-5', 'claude-opus-4-5'), true);
  });
  it('refuses a non-boundary over-match', () => {
    assert.equal(modelMatchesVersionPrefix('claude-opus-4-50-preview', 'claude-opus-4-5'), false);
  });
  it('auto-expands the claude- shorthand', () => {
    assert.equal(modelMatchesVersionPrefix('claude-opus-4-5-20251101', 'opus-4-5'), true);
    assert.equal(modelMatchesVersionPrefix('claude-opus-4-50-preview', 'opus-4-5'), false);
  });
  it('is case-insensitive', () => {
    assert.equal(modelMatchesVersionPrefix('Claude-Opus-4-5-20251101', 'claude-opus-4-5'), true);
  });
});

describe('validateModel — boundary-aware suggestion (no sibling over-match)', () => {
  let server: http.Server;
  let port = 0;
  before(async () => {
    server = http.createServer((_req, res) => {
      res.setHeader('content-type', 'application/json');
      // Decoy sibling listed FIRST so a naive substring filter would suggest it.
      res.end(JSON.stringify({ data: [
        { id: 'claude-opus-4-50-preview' },
        { id: 'claude-opus-4-5-20251101' },
      ] }));
    });
    await new Promise<void>(r => server.listen(0, () => r()));
    port = (server.address() as import('net').AddressInfo).port;
  });
  after(async () => { await new Promise<void>(r => server.close(() => r())); });

  it('suggests the boundary-correct dated id, not the sibling decoy', async () => {
    const res = await validateModel(cfg({ baseUrl: `http://127.0.0.1:${port}/v1` }), 'claude-opus-4-5');
    assert.equal(res.valid, false);
    assert.equal(res.suggestion, 'claude-opus-4-5-20251101');
  });
});

describe('[modelCapabilities] discovery logging', () => {
  let server: http.Server;
  let port = 0;
  before(async () => {
    server = http.createServer((_req, res) => {
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ data: [{ id: 'gpt-4o' }, { id: 'gpt-4o-mini' }] }));
    });
    await new Promise<void>(r => server.listen(0, () => r()));
    port = (server.address() as import('net').AddressInfo).port;
  });
  after(async () => { await new Promise<void>(r => server.close(() => r())); });

  it('writes a "cached N models" line via the logger', async () => {
    const spy = mock.method(fsHooks, 'appendFileSync', () => {});
    try {
      await listModels(cfg({ baseUrl: `http://127.0.0.1:${port}/v1` }));
    } finally {
      const lines = spy.mock.calls.map(c => String(c.arguments[1] ?? ''));
      spy.mock.restore();
      assert.ok(lines.some(l => /\[modelCapabilities\] cached 2 models/.test(l)),
        `expected a cached-models log line, got: ${lines.join(' | ')}`);
    }
  });

  it('writes a "fetch failed" line on a listing error', async () => {
    const spy = mock.method(fsHooks, 'appendFileSync', () => {});
    try {
      await listModels(cfg({ baseUrl: 'http://127.0.0.1:1' }));
    } finally {
      const lines = spy.mock.calls.map(c => String(c.arguments[1] ?? ''));
      spy.mock.restore();
      assert.ok(lines.some(l => /\[modelCapabilities\] fetch failed:/.test(l)),
        `expected a fetch-failed log line, got: ${lines.join(' | ')}`);
    }
  });
});
