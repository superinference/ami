import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';

import {
  toOtlpLogsPayload,
  otlpLogsUrl,
  sendOtlpBatch,
  AnalyticsTracker,
  type TelemetryBatch,
} from '../src/analytics';

after(() => { setTimeout(() => process.exit(0), 200); });

const batch: TelemetryBatch = {
  events: [
    { type: 'tool_use', timestamp: 1_700_000_000_000, data: { tool: 'bash', count: 3 } },
  ],
  sessionId: 'sess-1',
  batchId: 1,
};

describe('toOtlpLogsPayload', () => {
  it('builds a resourceLogs/scopeLogs/logRecords structure with service + session attrs', () => {
    const p = toOtlpLogsPayload(batch) as any;
    const rl = p.resourceLogs[0];
    const resAttrs = Object.fromEntries(rl.resource.attributes.map((a: any) => [a.key, a.value.stringValue]));
    assert.equal(resAttrs['service.name'], 'ami');
    assert.equal(resAttrs['session.id'], 'sess-1');
    assert.ok(resAttrs['service.version']);
    const rec = rl.scopeLogs[0].logRecords[0];
    assert.equal(rec.body.stringValue, 'tool_use');
    assert.equal(rec.timeUnixNano, String(1_700_000_000_000 * 1_000_000));
    const recAttrs = Object.fromEntries(rec.attributes.map((a: any) => [a.key, a.value.stringValue]));
    assert.equal(recAttrs.tool, 'bash');           // string passes through
    assert.equal(recAttrs.count, '3');             // non-string is JSON-stringified
  });

  it('merges extra resource attributes', () => {
    const p = toOtlpLogsPayload(batch, { 'deployment.environment': 'ci' }) as any;
    const resAttrs = Object.fromEntries(p.resourceLogs[0].resource.attributes.map((a: any) => [a.key, a.value.stringValue]));
    assert.equal(resAttrs['deployment.environment'], 'ci');
  });
});

describe('otlpLogsUrl', () => {
  it('appends /v1/logs to a base endpoint', () => {
    assert.equal(otlpLogsUrl('http://localhost:4318'), 'http://localhost:4318/v1/logs');
    assert.equal(otlpLogsUrl('http://localhost:4318/'), 'http://localhost:4318/v1/logs');
  });
  it('preserves an explicit /v1/{signal} path', () => {
    assert.equal(otlpLogsUrl('http://x/v1/logs'), 'http://x/v1/logs');
    assert.equal(otlpLogsUrl('http://x/v1/traces'), 'http://x/v1/traces');
  });
});

function mockServer(status: number): Promise<{ url: string; received: () => { path: string; body: string } | null; close: () => void }> {
  let received: { path: string; body: string } | null = null;
  return new Promise((resolve) => {
    const s = http.createServer(async (req, res) => {
      const chunks: Buffer[] = [];
      for await (const c of req) chunks.push(c as Buffer);
      received = { path: req.url ?? '', body: Buffer.concat(chunks).toString('utf-8') };
      res.writeHead(status);
      res.end('{}');
    });
    s.listen(0, '127.0.0.1', () => {
      const port = (s.address() as { port: number }).port;
      resolve({ url: `http://127.0.0.1:${port}`, received: () => received, close: () => s.close() });
    });
  });
}

describe('sendOtlpBatch', () => {
  it('POSTs the OTLP payload to /v1/logs and returns true on 2xx', async () => {
    const srv = await mockServer(200);
    try {
      const ok = await sendOtlpBatch(srv.url, batch);
      assert.equal(ok, true);
      const got = srv.received()!;
      assert.equal(got.path, '/v1/logs');
      const parsed = JSON.parse(got.body);
      assert.equal(parsed.resourceLogs[0].scopeLogs[0].logRecords[0].body.stringValue, 'tool_use');
    } finally { srv.close(); }
  });

  it('returns false on a non-2xx response', async () => {
    const srv = await mockServer(500);
    try {
      assert.equal(await sendOtlpBatch(srv.url, batch), false);
    } finally { srv.close(); }
  });

  it('returns false on a connection error (nothing listening)', async () => {
    // Port 0 reserved address that refuses: use a closed server's port.
    const srv = await mockServer(200);
    const dead = srv.url;
    srv.close();
    await new Promise((r) => setTimeout(r, 20));
    assert.equal(await sendOtlpBatch(dead, batch, { timeoutMs: 500 }), false);
  });

  it('forwards custom headers (e.g. auth) to the collector', async () => {
    let gotAuth: string | undefined;
    const mockFetch = (async (_url: string, init: any) => {
      gotAuth = init.headers.Authorization;
      return { ok: true } as Response;
    }) as unknown as typeof fetch;
    const ok = await sendOtlpBatch('http://x', batch, { fetchImpl: mockFetch, headers: { Authorization: 'Bearer t' } });
    assert.equal(ok, true);
    assert.equal(gotAuth, 'Bearer t');
  });

  it('returns false when no fetch implementation is available', async () => {
    const saved = (globalThis as any).fetch;
    (globalThis as any).fetch = undefined;
    try {
      assert.equal(await sendOtlpBatch('http://x', batch), false);
    } finally {
      (globalThis as any).fetch = saved;
    }
  });
});

describe('AnalyticsTracker OTLP integration', () => {
  it('enqueues events and flushOtlp posts them via the injected fetch', async () => {
    const calls: Array<{ url: string; body: string }> = [];
    const mockFetch = (async (url: string, init: any) => {
      calls.push({ url, body: String(init.body) });
      return { ok: true } as Response;
    }) as unknown as typeof fetch;

    const t = new AnalyticsTracker(undefined, { otlpEndpoint: 'http://collector:4318', otlpFetch: mockFetch });
    t.setSessionId('abc');
    t.log('turn_complete', { turns: 2 });
    t.log('tool_use', { tool: 'grep' });
    await t.flushOtlp();

    assert.equal(calls.length, 1);
    assert.equal(calls[0].url, 'http://collector:4318/v1/logs');
    const payload = JSON.parse(calls[0].body);
    const records = payload.resourceLogs[0].scopeLogs[0].logRecords;
    assert.equal(records.length, 2);
    assert.equal(payload.resourceLogs[0].resource.attributes.find((a: any) => a.key === 'session.id').value.stringValue, 'abc');
  });

  it('falls back to session.id "unknown" when no session id was set', async () => {
    const calls: string[] = [];
    const mockFetch = (async (_url: string, init: any) => { calls.push(String(init.body)); return { ok: true } as Response; }) as unknown as typeof fetch;
    const t = new AnalyticsTracker(undefined, { otlpEndpoint: 'http://c:4318', otlpFetch: mockFetch });
    t.log('x', {});   // no setSessionId beforehand
    await t.flushOtlp();
    const payload = JSON.parse(calls[0]);
    assert.equal(payload.resourceLogs[0].resource.attributes.find((a: any) => a.key === 'session.id').value.stringValue, 'unknown');
  });

  it('is a no-op when no OTLP endpoint is configured', async () => {
    let called = false;
    const mockFetch = (async () => { called = true; return { ok: true } as Response; }) as unknown as typeof fetch;
    const t = new AnalyticsTracker(undefined, { otlpFetch: mockFetch });
    t.log('x', {});
    await t.flushOtlp();
    assert.equal(called, false);
  });
});
