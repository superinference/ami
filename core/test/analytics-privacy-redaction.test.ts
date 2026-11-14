import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import * as http from 'http';

import {
  AnalyticsTracker,
  TelemetryBatchSender,
  getPrivacyLevel,
  isTelemetryDisabled,
  isEssentialTrafficOnly,
  isEnvTruthy,
  sanitizeToolNameForAnalytics,
  sanitizeAnalyticsData,
  sanitizeAnalyticsValue,
  shouldSampleEvent,
  postOtlpBatch,
  isRetryableStatus,
  MAX_ANALYTICS_STRING_LEN,
  type TelemetryBatch,
} from '../src/analytics';

describe('isEnvTruthy', () => {
  it('treats set non-falsey values as true', () => {
    for (const v of ['1', 'true', 'yes', 'on', 'TRUE', ' x ']) assert.equal(isEnvTruthy(v), true, v);
  });
  it('treats unset/empty/falsey values as false', () => {
    for (const v of [undefined, '', '0', 'false', 'no', 'off', 'OFF']) assert.equal(isEnvTruthy(v), false, String(v));
  });
});

describe('privacy level resolution', () => {
  it('defaults to "default" with no flags', () => {
    assert.equal(getPrivacyLevel({}), 'default');
    assert.equal(isTelemetryDisabled({}), false);
    assert.equal(isEssentialTrafficOnly({}), false);
  });
  it('DISABLE_TELEMETRY / DO_NOT_TRACK → no-telemetry', () => {
    assert.equal(getPrivacyLevel({ DISABLE_TELEMETRY: '1' }), 'no-telemetry');
    assert.equal(getPrivacyLevel({ DO_NOT_TRACK: '1' }), 'no-telemetry');
    assert.equal(isTelemetryDisabled({ DO_NOT_TRACK: '1' }), true);
  });
  it('nonessential-traffic flag wins as the most restrictive', () => {
    const env = { DISABLE_TELEMETRY: '1', SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC: '1' };
    assert.equal(getPrivacyLevel(env), 'essential-traffic');
    assert.equal(isEssentialTrafficOnly(env), true);
    assert.equal(isTelemetryDisabled(env), true);
    assert.equal(getPrivacyLevel({ CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: '1' }), 'essential-traffic');
  });
});

describe('sanitizeToolNameForAnalytics', () => {
  it('collapses mcp__server__tool to mcp_tool', () => {
    assert.equal(sanitizeToolNameForAnalytics('mcp__github__create_issue'), 'mcp_tool');
  });
  it('passes built-in names through', () => {
    assert.equal(sanitizeToolNameForAnalytics('bash'), 'bash');
    assert.equal(sanitizeToolNameForAnalytics(''), '');
  });
  it('honours the logToolDetails opt-in', () => {
    assert.equal(sanitizeToolNameForAnalytics('mcp__x__y', { logToolDetails: true }), 'mcp__x__y');
  });
});

describe('sanitizeAnalyticsValue / sanitizeAnalyticsData', () => {
  it('leaves clean primitive/nested data structurally identical', () => {
    const data = { nested: { deep: true }, count: 42, list: [1, 2, 3] };
    assert.deepEqual(sanitizeAnalyticsData(data, { env: {} }), data);
  });
  it('redacts credentials inside string values', () => {
    const out = sanitizeAnalyticsData({ note: 'token sk-ant-api03-ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdef' }, { env: {} });
    assert.ok(!String(out.note).includes('ABCDEFGHIJKLMNOPQRSTUVWXYZ'));
  });
  it('drops internal (underscore-prefixed) keys', () => {
    const out = sanitizeAnalyticsData({ _internal: 'x', keep: 1 }, { env: {} });
    assert.deepEqual(out, { keep: 1 });
  });
  it('sanitizes tool-name keys', () => {
    const out = sanitizeAnalyticsData({ toolName: 'mcp__srv__do', tool: 'bash' }, { env: {} });
    assert.equal(out.toolName, 'mcp_tool');
    assert.equal(out.tool, 'bash');
  });
  it('redacts free-text content keys to length-only by default', () => {
    const out = sanitizeAnalyticsData({ prompt: 'secret user text', model: 'opus' }, { env: {} });
    assert.equal(out.prompt, '<REDACTED>');
    assert.equal(out.prompt_length, 'secret user text'.length);
    assert.equal(out.model, 'opus');
  });
  it('keeps content when OTEL_LOG_USER_PROMPTS opts in', () => {
    const out = sanitizeAnalyticsData({ prompt: 'hello' }, { env: { OTEL_LOG_USER_PROMPTS: '1' } });
    assert.equal(out.prompt, 'hello');
  });
  it('truncates oversized strings with a marker', () => {
    const big = 'a'.repeat(MAX_ANALYTICS_STRING_LEN + 500);
    const out = sanitizeAnalyticsValue(big) as string;
    assert.ok(out.length < big.length);
    assert.match(out, /\[\+500\]$/);
  });
  it('bounds recursion depth on deeply nested values', () => {
    let v: unknown = 'leaf';
    for (let i = 0; i < 20; i++) v = { child: v };
    const out = JSON.stringify(sanitizeAnalyticsValue(v));
    assert.ok(out.includes('[nested]'));
  });
});

describe('shouldSampleEvent', () => {
  it('keeps all at rate >= 1 and drops all at rate <= 0', () => {
    assert.equal(shouldSampleEvent(1), true);
    assert.equal(shouldSampleEvent(2), true);
    assert.equal(shouldSampleEvent(0), false);
    assert.equal(shouldSampleEvent(-1), false);
  });
  it('uses the rng for fractional rates', () => {
    assert.equal(shouldSampleEvent(0.5, () => 0.4), true);
    assert.equal(shouldSampleEvent(0.5, () => 0.6), false);
  });
});

describe('AnalyticsTracker — sequence + sampling + egress redaction', () => {
  it('assigns a monotonic per-event sequence', () => {
    const t = new AnalyticsTracker();
    t.log('a'); t.log('b'); t.log('c');
    assert.deepEqual(t.getEvents().map(e => e.sequence), [0, 1, 2]);
  });

  it('drops sampled-out events and counts them', () => {
    const t = new AnalyticsTracker(undefined, { sampleRate: 0 });
    t.log('x');
    assert.equal(t.getEvents().length, 0);
    assert.equal(t.getDroppedEventCount(), 1);
  });

  it('keeps RAW data in memory but writes a redacted copy to JSONL', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-anl-'));
    try {
      const t = new AnalyticsTracker(dir, { env: {} });
      t.log('tool_call', { prompt: 'raw secret', toolName: 'mcp__s__t' });
      // in-memory keeps raw for local introspection
      assert.deepEqual(t.getEvents()[0].data, { prompt: 'raw secret', toolName: 'mcp__s__t' });
      // JSONL egress is minimized
      const line = fs.readFileSync(path.join(dir, 'analytics.jsonl'), 'utf8').trim();
      const parsed = JSON.parse(line);
      assert.equal(parsed.data.prompt, '<REDACTED>');
      assert.equal(parsed.data.prompt_length, 'raw secret'.length);
      assert.equal(parsed.data.toolName, 'mcp_tool');
      assert.equal(typeof parsed.sequence, 'number');
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('exposes the resolved privacy level', () => {
    assert.equal(new AnalyticsTracker(undefined, { env: { DO_NOT_TRACK: '1' } }).getPrivacyLevel(), 'no-telemetry');
  });
});

describe('isRetryableStatus / postOtlpBatch classification', () => {
  it('classifies statuses', () => {
    assert.equal(isRetryableStatus(undefined), true);
    assert.equal(isRetryableStatus(429), true);
    assert.equal(isRetryableStatus(503), true);
    assert.equal(isRetryableStatus(400), false);
    assert.equal(isRetryableStatus(404), false);
  });
  it('returns a non-retryable result when the injected fetch impl is explicitly null', async () => {
    // Force the "no fetch available" branch by injecting a null impl AND removing
    // the global fallback for the duration of the call.
    const savedFetch = globalThis.fetch;
    (globalThis as { fetch?: typeof fetch }).fetch = undefined;
    try {
      const r = await postOtlpBatch('http://x', { sessionId: 's', batchId: 1, events: [] }, { fetchImpl: undefined as unknown as typeof fetch });
      assert.deepEqual(r, { ok: false, retryable: false });
    } finally {
      (globalThis as { fetch?: typeof fetch }).fetch = savedFetch;
    }
  });
});

const sampleBatch: TelemetryBatch = { sessionId: 's', batchId: 1, events: [{ type: 'e', timestamp: 1, data: {} }] };

describe('AnalyticsTracker.flushOtlp — telemetry opt-out gate', () => {
  it('does not POST when telemetry is disabled', async () => {
    let called = 0;
    const fakeFetch = (async () => { called++; return { ok: true } as Response; }) as unknown as typeof fetch;
    const t = new AnalyticsTracker(undefined, { otlpEndpoint: 'http://x', otlpFetch: fakeFetch, env: { DISABLE_TELEMETRY: '1' } });
    t.log('e', { k: 1 });
    await t.flushOtlp();
    assert.equal(called, 0);
  });
});

describe('AnalyticsTracker.flushOtlp — retry/backoff', () => {
  function makeServer(handler: (n: number, res: http.ServerResponse) => void): Promise<{ base: string; close: () => Promise<void>; count: () => number }> {
    let n = 0;
    const server = http.createServer((req, res) => { n++; req.resume(); req.on('end', () => handler(n, res)); });
    return new Promise(resolve => {
      server.listen(0, '127.0.0.1', () => {
        const addr = server.address();
        const port = typeof addr === 'object' && addr ? addr.port : 0;
        resolve({
          base: `http://127.0.0.1:${port}`,
          close: () => new Promise<void>(r => server.close(() => r())),
          count: () => n,
        });
      });
    });
  }

  it('retries a 503 then succeeds, honouring maxAttempts', async () => {
    const srv = await makeServer((n, res) => { res.statusCode = n < 3 ? 503 : 200; res.end('ok'); });
    try {
      const t = new AnalyticsTracker(undefined, {
        otlpEndpoint: srv.base, env: {},
        otlpBaseBackoffMs: 0, otlpMaxAttempts: 5, sleepImpl: async () => {},
      });
      t.log('e', { k: 1 });
      await t.flushOtlp();
      assert.equal(srv.count(), 3); // two 503s then a 200
      assert.equal(t.getDroppedBatchCount(), 0);
    } finally {
      await srv.close();
    }
  });

  it('drops a 400 immediately (permanent)', async () => {
    const srv = await makeServer((_n, res) => { res.statusCode = 400; res.end('bad'); });
    try {
      const t = new AnalyticsTracker(undefined, {
        otlpEndpoint: srv.base, env: {},
        otlpBaseBackoffMs: 0, otlpMaxAttempts: 5, sleepImpl: async () => {},
      });
      t.log('e', { k: 1 });
      await t.flushOtlp();
      assert.equal(srv.count(), 1); // no retry on 4xx
      assert.equal(t.getDroppedBatchCount(), 1);
    } finally {
      await srv.close();
    }
  });

  it('gives up after maxAttempts on persistent 500 and drops the batch', async () => {
    const srv = await makeServer((_n, res) => { res.statusCode = 500; res.end('x'); });
    try {
      const t = new AnalyticsTracker(undefined, {
        otlpEndpoint: srv.base, env: {},
        otlpBaseBackoffMs: 0, otlpMaxAttempts: 3, sleepImpl: async () => {},
      });
      t.log('e', { k: 1 });
      await t.flushOtlp();
      assert.equal(srv.count(), 3);
      assert.equal(t.getDroppedBatchCount(), 1);
    } finally {
      await srv.close();
    }
  });
});

describe('TelemetryBatchSender.requeueFront', () => {
  it('re-queues drained events at the front, order preserved', () => {
    const s = new TelemetryBatchSender({ sessionId: 's', maxBatchSize: 1000, maxQueueSize: 100 });
    s.enqueue('a', {});
    const batches = s.drain();
    s.enqueue('b', {});
    s.requeueFront(batches);
    const out = s.flush()!;
    assert.deepEqual(out.events.map(e => e.type), ['a', 'b']);
  });

  it('respects maxQueueSize and counts overflow drops; ignores empty', () => {
    const s = new TelemetryBatchSender({ sessionId: 's', maxBatchSize: 1000, maxQueueSize: 1 });
    s.enqueue('a', {});
    s.requeueFront([{ sessionId: 's', batchId: 9, events: [{ type: 'x', timestamp: 1, data: {} }, { type: 'y', timestamp: 2, data: {} }] }]);
    assert.equal(s.queueSize, 1);
    assert.equal(s.totalDropped, 2);
    s.requeueFront([]); // no-op
    assert.equal(s.queueSize, 1);
  });
});

describe('sampleBatch sanity', () => {
  it('is a valid TelemetryBatch', () => {
    assert.equal(sampleBatch.events.length, 1);
  });
});
