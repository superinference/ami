/**
 * analytics-u024-w80 — three additive OTLP/telemetry parity capabilities (U024):
 *
 *  1. OTLP METRICS signal (Claude instrumentation.ts L130-215) + the metrics
 *     aggregation-temporality default knob (L113-116, default 'delta'):
 *     resolveMetricsTemporalityPreference, toOtlpMetricsPayload, AnalyticsTracker.exportMetrics.
 *  2. OTLP TRACES signal (Claude sessionTracing.ts L176-655): toOtlpTracePayload bridging
 *     the Profiler span tree into a parent-child OTLP trace, AnalyticsTracker.exportTrace,
 *     and flushOtlp emitting metrics+traces when a source is wired (end-to-end).
 *  3. TelemetryBatchSender LOSSLESS backpressure (Claude SerialBatchEventUploader L96-119,
 *     255-259) + runtime reconfiguration with loss-safe flush-before-swap
 *     (firstPartyEventLogger.ts reinitialize1PEventLoggingIfConfigChanged L391-449).
 *
 * All assertions fail without the new code (the symbols/behaviours did not exist).
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  resolveMetricsTemporalityPreference, OTLP_TEMPORALITY, toOtlpMetricsPayload, toOtlpTracePayload,
  otlpSignalUrl, resolveOtlpSignalEndpoint, postOtlpPayload, TelemetryBatchSender, AnalyticsTracker,
  type MetricSnapshot, type TelemetryBatch,
} from '../src/analytics';
import { Profiler, type SpanNode } from '../src/profiler';

// --- helpers ---------------------------------------------------------------

function metricMap(payload: Record<string, unknown>): Record<string, unknown> {
  const rm = (payload.resourceMetrics as Array<Record<string, unknown>>)[0];
  const sm = (rm.scopeMetrics as Array<Record<string, unknown>>)[0];
  const metrics = sm.metrics as Array<Record<string, unknown>>;
  return Object.fromEntries(metrics.map(m => [m.name as string, m]));
}
function spanList(payload: Record<string, unknown>): Array<Record<string, unknown>> {
  const rs = (payload.resourceSpans as Array<Record<string, unknown>>)[0];
  const ss = (rs.scopeSpans as Array<Record<string, unknown>>)[0];
  return ss.spans as Array<Record<string, unknown>>;
}
function spanAttrs(span: Record<string, unknown>): Record<string, string> {
  const attrs = span.attributes as Array<{ key: string; value: { stringValue: string } }>;
  return Object.fromEntries(attrs.map(a => [a.key, a.value.stringValue]));
}
/** A fetch stub capturing requests and returning a scripted status sequence. */
function stubFetch(statuses: number[]) {
  const calls: Array<{ url: string; body: unknown }> = [];
  let i = 0;
  const fn = (async (url: string, init: { body: string }) => {
    calls.push({ url, body: JSON.parse(init.body) });
    const status = statuses[Math.min(i++, statuses.length - 1)];
    return { ok: status >= 200 && status < 300, status, headers: { get: () => null } };
  }) as unknown as typeof fetch;
  return { fn, calls };
}
/** An env that allows telemetry egress under the test harness. */
function egressEnv(extra: Record<string, string> = {}): NodeJS.ProcessEnv {
  return { AMI_TELEMETRY_ALLOW_IN_TEST: '1', ...extra } as NodeJS.ProcessEnv;
}

// ===========================================================================
// 1. Metrics temporality default knob + OTLP metrics payload
// ===========================================================================

describe('resolveMetricsTemporalityPreference', () => {
  it('defaults to delta when unset', () => {
    assert.equal(resolveMetricsTemporalityPreference({} as NodeJS.ProcessEnv), 'delta');
  });
  it('honors an explicit cumulative', () => {
    assert.equal(resolveMetricsTemporalityPreference({ OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE: 'Cumulative' } as NodeJS.ProcessEnv), 'cumulative');
  });
  it('falls back to delta on an unknown value', () => {
    assert.equal(resolveMetricsTemporalityPreference({ OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE: 'bogus' } as NodeJS.ProcessEnv), 'delta');
  });
});

describe('toOtlpMetricsPayload', () => {
  const snap: MetricSnapshot = { gauges: { heap_used: 1234, bad: NaN }, counters: { events_total: 7 } };

  it('maps gauges to gauge dataPoints and skips non-finite values', () => {
    const m = metricMap(toOtlpMetricsPayload(snap, 's', {}, {} as NodeJS.ProcessEnv));
    assert.ok('heap_used' in m);
    assert.ok(!('bad' in m), 'NaN gauge must be skipped');
    const g = (m.heap_used.gauge as { dataPoints: Array<{ asDouble: number }> });
    assert.equal(g.dataPoints[0].asDouble, 1234);
  });

  it('maps counters to monotonic sums carrying the resolved delta temporality', () => {
    const m = metricMap(toOtlpMetricsPayload(snap, 's', {}, {} as NodeJS.ProcessEnv));
    const sum = m.events_total.sum as { dataPoints: Array<{ asDouble: number }>; aggregationTemporality: number; isMonotonic: boolean };
    assert.equal(sum.dataPoints[0].asDouble, 7);
    assert.equal(sum.isMonotonic, true);
    assert.equal(sum.aggregationTemporality, OTLP_TEMPORALITY.delta);
  });

  it('applies cumulative temporality to sums when configured', () => {
    const env = { OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE: 'cumulative' } as NodeJS.ProcessEnv;
    const m = metricMap(toOtlpMetricsPayload(snap, 's', {}, env));
    assert.equal((m.events_total.sum as { aggregationTemporality: number }).aggregationTemporality, OTLP_TEMPORALITY.cumulative);
  });

  it('bridges a real Profiler getAll() snapshot', () => {
    const p = new Profiler();
    p.incrementCounter('turns', 3);
    p.setGauge('depth', 5);
    const all = p.getAll();
    const m = metricMap(toOtlpMetricsPayload({ gauges: all }, 's', {}, {} as NodeJS.ProcessEnv));
    assert.equal((m.turns.gauge as { dataPoints: Array<{ asDouble: number }> }).dataPoints[0].asDouble, 3);
    assert.equal((m.depth.gauge as { dataPoints: Array<{ asDouble: number }> }).dataPoints[0].asDouble, 5);
  });
});

// ===========================================================================
// 2. OTLP traces payload from the Profiler span tree
// ===========================================================================

describe('toOtlpTracePayload', () => {
  function buildTree(): SpanNode[] {
    const p = new Profiler();
    const interaction = p.beginSpan('interaction', 'ami.interaction');
    const llm = p.beginSpan('llm_request', 'ami.llm_request', { parentId: interaction.id });
    p.endSpan(llm);
    p.endSpan(interaction, { final: true });
    return p.getSpanTree();
  }

  it('preserves the parent-child hierarchy via spanId/parentSpanId and one shared traceId', () => {
    const spans = spanList(toOtlpTracePayload(buildTree(), 'sess-1', {}, {} as NodeJS.ProcessEnv));
    assert.equal(spans.length, 2);
    const root = spans.find(s => s.name === 'ami.interaction')!;
    const child = spans.find(s => s.name === 'ami.llm_request')!;
    assert.ok(root && child);
    assert.equal(child.parentSpanId, root.spanId, 'child must point at the root spanId');
    assert.equal(root.parentSpanId, undefined, 'root must have no parentSpanId');
    assert.equal(root.traceId, child.traceId, 'all spans share one session trace id');
    assert.equal((root.traceId as string).length, 32);
    assert.equal((root.spanId as string).length, 16);
  });

  it('carries span.type + duration_ms attributes and merged node attrs', () => {
    const spans = spanList(toOtlpTracePayload(buildTree(), 'sess-1', {}, {} as NodeJS.ProcessEnv));
    const root = spans.find(s => s.name === 'ami.interaction')!;
    const a = spanAttrs(root);
    assert.equal(a['span.type'], 'interaction');
    assert.equal(a['agent.id'], 'main');
    assert.ok('duration_ms' in a, 'closed span carries duration_ms');
    assert.equal(a.final, 'true');
  });

  it('a still-open span uses its start time as the end time', () => {
    const p = new Profiler();
    const open = p.beginSpan('tool', 'ami.tool');
    const spans = spanList(toOtlpTracePayload(p.getSpanTree(), 's', {}, {} as NodeJS.ProcessEnv));
    const node = spans.find(s => s.name === 'ami.tool')!;
    assert.equal(node.endTimeUnixNano, node.startTimeUnixNano);
    assert.ok(!('duration_ms' in spanAttrs(node)), 'open span has no duration_ms');
    void open;
  });

  it('empty tree yields an empty spans array', () => {
    assert.equal(spanList(toOtlpTracePayload([], 's')).length, 0);
  });
});

// ===========================================================================
// Signal URL + endpoint resolution (shared by logs/metrics/traces)
// ===========================================================================

describe('otlpSignalUrl / resolveOtlpSignalEndpoint', () => {
  it('appends /v1/{signal} to a generic endpoint', () => {
    assert.equal(otlpSignalUrl('http://c:4318', 'metrics'), 'http://c:4318/v1/metrics');
    assert.equal(otlpSignalUrl('http://c:4318/', 'traces'), 'http://c:4318/v1/traces');
  });
  it('uses a per-signal endpoint verbatim', () => {
    assert.equal(otlpSignalUrl('http://c/custom', 'metrics', true), 'http://c/custom');
  });
  it('does not double-append when a /v1/{signal} path is already present', () => {
    assert.equal(otlpSignalUrl('http://c:4318/v1/metrics', 'metrics'), 'http://c:4318/v1/metrics');
  });
  it('prefers the dedicated per-signal endpoint env var', () => {
    const r = resolveOtlpSignalEndpoint('metrics', { OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: 'http://m', OTEL_EXPORTER_OTLP_ENDPOINT: 'http://g' } as NodeJS.ProcessEnv);
    assert.deepEqual(r, { url: 'http://m', perSignal: true });
  });
  it('falls back to the generic endpoint (not per-signal)', () => {
    const r = resolveOtlpSignalEndpoint('traces', { OTEL_EXPORTER_OTLP_ENDPOINT: 'http://g' } as NodeJS.ProcessEnv);
    assert.deepEqual(r, { url: 'http://g', perSignal: false });
  });
  it('returns null when nothing is configured', () => {
    assert.equal(resolveOtlpSignalEndpoint('metrics', {} as NodeJS.ProcessEnv), null);
  });
});

describe('postOtlpPayload', () => {
  it('returns ok on 2xx and posts the payload to the url', async () => {
    const { fn, calls } = stubFetch([200]);
    const res = await postOtlpPayload('http://c/v1/metrics', { resourceMetrics: [] }, { fetchImpl: fn });
    assert.equal(res.ok, true);
    assert.equal(calls[0].url, 'http://c/v1/metrics');
    assert.deepEqual(calls[0].body, { resourceMetrics: [] });
  });
  it('classifies a 500 as retryable and a 400 as permanent', async () => {
    assert.equal((await postOtlpPayload('u', {}, { fetchImpl: stubFetch([500]).fn })).retryable, true);
    assert.equal((await postOtlpPayload('u', {}, { fetchImpl: stubFetch([400]).fn })).retryable, false);
  });
});

// ===========================================================================
// 3a. TelemetryBatchSender lossless backpressure
// ===========================================================================

describe('TelemetryBatchSender backpressure', () => {
  it('enqueueAsync is non-blocking (lossy) when backpressure is OFF', async () => {
    const s = new TelemetryBatchSender({ sessionId: 's', maxQueueSize: 1, maxBatchSize: 99 });
    assert.equal(await s.enqueueAsync('a', {}), true);
    assert.equal(await s.enqueueAsync('b', {}), false); // dropped, not blocked
    assert.equal(s.totalDropped, 1);
    assert.equal(s.blockedProducerCount, 0);
  });

  it('enqueueAsync blocks on a full queue and resolves once drain frees space', async () => {
    const s = new TelemetryBatchSender({ sessionId: 's', maxQueueSize: 1, maxBatchSize: 99, backpressure: true });
    assert.equal(await s.enqueueAsync('a', {}), true); // queue now full (size 1)
    let resolved = false;
    const pending = s.enqueueAsync('b', {}).then(r => { resolved = true; return r; });
    await new Promise(r => setTimeout(r, 10));
    assert.equal(resolved, false, 'producer must still be blocked while the queue is full');
    assert.equal(s.blockedProducerCount, 1);
    s.drain(); // frees space + releases the blocked producer
    assert.equal(await pending, true);
    assert.equal(s.queueSize, 1, 'the released event was enqueued (lossless, not dropped)');
  });

  it('shutdown releases a blocked producer, which resolves false (not hung)', async () => {
    const s = new TelemetryBatchSender({ sessionId: 's', maxQueueSize: 1, backpressure: true, maxBatchSize: 99 });
    await s.enqueueAsync('a', {});
    const pending = s.enqueueAsync('b', {});
    await new Promise(r => setTimeout(r, 10));
    assert.equal(s.blockedProducerCount, 1);
    s.shutdown();
    assert.equal(await pending, false);
  });
});

// ===========================================================================
// 3b. TelemetryBatchSender runtime reconfiguration (loss-safe flush-before-swap)
// ===========================================================================

describe('TelemetryBatchSender reconfigure', () => {
  it('flushes buffered events through onBatch BEFORE applying new params (no loss)', () => {
    const batches: TelemetryBatch[] = [];
    const s = new TelemetryBatchSender({ sessionId: 's', maxBatchSize: 100, onBatch: b => batches.push(b) });
    s.enqueue('a', {});
    s.enqueue('b', {}); // buffered (below maxBatchSize), not yet emitted
    assert.equal(s.queueSize, 2);
    s.reconfigure({ maxBatchSize: 1 });
    // The 2 buffered events were force-flushed before the swap — none lost.
    const flushed = batches.flatMap(b => b.events.map(e => e.type));
    assert.deepEqual(flushed.sort(), ['a', 'b']);
    assert.equal(s.queueSize, 0);
    assert.equal(s.reconfigureCount, 1);
    // New maxBatchSize=1 takes effect: a single enqueue now emits immediately.
    s.enqueue('c', {});
    assert.equal(batches.flatMap(b => b.events.map(e => e.type)).includes('c'), true);
  });

  it('is a no-op after shutdown', () => {
    const s = new TelemetryBatchSender({ sessionId: 's' });
    s.shutdown();
    s.reconfigure({ maxBatchSize: 5 });
    assert.equal(s.reconfigureCount, 0);
  });
});

// ===========================================================================
// 2+1 end-to-end: AnalyticsTracker exports metrics + traces through flushOtlp
// ===========================================================================

describe('AnalyticsTracker.exportMetrics / exportTrace', () => {
  it('exportMetrics POSTs an OTLP metrics payload to /v1/metrics', async () => {
    const { fn, calls } = stubFetch([200]);
    const t = new AnalyticsTracker(undefined, { otlpEndpoint: 'http://c:4318', otlpFetch: fn, env: egressEnv() });
    t.setSessionId('sess');
    assert.equal(await t.exportMetrics({ counters: { total: 2 } }), true);
    assert.equal(calls[0].url, 'http://c:4318/v1/metrics');
    assert.ok((calls[0].body as Record<string, unknown>).resourceMetrics);
  });

  it('exportTrace POSTs an OTLP traces payload; empty spans short-circuit', async () => {
    const { fn, calls } = stubFetch([200]);
    const t = new AnalyticsTracker(undefined, { otlpEndpoint: 'http://c:4318', otlpFetch: fn, env: egressEnv() });
    const p = new Profiler();
    p.endSpan(p.beginSpan('tool', 'ami.tool'));
    assert.equal(await t.exportTrace(p.getSpanTree()), true);
    assert.equal(calls[0].url, 'http://c:4318/v1/traces');
    assert.equal(await t.exportTrace([]), false); // no spans → no POST
    assert.equal(calls.length, 1);
  });

  it('respects a dedicated per-signal metrics endpoint (verbatim)', async () => {
    const { fn, calls } = stubFetch([200]);
    const t = new AnalyticsTracker(undefined, {
      otlpEndpoint: 'http://c:4318', otlpFetch: fn,
      env: egressEnv({ OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: 'http://metrics-collector/ingest' }),
    });
    await t.exportMetrics({ gauges: { g: 1 } });
    assert.equal(calls[0].url, 'http://metrics-collector/ingest');
  });

  it('is guarded: no endpoint, after shutdown, and telemetry-disabled all return false', async () => {
    const none = new AnalyticsTracker(undefined, { env: egressEnv() });
    assert.equal(await none.exportMetrics({ counters: { a: 1 } }), false);

    const { fn } = stubFetch([200]);
    const disabled = new AnalyticsTracker(undefined, { otlpEndpoint: 'http://c', otlpFetch: fn, env: egressEnv({ DISABLE_TELEMETRY: '1' }) });
    assert.equal(await disabled.exportMetrics({ counters: { a: 1 } }), false);

    const t = new AnalyticsTracker(undefined, { otlpEndpoint: 'http://c', otlpFetch: fn, env: egressEnv() });
    await t.shutdown();
    assert.equal(await t.exportMetrics({ counters: { a: 1 } }), false);
  });

  it('flushOtlp exports metrics + traces end-to-end when sources are wired', async () => {
    const { fn, calls } = stubFetch([200, 200]);
    const p = new Profiler();
    p.incrementCounter('turns', 1);
    p.endSpan(p.beginSpan('interaction', 'ami.interaction'));
    const t = new AnalyticsTracker(undefined, {
      otlpEndpoint: 'http://c:4318', otlpFetch: fn, env: egressEnv(),
      metricsSource: () => ({ gauges: p.getAll() }),
      traceSource: () => p.getSpanTree(),
    });
    t.setSessionId('s');
    await t.flushOtlp();
    const urls = calls.map(c => c.url).sort();
    assert.deepEqual(urls, ['http://c:4318/v1/metrics', 'http://c:4318/v1/traces']);
  });
});

describe('AnalyticsTracker.reconfigureTelemetry', () => {
  it('forwards to a live sender and counts reconfigurations', async () => {
    const { fn } = stubFetch([200]);
    const t = new AnalyticsTracker(undefined, { otlpEndpoint: 'http://c', otlpFetch: fn, env: egressEnv() });
    t.setSessionId('s');
    t.log('seed', {}); // lazily create the sender
    t.reconfigureTelemetry({ maxBatchSize: 5, flushIntervalMs: 1000 });
    assert.equal(t.getTelemetryReconfigureCount(), 1);
    assert.equal(t.getExportIntervalMs(), 1000, 'tracker cadence kept in sync for a future sender');
    await t.shutdown();
  });

  it('is safe to call before any sender exists', () => {
    const t = new AnalyticsTracker(undefined, { env: egressEnv() });
    t.reconfigureTelemetry({ maxBatchSize: 3 });
    assert.equal(t.getTelemetryReconfigureCount(), 1);
  });
});
