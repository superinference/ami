/**
 * analytics-otel-config — OTEL exporter/protocol/interval config knobs +
 * telemetry lifecycle hardening added for the reference parity:
 *  - parseExporterTypes (OTEL spec 'none' sentinel, comma lists)
 *  - resolveOtlpProtocol (per-signal + generic OTEL_EXPORTER_OTLP_PROTOCOL, throws on unknown)
 *  - isTelemetrySuppressedForTest (NODE_ENV=test default + AMI override)
 *  - resolveExportIntervalMs (per-signal export cadence knobs + defaults)
 *  - resolveFlushTimeoutMs + formatFlushTimeoutGuidance (bounded shutdown flush)
 *  - AnalyticsTracker: console exporter, exporter 'none'/unsupported-protocol disable,
 *    NODE_ENV=test egress suppression, warn-once no-sink diagnostic, bounded flush
 *    with timeout-race + remediation guidance, post-shutdown export guard.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  parseExporterTypes,
  resolveOtlpProtocol,
  isTelemetrySuppressedForTest,
  resolveExportIntervalMs,
  DEFAULT_EXPORT_INTERVALS,
  resolveFlushTimeoutMs,
  DEFAULT_FLUSH_TIMEOUT_MS,
  formatFlushTimeoutGuidance,
  AnalyticsTracker,
  TelemetryBatchSender,
} from '../src/analytics';

describe('parseExporterTypes', () => {
  it('returns [] for unset/empty (caller supplies a default)', () => {
    assert.deepEqual(parseExporterTypes(undefined), []);
    assert.deepEqual(parseExporterTypes(''), []);
    assert.deepEqual(parseExporterTypes('   '), []);
  });
  it('parses a comma-separated list, trimming blanks', () => {
    assert.deepEqual(parseExporterTypes(' console , otlp '), ['console', 'otlp']);
  });
  it("filters the OTEL-spec 'none' sentinel (none alone disables)", () => {
    assert.deepEqual(parseExporterTypes('none'), []);
    assert.deepEqual(parseExporterTypes('otlp,none'), ['otlp']);
  });
});

describe('resolveOtlpProtocol', () => {
  it('defaults to http/json when unset', () => {
    assert.equal(resolveOtlpProtocol({}, 'logs'), 'http/json');
  });
  it('reads the generic OTEL_EXPORTER_OTLP_PROTOCOL', () => {
    assert.equal(resolveOtlpProtocol({ OTEL_EXPORTER_OTLP_PROTOCOL: 'grpc' }, 'logs'), 'grpc');
  });
  it('per-signal protocol overrides the generic one', () => {
    assert.equal(
      resolveOtlpProtocol(
        { OTEL_EXPORTER_OTLP_LOGS_PROTOCOL: 'http/protobuf', OTEL_EXPORTER_OTLP_PROTOCOL: 'grpc' },
        'logs',
      ),
      'http/protobuf',
    );
  });
  it('normalizes case/whitespace', () => {
    assert.equal(resolveOtlpProtocol({ OTEL_EXPORTER_OTLP_PROTOCOL: ' HTTP/JSON ' }, 'logs'), 'http/json');
  });
  it('throws on an unknown protocol', () => {
    assert.throws(() => resolveOtlpProtocol({ OTEL_EXPORTER_OTLP_PROTOCOL: 'carrier-pigeon' }, 'logs'), /Unknown OTLP protocol/);
  });
});

describe('isTelemetrySuppressedForTest', () => {
  it('suppresses when NODE_ENV=test', () => {
    assert.equal(isTelemetrySuppressedForTest({ NODE_ENV: 'test' }), true);
  });
  it('override opts back in', () => {
    assert.equal(isTelemetrySuppressedForTest({ NODE_ENV: 'test', AMI_TELEMETRY_ALLOW_IN_TEST: '1' }), false);
  });
  it('is false outside a test env', () => {
    assert.equal(isTelemetrySuppressedForTest({ NODE_ENV: 'production' }), false);
    assert.equal(isTelemetrySuppressedForTest({}), false);
  });
});

describe('resolveExportIntervalMs', () => {
  it('uses the per-signal defaults', () => {
    assert.equal(resolveExportIntervalMs('logs', {}), DEFAULT_EXPORT_INTERVALS.logs);
    assert.equal(resolveExportIntervalMs('metrics', {}), DEFAULT_EXPORT_INTERVALS.metrics);
    assert.equal(resolveExportIntervalMs('traces', {}), DEFAULT_EXPORT_INTERVALS.traces);
    assert.equal(DEFAULT_EXPORT_INTERVALS.logs, 5000);
    assert.equal(DEFAULT_EXPORT_INTERVALS.metrics, 60000);
  });
  it('reads the per-signal env knob', () => {
    assert.equal(resolveExportIntervalMs('logs', { OTEL_LOGS_EXPORT_INTERVAL: '1234' }), 1234);
    assert.equal(resolveExportIntervalMs('metrics', { OTEL_METRIC_EXPORT_INTERVAL: '7000' }), 7000);
  });
  it('falls back to the default for a non-positive / unparseable value', () => {
    assert.equal(resolveExportIntervalMs('logs', { OTEL_LOGS_EXPORT_INTERVAL: 'abc' }), 5000);
    assert.equal(resolveExportIntervalMs('logs', { OTEL_LOGS_EXPORT_INTERVAL: '0' }), 5000);
    assert.equal(resolveExportIntervalMs('logs', { OTEL_LOGS_EXPORT_INTERVAL: '-5' }), 5000);
  });
});

describe('resolveFlushTimeoutMs + formatFlushTimeoutGuidance', () => {
  it('defaults and reads the env knob', () => {
    assert.equal(resolveFlushTimeoutMs({}), DEFAULT_FLUSH_TIMEOUT_MS);
    assert.equal(DEFAULT_FLUSH_TIMEOUT_MS, 5000);
    assert.equal(resolveFlushTimeoutMs({ AMI_OTEL_FLUSH_TIMEOUT_MS: '2000' }), 2000);
  });
  it('falls back to the default for a bad value (0 allowed)', () => {
    assert.equal(resolveFlushTimeoutMs({ AMI_OTEL_FLUSH_TIMEOUT_MS: 'nope' }), 5000);
    assert.equal(resolveFlushTimeoutMs({ AMI_OTEL_FLUSH_TIMEOUT_MS: '0' }), 0);
  });
  it('guidance names the env knob and gives numbered remediation steps', () => {
    const g = formatFlushTimeoutGuidance(2000);
    assert.match(g, /2000/);
    assert.match(g, /AMI_OTEL_FLUSH_TIMEOUT_MS/);
    assert.match(g, /1\./);
    assert.match(g, /2\./);
    assert.match(g, /3\./);
    assert.match(g, /disable/i);
  });
});

function okFetch(counter: { n: number }): typeof fetch {
  return (async () => { counter.n++; return { ok: true, status: 200 } as Response; }) as unknown as typeof fetch;
}

describe('AnalyticsTracker — NODE_ENV=test egress suppression', () => {
  it('does not POST when NODE_ENV=test', async () => {
    const c = { n: 0 };
    const t = new AnalyticsTracker(undefined, {
      env: { NODE_ENV: 'test' }, otlpEndpoint: 'http://h/v1/logs', otlpFetch: okFetch(c),
    });
    t.log('e', {});
    await t.flushOtlp();
    assert.equal(c.n, 0);
  });
  it('override env opts back into egress under test', async () => {
    const c = { n: 0 };
    const t = new AnalyticsTracker(undefined, {
      env: { NODE_ENV: 'test', AMI_TELEMETRY_ALLOW_IN_TEST: '1' },
      otlpEndpoint: 'http://h/v1/logs', otlpFetch: okFetch(c),
    });
    t.log('e', {});
    await t.flushOtlp();
    assert.equal(c.n, 1);
  });
});

describe('AnalyticsTracker — exporter-type knob', () => {
  it("OTEL_LOGS_EXPORTER=none disables OTLP export", async () => {
    const c = { n: 0 };
    const t = new AnalyticsTracker(undefined, {
      env: { OTEL_LOGS_EXPORTER: 'none' }, otlpEndpoint: 'http://h/v1/logs', otlpFetch: okFetch(c),
    });
    t.log('e', {});
    await t.flushOtlp();
    assert.equal(c.n, 0);
  });
  it('console exporter dumps each event to the console sink (no endpoint needed)', () => {
    const lines: string[] = [];
    const t = new AnalyticsTracker(undefined, {
      env: { OTEL_LOGS_EXPORTER: 'console' }, consoleSink: (s) => lines.push(s),
    });
    t.log('my_event', { a: 1 });
    assert.equal(lines.length, 1);
    assert.match(lines[0], /my_event/);
  });
  it('console sink stays silent under NODE_ENV=test', () => {
    const lines: string[] = [];
    const t = new AnalyticsTracker(undefined, {
      env: { OTEL_LOGS_EXPORTER: 'console', NODE_ENV: 'test' }, consoleSink: (s) => lines.push(s),
    });
    t.log('my_event', {});
    assert.equal(lines.length, 0);
  });
});

describe('AnalyticsTracker — unsupported OTLP protocol disables export with a diagnostic', () => {
  it('grpc protocol → no POST + warn-once diagnostic', async () => {
    const c = { n: 0 };
    const diags: string[] = [];
    const t = new AnalyticsTracker(undefined, {
      env: { OTEL_EXPORTER_OTLP_PROTOCOL: 'grpc' },
      otlpEndpoint: 'http://h/v1/logs', otlpFetch: okFetch(c), onDiagnostic: (m) => diags.push(m),
    });
    t.log('e', {});
    await t.flushOtlp();
    assert.equal(c.n, 0);
    assert.ok(diags.some(d => /protocol/i.test(d) && /grpc/.test(d)));
  });
  it('an unknown protocol value also disables export (does not crash the constructor)', async () => {
    const c = { n: 0 };
    const t = new AnalyticsTracker(undefined, {
      env: { OTEL_EXPORTER_OTLP_PROTOCOL: 'bogus' },
      otlpEndpoint: 'http://h/v1/logs', otlpFetch: okFetch(c),
    });
    t.log('e', {});
    await t.flushOtlp();
    assert.equal(c.n, 0);
  });
});

describe('AnalyticsTracker — warn-once no-sink diagnostic', () => {
  it('warns exactly once when events have nowhere to go', () => {
    const diags: string[] = [];
    const t = new AnalyticsTracker(undefined, { env: {}, onDiagnostic: (m) => diags.push(m) });
    t.log('a', {});
    t.log('b', {});
    const sinkWarnings = diags.filter(d => /no .*sink|dropped/i.test(d));
    assert.equal(sinkWarnings.length, 1);
  });
});

describe('AnalyticsTracker — export interval knob consumed', () => {
  it('resolves the logs export interval from the env knob', () => {
    const t = new AnalyticsTracker(undefined, { env: { OTEL_LOGS_EXPORT_INTERVAL: '1234' } });
    assert.equal(t.getExportIntervalMs(), 1234);
  });
  it('falls back to the default', () => {
    const t = new AnalyticsTracker(undefined, { env: {} });
    assert.equal(t.getExportIntervalMs(), 5000);
  });
});

describe('AnalyticsTracker — bounded flush timeout race', () => {
  it('a zero budget requeues everything and emits remediation guidance (no POST)', async () => {
    const c = { n: 0 };
    const diags: string[] = [];
    const env: NodeJS.ProcessEnv = { AMI_OTEL_FLUSH_TIMEOUT_MS: '0' };
    const t = new AnalyticsTracker(undefined, {
      env, otlpEndpoint: 'http://h/v1/logs', otlpFetch: okFetch(c), onDiagnostic: (m) => diags.push(m),
    });
    t.log('e', {});
    await t.flushOtlp();
    assert.equal(c.n, 0, 'nothing sent under a zero flush budget');
    assert.ok(diags.some(d => /timeout/i.test(d) && /AMI_OTEL_FLUSH_TIMEOUT_MS/.test(d)));
    // Raise the budget and re-flush: the requeued event now ships (at-least-once).
    env.AMI_OTEL_FLUSH_TIMEOUT_MS = '5000';
    await t.flushOtlp();
    assert.equal(c.n, 1);
  });
  it('the normal path still sends within the default budget', async () => {
    const c = { n: 0 };
    const t = new AnalyticsTracker(undefined, {
      env: {}, otlpEndpoint: 'http://h/v1/logs', otlpFetch: okFetch(c),
    });
    t.log('e', {});
    await t.flushOtlp();
    assert.equal(c.n, 1);
  });
});

describe('AnalyticsTracker — post-shutdown export guard', () => {
  it('shutdown() flushes once, then rejects further egress', async () => {
    const c = { n: 0 };
    const t = new AnalyticsTracker(undefined, {
      env: {}, otlpEndpoint: 'http://h/v1/logs', otlpFetch: okFetch(c),
    });
    t.log('e1', {});
    await t.shutdown();
    assert.equal(c.n, 1, 'final flush shipped the queued event');
    t.log('e2', {});
    await t.flushOtlp();
    assert.equal(c.n, 1, 'no egress after shutdown');
    assert.equal(t.isShutdown(), true);
  });
});

describe('TelemetryBatchSender — post-shutdown guard', () => {
  it('rejects enqueue() and flush() after shutdown()', () => {
    const s = new TelemetryBatchSender({ sessionId: 's' });
    assert.equal(s.enqueue('a', {}), true);
    s.shutdown();
    assert.equal(s.enqueue('b', {}), false);
    assert.equal(s.flush(), null);
    assert.equal(s.isShutdown, true);
  });
});
