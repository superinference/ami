/**
 * U024 (w74) parity: analytics telemetry diagnostics + non-blocking disk sink.
 *
 * Covers two additive capabilities wired into core/src/analytics.ts:
 *  - createTelemetryDiagLogger: a level-filtered diagnostic boundary (Claude's
 *    ClaudeCodeDiagLogger) — error/warn surface to BOTH an error sink and a debug
 *    sink; info/debug/verbose are suppressed; throwing sinks are isolated. Wired so
 *    genuine telemetry failures (OTLP mirror / headers-helper) ALSO reach onError.
 *  - resolveFileFlushIntervalMs + the buffered disk sink: when a positive flush
 *    interval is configured, disk appends route through the existing
 *    createBufferedWriter so a slow fs.appendFileSync never stalls the caller's
 *    tick; the buffer is drained synchronously on shutdown()/closeWithGrace()
 *    (exit-safe). The default (interval 0) keeps the synchronous per-event append.
 */
import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  AnalyticsTracker,
  createTelemetryDiagLogger,
  resolveFileFlushIntervalMs,
} from '../src/analytics';

const tmpDirs: string[] = [];
function mkTmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'analytics-u024-w74-'));
  tmpDirs.push(d);
  return d;
}
after(() => {
  for (const d of tmpDirs) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ }
  }
});

const tick = () => new Promise<void>((r) => setImmediate(r));

describe('createTelemetryDiagLogger — level-filtered diagnostic boundary', () => {
  it('routes error() and warn() to BOTH the error sink and the debug sink', () => {
    const errors: Error[] = [];
    const debugs: string[] = [];
    const log = createTelemetryDiagLogger({
      onError: (e) => errors.push(e),
      onDebug: (m) => debugs.push(m),
    });
    log.error('boom');
    log.warn('careful');
    assert.equal(errors.length, 2, 'error + warn each produce one Error');
    assert.ok(errors[0] instanceof Error && errors[0].message === 'boom');
    assert.ok(errors[1] instanceof Error && errors[1].message === 'careful');
    assert.deepEqual(debugs, ['boom', 'careful'], 'debug sink gets verbatim messages');
  });

  it('suppresses info(), debug() and verbose() entirely', () => {
    const errors: Error[] = [];
    const debugs: string[] = [];
    const log = createTelemetryDiagLogger({
      onError: (e) => errors.push(e),
      onDebug: (m) => debugs.push(m),
    });
    log.info('i');
    log.debug('d');
    log.verbose('v');
    assert.equal(errors.length, 0, 'info/debug/verbose never reach the error sink');
    assert.equal(debugs.length, 0, 'info/debug/verbose never reach the debug sink');
  });

  it('isolates a throwing sink so it never propagates (best-effort)', () => {
    const log = createTelemetryDiagLogger({
      onError: () => { throw new Error('error-sink-threw'); },
      onDebug: () => { throw new Error('debug-sink-threw'); },
    });
    assert.doesNotThrow(() => log.error('x'));
    assert.doesNotThrow(() => log.warn('y'));
  });

  it('works with no deps wired (both sinks optional)', () => {
    const log = createTelemetryDiagLogger();
    assert.doesNotThrow(() => { log.error('x'); log.warn('y'); log.info('z'); });
  });
});

describe('resolveFileFlushIntervalMs', () => {
  it('prefers a positive explicit opt', () => {
    assert.equal(resolveFileFlushIntervalMs({ AMI_ANALYTICS_FILE_FLUSH_MS: '5000' }, 250), 250);
  });
  it('treats a non-positive opt as disabled (0)', () => {
    assert.equal(resolveFileFlushIntervalMs({}, 0), 0);
    assert.equal(resolveFileFlushIntervalMs({}, -10), 0);
  });
  it('reads a positive env value when no opt is given', () => {
    assert.equal(resolveFileFlushIntervalMs({ AMI_ANALYTICS_FILE_FLUSH_MS: '1500' }), 1500);
  });
  it('returns 0 for unset / non-positive / non-numeric env', () => {
    assert.equal(resolveFileFlushIntervalMs({}), 0);
    assert.equal(resolveFileFlushIntervalMs({ AMI_ANALYTICS_FILE_FLUSH_MS: '0' }), 0);
    assert.equal(resolveFileFlushIntervalMs({ AMI_ANALYTICS_FILE_FLUSH_MS: '-3' }), 0);
    assert.equal(resolveFileFlushIntervalMs({ AMI_ANALYTICS_FILE_FLUSH_MS: 'nope' }), 0);
  });
});

describe('AnalyticsTracker — buffered (non-blocking) disk sink', () => {
  it('defaults to a synchronous append (events on disk immediately; interval 0)', () => {
    const dir = mkTmp();
    const t = new AnalyticsTracker(dir, { env: {} });
    assert.equal(t.getFileFlushIntervalMs(), 0, 'default: synchronous append');
    t.log('a', { k: 1 });
    t.log('b', { k: 2 });
    const logPath = path.join(dir, 'analytics.jsonl');
    assert.ok(fs.existsSync(logPath), 'synchronous sink writes immediately');
    assert.equal(fs.readFileSync(logPath, 'utf8').trim().split('\n').length, 2);
  });

  it('buffers appends when a flush interval is configured, draining synchronously on shutdown', async () => {
    const dir = mkTmp();
    // Large idle interval so only the explicit drain (not the timer) flushes.
    const t = new AnalyticsTracker(dir, { env: {}, fileFlushIntervalMs: 60_000 });
    assert.equal(t.getFileFlushIntervalMs(), 60_000);
    t.log('a', { k: 1 });
    t.log('b', { k: 2 });
    const logPath = path.join(dir, 'analytics.jsonl');
    // RED on revert (sync append wiring): the file would already exist here.
    assert.ok(!fs.existsSync(logPath), 'buffered sink defers the write off the caller tick');
    // In-memory sink is unaffected — events are queryable immediately.
    assert.equal(t.getEvents().length, 2);
    await t.shutdown();
    assert.ok(fs.existsSync(logPath), 'shutdown() drains the buffer (exit-safe)');
    assert.equal(fs.readFileSync(logPath, 'utf8').trim().split('\n').length, 2);
  });

  it('closeWithGrace() also drains the buffered disk sink synchronously', async () => {
    const dir = mkTmp();
    const t = new AnalyticsTracker(dir, { env: {}, fileFlushIntervalMs: 60_000 });
    t.log('only', { k: 9 });
    const logPath = path.join(dir, 'analytics.jsonl');
    assert.ok(!fs.existsSync(logPath));
    await t.closeWithGrace(0);
    assert.equal(fs.readFileSync(logPath, 'utf8').trim().split('\n').length, 1);
  });

  it('flushes a size-overflow batch via setImmediate without blocking log()', async () => {
    const dir = mkTmp();
    const t = new AnalyticsTracker(dir, { env: {}, fileFlushIntervalMs: 60_000 });
    const logPath = path.join(dir, 'analytics.jsonl');
    // Default buffer size is 100: the 100th write detaches a deferred overflow.
    for (let i = 0; i < 101; i++) t.log('bulk', { i });
    // The overflow write runs on a macrotask, not in log()'s tick.
    await tick();
    await tick();
    const afterOverflow = fs.readFileSync(logPath, 'utf8').trim().split('\n').length;
    assert.equal(afterOverflow, 100, 'deferred overflow flushed exactly the first batch');
    await t.shutdown();
    assert.equal(fs.readFileSync(logPath, 'utf8').trim().split('\n').length, 101, 'drain writes the remainder');
  });
});

describe('AnalyticsTracker — genuine telemetry failures surface through onError', () => {
  it('OTLP-mirror failure reaches onError AND onDiagnostic (warn level), still swallowed', () => {
    const diags: string[] = [];
    const errors: Error[] = [];
    const t = new AnalyticsTracker(undefined, {
      env: {},
      otlpEndpoint: 'http://x/v1/logs',
      onDiagnostic: (m) => diags.push(m),
      onError: (e) => errors.push(e),
    });
    // Inject a sender whose enqueue throws (corrupt batch pipeline simulation).
    (t as unknown as { _telemetry: unknown })._telemetry = {
      enqueue() { throw new Error('boom'); },
      startAutoFlush() { /* noop */ },
    };
    assert.doesNotThrow(() => t.log('a', {}));
    assert.doesNotThrow(() => t.log('b', {}));
    // Warn routes to BOTH sinks, once (latched), without crashing log().
    assert.equal(diags.filter(d => d.includes('OTLP mirror failed')).length, 1);
    assert.equal(errors.length, 1, 'error sink receives the failure exactly once');
    assert.ok(errors[0] instanceof Error && errors[0].message.includes('OTLP mirror failed'));
    assert.equal(t.getEvents().length, 2, 'in-memory sink unaffected');
  });

  it('headers-helper failure reaches onError AND onDiagnostic during export', async () => {
    const diags: string[] = [];
    const errors: Error[] = [];
    const fakeFetch = (async () => ({ ok: true, status: 200 } as Response)) as unknown as typeof fetch;
    const t = new AnalyticsTracker(undefined, {
      env: {},
      otlpEndpoint: 'http://h/v1/logs',
      otlpFetch: fakeFetch,
      otlpHeaders: { 'x-static': 'a' },
      otlpHeadersHelper: { command: 'c', exec: () => { throw new Error('boom'); } },
      onDiagnostic: (m) => diags.push(m),
      onError: (e) => errors.push(e),
    });
    t.log('e', {});
    await t.flushOtlp();
    assert.ok(diags.some(d => d.includes('otelHeadersHelper failed')));
    assert.ok(errors.some(e => e.message.includes('otelHeadersHelper failed')));
  });

  it('a failure with no onError wired is still swallowed (error sink optional)', () => {
    const diags: string[] = [];
    const t = new AnalyticsTracker(undefined, {
      env: {},
      otlpEndpoint: 'http://x/v1/logs',
      onDiagnostic: (m) => diags.push(m),
    });
    (t as unknown as { _telemetry: unknown })._telemetry = {
      enqueue() { throw new Error('boom'); },
      startAutoFlush() { /* noop */ },
    };
    assert.doesNotThrow(() => t.log('a', {}));
    assert.equal(diags.filter(d => d.includes('OTLP mirror failed')).length, 1);
  });
});
