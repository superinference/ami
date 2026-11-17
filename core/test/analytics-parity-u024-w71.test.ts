import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  AnalyticsTracker,
  TelemetryBatchSender,
  FailedBatchStore,
  CLOSE_GRACE_MS,
  DEFAULT_SHUTDOWN_TIMEOUT_MS,
  resolveShutdownTimeoutMs,
  stripLocalOnlyFields,
  resolveLocalOnlyFields,
  detectWslForAnalytics,
  getEnvironmentFingerprint,
  buildTelemetryResourceAttrs,
  type TelemetryBatch,
} from '../src/analytics';

// Env without NODE_ENV='test' so off-machine egress is NOT suppressed.
const liveEnv = (extra: Record<string, string> = {}): NodeJS.ProcessEnv =>
  ({ ...extra } as NodeJS.ProcessEnv);

const okFetch = (counter: { n: number }, capture?: Array<Record<string, unknown>>): typeof fetch =>
  (async (_url: string, init?: { body?: string }) => {
    counter.n++;
    if (capture && init?.body) capture.push(JSON.parse(init.body));
    return { ok: true, status: 200 } as Response;
  }) as unknown as typeof fetch;

function readJsonl(file: string): Array<Record<string, unknown>> {
  if (!fs.existsSync(file)) return [];
  return fs.readFileSync(file, 'utf8').trim().split('\n').filter(Boolean).map(l => JSON.parse(l));
}

function mkTmp(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'analytics-u024-w71-'));
}

// ---------------------------------------------------------------------------
// F1 — AsyncLocalStorage-backed invocation source (workloadContext parity)
// ---------------------------------------------------------------------------
describe('AnalyticsTracker.runWithSource (ALS source context)', () => {
  it('resolves currentSource() for the whole async chain, across awaits', async () => {
    const t = new AnalyticsTracker();
    const seen = await t.runWithSource('agent', async () => {
      await new Promise(r => setImmediate(r));
      return t.currentSource();
    });
    assert.equal(seen, 'agent');
    // Outside any boundary → undefined (no leak).
    assert.equal(t.currentSource(), undefined);
  });

  it('isolates concurrent/interleaved chains (a global slot would cross-contaminate)', async () => {
    const t = new AnalyticsTracker();
    const a = t.runWithSource('srcA', async () => {
      await new Promise(r => setImmediate(r));
      await new Promise(r => setImmediate(r));
      return t.currentSource();
    });
    const b = t.runWithSource('srcB', async () => {
      await new Promise(r => setImmediate(r));
      return t.currentSource();
    });
    assert.deepEqual(await Promise.all([a, b]), ['srcA', 'srcB']);
  });

  it('gates log() via setAllowedSources using the ALS source across an await', async () => {
    const t = new AnalyticsTracker();
    t.setAllowedSources(['cron']);
    await t.runWithSource('cron', async () => {
      await new Promise(r => setImmediate(r));
      t.log('kept');
    });
    // A disallowed (or absent) source is dropped.
    await t.runWithSource('other', async () => { t.log('dropped'); });
    t.log('also_dropped');
    assert.equal(t.getEventsByType('kept').length, 1);
    assert.equal(t.getEventsByType('dropped').length, 0);
    assert.equal(t.getEventsByType('also_dropped').length, 0);
  });

  it('a fresh runWithSource(undefined) boundary shadows a leaked stack source', () => {
    const t = new AnalyticsTracker();
    t.pushSource('leaked');
    assert.equal(t.currentSource(), 'leaked');
    const inner = t.runWithSource(undefined, () => t.currentSource());
    assert.equal(inner, undefined, 'ALS boundary wins even when its source is undefined');
    // Stack still resolves outside the boundary.
    assert.equal(t.currentSource(), 'leaked');
  });
});

// ---------------------------------------------------------------------------
// F2 — immediateMode batching bypass (bufferedWriter parity)
// ---------------------------------------------------------------------------
describe('TelemetryBatchSender immediateMode', () => {
  it('flushes one batch per enqueue when immediateMode is on', () => {
    const batches: TelemetryBatch[] = [];
    const s = new TelemetryBatchSender({
      sessionId: 's', maxBatchSize: 100, immediateMode: true,
      onBatch: (b) => batches.push(b),
    });
    s.enqueue('a', { x: 1 });
    s.enqueue('b', { x: 2 });
    assert.equal(batches.length, 2, 'each event flushed immediately');
    assert.equal(batches[0].events.length, 1);
  });

  it('without immediateMode a single enqueue stays buffered (discriminator)', () => {
    const batches: TelemetryBatch[] = [];
    const s = new TelemetryBatchSender({
      sessionId: 's', maxBatchSize: 100,
      onBatch: (b) => batches.push(b),
    });
    s.enqueue('a', { x: 1 });
    assert.equal(batches.length, 0, 'buffered until the size boundary');
  });

  it('AnalyticsTracker threads telemetryImmediateMode to the sender', async () => {
    const c = { n: 0 };
    const t = new AnalyticsTracker(undefined, {
      env: liveEnv(), otlpEndpoint: 'http://h/v1/logs', otlpFetch: okFetch(c),
      telemetryMaxBatchSize: 100, telemetryImmediateMode: true,
    });
    t.log('e', {});
    await t.whenSent();
    assert.equal(c.n, 1, 'immediate mode POSTed the single event mid-session');
  });

  it('AMI_OTEL_IMMEDIATE_FLUSH env enables immediate mode', async () => {
    const c = { n: 0 };
    const t = new AnalyticsTracker(undefined, {
      env: liveEnv({ AMI_OTEL_IMMEDIATE_FLUSH: '1' }),
      otlpEndpoint: 'http://h/v1/logs', otlpFetch: okFetch(c), telemetryMaxBatchSize: 100,
    });
    t.log('e', {});
    await t.whenSent();
    assert.equal(c.n, 1);
  });
});

// ---------------------------------------------------------------------------
// F3 — separate shutdown-timeout knob (instrumentation.ts parity)
// ---------------------------------------------------------------------------
describe('resolveShutdownTimeoutMs', () => {
  it('defaults to DEFAULT_SHUTDOWN_TIMEOUT_MS (AMI keeps its grace default)', () => {
    assert.equal(resolveShutdownTimeoutMs({} as NodeJS.ProcessEnv), DEFAULT_SHUTDOWN_TIMEOUT_MS);
    assert.equal(DEFAULT_SHUTDOWN_TIMEOUT_MS, CLOSE_GRACE_MS);
  });
  it('honors AMI_OTEL_SHUTDOWN_TIMEOUT_MS then CLAUDE_CODE_OTEL_SHUTDOWN_TIMEOUT_MS (AMI wins)', () => {
    assert.equal(resolveShutdownTimeoutMs({ AMI_OTEL_SHUTDOWN_TIMEOUT_MS: '1234' } as NodeJS.ProcessEnv), 1234);
    assert.equal(resolveShutdownTimeoutMs({ CLAUDE_CODE_OTEL_SHUTDOWN_TIMEOUT_MS: '50' } as NodeJS.ProcessEnv), 50);
    assert.equal(
      resolveShutdownTimeoutMs({ AMI_OTEL_SHUTDOWN_TIMEOUT_MS: '99', CLAUDE_CODE_OTEL_SHUTDOWN_TIMEOUT_MS: '50' } as NodeJS.ProcessEnv),
      99,
    );
  });
  it('honors a literal 0 and rejects non-finite/negative', () => {
    assert.equal(resolveShutdownTimeoutMs({ AMI_OTEL_SHUTDOWN_TIMEOUT_MS: '0' } as NodeJS.ProcessEnv), 0);
    assert.equal(resolveShutdownTimeoutMs({ AMI_OTEL_SHUTDOWN_TIMEOUT_MS: '-5' } as NodeJS.ProcessEnv), DEFAULT_SHUTDOWN_TIMEOUT_MS);
    assert.equal(resolveShutdownTimeoutMs({ AMI_OTEL_SHUTDOWN_TIMEOUT_MS: 'abc' } as NodeJS.ProcessEnv), DEFAULT_SHUTDOWN_TIMEOUT_MS);
  });
  it('closeWithGrace() bounds the drain by the resolved shutdown knob (not CLOSE_GRACE_MS)', async () => {
    let graceUsed = -1;
    const hangingFetch = (async () => { await new Promise<void>(() => {}); return { ok: true } as Response; }) as unknown as typeof fetch;
    const t = new AnalyticsTracker(undefined, {
      env: liveEnv({ AMI_OTEL_SHUTDOWN_TIMEOUT_MS: '77' }),
      otlpEndpoint: 'http://h/v1/logs', otlpFetch: hangingFetch,
      sleepImpl: async (ms) => { graceUsed = ms; },
    });
    t.log('e', {});
    await t.closeWithGrace();
    assert.equal(graceUsed, 77, 'grace came from the env knob, not the 3000ms default');
    assert.equal(t.isShutdown(), true);
  });
});

// ---------------------------------------------------------------------------
// F4 — WSL version resource attribute (instrumentation.ts parity)
// ---------------------------------------------------------------------------
describe('detectWslForAnalytics', () => {
  it('returns null off-WSL regardless of kernel string', () => {
    assert.equal(detectWslForAnalytics({} as NodeJS.ProcessEnv, '5.15.0-microsoft-standard-WSL2'), null);
  });
  it('classifies WSL2 from the kernel tag when a marker is present', () => {
    assert.equal(detectWslForAnalytics({ WSL_DISTRO_NAME: 'Ubuntu' } as NodeJS.ProcessEnv, '5.15.0-microsoft-standard-WSL2'), '2');
  });
  it('classifies WSL1 from a microsoft kernel without the wsl2 tag', () => {
    assert.equal(detectWslForAnalytics({ WSL_DISTRO_NAME: 'Ubuntu' } as NodeJS.ProcessEnv, '4.4.0-microsoft'), '1');
  });
  it('assumes WSL2 when the marker is present but the kernel is untagged', () => {
    assert.equal(detectWslForAnalytics({ WSL_INTEROP: '/run/x' } as NodeJS.ProcessEnv, '6.1.0-generic'), '2');
  });
  it('getEnvironmentFingerprint adds wsl.version only on WSL', () => {
    const off = getEnvironmentFingerprint({} as NodeJS.ProcessEnv, { kernel: 'x-microsoft-standard-WSL2' });
    assert.equal('wsl.version' in off, false);
    const on = getEnvironmentFingerprint({ WSL_DISTRO_NAME: 'Ubuntu' } as NodeJS.ProcessEnv, { kernel: 'x-microsoft-standard-WSL2' });
    assert.equal(on['wsl.version'], '2');
  });
  it('buildTelemetryResourceAttrs surfaces wsl.version on WSL', () => {
    const attrs = buildTelemetryResourceAttrs('sess', { WSL_DISTRO_NAME: 'Ubuntu' } as NodeJS.ProcessEnv);
    assert.equal(attrs['wsl.version'], '2');
  });
});

// ---------------------------------------------------------------------------
// F6 — differential local-only field redaction (sink.ts / index.ts parity)
// ---------------------------------------------------------------------------
describe('stripLocalOnlyFields / resolveLocalOnlyFields', () => {
  it('returns the SAME reference when nothing matches (copy-on-write)', () => {
    const data = { a: 1, b: 2 };
    assert.equal(stripLocalOnlyFields(data, { names: new Set(['z']) }), data);
  });
  it('clones without matched names and leaves the original intact', () => {
    const data = { a: 1, secret: 2 };
    const out = stripLocalOnlyFields(data, { names: new Set(['secret']) });
    assert.notEqual(out, data);
    assert.deepEqual(out, { a: 1 });
    assert.deepEqual(data, { a: 1, secret: 2 });
  });
  it('strips by prefix', () => {
    assert.deepEqual(stripLocalOnlyFields({ a: 1, _PROTO_x: 2, local_y: 3 }, { prefix: 'local_' }), { a: 1, _PROTO_x: 2 });
  });
  it('resolveLocalOnlyFields returns null when nothing configured', () => {
    assert.equal(resolveLocalOnlyFields({} as NodeJS.ProcessEnv, {}), null);
  });
  it('merges explicit fields with the env list and prefix', () => {
    const m = resolveLocalOnlyFields({ AMI_ANALYTICS_LOCAL_ONLY_FIELDS: 'b, c' } as NodeJS.ProcessEnv, { localOnlyFields: ['a'], localOnlyFieldPrefix: 'p_' });
    assert.ok(m);
    assert.deepEqual([...(m!.names ?? [])].sort(), ['a', 'b', 'c']);
    assert.equal(m!.prefix, 'p_');
  });
});

describe('AnalyticsTracker differential redaction (local vs off-machine)', () => {
  it('keeps local-only fields in the JSONL but strips them from the OTLP export', async () => {
    const dir = mkTmp();
    try {
      const c = { n: 0 };
      const captured: Array<Record<string, unknown>> = [];
      const t = new AnalyticsTracker(dir, {
        env: liveEnv(), otlpEndpoint: 'http://h/v1/logs', otlpFetch: okFetch(c, captured),
        localOnlyFields: ['secretlocal'],
      });
      t.log('e', { a: 1, secretlocal: 'xyz' });
      await t.flushOtlp();
      // JSONL (privileged/local) keeps the full payload.
      const lines = readJsonl(path.join(dir, 'analytics.jsonl'));
      assert.equal(lines.length, 1);
      assert.equal((lines[0].data as Record<string, unknown>).secretlocal, 'xyz');
      // OTLP (general-access/off-machine) copy has it stripped.
      assert.equal(c.n, 1);
      const body = JSON.stringify(captured);
      assert.equal(body.includes('secretlocal'), false, 'local-only field must not leave the machine');
      assert.equal(body.includes('"a"'), true, 'non-local fields still exported');
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// F5 — health-probe immediate drain of persisted failed batches + re-entrancy guard
// ---------------------------------------------------------------------------
describe('AnalyticsTracker persisted-batch recovery (health-probe drain)', () => {
  it('a successful mid-session send immediately drains a pre-spooled failed batch', async () => {
    const dir = mkTmp();
    try {
      const failedDir = path.join(dir, 'telemetry-failed');
      // Pre-seed a spooled failed batch (as a prior session would have).
      const store = new FailedBatchStore(failedDir);
      const seeded = store.spool([{ type: 'old', timestamp: Date.now(), data: { k: 1 } }]);
      assert.ok(seeded, 'spool wrote a file');
      assert.equal(store.list().length, 1);

      const c = { n: 0 };
      const t = new AnalyticsTracker(dir, {
        env: liveEnv(), otlpEndpoint: 'http://h/v1/logs', otlpFetch: okFetch(c),
        telemetryMaxBatchSize: 1, // every log emits a boundary batch -> mid-session send
        persistFailedBatches: true, failedBatchDir: failedDir,
      });
      t.log('live', { a: 1 });
      await t.whenSent();
      // The live send succeeded AND the pre-spooled file was re-sent and deleted.
      assert.equal(store.list().length, 0, 'spooled batch recovered after the healthy send');
      assert.ok(c.n >= 2, 'both the live batch and the recovered batch were POSTed');
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('recoverPersistedBatches is re-entrancy guarded (only one concurrent drain runs)', async () => {
    const dir = mkTmp();
    try {
      const failedDir = path.join(dir, 'telemetry-failed');
      const store = new FailedBatchStore(failedDir);
      store.spool([{ type: 'old', timestamp: Date.now(), data: { k: 1 } }]);
      const c = { n: 0 };
      const t = new AnalyticsTracker(dir, {
        env: liveEnv(), otlpEndpoint: 'http://h/v1/logs', otlpFetch: okFetch(c),
        persistFailedBatches: true, failedBatchDir: failedDir,
      });
      const [r1, r2] = await Promise.all([t.recoverPersistedBatches(), t.recoverPersistedBatches()]);
      assert.equal(r1 + r2, 1, 'exactly one drain recovered the single file; the other short-circuited');
      assert.equal(store.list().length, 0);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});
