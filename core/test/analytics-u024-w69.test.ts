/**
 * analytics-u024-w69 — additive analytics parity (unit U024, wave w69). Each block
 * targets one confirmed gap against core/core/src/analytics.ts:
 *  - generateToolSummary truncation indicator ('…' + '(+N more lines)')         [truncate.ts]
 *  - privacy-preserving file-path/content hashing (hashFilePath/hashFileContent)  [fileOperationAnalytics.ts]
 *  - per-sink killswitch (memory/file/otlp + env knobs)                           [sinkKillswitch.ts / sink.ts]
 *  - exception isolation of the OTLP mirror path in log()                         [sink.ts]
 *  - per-event process metrics with CPU% delta (ProcessMetricsSampler)           [metadata.ts]
 *  - durable failed-batch disk spool with cross-run retry (FailedBatchStore)      [firstPartyEventLoggingExporter.ts]
 *
 * RED/GREEN: each block fails if its feature is reverted (noted inline).
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  AnalyticsTracker, generateToolSummary, TOOL_SUMMARY_LINE_MAX,
  sanitizeAnalyticsData, hashFilePath, hashFileContent, FILE_PATH_HASH_LEN, MAX_CONTENT_HASH_SIZE,
  resolveKilledSinks, SINK_NAMES,
  ProcessMetricsSampler, FailedBatchStore, FAILED_BATCH_FILE_PREFIX,
  type TelemetryBatch,
} from '../src/analytics';

const tmpDirs: string[] = [];
function mkTmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u024-'));
  tmpDirs.push(d);
  return d;
}
afterEach(() => {
  while (tmpDirs.length) {
    try { fs.rmSync(tmpDirs.pop()!, { recursive: true, force: true }); } catch { /* ignore */ }
  }
});

const okFetch = (counter: { n: number }) =>
  (async () => { counter.n++; return { ok: true, status: 200 } as Response; }) as unknown as typeof fetch;
const throwFetch = (counter?: { n: number }) =>
  (async () => { if (counter) counter.n++; throw new Error('network down'); }) as unknown as typeof fetch;
const status4xx = (counter?: { n: number }) =>
  (async () => { if (counter) counter.n++; return { ok: false, status: 400, headers: { get: () => null } } as unknown as Response; }) as unknown as typeof fetch;
const noSleep = async () => {};

// ---------------------------------------------------------------------------
// generateToolSummary truncation indicator
// ---------------------------------------------------------------------------
describe('generateToolSummary truncation indicator', () => {
  it('appends … when the first line exceeds the cap (distinguishes clipped from exact)', () => {
    // RED on revert: without the '…' marker an exactly-100-char line and a clipped
    // 100-char line are indistinguishable.
    const exact = 'Y'.repeat(TOOL_SUMMARY_LINE_MAX);
    const long = 'X'.repeat(TOOL_SUMMARY_LINE_MAX + 50);
    const out = generateToolSummary([
      { toolName: 'a', output: exact, isError: false },
      { toolName: 'b', output: long, isError: false },
    ]).split('\n');
    assert.ok(!out[0].endsWith('…'), 'exact-length line keeps no marker');
    assert.ok(out[1].endsWith('…'), 'clipped line gets the … marker');
    assert.ok(!out[1].includes('X'.repeat(TOOL_SUMMARY_LINE_MAX + 1)));
  });

  it('appends a (+N more lines) marker for multi-line output', () => {
    const out = generateToolSummary([
      { toolName: 't', output: 'first\nsecond\nthird', isError: true },
    ]);
    assert.ok(out.includes('t: ERROR — first'));
    assert.ok(out.includes('(+2 more lines)'));
    assert.ok(!out.includes('second'));
  });

  it('leaves a single short line untouched', () => {
    assert.equal(generateToolSummary([{ toolName: 'x', output: 'done', isError: false }]), 'x: OK — done');
  });
});

// ---------------------------------------------------------------------------
// file-path / content hashing
// ---------------------------------------------------------------------------
describe('hashFilePath / hashFileContent', () => {
  it('produces a stable truncated path hash and a full content hash', () => {
    const p = '/home/alice/project/secret.ts';
    assert.equal(hashFilePath(p).length, FILE_PATH_HASH_LEN);
    assert.equal(hashFilePath(p), hashFilePath(p)); // stable
    assert.notEqual(hashFilePath(p), hashFilePath('/other'));
    assert.equal(hashFileContent('body')!.length, 64);
  });

  it('refuses to hash content above the size guard', () => {
    assert.equal(hashFileContent('x'.repeat(MAX_CONTENT_HASH_SIZE + 1)), undefined);
    assert.equal(typeof hashFileContent('x'.repeat(MAX_CONTENT_HASH_SIZE)), 'string');
  });

  it('replaces raw path fields with a hash on the egress copy', () => {
    // RED on revert: without PATH_KEYS hashing the raw path (with the username /
    // project name) survives on the exported copy.
    const raw = '/home/alice/private-project/x.ts';
    const out = sanitizeAnalyticsData({ file_path: raw, path: '/etc/app', keep: 'ok' }, { env: {} });
    assert.equal(out.file_path, undefined);
    assert.equal(out.file_path_hash, hashFilePath(raw));
    assert.equal(out.path_hash, hashFilePath('/etc/app'));
    assert.equal(out.keep, 'ok');
    assert.ok(!JSON.stringify(out).includes('alice'));
  });

  it('keeps raw paths when user-prompt logging is explicitly opted in', () => {
    const out = sanitizeAnalyticsData({ file_path: '/a/b.ts' }, { env: {}, logUserPrompts: true });
    assert.equal(out.file_path, '/a/b.ts');
    assert.equal(out.file_path_hash, undefined);
  });
});

// ---------------------------------------------------------------------------
// per-sink killswitch
// ---------------------------------------------------------------------------
describe('resolveKilledSinks', () => {
  it('fails open with no config', () => {
    assert.deepEqual(resolveKilledSinks({}, {}), {});
  });
  it('AMI_NO_ANALYTICS kills every sink', () => {
    const r = resolveKilledSinks({ AMI_NO_ANALYTICS: '1' }, {});
    for (const s of SINK_NAMES) assert.equal(r[s], true);
  });
  it('SUPERINFERENCE_DISABLE_SINKS kills a named subset and ignores junk', () => {
    const r = resolveKilledSinks({ SUPERINFERENCE_DISABLE_SINKS: 'otlp, bogus ,file' }, {});
    assert.equal(r.otlp, true);
    assert.equal(r.file, true);
    assert.equal(r.memory, undefined);
  });
  it('explicit map overrides and merges', () => {
    const r = resolveKilledSinks({}, { memory: true });
    assert.equal(r.memory, true);
  });
});

describe('AnalyticsTracker per-sink killswitch', () => {
  it('memory sink off: events are not retained but egress still happens', () => {
    const dir = mkTmp();
    const t = new AnalyticsTracker(dir, { env: {}, killedSinks: { memory: true } });
    t.log('e', { a: 1 });
    assert.equal(t.getEvents().length, 0, 'nothing retained in memory');
    const disk = fs.readFileSync(path.join(dir, 'analytics.jsonl'), 'utf8');
    assert.ok(disk.includes('"type":"e"'), 'file sink still wrote');
  });

  it('file sink off: in-memory retained, nothing written to disk', () => {
    const dir = mkTmp();
    const t = new AnalyticsTracker(dir, { env: {}, killedSinks: { file: true } });
    t.log('e', { a: 1 });
    assert.equal(t.getEvents().length, 1);
    assert.ok(!fs.existsSync(path.join(dir, 'analytics.jsonl')));
  });

  it('otlp sink off: no network traffic even with an endpoint, re-enables on demand', async () => {
    // RED on revert: without the killswitch the OTLP mirror POSTs regardless.
    const counter = { n: 0 };
    const t = new AnalyticsTracker(undefined, {
      env: {}, otlpEndpoint: 'http://x/v1/logs', otlpFetch: okFetch(counter),
      killedSinks: { otlp: true }, telemetryMaxBatchSize: 1,
    });
    t.log('a', {});
    await t.whenSent();
    await t.flushOtlp();
    assert.equal(counter.n, 0, 'killed otlp sends nothing');
    t.setKilledSinks({ otlp: false });
    t.log('b', {});
    await t.whenSent();
    assert.equal(counter.n, 1, 're-enabled otlp resumes sending');
  });
});

// ---------------------------------------------------------------------------
// exception isolation of the OTLP mirror
// ---------------------------------------------------------------------------
describe('AnalyticsTracker OTLP-mirror exception isolation', () => {
  it('swallows a throwing sender enqueue and warns once instead of crashing log()', () => {
    // RED on revert (no try/catch): the throw propagates out of log().
    const diags: string[] = [];
    const t = new AnalyticsTracker(undefined, {
      env: {}, otlpEndpoint: 'http://x/v1/logs', onDiagnostic: (m) => diags.push(m),
    });
    // Inject a sender whose enqueue throws (simulating a corrupt batch pipeline).
    (t as unknown as { _telemetry: unknown })._telemetry = {
      enqueue() { throw new Error('boom'); },
      startAutoFlush() { /* noop */ },
    };
    assert.doesNotThrow(() => t.log('a', {}));
    assert.doesNotThrow(() => t.log('b', {}));
    assert.equal(diags.filter(d => d.includes('OTLP mirror failed')).length, 1, 'warned exactly once');
    assert.equal(t.getEvents().length, 2, 'in-memory sink unaffected');
  });
});

// ---------------------------------------------------------------------------
// process metrics
// ---------------------------------------------------------------------------
describe('ProcessMetricsSampler', () => {
  it('has no cpuPercent on the first sample, then derives it from the delta', () => {
    let cpu = { user: 0, system: 0 };
    let t = 1000;
    const s = new ProcessMetricsSampler({
      memoryUsage: () => ({ rss: 10, heapTotal: 8, heapUsed: 4, external: 2, arrayBuffers: 1 }) as NodeJS.MemoryUsage,
      cpuUsage: () => ({ ...cpu }),
      uptime: () => 5,
      now: () => t,
    });
    const first = s.sample()!;
    assert.equal(first.cpuPercent, undefined);
    assert.equal(first.rss, 10);
    // 100ms wall, 50ms (50_000us) CPU -> 50%.
    cpu = { user: 30_000, system: 20_000 };
    t = 1100;
    const second = s.sample()!;
    assert.ok(Math.abs(second.cpuPercent! - 50) < 1e-6);
  });

  it('returns undefined when a reader throws', () => {
    const s = new ProcessMetricsSampler({ memoryUsage: () => { throw new Error('x'); } });
    assert.equal(s.sample(), undefined);
  });
});

describe('AnalyticsTracker process-metrics attachment', () => {
  it('stamps a process field on each event when enabled', () => {
    const t = new AnalyticsTracker(undefined, {
      env: {}, includeProcessMetrics: true,
      processMetricsDeps: {
        memoryUsage: () => ({ rss: 1, heapTotal: 1, heapUsed: 1, external: 0, arrayBuffers: 0 }) as NodeJS.MemoryUsage,
        cpuUsage: () => ({ user: 1, system: 1 }),
        uptime: () => 1, now: () => 1,
      },
    });
    t.log('e', { a: 1 });
    const ev = t.getEvents()[0];
    assert.ok(ev.data.process, 'process metrics attached');
    assert.equal((ev.data.process as { rss: number }).rss, 1);
  });

  it('omits process metrics by default', () => {
    const t = new AnalyticsTracker(undefined, { env: {} });
    t.log('e', {});
    assert.equal(t.getEvents()[0].data.process, undefined);
  });
});

// ---------------------------------------------------------------------------
// durable failed-batch spool + cross-run retry
// ---------------------------------------------------------------------------
describe('FailedBatchStore', () => {
  it('spools, lists, loads and deletes a batch round-trip', () => {
    const dir = mkTmp();
    const store = new FailedBatchStore(dir);
    const events = [{ type: 'a', timestamp: 1, data: { x: 1 } }, { type: 'b', timestamp: 2, data: {} }];
    const file = store.spool(events)!;
    assert.ok(path.basename(file).startsWith(FAILED_BATCH_FILE_PREFIX));
    const listed = store.list();
    assert.deepEqual(listed, [file]);
    assert.deepEqual(store.load(file), events);
    store.delete(file);
    assert.deepEqual(store.list(), []);
  });

  it('spooling an empty batch is a no-op', () => {
    const dir = mkTmp();
    const store = new FailedBatchStore(dir);
    assert.equal(store.spool([]), null);
    assert.deepEqual(store.list(), []);
  });

  it('load tolerates poison lines and missing files', () => {
    const dir = mkTmp();
    const store = new FailedBatchStore(dir);
    const f = path.join(dir, `${FAILED_BATCH_FILE_PREFIX}x.jsonl`);
    fs.writeFileSync(f, '{"type":"ok","timestamp":1,"data":{}}\nnot json\n\n');
    assert.equal(store.load(f).length, 1);
    assert.deepEqual(store.load(path.join(dir, 'nope.jsonl')), []);
  });
});

describe('AnalyticsTracker durable failed-batch cross-run retry', () => {
  it('spools a permanently-dropped batch, then a later session re-sends and deletes it', async () => {
    // RED on revert: without spool-on-drop + recoverPersistedBatches the dropped
    // batch is lost and the second session sends nothing.
    const dir = mkTmp();
    const failedDir = path.join(dir, 'telemetry-failed');
    const endpoint = 'http://collector/v1/logs';

    // Session A: collector is down -> batch dropped -> spooled to disk.
    const a = new AnalyticsTracker(dir, {
      env: {}, otlpEndpoint: endpoint, otlpFetch: throwFetch(),
      otlpMaxAttempts: 1, sleepImpl: noSleep,
      persistFailedBatches: true, failedBatchDir: failedDir,
    });
    a.log('turn', { n: 1 });
    await a.flushOtlp();
    const spooled = new FailedBatchStore(failedDir).list();
    assert.equal(spooled.length, 1, 'dropped batch was spooled');

    // Session B: collector is back -> recovery re-sends + deletes the spool file.
    const sent = { n: 0 };
    const b = new AnalyticsTracker(dir, {
      env: {}, otlpEndpoint: endpoint, otlpFetch: okFetch(sent),
      persistFailedBatches: true, failedBatchDir: failedDir,
    });
    const recovered = await b.recoverPersistedBatches();
    assert.equal(recovered, 1, 'one file recovered');
    assert.equal(sent.n, 1, 're-POSTed the spooled batch');
    assert.equal(new FailedBatchStore(failedDir).list().length, 0, 'spool file deleted on success');
  });

  it('a permanent (4xx) spool file is discarded on recovery; a retryable one is kept', async () => {
    const dir = mkTmp();
    const failedDir = path.join(dir, 'tf');
    const store = new FailedBatchStore(failedDir);
    const batch: TelemetryBatch['events'] = [{ type: 'x', timestamp: 1, data: {} }];

    // 4xx -> permanent -> deleted without resend-forever.
    store.spool(batch);
    const perm = new AnalyticsTracker(dir, {
      env: {}, otlpEndpoint: 'http://c/v1/logs', otlpFetch: status4xx(),
      persistFailedBatches: true, failedBatchDir: failedDir,
    });
    assert.equal(await perm.recoverPersistedBatches(), 0, 'no success counted');
    assert.equal(store.list().length, 0, '4xx spool discarded');

    // network error -> retryable -> file kept for next time.
    store.spool(batch);
    const retry = new AnalyticsTracker(dir, {
      env: {}, otlpEndpoint: 'http://c/v1/logs', otlpFetch: throwFetch(),
      persistFailedBatches: true, failedBatchDir: failedDir,
    });
    assert.equal(await retry.recoverPersistedBatches(), 0);
    assert.equal(store.list().length, 1, 'retryable spool kept');
  });

  it('recovery is a no-op when the otlp sink is killed', async () => {
    const dir = mkTmp();
    const failedDir = path.join(dir, 'tf');
    new FailedBatchStore(failedDir).spool([{ type: 'x', timestamp: 1, data: {} }]);
    const sent = { n: 0 };
    const t = new AnalyticsTracker(dir, {
      env: {}, otlpEndpoint: 'http://c/v1/logs', otlpFetch: okFetch(sent),
      persistFailedBatches: true, failedBatchDir: failedDir, killedSinks: { otlp: true },
    });
    assert.equal(await t.recoverPersistedBatches(), 0);
    assert.equal(sent.n, 0);
    assert.equal(new FailedBatchStore(failedDir).list().length, 1, 'file untouched while killed');
  });
});
