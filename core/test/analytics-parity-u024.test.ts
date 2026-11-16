import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  AnalyticsTracker,
  TelemetryBatchSender,
  sanitizeAnalyticsData,
  isToolContentLoggingEnabled,
  truncateToolInputValue,
  extractToolInputForTelemetry,
  TOOL_INPUT_STRING_TRUNCATE_AT,
  TOOL_INPUT_MAX_COLLECTION_ITEMS,
  TOOL_INPUT_MAX_JSON_CHARS,
  resolveBetaTracingConfig,
  filterEventForEgress,
  TelemetryContentDedup,
  DEDUP_MIN_CONTENT_LEN,
  OtelHeadersHelper,
  validateOtelHeaders,
  resolveOtelHeadersDebounceMs,
  DEFAULT_OTEL_HEADERS_DEBOUNCE_MS,
  raceWithTimeout,
  CLOSE_GRACE_MS,
} from '../src/analytics';

const liveEnv = (extra: Record<string, string> = {}): NodeJS.ProcessEnv =>
  ({ ...extra } as NodeJS.ProcessEnv); // no NODE_ENV='test' → egress not suppressed

function readJsonl(file: string): Array<Record<string, unknown>> {
  if (!fs.existsSync(file)) return [];
  return fs.readFileSync(file, 'utf8').trim().split('\n').filter(Boolean).map(l => JSON.parse(l));
}

// ---------------------------------------------------------------------------
// G1 — Egress allowlist (datadog.ts parity): event-name + field double gate
// ---------------------------------------------------------------------------
describe('filterEventForEgress (egress allowlist)', () => {
  it('drops an event whose type is not on the event allowlist', () => {
    assert.equal(filterEventForEgress('nope', { a: 1 }, { events: ['ok'] }), null);
  });
  it('keeps an allowed event and filters to the field allowlist', () => {
    assert.deepEqual(
      filterEventForEgress('ok', { a: 1, b: 2, c: 3 }, { events: ['ok'], fields: ['a', 'c'] }),
      { a: 1, c: 3 },
    );
  });
  it('an unset field axis keeps all fields (returns the same object)', () => {
    const data = { a: 1 };
    assert.equal(filterEventForEgress('ok', data, { events: ['ok'] }), data);
  });
  it('an unset event axis allows every type', () => {
    assert.deepEqual(filterEventForEgress('anything', { a: 1, b: 2 }, { fields: ['a'] }), { a: 1 });
  });
  it('accepts a Set as well as an array', () => {
    assert.equal(filterEventForEgress('x', { a: 1 }, { events: new Set(['y']) }), null);
    assert.deepEqual(filterEventForEgress('x', { a: 1, b: 2 }, { fields: new Set(['b']) }), { b: 2 });
  });

  it('AnalyticsTracker: a disallowed event stays in-memory but never reaches disk', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'u024-egress-'));
    try {
      const t = new AnalyticsTracker(dir, { env: liveEnv(), egressEventAllowlist: ['allowed'] });
      t.log('allowed', { keep: 'yes', drop: 'no' });
      t.log('blocked', { secret: 'x' });
      assert.equal(t.getEvents().length, 2, 'both events kept in memory');
      const lines = readJsonl(path.join(dir, 'analytics.jsonl'));
      assert.equal(lines.length, 1, 'only the allowed event is written to disk');
      assert.equal(lines[0].type, 'allowed');
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('AnalyticsTracker: the field allowlist strips unlisted fields on disk', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'u024-egressf-'));
    try {
      const t = new AnalyticsTracker(dir, { env: liveEnv(), egressFieldAllowlist: ['keep'] });
      t.log('e', { keep: 'yes', drop: 'no' });
      const lines = readJsonl(path.join(dir, 'analytics.jsonl'));
      assert.deepEqual(lines[0].data, { keep: 'yes' });
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// G3 — OTEL_LOG_TOOL_CONTENT: tool content gate distinct from user prompts
// ---------------------------------------------------------------------------
describe('isToolContentLoggingEnabled + tool-content gating', () => {
  it('reads OTEL_LOG_TOOL_CONTENT', () => {
    assert.equal(isToolContentLoggingEnabled({} as NodeJS.ProcessEnv), false);
    assert.equal(isToolContentLoggingEnabled({ OTEL_LOG_TOOL_CONTENT: '1' } as NodeJS.ProcessEnv), true);
  });

  it('tool content keys are redacted to length-only by default', () => {
    const out = sanitizeAnalyticsData({ output: 'hello world', result: 'r' }, { env: {} as NodeJS.ProcessEnv });
    assert.equal(out.output, '<REDACTED>');
    assert.equal(out.output_length, 'hello world'.length);
  });

  it('OTEL_LOG_TOOL_CONTENT unlocks tool content WITHOUT unlocking user prompts', () => {
    const env = { OTEL_LOG_TOOL_CONTENT: '1' } as NodeJS.ProcessEnv;
    const out = sanitizeAnalyticsData({ output: 'tool out', prompt: 'secret ask' }, { env });
    assert.equal(out.output, 'tool out', 'tool content exported');
    assert.equal(out.prompt, '<REDACTED>', 'user prompt still redacted');
  });

  it('logUserPrompts unlocks BOTH prompts and tool content', () => {
    const out = sanitizeAnalyticsData({ output: 'o', prompt: 'p' }, { env: {} as NodeJS.ProcessEnv, logUserPrompts: true });
    assert.equal(out.output, 'o');
    assert.equal(out.prompt, 'p');
  });
});

// ---------------------------------------------------------------------------
// G6 — Bounded tool-input serialization (metadata.ts parity)
// ---------------------------------------------------------------------------
describe('truncateToolInputValue / extractToolInputForTelemetry', () => {
  it('clips long strings keeping a char-count marker', () => {
    const long = 'x'.repeat(TOOL_INPUT_STRING_TRUNCATE_AT + 50);
    const r = truncateToolInputValue(long) as string;
    assert.ok(r.includes(`[${long.length} chars]`));
    assert.ok(r.length < long.length);
  });
  it('keeps short strings, numbers, booleans, null verbatim', () => {
    assert.equal(truncateToolInputValue('short'), 'short');
    assert.equal(truncateToolInputValue(42), 42);
    assert.equal(truncateToolInputValue(true), true);
    assert.equal(truncateToolInputValue(null), null);
  });
  it('caps collections and adds an overflow marker', () => {
    const arr = Array.from({ length: TOOL_INPUT_MAX_COLLECTION_ITEMS + 5 }, (_, i) => i);
    const r = truncateToolInputValue(arr) as unknown[];
    assert.equal(r.length, TOOL_INPUT_MAX_COLLECTION_ITEMS + 1);
    assert.equal(r[r.length - 1], `…[${arr.length} items]`);
  });
  it('bounds nesting depth to <nested>', () => {
    const deep = { a: { b: { c: { d: 1 } } } };
    const r = truncateToolInputValue(deep) as Record<string, unknown>;
    // depth 0 (a) -> depth 1 (b) -> depth 2 collapses
    assert.deepEqual(r, { a: { b: '<nested>' } });
  });
  it('strips internal (_) keys from objects', () => {
    const r = truncateToolInputValue({ keep: 1, _simulated: 2 }) as Record<string, unknown>;
    assert.deepEqual(r, { keep: 1 });
  });
  it('extract returns undefined unless tool details enabled', () => {
    assert.equal(extractToolInputForTelemetry({ a: 1 }, {} as NodeJS.ProcessEnv), undefined);
    const json = extractToolInputForTelemetry({ a: 1 }, { OTEL_LOG_TOOL_DETAILS: '1' } as NodeJS.ProcessEnv);
    assert.equal(json, '{"a":1}');
  });
  it('extract hard-caps the serialized JSON length', () => {
    // Build a payload that stays large AFTER per-field/collection bounding: 20 top
    // keys, each a nested object of 20 short-string entries (~400 entries total).
    const big: Record<string, unknown> = {};
    for (let i = 0; i < TOOL_INPUT_MAX_COLLECTION_ITEMS; i++) {
      const inner: Record<string, string> = {};
      for (let j = 0; j < TOOL_INPUT_MAX_COLLECTION_ITEMS; j++) inner[`key_${j}`] = `value_${j}`;
      big[`field_${i}`] = inner;
    }
    const json = extractToolInputForTelemetry(big, { AMI_LOG_TOOL_DETAILS: '1' } as NodeJS.ProcessEnv)!;
    assert.ok(json.endsWith('…[truncated]'));
    assert.ok(json.length <= TOOL_INPUT_MAX_JSON_CHARS + '…[truncated]'.length);
  });
  it('sanitize wires a tool_input key through the bounded serializer when details on', () => {
    const env = { OTEL_LOG_TOOL_DETAILS: '1' } as NodeJS.ProcessEnv;
    const out = sanitizeAnalyticsData({ tool_input: { path: '/a', n: 1 } }, { env });
    assert.equal(typeof out.tool_input, 'string');
    assert.ok((out.tool_input as string).includes('path'));
  });
  it('sanitize redacts a tool_input key to length when details off', () => {
    const out = sanitizeAnalyticsData({ tool_input: { path: '/a' } }, { env: {} as NodeJS.ProcessEnv });
    assert.equal(out.tool_input, '<REDACTED>');
    assert.ok(typeof out.tool_input_length === 'number');
  });
});

// ---------------------------------------------------------------------------
// G4 — ENABLE_BETA_TRACING_DETAILED + BETA_TRACING_ENDPOINT
// ---------------------------------------------------------------------------
describe('resolveBetaTracingConfig', () => {
  it('requires BOTH the flag and the endpoint', () => {
    assert.deepEqual(resolveBetaTracingConfig({} as NodeJS.ProcessEnv), { enabled: false, endpoint: null });
    assert.deepEqual(resolveBetaTracingConfig({ ENABLE_BETA_TRACING_DETAILED: '1' } as NodeJS.ProcessEnv), { enabled: false, endpoint: null });
    assert.deepEqual(resolveBetaTracingConfig({ BETA_TRACING_ENDPOINT: 'http://x' } as NodeJS.ProcessEnv), { enabled: false, endpoint: 'http://x' });
  });
  it('is enabled when both present', () => {
    const c = resolveBetaTracingConfig({ ENABLE_BETA_TRACING_DETAILED: '1', BETA_TRACING_ENDPOINT: 'http://x' } as NodeJS.ProcessEnv);
    assert.deepEqual(c, { enabled: true, endpoint: 'http://x' });
  });
  it('detailed tracing unlocks content in sanitize', () => {
    const env = { ENABLE_BETA_TRACING_DETAILED: '1', BETA_TRACING_ENDPOINT: 'http://x' } as NodeJS.ProcessEnv;
    const out = sanitizeAnalyticsData({ prompt: 'p', output: 'o' }, { env });
    assert.equal(out.prompt, 'p');
    assert.equal(out.output, 'o');
  });
  it('tracker exposes the resolved beta tracing config', () => {
    const t = new AnalyticsTracker(undefined, { env: { ENABLE_BETA_TRACING_DETAILED: '1', BETA_TRACING_ENDPOINT: 'http://x' } as NodeJS.ProcessEnv });
    assert.deepEqual(t.getBetaTracingConfig(), { enabled: true, endpoint: 'http://x' });
  });
});

// ---------------------------------------------------------------------------
// G5 — Hash-based content deduplication (betaSessionTracing.ts parity)
// ---------------------------------------------------------------------------
describe('TelemetryContentDedup', () => {
  it('emits full content first, preview+hash+length thereafter', () => {
    const d = new TelemetryContentDedup();
    const big = 'z'.repeat(DEDUP_MIN_CONTENT_LEN + 10);
    const first = d.dedupe({ body: big });
    assert.equal(first.body, big, 'first occurrence full');
    const second = d.dedupe({ body: big });
    assert.notEqual(second.body, big);
    assert.equal((second.body as string).length <= 500, true);
    assert.equal(second.body_length, big.length);
    assert.equal(typeof second.body_hash, 'string');
    assert.equal((second.body_hash as string).length, 12);
  });
  it('leaves short strings and non-strings untouched (returns same object)', () => {
    const d = new TelemetryContentDedup();
    const data = { s: 'short', n: 5 };
    assert.equal(d.dedupe(data), data);
  });
  it('reset forgets hashes so content logs full again', () => {
    const d = new TelemetryContentDedup();
    const big = 'q'.repeat(DEDUP_MIN_CONTENT_LEN + 1);
    d.dedupe({ b: big });
    assert.equal(d.seenCount, 1);
    d.reset();
    assert.equal(d.seenCount, 0);
    assert.equal(d.dedupe({ b: big }).b, big);
  });

  it('AnalyticsTracker dedupeContent collapses a repeated body on disk', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'u024-dedup-'));
    try {
      const t = new AnalyticsTracker(dir, { env: liveEnv(), dedupeContent: true });
      const big = 'A'.repeat(DEDUP_MIN_CONTENT_LEN + 5);
      t.log('e', { body: big });
      t.log('e', { body: big });
      const lines = readJsonl(path.join(dir, 'analytics.jsonl'));
      assert.equal((lines[0].data as Record<string, unknown>).body, big);
      const d2 = lines[1].data as Record<string, unknown>;
      assert.equal(d2.body_length, big.length);
      assert.ok((d2.body as string).length <= 500);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// G2 — otelHeadersHelper (auth.ts parity)
// ---------------------------------------------------------------------------
describe('OtelHeadersHelper', () => {
  it('resolves the debounce window from env with Claude fallback + default', () => {
    assert.equal(resolveOtelHeadersDebounceMs({} as NodeJS.ProcessEnv), DEFAULT_OTEL_HEADERS_DEBOUNCE_MS);
    assert.equal(resolveOtelHeadersDebounceMs({ AMI_OTEL_HEADERS_HELPER_DEBOUNCE_MS: '1000' } as NodeJS.ProcessEnv), 1000);
    assert.equal(resolveOtelHeadersDebounceMs({ CLAUDE_CODE_OTEL_HEADERS_HELPER_DEBOUNCE_MS: '0' } as NodeJS.ProcessEnv), 0);
    assert.equal(resolveOtelHeadersDebounceMs({ AMI_OTEL_HEADERS_HELPER_DEBOUNCE_MS: 'nope' } as NodeJS.ProcessEnv), DEFAULT_OTEL_HEADERS_DEBOUNCE_MS);
  });

  it('runs the command once and caches within the debounce window', () => {
    let calls = 0;
    let clock = 1000;
    const h = new OtelHeadersHelper('auth-cmd', {
      exec: () => { calls++; return JSON.stringify({ Authorization: `Bearer ${calls}` }); },
      env: { AMI_OTEL_HEADERS_HELPER_DEBOUNCE_MS: '10000' } as NodeJS.ProcessEnv,
      now: () => clock,
    });
    assert.deepEqual(h.get(), { Authorization: 'Bearer 1' });
    clock += 5000; // within window
    assert.deepEqual(h.get(), { Authorization: 'Bearer 1' }, 'cached');
    assert.equal(calls, 1);
    clock += 6000; // past window
    assert.deepEqual(h.get(), { Authorization: 'Bearer 2' }, 're-run after debounce');
    assert.equal(calls, 2);
  });

  it('empty command yields {}', () => {
    assert.deepEqual(new OtelHeadersHelper('', { exec: () => '{}' }).get(), {});
  });

  it('throws on empty output / non-object / non-string value', () => {
    assert.throws(() => new OtelHeadersHelper('c', { exec: () => '' }).get(), /did not return a valid value/);
    assert.throws(() => new OtelHeadersHelper('c', { exec: () => '[]' }).get(), /JSON object/);
    assert.throws(() => new OtelHeadersHelper('c', { exec: () => '{"a":1}' }).get(), /non-string value/);
  });

  it('validateOtelHeaders accepts a string map and rejects bad shapes', () => {
    assert.deepEqual(validateOtelHeaders({ a: 'b' }), { a: 'b' });
    assert.throws(() => validateOtelHeaders(null), /JSON object/);
    assert.throws(() => validateOtelHeaders({ a: 2 }), /non-string/);
  });

  it('AnalyticsTracker merges helper headers over static ones on the OTLP POST', async () => {
    let seen: Record<string, string> | undefined;
    const fakeFetch = (async (_url: string, init: { headers: Record<string, string> }) => {
      seen = init.headers; return { ok: true, status: 200 } as Response;
    }) as unknown as typeof fetch;
    const t = new AnalyticsTracker(undefined, {
      env: liveEnv(),
      otlpEndpoint: 'http://h/v1/logs',
      otlpFetch: fakeFetch,
      otlpHeaders: { 'x-static': 'a', Authorization: 'old' },
      otlpHeadersHelper: { command: 'c', exec: () => JSON.stringify({ Authorization: 'new' }) },
    });
    t.log('e', {});
    await t.flushOtlp();
    assert.equal(seen?.['x-static'], 'a');
    assert.equal(seen?.['Authorization'], 'new', 'helper header wins');
  });

  it('AnalyticsTracker falls back to static headers + warns once when the helper throws', async () => {
    let seen: Record<string, string> | undefined;
    const diags: string[] = [];
    const fakeFetch = (async (_url: string, init: { headers: Record<string, string> }) => {
      seen = init.headers; return { ok: true, status: 200 } as Response;
    }) as unknown as typeof fetch;
    const t = new AnalyticsTracker(undefined, {
      env: liveEnv(),
      otlpEndpoint: 'http://h/v1/logs',
      otlpFetch: fakeFetch,
      otlpHeaders: { 'x-static': 'a' },
      otlpHeadersHelper: { command: 'c', exec: () => { throw new Error('boom'); } },
      onDiagnostic: (m) => diags.push(m),
    });
    t.log('e', {});
    await t.flushOtlp();
    assert.equal(seen?.['x-static'], 'a');
    assert.ok(diags.some(d => d.includes('otelHeadersHelper failed')));
  });
});

// ---------------------------------------------------------------------------
// G7 — pendingCount readable after close
// ---------------------------------------------------------------------------
describe('TelemetryBatchSender.pendingCount', () => {
  it('counts queued events and remains readable after shutdown', () => {
    const s = new TelemetryBatchSender({ sessionId: 's', maxBatchSize: 100 });
    s.enqueue('a', {});
    s.enqueue('b', {});
    assert.equal(s.pendingCount, 2);
    s.shutdown();
    assert.equal(s.pendingCount, 2, 'readable after shutdown for diagnostics');
  });
  it('includes retained ready batches', () => {
    const s = new TelemetryBatchSender({ sessionId: 's', maxBatchSize: 1 }); // no onBatch → retained
    s.enqueue('a', {}); // emits a ready batch of 1
    s.enqueue('b', {}); // emits another ready batch of 1
    assert.equal(s.pendingCount, 2);
  });
  it('tracker.getPendingTelemetryCount reports queued telemetry', () => {
    const t = new AnalyticsTracker(undefined, {
      env: liveEnv(),
      otlpEndpoint: 'http://h/v1/logs',
      otlpFetch: (async () => ({ ok: true, status: 200 } as Response)) as unknown as typeof fetch,
      telemetryMaxBatchSize: 100,
    });
    t.log('e', {});
    assert.equal(t.getPendingTelemetryCount(), 1);
  });
});

// ---------------------------------------------------------------------------
// G8 — Bounded graceful drain on close (HybridTransport parity)
// ---------------------------------------------------------------------------
describe('raceWithTimeout + closeWithGrace', () => {
  it('resolves on the timeout even if the promise never settles', async () => {
    const hang = new Promise<void>(() => { /* never resolves */ });
    let slept = 0;
    await raceWithTimeout(hang, 123, async (ms) => { slept = ms; });
    assert.equal(slept, 123);
  });
  it('swallows a rejected promise (best-effort)', async () => {
    await assert.doesNotReject(raceWithTimeout(Promise.reject(new Error('x')), 5, async () => {}));
  });
  it('closeWithGrace latches shutdown even when flush hangs', async () => {
    let fetchStarted = false;
    const hangingFetch = (async () => {
      fetchStarted = true;
      await new Promise<void>(() => {}); // hang
      return { ok: true } as Response;
    }) as unknown as typeof fetch;
    const t = new AnalyticsTracker(undefined, {
      env: liveEnv(),
      otlpEndpoint: 'http://h/v1/logs',
      otlpFetch: hangingFetch,
      sleepImpl: async () => {}, // instant grace timeout
    });
    t.log('e', {});
    await t.closeWithGrace(50);
    assert.equal(t.isShutdown(), true, 'shutdown latched despite hang');
    assert.equal(fetchStarted, true);
    // Default grace constant is exported and positive.
    assert.ok(CLOSE_GRACE_MS > 0);
  });
  it('closeWithGrace is idempotent', async () => {
    const t = new AnalyticsTracker(undefined, { env: liveEnv() });
    await t.closeWithGrace();
    await t.closeWithGrace();
    assert.equal(t.isShutdown(), true);
  });
});
