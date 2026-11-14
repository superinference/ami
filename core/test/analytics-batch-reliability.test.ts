/**
 * analytics-batch-reliability — telemetry batching hardening (Claude parity):
 *  - per-event-TYPE sampling (sample_rate map) with sample_rate stamping
 *  - mid-session boundary send via the onBatch hook (not only at shutdown)
 *  - permanently-dropped-batch diagnostics (onBatchDropped + droppedBatchCount)
 *  - resolveSampleRate / trySerialize helpers
 *  - maxBatchBytes byte ceiling threaded from the tracker
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  AnalyticsTracker, resolveSampleRate, trySerialize,
} from '../src/analytics';

const okFetch = (counter: { n: number }) =>
  (async () => { counter.n++; return { ok: true, status: 200 } as Response; }) as unknown as typeof fetch;

describe('resolveSampleRate', () => {
  it('returns the per-type rate when present and in range', () => {
    assert.equal(resolveSampleRate('busy', { busy: 0.25 }, 1), 0.25);
    assert.equal(resolveSampleRate('busy', { busy: 0 }, 1), 0);
  });
  it('falls back for a missing type', () => {
    assert.equal(resolveSampleRate('other', { busy: 0.1 }, 0.9), 0.9);
    assert.equal(resolveSampleRate('x', undefined, 0.5), 0.5);
  });
  it('falls back for out-of-range / non-finite rates', () => {
    assert.equal(resolveSampleRate('t', { t: -1 }, 1), 1);
    assert.equal(resolveSampleRate('t', { t: NaN }, 0.7), 0.7);
    assert.equal(resolveSampleRate('t', { t: Infinity }, 0.3), 0.3);
  });
});

describe('trySerialize', () => {
  it('serializes a plain value', () => {
    assert.equal(trySerialize({ a: 1 }), '{"a":1}');
  });
  it('returns null on a circular reference', () => {
    const c: Record<string, unknown> = {}; c.self = c;
    assert.equal(trySerialize(c), null);
  });
  it('returns null on a BigInt', () => {
    assert.equal(trySerialize({ big: 10n }), null);
  });
  it('returns null when the value stringifies to undefined', () => {
    assert.equal(trySerialize(undefined), null);
  });
});

describe('AnalyticsTracker per-type sampling', () => {
  it('drops an event whose per-type rate is 0 and counts it', () => {
    const t = new AnalyticsTracker(undefined, { env: {}, sampleRates: { noisy: 0 } });
    t.log('noisy', { a: 1 });
    t.log('kept', { b: 2 });
    assert.equal(t.getEventsByType('noisy').length, 0);
    assert.equal(t.getEventsByType('kept').length, 1);
    assert.equal(t.getDroppedEventCount(), 1);
  });

  it('stamps the applied sample_rate when kept below full rate', () => {
    // rand always 0 -> always below rate -> kept.
    const t = new AnalyticsTracker(undefined, { env: {}, sampleRates: { half: 0.5 }, rand: () => 0 });
    t.log('half', { a: 1 });
    const ev = t.getEventsByType('half')[0];
    assert.equal(ev.data.sample_rate, 0.5);
  });

  it('does not stamp sample_rate at full rate and uses the default for unlisted types', () => {
    const t = new AnalyticsTracker(undefined, { env: {}, sampleRates: { x: 0.5 }, rand: () => 0 });
    t.log('full', { a: 1 });
    assert.equal(t.getEventsByType('full')[0].data.sample_rate, undefined);
  });
});

describe('AnalyticsTracker mid-session boundary send', () => {
  it('POSTs a batch at the size boundary before shutdown drain', async () => {
    const counter = { n: 0 };
    const t = new AnalyticsTracker(undefined, {
      env: {}, otlpEndpoint: 'http://localhost:1/v1/logs', otlpFetch: okFetch(counter),
      telemetryMaxBatchSize: 2,
    });
    t.log('a', {});
    t.log('b', {}); // trips the boundary -> onBatch -> scheduled send
    await t.whenSent();
    assert.equal(counter.n, 1, 'boundary batch POSTed mid-session');
    t.log('c', {});
    await t.flushOtlp();
    assert.equal(counter.n, 2, 'shutdown drain POSTs the tail');
  });

  it('whenSent is a no-op when nothing was scheduled', async () => {
    const t = new AnalyticsTracker(undefined, { env: {} });
    await t.whenSent();
    assert.ok(true);
  });
});

describe('AnalyticsTracker dropped-batch diagnostics', () => {
  it('fires onBatchDropped and increments droppedBatchCount on a permanent (4xx) failure', async () => {
    const drops: Array<[number, number]> = [];
    const badFetch = (async () => ({ ok: false, status: 400, headers: { get: () => null } } as unknown as Response)) as unknown as typeof fetch;
    const t = new AnalyticsTracker(undefined, {
      env: {}, otlpEndpoint: 'http://localhost:1/v1/logs', otlpFetch: badFetch,
      telemetryMaxBatchSize: 1,
      onBatchDropped: (count, total) => drops.push([count, total]),
    });
    t.log('a', {}); // boundary (size 1) -> scheduled send -> 400 -> dropped
    await t.whenSent();
    assert.equal(t.getDroppedBatchCount(), 1);
    assert.deepEqual(drops, [[1, 1]]);
  });
});
