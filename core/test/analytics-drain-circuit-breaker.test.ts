/**
 * analytics-drain-circuit-breaker — shutdown-drain reliability (the reference parity with
 * SerialBatchEventUploader):
 *  - maxConsecutiveFailures: a cross-batch circuit breaker that ADVANCES past
 *    dropped batches (fresh per-batch budget) until N fail in a row, then gives up
 *    the flush and requeues the remainder; a success resets the streak.
 *  - getConsecutiveFailureCount introspection surface.
 *  - server Retry-After / 429 honoured by postOtlpBatch + flushOtlp backoff.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { AnalyticsTracker, postOtlpBatch, type TelemetryBatch } from '../src/analytics';

/** A fetch that returns the given status for every call (OTLP POST). */
function statusFetch(status: number, counter?: { n: number }): typeof fetch {
  return (async () => {
    if (counter) counter.n++;
    return { ok: status >= 200 && status < 300, status, headers: { get: () => null } } as unknown as Response;
  }) as unknown as typeof fetch;
}

// Load several events as SEPARATE drain batches without tripping a mid-session
// (onBatch) send: a high count cap means no count-boundary emit during log(),
// while a 1-byte byte cap makes drain's takeBatch split one event per batch.
function trackerWithNBatches(n: number, opts: Record<string, unknown>): AnalyticsTracker {
  const t = new AnalyticsTracker(undefined, {
    env: {},
    otlpEndpoint: 'http://localhost:1/v1/logs',
    telemetryMaxBatchSize: 1000,
    telemetryMaxBatchBytes: 1,
    otlpMaxAttempts: 1,
    otlpBaseBackoffMs: 0,
    sleepImpl: async () => {},
    ...opts,
  });
  for (let i = 0; i < n; i++) t.log(`e${i}`, { i });
  return t;
}

describe('AnalyticsTracker maxConsecutiveFailures circuit breaker', () => {
  it('advances past dropped batches until the streak is reached, then stops', async () => {
    // 3 drain batches, all fail (503). maxConsecutiveFailures=2 → drop the first
    // two (advancing), trip on the 2nd, and requeue the 3rd (never sent).
    const counter = { n: 0 };
    const t = trackerWithNBatches(3, {
      otlpFetch: statusFetch(503, counter),
      maxConsecutiveFailures: 2,
    });
    await t.flushOtlp();
    assert.equal(t.getDroppedBatchCount(), 2, 'two batches dropped before tripping');
    assert.equal(t.getConsecutiveFailureCount(), 2, 'streak reached the configured max');
    assert.equal(counter.n, 2, 'third batch was requeued, not POSTed');
  });

  it('a successful send between failures resets the streak', async () => {
    // Fail, succeed, fail, fail with maxConsecutiveFailures=2: the success resets
    // the streak so the breaker only trips on the 2nd+3rd trailing failures.
    let call = 0;
    const seq = [503, 200, 503, 503]; // batch0 fail, batch1 ok, batch2 fail, batch3 fail
    const fetchImpl = (async () => {
      const status = seq[call++] ?? 503;
      return { ok: status >= 200 && status < 300, status, headers: { get: () => null } } as unknown as Response;
    }) as unknown as typeof fetch;
    const t = trackerWithNBatches(4, { otlpFetch: fetchImpl, maxConsecutiveFailures: 2 });
    await t.flushOtlp();
    // batch0 dropped (streak 1), batch1 ok (reset), batch2 dropped (streak 1),
    // batch3 dropped (streak 2 → trip). 3 batches dropped total.
    assert.equal(t.getDroppedBatchCount(), 3);
    assert.equal(t.getConsecutiveFailureCount(), 2);
  });

  it('default (unset) stops the drain on the FIRST failure (regression lock)', async () => {
    const counter = { n: 0 };
    const t = trackerWithNBatches(3, { otlpFetch: statusFetch(503, counter) });
    await t.flushOtlp();
    assert.equal(t.getDroppedBatchCount(), 1, 'only the first batch is dropped');
    assert.equal(counter.n, 1, 'remaining batches requeued, not POSTed');
  });

  it('getConsecutiveFailureCount starts at 0 and resets to 0 on an all-success drain', async () => {
    const t = trackerWithNBatches(2, { otlpFetch: statusFetch(200), maxConsecutiveFailures: 2 });
    assert.equal(t.getConsecutiveFailureCount(), 0);
    await t.flushOtlp();
    assert.equal(t.getConsecutiveFailureCount(), 0);
    assert.equal(t.getDroppedBatchCount(), 0);
  });
});

describe('postOtlpBatch Retry-After / 429 handling', () => {
  const batch: TelemetryBatch = { sessionId: 's', batchId: 1, events: [{ type: 'e', timestamp: 1, data: {} }] };

  it('parses a numeric (seconds) Retry-After on a 429 into ms and marks it retryable', async () => {
    const fetchImpl = (async () => ({
      ok: false, status: 429,
      headers: { get: (k: string) => (k.toLowerCase() === 'retry-after' ? '2' : null) },
    } as unknown as Response)) as unknown as typeof fetch;
    const res = await postOtlpBatch('http://h/v1/logs', batch, { fetchImpl, env: {} });
    assert.equal(res.ok, false);
    assert.equal(res.status, 429);
    assert.equal(res.retryable, true);
    assert.equal(res.retryAfterMs, 2000);
  });

  it('parses an HTTP-date Retry-After into a non-negative ms delay', async () => {
    const future = new Date(Date.now() + 5000).toUTCString();
    const fetchImpl = (async () => ({
      ok: false, status: 503,
      headers: { get: (k: string) => (k.toLowerCase() === 'retry-after' ? future : null) },
    } as unknown as Response)) as unknown as typeof fetch;
    const res = await postOtlpBatch('http://h/v1/logs', batch, { fetchImpl, env: {} });
    assert.ok((res.retryAfterMs ?? -1) >= 0 && (res.retryAfterMs ?? 0) <= 5000);
  });
});

describe('AnalyticsTracker.flushOtlp honours server Retry-After for backoff', () => {
  it('sleeps the server-supplied Retry-After (not the jittered default) before retrying', async () => {
    const slept: number[] = [];
    let call = 0;
    const fetchImpl = (async () => {
      call++;
      if (call === 1) {
        return { ok: false, status: 429, headers: { get: (k: string) => (k.toLowerCase() === 'retry-after' ? '3' : null) } } as unknown as Response;
      }
      return { ok: true, status: 200, headers: { get: () => null } } as unknown as Response;
    }) as unknown as typeof fetch;
    const t = new AnalyticsTracker(undefined, {
      env: {}, otlpEndpoint: 'http://h/v1/logs', otlpFetch: fetchImpl,
      otlpMaxAttempts: 3, otlpBaseBackoffMs: 500, otlpMaxBackoffMs: 30_000,
      sleepImpl: async (ms) => { slept.push(ms); },
      rand: () => 0, // would make the jittered default 250ms, so 3000 proves Retry-After won
    });
    t.log('e', { k: 1 });
    await t.flushOtlp();
    assert.deepEqual(slept, [3000], 'honoured the 3s server hint, not the backoff default');
    assert.equal(t.getDroppedBatchCount(), 0, 'retry succeeded');
  });
});
