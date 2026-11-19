import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { AnalyticsTracker } from '../src/analytics';

// Empty-env helper: no DO_NOT_TRACK / DISABLE_TELEMETRY / OTEL_* overrides, so the
// OTLP batch transport is enabled by default.
const E = (o: Record<string, string> = {}) => o as unknown as NodeJS.ProcessEnv;

describe('AnalyticsTracker.getTelemetrySnapshot (introspection)', () => {
  it('reports active=false and zeroed counters when no OTLP transport was started', () => {
    // No endpoint ⇒ the lazily-created TelemetryBatchSender is never constructed.
    const t = new AnalyticsTracker(undefined, { env: E({}) });
    t.log('session_start', {});
    const snap = t.getTelemetrySnapshot();
    assert.equal(snap.active, false);
    assert.equal(snap.totalSent, 0);
    assert.equal(snap.totalDropped, 0);
    assert.equal(snap.queueSize, 0);
    assert.equal(snap.pendingCount, 0);
    assert.equal(snap.batchId, 0);
    assert.equal(snap.droppedEvents, 0);
  });

  it('surfaces live queue depth once events are enqueued on the OTLP transport', () => {
    const fetchStub = (async () => ({ ok: true }) as Response) as unknown as typeof fetch;
    const t = new AnalyticsTracker(undefined, {
      otlpEndpoint: 'http://127.0.0.1:1/v1/logs',
      otlpFetch: fetchStub,
      env: E({}),
      // Large batch size ⇒ no size-boundary flush, so events stay queued and the
      // snapshot can observe non-zero queue/pending depth.
      telemetryMaxBatchSize: 1000,
    });
    try {
      t.log('a', {});
      t.log('b', {});
      t.log('c', {});
      const snap = t.getTelemetrySnapshot();
      assert.equal(snap.active, true, 'transport active once endpoint-backed events are logged');
      assert.equal(snap.queueSize, 3, 'all three queued events counted');
      assert.equal(snap.pendingCount, 3, 'pending mirrors queued depth before any drain');
      assert.equal(snap.totalSent, 0, 'nothing exported yet');
      assert.equal(snap.batchId, 0, 'no batch cut yet');
    } finally {
      t.shutdown();
    }
  });

  it('surfaces source-gated dropped events in the snapshot', () => {
    const t = new AnalyticsTracker(undefined, { env: E({}) });
    // Restrict allowed sources, then log from outside any allowed source ⇒ dropped.
    t.setAllowedSources(['allowed-source']);
    t.log('gated-1', {});
    t.log('gated-2', {});
    const snap = t.getTelemetrySnapshot();
    assert.equal(snap.droppedEvents, 2, 'both source-gated drops reflected');
    assert.equal(snap.droppedEvents, t.getDroppedEventCount(), 'snapshot agrees with getDroppedEventCount()');
  });
});
