import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { ProviderHealthTracker } from '../src/provider-health';

describe('ProviderHealthTracker', () => {
  let tracker: ProviderHealthTracker;

  beforeEach(() => {
    tracker = new ProviderHealthTracker();
  });

  it('returns zeroed stats when no requests recorded', () => {
    const stats = tracker.getStats();
    assert.equal(stats.requestCount, 0);
    assert.equal(stats.errorCount, 0);
    assert.equal(stats.successCount, 0);
    assert.equal(stats.errorRate, 0);
    assert.equal(stats.availability, 1);
    assert.equal(stats.latency.min, 0);
    assert.equal(stats.latency.max, 0);
    assert.equal(stats.latency.avg, 0);
    assert.equal(stats.latency.p50, 0);
    assert.equal(stats.latency.p95, 0);
    assert.equal(stats.lastErrorTime, null);
    assert.equal(stats.lastSuccessTime, null);
  });

  it('tracks a single successful request', () => {
    tracker.recordRequest(150);
    const stats = tracker.getStats();
    assert.equal(stats.requestCount, 1);
    assert.equal(stats.successCount, 1);
    assert.equal(stats.errorCount, 0);
    assert.equal(stats.errorRate, 0);
    assert.equal(stats.availability, 1);
    assert.equal(stats.latency.min, 150);
    assert.equal(stats.latency.max, 150);
    assert.equal(stats.latency.avg, 150);
    assert.equal(stats.latency.p50, 150);
    assert.equal(stats.latency.p95, 150);
    assert.ok(stats.lastSuccessTime !== null);
    assert.equal(stats.lastErrorTime, null);
  });

  it('tracks multiple requests with correct min/max/avg', () => {
    tracker.recordRequest(100);
    tracker.recordRequest(200);
    tracker.recordRequest(300);
    const stats = tracker.getStats();
    assert.equal(stats.requestCount, 3);
    assert.equal(stats.latency.min, 100);
    assert.equal(stats.latency.max, 300);
    assert.equal(stats.latency.avg, 200);
  });

  it('computes p50 and p95 with a known distribution', () => {
    for (let i = 1; i <= 100; i++) {
      tracker.recordRequest(i);
    }
    const stats = tracker.getStats();
    assert.equal(stats.latency.p50, 51);
    assert.equal(stats.latency.p95, 96);
    assert.equal(stats.latency.min, 1);
    assert.equal(stats.latency.max, 100);
  });

  it('computes error rate correctly', () => {
    tracker.recordRequest(100);
    tracker.recordRequest(100);
    tracker.recordError(50);
    tracker.recordRequest(100);
    const stats = tracker.getStats();
    assert.equal(stats.requestCount, 4);
    assert.equal(stats.errorCount, 1);
    assert.equal(stats.successCount, 3);
    assert.equal(stats.errorRate, 0.25);
    assert.equal(stats.availability, 0.75);
  });

  it('maintains sliding window of 100 entries', () => {
    for (let i = 0; i < 150; i++) {
      tracker.recordRequest(i < 50 ? 1000 : 10);
    }
    const stats = tracker.getStats();
    assert.equal(stats.requestCount, 150);
    assert.equal(stats.latency.min, 10);
    assert.equal(stats.latency.max, 10);
    assert.equal(stats.latency.avg, 10);
  });

  it('computes availability as 1 - errorRate', () => {
    tracker.recordRequest(100);
    tracker.recordError(50);
    const stats = tracker.getStats();
    assert.equal(stats.availability, 0.5);
  });

  it('reset clears all state', () => {
    tracker.recordRequest(100);
    tracker.recordError(50);
    tracker.reset();
    const stats = tracker.getStats();
    assert.equal(stats.requestCount, 0);
    assert.equal(stats.errorCount, 0);
    assert.equal(stats.successCount, 0);
    assert.equal(stats.latency.min, 0);
    assert.equal(stats.lastErrorTime, null);
    assert.equal(stats.lastSuccessTime, null);
  });

  it('tracks lastErrorTime and lastSuccessTime', () => {
    tracker.recordRequest(10);
    const afterSuccess = tracker.getStats();
    assert.ok(afterSuccess.lastSuccessTime !== null);
    assert.equal(afterSuccess.lastErrorTime, null);

    tracker.recordError(5);
    const afterError = tracker.getStats();
    assert.ok(afterError.lastErrorTime !== null);
    assert.ok(afterError.lastSuccessTime !== null);
    assert.ok(afterError.lastErrorTime! >= afterError.lastSuccessTime!);
  });

  it('handles error with default latency of 0', () => {
    tracker.recordError();
    const stats = tracker.getStats();
    assert.equal(stats.errorCount, 1);
    assert.equal(stats.latency.min, 0);
  });

  it('includes error latencies in the window', () => {
    tracker.recordRequest(100);
    tracker.recordError(500);
    tracker.recordRequest(200);
    const stats = tracker.getStats();
    assert.equal(stats.latency.max, 500);
    assert.equal(stats.latency.min, 100);
  });
});

describe('ProviderCoordinator health integration', () => {
  it('recordRequestLatency updates health stats', async () => {
    const { ProviderCoordinator } = await import('../src/provider-coordinator');
    const coord = new ProviderCoordinator({
      primary: { apiKey: 'test-key', model: 'test-model', baseUrl: 'http://localhost:1' },
    });
    coord.recordRequestLatency(250);
    coord.recordRequestLatency(350);
    const stats = coord.getHealthStats();
    assert.equal(stats.requestCount, 2);
    assert.equal(stats.successCount, 2);
    assert.equal(stats.latency.min, 250);
    assert.equal(stats.latency.max, 350);
  });

  it('recordError tracks error in health stats', async () => {
    const { ProviderCoordinator } = await import('../src/provider-coordinator');
    const coord = new ProviderCoordinator({
      primary: { apiKey: 'test-key', model: 'test-model', baseUrl: 'http://localhost:1' },
    });
    coord.recordRequestLatency(100);
    coord.recordError();
    const stats = coord.getHealthStats();
    assert.equal(stats.requestCount, 2);
    assert.equal(stats.successCount, 1);
    assert.equal(stats.errorCount, 1);
    assert.equal(stats.errorRate, 0.5);
  });

  it('getHealthStats returns valid data after mixed operations', async () => {
    const { ProviderCoordinator } = await import('../src/provider-coordinator');
    const coord = new ProviderCoordinator({
      primary: { apiKey: 'test-key', model: 'test-model', baseUrl: 'http://localhost:1' },
    });
    for (let i = 0; i < 10; i++) {
      coord.recordRequestLatency(100 + i * 10);
    }
    coord.recordError();
    const stats = coord.getHealthStats();
    assert.equal(stats.requestCount, 11);
    assert.ok(stats.availability > 0.9);
    assert.ok(stats.latency.avg > 0);
  });
});
