import { CircularBuffer } from './utils/circular-buffer';

const WINDOW_SIZE = 100;

/**
 * Number of consecutive failures after which the tracker signals that the
 * provider should be treated as down (stop hammering it / trip a fallback).
 * Mirrors the "consecutive overload" circuit-breaker decision point used by
 * retry layers, as opposed to a diluted lifetime error rate.
 */
const CIRCUIT_BREAKER_THRESHOLD = 5;

export interface ProviderHealthStats {
  requestCount: number;
  errorCount: number;
  successCount: number;
  errorRate: number;
  availability: number;
  /**
   * Error rate over the most recent WINDOW_SIZE outcomes (not lifetime), so a
   * provider that was healthy for thousands of requests but is now failing
   * every call is reflected promptly instead of recovering arbitrarily slowly.
   */
  windowedErrorRate: number;
  /** Availability (1 - windowedErrorRate) over the most recent WINDOW_SIZE outcomes. */
  windowedAvailability: number;
  /** Number of failures since the last recorded success. Reset on any success. */
  consecutiveErrors: number;
  /**
   * True when consecutiveErrors has reached CIRCUIT_BREAKER_THRESHOLD — a
   * "provider is currently down, stop hammering it" signal for abort/fallback
   * decisions that a lifetime error rate alone cannot express.
   */
  shouldCircuitBreak: boolean;
  latency: {
    min: number;
    max: number;
    avg: number;
    p50: number;
    p95: number;
  };
  lastErrorTime: number | null;
  lastSuccessTime: number | null;
}

export class ProviderHealthTracker {
  private _requestCount = 0;
  private _errorCount = 0;
  private _successCount = 0;
  // Rolling windows bounded to WINDOW_SIZE via the shared O(1) ring buffer (no
  // per-overflow slice()/shift() re-allocation) — the one canonical recency-window
  // helper (No-Duplication rule).
  private _latencyWindow = new CircularBuffer<number>(WINDOW_SIZE);
  // Rolling window of recent outcomes: 1 = error, 0 = success. Bounded to
  // WINDOW_SIZE so windowed error-rate/availability track recent behavior.
  private _outcomeWindow = new CircularBuffer<number>(WINDOW_SIZE);
  private _consecutiveErrors = 0;
  private _lastErrorTime: number | null = null;
  private _lastSuccessTime: number | null = null;

  recordRequest(latencyMs: number): void {
    this._requestCount++;
    this._successCount++;
    this._consecutiveErrors = 0;
    this._lastSuccessTime = Date.now();
    this.pushLatency(latencyMs);
    this.pushOutcome(0);
  }

  recordError(latencyMs: number = 0): void {
    this._requestCount++;
    this._errorCount++;
    this._consecutiveErrors++;
    this._lastErrorTime = Date.now();
    this.pushLatency(latencyMs);
    this.pushOutcome(1);
  }

  getStats(): ProviderHealthStats {
    const total = this._requestCount;
    const errorRate = total > 0 ? this._errorCount / total : 0;

    const windowLen = this._outcomeWindow.size;
    const windowErrors =
      windowLen > 0 ? this._outcomeWindow.toArray().reduce((a, b) => a + b, 0) : 0;
    const windowedErrorRate = windowLen > 0 ? windowErrors / windowLen : 0;

    return {
      requestCount: this._requestCount,
      errorCount: this._errorCount,
      successCount: this._successCount,
      errorRate,
      availability: 1 - errorRate,
      windowedErrorRate,
      windowedAvailability: 1 - windowedErrorRate,
      consecutiveErrors: this._consecutiveErrors,
      shouldCircuitBreak: this._consecutiveErrors >= CIRCUIT_BREAKER_THRESHOLD,
      latency: this.computeLatencyStats(),
      lastErrorTime: this._lastErrorTime,
      lastSuccessTime: this._lastSuccessTime,
    };
  }

  reset(): void {
    this._requestCount = 0;
    this._errorCount = 0;
    this._successCount = 0;
    this._latencyWindow.clear();
    this._outcomeWindow.clear();
    this._consecutiveErrors = 0;
    this._lastErrorTime = null;
    this._lastSuccessTime = null;
  }

  private pushOutcome(outcome: 0 | 1): void {
    this._outcomeWindow.add(outcome);
  }

  private pushLatency(ms: number): void {
    this._latencyWindow.add(ms);
  }

  private computeLatencyStats(): ProviderHealthStats['latency'] {
    if (this._latencyWindow.size === 0) {
      return { min: 0, max: 0, avg: 0, p50: 0, p95: 0 };
    }

    const sorted = this._latencyWindow.toArray().sort((a, b) => a - b);
    const len = sorted.length;
    const sum = sorted.reduce((a, b) => a + b, 0);

    return {
      min: sorted[0],
      max: sorted[len - 1],
      avg: Math.round((sum / len) * 100) / 100,
      p50: sorted[Math.floor(len * 0.5)],
      p95: sorted[Math.min(Math.floor(len * 0.95), len - 1)],
    };
  }
}
