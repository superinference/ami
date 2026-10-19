const WINDOW_SIZE = 100;

export interface ProviderHealthStats {
  requestCount: number;
  errorCount: number;
  successCount: number;
  errorRate: number;
  availability: number;
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
  private _latencyWindow: number[] = [];
  private _lastErrorTime: number | null = null;
  private _lastSuccessTime: number | null = null;

  recordRequest(latencyMs: number): void {
    this._requestCount++;
    this._successCount++;
    this._lastSuccessTime = Date.now();
    this.pushLatency(latencyMs);
  }

  recordError(latencyMs: number = 0): void {
    this._requestCount++;
    this._errorCount++;
    this._lastErrorTime = Date.now();
    this.pushLatency(latencyMs);
  }

  getStats(): ProviderHealthStats {
    const total = this._requestCount;
    const errorRate = total > 0 ? this._errorCount / total : 0;

    return {
      requestCount: this._requestCount,
      errorCount: this._errorCount,
      successCount: this._successCount,
      errorRate,
      availability: 1 - errorRate,
      latency: this.computeLatencyStats(),
      lastErrorTime: this._lastErrorTime,
      lastSuccessTime: this._lastSuccessTime,
    };
  }

  reset(): void {
    this._requestCount = 0;
    this._errorCount = 0;
    this._successCount = 0;
    this._latencyWindow = [];
    this._lastErrorTime = null;
    this._lastSuccessTime = null;
  }

  private pushLatency(ms: number): void {
    this._latencyWindow.push(ms);
    if (this._latencyWindow.length > WINDOW_SIZE) {
      this._latencyWindow = this._latencyWindow.slice(-WINDOW_SIZE);
    }
  }

  private computeLatencyStats(): ProviderHealthStats['latency'] {
    if (this._latencyWindow.length === 0) {
      return { min: 0, max: 0, avg: 0, p50: 0, p95: 0 };
    }

    const sorted = [...this._latencyWindow].sort((a, b) => a - b);
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
