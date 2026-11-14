import { percentile as linearPercentile } from './utils/reservoir';

export interface TimingStatistics {
  count: number;
  avgMs: number;
  totalMs: number;
  minMs: number;
  maxMs: number;
  p50Ms: number;
  p75Ms: number;
  p95Ms: number;
  p99Ms: number;
  stddevMs: number;
}

/** A single memory-annotated timeline checkpoint. */
export interface ProfilerCheckpoint {
  label: string;
  /** Absolute wall-clock time (ms epoch) the checkpoint was marked. */
  time: number;
  /** Resident set size at the checkpoint (bytes). */
  rssBytes: number;
  /** V8 heap-used at the checkpoint (bytes). */
  heapBytes: number;
}

/** One rendered timeline row: cumulative + per-step delta + memory snapshot. */
export interface TimelineEntry {
  label: string;
  cumulativeMs: number;
  deltaMs: number;
  rssBytes: number;
  heapBytes: number;
}

/** A recorded slow-operation anomaly. */
export interface SlowOperation {
  operation: string;
  durationMs: number;
  timestamp: number;
  thresholdMs: number;
}

/** Render-performance metrics derived from frame durations. */
export interface FpsMetrics {
  averageFps: number;
  /** FPS at the slowest 1% of frames (the p99 frame time) — the "1% low" gamers quote. */
  low1PctFps: number;
}

type MemSnapshot = () => { rss: number; heapUsed: number };
const defaultMem: MemSnapshot = () => {
  try {
    const m = process.memoryUsage();
    return { rss: m.rss, heapUsed: m.heapUsed };
  } catch {
    return { rss: 0, heapUsed: 0 };
  }
};

export interface TurnTiming {
  turnIndex: number;
  startTime: number;
  endTime: number;
  duration: number;
  apiLatency: number;
  toolExecutionTime: number;
  compactionTime: number;
  tokensProduced: number;
  tokensConsumed: number;
  ttftMs: number;
  outputTokensPerSec: number;
}

/**
 * High-resolution timing profiler for engine operations.
 *
 * Tracks per-label durations with full statistical analysis,
 * per-turn breakdowns, TTFT (time-to-first-token), and streaming metrics.
 *
 * Usage:
 *   const end = profiler.start('api_call');
 *   // ... do work ...
 *   end();
 *
 *   profiler.recordTtft('api_call', 120);
 *   profiler.beginTurn(0);
 *   profiler.endTurn(0, { apiLatency: 500, ... });
 */
export interface ProfilerOpts {
  /** Default slow-operation threshold (ms). Default 1000. */
  slowThresholdMs?: number;
  /** Max retained slow-operation records. Default 50. */
  maxSlowOps?: number;
  /** Injectable memory snapshot (testability). */
  memSnapshot?: MemSnapshot;
}

export class Profiler {
  private timings: Map<string, number[]> = new Map();
  private ttftTimings: Map<string, number[]> = new Map();
  private turns: Map<number, TurnTiming> = new Map();
  private turnStartTimes: Map<number, number> = new Map();

  // --- memory-annotated timeline checkpoints ---
  private _checkpoints: ProfilerCheckpoint[] = [];
  private _mem: MemSnapshot;

  // --- slow-operation detection ---
  private _slowThresholdMs: number;
  private _slowThresholds = new Map<string, number>();
  private _slowOps: SlowOperation[] = [];
  private _maxSlowOps: number;

  // --- generic counter / gauge / set metrics ---
  private _counters = new Map<string, number>();
  private _gauges = new Map<string, number>();
  private _sets = new Map<string, Set<string>>();

  constructor(opts?: ProfilerOpts) {
    this._slowThresholdMs = opts?.slowThresholdMs ?? 1000;
    this._maxSlowOps = Math.max(1, opts?.maxSlowOps ?? 50);
    this._mem = opts?.memSnapshot ?? defaultMem;
  }

  /**
   * Start timing a labelled operation.
   * Returns a function that, when called, records the elapsed duration.
   */
  start(label: string): () => void {
    const startTime = Date.now();
    return () => {
      this.record(label, Date.now() - startTime);
    };
  }

  /** Record a pre-measured duration for a labelled operation. */
  record(label: string, durationMs: number): void {
    const existing = this.timings.get(label) || [];
    existing.push(durationMs);
    this.timings.set(label, existing);
    this._maybeFlagSlow(label, durationMs);
  }

  /**
   * Time an async operation: records its duration whether it resolves OR throws
   * (so failure latency is still observed), then re-throws on failure. A failure
   * also bumps the `<label>.failed` counter for introspection.
   */
  async timeAsync<T>(label: string, fn: () => Promise<T>): Promise<T> {
    const start = Date.now();
    try {
      const result = await fn();
      this.record(label, Date.now() - start);
      return result;
    } catch (err) {
      this.record(label, Date.now() - start);
      this.incrementCounter(`${label}.failed`);
      throw err;
    }
  }

  // --- slow-operation detection ---------------------------------------------

  /** Set the slow threshold (ms) globally, or for one label when `label` is given. */
  setSlowThreshold(ms: number, label?: string): void {
    if (label) this._slowThresholds.set(label, ms);
    else this._slowThresholdMs = ms;
  }

  private _maybeFlagSlow(label: string, durationMs: number): void {
    const threshold = this._slowThresholds.get(label) ?? this._slowThresholdMs;
    if (durationMs > threshold) {
      this._slowOps.push({ operation: label, durationMs, timestamp: Date.now(), thresholdMs: threshold });
      if (this._slowOps.length > this._maxSlowOps) this._slowOps = this._slowOps.slice(-this._maxSlowOps);
    }
  }

  /** Recent slow operations (most-recent last), optionally limited to the last `limit`. */
  getSlowOperations(limit?: number): SlowOperation[] {
    const all = [...this._slowOps];
    return limit && limit > 0 ? all.slice(-limit) : all;
  }

  // --- memory-annotated timeline --------------------------------------------

  /** Mark a named checkpoint, capturing wall-clock time + a memory snapshot. */
  mark(label: string): void {
    const m = this._mem();
    this._checkpoints.push({ label, time: Date.now(), rssBytes: m.rss, heapBytes: m.heapUsed });
  }

  /** Chronological timeline with cumulative time, per-step delta, and memory per mark. */
  getTimeline(): TimelineEntry[] {
    if (this._checkpoints.length === 0) return [];
    const t0 = this._checkpoints[0].time;
    let prev = t0;
    return this._checkpoints.map(cp => {
      const entry: TimelineEntry = {
        label: cp.label,
        cumulativeMs: cp.time - t0,
        deltaMs: cp.time - prev,
        rssBytes: cp.rssBytes,
        heapBytes: cp.heapBytes,
      };
      prev = cp.time;
      return entry;
    });
  }

  /** Render the timeline as aligned text (label, +cumulative, Δdelta, heap MB). */
  formatTimeline(): string {
    const rows = this.getTimeline();
    if (rows.length === 0) return '(no checkpoints)';
    const pad = Math.max(...rows.map(r => r.label.length));
    return rows
      .map(r => `${r.label.padEnd(pad)}  ${String(r.cumulativeMs).padStart(7)}ms  Δ${String(r.deltaMs).padStart(6)}ms  ${(r.heapBytes / 1_048_576).toFixed(1)}MB`)
      .join('\n');
  }

  // --- generic metrics ------------------------------------------------------

  incrementCounter(name: string, by = 1): void {
    this._counters.set(name, (this._counters.get(name) ?? 0) + by);
  }
  getCounter(name: string): number { return this._counters.get(name) ?? 0; }

  setGauge(name: string, value: number): void { this._gauges.set(name, value); }
  getGauge(name: string): number | undefined { return this._gauges.get(name); }

  /** Add a value to a distinct-set (cardinality) metric. */
  addToSet(name: string, value: string): void {
    const s = this._sets.get(name) ?? new Set<string>();
    s.add(value);
    this._sets.set(name, s);
  }
  getSetSize(name: string): number { return this._sets.get(name)?.size ?? 0; }

  getMetrics(): { counters: Record<string, number>; gauges: Record<string, number>; sets: Record<string, number> } {
    const counters = Object.fromEntries(this._counters);
    const gauges = Object.fromEntries(this._gauges);
    const sets: Record<string, number> = {};
    for (const [k, v] of this._sets) sets[k] = v.size;
    return { counters, gauges, sets };
  }

  /** Record a time-to-first-token measurement for a labelled operation. */
  recordTtft(label: string, ttftMs: number): void {
    const existing = this.ttftTimings.get(label) || [];
    existing.push(ttftMs);
    this.ttftTimings.set(label, existing);
  }

  /** Mark the beginning of a turn for per-turn profiling. */
  beginTurn(turnIndex: number): void {
    this.turnStartTimes.set(turnIndex, Date.now());
  }

  /** Record completed turn metrics. */
  endTurn(turnIndex: number, metrics: {
    apiLatency: number;
    toolExecutionTime: number;
    compactionTime: number;
    tokensProduced: number;
    tokensConsumed: number;
    ttftMs: number;
  }): void {
    const startTime = this.turnStartTimes.get(turnIndex) || Date.now();
    const endTime = Date.now();
    const duration = endTime - startTime;
    const streamingDuration = metrics.apiLatency - metrics.ttftMs;
    const outputTokensPerSec = streamingDuration > 0
      ? (metrics.tokensProduced / (streamingDuration / 1000))
      : 0;

    this.turns.set(turnIndex, {
      turnIndex,
      startTime,
      endTime,
      duration,
      apiLatency: metrics.apiLatency,
      toolExecutionTime: metrics.toolExecutionTime,
      compactionTime: metrics.compactionTime,
      tokensProduced: metrics.tokensProduced,
      tokensConsumed: metrics.tokensConsumed,
      ttftMs: metrics.ttftMs,
      outputTokensPerSec,
    });
  }

  /** Get a summary of all recorded timings. */
  getSummary(): Record<string, { count: number; avgMs: number; totalMs: number }> {
    const summary: Record<string, { count: number; avgMs: number; totalMs: number }> = {};
    for (const [label, durations] of this.timings) {
      const total = durations.reduce((a, b) => a + b, 0);
      summary[label] = {
        count: durations.length,
        avgMs: Math.round(total / durations.length),
        totalMs: total,
      };
    }
    return summary;
  }

  /** Get full statistics for a specific label. */
  getStatistics(label: string): TimingStatistics | null {
    const durations = this.timings.get(label);
    if (!durations || durations.length === 0) return null;
    return computeStatistics(durations);
  }

  /** Get full statistics for all labels. */
  getAllStatistics(): Record<string, TimingStatistics> {
    const result: Record<string, TimingStatistics> = {};
    for (const [label, durations] of this.timings) {
      if (durations.length > 0) {
        result[label] = computeStatistics(durations);
      }
    }
    return result;
  }

  /** Get raw duration arrays for all labels. */
  getRawTimings(): Map<string, number[]> {
    return new Map(
      Array.from(this.timings.entries()).map(([k, v]) => [k, [...v]])
    );
  }

  /** Get raw durations for a single label. */
  getRawDurations(label: string): number[] {
    return [...(this.timings.get(label) || [])];
  }

  /** Get TTFT statistics for a label. */
  getTtftStatistics(label: string): TimingStatistics | null {
    const durations = this.ttftTimings.get(label);
    if (!durations || durations.length === 0) return null;
    return computeStatistics(durations);
  }

  /** Get all TTFT raw timings. */
  getRawTtft(): Map<string, number[]> {
    return new Map(
      Array.from(this.ttftTimings.entries()).map(([k, v]) => [k, [...v]])
    );
  }

  /** Get all recorded turn timings sorted by index. */
  getTurns(): TurnTiming[] {
    return Array.from(this.turns.values()).sort((a, b) => a.turnIndex - b.turnIndex);
  }

  /** Get a specific turn's timing data. */
  getTurn(turnIndex: number): TurnTiming | undefined {
    return this.turns.get(turnIndex);
  }

  /** Clear all recorded timings. */
  reset(): void {
    this.timings.clear();
    this.ttftTimings.clear();
    this.turns.clear();
    this.turnStartTimes.clear();
    this._checkpoints = [];
    this._slowOps = [];
    this._slowThresholds.clear();
    this._counters.clear();
    this._gauges.clear();
    this._sets.clear();
  }
}

/**
 * Render-performance tracker: feed it per-frame durations (ms) and read back the
 * average FPS and the "1% low" FPS (frame rate at the slowest 1% of frames — the
 * p99 frame time). Bounded memory via the shared reservoir.
 */
export class FpsTracker {
  private _samples: number[] = [];
  private _max: number;

  constructor(maxSamples = 1024) {
    this._max = Math.max(1, maxSamples);
  }

  record(frameDurationMs: number): void {
    if (!(frameDurationMs > 0)) return;
    this._samples.push(frameDurationMs);
    if (this._samples.length > this._max) this._samples = this._samples.slice(-this._max);
  }

  getMetrics(): FpsMetrics {
    if (this._samples.length === 0) return { averageFps: 0, low1PctFps: 0 };
    const avgFrame = this._samples.reduce((a, b) => a + b, 0) / this._samples.length;
    const p99Frame = linearPercentile(this._samples, 99);
    return {
      averageFps: avgFrame > 0 ? 1000 / avgFrame : 0,
      low1PctFps: p99Frame > 0 ? 1000 / p99Frame : 0,
    };
  }

  reset(): void {
    this._samples = [];
  }
}

/** Compute percentile from a sorted array using nearest-rank method. */
export function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  if (sorted.length === 1) return sorted[0];
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
}

/** Compute full statistics from a duration array. */
export function computeStatistics(durations: number[]): TimingStatistics {
  const n = durations.length;
  if (n === 0) {
    return { count: 0, avgMs: 0, totalMs: 0, minMs: 0, maxMs: 0, p50Ms: 0, p75Ms: 0, p95Ms: 0, p99Ms: 0, stddevMs: 0 };
  }

  const total = durations.reduce((a, b) => a + b, 0);
  const avg = total / n;
  const sorted = [...durations].sort((a, b) => a - b);

  const variance = durations.reduce((sum, d) => sum + (d - avg) ** 2, 0) / n;
  const stddev = Math.sqrt(variance);

  return {
    count: n,
    avgMs: Math.round(avg * 100) / 100,
    totalMs: total,
    minMs: sorted[0],
    maxMs: sorted[n - 1],
    p50Ms: percentile(sorted, 50),
    p75Ms: percentile(sorted, 75),
    p95Ms: percentile(sorted, 95),
    p99Ms: percentile(sorted, 99),
    stddevMs: Math.round(stddev * 100) / 100,
  };
}
