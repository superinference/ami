import { percentile as linearPercentile, Reservoir, DEFAULT_RESERVOIR_SIZE } from './utils/reservoir';

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

/** Column-alignment knobs for {@link Profiler.formatTimeline}. */
export interface FormatTimelineOpts {
  /** padStart width for the cumulative-ms column (default 7). */
  totalPad?: number;
  /** padStart width for the per-step delta-ms column (default 6). */
  deltaPad?: number;
}

/** A recorded slow-operation anomaly. */
export interface SlowOperation {
  operation: string;
  durationMs: number;
  timestamp: number;
  thresholdMs: number;
}

/** Severity of a per-operation bottleneck affordance hint. */
export type SlowSeverity = 'none' | 'slow' | 'very-slow' | 'bottleneck';

/** A UI-agnostic bottleneck affordance hint for one measured operation. */
export interface SlowWarning {
  severity: SlowSeverity;
  /** Short reason label ('' when severity is 'none'); the frontend adds its own glyph/colour. */
  text: string;
}

/** Thresholds for {@link slowWarning}; omitted fields use Claude-parity defaults. */
export interface SlowWarningOpts {
  /** delta over this (ms) is "slow" (default 100). */
  slowMs?: number;
  /** delta over this (ms) is "very-slow" (default 1000). */
  verySlowMs?: number;
  /** sensitivity for known-bottleneck special-casing (default 50). */
  bottleneckMs?: number;
}

/**
 * Classify one operation's elapsed time into a bottleneck affordance severity,
 * mirroring Claude's queryProfiler getSlowWarning. UI-agnostic: returns a severity
 * + short label the frontend maps to its own glyph/colour, so the threshold policy
 * lives in ONE place (core) instead of being re-encoded at each render site.
 *
 * The first-checkpoint mark (`query_user_input_received`) measures time from process
 * start, not real overhead, so it is never flagged — matching the reference.
 */
export function slowWarning(deltaMs: number, name = '', opts?: SlowWarningOpts): SlowWarning {
  if (name === 'query_user_input_received') return { severity: 'none', text: '' };
  const slowMs = opts?.slowMs ?? 100;
  const verySlowMs = opts?.verySlowMs ?? 1000;
  const bottleneckMs = opts?.bottleneckMs ?? 50;
  if (deltaMs > verySlowMs) return { severity: 'very-slow', text: 'VERY SLOW' };
  if (deltaMs > slowMs) return { severity: 'slow', text: 'SLOW' };
  // Known-bottleneck special-casing: lower sensitivity than the generic slow line.
  if (name.includes('git_status') && deltaMs > bottleneckMs) return { severity: 'bottleneck', text: 'git status' };
  if (name.includes('tool_schema') && deltaMs > bottleneckMs) return { severity: 'bottleneck', text: 'tool schemas' };
  return { severity: 'none', text: '' };
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
  /** Prompt tokens served from cache this turn (0 when not reported). */
  cacheReadTokens: number;
  /** Cache-write (cache_creation) tokens this turn (0 when not reported). */
  cacheCreationTokens: number;
  /** Fraction of prompt tokens served from cache (0..1). */
  cacheHitRate: number;
  /** Input (prompt) tokens per second over the API latency window. */
  inputTokensPerSec: number;
  /**
   * True when endTurn() was called with no matching beginTurn() for this index.
   * In that case startTime/endTime are equal and `duration` is 0 (not meaningful)
   * rather than silently reporting a full turn against a substituted start clock.
   */
  incomplete: boolean;
}

/**
 * Bounded-memory timing aggregate: keeps count/sum/min/max (and sum-of-squares for
 * stddev) EXACT while retaining at most `size` raw samples via Algorithm R, so
 * percentiles are estimated without memory growing with the number of observations.
 * Mirrors Claude's RESERVOIR_SIZE histogram store.
 */
class TimingAggregate {
  private _count = 0;
  private _sum = 0;
  private _sumSq = 0;
  private _min = Infinity;
  private _max = -Infinity;
  private _res: Reservoir;

  constructor(size: number, rand: () => number) {
    this._res = new Reservoir(size, rand);
  }

  add(value: number): void {
    this._count++;
    this._sum += value;
    this._sumSq += value * value;
    if (value < this._min) this._min = value;
    if (value > this._max) this._max = value;
    this._res.add(value);
  }

  get count(): number { return this._count; }
  get sum(): number { return this._sum; }
  samples(): number[] { return this._res.values(); }

  statistics(): TimingStatistics {
    const base = computeStatistics(this._res.values()); // percentiles over the bounded sample
    const avg = this._count > 0 ? this._sum / this._count : 0;
    const variance = this._count > 0 ? Math.max(0, this._sumSq / this._count - avg * avg) : 0;
    return {
      ...base,
      count: this._count,
      avgMs: Math.round(avg * 100) / 100,
      totalMs: this._sum,
      minMs: this._count > 0 ? this._min : 0,
      maxMs: this._count > 0 ? this._max : 0,
      stddevMs: Math.round(Math.sqrt(variance) * 100) / 100,
    };
  }
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
  /** Bounded-memory reservoir size per timing label (default 1024). */
  maxSamples?: number;
  /** Max retained per-turn records; oldest are evicted past this (default 10000). */
  maxTurns?: number;
  /** RNG injection for deterministic reservoir sampling (testability). */
  rand?: () => number;
  /**
   * Labels whose name starts with any of these prefixes are NEVER flagged as a
   * slow operation (Claude excludes the model-prompt op from its slow-sync
   * watchdog so the LLM's own latency doesn't drown the signal). Default empty.
   */
  slowExcludePrefixes?: string[];
}

export class Profiler {
  private timings: Map<string, TimingAggregate> = new Map();
  private ttftTimings: Map<string, TimingAggregate> = new Map();
  private turns: Map<number, TurnTiming> = new Map();
  private turnStartTimes: Map<number, number> = new Map();
  private _maxSamples: number;
  private _maxTurns: number;
  private _rand: () => number;

  // --- memory-annotated timeline checkpoints ---
  private _checkpoints: ProfilerCheckpoint[] = [];
  private _mem: MemSnapshot;

  // --- slow-operation detection ---
  private _slowThresholdMs: number;
  private _slowThresholds = new Map<string, number>();
  private _slowOps: SlowOperation[] = [];
  private _maxSlowOps: number;
  private _slowExcludePrefixes: string[];

  // --- generic counter / gauge / set metrics ---
  private _counters = new Map<string, number>();
  private _gauges = new Map<string, number>();
  private _sets = new Map<string, Set<string>>();

  constructor(opts?: ProfilerOpts) {
    this._slowThresholdMs = opts?.slowThresholdMs ?? 1000;
    this._maxSlowOps = Math.max(1, opts?.maxSlowOps ?? 50);
    this._slowExcludePrefixes = opts?.slowExcludePrefixes ?? [];
    this._mem = opts?.memSnapshot ?? defaultMem;
    this._maxSamples = Math.max(1, opts?.maxSamples ?? DEFAULT_RESERVOIR_SIZE);
    this._maxTurns = Math.max(1, opts?.maxTurns ?? 10_000);
    this._rand = opts?.rand ?? Math.random;
  }

  /**
   * Start timing a labelled operation.
   * Returns a function that, when called, records the elapsed duration.
   */
  start(label: string): () => void {
    // Monotonic high-resolution clock: elapsed is immune to wall-clock jumps
    // (NTP steps, DST, manual clock changes) that Date.now() is subject to.
    const startTime = performance.now();
    return () => {
      this.record(label, performance.now() - startTime);
    };
  }

  /**
   * Record a pre-measured duration for a labelled operation (bounded reservoir).
   * Defensively rejects NaN and clamps negatives to 0 so a clock anomaly (or a
   * bad caller-supplied value) can never poison the min/max/sum/sample arrays.
   */
  record(label: string, durationMs: number): void {
    if (Number.isNaN(durationMs)) return;
    const value = durationMs < 0 ? 0 : durationMs;
    let agg = this.timings.get(label);
    if (!agg) { agg = new TimingAggregate(this._maxSamples, this._rand); this.timings.set(label, agg); }
    agg.add(value);
    this._maybeFlagSlow(label, value);
  }

  /**
   * Time an async operation: records its duration whether it resolves OR throws
   * (so failure latency is still observed), then re-throws on failure. A failure
   * also bumps the `<label>.failed` counter for introspection.
   */
  async timeAsync<T>(label: string, fn: () => Promise<T>): Promise<T> {
    const start = performance.now();
    try {
      const result = await fn();
      this.record(label, performance.now() - start);
      return result;
    } catch (err) {
      this.record(label, performance.now() - start);
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
    // Excluded operations (e.g. the model-prompt op) never register as "slow":
    // their latency is expected and would drown the watchdog signal.
    if (this._slowExcludePrefixes.some(p => label.startsWith(p))) return;
    const threshold = this._slowThresholds.get(label) ?? this._slowThresholdMs;
    if (durationMs > threshold) {
      this._slowOps.push({ operation: label, durationMs, timestamp: Date.now(), thresholdMs: threshold });
      if (this._slowOps.length > this._maxSlowOps) this._slowOps = this._slowOps.slice(-this._maxSlowOps);
    }
  }

  /**
   * Recent slow operations (most-recent last). `limit` keeps only the last N;
   * `maxAgeMs` (TTL) drops entries older than that many ms relative to now, so a
   * live diagnostic surfaces only *recent* anomalies rather than the whole
   * session's backlog (mirrors Claude's DevBar recent-slow-ops window).
   */
  getSlowOperations(limit?: number, maxAgeMs?: number): SlowOperation[] {
    let all = [...this._slowOps];
    if (maxAgeMs !== undefined && maxAgeMs >= 0) {
      const cutoff = Date.now() - maxAgeMs;
      all = all.filter(op => op.timestamp >= cutoff);
    }
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

  /**
   * Render the timeline as aligned text (label, +cumulative, Δdelta, heap MB).
   * `totalPad`/`deltaPad` control the padStart widths of the cumulative and delta
   * columns so callers can align to their expected magnitude (Claude's
   * formatTimelineLine uses 8/7 for startup, 10/9 for query); defaults match the
   * previous hard-coded 7/6 so existing callers are unchanged.
   */
  formatTimeline(opts?: FormatTimelineOpts): string {
    const rows = this.getTimeline();
    if (rows.length === 0) return '(no checkpoints)';
    const totalPad = opts?.totalPad ?? 7;
    const deltaPad = opts?.deltaPad ?? 6;
    const pad = Math.max(...rows.map(r => r.label.length));
    return rows
      .map(r => `${r.label.padEnd(pad)}  ${String(r.cumulativeMs).padStart(totalPad)}ms  Δ${String(r.deltaMs).padStart(deltaPad)}ms  ${(r.heapBytes / 1_048_576).toFixed(1)}MB`)
      .join('\n');
  }

  /**
   * Derive per-phase durations by pairing `<phase>_start` / `<phase>_end` checkpoints
   * (mirrors Claude's getPhaseSummary). A phase with a start but no matching end is
   * omitted. Returns phase → elapsed ms.
   */
  getPhaseSummary(): Record<string, number> {
    const starts = new Map<string, number>();
    const out: Record<string, number> = {};
    for (const cp of this._checkpoints) {
      if (cp.label.endsWith('_start')) {
        starts.set(cp.label.slice(0, -'_start'.length), cp.time);
      } else if (cp.label.endsWith('_end')) {
        const phase = cp.label.slice(0, -'_end'.length);
        const s = starts.get(phase);
        if (s !== undefined) out[phase] = cp.time - s;
      }
    }
    return out;
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

  /** Record a time-to-first-token measurement for a labelled operation (bounded reservoir). */
  recordTtft(label: string, ttftMs: number): void {
    let agg = this.ttftTimings.get(label);
    if (!agg) { agg = new TimingAggregate(this._maxSamples, this._rand); this.ttftTimings.set(label, agg); }
    agg.add(ttftMs);
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
    /** Prompt tokens served from cache (optional; defaults to 0). */
    cacheReadTokens?: number;
    /** Cache-write tokens (optional; defaults to 0). */
    cacheCreationTokens?: number;
  }): void {
    const endTime = Date.now();
    // Boundary guard: a missing beginTurn() must be observable, not silently
    // reported as an instant turn against a substituted start clock. Use `??`
    // (not `||`, which would also discard a legitimate 0 timestamp) and flag
    // the turn incomplete. Clamp duration >= 0 so a backward wall-clock step
    // between beginTurn and endTurn can never yield a negative turn duration.
    const recordedStart = this.turnStartTimes.get(turnIndex);
    const incomplete = recordedStart === undefined;
    const startTime = recordedStart ?? endTime;
    const duration = Math.max(0, endTime - startTime);
    const streamingDuration = metrics.apiLatency - metrics.ttftMs;
    const outputTokensPerSec = streamingDuration > 0
      ? (metrics.tokensProduced / (streamingDuration / 1000))
      : 0;
    const cacheReadTokens = Math.max(0, metrics.cacheReadTokens ?? 0);
    const cacheCreationTokens = Math.max(0, metrics.cacheCreationTokens ?? 0);
    const promptTokens = metrics.tokensConsumed + cacheReadTokens;
    const cacheHitRate = promptTokens > 0 ? cacheReadTokens / promptTokens : 0;
    const inputTokensPerSec = metrics.apiLatency > 0
      ? (metrics.tokensConsumed / (metrics.apiLatency / 1000))
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
      cacheReadTokens,
      cacheCreationTokens,
      cacheHitRate,
      inputTokensPerSec,
      incomplete,
    });
    this._evictOldTurns();
  }

  /** Bound retained turns for long-running (cron) sessions: evict the oldest. */
  private _evictOldTurns(): void {
    while (this.turns.size > this._maxTurns) {
      const oldest = this.turns.keys().next().value;
      if (oldest === undefined) break;
      this.turns.delete(oldest);
    }
  }

  /**
   * Finalize any turn that called beginTurn() but never endTurn() — an engine abort,
   * crash, or teardown. Each pending turn is recorded as an `incomplete` turn with its
   * elapsed-so-far duration (mirrors Claude's closeOpenSpans), so an aborted run's
   * partial turn still appears in the report instead of vanishing, and the
   * turnStartTimes map can't leak start entries that never get an endTurn(). Clears
   * the pending-start map and returns the number of turns finalized.
   */
  closeOpenTurns(): number {
    const now = Date.now();
    let closed = 0;
    for (const [turnIndex, startTime] of this.turnStartTimes) {
      if (this.turns.has(turnIndex)) continue; // already ended normally
      this.turns.set(turnIndex, {
        turnIndex,
        startTime,
        endTime: now,
        duration: Math.max(0, now - startTime),
        apiLatency: 0,
        toolExecutionTime: 0,
        compactionTime: 0,
        tokensProduced: 0,
        tokensConsumed: 0,
        ttftMs: 0,
        outputTokensPerSec: 0,
        cacheReadTokens: 0,
        cacheCreationTokens: 0,
        cacheHitRate: 0,
        inputTokensPerSec: 0,
        incomplete: true,
      });
      closed++;
    }
    this.turnStartTimes.clear();
    this._evictOldTurns();
    return closed;
  }

  /** Get a summary of all recorded timings. */
  getSummary(): Record<string, { count: number; avgMs: number; totalMs: number }> {
    const summary: Record<string, { count: number; avgMs: number; totalMs: number }> = {};
    for (const [label, agg] of this.timings) {
      summary[label] = {
        count: agg.count,
        avgMs: Math.round(agg.sum / agg.count),
        totalMs: agg.sum,
      };
    }
    return summary;
  }

  /** Get full statistics for a specific label. */
  getStatistics(label: string): TimingStatistics | null {
    const agg = this.timings.get(label);
    if (!agg || agg.count === 0) return null;
    return agg.statistics();
  }

  /** Get full statistics for all labels. */
  getAllStatistics(): Record<string, TimingStatistics> {
    const result: Record<string, TimingStatistics> = {};
    for (const [label, agg] of this.timings) {
      if (agg.count > 0) result[label] = agg.statistics();
    }
    return result;
  }

  /** Get the (bounded) retained duration samples for all labels. */
  getRawTimings(): Map<string, number[]> {
    return new Map(Array.from(this.timings.entries()).map(([k, v]) => [k, v.samples()]));
  }

  /** Get the (bounded) retained duration samples for a single label. */
  getRawDurations(label: string): number[] {
    return this.timings.get(label)?.samples() ?? [];
  }

  /** Get TTFT statistics for a label. */
  getTtftStatistics(label: string): TimingStatistics | null {
    const agg = this.ttftTimings.get(label);
    if (!agg || agg.count === 0) return null;
    return agg.statistics();
  }

  /** Get all (bounded) TTFT retained samples. */
  getRawTtft(): Map<string, number[]> {
    return new Map(Array.from(this.ttftTimings.entries()).map(([k, v]) => [k, v.samples()]));
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

/**
 * Percentile of a numeric sample using linear interpolation between neighbouring
 * ranks — the NIST/"linear" method Claude's stats.tsx percentile() uses
 * (`index = p/100*(n-1)`; interpolate between floor/ceil). Delegates to the ONE
 * canonical implementation in utils/reservoir so core carries a single percentile
 * algorithm instead of a nearest-rank vs linear split (no-duplication). Input need
 * not be pre-sorted; non-finite entries are ignored and `p` is clamped to [0,100].
 */
export function percentile(values: number[], p: number): number {
  return linearPercentile(values, p);
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
