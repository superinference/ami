import { AsyncLocalStorage } from 'async_hooks';
import { DEFAULT_RESERVOIR_SIZE } from './utils/reservoir';
import { formatFileSize } from './utils/format';
import { defaultMem, formatSecondsShort } from './profiler-fps-tracker';
import type { AgentTrack, FormatTimelineOpts, MemSnapshot, PhaseDefinitions, ProfilerCheckpoint, ProfilerOpts, RequestPhase, SlowOperation, SpanHandle, SpanNode, SpanRecord, SpanType, TimelineEntry, TimingLifecycleEvent, TimingStatistics, TraceEvent, TtftBreakdown, TurnTiming } from './profiler-fps-tracker';
import { TimingAggregate } from './profiler-compute-statistics';
export { FpsTracker, SLOW_SEVERITYS, formatSecondsShort, percentile, slowWarning } from './profiler-fps-tracker';
export type { AgentTrack, FormatTimelineOpts, FpsMetrics, PhaseDefinitions, ProfilerCheckpoint, ProfilerOpts, RequestPhase, SlowOperation, SlowSeverity, SlowWarning, SlowWarningOpts, SpanHandle, SpanNode, SpanType, TimelineEntry, TimingLifecycleEvent, TimingStatistics, TraceEvent, TtftBreakdown, TurnTiming } from './profiler-fps-tracker';
export { computeStatistics } from './profiler-compute-statistics';

export class Profiler {
  private timings: Map<string, TimingAggregate> = new Map();
  private ttftTimings: Map<string, TimingAggregate> = new Map();
  private turns: Map<number, TurnTiming> = new Map();
  private turnStartTimes: Map<number, number> = new Map();
  private _maxSamples: number;
  private _maxTurns: number;
  private _maxCheckpoints: number;
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

  // --- generic counter / gauge / set / histogram metrics ---
  private _counters = new Map<string, number>();
  private _gauges = new Map<string, number>();
  private _sets = new Map<string, Set<string>>();
  private _histograms = new Map<string, TimingAggregate>();

  // --- hierarchical span tree (interaction -> llm_request/tool -> execution) ---
  private _spans = new Map<string, SpanRecord>();
  private _spanSeq = 0;
  private _spanAls = new AsyncLocalStorage<string | undefined>();

  constructor(opts?: ProfilerOpts) {
    this._slowThresholdMs = opts?.slowThresholdMs ?? 1000;
    this._maxSlowOps = Math.max(1, opts?.maxSlowOps ?? 50);
    this._slowExcludePrefixes = opts?.slowExcludePrefixes ?? [];
    this._mem = opts?.memSnapshot ?? defaultMem;
    this._maxSamples = Math.max(1, opts?.maxSamples ?? DEFAULT_RESERVOIR_SIZE);
    this._maxTurns = Math.max(1, opts?.maxTurns ?? 10_000);
    this._maxCheckpoints = Math.max(1, opts?.maxCheckpoints ?? 10_000);
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
   *
   * Optionally emits a `started → completed|failed` lifecycle (with elapsed ms and,
   * on success, data pulled from the result via `getData`) so a caller can mirror
   * the reference's withDiagnosticsTiming structured logging without re-implementing the
   * timing/try-catch per call site.
   */
  async timeAsync<T>(
    label: string,
    fn: () => Promise<T>,
    opts?: {
      /** Extract extra fields from the result for the `completed` lifecycle event. */
      getData?: (result: T) => Record<string, unknown>;
      /** Receive the started/completed/failed lifecycle events. */
      onLifecycle?: (ev: TimingLifecycleEvent) => void;
    },
  ): Promise<T> {
    const start = performance.now();
    opts?.onLifecycle?.({ label, phase: 'started' });
    try {
      const result = await fn();
      const durationMs = performance.now() - start;
      this.record(label, durationMs);
      opts?.onLifecycle?.({ label, phase: 'completed', durationMs, data: opts.getData ? opts.getData(result) : {} });
      return result;
    } catch (err) {
      const durationMs = performance.now() - start;
      this.record(label, durationMs);
      this.incrementCounter(`${label}.failed`);
      opts?.onLifecycle?.({ label, phase: 'failed', durationMs });
      throw err;
    }
  }

  // --- process-start / cold-start anchor ------------------------------------

  /**
   * Milliseconds elapsed since process (context) start. `performance.now()` is
   * measured relative to `performance.timeOrigin` (process start), so this is the
   * same clock the reference's headlessProfiler uses for its time_to_* cold-start fields —
   * monotonic and immune to wall-clock jumps.
   */
  timeSinceProcessStart(): number {
    return performance.now();
  }

  /**
   * Record the turn-0 cold-start latency: time from process start to the first
   * yielded output (system/assistant message). Returns the recorded ms so the
   * caller can also surface it directly.
   */
  recordColdStart(label = 'cold_start'): number {
    const ms = performance.now();
    this.record(label, ms);
    return ms;
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
   * session's backlog (mirrors the reference's DevBar recent-slow-ops window).
   */
  getSlowOperations(limit?: number, maxAgeMs?: number): SlowOperation[] {
    let all = [...this._slowOps];
    if (maxAgeMs !== undefined && maxAgeMs >= 0) {
      const cutoff = Date.now() - maxAgeMs;
      all = all.filter(op => op.timestamp >= cutoff);
    }
    return limit && limit > 0 ? all.slice(-limit) : all;
  }

  /**
   * Render the most-recent slow operations as a single human-readable line for a live
   * diagnostic bar, mirroring the reference's DevBar (`slow sync: op (1.2s) · op (0.3s)`).
   * Returns '' when no slow op falls in the window, so a frontend can hide the line
   * entirely. UI-agnostic: no colour/glyph — the frontend adds its own chrome.
   */
  formatSlowOperations(limit = 3, maxAgeMs?: number): string {
    const ops = this.getSlowOperations(limit, maxAgeMs);
    if (ops.length === 0) return '';
    const parts = ops.map(o => `${o.operation} (${formatSecondsShort(o.durationMs)})`);
    return `slow sync: ${parts.join(' · ')}`;
  }

  // --- memory-annotated timeline --------------------------------------------

  /** Mark a named checkpoint, capturing wall-clock time + a memory snapshot. */
  mark(label: string): void {
    const m = this._mem();
    this._checkpoints.push({ label, time: Date.now(), rssBytes: m.rss, heapBytes: m.heapUsed });
    // Bounded retention: drop the oldest checkpoints so a long session can't grow
    // the timeline without limit (parity with the perfetto MAX_EVENTS cap).
    if (this._checkpoints.length > this._maxCheckpoints) {
      this._checkpoints = this._checkpoints.slice(-this._maxCheckpoints);
    }
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
   * Render the timeline as aligned text (label, +cumulative, Δdelta, RSS/Heap).
   * `totalPad`/`deltaPad` control the padStart widths of the cumulative and delta
   * columns so callers can align to their expected magnitude (the reference's
   * formatTimelineLine uses 8/7 for startup, 10/9 for query); defaults match the
   * previous hard-coded 7/6 so existing callers are unchanged.
   *
   * Memory is rendered via the shared {@link formatFileSize} humanizer — the SAME
   * helper every other size/limit message uses — so a GB-scale RSS reads as `2GB`
   * instead of the old ad-hoc heap-only `2048.0MB`, and both RSS and Heap are shown
   * (parity with the reference's profilerBase formatTimelineLine `RSS: .., Heap: ..`).
   */
  formatTimeline(opts?: FormatTimelineOpts): string {
    const rows = this.getTimeline();
    if (rows.length === 0) return '(no checkpoints)';
    const totalPad = opts?.totalPad ?? 7;
    const deltaPad = opts?.deltaPad ?? 6;
    const pad = Math.max(...rows.map(r => r.label.length));
    return rows
      .map(r => `${r.label.padEnd(pad)}  ${String(r.cumulativeMs).padStart(totalPad)}ms  Δ${String(r.deltaMs).padStart(deltaPad)}ms  RSS: ${formatFileSize(r.rssBytes)}, Heap: ${formatFileSize(r.heapBytes)}`)
      .join('\n');
  }

  /**
   * Derive per-phase durations by pairing `<phase>_start` / `<phase>_end` checkpoints
   * (mirrors the reference's getPhaseSummary). A phase with a start but no matching end is
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

  /**
   * Resolve a declarative {@link PhaseDefinitions} map into named elapsed-ms numbers
   * by pairing each phase's `[startCheckpoint, endCheckpoint]` against the FIRST
   * occurrence of each checkpoint label in the recorded timeline (mirrors the reference's
   * startupProfiler PHASE_DEFINITIONS). A phase whose start OR end checkpoint was
   * never marked is omitted, so the caller only sees measurable phases.
   */
  getPhaseDurations(defs: PhaseDefinitions): Record<string, number> {
    // First-occurrence index of each checkpoint label (some fire more than once;
    // the first is the authoritative boundary, matching the reference's ordered marks).
    const firstTime = new Map<string, number>();
    for (const cp of this._checkpoints) {
      if (!firstTime.has(cp.label)) firstTime.set(cp.label, cp.time);
    }
    const out: Record<string, number> = {};
    for (const [phase, [startLabel, endLabel]] of Object.entries(defs)) {
      const s = firstTime.get(startLabel);
      const e = firstTime.get(endLabel);
      if (s !== undefined && e !== undefined) out[phase] = e - s;
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

  /**
   * Observe one value into a named histogram (bounded reservoir, reusing the same
   * {@link TimingAggregate} store the timing path uses so there is ONE sampling
   * implementation). Mirrors the reference's stats.tsx `observe`. NaN is ignored.
   */
  observe(name: string, value: number): void {
    if (Number.isNaN(value)) return;
    let agg = this._histograms.get(name);
    if (!agg) { agg = new TimingAggregate(this._maxSamples, this._rand); this._histograms.set(name, agg); }
    agg.add(value);
  }

  /**
   * Flat O(1) snapshot of every generic metric as one numeric record — counters and
   * gauges verbatim, sets as cardinality, and each non-empty histogram expanded into
   * `<name>_count/_min/_max/_avg/_p50/_p95/_p99`. Mirrors the reference's stats.tsx `getAll()`
   * (e.g. for a `lastSessionMetrics` dump) so a consumer needs no post-hoc aggregation.
   */
  getAll(): Record<string, number> {
    const out: Record<string, number> = {};
    for (const [k, v] of this._counters) out[k] = v;
    for (const [k, v] of this._gauges) out[k] = v;
    for (const [name, agg] of this._histograms) {
      if (agg.count === 0) continue;
      const s = agg.statistics();
      out[`${name}_count`] = s.count;
      out[`${name}_min`] = s.minMs;
      out[`${name}_max`] = s.maxMs;
      out[`${name}_avg`] = s.avgMs;
      out[`${name}_p50`] = s.p50Ms;
      out[`${name}_p95`] = s.p95Ms;
      out[`${name}_p99`] = s.p99Ms;
    }
    for (const [k, v] of this._sets) out[k] = v.size;
    return out;
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
    /** This turn's request was speculative (prefetch). Default false. */
    isSpeculative?: boolean;
    /** Request origin ('user' | 'speculation' | ...). Default ''. */
    querySource?: string;
    /** Provider message id. Default ''. */
    messageId?: string;
    /** Request-build time before the wire (ms). Clamped >= 0. Default 0. */
    requestSetupMs?: number;
    /** Per-retry attempt start offsets (ms). Default []. */
    attemptStartTimes?: number[];
    /** query_started → api_request_sent overhead (ms). Clamped >= 0. Default 0. */
    queryOverheadMs?: number;
    /** Process-start → first system message (ms). Clamped >= 0. Default 0. */
    timeToSystemMessageMs?: number;
    /** Turn start → query_started (ms). Clamped >= 0. Default 0. */
    timeToQueryStartMs?: number;
    /** Turn start → first response headers received (ms). Clamped >= 0. Default 0. */
    headersReceivedMs?: number;
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
      isSpeculative: metrics.isSpeculative ?? false,
      querySource: metrics.querySource ?? '',
      messageId: metrics.messageId ?? '',
      requestSetupMs: Math.max(0, metrics.requestSetupMs ?? 0),
      attemptStartTimes: metrics.attemptStartTimes ? [...metrics.attemptStartTimes] : [],
      queryOverheadMs: Math.max(0, metrics.queryOverheadMs ?? 0),
      timeToSystemMessageMs: Math.max(0, metrics.timeToSystemMessageMs ?? 0),
      timeToQueryStartMs: Math.max(0, metrics.timeToQueryStartMs ?? 0),
      headersReceivedMs: Math.max(0, metrics.headersReceivedMs ?? 0),
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
   * elapsed-so-far duration (mirrors the reference's closeOpenSpans), so an aborted run's
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
        isSpeculative: false,
        querySource: '',
        messageId: '',
        requestSetupMs: 0,
        attemptStartTimes: [],
        queryOverheadMs: 0,
        timeToSystemMessageMs: 0,
        timeToQueryStartMs: 0,
        headersReceivedMs: 0,
        incomplete: true,
      });
      closed++;
    }
    this.turnStartTimes.clear();
    this._evictOldTurns();
    return closed;
  }

  /**
   * TTL safety-net: evict started-but-never-ended turn records older than
   * `maxAgeMs` so an aborted/exception run can't leak `turnStartTimes` entries
   * until reset() (mirrors the reference's sessionTracing orphaned-span cleanup sweep).
   * Unlike closeOpenTurns() this DROPS the start without recording a turn — use
   * it for periodic background cleanup. A start that already ended normally is
   * never touched; a negative `maxAgeMs` is a no-op. Returns the count evicted.
   */
  sweepStaleTurns(maxAgeMs: number): number {
    if (!(maxAgeMs >= 0)) return 0;
    const cutoff = Date.now() - maxAgeMs;
    let evicted = 0;
    for (const [turnIndex, startTime] of this.turnStartTimes) {
      if (this.turns.has(turnIndex)) continue; // ended normally — keep
      if (startTime < cutoff) {
        this.turnStartTimes.delete(turnIndex);
        evicted++;
      }
    }
    return evicted;
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

  /**
   * Decompose a turn's TTFT into pre-request overhead (everything before the request
   * hit the wire = requestSetupMs + queryOverheadMs) vs network latency (the remaining
   * time on the wire until the first streamed chunk), with each as a percentage of the
   * total — mirroring the reference's queryProfiler TTFT report split. Network is clamped >= 0
   * (overhead can exceed a tiny TTFT). Returns null for an unknown turn or when TTFT
   * isn't meaningful (<= 0).
   */
  getTtftBreakdown(turnIndex: number): TtftBreakdown | null {
    const t = this.turns.get(turnIndex);
    if (!t || t.ttftMs <= 0) return null;
    const totalMs = t.ttftMs;
    const preRequestMs = t.requestSetupMs + t.queryOverheadMs;
    const networkMs = Math.max(0, totalMs - preRequestMs);
    const pct = (part: number) => Math.round((part / totalMs) * 1000) / 10;
    return {
      totalMs,
      preRequestMs,
      networkMs,
      preRequestPercent: pct(preRequestMs),
      networkPercent: pct(networkMs),
    };
  }

  /**
   * Decompose a turn's single LLM request into labelled phases — `request_setup`
   * (build time before the wire), `first_token` (setup-end → first streamed chunk),
   * `sampling` (first chunk → stream end), and a `retries` count derived from
   * attemptStartTimes — mirroring the reference's perfetto per-request sub-spans so a
   * consumer can see where the time inside a request went. Returns null for an
   * unknown turn. (ITPS/OTPS/cache-hit-rate are already on {@link TurnTiming}.)
   */
  getRequestPhases(turnIndex: number): RequestPhase[] | null {
    const t = this.turns.get(turnIndex);
    if (!t) return null;
    const phases: RequestPhase[] = [
      { phase: 'request_setup', durationMs: t.requestSetupMs },
      { phase: 'first_token', durationMs: Math.max(0, t.ttftMs - t.requestSetupMs) },
      { phase: 'sampling', durationMs: Math.max(0, t.apiLatency - t.ttftMs) },
    ];
    if (t.attemptStartTimes.length > 1) {
      phases.push({ phase: 'retries', durationMs: t.attemptStartTimes.length - 1 });
    }
    return phases;
  }

  /**
   * Export recorded checkpoints + turns as Chrome-Trace-Event-format records
   * (viewable in ui.perfetto.dev / chrome://tracing). Checkpoints become instant
   * ('i') events on a "timeline" track; each turn becomes a complete ('X') event
   * on a "turns" track carrying its per-request span params (speculative flag,
   * query source, message id, cache hit rate, token throughput). Timestamps are
   * microseconds anchored at the earliest recorded event. Returns [] when nothing
   * was recorded. Mirrors the reference's perfettoTracing exporter (UI-agnostic core).
   */
  toTraceEvents(opts?: { pid?: number }): TraceEvent[] {
    const times: number[] = this._checkpoints.map(c => c.time);
    for (const t of this.turns.values()) times.push(t.startTime);
    if (times.length === 0) return [];
    const base = Math.min(...times);
    const pid = opts?.pid ?? 1;
    const events: TraceEvent[] = [];
    for (const cp of this._checkpoints) {
      events.push({
        name: cp.label, cat: 'timeline', ph: 'i', s: 'g',
        ts: (cp.time - base) * 1000, pid, tid: 0,
        args: { rss_bytes: cp.rssBytes, heap_bytes: cp.heapBytes },
      });
    }
    for (const t of this.getTurns()) {
      events.push({
        name: `turn ${t.turnIndex}`, cat: 'turn', ph: 'X',
        ts: (t.startTime - base) * 1000, dur: t.duration * 1000, pid, tid: 1,
        args: {
          api_latency_ms: t.apiLatency,
          ttft_ms: t.ttftMs,
          tokens_produced: t.tokensProduced,
          tokens_consumed: t.tokensConsumed,
          output_tps: t.outputTokensPerSec,
          input_tps: t.inputTokensPerSec,
          cache_hit_rate: t.cacheHitRate,
          cache_read_tokens: t.cacheReadTokens,
          cache_creation_tokens: t.cacheCreationTokens,
          request_setup_ms: t.requestSetupMs,
          query_overhead_ms: t.queryOverheadMs,
          headers_received_ms: t.headersReceivedMs,
          is_speculative: t.isSpeculative,
          query_source: t.querySource,
          message_id: t.messageId,
          incomplete: t.incomplete,
        },
      });
    }
    return events;
  }

  /** Serialize {@link toTraceEvents} as a Chrome-Trace JSON string for a .json trace file. */
  toTraceJson(opts?: { pid?: number }): string {
    return JSON.stringify({ traceEvents: this.toTraceEvents(opts), displayTimeUnit: 'ms' });
  }

  // --- hierarchical span tree -----------------------------------------------

  /**
   * Open a trace span. Its parent is `opts.parentId` when given, else the span that is
   * currently active on this async call-stack (propagated via AsyncLocalStorage inside
   * {@link withSpan}) — so nested timings reconstruct a per-turn tree
   * (interaction → llm_request/tool → execution) WITHOUT threading handles. Each call
   * gets a unique id, so two parallel same-name requests are disambiguated by handle
   * (not by ordering). Returns the handle {@link endSpan} consumes.
   */
  beginSpan(type: SpanType, name: string, opts?: { agentId?: string; parentAgentId?: string; parentId?: string; attrs?: Record<string, unknown> }): SpanHandle {
    const id = `span_${++this._spanSeq}`;
    const parentId = opts?.parentId ?? this._spanAls.getStore();
    const parent = parentId !== undefined ? this._spans.get(parentId) : undefined;
    const agentId = opts?.agentId ?? parent?.agentId ?? 'main';
    this._spans.set(id, {
      id, type, name,
      startMs: performance.now(),
      agentId,
      parentAgentId: opts?.parentAgentId ?? parent?.parentAgentId,
      parentId,
      attrs: { ...(opts?.attrs ?? {}) },
    });
    return { id, type, name };
  }

  /**
   * Close a span by its handle, recording its duration and merging any final attrs.
   * Idempotent and defensive: an unknown handle or a second end() is a no-op (so a
   * double-finally or a stale handle can't corrupt the tree).
   */
  endSpan(handle: SpanHandle, attrs?: Record<string, unknown>): void {
    const rec = this._spans.get(handle.id);
    if (!rec || rec.endMs !== undefined) return;
    rec.endMs = performance.now();
    if (attrs) Object.assign(rec.attrs, attrs);
  }

  /**
   * Run `fn` inside a new span whose id is propagated via AsyncLocalStorage, so any
   * span opened during `fn` nests under it automatically. The span is closed in a
   * finally so its duration is recorded even when `fn` throws (the error re-throws).
   */
  async withSpan<T>(type: SpanType, name: string, fn: (handle: SpanHandle) => Promise<T> | T, opts?: { agentId?: string; parentAgentId?: string; attrs?: Record<string, unknown> }): Promise<T> {
    const handle = this.beginSpan(type, name, opts);
    try {
      return await this._spanAls.run(handle.id, () => fn(handle));
    } finally {
      this.endSpan(handle);
    }
  }

  /** The raw span record for a handle id (deep-copied node), or undefined. */
  getSpan(id: string): SpanNode | undefined {
    const rec = this._spans.get(id);
    return rec ? this._nodeFor(rec, () => false) : undefined;
  }

  /** Build a {@link SpanNode} for a record, recursively including children that pass `include`. */
  private _nodeFor(rec: SpanRecord, include: (child: SpanRecord) => boolean): SpanNode {
    const children: SpanNode[] = [];
    for (const r of this._spans.values()) {
      if (r.parentId === rec.id && include(r)) children.push(this._nodeFor(r, include));
    }
    return {
      id: rec.id, type: rec.type, name: rec.name,
      startMs: rec.startMs, endMs: rec.endMs,
      durationMs: rec.endMs !== undefined ? rec.endMs - rec.startMs : undefined,
      agentId: rec.agentId, parentAgentId: rec.parentAgentId,
      attrs: { ...rec.attrs },
      children,
    };
  }

  /**
   * Reconstruct the full span forest: every root span (one with no in-tree parent)
   * with its nested children across all agents. Mirrors the reference's sessionTracing span
   * tree so a report can show which llm_request/tool ran inside which interaction.
   */
  getSpanTree(): SpanNode[] {
    const roots: SpanNode[] = [];
    for (const rec of this._spans.values()) {
      if (rec.parentId === undefined || !this._spans.has(rec.parentId)) {
        roots.push(this._nodeFor(rec, () => true));
      }
    }
    return roots;
  }

  /**
   * Group spans into per-agent swimlane tracks (parity with the reference's perfetto
   * per-agent process/thread tracks). Each track holds the spans OWNED by that agent
   * (same-agent children nested; cross-agent children surface as their own agent's
   * roots) plus the agent's parent-agent link, so a swarm's teammate/subagent timings
   * can render on distinct tracks with a parent hierarchy.
   */
  getAgentTracks(): AgentTrack[] {
    const byAgent = new Map<string, AgentTrack>();
    for (const rec of this._spans.values()) {
      let track = byAgent.get(rec.agentId);
      if (!track) { track = { agentId: rec.agentId, parentAgentId: rec.parentAgentId, spans: [] }; byAgent.set(rec.agentId, track); }
      if (track.parentAgentId === undefined && rec.parentAgentId !== undefined) track.parentAgentId = rec.parentAgentId;
      // A span is a root of its agent's track when its in-tree parent belongs to a
      // DIFFERENT agent (or it has none) — same-agent parents nest it as a child.
      const parent = rec.parentId !== undefined ? this._spans.get(rec.parentId) : undefined;
      if (!parent || parent.agentId !== rec.agentId) {
        track.spans.push(this._nodeFor(rec, child => child.agentId === rec.agentId));
      }
    }
    return [...byAgent.values()];
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
    this._histograms.clear();
    this._spans.clear();
    this._spanSeq = 0;
  }
}
