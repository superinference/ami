import { AsyncLocalStorage } from 'async_hooks';
import { percentile as linearPercentile, Reservoir, DEFAULT_RESERVOIR_SIZE } from './utils/reservoir';
import { formatFileSize } from './utils/format';

/**
 * Render milliseconds as seconds with one fixed decimal place (e.g. `1234` → `"1.2s"`).
 * Unlike a composite duration string, this ALWAYS keeps the decimal — use it for
 * sub-minute timings where the fractional second is meaningful (TTFT, hook durations,
 * slow-op lines). Mirrors Claude's utils/format.ts `formatSecondsShort`.
 */
export function formatSecondsShort(ms: number): string {
  return `${(ms / 1000).toFixed(1)}s`;
}

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
  /** This turn's LLM request was issued speculatively (prefetch), not user-driven. */
  isSpeculative: boolean;
  /** Where the request originated ('user' | 'speculation' | ... ; '' when unknown). */
  querySource: string;
  /** Provider message id for this turn's response ('' when not reported). */
  messageId: string;
  /** Time spent building the request before it hit the wire (ms, >= 0). */
  requestSetupMs: number;
  /** Per-retry attempt start offsets (ms from turn start); empty when single-attempt. */
  attemptStartTimes: number[];
  /** query_started → api_request_sent overhead (ms, >= 0). */
  queryOverheadMs: number;
  /** Process-start → first system message (ms, >= 0; meaningful for turn 0). */
  timeToSystemMessageMs: number;
  /** Turn start → query_started (ms, >= 0). */
  timeToQueryStartMs: number;
  /** Turn start → first response headers received (ms, >= 0) for connect-vs-first-token triage. */
  headersReceivedMs: number;
  /**
   * True when endTurn() was called with no matching beginTurn() for this index.
   * In that case startTime/endTime are equal and `duration` is 0 (not meaningful)
   * rather than silently reporting a full turn against a substituted start clock.
   */
  incomplete: boolean;
}

/** One lifecycle event emitted by {@link Profiler.timeAsync} (started/completed/failed). */
export interface TimingLifecycleEvent {
  label: string;
  phase: 'started' | 'completed' | 'failed';
  /** Elapsed ms at completion/failure (absent for the `started` event). */
  durationMs?: number;
  /** Extra data pulled from the result by `getData` (completed only). */
  data?: Record<string, unknown>;
}

/** One Chrome-Trace-Event-format record for {@link Profiler.toTraceEvents}. */
export interface TraceEvent {
  name: string;
  /** Comma-separated categories. */
  cat: string;
  /** Phase: 'X' = complete (has dur), 'i' = instant. */
  ph: 'X' | 'i';
  /** Timestamp in microseconds, anchored at the trace start. */
  ts: number;
  /** Duration in microseconds (complete events only). */
  dur?: number;
  pid: number;
  tid: number;
  args?: Record<string, unknown>;
  /** Instant-event scope ('g' global) — only set on 'i' events. */
  s?: 'g';
}

/**
 * Declarative startup phase-boundary config: each named phase is a
 * `[startCheckpoint, endCheckpoint]` pair of {@link Profiler.mark} labels, mirroring
 * Claude's startupProfiler `PHASE_DEFINITIONS`. {@link Profiler.getPhaseDurations}
 * resolves it into named elapsed-ms numbers.
 */
export type PhaseDefinitions = Record<string, readonly [string, string]>;

/** TTFT decomposed into pre-request overhead vs on-the-wire network latency. */
export interface TtftBreakdown {
  /** Total time-to-first-token (ms). */
  totalMs: number;
  /** Everything before the request hit the wire (setup + query overhead, ms). */
  preRequestMs: number;
  /** Time on the wire until the first streamed chunk (ms, clamped >= 0). */
  networkMs: number;
  /** Pre-request overhead as a percentage of total TTFT (0..100, 1 decimal). */
  preRequestPercent: number;
  /** Network latency as a percentage of total TTFT (0..100, 1 decimal). */
  networkPercent: number;
}

/** One phase of a single request's lifecycle (Request Setup / First Token / Sampling / retries). */
export interface RequestPhase {
  phase: string;
  durationMs: number;
}

/** The hierarchy level of a trace span (free-form; the well-known kinds mirror Claude). */
export type SpanType = 'interaction' | 'llm_request' | 'tool' | 'tool.execution' | 'tool.blocked_on_user' | string;

/** Opaque handle returned by {@link Profiler.beginSpan}; consumed by {@link Profiler.endSpan}. */
export interface SpanHandle {
  id: string;
  type: SpanType;
  name: string;
}

/** One node in the reconstructed span tree (nested children, memory-safe deep copy). */
export interface SpanNode {
  id: string;
  type: SpanType;
  name: string;
  /** Start time (ms, performance.now()-relative). */
  startMs: number;
  /** End time (ms); undefined while the span is still open. */
  endMs?: number;
  /** Elapsed ms (endMs - startMs); undefined while open. */
  durationMs?: number;
  /** Owning agent id ('main' when unspecified) for swimlane grouping. */
  agentId: string;
  /** Parent agent id for cross-agent (subagent/teammate) hierarchy. */
  parentAgentId?: string;
  attrs: Record<string, unknown>;
  children: SpanNode[];
}

/** One per-agent swimlane: all spans owned by an agent, with its parent-agent link. */
export interface AgentTrack {
  agentId: string;
  parentAgentId?: string;
  spans: SpanNode[];
}

/** Internal mutable span record (the tree is reconstructed on demand from these). */
interface SpanRecord {
  id: string;
  type: SpanType;
  name: string;
  startMs: number;
  endMs?: number;
  agentId: string;
  parentAgentId?: string;
  parentId?: string;
  attrs: Record<string, unknown>;
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
  /**
   * Max retained timeline checkpoints; oldest are evicted past this so a
   * long-running session's `mark()` stream can't grow unbounded (mirrors
   * Claude's perfetto MAX_EVENTS cap). Default 10000.
   */
  maxCheckpoints?: number;
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
   * Claude's withDiagnosticsTiming structured logging without re-implementing the
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
   * same clock Claude's headlessProfiler uses for its time_to_* cold-start fields —
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

  /**
   * Render the most-recent slow operations as a single human-readable line for a live
   * diagnostic bar, mirroring Claude's DevBar (`slow sync: op (1.2s) · op (0.3s)`).
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
   * columns so callers can align to their expected magnitude (Claude's
   * formatTimelineLine uses 8/7 for startup, 10/9 for query); defaults match the
   * previous hard-coded 7/6 so existing callers are unchanged.
   *
   * Memory is rendered via the shared {@link formatFileSize} humanizer — the SAME
   * helper every other size/limit message uses — so a GB-scale RSS reads as `2GB`
   * instead of the old ad-hoc heap-only `2048.0MB`, and both RSS and Heap are shown
   * (parity with Claude's profilerBase formatTimelineLine `RSS: .., Heap: ..`).
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

  /**
   * Resolve a declarative {@link PhaseDefinitions} map into named elapsed-ms numbers
   * by pairing each phase's `[startCheckpoint, endCheckpoint]` against the FIRST
   * occurrence of each checkpoint label in the recorded timeline (mirrors Claude's
   * startupProfiler PHASE_DEFINITIONS). A phase whose start OR end checkpoint was
   * never marked is omitted, so the caller only sees measurable phases.
   */
  getPhaseDurations(defs: PhaseDefinitions): Record<string, number> {
    // First-occurrence index of each checkpoint label (some fire more than once;
    // the first is the authoritative boundary, matching Claude's ordered marks).
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
   * implementation). Mirrors Claude's stats.tsx `observe`. NaN is ignored.
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
   * `<name>_count/_min/_max/_avg/_p50/_p95/_p99`. Mirrors Claude's stats.tsx `getAll()`
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
   * until reset() (mirrors Claude's sessionTracing orphaned-span cleanup sweep).
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
   * total — mirroring Claude's queryProfiler TTFT report split. Network is clamped >= 0
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
   * attemptStartTimes — mirroring Claude's perfetto per-request sub-spans so a
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
   * was recorded. Mirrors Claude's perfettoTracing exporter (UI-agnostic core).
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
   * with its nested children across all agents. Mirrors Claude's sessionTracing span
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
   * Group spans into per-agent swimlane tracks (parity with Claude's perfetto
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

/**
 * Render-performance tracker: feed it per-frame render DURATIONS (ms) via
 * {@link record} and read back {@link FpsMetrics} — the wall-clock average FPS and
 * the "1% low" FPS (frame rate at the slowest 1% of frames, the p99 frame time) —
 * via {@link getMetrics}.
 *
 * Matches Claude Code's `utils/fpsTracker.ts` algorithm EXACTLY (parity):
 *   - `averageFps` is WALL-CLOCK based — `totalFrames / (elapsedBetweenFirstAndLastRecord / 1000)` —
 *     i.e. the frame rate ACHIEVED over the recording window, not the inverse of the mean
 *     frame duration (two frames recorded 1s apart is 2fps regardless of how fast each
 *     rendered). The clock is sampled on every {@link record}; `now` is injectable for tests.
 *   - `low1PctFps` is derived from the p99 frame time via a DESCENDING sort and
 *     `p99Index = max(0, ceil(len * 0.01) - 1)` (nearest-rank on the slowest 1%).
 *   - Both are rounded to two decimals.
 *
 * Three defensive boundary guards (parity with the reference) make {@link getMetrics}
 * return `undefined` rather than a bogus/NaN/∞ reading:
 *   1. no frames recorded (empty sample window);
 *   2. the recording window has non-positive elapsed wall-clock (`<= 0`) — so the
 *      average never divides by zero or goes negative;
 *   3. a zero p99 frame time yields `low1PctFps = 0` (never `1000 / 0`).
 *
 * AMI superset over the reference: the per-frame sample buffer is BOUNDED
 * (`maxSamples`, default 1024) for long-session memory safety, while the wall-clock
 * average uses the true total frame count ({@link _totalFrames}) so bounding the p99
 * buffer never distorts the achieved-FPS denominator. The reference keeps an unbounded
 * array; this keeps memory flat without changing the numbers for a normal window.
 */
export class FpsTracker {
  private _samples: number[] = [];
  private _totalFrames = 0;
  private _firstRenderTime: number | undefined;
  private _lastRenderTime: number | undefined;
  private _max: number;
  private _now: () => number;

  constructor(opts: { maxSamples?: number; now?: () => number } = {}) {
    this._max = Math.max(1, opts.maxSamples ?? 1024);
    this._now = opts.now ?? (() => performance.now());
  }

  record(frameDurationMs: number): void {
    const now = this._now();
    if (this._firstRenderTime === undefined) this._firstRenderTime = now;
    this._lastRenderTime = now;
    this._totalFrames++;
    this._samples.push(frameDurationMs);
    if (this._samples.length > this._max) this._samples = this._samples.slice(-this._max);
  }

  getMetrics(): FpsMetrics | undefined {
    // Guard 1: no frames / no clock window recorded.
    if (
      this._samples.length === 0 ||
      this._firstRenderTime === undefined ||
      this._lastRenderTime === undefined
    ) {
      return undefined;
    }
    const totalTimeMs = this._lastRenderTime - this._firstRenderTime;
    // Guard 2: non-positive elapsed wall-clock — never divide by zero / go negative.
    if (totalTimeMs <= 0) return undefined;
    const averageFps = this._totalFrames / (totalTimeMs / 1000);
    const sorted = this._samples.slice().sort((a, b) => b - a);
    const p99Index = Math.max(0, Math.ceil(sorted.length * 0.01) - 1);
    const p99FrameTimeMs = sorted[p99Index]!;
    // Guard 3: a zero (or non-positive) p99 frame time yields 0, never 1000/0.
    const low1PctFps = p99FrameTimeMs > 0 ? 1000 / p99FrameTimeMs : 0;
    return {
      averageFps: Math.round(averageFps * 100) / 100,
      low1PctFps: Math.round(low1PctFps * 100) / 100,
    };
  }

  reset(): void {
    this._samples = [];
    this._totalFrames = 0;
    this._firstRenderTime = undefined;
    this._lastRenderTime = undefined;
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
