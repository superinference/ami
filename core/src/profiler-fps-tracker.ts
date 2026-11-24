// Split from profiler.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { percentile as linearPercentile } from './utils/reservoir';

/**
 * Render milliseconds as seconds with one fixed decimal place (e.g. `1234` → `"1.2s"`).
 * Unlike a composite duration string, this ALWAYS keeps the decimal — use it for
 * sub-minute timings where the fractional second is meaningful (TTFT, hook durations,
 * slow-op lines). Mirrors the reference's utils/format.ts `formatSecondsShort`.
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
export const SLOW_SEVERITYS = ['none', 'slow', 'very-slow', 'bottleneck'] as const;

export type SlowSeverity = (typeof SLOW_SEVERITYS)[number];

/** A UI-agnostic bottleneck affordance hint for one measured operation. */
export interface SlowWarning {
  severity: SlowSeverity;
  /** Short reason label ('' when severity is 'none'); the frontend adds its own glyph/colour. */
  text: string;
}

/** Thresholds for {@link slowWarning}; omitted fields use the reference-parity defaults. */
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
 * mirroring the reference's queryProfiler getSlowWarning. UI-agnostic: returns a severity
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

export type MemSnapshot = () => { rss: number; heapUsed: number };

export const defaultMem: MemSnapshot = () => {
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
 * the reference's startupProfiler `PHASE_DEFINITIONS`. {@link Profiler.getPhaseDurations}
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

/** The hierarchy level of a trace span (free-form; the well-known kinds mirror the reference). */
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
export interface SpanRecord {
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
   * the reference's perfetto MAX_EVENTS cap). Default 10000.
   */
  maxCheckpoints?: number;
  /** RNG injection for deterministic reservoir sampling (testability). */
  rand?: () => number;
  /**
   * Labels whose name starts with any of these prefixes are NEVER flagged as a
   * slow operation (the reference excludes the model-prompt op from its slow-sync
   * watchdog so the LLM's own latency doesn't drown the signal). Default empty.
   */
  slowExcludePrefixes?: string[];
}

/**
 * Render-performance tracker: feed it per-frame render DURATIONS (ms) via
 * {@link record} and read back {@link FpsMetrics} — the wall-clock average FPS and
 * the "1% low" FPS (frame rate at the slowest 1% of frames, the p99 frame time) —
 * via {@link getMetrics}.
 *
 * Matches the reference's `utils/fpsTracker.ts` algorithm EXACTLY (parity):
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
 * ranks — the NIST/"linear" method the reference's stats.tsx percentile() uses
 * (`index = p/100*(n-1)`; interpolate between floor/ceil). Delegates to the ONE
 * canonical implementation in utils/reservoir so core carries a single percentile
 * algorithm instead of a nearest-rank vs linear split (no-duplication). Input need
 * not be pre-sorted; non-finite entries are ignored and `p` is clamped to [0,100].
 */
export function percentile(values: number[], p: number): number {
  return linearPercentile(values, p);
}
