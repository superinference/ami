import { Reservoir } from './utils/reservoir';

export interface ToolStats {
  tool: string;
  callCount: number;
  errorCount: number;
  totalDurationMs: number;
  avgDurationMs: number;
  minDurationMs: number;
  maxDurationMs: number;
  errorRate: number;
  /** Calls that did NOT error (callCount - errorCount). */
  successCount: number;
  /** Fraction of calls that succeeded (1 - errorRate); 0 when no calls. */
  successRate: number;
  /** Tokens spent emitting this tool's tool_use blocks (request side). */
  requestTokens: number;
  /** Tokens in this tool's tool_result payloads (result side). */
  resultTokens: number;
  /** Tail-latency percentiles from a bounded reservoir sample. */
  p50DurationMs: number;
  p95DurationMs: number;
  p99DurationMs: number;
}

const EMPTY_STATS: ToolStats = {
  tool: '',
  callCount: 0,
  errorCount: 0,
  totalDurationMs: 0,
  avgDurationMs: 0,
  minDurationMs: 0,
  maxDurationMs: 0,
  errorRate: 0,
  successCount: 0,
  successRate: 0,
  requestTokens: 0,
  resultTokens: 0,
  p50DurationMs: 0,
  p95DurationMs: 0,
  p99DurationMs: 0,
};

interface ToolEntry {
  callCount: number;
  errorCount: number;
  totalDurationMs: number;
  minDurationMs: number;
  maxDurationMs: number;
  requestTokens: number;
  resultTokens: number;
  durations: Reservoir;
}

/** Aggregate tool-usage summary across every tracked tool. */
export interface OverallToolStats {
  totalCalls: number;
  totalErrors: number;
  totalSuccesses: number;
  totalDurationMs: number;
  avgDurationMs: number;
  errorRate: number;
  successRate: number;
  totalRequestTokens: number;
  totalResultTokens: number;
  /** Number of distinct tools that have been recorded. */
  distinctTools: number;
}

/** A serializable point-in-time view of all analytics (overall + per-tool). */
export interface ToolAnalyticsSnapshot {
  overall: OverallToolStats;
  tools: ToolStats[];
}

/** Per-tool-type context-bloat remediation hint (copy-pasteable advice + estimated savings). */
export interface ContextHint {
  tool: string;
  resultTokens: number;
  /** Share of the total context tokens this tool's results occupy (0..1). */
  pctOfTotal: number;
  estimatedSavingsTokens: number;
  advice: string;
}

/** Fraction of a tool's result tokens that targeted usage could realistically save. */
const SAVINGS_FRACTION: Record<string, number> = {
  bash: 0.5,
  read: 0.3,
  file_read: 0.3,
  grep: 0.3,
  web_fetch: 0.4,
  webfetch: 0.4,
};
const GENERIC_SAVINGS_FRACTION = 0.3;
/** A tool must occupy at least this share of context before a hint is emitted. */
const HINT_THRESHOLD = 0.2;

const ADVICE: Record<string, string> = {
  bash: 'Pipe long command output through head/tail/grep so only the relevant lines enter context.',
  read: 'Read with offset/limit to load only the needed line range instead of the whole file.',
  file_read: 'Read with offset/limit to load only the needed line range instead of the whole file.',
  grep: 'Narrow grep with a glob/type filter (and output_mode=files_with_matches) to cut result size.',
  web_fetch: 'Fetch only the section you need (narrower URL/prompt) rather than whole pages.',
  webfetch: 'Fetch only the section you need (narrower URL/prompt) rather than whole pages.',
};

function toolKey(tool: string): string {
  return tool.toLowerCase();
}

/**
 * Coerce an untrusted duration into a finite, non-negative millisecond value.
 * Guards the aggregates/percentiles against NaN, ±Infinity, and negative inputs
 * that would otherwise poison totals (NaN avg) or pin `min` below zero.
 */
function sanitizeDuration(durationMs: number): number {
  return Number.isFinite(durationMs) ? Math.max(0, durationMs) : 0;
}

function newEntry(): ToolEntry {
  return {
    callCount: 0,
    errorCount: 0,
    totalDurationMs: 0,
    minDurationMs: Infinity,
    maxDurationMs: 0,
    requestTokens: 0,
    resultTokens: 0,
    durations: new Reservoir(),
  };
}

export class ToolAnalytics {
  private data = new Map<string, ToolEntry>();

  record(
    toolName: string,
    durationMs: number,
    isError: boolean,
    tokens?: { requestTokens?: number; resultTokens?: number },
  ): void {
    const key = toolKey(toolName);
    const entry = this.data.get(key) ?? newEntry();
    const d = sanitizeDuration(durationMs);
    entry.callCount++;
    if (isError) entry.errorCount++;
    entry.totalDurationMs += d;
    if (d < entry.minDurationMs) entry.minDurationMs = d;
    if (d > entry.maxDurationMs) entry.maxDurationMs = d;
    entry.durations.add(d);
    if (tokens?.requestTokens) entry.requestTokens += Math.max(0, tokens.requestTokens);
    if (tokens?.resultTokens) entry.resultTokens += Math.max(0, tokens.resultTokens);
    this.data.set(key, entry);
  }

  getStats(toolName: string): ToolStats {
    const key = toolKey(toolName);
    const entry = this.data.get(key);
    if (!entry) return { ...EMPTY_STATS, tool: key };
    const errorRate = entry.callCount > 0 ? entry.errorCount / entry.callCount : 0;
    return {
      tool: key,
      callCount: entry.callCount,
      errorCount: entry.errorCount,
      totalDurationMs: entry.totalDurationMs,
      avgDurationMs: entry.callCount > 0 ? entry.totalDurationMs / entry.callCount : 0,
      minDurationMs: entry.minDurationMs === Infinity ? 0 : entry.minDurationMs,
      maxDurationMs: entry.maxDurationMs,
      errorRate,
      successCount: entry.callCount - entry.errorCount,
      successRate: entry.callCount > 0 ? 1 - errorRate : 0,
      requestTokens: entry.requestTokens,
      resultTokens: entry.resultTokens,
      p50DurationMs: entry.durations.percentile(50),
      p95DurationMs: entry.durations.percentile(95),
      p99DurationMs: entry.durations.percentile(99),
    };
  }

  getSummary(): ToolStats[] {
    return Array.from(this.data.keys())
      .map(name => this.getStats(name))
      .sort((a, b) => b.callCount - a.callCount);
  }

  /**
   * Per-tool context-bloat remediation hints: for each tool whose result tokens
   * occupy more than {@link HINT_THRESHOLD} of total context, emit targeted,
   * copy-pasteable advice and an estimated token saving. Mirrors the reference's
   * contextSuggestions (Bash 50% / Read+Grep 30% / WebFetch 40% / generic 30%).
   */
  getContextHints(totalContextTokens: number): ContextHint[] {
    if (totalContextTokens <= 0) return [];
    const hints: ContextHint[] = [];
    for (const stats of this.getSummary()) {
      if (stats.resultTokens <= 0) continue;
      const pct = stats.resultTokens / totalContextTokens;
      if (pct < HINT_THRESHOLD) continue;
      const key = toolKey(stats.tool);
      const savingsFraction = SAVINGS_FRACTION[key] ?? GENERIC_SAVINGS_FRACTION;
      const advice = ADVICE[key] ?? `${stats.tool} results dominate context — request only the data you need.`;
      hints.push({
        tool: stats.tool,
        resultTokens: stats.resultTokens,
        pctOfTotal: pct,
        estimatedSavingsTokens: Math.round(stats.resultTokens * savingsFraction),
        advice,
      });
    }
    return hints.sort((a, b) => b.resultTokens - a.resultTokens);
  }

  get totalCalls(): number {
    let sum = 0;
    for (const e of this.data.values()) sum += e.callCount;
    return sum;
  }

  get totalErrors(): number {
    let sum = 0;
    for (const e of this.data.values()) sum += e.errorCount;
    return sum;
  }

  get totalRequestTokens(): number {
    let sum = 0;
    for (const e of this.data.values()) sum += e.requestTokens;
    return sum;
  }

  get totalResultTokens(): number {
    let sum = 0;
    for (const e of this.data.values()) sum += e.resultTokens;
    return sum;
  }

  /** Total wall-clock duration (ms) summed across every tool call. */
  get totalDurationMs(): number {
    let sum = 0;
    for (const e of this.data.values()) sum += e.totalDurationMs;
    return sum;
  }

  /**
   * Aggregate tool-usage summary across all tools in a single object — the
   * session-level view the reference surfaces alongside the per-tool breakdown.
   * All rates are 0 when nothing has been recorded (no NaN).
   */
  getOverallStats(): OverallToolStats {
    let totalCalls = 0;
    let totalErrors = 0;
    let totalDurationMs = 0;
    let totalRequestTokens = 0;
    let totalResultTokens = 0;
    let distinctTools = 0;
    for (const e of this.data.values()) {
      distinctTools++;
      totalCalls += e.callCount;
      totalErrors += e.errorCount;
      totalDurationMs += e.totalDurationMs;
      totalRequestTokens += e.requestTokens;
      totalResultTokens += e.resultTokens;
    }
    const errorRate = totalCalls > 0 ? totalErrors / totalCalls : 0;
    return {
      totalCalls,
      totalErrors,
      totalSuccesses: totalCalls - totalErrors,
      totalDurationMs,
      avgDurationMs: totalCalls > 0 ? totalDurationMs / totalCalls : 0,
      errorRate,
      successRate: totalCalls > 0 ? 1 - errorRate : 0,
      totalRequestTokens,
      totalResultTokens,
      distinctTools,
    };
  }

  /**
   * Fold another analytics instance into this one — used to roll sub-agent /
   * cross-session tool usage up into a parent aggregate. Counts, durations and
   * tokens accumulate; min/max extend; latency samples are re-observed into the
   * reservoir so percentiles stay representative.
   */
  merge(other: ToolAnalytics): void {
    for (const [key, src] of other.data) {
      const dst = this.data.get(key) ?? newEntry();
      dst.callCount += src.callCount;
      dst.errorCount += src.errorCount;
      dst.totalDurationMs += src.totalDurationMs;
      if (src.minDurationMs < dst.minDurationMs) dst.minDurationMs = src.minDurationMs;
      if (src.maxDurationMs > dst.maxDurationMs) dst.maxDurationMs = src.maxDurationMs;
      dst.requestTokens += src.requestTokens;
      dst.resultTokens += src.resultTokens;
      for (const v of src.durations.values()) dst.durations.add(v);
      this.data.set(key, dst);
    }
  }

  /** A serializable, point-in-time snapshot of the full analytics state. */
  snapshot(): ToolAnalyticsSnapshot {
    return { overall: this.getOverallStats(), tools: this.getSummary() };
  }

  reset(): void {
    this.data.clear();
  }
}
