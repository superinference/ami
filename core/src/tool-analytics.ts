export interface ToolStats {
  tool: string;
  callCount: number;
  errorCount: number;
  totalDurationMs: number;
  avgDurationMs: number;
  minDurationMs: number;
  maxDurationMs: number;
  errorRate: number;
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
};

interface ToolEntry {
  callCount: number;
  errorCount: number;
  totalDurationMs: number;
  minDurationMs: number;
  maxDurationMs: number;
}

export class ToolAnalytics {
  private data = new Map<string, ToolEntry>();

  record(toolName: string, durationMs: number, isError: boolean): void {
    const entry = this.data.get(toolName) ?? {
      callCount: 0,
      errorCount: 0,
      totalDurationMs: 0,
      minDurationMs: Infinity,
      maxDurationMs: 0,
    };
    entry.callCount++;
    if (isError) entry.errorCount++;
    entry.totalDurationMs += durationMs;
    if (durationMs < entry.minDurationMs) entry.minDurationMs = durationMs;
    if (durationMs > entry.maxDurationMs) entry.maxDurationMs = durationMs;
    this.data.set(toolName, entry);
  }

  getStats(toolName: string): ToolStats {
    const entry = this.data.get(toolName);
    if (!entry) return { ...EMPTY_STATS, tool: toolName };
    return {
      tool: toolName,
      callCount: entry.callCount,
      errorCount: entry.errorCount,
      totalDurationMs: entry.totalDurationMs,
      avgDurationMs: entry.callCount > 0 ? entry.totalDurationMs / entry.callCount : 0,
      minDurationMs: entry.minDurationMs === Infinity ? 0 : entry.minDurationMs,
      maxDurationMs: entry.maxDurationMs,
      errorRate: entry.callCount > 0 ? entry.errorCount / entry.callCount : 0,
    };
  }

  getSummary(): ToolStats[] {
    return Array.from(this.data.keys())
      .map(name => this.getStats(name))
      .sort((a, b) => b.callCount - a.callCount);
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

  reset(): void {
    this.data.clear();
  }
}
