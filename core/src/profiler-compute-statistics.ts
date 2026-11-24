// Split from profiler.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { Reservoir } from './utils/reservoir';
import { percentile } from './profiler-fps-tracker';
import type { TimingStatistics } from './profiler-fps-tracker';

/**
 * Bounded-memory timing aggregate: keeps count/sum/min/max (and sum-of-squares for
 * stddev) EXACT while retaining at most `size` raw samples via Algorithm R, so
 * percentiles are estimated without memory growing with the number of observations.
 * Mirrors the reference's RESERVOIR_SIZE histogram store.
 */
export class TimingAggregate {
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
