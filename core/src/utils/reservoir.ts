/**
 * Bounded-memory reservoir sampling (Algorithm R) + percentiles.
 *
 * Canonical, single-source helper shared by every subsystem that needs a
 * memory-capped view of a potentially-unbounded metric stream (per-tool
 * latency tail, profiler timing distributions, …). Keeping N ≤ `size` samples
 * uniformly at random lets p50/p95/p99 be estimated without growing memory with
 * the number of observations — one pathological value can no longer pin `max`
 * forever and the tail is robustly characterised.
 */

export const DEFAULT_RESERVOIR_SIZE = 1024;

export class Reservoir {
  private _samples: number[] = [];
  private _seen = 0;
  private readonly _size: number;
  private readonly _rand: () => number;

  constructor(size: number = DEFAULT_RESERVOIR_SIZE, rand: () => number = Math.random) {
    this._size = Math.max(1, Math.floor(size));
    this._rand = rand;
  }

  /** Observe one value; retains a uniform sample of at most `size` values (Algorithm R). */
  add(value: number): void {
    this._seen++;
    if (this._samples.length < this._size) {
      this._samples.push(value);
      return;
    }
    // Replace an existing sample with decreasing probability size/seen.
    const j = Math.floor(this._rand() * this._seen);
    if (j < this._size) this._samples[j] = value;
  }

  /** Total number of values observed (may exceed the retained sample count). */
  get count(): number { return this._seen; }

  /** Number of values currently retained (≤ size). */
  get sampleCount(): number { return this._samples.length; }

  /** A copy of the retained samples. */
  values(): number[] { return [...this._samples]; }

  /** Percentile over the retained samples (0..100), linear-interpolated. */
  percentile(p: number): number {
    return percentile(this._samples, p);
  }

  reset(): void {
    this._samples = [];
    this._seen = 0;
  }
}

/**
 * Percentile of a numeric sample using linear interpolation between neighbouring
 * ranks (the NIST/"linear" method). `values` need not be pre-sorted. Returns 0
 * for an empty sample; `p` is clamped to [0, 100].
 */
export function percentile(values: number[], p: number): number {
  const n = values.length;
  if (n === 0) return 0;
  if (n === 1) return values[0];
  const sorted = [...values].sort((a, b) => a - b);
  const clamped = Math.max(0, Math.min(100, p));
  // Rank position in [0, n-1].
  const rank = (clamped / 100) * (n - 1);
  const lo = Math.floor(rank);
  const hi = Math.ceil(rank);
  if (lo === hi) return sorted[lo];
  const frac = rank - lo;
  return sorted[lo] + (sorted[hi] - sorted[lo]) * frac;
}
