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

/**
 * Normalise a requested reservoir capacity to a finite integer ≥ 1.
 *
 * FUNCTIONAL (additive robustness): a non-finite `size` (NaN / ±Infinity) is no
 * longer propagated into `_size`. Previously `Math.max(1, Math.floor(NaN))`
 * evaluated to `NaN`, so `samples.length < NaN` was always false and the
 * reservoir silently retained *zero* samples — every percentile collapsed to 0.
 * NaN/-Infinity now fall back to `DEFAULT_RESERVOIR_SIZE`; +Infinity is likewise
 * capped to the default so the "bounded memory" invariant always holds.
 * Exported so consumers can reuse the exact clamping rule.
 */
export function normalizeReservoirSize(size: number, fallback: number = DEFAULT_RESERVOIR_SIZE): number {
  if (!Number.isFinite(size)) return Math.max(1, Math.floor(fallback));
  return Math.max(1, Math.floor(size));
}

export class Reservoir {
  private _samples: number[] = [];
  private _seen = 0;
  private readonly _size: number;
  private readonly _rand: () => number;

  constructor(size: number = DEFAULT_RESERVOIR_SIZE, rand: () => number = Math.random) {
    this._size = normalizeReservoirSize(size);
    this._rand = rand;
  }

  /**
   * Observe one value; retains a uniform sample of at most `size` values (Algorithm R).
   *
   * FUNCTIONAL (additive robustness): non-finite observations (NaN / ±Infinity)
   * are ignored — not stored and not counted. A single NaN in the sample poisons
   * the comparison-based sort in {@link percentile} (NaN compares false against
   * everything), yielding a garbage tail; ±Infinity pins the tail forever,
   * defeating the whole point of bounded sampling. Guarding here keeps the
   * canonical helper correct even when a caller forgets to pre-filter.
   */
  add(value: number): void {
    if (!Number.isFinite(value)) return;
    this._seen++;
    if (this._samples.length < this._size) {
      this._samples.push(value);
      return;
    }
    // Replace an existing sample with decreasing probability size/seen.
    const j = Math.floor(this._rand() * this._seen);
    // Guard the index: a hostile/buggy `rand` returning a non-finite or
    // out-of-range value must never write a negative/sparse/NaN index (which
    // would leak a phantom slot into values()). Only in-range writes replace.
    if (j >= 0 && j < this._size) this._samples[j] = value;
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
 *
 * FUNCTIONAL (additive robustness):
 *  - Non-finite entries (NaN / ±Infinity) are filtered before sorting. A single
 *    NaN makes the comparator return false for every comparison, leaving `sort`
 *    order-dependent and the result arbitrary; ±Infinity skews interpolation.
 *    Filtering yields the percentile of the *valid* observations, and an
 *    all-non-finite input collapses to the documented empty-sample `0`.
 *  - A non-finite `p` (NaN / ±Infinity) no longer leaks a `NaN` result:
 *    `Math.max(0, Math.min(100, NaN))` is `NaN`, which poisoned `rank`. It is
 *    now treated as the 0th percentile (the sample minimum), a safe default.
 */
export function percentile(values: number[], p: number): number {
  // Drop non-finite observations so they cannot corrupt the sort/interpolation.
  const finite = values.filter(Number.isFinite);
  const n = finite.length;
  if (n === 0) return 0;
  if (n === 1) return finite[0];
  const sorted = [...finite].sort((a, b) => a - b);
  // Treat a non-finite percentile request as the 0th percentile (minimum).
  const safeP = Number.isFinite(p) ? p : 0;
  const clamped = Math.max(0, Math.min(100, safeP));
  // Rank position in [0, n-1].
  const rank = (clamped / 100) * (n - 1);
  const lo = Math.floor(rank);
  const hi = Math.ceil(rank);
  if (lo === hi) return sorted[lo];
  const frac = rank - lo;
  return sorted[lo] + (sorted[hi] - sorted[lo]) * frac;
}
