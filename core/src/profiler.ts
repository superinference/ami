/**
 * Simple timing profiler for key engine operations.
 *
 * Usage:
 *   const end = profiler.start('api_call');
 *   // ... do work ...
 *   end();
 */
export class Profiler {
  private timings: Map<string, number[]> = new Map();

  /**
   * Start timing a labelled operation.
   * Returns a function that, when called, records the elapsed duration.
   */
  start(label: string): () => void {
    const startTime = Date.now();
    return () => {
      const duration = Date.now() - startTime;
      const existing = this.timings.get(label) || [];
      existing.push(duration);
      this.timings.set(label, existing);
    };
  }

  /** Record a pre-measured duration for a labelled operation. */
  record(label: string, durationMs: number): void {
    const existing = this.timings.get(label) || [];
    existing.push(durationMs);
    this.timings.set(label, existing);
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

  /** Clear all recorded timings. */
  reset(): void {
    this.timings.clear();
  }
}
