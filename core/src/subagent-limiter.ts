import { parseEnvInt } from './utils/env';

const DEFAULT_MAX_CONCURRENT = 20;
const DEFAULT_MAX_DEPTH = 0;
const DEFAULT_MAX_LIFETIME = 200;

// Sane bounds for the env-configurable agent limits. A value outside its range
// (e.g. a negative concurrency, or an absurd lifetime from a typo/injection) is
// REJECTED back to the default rather than silently applied — an unvalidated
// `0` concurrency would deadlock all spawns; a negative depth would block them.
const MAX_CONCURRENT_BOUNDS = { min: 1, max: 1000 };
const MAX_DEPTH_BOUNDS = { min: 0, max: 100 };
const MAX_LIFETIME_BOUNDS = { min: 1, max: 100_000 };

/** parseEnvInt + range validation: out-of-range or invalid values fall back. */
export function parseEnvIntInRange(
  envVar: string | undefined,
  fallback: number,
  bounds: { min: number; max: number },
): number {
  const n = parseEnvInt(envVar, fallback);
  if (n < bounds.min || n > bounds.max) return fallback;
  return n;
}

export class SubagentLimiter {
  private running = 0;
  private totalSpawned = 0;
  // Each queued waiter carries BOTH resolve and reject so the limiter can settle
  // it from any drain path (slot handoff, lifetime exhaustion, or reset) — a
  // waiter must never be silently dropped, or its `await acquire()` hangs forever.
  private waitQueue: Array<{ resolve: () => void; reject: (err: Error) => void }> = [];
  readonly maxConcurrent: number;
  readonly maxDepth: number;
  readonly maxLifetime: number;

  constructor(maxConcurrent?: number, maxDepth?: number, maxLifetime?: number) {
    this.maxConcurrent = maxConcurrent ??
      parseEnvIntInRange(process.env.CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS, DEFAULT_MAX_CONCURRENT, MAX_CONCURRENT_BOUNDS);
    this.maxDepth = maxDepth ??
      parseEnvIntInRange(process.env.CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH, DEFAULT_MAX_DEPTH, MAX_DEPTH_BOUNDS);
    this.maxLifetime = maxLifetime ??
      parseEnvIntInRange(process.env.CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION, DEFAULT_MAX_LIFETIME, MAX_LIFETIME_BOUNDS);
  }

  get activeCount(): number { return this.running; }
  get queuedCount(): number { return this.waitQueue.length; }
  get lifetimeCount(): number { return this.totalSpawned; }

  /**
   * Introspection: concurrency slots not currently occupied. Never negative even
   * if `running` were ever driven above `maxConcurrent` by a double-acquire bug.
   */
  get availableSlots(): number { return Math.max(0, this.maxConcurrent - this.running); }

  /** Single source for the session-limit error so acquire paths never drift. */
  private lifetimeError(): Error {
    return new Error(
      `Session subagent limit reached (${this.maxLifetime}). ` +
      'Use /clear to reset or set CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION to increase.',
    );
  }

  canSpawnAtDepth(currentDepth: number): boolean {
    return currentDepth <= this.maxDepth;
  }

  isLifetimeBudgetExhausted(): boolean {
    return this.totalSpawned >= this.maxLifetime;
  }

  /**
   * Record a spawn against the lifetime budget without touching concurrency.
   * Used when depth/concurrency are enforced elsewhere (e.g. task.ts) and only
   * the per-session lifetime cap should be tracked by this limiter.
   */
  recordSpawn(): void {
    this.totalSpawned++;
  }

  async acquire(): Promise<void> {
    if (this.totalSpawned >= this.maxLifetime) {
      throw this.lifetimeError();
    }
    if (this.running < this.maxConcurrent) {
      this.running++;
      this.totalSpawned++;
      return;
    }
    // At the concurrency cap: queue the waiter. The lifetime budget is re-checked
    // at drain time by `release()`, since `totalSpawned` can advance while waiting.
    return new Promise<void>((resolve, reject) => {
      this.waitQueue.push({ resolve, reject });
    });
  }

  /**
   * FUNCTIONAL CHANGE (leak + hang fix): hand the freed concurrency slot to the
   * next waiter that can actually run. Previously, if the next waiter failed the
   * lifetime check at drain time, its slot was lost (`running` never decremented)
   * AND the remaining queued waiters were never drained — they hung forever. Now
   * a waiter that cannot run is rejected and the SAME slot is offered to the next
   * waiter; if none can take it, the slot is correctly returned to the free pool.
   */
  release(): void {
    while (this.waitQueue.length > 0) {
      const next = this.waitQueue.shift()!;
      if (this.totalSpawned >= this.maxLifetime) {
        // No queued waiter can ever succeed once lifetime is exhausted; reject and
        // keep draining so none are left pending, then free the slot below.
        next.reject(this.lifetimeError());
        continue;
      }
      this.totalSpawned++;
      next.resolve();
      return; // slot handed off to a live waiter
    }
    this.running = Math.max(0, this.running - 1);
  }

  /**
   * FUNCTIONAL CHANGE (additive): non-blocking acquire. Reserves and returns true
   * when a concurrency slot is free AND the lifetime budget has room; returns
   * false WITHOUT queueing when at the concurrency cap or when the lifetime budget
   * is exhausted. Lets callers surface an over-limit rejection instead of awaiting.
   */
  tryAcquire(): boolean {
    if (this.totalSpawned >= this.maxLifetime) return false;
    if (this.running < this.maxConcurrent) {
      this.running++;
      this.totalSpawned++;
      return true;
    }
    return false;
  }

  async run<T>(fn: () => Promise<T>): Promise<T> {
    await this.acquire();
    try {
      return await fn();
    } finally {
      this.release();
    }
  }

  /**
   * FUNCTIONAL CHANGE (hang fix): reject every pending queued acquisition before
   * clearing state. Previously `reset()` truncated the queue in place, so any
   * `await acquire()` that was waiting on a slot never settled and the caller hung
   * forever. Rejecting makes a reset/crash observable to awaiters.
   */
  reset(): void {
    const pending = this.waitQueue.splice(0, this.waitQueue.length);
    for (const w of pending) {
      w.reject(new Error('SubagentLimiter was reset; pending subagent acquisition cancelled.'));
    }
    this.running = 0;
    this.totalSpawned = 0;
  }
}
