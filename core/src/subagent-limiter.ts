import { parseEnvInt } from './utils/env';
import { makeAbortError } from './error-classifier';

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

/**
 * parseEnvInt + range validation: out-of-range or invalid values fall back.
 * Thin wrapper over {@link parseEnvInt}'s bounds (single source of the range
 * logic — §0b.6 no duplication) that fixes both bounds as required.
 */
export function parseEnvIntInRange(
  envVar: string | undefined,
  fallback: number,
  bounds: { min: number; max: number },
): number {
  return parseEnvInt(envVar, fallback, bounds);
}

/** One parked acquire(): both settlers so any drain path can resolve it. */
interface WaitEntry {
  resolve: () => void;
  reject: (err: Error) => void;
}

export class SubagentLimiter {
  private running = 0;
  private totalSpawned = 0;
  // Each queued waiter carries BOTH resolve and reject so the limiter can settle
  // it from any drain path (slot handoff, lifetime exhaustion, reset, or abort) —
  // a waiter must never be silently dropped, or its `await acquire()` hangs forever.
  private waitQueue: WaitEntry[] = [];

  // Constructor overrides take precedence over the env vars. When absent, the
  // limits are resolved LIVE by the getters below so a long-lived limiter honours
  // runtime env/config changes (matching Claude's per-batch getMaxToolUseConcurrency()).
  private readonly maxConcurrentOverride?: number;
  private readonly maxDepthOverride?: number;
  private readonly maxLifetimeOverride?: number;

  constructor(maxConcurrent?: number, maxDepth?: number, maxLifetime?: number) {
    this.maxConcurrentOverride = maxConcurrent;
    this.maxDepthOverride = maxDepth;
    this.maxLifetimeOverride = maxLifetime;
  }

  /**
   * Runtime-reconfigurable limits: an explicit constructor value wins; otherwise
   * the env var is re-read (and range-validated) on EVERY access, so changing
   * `CLAUDE_CODE_MAX_*` between turns takes effect on an already-created limiter
   * without a restart.
   */
  get maxConcurrent(): number {
    return this.maxConcurrentOverride ??
      parseEnvIntInRange(process.env.CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS, DEFAULT_MAX_CONCURRENT, MAX_CONCURRENT_BOUNDS);
  }

  get maxDepth(): number {
    return this.maxDepthOverride ??
      parseEnvIntInRange(process.env.CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH, DEFAULT_MAX_DEPTH, MAX_DEPTH_BOUNDS);
  }

  get maxLifetime(): number {
    return this.maxLifetimeOverride ??
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

  /** Standard abort rejection for a queued acquisition cancelled via AbortSignal. */
  private abortError(): Error {
    return makeAbortError(undefined, 'Subagent acquisition aborted before a concurrency slot was available.');
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

  /**
   * FUNCTIONAL CHANGE (additive, abort parity): optional `signal` cancels a
   * QUEUED (not-yet-started) acquisition. If the signal is already aborted the
   * call rejects without taking a slot or lifetime budget; if it aborts while
   * parked, the waiter is spliced out of the queue and its Promise rejected, so
   * a cancelled spawn can never strand a concurrency slot or hang forever.
   * Mirrors Claude threading `abortController.signal` through the tool fan-out.
   */
  async acquire(signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) {
      throw this.abortError();
    }
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
      const entry: WaitEntry = { resolve, reject };
      if (signal) {
        const onAbort = () => {
          const i = this.waitQueue.indexOf(entry);
          if (i >= 0) this.waitQueue.splice(i, 1);
          reject(this.abortError());
        };
        // Settling via any drain path (handoff, lifetime reject, reset) must also
        // detach the listener so it cannot fire after the Promise is settled.
        entry.resolve = () => { signal.removeEventListener('abort', onAbort); resolve(); };
        entry.reject = (err: Error) => { signal.removeEventListener('abort', onAbort); reject(err); };
        signal.addEventListener('abort', onAbort, { once: true });
      }
      this.waitQueue.push(entry);
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

  async run<T>(fn: () => Promise<T>, signal?: AbortSignal): Promise<T> {
    await this.acquire(signal);
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
