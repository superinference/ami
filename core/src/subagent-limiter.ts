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
  private waitQueue: Array<{ resolve: () => void }> = [];
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
      throw new Error(
        `Session subagent limit reached (${this.maxLifetime}). ` +
        'Use /clear to reset or set CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION to increase.',
      );
    }
    if (this.running < this.maxConcurrent) {
      this.running++;
      this.totalSpawned++;
      return;
    }
    return new Promise<void>((resolve, reject) => {
      this.waitQueue.push({
        resolve: () => {
          if (this.totalSpawned >= this.maxLifetime) {
            reject(new Error(
              `Session subagent limit reached (${this.maxLifetime}). ` +
              'Use /clear to reset or set CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION to increase.',
            ));
            return;
          }
          this.totalSpawned++;
          resolve();
        },
      });
    });
  }

  release(): void {
    if (this.waitQueue.length > 0) {
      const next = this.waitQueue.shift()!;
      next.resolve();
    } else {
      this.running = Math.max(0, this.running - 1);
    }
  }

  async run<T>(fn: () => Promise<T>): Promise<T> {
    await this.acquire();
    try {
      return await fn();
    } finally {
      this.release();
    }
  }

  reset(): void {
    this.running = 0;
    this.totalSpawned = 0;
    this.waitQueue.length = 0;
  }
}
