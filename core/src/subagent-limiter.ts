import { parseEnvInt } from './utils/env';

const DEFAULT_MAX_CONCURRENT = 20;
const DEFAULT_MAX_DEPTH = 0;
const DEFAULT_MAX_LIFETIME = 200;

export class SubagentLimiter {
  private running = 0;
  private totalSpawned = 0;
  private waitQueue: Array<{ resolve: () => void }> = [];
  readonly maxConcurrent: number;
  readonly maxDepth: number;
  readonly maxLifetime: number;

  constructor(maxConcurrent?: number, maxDepth?: number, maxLifetime?: number) {
    this.maxConcurrent = maxConcurrent ??
      parseEnvInt(process.env.CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS, DEFAULT_MAX_CONCURRENT);
    this.maxDepth = maxDepth ??
      parseEnvInt(process.env.CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH, DEFAULT_MAX_DEPTH);
    this.maxLifetime = maxLifetime ??
      parseEnvInt(process.env.CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION, DEFAULT_MAX_LIFETIME);
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
