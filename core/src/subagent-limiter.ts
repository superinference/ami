const DEFAULT_MAX_CONCURRENT = 20;
const DEFAULT_MAX_DEPTH = 0;

export class SubagentLimiter {
  private running = 0;
  private waitQueue: Array<{ resolve: () => void }> = [];
  readonly maxConcurrent: number;
  readonly maxDepth: number;

  constructor(maxConcurrent?: number, maxDepth?: number) {
    this.maxConcurrent = maxConcurrent ??
      (parseInt(process.env.CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS ?? '', 10) ||
      DEFAULT_MAX_CONCURRENT);
    this.maxDepth = maxDepth ??
      (parseInt(process.env.CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH ?? '', 10) ||
      DEFAULT_MAX_DEPTH);
  }

  get activeCount(): number { return this.running; }
  get queuedCount(): number { return this.waitQueue.length; }

  canSpawnAtDepth(currentDepth: number): boolean {
    return currentDepth <= this.maxDepth;
  }

  async acquire(): Promise<void> {
    if (this.running < this.maxConcurrent) {
      this.running++;
      return;
    }
    return new Promise<void>(resolve => {
      this.waitQueue.push({ resolve });
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
    this.waitQueue.length = 0;
  }
}
