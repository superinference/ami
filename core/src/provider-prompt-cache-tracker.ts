// Split from provider.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { CACHE_MISS_WARN_THRESHOLD } from './provider-generate-text-to-stream-adapter';

export class PromptCacheTracker {
  private _hits = 0;
  private _misses = 0;
  private _consecutiveMisses = 0;

  recordCacheHit(promptTokens: number, cachedTokens: number): void {
    this._hits++;
    this._consecutiveMisses = 0;
  }

  recordCacheMiss(promptTokens: number, cachedTokens: number): void {
    this._misses++;
    this._consecutiveMisses++;
  }

  getHitRate(): number {
    const total = this._hits + this._misses;
    if (total === 0) return 0;
    return this._hits / total;
  }

  getTotalRequests(): number {
    return this._hits + this._misses;
  }

  getConsecutiveMisses(): number {
    return this._consecutiveMisses;
  }

  resetConsecutiveMisses(): void {
    this._consecutiveMisses = 0;
  }

  shouldWarnAboutMisses(): boolean {
    return this._consecutiveMisses >= CACHE_MISS_WARN_THRESHOLD;
  }
}
