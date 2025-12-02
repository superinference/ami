import * as fs from 'fs';

export interface FileState {
  path: string;
  content: string;
  mtime: number;
  tokenEstimate: number;
}

/**
 * Tracks files the engine has read to avoid re-sending unchanged content.
 * When a file has not changed since the last read (same mtime), callers can
 * return a short summary instead of the full content, saving tokens.
 */
export class FileCache {
  private cache: Map<string, FileState> = new Map();

  /** Record a file read. Token estimate uses the ~4 chars/token heuristic. */
  set(path: string, content: string, mtime: number): void {
    this.cache.set(path, {
      path,
      content,
      mtime,
      tokenEstimate: Math.ceil(content.length / 4),
    });
  }

  /**
   * Check if a file has changed since the last read.
   * Returns true when the file's mtime on disk differs from the cached value,
   * or when the file cannot be stat'd (e.g. deleted).
   * Returns false if the file is not in the cache.
   */
  hasChanged(path: string): boolean {
    const cached = this.cache.get(path);
    if (!cached) return false;

    try {
      const stat = fs.statSync(path);
      return stat.mtimeMs !== cached.mtime;
    } catch {
      // File no longer accessible — treat as changed
      return true;
    }
  }

  /**
   * Get cached content.
   * Returns null if the file is not cached or has changed on disk.
   */
  get(path: string): FileState | null {
    const cached = this.cache.get(path);
    if (!cached) return null;
    if (this.hasChanged(path)) return null;
    return cached;
  }

  /** Clear all cached entries. */
  clear(): void {
    this.cache.clear();
  }

  /** Get total estimated tokens across all cached files. */
  getTotalTokens(): number {
    let total = 0;
    for (const state of this.cache.values()) {
      total += state.tokenEstimate;
    }
    return total;
  }

  /** Get cache statistics. */
  getStats(): { files: number; tokens: number } {
    return {
      files: this.cache.size,
      tokens: this.getTotalTokens(),
    };
  }
}

// ---------------------------------------------------------------------------
// Singleton per working directory so every tool invocation shares one cache
// ---------------------------------------------------------------------------
const cachesByDir: Map<string, FileCache> = new Map();

export function getFileCache(cwd: string): FileCache {
  let cache = cachesByDir.get(cwd);
  if (!cache) {
    cache = new FileCache();
    cachesByDir.set(cwd, cache);
  }
  return cache;
}
