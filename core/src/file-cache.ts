import * as fs from 'fs';
import * as crypto from 'crypto';
import { decodeBuffer } from './tools/tool-utils';

export interface FileState {
  path: string;
  content: string;
  mtime: number;
  contentHash: string;
  tokenEstimate: number;
  /** True when the model has only seen a PARTIAL view of this file (size-capped
   *  partial read, truncated/stripped injected view). A partial view is not a
   *  safe basis for a full overwrite — file_edit's write path requires a full
   *  Read first when this is set, to avoid clobbering content never seen. */
  isPartialView?: boolean;
}

function hashContent(content: string): string {
  return crypto.createHash('sha256').update(content).digest('hex');
}

/**
 * Indirection seam for the post-write metadata stat. Routing `setWritten`'s
 * stat through a plain mutable object lets tests simulate NFS/CSI metadata lag
 * (a stat that fails right after a successful write) without reassigning the
 * frozen ESM `fs` namespace. Production uses the real `fs.statSync`.
 */
export const fsHooks: { statSync: typeof fs.statSync } = { statSync: fs.statSync };

/**
 * Tracks files the engine has read to avoid re-sending unchanged content.
 * When a file has not changed since the last read (same mtime), callers can
 * return a short summary instead of the full content, saving tokens.
 */
export const MAX_CACHE_BYTES = 16 * 1024 * 1024; // 16 MB

export class FileCache {
  private cache: Map<string, FileState> = new Map();
  private totalBytes: number = 0;

  /** Record a file read. Token estimate uses the ~4 chars/token heuristic. */
  set(path: string, content: string, mtime: number): void {
    const byteSize = Buffer.byteLength(content, 'utf-8');
    const existing = this.cache.get(path);
    if (existing) {
      this.totalBytes -= Buffer.byteLength(existing.content, 'utf-8');
    }
    this.cache.set(path, {
      path,
      content,
      mtime,
      contentHash: hashContent(content),
      tokenEstimate: Math.ceil(content.length / 4),
    });
    this.totalBytes += byteSize;

    // Evict oldest entries (Map preserves insertion order) until under limit
    const iter = this.cache.keys();
    while (this.totalBytes > MAX_CACHE_BYTES && this.cache.size > 1) {
      const oldest = iter.next().value;
      if (oldest === undefined || oldest === path) break;
      const entry = this.cache.get(oldest);
      if (entry) this.totalBytes -= Buffer.byteLength(entry.content, 'utf-8');
      this.cache.delete(oldest);
    }
  }

  /**
   * Cache content after a successful write.
   * If stat fails (NFS/CSI metadata lag), skip caching rather than storing a
   * fake mtime — a Date.now() fallback would make the next hasChanged() see a
   * mismatch and falsely report "file modified since last read".
   * Never throws: a completed write must not be turned into an error.
   */
  setWritten(path: string, content: string): void {
    try {
      this.set(path, content, fsHooks.statSync(path).mtimeMs);
    } catch {
      // Write already succeeded; metadata can lag on network filesystems.
    }
  }

  /**
   * Check if a file has changed since the last read.
   * Compares mtime first (fast path). When mtime matches but the filesystem
   * has coarse timestamps (e.g. ext4 1-second, NFS), falls back to a content
   * hash comparison so rapid writes within the same tick are still detected.
   */
  hasChanged(path: string): boolean {
    const cached = this.cache.get(path);
    if (!cached) return false;

    try {
      const stat = fs.statSync(path);
      if (stat.mtimeMs !== cached.mtime) return true;
      if (!cached.content) return false;
      const rawBuf = fs.readFileSync(path);
      const currentContent = decodeBuffer(rawBuf).text;
      return hashContent(currentContent) !== cached.contentHash;
    } catch {
      return true;
    }
  }

  /**
   * Track only the mtime of a file (e.g. after a range read where full content
   * is not available). This enables hasChanged() without caching content.
   * Does not overwrite an existing full-content entry.
   */
  trackMtime(path: string, mtime: number): void {
    if (!this.cache.has(path)) {
      this.cache.set(path, { path, content: '', mtime, contentHash: '', tokenEstimate: 0 });
    }
  }

  /**
   * Record that the model has only seen a PARTIAL view of this file (e.g. a
   * size-capped partial read). Tracks the mtime (so change detection works) but
   * no content, and flags the entry partial so file_edit's full-overwrite path
   * can require an explicit full Read before clobbering unseen content.
   */
  markPartialView(path: string, mtime: number): void {
    this.cache.set(path, { path, content: '', mtime, contentHash: '', tokenEstimate: 0, isPartialView: true });
  }

  /** True when the cached view of this file is only partial (see markPartialView). */
  isPartialView(path: string): boolean {
    return this.cache.get(path)?.isPartialView === true;
  }

  /**
   * Get cached content.
   * Returns null if the file is not cached, has changed on disk,
   * or is a mtime-only entry (no content).
   */
  get(path: string): FileState | null {
    const cached = this.cache.get(path);
    if (!cached || !cached.content) return null;
    if (this.hasChanged(path)) return null;
    return cached;
  }

  /**
   * List the paths of every file currently tracked in the cache (read into
   * context), in insertion order. Powers the `/files` command's enumeration of
   * the read-file-state cache. Includes mtime-only and partial-view entries, since
   * they still represent files the model has seen.
   */
  listPaths(): string[] {
    return Array.from(this.cache.keys());
  }

  /** Remove a single file from the cache (e.g. after a failed edit so the next read returns full content). */
  delete(path: string): void {
    const entry = this.cache.get(path);
    if (entry) this.totalBytes -= Buffer.byteLength(entry.content, 'utf-8');
    this.cache.delete(path);
  }

  /** Clear all cached entries. */
  clear(): void {
    this.cache.clear();
    this.totalBytes = 0;
  }

  /** Get total estimated tokens across all cached files. */
  getTotalTokens(): number {
    let total = 0;
    for (const state of this.cache.values()) {
      total += state.tokenEstimate;
    }
    return total;
  }

  /** Total bytes currently cached. */
  getTotalBytes(): number {
    return this.totalBytes;
  }

  /** Get cache statistics. */
  getStats(): { files: number; tokens: number; bytes: number } {
    return {
      files: this.cache.size,
      tokens: this.getTotalTokens(),
      bytes: this.totalBytes,
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
