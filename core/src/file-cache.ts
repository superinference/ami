import * as fs from 'fs';
import * as path from 'path';
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
  /** Line `offset` of the most recent RANGE read (offset/limit), when the last
   *  read was a windowed read rather than a full read. Enables deduping an
   *  identical repeat range read on an unchanged file. Undefined for full reads. */
  offset?: number;
  /** Line `limit` (window size) of the most recent RANGE read — see `offset`. */
  limit?: number;
}

function hashContent(content: string): string {
  return crypto.createHash('sha256').update(content).digest('hex');
}

/**
 * Distinguish a genuinely-deleted file (ENOENT) from a transient filesystem
 * glitch (EACCES churn, EBUSY, EISDIR, atomic-save tmp→rename race window,
 * network-FS hiccup). Only ENOENT should invalidate a cached read-state entry;
 * a transient error must keep the entry so a later Edit does not fail just
 * because a `stat`/`read` momentarily glitched.
 */
function isENOENT(err: unknown): boolean {
  return (
    !!err &&
    typeof err === 'object' &&
    (err as NodeJS.ErrnoException).code === 'ENOENT'
  );
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
export const MAX_CACHE_BYTES = 16 * 1024 * 1024; // 16 MB (default byte ceiling)

/**
 * Default ceiling on the NUMBER of cached entries, independent of byte size.
 * A session that reads thousands of tiny files never trips the byte budget, so
 * without this the entry count would grow unbounded. The analogous upstream
 * knob (read-file-state cache size) defaults to 100; AMI uses a more generous
 * default and exposes it as a constructor knob for callers that want tighter
 * bounds. Both the byte ceiling and the entry ceiling are enforced on every set.
 */
export const DEFAULT_MAX_ENTRIES = 4096;

export class FileCache {
  private cache: Map<string, FileState> = new Map();
  private totalBytes: number = 0;

  /**
   * @param maxSizeBytes aggregate byte ceiling for cached content (default 16 MB).
   * @param maxEntries   ceiling on the number of cached entries (default 4096).
   * Both bounds are enforced with least-recently-used eviction on every set().
   */
  constructor(
    private readonly maxSizeBytes: number = MAX_CACHE_BYTES,
    private readonly maxEntries: number = DEFAULT_MAX_ENTRIES,
  ) {}

  /**
   * Canonicalize a path key so divergent spellings of the same file collapse to
   * one entry (relative-vs-absolute redundant `/foo/../bar` segments, trailing
   * separators, `./` noise, duplicate separators). Keeps the read-before-edit /
   * staleness guard consistent regardless of how a caller spells the path.
   */
  private norm(key: string): string {
    return path.normalize(key);
  }

  /**
   * Enforce both eviction bounds (byte size and entry count) with LRU order:
   * the Map is kept in least-recently-used→most-recently-used order (set() and a
   * successful get() move an entry to the end), so eviction drops from the FRONT.
   * The just-inserted `justSet` key sits at the end and is never evicted here.
   */
  private evict(justSet: string): void {
    if (this.totalBytes <= this.maxSizeBytes && this.cache.size <= this.maxEntries) return;
    const iter = this.cache.keys();
    while (
      (this.totalBytes > this.maxSizeBytes || this.cache.size > this.maxEntries) &&
      this.cache.size > 1
    ) {
      const next = iter.next();
      if (next.done) break;
      const oldest = next.value;
      if (oldest === justSet) break; // reached the most-recently-used entry
      const entry = this.cache.get(oldest);
      if (entry) this.totalBytes -= Buffer.byteLength(entry.content, 'utf-8');
      this.cache.delete(oldest);
    }
  }

  /** Record a file read. Token estimate uses the ~4 chars/token heuristic. */
  set(path: string, content: string, mtime: number): void {
    const key = this.norm(path);
    const byteSize = Buffer.byteLength(content, 'utf-8');
    const existing = this.cache.get(key);
    if (existing) {
      this.totalBytes -= Buffer.byteLength(existing.content, 'utf-8');
    }
    // Delete-before-set so an update moves the entry to the most-recently-used
    // position (end of the Map) rather than keeping its stale insertion slot.
    this.cache.delete(key);
    this.cache.set(key, {
      path: key,
      content,
      mtime,
      contentHash: hashContent(content),
      tokenEstimate: Math.ceil(content.length / 4),
    });
    this.totalBytes += byteSize;
    this.evict(key);
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
   *
   * The content re-read is bounded: a cached file that has since grown past the
   * byte ceiling is treated as changed WITHOUT slurping it into memory to hash,
   * so a mtime-stable giant file cannot be turned into a memory/CPU DoS here.
   * A missing file (ENOENT) is "changed" (deleted); any other transient FS
   * error keeps the cached entry rather than reporting a false change.
   */
  hasChanged(path: string): boolean {
    const key = this.norm(path);
    const cached = this.cache.get(key);
    if (!cached) return false;

    try {
      const stat = fs.statSync(key);
      if (stat.mtimeMs !== cached.mtime) return true;
      if (!cached.content) return false;
      // Bounded freshness check: never read a file larger than the cache's byte
      // ceiling just to hash it. Treat an over-cap file as changed so callers
      // re-read it through the normal size-limited read path.
      if (stat.size > this.maxSizeBytes) return true;
      const rawBuf = fs.readFileSync(key);
      const currentContent = decodeBuffer(rawBuf).text;
      return hashContent(currentContent) !== cached.contentHash;
    } catch (err) {
      return isENOENT(err);
    }
  }

  /**
   * Track only the mtime of a file (e.g. after a range read where full content
   * is not available). This enables hasChanged() without caching content.
   * Does not overwrite an existing full-content entry.
   */
  trackMtime(path: string, mtime: number): void {
    const key = this.norm(path);
    if (!this.cache.has(key)) {
      this.cache.set(key, { path: key, content: '', mtime, contentHash: '', tokenEstimate: 0 });
      this.evict(key);
    }
  }

  /**
   * Record that the model has only seen a PARTIAL view of this file (e.g. a
   * size-capped partial read). Tracks the mtime (so change detection works) but
   * no content, and flags the entry partial so file_edit's full-overwrite path
   * can require an explicit full Read before clobbering unseen content.
   */
  markPartialView(path: string, mtime: number): void {
    const key = this.norm(path);
    this.cache.delete(key);
    this.cache.set(key, { path: key, content: '', mtime, contentHash: '', tokenEstimate: 0, isPartialView: true });
    this.evict(key);
  }

  /** True when the cached view of this file is only partial (see markPartialView). */
  isPartialView(path: string): boolean {
    return this.cache.get(this.norm(path))?.isPartialView === true;
  }

  /**
   * Record that the model's most recent read of this file was a specific line
   * RANGE (offset/limit). Preserves any already-cached full content (so a later
   * full re-read still dedups) while stamping the window + mtime, so an identical
   * repeat range read on an unchanged file can be short-circuited. Mirrors Claude
   * Code's readFileState range tracking.
   */
  markRangeRead(path: string, mtime: number, offset: number, limit: number): void {
    const key = this.norm(path);
    const existing = this.cache.get(key);
    this.cache.delete(key);
    this.cache.set(key, existing
      ? { ...existing, path: key, mtime, offset, limit }
      : { path: key, content: '', mtime, contentHash: '', tokenEstimate: 0, offset, limit });
    this.evict(key);
  }

  /**
   * True when the file's most recent read was an IDENTICAL range (same offset AND
   * limit), it was not a partial view, and the file is unchanged on disk since —
   * i.e. a repeat ranged read can be served from the "unchanged" stub instead of
   * re-streaming the same window. Mirrors Claude Code's range dedup short-circuit.
   */
  isRangeUnchanged(path: string, offset: number, limit: number): boolean {
    const cached = this.cache.get(this.norm(path));
    if (!cached || cached.isPartialView) return false;
    if (cached.offset !== offset || cached.limit !== limit) return false;
    return !this.hasChanged(path);
  }

  /**
   * Get cached content.
   * Returns null if the file is not cached, has changed on disk,
   * or is a mtime-only entry (no content).
   */
  get(path: string): FileState | null {
    const key = this.norm(path);
    const cached = this.cache.get(key);
    if (!cached || !cached.content) return null;
    if (this.hasChanged(key)) return null;
    // LRU: a successful read marks the entry most-recently-used.
    this.cache.delete(key);
    this.cache.set(key, cached);
    return cached;
  }

  /**
   * List the paths of every file currently tracked in the cache (read into
   * context), in least-recently-used→most-recently-used order. Powers the
   * `/files` command's enumeration of the read-file-state cache. Includes
   * mtime-only and partial-view entries, since they still represent files the
   * model has seen.
   */
  listPaths(): string[] {
    return Array.from(this.cache.keys());
  }

  /** Remove a single file from the cache (e.g. after a failed edit so the next read returns full content). */
  delete(path: string): void {
    const key = this.norm(path);
    const entry = this.cache.get(key);
    if (entry) this.totalBytes -= Buffer.byteLength(entry.content, 'utf-8');
    this.cache.delete(key);
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

  /**
   * Get cache statistics. `entries` lists the cached path keys so diagnostics /
   * introspection can enumerate exactly WHICH files are resident, not just how
   * many.
   */
  getStats(): { files: number; tokens: number; bytes: number; entries: string[] } {
    return {
      files: this.cache.size,
      tokens: this.getTotalTokens(),
      bytes: this.totalBytes,
      entries: Array.from(this.cache.keys()),
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
