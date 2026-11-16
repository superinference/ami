import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';
import { decodeBuffer } from './tools/tool-utils';
import { estimateTokensForFileType } from './utils/tokens';

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
  /** Text encoding the cached `content` was decoded from (e.g. `utf-8`,
   *  `utf16le`). Stored so the mtime-hit read accelerator ({@link FileCache.getCachedRead})
   *  can return the content WITH its original encoding, letting a caller re-emit
   *  an unchanged file without re-detecting/re-decoding it from disk. Absent on
   *  mtime-only / partial / range stub entries that hold no content. */
  encoding?: string;
}

/**
 * Maximum number of bytes fed into a single SHA-256 digest. Hashing an entire
 * large / base64-encoded payload (e.g. an inlined image) copies and digests the
 * whole buffer, which can exhaust memory/CPU. Above this threshold we fold the
 * content LENGTH into the digest and hash only a bounded prefix, so the hash is
 * still change-sensitive (any length change or first-`MAX_CONTENT_HASH_SIZE`-byte
 * change is detected) while the work stays O(threshold). Mirrors upstream's
 * MAX_CONTENT_HASH_SIZE (100KB) memory-exhaustion guard.
 */
export const MAX_CONTENT_HASH_SIZE = 100 * 1024; // 100 KB

function hashContent(content: string): string {
  const h = crypto.createHash('sha256');
  if (content.length > MAX_CONTENT_HASH_SIZE) {
    // Bound the digest work: fold the full length in, then hash only a prefix.
    h.update(String(content.length));
    h.update(content.slice(0, MAX_CONTENT_HASH_SIZE));
  } else {
    h.update(content);
  }
  return h.digest('hex');
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
 * Filesystem errnos that mean "this path cannot be accessed right now" without
 * implying the cached read-state is wrong: a permission/lock/type/resource
 * condition (EACCES/EPERM denied, EBUSY/ETXTBSY locked, EISDIR path became a
 * directory, ELOOP/ENOTDIR path-component churn, E*FILE/ENOMEM/EAGAIN resource
 * exhaustion, EIO/ETIMEDOUT transient I/O). On any of these during a freshness
 * check the cached entry is KEPT (treated as unchanged) rather than reported as
 * modified, so a momentary glitch does not force a spurious re-read or fail a
 * later edit. A thrown value that is NOT one of these (and not ENOENT) is a
 * genuinely unexpected error and is surfaced to the caller rather than swallowed.
 */
const FS_INACCESSIBLE_ERRNOS = new Set([
  'EACCES', 'EPERM', 'EBUSY', 'ETXTBSY', 'EISDIR', 'ELOOP', 'ENOTDIR',
  'EMFILE', 'ENFILE', 'ENOMEM', 'EAGAIN', 'EIO', 'ETIMEDOUT',
]);

/**
 * True when `err` is a filesystem error that means the path is temporarily/structurally
 * inaccessible (see {@link FS_INACCESSIBLE_ERRNOS}) — distinct from ENOENT (deleted)
 * and from a genuinely unexpected error. Mirrors Claude Code's isENOENT-only eviction
 * while classifying the remaining FS errnos as "keep the entry, do not swallow bugs".
 */
export function isFsInaccessible(err: unknown): boolean {
  return (
    !!err &&
    typeof err === 'object' &&
    FS_INACCESSIBLE_ERRNOS.has((err as NodeJS.ErrnoException).code ?? '')
  );
}

/**
 * Indirection seam for the post-write metadata stat. Routing `setWritten`'s
 * stat through a plain mutable object lets tests simulate NFS/CSI metadata lag
 * (a stat that fails right after a successful write) without reassigning the
 * frozen ESM `fs` namespace. Production uses the real `fs.statSync`.
 */
export const fsHooks: {
  statSync: typeof fs.statSync;
  readFileSync: typeof fs.readFileSync;
} = { statSync: fs.statSync, readFileSync: fs.readFileSync };

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

  /**
   * Record a file read. The token estimate is file-type-aware: dense structured
   * content (JSON/JSONL/JSONC) is counted at ~2 chars/token, other text at ~4.
   */
  set(filePath: string, content: string, mtime: number, encoding: string = 'utf-8'): void {
    const key = this.norm(filePath);
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
      // Preserve the decode encoding so getCachedRead() can return content in
      // its original encoding on an mtime hit without re-detecting from disk.
      encoding,
      contentHash: hashContent(content),
      // File-type-aware token estimate: dense structured text (JSON/JSONL/JSONC)
      // is dominated by single-character tokens, so a generic ~4 chars/token ratio
      // badly UNDER-counts it. Estimate against the real per-file-type ratio (JSON
      // ~2 chars/token, else ~4) so the cache's token total — surfaced via
      // getTotalTokens()/getStats()/`/files` — reflects actual context cost.
      // Mirrors Claude's bytesPerTokenForFileType-aware read-state accounting.
      tokenEstimate: estimateTokensForFileType(content, path.extname(key)),
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
   *
   * When full content is cached, the CONTENT HASH is authoritative: it both
   *   (a) detects a same-mtime-tick rewrite on coarse-timestamp filesystems
   *       (ext4 1-second, NFS), and
   *   (b) suppresses a spurious "modified since read" when only the timestamp
   *       moved but the bytes are identical (atomic re-save, `touch`, a linter
   *       that rewrote identical content, `git checkout` to the same content).
   *
   * When only an mtime is tracked (no content — partial/range/track-only
   * entries), the floored mtime is compared instead. Flooring suppresses
   * sub-millisecond / watcher-touch jitter on filesystems that report
   * fractional-millisecond timestamps.
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
      // mtime-only / partial entry (no cached content to compare): floor both
      // sides so sub-millisecond timestamp jitter (watcher touch, fs rounding)
      // is not reported as a change; a whole-ms move still is.
      if (!cached.content) {
        return Math.floor(stat.mtimeMs) !== Math.floor(cached.mtime);
      }
      // Content entry: any mtime change is "modified since read" (safety guard —
      // the read-before-edit contract rejects an edit when the file's mtime moved
      // so the caller must re-read first), regardless of whether bytes match.
      if (stat.mtimeMs !== cached.mtime) return true;
      // Bounded freshness check: never read a file larger than the cache's byte
      // ceiling just to hash it. Treat an over-cap file as changed so callers
      // re-read it through the normal size-limited read path.
      if (stat.size > this.maxSizeBytes) return true;
      const rawBuf = fsHooks.readFileSync(key);
      const currentContent = decodeBuffer(rawBuf as Buffer).text;
      return hashContent(currentContent) !== cached.contentHash;
    } catch (err) {
      // Deleted file → changed (invalidate). A known-inaccessible FS condition
      // (permission/lock/type/resource) → keep the entry (not "changed"). Any
      // OTHER, genuinely unexpected error is surfaced rather than silently
      // swallowed into a false "unchanged", so real bugs are not masked.
      if (isENOENT(err)) return true;
      if (isFsInaccessible(err)) return false;
      throw err;
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
   * mtime-hit read accelerator. When the caller ALREADY has the file's current
   * mtime in hand (from a `stat` it just performed) and it matches the cached
   * entry's mtime, return the cached `content` + its original `encoding` WITHOUT
   * re-reading or re-hashing the file from disk — turning a repeat full read of
   * an unchanged (possibly large) file into an O(1) lookup. Returns `null` on a
   * miss (not cached, no cached content, or mtime differs) so the caller falls
   * back to a full disk read.
   *
   * This is the fast optimization counterpart to {@link get}: `get()` re-reads +
   * re-hashes to also catch a same-mtime-tick rewrite on coarse-timestamp
   * filesystems (used by the read-before-edit staleness guard), whereas this
   * accelerator trusts the already-statted mtime for the read-tool hot path.
   * Mirrors Claude Code's `fileReadCache` mtime-hit fast return.
   */
  getCachedRead(path: string, mtimeMs: number): { content: string; encoding: string } | null {
    const key = this.norm(path);
    const cached = this.cache.get(key);
    if (!cached || !cached.content || cached.mtime !== mtimeMs) return null;
    // LRU: a served cache hit marks the entry most-recently-used.
    this.cache.delete(key);
    this.cache.set(key, cached);
    return { content: cached.content, encoding: cached.encoding ?? 'utf-8' };
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

  /**
   * Deep-copy this cache into a new, fully independent instance, preserving the
   * size configuration (byte + entry ceilings), the accumulated byte total, and
   * the complete least-recently-used→most-recently-used order.
   *
   * Used for per-subagent read-file-state ISOLATION (clone-on-fork): a forked
   * sub-engine starts from a SNAPSHOT of its parent's cache — so it already knows
   * what the parent read and need not re-read — while its subsequent reads/edits
   * mutate only its own copy and never leak back into (or get clobbered by) the
   * parent's cache. Mirrors Claude Code's `cloneFileStateCache`, which clones the
   * parent's `readFileState` into every forked agent context.
   */
  clone(): FileCache {
    const copy = new FileCache(this.maxSizeBytes, this.maxEntries);
    for (const [key, state] of this.cache) {
      copy.cache.set(key, { ...state });
    }
    copy.totalBytes = this.totalBytes;
    return copy;
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

/**
 * Per-forked-subagent ISOLATED caches, keyed by `${cwd}\0${agentId}`. A sub-engine
 * running under an `agentId` gets its own cache — cloned from the directory's shared
 * cache on first access (clone-on-fork) and dropped via {@link releaseFileCache} when
 * the sub-agent finishes (release-on-finish) — so a sub-agent's reads/edits never
 * pollute the parent's read-before-edit state and the parent can never edit a file a
 * sub-agent changed on disk without being forced to re-read it first.
 */
const agentCaches: Map<string, FileCache> = new Map();

function agentCacheKey(cwd: string, agentId: string): string {
  return `${cwd}\0${agentId}`;
}

/**
 * Get the file cache for a working directory. When `agentId` is supplied (a forked
 * sub-engine), return that sub-agent's ISOLATED cache instead of the shared one,
 * cloning it from the directory's shared cache on first access so the child inherits
 * the parent's already-read state while keeping its own mutations private.
 */
export function getFileCache(cwd: string, agentId?: string): FileCache {
  if (agentId) {
    const key = agentCacheKey(cwd, agentId);
    let agentCache = agentCaches.get(key);
    if (!agentCache) {
      agentCache = getFileCache(cwd).clone();
      agentCaches.set(key, agentCache);
    }
    return agentCache;
  }
  let cache = cachesByDir.get(cwd);
  if (!cache) {
    cache = new FileCache();
    cachesByDir.set(cwd, cache);
  }
  return cache;
}

/**
 * Release a forked sub-agent's isolated file cache once the sub-engine finishes
 * (release-on-finish), so per-agent read-file-state does not leak or accumulate
 * across runs. A no-op when no isolated cache was created for the pair.
 */
export function releaseFileCache(cwd: string, agentId: string): void {
  agentCaches.delete(agentCacheKey(cwd, agentId));
}
