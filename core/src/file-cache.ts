import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';
import { decodeBuffer, normalizePathForComparison } from './tools/tool-utils';
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

/**
 * Why a cached file is considered stale, as classified by {@link FileCache.describeChange}:
 * - `unchanged` — not cached, or the file still matches the cached read (no action needed).
 * - `mtime`     — the file's modification time moved since the read (a user or another tool
 *                 rewrote it). The bytes may or may not differ, but the read-before-edit
 *                 contract still requires a fresh read.
 * - `content`   — the timestamp is UNCHANGED but the bytes differ (a same-second rewrite on a
 *                 coarse-timestamp filesystem, or an over-cap grow) — the classic signature of
 *                 a linter/formatter that rewrote the file in place.
 * - `missing`   — the file no longer exists on disk (deleted/moved since the read).
 */
export type FileChangeReason = 'unchanged' | 'mtime' | 'content' | 'missing';

/** Discriminated freshness result — see {@link FileCache.describeChange}. */
export interface FileChangeResult {
  changed: boolean;
  reason: FileChangeReason;
}

/**
 * Build the Claude-style "read it again" affordance message for a stale file, naming the
 * LIKELY cause (user/tool vs linter/formatter vs deletion) and the exact next action, so the
 * model recovers correctly instead of seeing a flat "modified since read". Driven by the
 * {@link FileChangeReason} from {@link FileCache.describeChange}. `verb` is the blocked action
 * (e.g. `editing`, `overwriting`).
 */
export function describeFileChangeMessage(reason: FileChangeReason, verb: string): string {
  switch (reason) {
    case 'content':
      return `File has been modified since you last read it — its timestamp is unchanged but its contents differ, which usually means a linter or formatter rewrote it in place. Read the file again before ${verb}.`;
    case 'missing':
      return `File no longer exists on disk — it was deleted or moved since you last read it. Read the current file again before ${verb}.`;
    case 'mtime':
    default:
      return `File has been modified since you last read it, either by you or by another tool. Read the file again before ${verb}.`;
  }
}

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
   *
   * Routes through {@link normalizePathForComparison}, so on a case-INSENSITIVE
   * filesystem (Windows, macOS/APFS) two spellings that differ only by letter-case
   * (or by `\` vs `/`) collapse to ONE cache entry — matching the OS, so a read
   * cached under `/Repo/A.ts` and a later edit of `/repo/a.ts` share read-state
   * instead of silently desyncing. On Linux (case-sensitive) this is `path.normalize`.
   */
  private norm(key: string): string {
    return normalizePathForComparison(key);
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
    return this.describeChange(path).changed;
  }

  /**
   * Classify WHY (and whether) a cached file is stale, returning a discriminated
   * {@link FileChangeResult} so downstream consumers (file_edit / notebook_edit) can emit the
   * Claude-style affordance that names the likely cause and the exact next action instead of a
   * flat "modified since read". {@link hasChanged} is the boolean projection of this.
   *
   * The classification mirrors the old hasChanged logic exactly:
   * - no cached entry → `unchanged` (nothing tracked).
   * - mtime-only / partial entry (no cached content): floor both sides so sub-millisecond
   *   timestamp jitter (watcher touch, fs rounding) is not a change; a whole-ms move → `mtime`.
   * - content entry, mtime moved → `mtime` (user or another tool rewrote it; safety guard
   *   rejects the edit regardless of whether bytes match).
   * - content entry, mtime stable but file grew past the byte ceiling → `content` (treated as
   *   changed WITHOUT slurping it, so a mtime-stable giant file can't become a memory/CPU DoS).
   * - content entry, mtime stable, hash differs → `content` (same-tick rewrite, e.g. a linter).
   * - content entry, hash matches → `unchanged`.
   * - ENOENT → `missing` (deleted). Known-inaccessible FS condition → `unchanged` (keep entry).
   *   Any OTHER unexpected error is surfaced, not swallowed into a false "unchanged".
   */
  describeChange(path: string): FileChangeResult {
    const key = this.norm(path);
    const cached = this.cache.get(key);
    if (!cached) return { changed: false, reason: 'unchanged' };

    try {
      const stat = fs.statSync(key);
      if (!cached.content) {
        const changed = Math.floor(stat.mtimeMs) !== Math.floor(cached.mtime);
        return { changed, reason: changed ? 'mtime' : 'unchanged' };
      }
      if (stat.mtimeMs !== cached.mtime) return { changed: true, reason: 'mtime' };
      if (stat.size > this.maxSizeBytes) return { changed: true, reason: 'content' };
      const rawBuf = fsHooks.readFileSync(key);
      const currentContent = decodeBuffer(rawBuf as Buffer).text;
      const changed = hashContent(currentContent) !== cached.contentHash;
      return { changed, reason: changed ? 'content' : 'unchanged' };
    } catch (err) {
      if (isENOENT(err)) return { changed: true, reason: 'missing' };
      if (isFsInaccessible(err)) return { changed: false, reason: 'unchanged' };
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
