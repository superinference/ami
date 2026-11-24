// Split from file-cache.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import type { FileCache } from './file-cache';

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
export const FILE_CHANGE_REASONS = ['unchanged', 'mtime', 'content', 'missing'] as const;

export type FileChangeReason = (typeof FILE_CHANGE_REASONS)[number];

/** Discriminated freshness result — see {@link FileCache.describeChange}. */
export interface FileChangeResult {
  changed: boolean;
  reason: FileChangeReason;
}

/**
 * Serialized snapshot of a {@link FileCache} — its size configuration (byte + entry
 * ceilings) plus every entry in least-recently-used→most-recently-used order.
 * Produced by {@link FileCache.dump} and consumed by {@link FileCache.load} /
 * {@link FileCache.fromDump}, so read-file-state can be snapshotted across a
 * compaction boundary or forked for an isolated sub-agent branch WITHOUT losing the
 * size config. Mirrors the reference's `FileStateCache.dump()`/`load()` serialization.
 */
export interface FileCacheDump {
  maxSizeBytes: number;
  maxEntries: number;
  entries: Array<[string, FileState]>;
}

/**
 * Build the reference-style "read it again" affordance message for a stale file, naming the
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

/**
 * Distinguish a genuinely-deleted file (ENOENT) from a transient filesystem
 * glitch (EACCES churn, EBUSY, EISDIR, atomic-save tmp→rename race window,
 * network-FS hiccup). Only ENOENT should invalidate a cached read-state entry;
 * a transient error must keep the entry so a later Edit does not fail just
 * because a `stat`/`read` momentarily glitched.
 */
export function isENOENT(err: unknown): boolean {
  return (
    !!err &&
    typeof err === 'object' &&
    (err as NodeJS.ErrnoException).code === 'ENOENT'
  );
}

/**
 * Default ceiling on the NUMBER of cached entries, independent of byte size.
 * A session that reads thousands of tiny files never trips the byte budget, so
 * without this the entry count would grow unbounded. The analogous upstream
 * knob (read-file-state cache size) defaults to 100; AMI uses a more generous
 * default and exposes it as a constructor knob for callers that want tighter
 * bounds. Both the byte ceiling and the entry ceiling are enforced on every set.
 */
export const DEFAULT_MAX_ENTRIES = 4096;

/**
 * Merge two file caches with timestamp-wins semantics, returning a NEW cache that
 * leaves both inputs untouched. Clones `first`, then overlays each of `second`'s
 * entries only where it is strictly newer (by `mtime`) than — or absent from —
 * `first`. The result inherits `first`'s size configuration. Mirrors the reference's
 * `mergeFileStateCaches(first, second)` — the primitive for reconciling two
 * independently-read read-file-state views (compaction/sub-agent branches) while
 * keeping the freshest entry per file.
 */
export function mergeFileStateCaches(first: FileCache, second: FileCache): FileCache {
  const merged = first.clone();
  merged.mergeFrom(second);
  return merged;
}

export function agentCacheKey(cwd: string, agentId: string): string {
  return `${cwd}\0${agentId}`;
}
