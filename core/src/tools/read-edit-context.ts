/**
 * Edit-context windowing helpers for the file_edit tool.
 *
 * When a string-replace edit's `old_string` is not found, dumping the first N
 * lines of the file is unhelpful for a large file whose closest match is far from
 * the top. Claude Code's `scanForContext`/`sliceContext` show a bounded window of
 * lines AROUND the match with their TRUE 1-based line numbers (re-based via
 * `adjustHunkLineNumbers`) and a `truncated` signal when the file extends beyond
 * the window. This module provides the equivalent WINDOWED-CONTEXT view over the
 * already-decoded file content: file_edit authoritatively reads the whole file
 * (bounded by the size cap) for fuzzy matching, so the value here is the focused,
 * line-numbered, truncation-aware DISPLAY around the point of interest — not a
 * read-avoidance (which cannot apply once full content is required for matching).
 *
 * It also provides {@link readFileCapped}: a memory-bounded, truncation-aware
 * incremental reader (parity: Claude Code's `readCapped` in readEditContext.ts).
 * file_edit still needs the WHOLE file in memory for fuzzy matching, so the chunked
 * NEEDLE-scan read-avoidance of Claude's scanForContext does not apply here; the
 * applicable, valuable part is the BOUNDED read — reading the existing file into
 * memory in chunks with a hard byte cap, so a file that is (or GROWS past an
 * earlier stat, a TOCTOU window) beyond the cap is reported as `truncated` instead
 * of being slurped whole into a single unbounded allocation that could OOM.
 */

import { open, type FileHandle } from 'fs/promises';
import { isENOENT } from '../utils/errno';

/** Default number of context lines to show on each side of the window center. */
export const DEFAULT_EDIT_CONTEXT_LINES = 3;

/** Chunk size for {@link readFileCapped}'s incremental reads. */
export const READ_CHUNK_SIZE = 64 * 1024;

/** The outcome of a {@link readFileCapped} call that actually opened a file. */
export interface CappedRead {
  /**
   * The file's raw bytes, up to the cap, for the caller to decode. EMPTY when
   * {@link truncated} is true — an over-cap file is not materialized.
   */
  buffer: Buffer;
  /**
   * True when the file exceeded `maxBytes` (its content is intentionally NOT
   * returned). The display-time analogue is {@link EditContextWindow.truncated};
   * here it is the read-time "file too large to load" signal.
   */
  truncated: boolean;
}

/**
 * Read the file at `path` into memory in {@link READ_CHUNK_SIZE} chunks, growing a
 * single buffer (doubling, capped at `maxBytes + READ_CHUNK_SIZE`) so a small file
 * allocates little and a large one is detected early. As soon as the bytes read
 * exceed `maxBytes` the read STOPS and returns `{ buffer: <empty>, truncated: true }`
 * — the file is never fully materialized, bounding peak memory regardless of
 * concurrent growth after any earlier `stat`. A file exactly `maxBytes` long is NOT
 * truncated. Returns `null` on ENOENT (absent file → caller treats as "new file");
 * any other open/read error propagates. The handle is always closed.
 *
 * Parity: Claude Code's `readCapped(handle)` (returns null past MAX_SCAN_BYTES);
 * AMI folds the open + ENOENT handling in and surfaces `truncated` explicitly
 * rather than overloading `null` for both "absent" and "too big".
 */
export async function readFileCapped(path: string, maxBytes: number): Promise<CappedRead | null> {
  let handle: FileHandle;
  try {
    handle = await open(path, 'r');
  } catch (e) {
    if (isENOENT(e)) return null;
    throw e;
  }
  try {
    // Start no larger than one chunk (or the cap, whichever is smaller) so a tiny
    // cap or a tiny file does not over-allocate; grow on demand below.
    let buf = Buffer.allocUnsafe(Math.min(READ_CHUNK_SIZE, Math.max(1, maxBytes)));
    let total = 0;
    for (;;) {
      if (total === buf.length) {
        // Grow, but never past maxBytes + one chunk — just enough headroom to let a
        // single read cross the cap so truncation is detected, not silently kept.
        const grown = Buffer.allocUnsafe(Math.min(buf.length * 2, maxBytes + READ_CHUNK_SIZE));
        buf.copy(grown, 0, 0, total);
        buf = grown;
      }
      const { bytesRead } = await handle.read(buf, total, buf.length - total, total);
      if (bytesRead === 0) break;
      total += bytesRead;
      if (total > maxBytes) return { buffer: Buffer.alloc(0), truncated: true };
    }
    return { buffer: buf.subarray(0, total), truncated: false };
  } finally {
    await handle.close();
  }
}

/** A single rendered line paired with its true 1-based position in the file. */
export interface EditContextLine {
  /** 1-based line number in the source file. */
  lineNumber: number;
  /** The line text (newline stripped). */
  text: string;
}

/** A bounded window of file lines around a center, with omission accounting. */
export interface EditContextWindow {
  /** The windowed lines, each carrying its true 1-based file line number. */
  lines: EditContextLine[];
  /** How many lines precede the window (omitted above); 0 when it starts at line 1. */
  omittedBefore: number;
  /** How many lines follow the window (omitted below); 0 when it reaches EOF. */
  omittedAfter: number;
  /**
   * True when the window is a partial view of the file (lines were omitted on
   * either side) — the display-time analogue of Claude's scan `truncated` flag.
   */
  truncated: boolean;
}

/**
 * Extract a bounded `±contextLines` window of lines centered on `centerLine1`
 * (1-based) from `content`, preserving each line's true 1-based number so a
 * localized edit/miss is shown at its real file position rather than re-numbered
 * from the window start. `centerLine1` is clamped into the valid line range and a
 * negative `contextLines` is treated as 0. An empty file yields an empty window.
 */
export function extractContextWindow(
  content: string,
  centerLine1: number,
  contextLines: number = DEFAULT_EDIT_CONTEXT_LINES,
): EditContextWindow {
  const all = content.split('\n');
  const total = all.length;
  // An empty string splits to [''] (one empty line); treat a truly empty file as
  // a zero-line window so callers render nothing rather than a bogus blank line.
  if (content.length === 0) {
    return { lines: [], omittedBefore: 0, omittedAfter: 0, truncated: false };
  }

  const ctx = contextLines > 0 ? contextLines : 0;
  const center = Math.min(Math.max(centerLine1, 1), total);
  const startLine = Math.max(1, center - ctx); // 1-based inclusive
  const endLine = Math.min(total, center + ctx); // 1-based inclusive

  const lines: EditContextLine[] = [];
  for (let n = startLine; n <= endLine; n++) {
    lines.push({ lineNumber: n, text: all[n - 1] });
  }

  const omittedBefore = startLine - 1;
  const omittedAfter = total - endLine;
  return {
    lines,
    omittedBefore,
    omittedAfter,
    truncated: omittedBefore > 0 || omittedAfter > 0,
  };
}

/**
 * Render an {@link EditContextWindow} as `N: <text>` numbered lines bracketed by
 * `... (K lines above/below)` omission markers, mirroring the truncation-aware
 * context block Claude shows around an edit site.
 */
export function renderContextWindow(window: EditContextWindow): string {
  const parts: string[] = [];
  if (window.omittedBefore > 0) {
    parts.push(`... (${window.omittedBefore} line${window.omittedBefore === 1 ? '' : 's'} above)`);
  }
  for (const l of window.lines) {
    parts.push(`${l.lineNumber}: ${l.text}`);
  }
  if (window.omittedAfter > 0) {
    parts.push(`... (${window.omittedAfter} line${window.omittedAfter === 1 ? '' : 's'} below)`);
  }
  return parts.join('\n');
}
