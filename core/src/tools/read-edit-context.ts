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
 */

/** Default number of context lines to show on each side of the window center. */
export const DEFAULT_EDIT_CONTEXT_LINES = 3;

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
