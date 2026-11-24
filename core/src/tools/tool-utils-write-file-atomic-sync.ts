// Split from tool-utils.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';

export function writeFileAtomicSync(target: string, data: string | Buffer, opts?: { mode?: number }): void {
  // An explicit mode (create-with-exact-bits) overrides preservation; otherwise
  // preserve the existing file's permissions across the atomic replace.
  let mode: number | undefined = opts?.mode;
  if (mode === undefined) {
    try { mode = fs.statSync(target).mode; } catch { /* new file — no mode to preserve */ }
  }

  const tmp = `${target}.tmp.${process.pid}.${Date.now()}`;
  try {
    fs.writeFileSync(tmp, data, { flush: true });
    if (mode !== undefined) {
      try { fs.chmodSync(tmp, mode); } catch { /* best-effort permission preservation */ }
    }
    fs.renameSync(tmp, target);
  } catch {
    try { fs.unlinkSync(tmp); } catch { /* temp may not exist */ }
    fs.writeFileSync(target, data);
    if (mode !== undefined) {
      try { fs.chmodSync(target, mode); } catch { /* best-effort permission application */ }
    }
  }
}

export function normalizeToLf(text: string): string {
  return text.replace(/\r\n/g, '\n');
}

export function convertToLineEnding(text: string, ending: '\r\n' | '\n'): string {
  const normalized = normalizeToLf(text);
  if (ending === '\r\n') return normalized.replace(/\n/g, '\r\n');
  return normalized;
}

/**
 * Strip trailing spaces/tabs from each line while PRESERVING the exact line
 * endings (CRLF / LF / CR). Splitting on a capturing EOL regex keeps the
 * terminators as odd-indexed segments so only the even-indexed line CONTENT is
 * trimmed; a blank/whitespace-only line collapses to empty but its newline is
 * kept. Single source for the edit/write content normalization so no call site
 * re-implements it. Markdown is handled by the caller via {@link isMarkdownPath}
 * (two trailing spaces are a hard line break there, so stripping is skipped).
 */
export function stripTrailingWhitespace(text: string): string {
  const parts = text.split(/(\r\n|\n|\r)/);
  let result = '';
  for (let i = 0; i < parts.length; i++) {
    const part = parts[i];
    if (part === undefined) continue;
    result += i % 2 === 0 ? part.replace(/[ \t]+$/, '') : part;
  }
  return result;
}

/**
 * Markdown files (.md/.mdx) use two trailing spaces as a hard line break, so
 * trailing-whitespace stripping must be skipped for them. Case-insensitive.
 */
export function isMarkdownPath(filePath: string): boolean {
  return /\.(md|mdx)$/i.test(filePath);
}

/** Lines of unchanged context shown around a changed region in a unified diff. */
export const DIFF_CONTEXT_LINES = 3;

/**
 * Context width used for a {@link boundedChangeSnippet} — wider than the normal
 * diff context so an out-of-band change (a formatter/linter rewrite, a
 * format-on-save) is surfaced to the model WITH enough surrounding code to
 * locate it. Parity: the reference's getSnippetForTwoFileDiff `context: 8`.
 */
export const DIFF_SNIPPET_CONTEXT_LINES = 8;

/**
 * Byte cap on a {@link boundedChangeSnippet}. A format-on-save of a large file
 * could otherwise inject the whole file into the turn; 8 KiB preserves the
 * changed region while bounding the worst case. Parity: the reference's
 * DIFF_SNIPPET_MAX_BYTES (8192).
 */
export const DIFF_SNIPPET_MAX_BYTES = 8192;

/**
 * Context width that forces the WHOLE file into a single hunk (mirrors the reference
 * Code's `singleHunk` → `context: 100_000`). Any realistic file has fewer than
 * this many unchanged lines between two edits, so every change folds into one
 * contiguous hunk.
 */
export const DIFF_SINGLE_HUNK_CONTEXT = 100_000;

/**
 * Upper bound (old-line-count × new-line-count) for running the exact O(n·m)
 * line LCS. Past this the diff would be too slow/memory-hungry to compute on the
 * request path, so we fall back to the cheap O(n) prefix/suffix single-region
 * differ — mirroring the reference's `DIFF_TIMEOUT_MS` guard where `structuredPatch`
 * bails and the caller degrades gracefully rather than hanging.
 */
export const MAX_DIFF_CELLS = 4_000_000;

/**
 * Options for {@link buildUnifiedDiff}. All optional; the defaults reproduce the
 * previous single-purpose behavior (3 context lines, whitespace-sensitive, no
 * cap) so existing call sites are unaffected.
 */
export interface UnifiedDiffOptions {
  /** Unchanged lines shown around each change region. Default {@link DIFF_CONTEXT_LINES}. */
  contextLines?: number;
  /** Compare lines with whitespace collapsed/trimmed when deciding equality. */
  ignoreWhitespace?: boolean;
  /** Emit everything as one hunk (sets context to {@link DIFF_SINGLE_HUNK_CONTEXT}). */
  singleHunk?: boolean;
  /** Byte cap on the rendered diff; past it, truncate at a line boundary with a marker. */
  maxBytes?: number;
}
