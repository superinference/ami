/**
 * Position → symbol extraction for human-readable tool / diagnostic output.
 *
 * Resolves an LSP `(line, character)` coordinate into the identifier/word at that
 * spot so a message can show a meaningful `symbol: foo` label instead of a bare
 * `line:char` position. Mirrors Claude Code's `getSymbolAtPosition`
 * (tools/LSPTool/symbolContext.ts) and is defensive through the same lens:
 *   1. BOUNDARY — reads only the first {@link MAX_READ_BYTES} of the file (never
 *      the whole thing) so a huge file can't blow up a synchronous render path.
 *   2. EDGE CASE — when the 64KB window is completely filled AND the target is the
 *      last split line, that line may be truncated mid-token, so it returns `null`
 *      rather than a half-symbol.
 *   3. bounds checks — an out-of-range line or character returns `null`.
 *   4. length cap — a matched symbol longer than {@link MAX_SYMBOL_LENGTH} is
 *      truncated with an ellipsis so one pathological token can't dominate output.
 *   5. FALLBACK — the whole read+parse is wrapped in try/catch so ENOENT /
 *      permission / encoding errors degrade to `null` (the caller then shows the
 *      raw `line:char`), logging unexpected `Error`s at `warn` rather than throwing.
 *
 * It is a standalone core primitive (no engine/UI dependency): the `line` and
 * `character` of a position are 0-indexed, matching the LSP wire protocol.
 */
import * as fs from 'fs';
import { expandPath } from '../utils/path';
import { log } from '../logger';

/**
 * Bounded read window (64KB). Most diagnostics/hover targets sit near recent
 * edits, so 64KB (~1000 lines of typical code) covers the common case without
 * ever streaming a multi-hundred-MB file into a synchronous render. Matches
 * Claude Code's `MAX_READ_BYTES`.
 */
export const MAX_READ_BYTES = 64 * 1024;

/** Max length of a returned symbol before it is ellipsis-truncated. */
export const MAX_SYMBOL_LENGTH = 30;

/**
 * Token grammar matching the word/symbol at a position across languages:
 *   - identifiers: alphanumeric + `_` + `$`
 *   - Rust lifetimes / macros: `'a`, `'static`, `name!`
 *   - operator runs: `+`, `-`, `*`, `==`, `<=`, …
 * Mirrors Claude Code's inclusive `symbolPattern` so the same coordinate yields
 * the same label AMI-side.
 */
const SYMBOL_PATTERN = /[\w$'!]+|[+\-*/%&|^~<>=]+/g;

/** Ellipsis-truncate a symbol to {@link MAX_SYMBOL_LENGTH} display characters. */
function truncateSymbol(symbol: string): string {
  if (symbol.length <= MAX_SYMBOL_LENGTH) return symbol;
  return symbol.slice(0, MAX_SYMBOL_LENGTH - 1) + '…';
}

/**
 * Read at most {@link MAX_READ_BYTES} bytes from the start of a file, returning
 * the decoded text and the number of bytes actually read. Kept separate so the
 * extraction logic stays pure and the only syscall surface is here. Uses the
 * explicit fd open/read/close dance (not `fs.readFileSync`) so the read is HARD
 * capped at the window regardless of file size.
 */
function readBoundedHead(absolutePath: string): { content: string; bytesRead: number } {
  const fd = fs.openSync(absolutePath, 'r');
  try {
    const buffer = Buffer.alloc(MAX_READ_BYTES);
    const bytesRead = fs.readSync(fd, buffer, 0, MAX_READ_BYTES, 0);
    return { content: buffer.toString('utf-8', 0, bytesRead), bytesRead };
  } finally {
    fs.closeSync(fd);
  }
}

/**
 * Extract the symbol/word at `(line, character)` in `filePath` (both 0-indexed,
 * matching the LSP wire protocol). Returns the symbol — truncated to
 * {@link MAX_SYMBOL_LENGTH} — or `null` when extraction can't safely yield one
 * (out-of-bounds, mid-window truncation, no token under the cursor, or any I/O
 * error). A `null` return is the caller's signal to fall back to the raw
 * `line:char` position display.
 */
export function getSymbolAtPosition(
  filePath: string,
  line: number,
  character: number,
): string | null {
  try {
    const absolutePath = expandPath(filePath);
    const { content, bytesRead } = readBoundedHead(absolutePath);
    const lines = content.split('\n');

    if (line < 0 || line >= lines.length) return null;
    // A completely-filled window means the file continues past it, so the last
    // split element may be a truncated half-line — refuse to extract from it.
    if (bytesRead === MAX_READ_BYTES && line === lines.length - 1) return null;

    const lineContent = lines[line];
    if (!lineContent || character < 0 || character >= lineContent.length) return null;

    SYMBOL_PATTERN.lastIndex = 0;
    let match: RegExpExecArray | null;
    while ((match = SYMBOL_PATTERN.exec(lineContent)) !== null) {
      const start = match.index;
      const end = start + match[0].length;
      if (character >= start && character < end) {
        return truncateSymbol(match[0]);
      }
    }
    return null;
  } catch (error) {
    // Display enhancement, not a critical path: log unexpected errors for
    // debugging and fall back to `null` (position display) rather than throwing.
    if (error instanceof Error) {
      log(
        'lsp',
        `[LSP] symbol extraction failed for ${filePath}:${line}:${character}: ${error.message}`,
        undefined,
        'warn',
      );
    }
    return null;
  }
}

/**
 * Resolve the symbol at a USER-FACING 1-based `(line, character)` — the coordinate
 * form editors and the LSP tool's `line`/`character` inputs use — by converting to
 * the 0-based protocol indices {@link getSymbolAtPosition} expects (`line-1`,
 * `character-1`) and delegating. Mirrors Claude Code's `getSymbolAtPosition`
 * call-site conversion (tools/LSPTool/UI.tsx: `getSymbolAtPosition(filePath,
 * line-1, character-1)`), consolidating the off-by-one into ONE place so every
 * 1-based caller resolves the same symbol instead of re-deriving the conversion.
 * A non-positive line/character (0-based would be negative) returns `null`.
 */
export function getSymbolAtPosition1Based(
  filePath: string,
  line: number,
  character: number,
): string | null {
  if (!Number.isInteger(line) || !Number.isInteger(character) || line < 1 || character < 1) {
    return null;
  }
  return getSymbolAtPosition(filePath, line - 1, character - 1);
}
