/**
 * read-file-range — line-oriented file reader with two code paths and an
 * optional byte budget. A self-contained core building block consumed by the
 * `file_read` tool; it does not depend on any upper layer.
 *
 * Fast path (regular files < {@link FAST_PATH_MAX_SIZE}):
 *   Read the whole file with readFile() and split lines in memory. Avoids the
 *   per-chunk async overhead of a stream and is faster for typical source files.
 *
 * Streaming path (large files, pipes, devices):
 *   Use createReadStream + manual indexOf('\n') scanning. Content is accumulated
 *   ONLY for lines inside the requested range — lines outside the range are
 *   counted (so totalLines stays accurate) then discarded, so reading line 1 of
 *   a 100 GB file does not balloon RSS. In truncate mode a huge single line
 *   whose fragment overflows the remaining byte budget is discarded mid-stream,
 *   so `partial` cannot grow unbounded on a newline-free file.
 *
 * Both paths strip a leading UTF-8 BOM and normalize CRLF -> LF.
 *
 * `maxBytes` behavior depends on `options.truncateOnByteLimit`:
 *   false (default): throws {@link FileTooLargeError} when the file size (fast
 *     path) or total streamed bytes (streaming path) exceed maxBytes.
 *   true: caps SELECTED OUTPUT at maxBytes, stopping at the last complete line
 *     that fits and setting `truncatedByBytes` in the result. Never throws.
 *
 * Mirrors Claude Code's utils/readFileInRange.ts (fast/streaming dichotomy,
 * FAST_PATH_MAX_SIZE, maxBytes + truncateOnByteLimit + truncatedByBytes,
 * bounded-memory single-line discard, accurate totalLines/totalBytes/readBytes).
 */
import { createReadStream } from 'fs';
import { stat as fsStat, readFile, open } from 'fs/promises';
import { formatFileSize } from '../utils/format';

/** Regular files below this size use the in-memory fast path; larger ones stream. */
export const FAST_PATH_MAX_SIZE = 10 * 1024 * 1024; // 10 MB

export interface ReadFileRangeResult {
  /** Selected lines joined by LF (no line-number gutter). */
  content: string;
  /** Number of lines in `content`. */
  lineCount: number;
  /** Total line count of the whole file (counts out-of-range lines too). */
  totalLines: number;
  /** Total byte length of the whole file as scanned. */
  totalBytes: number;
  /** Byte length of the returned `content`. */
  readBytes: number;
  /** File mtime in ms. */
  mtimeMs: number;
  /** True when output was clipped to `maxBytes` under truncate mode. */
  truncatedByBytes?: boolean;
}

/**
 * Thrown (in the default, non-truncate mode) when a file's size or total
 * streamed bytes exceed the `maxBytes` budget, so a caller can surface an
 * actionable "use offset/limit" error instead of loading an unbounded file into
 * memory. Mirrors Claude Code's FileTooLargeError.
 */
export class FileTooLargeError extends Error {
  constructor(
    public sizeInBytes: number,
    public maxSizeBytes: number,
  ) {
    super(
      `File content (${formatFileSize(sizeInBytes)}) exceeds the maximum allowed size (${formatFileSize(maxSizeBytes)}). Use offset and limit to read specific portions of the file, or search for specific content instead of reading the whole file.`,
    );
    this.name = 'FileTooLargeError';
  }
}

/**
 * Read lines `[offset, offset + maxLines)` from a file, choosing an in-memory or
 * streaming code path by size and optionally enforcing a byte budget. See the
 * module header for the full contract.
 */
export async function readFileInRange(
  filePath: string,
  offset = 0,
  maxLines?: number,
  maxBytes?: number,
  signal?: AbortSignal,
  options?: { truncateOnByteLimit?: boolean },
): Promise<ReadFileRangeResult> {
  signal?.throwIfAborted();
  const truncateOnByteLimit = options?.truncateOnByteLimit ?? false;

  const stats = await fsStat(filePath);
  if (stats.isDirectory()) {
    throw new Error(`EISDIR: illegal operation on a directory, read '${filePath}'`);
  }

  if (stats.isFile() && stats.size < FAST_PATH_MAX_SIZE) {
    if (!truncateOnByteLimit && maxBytes !== undefined && stats.size > maxBytes) {
      throw new FileTooLargeError(stats.size, maxBytes);
    }
    const text = await readFile(filePath, { encoding: 'utf8', signal });
    return readFileInRangeFast(
      text,
      stats.mtimeMs,
      offset,
      maxLines,
      truncateOnByteLimit ? maxBytes : undefined,
    );
  }

  return readFileInRangeStreaming(filePath, stats.mtimeMs, offset, maxLines, maxBytes, truncateOnByteLimit, signal);
}

/** In-memory fast path: readFile already done, split + select here. */
function readFileInRangeFast(
  raw: string,
  mtimeMs: number,
  offset: number,
  maxLines: number | undefined,
  truncateAtBytes: number | undefined,
): ReadFileRangeResult {
  const endLine = maxLines !== undefined ? offset + maxLines : Infinity;
  // Strip a leading UTF-8 BOM.
  const text = raw.charCodeAt(0) === 0xfeff ? raw.slice(1) : raw;

  const selectedLines: string[] = [];
  let lineIndex = 0;
  let startPos = 0;
  let newlinePos: number;
  let selectedBytes = 0;
  let truncatedByBytes = false;

  const tryPush = (line: string): void => {
    if (truncateAtBytes !== undefined) {
      const sep = selectedLines.length > 0 ? 1 : 0;
      const nextBytes = selectedBytes + sep + Buffer.byteLength(line);
      if (nextBytes > truncateAtBytes) {
        truncatedByBytes = true;
        return;
      }
      selectedBytes = nextBytes;
    }
    selectedLines.push(line);
  };

  const stripCr = (s: string): string => (s.endsWith('\r') ? s.slice(0, -1) : s);

  while ((newlinePos = text.indexOf('\n', startPos)) !== -1) {
    if (lineIndex >= offset && lineIndex < endLine && !truncatedByBytes) {
      tryPush(stripCr(text.slice(startPos, newlinePos)));
    }
    lineIndex++;
    startPos = newlinePos + 1;
  }
  // Final unterminated line: content after the last newline (or the whole file
  // when there is none). A file that ends WITH a newline has no trailing line —
  // it is not counted, so totalLines is the human line count, not an off-by-one
  // phantom. An out-of-range trailing line is still counted (accurate totals).
  if (startPos < text.length) {
    if (lineIndex >= offset && lineIndex < endLine && !truncatedByBytes) {
      tryPush(stripCr(text.slice(startPos)));
    }
    lineIndex++;
  }

  const content = selectedLines.join('\n');
  return {
    content,
    lineCount: selectedLines.length,
    totalLines: lineIndex,
    totalBytes: Buffer.byteLength(text, 'utf8'),
    readBytes: Buffer.byteLength(content, 'utf8'),
    mtimeMs,
    ...(truncatedByBytes ? { truncatedByBytes: true } : {}),
  };
}

/** Streaming path: scan the file in chunks, only accumulating in-range lines. */
function readFileInRangeStreaming(
  filePath: string,
  mtimeMs: number,
  offset: number,
  maxLines: number | undefined,
  maxBytes: number | undefined,
  truncateOnByteLimit: boolean,
  signal?: AbortSignal,
): Promise<ReadFileRangeResult> {
  return new Promise<ReadFileRangeResult>((resolve, reject) => {
    const stream = createReadStream(filePath, {
      encoding: 'utf8',
      highWaterMark: 512 * 1024,
      ...(signal ? { signal } : undefined),
    });

    let endLine = maxLines !== undefined ? offset + maxLines : Infinity;
    let totalBytesRead = 0;
    let selectedBytes = 0;
    let truncatedByBytes = false;
    let currentLineIndex = 0;
    const selectedLines: string[] = [];
    let partial = '';
    let isFirstChunk = true;
    // True when the most recent chunk ended with content not followed by a
    // newline, i.e. the file so far has an unterminated trailing line. Lets the
    // 'end' handler count a final line exactly once without counting a phantom
    // empty line after a trailing newline (mirrors the fast path).
    let hasTrailingContent = false;

    const stripCr = (s: string): string => (s.endsWith('\r') ? s.slice(0, -1) : s);

    stream.on('data', (raw: string | Buffer) => {
      let chunk = typeof raw === 'string' ? raw : raw.toString('utf8');
      if (isFirstChunk) {
        isFirstChunk = false;
        if (chunk.charCodeAt(0) === 0xfeff) chunk = chunk.slice(1);
      }

      totalBytesRead += Buffer.byteLength(chunk);
      if (!truncateOnByteLimit && maxBytes !== undefined && totalBytesRead > maxBytes) {
        stream.destroy(new FileTooLargeError(totalBytesRead, maxBytes));
        return;
      }

      const data = partial.length > 0 ? partial + chunk : chunk;
      partial = '';

      let startPos = 0;
      let newlinePos: number;
      while ((newlinePos = data.indexOf('\n', startPos)) !== -1) {
        if (currentLineIndex >= offset && currentLineIndex < endLine) {
          const line = stripCr(data.slice(startPos, newlinePos));
          if (truncateOnByteLimit && maxBytes !== undefined) {
            const sep = selectedLines.length > 0 ? 1 : 0;
            const nextBytes = selectedBytes + sep + Buffer.byteLength(line);
            if (nextBytes > maxBytes) {
              // Cap hit — collapse the selection range so nothing more is
              // accumulated; keep scanning to count totalLines.
              truncatedByBytes = true;
              endLine = currentLineIndex;
            } else {
              selectedBytes = nextBytes;
              selectedLines.push(line);
            }
          } else {
            selectedLines.push(line);
          }
        }
        currentLineIndex++;
        startPos = newlinePos + 1;
      }

      // Track whether this chunk left an unterminated trailing line. Keep its
      // fragment only when inside the selected range; outside the range we just
      // count it (for totalLines) and discard it (bounded memory on huge
      // single-line files).
      hasTrailingContent = startPos < data.length;
      if (hasTrailingContent && currentLineIndex >= offset && currentLineIndex < endLine) {
        const fragment = data.slice(startPos);
        if (truncateOnByteLimit && maxBytes !== undefined) {
          const sep = selectedLines.length > 0 ? 1 : 0;
          const fragBytes = selectedBytes + sep + Buffer.byteLength(fragment);
          if (fragBytes > maxBytes) {
            // A single in-range line whose fragment already overflows the budget
            // can never fit: mark truncated, collapse the range, discard it.
            truncatedByBytes = true;
            endLine = currentLineIndex;
            return;
          }
        }
        partial = fragment;
      }
    });

    stream.once('end', () => {
      // Count/emit a final unterminated line exactly once; a file ending WITH a
      // newline leaves no trailing line (no phantom empty line), matching the
      // fast path and AMI's human line-count convention.
      if (hasTrailingContent) {
        if (currentLineIndex >= offset && currentLineIndex < endLine) {
          const line = stripCr(partial);
          if (truncateOnByteLimit && maxBytes !== undefined) {
            const sep = selectedLines.length > 0 ? 1 : 0;
            const nextBytes = selectedBytes + sep + Buffer.byteLength(line);
            if (nextBytes > maxBytes) {
              truncatedByBytes = true;
            } else {
              selectedLines.push(line);
            }
          } else {
            selectedLines.push(line);
          }
        }
        currentLineIndex++;
      }

      const content = selectedLines.join('\n');
      resolve({
        content,
        lineCount: selectedLines.length,
        totalLines: currentLineIndex,
        totalBytes: totalBytesRead,
        readBytes: Buffer.byteLength(content, 'utf8'),
        mtimeMs,
        ...(truncatedByBytes ? { truncatedByBytes: true } : {}),
      });
    });

    stream.once('error', reject);
  });
}

/**
 * Result of a BYTE-oriented bounded read ({@link readFileRange} / {@link tailFile}).
 * `content` is a flat UTF-8 string decoded from an owned buffer (no sliced-parent
 * retention), `bytesRead` is how many bytes were actually decoded, and `bytesTotal`
 * is the file's full size at read time. Distinct from {@link ReadFileRangeResult},
 * which is line-oriented. Mirrors Claude Code's ByteRange read result
 * (utils/fsOperations.ts readFileRange/tailFile).
 */
export interface ByteRangeResult {
  /** Decoded UTF-8 string of the bytes that were read. */
  content: string;
  /** Number of bytes actually read (<= requested maxBytes). */
  bytesRead: number;
  /** Total size of the file in bytes at read time. */
  bytesTotal: number;
}

/**
 * Drain exactly `length` bytes from `fh` starting at file `position` into `buffer`,
 * looping across short reads (a single `read()` may return fewer bytes than asked)
 * and stopping early on EOF (`bytesRead === 0`). Returns the number of bytes landed
 * in `buffer`. Shared by {@link readFileRange} and {@link tailFile} so the
 * short-read loop exists in ONE place. Mirrors Claude Code's read-loop in
 * fsOperations.ts.
 */
async function drainExact(
  fh: Awaited<ReturnType<typeof open>>,
  buffer: Buffer,
  length: number,
  position: number,
): Promise<number> {
  let totalRead = 0;
  while (totalRead < length) {
    const { bytesRead } = await fh.read(buffer, totalRead, length - totalRead, position + totalRead);
    if (bytesRead === 0) break;
    totalRead += bytesRead;
  }
  return totalRead;
}

/**
 * Read up to `maxBytes` bytes from `filePath` starting at byte `offset`, WITHOUT
 * loading the whole file. Returns a {@link ByteRangeResult} whose `content` is a
 * flat UTF-8 string copied out of an owned buffer (so no reference to a larger
 * parent is retained), or `null` when the file is smaller than or equal to the
 * requested `offset` (nothing to read there). Uses a short-read loop so a partial
 * `read()` still fills the buffer. Mirrors Claude Code's readFileRange
 * (utils/fsOperations.ts).
 */
export async function readFileRange(
  filePath: string,
  offset: number,
  maxBytes: number,
): Promise<ByteRangeResult | null> {
  const fh = await open(filePath, 'r');
  try {
    const size = (await fh.stat()).size;
    if (size <= offset) return null;
    const bytesToRead = Math.min(size - offset, maxBytes);
    const buffer = Buffer.allocUnsafe(bytesToRead);
    const totalRead = await drainExact(fh, buffer, bytesToRead, offset);
    return { content: buffer.toString('utf8', 0, totalRead), bytesRead: totalRead, bytesTotal: size };
  } finally {
    await fh.close();
  }
}

/**
 * Read only the LAST `maxBytes` bytes of `filePath` (offset = max(0, size-maxBytes)),
 * so viewing the end of a huge log/journal never loads the whole file. Returns the
 * entire file when it is smaller than `maxBytes`, and an empty result for a 0-byte
 * file. Uses the same short-read loop as {@link readFileRange}. Mirrors Claude
 * Code's tailFile (utils/fsOperations.ts).
 */
export async function tailFile(filePath: string, maxBytes: number): Promise<ByteRangeResult> {
  const fh = await open(filePath, 'r');
  try {
    const size = (await fh.stat()).size;
    if (size === 0) return { content: '', bytesRead: 0, bytesTotal: 0 };
    const offset = Math.max(0, size - maxBytes);
    const bytesToRead = size - offset;
    const buffer = Buffer.allocUnsafe(bytesToRead);
    const totalRead = await drainExact(fh, buffer, bytesToRead, offset);
    return { content: buffer.toString('utf8', 0, totalRead), bytesRead: totalRead, bytesTotal: size };
  } finally {
    await fh.close();
  }
}
