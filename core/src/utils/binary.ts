import * as fs from 'fs';
import * as path from 'path';

/**
 * Low-level binary-detection primitives — a single canonical implementation of
 * "is this file/content binary?" shared by every caller that must avoid piping a
 * non-text blob into a string context (the file-read tool's binary guard AND the
 * untracked-file capture in {@link ../utils/git}). Kept in a dependency-free leaf
 * module so a low-level util (e.g. git.ts) can reuse the heuristic without pulling
 * in the heavy file-read tool graph.
 */

/** Bytes sampled from the head of a file for the content-density binary check. */
export const BINARY_CHECK_BYTES = 8192;

/**
 * File extensions that are binary by convention. Checking the extension first is a
 * zero-I/O fast path that avoids opening + sniffing a file whose type is obvious.
 */
export const KNOWN_BINARY_EXTENSIONS = new Set([
  // Executables & shared libraries
  '.exe', '.dll', '.so', '.dylib', '.o', '.obj', '.a', '.lib', '.bin', '.app',
  '.wasm', '.class', '.pyc', '.pyo', '.dex', '.rlib',
  // Archives & compressed
  '.zip', '.gz', '.tar', '.tgz', '.bz2', '.xz', '.z', '.7z', '.rar', '.lz4', '.zst',
  '.jar', '.war', '.ear', '.iso', '.img', '.dmg',
  // Databases
  '.dat', '.data', '.db', '.sqlite', '.sqlite3', '.mdb', '.idx',
  // Office / documents (binary formats)
  '.xlsm', '.xlsx', '.xls', '.docx', '.doc', '.pptx', '.ppt',
  '.odt', '.ods', '.odp',
  // Data serialisation
  '.parquet', '.avro', '.orc', '.feather', '.arrow',
  '.protobuf', '.pb', '.msgpack',
  // Media — audio
  '.mp3', '.wav', '.flac', '.ogg', '.aac', '.m4a', '.opus', '.wma', '.aiff',
  // Media — video
  '.mp4', '.avi', '.mov', '.mkv', '.webm', '.wmv', '.flv', '.m4v', '.mpeg', '.mpg',
  // Media — image (non-viewable; viewable images handled separately)
  '.heic', '.heif', '.tiff', '.tif',
  // E-books
  '.epub', '.mobi',
  // Fonts
  '.ttf', '.otf', '.woff', '.woff2', '.eot',
  // Design & 3D assets
  '.psd', '.ai', '.eps', '.sketch', '.fig', '.xd', '.blend', '.fbx', '.3ds', '.max', '.stl', '.glb',
  // Flash
  '.swf', '.fla',
  // OS installers & packages
  '.msi', '.deb', '.rpm', '.apk', '.pkg', '.cab', '.appimage',
  // Lock / profiling data
  '.lockb',
  // Native addons
  '.node',
]);

/** Whether `ext` (with or without a leading dot) names a known-binary extension. */
export function isBinaryByExtension(ext: string): boolean {
  return KNOWN_BINARY_EXTENSIONS.has(ext.toLowerCase());
}

/** Whether `filePath`'s extension names a known-binary type (zero-I/O). */
export function hasBinaryExtension(filePath: string): boolean {
  return isBinaryByExtension(path.extname(filePath));
}

/**
 * Buffer-level binary heuristic (mirrors the reference's `isBinaryContent`), scanning
 * at most the first {@link BINARY_CHECK_BYTES} bytes of `buffer`:
 *  (a) any NUL byte → binary immediately;
 *  (b) non-printable control-character DENSITY — count control bytes (<32) other
 *      than tab(9)/LF(10)/CR(13); if >10% of the sampled bytes are non-printable
 *      control chars the content is binary. Catches binaries (compiled objects,
 *      compressed blobs, UTF-16-without-BOM) that are sparse on NULs but dense in
 *      other control bytes. An empty buffer is treated as text.
 */
export function isBinaryContent(buffer: Buffer): boolean {
  const n = Math.min(buffer.length, BINARY_CHECK_BYTES);
  if (n === 0) return false;
  let nonPrintable = 0;
  for (let i = 0; i < n; i++) {
    const b = buffer[i];
    if (b === 0) return true;
    if (b < 32 && b !== 9 && b !== 10 && b !== 13) nonPrintable++;
  }
  return nonPrintable / n > 0.1;
}

/**
 * Read the head of `filePath` and apply {@link isBinaryContent}. Opens the file,
 * reads up to {@link BINARY_CHECK_BYTES} bytes, and never materializes more than
 * that buffer.
 */
export async function checkBinaryContent(filePath: string): Promise<boolean> {
  const fd = await fs.promises.open(filePath, 'r');
  try {
    const buffer = Buffer.alloc(BINARY_CHECK_BYTES);
    const { bytesRead } = await fd.read(buffer, 0, BINARY_CHECK_BYTES, 0);
    return isBinaryContent(buffer.subarray(0, bytesRead));
  } finally {
    await fd.close();
  }
}
