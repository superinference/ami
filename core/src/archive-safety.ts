/**
 * archive-safety.ts — self-contained, dependency-free ZIP reader with the
 * safety guards and Unix-mode recovery needed to install a plugin/skill bundle
 * from a `.zip` / `.mcpb` / `.dxt` archive at parity with the git-clone path.
 *
 * Why this exists:
 *  - AMI's plugin install path previously only `git clone`d a repo or symlinked
 *    a local directory. A Claude-Code-compatible bundle can ship as a zip
 *    (`.mcpb` / `.dxt`), and a naive extractor loses two things the clone path
 *    gets for free: (a) executable permission bits, and (b) protection against
 *    path-traversal / zip-bomb / oversized archives.
 *  - `parseZipModes()` recovers Unix `st_mode` from the central directory so an
 *    extracted script keeps its `+x` bit (parity with the git-clone path), which
 *    a plain unzip would strip (everything would become 0644).
 *
 * Implementation note: this uses ONLY Node built-ins (`node:zlib`,
 * `node:buffer`) — no `fflate`/`adm-zip` dependency. The central directory is
 * the authoritative index (name, compression method, sizes, local-header
 * offset, external attributes) so extraction is robust even for entries written
 * with a streaming data-descriptor (bit 3 of the general-purpose flag).
 *
 * Format references: PKZIP APPNOTE.TXT §4.3.7 (local file header), §4.3.12
 * (central directory header), §4.3.16 (end of central directory). ZIP64 is
 * intentionally not supported — archives >4GB or >65535 entries are rejected,
 * which is fine for marketplace/MCPB bundles (a few MB).
 */
import { inflateRawSync } from 'node:zlib';
import { Buffer } from 'node:buffer';
import { isAbsolute, normalize } from 'node:path';

import { containsPathTraversal } from './permissions';

// PKZIP signatures (little-endian uint32).
const SIG_LOCAL_FILE = 0x04034b50;
const SIG_CENTRAL_DIR = 0x02014b50;
const SIG_EOCD = 0x06054b50;

// Compression methods we understand.
const METHOD_STORE = 0;
const METHOD_DEFLATE = 8;

// ZIP64 sentinels in the 32/16-bit EOCD fields.
const ZIP64_U32 = 0xffffffff;
const ZIP64_U16 = 0xffff;

/**
 * Streaming resource limits enforced during extraction. These mirror the
 * guards a decompression path must have so a hostile bundle cannot exhaust
 * disk/memory or smuggle a path-traversal write.
 */
export const ARCHIVE_LIMITS = {
  /** Largest single uncompressed entry (512 MB). */
  MAX_FILE_SIZE: 512 * 1024 * 1024,
  /** Largest cumulative uncompressed payload (1 GB). */
  MAX_TOTAL_SIZE: 1024 * 1024 * 1024,
  /** Largest number of entries. */
  MAX_FILE_COUNT: 100000,
  /** Running `uncompressed/compressed` ratio above this is treated as a bomb. */
  MAX_COMPRESSION_RATIO: 50,
} as const;

/** Thrown when an archive violates a safety limit or is malformed/unsupported. */
export class ArchiveSafetyError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ArchiveSafetyError';
  }
}

/** A parsed central-directory entry (the authoritative per-file record). */
interface CentralEntry {
  name: string;
  /** Host OS high byte of `versionMadeBy` (3 === Unix). */
  hostOs: number;
  method: number;
  compressedSize: number;
  uncompressedSize: number;
  localHeaderOffset: number;
  /** Unix `st_mode` recovered from the high 16 bits of externalAttr (Unix host only). */
  mode?: number;
}

/**
 * Whether an in-archive path is safe to extract: relative, no absolute root,
 * no `..` traversal segment. Parity with Claude Code's `isPathSafe`.
 */
export function isArchivePathSafe(filePath: string): boolean {
  if (typeof filePath !== 'string' || filePath.length === 0) return false;
  if (containsPathTraversal(filePath)) return false;
  const normalized = normalize(filePath);
  if (containsPathTraversal(normalized)) return false;
  if (isAbsolute(normalized)) return false;
  return true;
}

/** Locate the End Of Central Directory record; returns its byte offset or -1. */
function findEocd(buf: Buffer): number {
  // The EOCD is the 22-byte fixed record plus an optional comment of up to
  // 65535 bytes, so it lives in the trailing `22 + 0xffff` bytes. Scan
  // backwards — it is normally the very last 22 bytes.
  if (buf.length < 22) return -1;
  const min = Math.max(0, buf.length - 22 - 0xffff);
  for (let i = buf.length - 22; i >= min; i--) {
    if (buf.readUInt32LE(i) === SIG_EOCD) return i;
  }
  return -1;
}

/**
 * Parse the central directory into per-entry records. Returns `null` when the
 * archive is malformed (no EOCD) or ZIP64 (unsupported) — callers treat that as
 * "no recoverable metadata".
 */
function parseCentralDirectory(buf: Buffer): CentralEntry[] | null {
  const eocd = findEocd(buf);
  if (eocd < 0) return null;

  const entryCount = buf.readUInt16LE(eocd + 10);
  const cdOffset = buf.readUInt32LE(eocd + 16);
  // ZIP64: the real values live in a separate record we do not parse. Reject
  // rather than mis-read the sentinels as a tiny archive.
  if (entryCount === ZIP64_U16 || cdOffset === ZIP64_U32) return null;

  const entries: CentralEntry[] = [];
  let off = cdOffset;
  for (let i = 0; i < entryCount; i++) {
    if (off + 46 > buf.length || buf.readUInt32LE(off) !== SIG_CENTRAL_DIR) break;
    const versionMadeBy = buf.readUInt16LE(off + 4);
    const method = buf.readUInt16LE(off + 10);
    const compressedSize = buf.readUInt32LE(off + 20);
    const uncompressedSize = buf.readUInt32LE(off + 24);
    const nameLen = buf.readUInt16LE(off + 28);
    const extraLen = buf.readUInt16LE(off + 30);
    const commentLen = buf.readUInt16LE(off + 32);
    const externalAttr = buf.readUInt32LE(off + 38);
    const localHeaderOffset = buf.readUInt32LE(off + 42);
    const name = buf.toString('utf8', off + 46, off + 46 + nameLen);

    const hostOs = versionMadeBy >> 8;
    // For a Unix host, the high 16 bits of externalAttr hold st_mode.
    const mode = hostOs === 3 ? (externalAttr >>> 16) & 0xffff : undefined;

    entries.push({
      name,
      hostOs,
      method,
      compressedSize,
      uncompressedSize,
      localHeaderOffset,
      mode: mode || undefined,
    });

    off += 46 + nameLen + extraLen + commentLen;
  }
  return entries;
}

/**
 * Parse Unix file modes from a zip's central directory.
 *
 * A plain unzip surfaces only `name → bytes` and drops the external file
 * attributes, so executable bits are lost (everything becomes 0644). This
 * reconstructs `name → st_mode` for entries written on a Unix host
 * (`versionMadeBy` high byte === 3) that carry a non-zero mode; entries from
 * other hosts, or with no mode bits, are omitted (callers treat a missing key
 * as "use the default mode").
 *
 * Returns `{}` for a malformed archive (no EOCD) or a ZIP64 archive
 * (>4GB / >65535 entries), both of which are out of scope here.
 */
export function parseZipModes(data: Uint8Array): Record<string, number> {
  const buf = Buffer.from(data.buffer, data.byteOffset, data.byteLength);
  const entries = parseCentralDirectory(buf);
  if (!entries) return {};
  const modes: Record<string, number> = {};
  for (const e of entries) {
    if (e.mode) modes[e.name] = e.mode;
  }
  return modes;
}

/** One extracted entry: its decompressed bytes and (optionally) its Unix mode. */
export interface ExtractedEntry {
  data: Uint8Array;
  /** Unix st_mode recovered from the central directory, if the entry had one. */
  mode?: number;
}

/** Result of a safe extraction. */
export interface ExtractResult {
  files: Record<string, ExtractedEntry>;
  fileCount: number;
  totalUncompressedSize: number;
}

/**
 * Per-call overrides for {@link ARCHIVE_LIMITS}. Any field left unset keeps the
 * default. A caller in a tighter context (e.g. a small-bundle-only install path)
 * can lower a limit without touching the shared constant.
 */
export type ArchiveLimits = Partial<typeof ARCHIVE_LIMITS>;

/**
 * Safely extract a zip archive entirely in memory, enforcing the resource
 * limits and path-safety on every entry and short-circuiting on the first
 * violation (so a bomb/oversized archive is caught before the whole payload is
 * materialized). Directory entries (names ending in `/`) are skipped; only
 * regular files are returned. Unix modes are recovered via the central
 * directory so executable bits survive.
 *
 * @throws ArchiveSafetyError on a malformed/ZIP64/unsupported archive or a
 *   limit/path-safety violation.
 */
export function extractZip(data: Uint8Array, limits: ArchiveLimits = {}): ExtractResult {
  const lim = { ...ARCHIVE_LIMITS, ...limits };
  const buf = Buffer.from(data.buffer, data.byteOffset, data.byteLength);
  const compressedSize = buf.length || 1; // avoid /0 for an empty buffer
  const entries = parseCentralDirectory(buf);
  if (!entries) {
    throw new ArchiveSafetyError('Archive is malformed or uses an unsupported (ZIP64) format.');
  }

  const files: Record<string, ExtractedEntry> = {};
  let fileCount = 0;
  let totalUncompressedSize = 0;

  for (const entry of entries) {
    // Directory marker — carries no file content.
    if (entry.name.endsWith('/')) continue;

    fileCount++;
    if (fileCount > lim.MAX_FILE_COUNT) {
      throw new ArchiveSafetyError(
        `Archive contains too many files: ${fileCount} (max: ${lim.MAX_FILE_COUNT}).`,
      );
    }

    if (!isArchivePathSafe(entry.name)) {
      throw new ArchiveSafetyError(
        `Unsafe file path detected: "${entry.name}". Path traversal and absolute paths are not allowed.`,
      );
    }

    if (entry.uncompressedSize > lim.MAX_FILE_SIZE) {
      throw new ArchiveSafetyError(
        `File "${entry.name}" is too large: ${Math.round(entry.uncompressedSize / 1024 / 1024)}MB ` +
          `(max: ${Math.round(lim.MAX_FILE_SIZE / 1024 / 1024)}MB).`,
      );
    }

    totalUncompressedSize += entry.uncompressedSize;
    if (totalUncompressedSize > lim.MAX_TOTAL_SIZE) {
      throw new ArchiveSafetyError(
        `Archive total size is too large: ${Math.round(totalUncompressedSize / 1024 / 1024)}MB ` +
          `(max: ${Math.round(lim.MAX_TOTAL_SIZE / 1024 / 1024)}MB).`,
      );
    }

    const ratio = totalUncompressedSize / compressedSize;
    if (ratio > lim.MAX_COMPRESSION_RATIO) {
      throw new ArchiveSafetyError(
        `Suspicious compression ratio detected: ${ratio.toFixed(1)}:1 ` +
          `(max: ${lim.MAX_COMPRESSION_RATIO}:1). This may be a zip bomb.`,
      );
    }

    files[entry.name] = { data: decompressEntry(buf, entry), mode: entry.mode };
  }

  return { files, fileCount, totalUncompressedSize };
}

/** Decompress a single entry using its local header to locate the data. */
function decompressEntry(buf: Buffer, entry: CentralEntry): Uint8Array {
  const lho = entry.localHeaderOffset;
  if (lho + 30 > buf.length || buf.readUInt32LE(lho) !== SIG_LOCAL_FILE) {
    throw new ArchiveSafetyError(`Malformed local header for "${entry.name}".`);
  }
  // Local header name/extra lengths can differ from the central directory's, so
  // the data offset must be computed from the LOCAL header.
  const localNameLen = buf.readUInt16LE(lho + 26);
  const localExtraLen = buf.readUInt16LE(lho + 28);
  const dataStart = lho + 30 + localNameLen + localExtraLen;
  const dataEnd = dataStart + entry.compressedSize;
  if (dataEnd > buf.length) {
    throw new ArchiveSafetyError(`Entry "${entry.name}" extends past the end of the archive.`);
  }
  const compressed = buf.subarray(dataStart, dataEnd);

  if (entry.method === METHOD_STORE) {
    // Stored (uncompressed): copy out so the result does not alias the archive buffer.
    return Uint8Array.prototype.slice.call(compressed);
  }
  if (entry.method === METHOD_DEFLATE) {
    return new Uint8Array(inflateRawSync(compressed));
  }
  throw new ArchiveSafetyError(
    `Entry "${entry.name}" uses an unsupported compression method (${entry.method}).`,
  );
}
