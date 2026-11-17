import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { deflateRawSync } from 'node:zlib';
import { Buffer } from 'node:buffer';
import { crc32 } from 'node:zlib';

import {
  parseZipModes,
  extractZip,
  isArchivePathSafe,
  ArchiveSafetyError,
  ARCHIVE_LIMITS,
} from '../src/archive-safety';

// ---------------------------------------------------------------------------
// Minimal PKZIP writer — enough to exercise parseZipModes / extractZip.
// ---------------------------------------------------------------------------

interface ZipInput {
  name: string;
  /** Raw file bytes (will be STORE or DEFLATE compressed per `method`). */
  content?: Buffer;
  /** 0 = store, 8 = deflate. Default 8. */
  method?: number;
  /** Unix st_mode to encode (e.g. 0o100755). 0/undefined → no mode. */
  mode?: number;
  /** versionMadeBy high byte (host OS). 3 = Unix (default), 0 = DOS. */
  hostOs?: number;
  /** Force the stored compressed size to a bogus value (bomb/overflow tests). */
  forceCompressedSize?: number;
  /** Force the stored uncompressed size (size-limit tests). */
  forceUncompressedSize?: number;
}

function u16(n: number): Buffer {
  const b = Buffer.alloc(2);
  b.writeUInt16LE(n >>> 0);
  return b;
}
function u32(n: number): Buffer {
  const b = Buffer.alloc(4);
  b.writeUInt32LE(n >>> 0);
  return b;
}

function buildZip(entries: ZipInput[]): Buffer {
  const localParts: Buffer[] = [];
  const centralParts: Buffer[] = [];
  const offsets: number[] = [];
  let offset = 0;

  for (const e of entries) {
    const method = e.method ?? 8;
    const raw = e.content ?? Buffer.alloc(0);
    const compressed = method === 0 ? raw : deflateRawSync(raw);
    const crc = crc32(raw) >>> 0;
    const nameBuf = Buffer.from(e.name, 'utf8');
    const compSize = e.forceCompressedSize ?? compressed.length;
    const uncompSize = e.forceUncompressedSize ?? raw.length;

    offsets.push(offset);

    const local = Buffer.concat([
      u32(0x04034b50),
      u16(20), // version needed
      u16(0), // flags
      u16(method),
      u16(0), // time
      u16(0), // date
      u32(crc),
      u32(compSize),
      u32(uncompSize),
      u16(nameBuf.length),
      u16(0), // extra len
      nameBuf,
      compressed,
    ]);
    localParts.push(local);
    offset += local.length;

    const hostOs = e.hostOs ?? 3;
    const versionMadeBy = (hostOs << 8) | 20;
    const externalAttr = e.mode ? (e.mode << 16) >>> 0 : 0;

    const central = Buffer.concat([
      u32(0x02014b50),
      u16(versionMadeBy),
      u16(20), // version needed
      u16(0), // flags
      u16(method),
      u16(0), // time
      u16(0), // date
      u32(crc),
      u32(compSize),
      u32(uncompSize),
      u16(nameBuf.length),
      u16(0), // extra len
      u16(0), // comment len
      u16(0), // disk start
      u16(0), // internal attrs
      u32(externalAttr),
      u32(offsets[offsets.length - 1]!),
      nameBuf,
    ]);
    centralParts.push(central);
  }

  const localBlob = Buffer.concat(localParts);
  const centralBlob = Buffer.concat(centralParts);
  const eocd = Buffer.concat([
    u32(0x06054b50),
    u16(0), // disk
    u16(0), // disk with cd
    u16(entries.length),
    u16(entries.length),
    u32(centralBlob.length),
    u32(localBlob.length), // cd offset
    u16(0), // comment len
  ]);

  return Buffer.concat([localBlob, centralBlob, eocd]);
}

// ---------------------------------------------------------------------------
// isArchivePathSafe
// ---------------------------------------------------------------------------

describe('isArchivePathSafe', () => {
  it('accepts a plain relative path', () => {
    assert.equal(isArchivePathSafe('skills/foo/SKILL.md'), true);
    assert.equal(isArchivePathSafe('plugin.json'), true);
  });
  it('rejects an empty / non-string path', () => {
    assert.equal(isArchivePathSafe(''), false);
    assert.equal(isArchivePathSafe(undefined as unknown as string), false);
  });
  it('rejects an absolute path', () => {
    assert.equal(isArchivePathSafe('/etc/passwd'), false);
  });
  it('rejects any path containing a .. traversal segment (parity: Claude isPathSafe)', () => {
    assert.equal(isArchivePathSafe('../evil'), false);
    assert.equal(isArchivePathSafe('a/../../evil'), false);
    // Claude's containsPathTraversal rejects ANY `..` segment, even one that would
    // normalize away — so this is rejected, not resolved.
    assert.equal(isArchivePathSafe('a/b/../c'), false);
    // A literal "..." (three dots) is a normal filename, not traversal.
    assert.equal(isArchivePathSafe('a/.../c'), true);
  });
});

// ---------------------------------------------------------------------------
// parseZipModes — THE confirmed parity gap
// ---------------------------------------------------------------------------

describe('parseZipModes', () => {
  it('recovers Unix exec bits for a Unix-host entry', () => {
    const zip = buildZip([
      { name: 'run.sh', content: Buffer.from('#!/bin/sh\n'), mode: 0o100755 },
      { name: 'readme.md', content: Buffer.from('hi'), mode: 0o100644 },
    ]);
    const modes = parseZipModes(new Uint8Array(zip));
    assert.equal(modes['run.sh'], 0o100755);
    assert.equal(modes['readme.md'], 0o100644);
  });

  it('omits entries authored on a non-Unix host (versionMadeBy high byte !== 3)', () => {
    const zip = buildZip([
      { name: 'dos.txt', content: Buffer.from('x'), mode: 0o100755, hostOs: 0 },
    ]);
    const modes = parseZipModes(new Uint8Array(zip));
    assert.equal('dos.txt' in modes, false);
  });

  it('omits entries whose mode bits are zero', () => {
    const zip = buildZip([{ name: 'nomode.txt', content: Buffer.from('x'), mode: 0 }]);
    const modes = parseZipModes(new Uint8Array(zip));
    assert.deepEqual(modes, {});
  });

  it('returns {} for a buffer with no EOCD signature', () => {
    assert.deepEqual(parseZipModes(new Uint8Array(Buffer.from('not a zip at all'))), {});
    assert.deepEqual(parseZipModes(new Uint8Array(Buffer.alloc(4))), {});
  });

  it('returns {} for a ZIP64 archive (entryCount sentinel 0xffff)', () => {
    const zip = buildZip([{ name: 'a.txt', content: Buffer.from('x'), mode: 0o100644 }]);
    // Overwrite the EOCD total-entries field with the ZIP64 sentinel.
    const eocd = zip.length - 22;
    zip.writeUInt16LE(0xffff, eocd + 10);
    assert.deepEqual(parseZipModes(new Uint8Array(zip)), {});
  });

  it('respects a non-zero byteOffset on the input view', () => {
    const zip = buildZip([{ name: 'run.sh', content: Buffer.from('x'), mode: 0o100755 }]);
    const padded = Buffer.concat([Buffer.alloc(7, 0xaa), zip]);
    const view = new Uint8Array(padded.buffer, padded.byteOffset + 7, zip.length);
    assert.equal(parseZipModes(view)['run.sh'], 0o100755);
  });
});

// ---------------------------------------------------------------------------
// extractZip
// ---------------------------------------------------------------------------

describe('extractZip', () => {
  it('extracts deflate + stored entries and preserves bytes', () => {
    const body = Buffer.from('console.log("hi")\n'.repeat(50));
    const zip = buildZip([
      { name: 'a.js', content: body, method: 8 },
      { name: 'b.txt', content: Buffer.from('stored'), method: 0 },
    ]);
    const res = extractZip(new Uint8Array(zip));
    assert.equal(res.fileCount, 2);
    assert.deepEqual(Buffer.from(res.files['a.js']!.data), body);
    assert.deepEqual(Buffer.from(res.files['b.txt']!.data), Buffer.from('stored'));
  });

  it('attaches recovered Unix modes to extracted entries', () => {
    const zip = buildZip([{ name: 'run.sh', content: Buffer.from('x'), mode: 0o100755 }]);
    const res = extractZip(new Uint8Array(zip));
    assert.equal(res.files['run.sh']!.mode, 0o100755);
  });

  it('skips directory marker entries', () => {
    const zip = buildZip([
      { name: 'dir/', content: Buffer.alloc(0), method: 0 },
      { name: 'dir/file.txt', content: Buffer.from('y'), method: 0 },
    ]);
    const res = extractZip(new Uint8Array(zip));
    assert.equal(res.fileCount, 1);
    assert.equal('dir/' in res.files, false);
    assert.deepEqual(Buffer.from(res.files['dir/file.txt']!.data), Buffer.from('y'));
  });

  it('throws on a path-traversal entry', () => {
    const zip = buildZip([{ name: '../escape.txt', content: Buffer.from('x'), method: 0 }]);
    assert.throws(() => extractZip(new Uint8Array(zip)), ArchiveSafetyError);
  });

  it('throws on an absolute-path entry', () => {
    const zip = buildZip([{ name: '/abs.txt', content: Buffer.from('x'), method: 0 }]);
    assert.throws(() => extractZip(new Uint8Array(zip)), /Unsafe file path/);
  });

  it('throws on an unsupported compression method', () => {
    const zip = buildZip([
      { name: 'z.txt', content: Buffer.from('x'), method: 0, forceCompressedSize: 1 },
    ]);
    // Flip the method to bzip2 (12) in both headers after the fact.
    // local header method is at offset 8; central header method follows the local blob.
    zip.writeUInt16LE(12, 8);
    // locate central dir method: find the central sig then +10.
    const central = zip.indexOf(Buffer.from([0x50, 0x4b, 0x01, 0x02]));
    zip.writeUInt16LE(12, central + 10);
    assert.throws(() => extractZip(new Uint8Array(zip)), /unsupported compression method/);
  });

  it('defaults MAX_FILE_COUNT to 100000 and enforces it via an override', () => {
    assert.equal(ARCHIVE_LIMITS.MAX_FILE_COUNT, 100000);
    const zip = buildZip([
      { name: 'a.txt', content: Buffer.from('x'), method: 0 },
      { name: 'b.txt', content: Buffer.from('y'), method: 0 },
    ]);
    assert.throws(
      () => extractZip(new Uint8Array(zip), { MAX_FILE_COUNT: 1 }),
      /too many files/,
    );
  });

  it('throws when a single entry exceeds MAX_FILE_SIZE', () => {
    const zip = buildZip([
      {
        name: 'big.bin',
        content: Buffer.from('x'),
        method: 0,
        forceUncompressedSize: ARCHIVE_LIMITS.MAX_FILE_SIZE + 1,
      },
    ]);
    assert.throws(() => extractZip(new Uint8Array(zip)), /is too large/);
  });

  it('throws when the cumulative size exceeds MAX_TOTAL_SIZE (via an override)', () => {
    const zip = buildZip([
      { name: 'a', content: Buffer.from('x'.repeat(10)), method: 0 },
      { name: 'b', content: Buffer.from('y'.repeat(10)), method: 0 },
    ]);
    assert.throws(
      // High ratio allowance so the TOTAL guard, not the bomb guard, fires first.
      () => extractZip(new Uint8Array(zip), { MAX_TOTAL_SIZE: 15, MAX_COMPRESSION_RATIO: 1e9 }),
      /total size is too large/,
    );
  });

  it('throws when the running compression ratio looks like a zip bomb', () => {
    // 1 tiny stored byte but an advertised uncompressed size of 10MB → ratio ≫ 50.
    const zip = buildZip([
      {
        name: 'bomb',
        content: Buffer.from('x'),
        method: 0,
        forceUncompressedSize: 10 * 1024 * 1024,
      },
    ]);
    assert.throws(() => extractZip(new Uint8Array(zip)), /zip bomb|compression ratio/);
  });

  it('throws on a malformed archive (no central directory)', () => {
    assert.throws(
      () => extractZip(new Uint8Array(Buffer.from('garbage'))),
      /malformed or uses an unsupported/,
    );
  });

  it('throws when an entry claims more data than the archive holds', () => {
    const zip = buildZip([
      { name: 'short', content: Buffer.from('ab'), method: 0, forceCompressedSize: 9999 },
    ]);
    assert.throws(() => extractZip(new Uint8Array(zip)), /past the end/);
  });
});
