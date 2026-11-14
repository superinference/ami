import { describe, it, mock, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  fileReadTool,
  compressImageBuffer,
  compressImageBufferWithTokenLimit,
  createImageMetadataText,
  parseImageToolMetadata,
  ImageResizeError,
  imageHooks,
  IMAGE_TARGET_RAW_SIZE,
  IMAGE_MAX_WIDTH,
} from '../src/tools/file-read';
import type { ToolContext } from '../src/types';

/** Minimal PNG header (8-byte sig + IHDR width/height big-endian at 16/20). */
function pngHeader(width: number, height: number, pad = 0): Buffer {
  const buf = Buffer.alloc(Math.max(24, 24 + pad), 0);
  buf.set([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a], 0);
  buf.writeUInt32BE(width, 16);
  buf.writeUInt32BE(height, 20);
  return buf;
}

function fakeSharp(
  meta: { width: number; height: number; format: string },
  sizes: { raw?: number; png?: number; jpeg?: number },
) {
  return (_input: Buffer) => {
    const inst: any = {
      _enc: 'raw',
      async metadata() { return meta; },
      resize() { return inst; },
      png() { inst._enc = 'png'; return inst; },
      jpeg() { inst._enc = 'jpeg'; return inst; },
      async toBuffer() {
        const len = inst._enc === 'png' ? (sizes.png ?? 10)
          : inst._enc === 'jpeg' ? (sizes.jpeg ?? 10)
          : (sizes.raw ?? 10);
        return Buffer.alloc(len);
      },
    };
    return inst;
  };
}

describe('compressImageBuffer', () => {
  it('returns the image as-is when already within the byte budget and pixel cap', async () => {
    mock.method(imageHooks, 'loadSharp', (() => fakeSharp({ width: 100, height: 100, format: 'png' }, {})) as any);
    const raw = pngHeader(100, 100, 50);
    const out = await compressImageBuffer(raw, IMAGE_TARGET_RAW_SIZE, 'image/png');
    assert.equal(out.base64, raw.toString('base64'));
    assert.equal(out.mediaType, 'image/png');
    assert.equal(out.dimensions?.displayWidth, 100);
  });

  it('PNG-first: re-encodes an over-budget PNG as PNG when that fits', async () => {
    mock.method(imageHooks, 'loadSharp', (() => fakeSharp({ width: 800, height: 600, format: 'png' }, { png: 50 })) as any);
    const out = await compressImageBuffer(Buffer.alloc(100), 80, 'image/png');
    assert.equal(out.mediaType, 'image/png');
  });

  it('falls back to progressively lower JPEG quality when PNG is still too big', async () => {
    mock.method(imageHooks, 'loadSharp', (() => fakeSharp({ width: 800, height: 600, format: 'png' }, { png: 1000, jpeg: 40 })) as any);
    const out = await compressImageBuffer(Buffer.alloc(100), 80, 'image/png');
    assert.equal(out.mediaType, 'image/jpeg');
  });

  it('downscales to the pixel cap when dimensions exceed it', async () => {
    mock.method(imageHooks, 'loadSharp', (() => fakeSharp({ width: 4000, height: 1000, format: 'jpeg' }, { raw: 50 })) as any);
    const out = await compressImageBuffer(Buffer.alloc(100), 80, 'image/jpeg');
    assert.equal(out.dimensions?.displayWidth, IMAGE_MAX_WIDTH);
    assert.equal(out.dimensions?.displayHeight, 500); // aspect preserved: 1000*2000/4000
  });

  it('ultra-compresses as a last resort when nothing else fits', async () => {
    mock.method(imageHooks, 'loadSharp', (() => fakeSharp({ width: 3000, height: 3000, format: 'png' }, { raw: 1000, png: 1000, jpeg: 1000 })) as any);
    const out = await compressImageBuffer(Buffer.alloc(100), 80, 'image/png');
    assert.equal(out.mediaType, 'image/jpeg');
    assert.equal(out.dimensions?.displayWidth, 1000);
  });

  it('on sharp failure, allows a within-limits image through', async () => {
    mock.method(imageHooks, 'loadSharp', (() => { throw new Error('no sharp'); }) as any);
    const raw = pngHeader(100, 100, 10);
    const out = await compressImageBuffer(raw, 5, 'image/png');
    assert.equal(out.base64, raw.toString('base64'));
    assert.equal(out.dimensions?.originalWidth, 100);
  });

  it('on sharp failure, throws ImageResizeError when pixel dimensions exceed the cap', async () => {
    mock.method(imageHooks, 'loadSharp', (() => { throw new Error('no sharp'); }) as any);
    const raw = pngHeader(5000, 5000, 10);
    await assert.rejects(() => compressImageBuffer(raw, 5, 'image/png'), (e: Error) => {
      assert.ok(e instanceof ImageResizeError);
      assert.match(e.message, /dimensions exceed/);
      return true;
    });
  });

  it('on sharp failure, throws ImageResizeError when base64 exceeds the 5 MB API limit', async () => {
    mock.method(imageHooks, 'loadSharp', (() => { throw new Error('no sharp'); }) as any);
    // 5 MB of non-PNG bytes -> base64 > 5 MB -> rejected.
    const raw = Buffer.alloc(5 * 1024 * 1024, 0x01);
    await assert.rejects(() => compressImageBuffer(raw, 5, 'image/jpeg'), (e: Error) => {
      assert.ok(e instanceof ImageResizeError);
      assert.match(e.message, /5 MB API limit/);
      return true;
    });
  });
});

describe('compressImageBufferWithTokenLimit', () => {
  it('converts a token budget to a byte budget and defers to compressImageBuffer', async () => {
    mock.method(imageHooks, 'loadSharp', (() => fakeSharp({ width: 100, height: 100, format: 'png' }, {})) as any);
    const raw = pngHeader(100, 100, 5);
    // Large token budget -> within-limits -> returned as-is.
    const out = await compressImageBufferWithTokenLimit(raw, 100_000, 'image/png');
    assert.equal(out.base64, raw.toString('base64'));
  });
});

describe('createImageMetadataText', () => {
  it('returns a scale-factor note for a downscaled image', () => {
    const text = createImageMetadataText(
      { originalWidth: 4000, originalHeight: 2000, displayWidth: 2000, displayHeight: 1000 },
      '/tmp/a.png',
    );
    assert.match(text!, /source: \/tmp\/a\.png/);
    assert.match(text!, /Multiply coordinates by 2\.00/);
  });

  it('returns null when nothing useful is available', () => {
    assert.equal(createImageMetadataText({ originalWidth: 100, originalHeight: 100, displayWidth: 100, displayHeight: 100 }), null);
  });

  it('returns a source-only note when dimensions are invalid but a path is given', () => {
    assert.equal(createImageMetadataText({ displayWidth: 0 }, '/tmp/b.png'), '[Image source: /tmp/b.png]');
  });

  it('returns null when dimensions are invalid and no path is given', () => {
    assert.equal(createImageMetadataText({ displayWidth: 0 }), null);
  });
});

describe('fileReadTool image output carries coordinate metadata when downscaled', () => {
  let tmpDir: string;
  beforeEach(() => { tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'img-meta-')); });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });
  const ctx = (): ToolContext => ({ cwd: tmpDir, abortSignal: new AbortController().signal });

  it('emits a `metadata` note in the image JSON for a resized image', async () => {
    const fp = path.join(tmpDir, 'big.png');
    fs.writeFileSync(fp, Buffer.alloc(IMAGE_TARGET_RAW_SIZE + 1, 0x01));
    mock.method(imageHooks, 'loadSharp', (() => (_i: Buffer) => {
      const inst: any = {
        _enc: 'raw',
        async metadata() { return { width: 4000, height: 2000, format: 'jpeg' }; },
        resize() { return inst; },
        png() { inst._enc = 'png'; return inst; },
        jpeg() { inst._enc = 'jpeg'; return inst; },
        async toBuffer() { return Buffer.alloc(100); },
      };
      return inst;
    }) as any);
    const res = await fileReadTool.execute({ file_path: fp }, ctx());
    assert.equal(res.isError, undefined);
    const parsed = JSON.parse(res.output);
    assert.equal(parsed.type, 'image');
    assert.match(parsed.metadata, /Multiply coordinates by 2\.00/);
    assert.match(parsed.metadata, new RegExp(`${IMAGE_MAX_WIDTH}`));
  });

  it('surfaces an ImageResizeError from file_read as an error result', async () => {
    const fp = path.join(tmpDir, 'over.png');
    // 5000x5000 PNG header, raw under 5 MB but over the pixel cap; sharp missing -> throw.
    const buf = Buffer.alloc(64, 0);
    buf.set([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a], 0);
    buf.writeUInt32BE(5000, 16);
    buf.writeUInt32BE(5000, 20);
    fs.writeFileSync(fp, buf);
    mock.method(imageHooks, 'loadSharp', (() => { throw new Error('no sharp'); }) as any);
    const res = await fileReadTool.execute({ file_path: fp }, ctx());
    assert.equal(res.isError, true);
    assert.match(res.output, /dimensions exceed/);
  });
});

describe('parseImageToolMetadata', () => {
  it('extracts the metadata note from a file_read image output', () => {
    const output = JSON.stringify({ type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'AA' }, metadata: '[Image: source: x]' });
    assert.equal(parseImageToolMetadata(output), '[Image: source: x]');
  });

  it('returns null for an image output without metadata', () => {
    const output = JSON.stringify({ type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'AA' } });
    assert.equal(parseImageToolMetadata(output), null);
  });

  it('returns null for a non-image / non-JSON output', () => {
    assert.equal(parseImageToolMetadata('plain text'), null);
    assert.equal(parseImageToolMetadata('{bad json "metadata"'), null);
  });
});
