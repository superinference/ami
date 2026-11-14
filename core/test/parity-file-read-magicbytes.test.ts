import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  detectImageFormatFromBuffer,
  detectImageFormatFromBase64,
  encodeImageFile,
  fileReadTool,
  parseImageToolOutput,
  isImagePath,
  isBinaryByExtension,
  imageHooks,
  API_IMAGE_MAX_BASE64_SIZE,
} from '../src/tools/file-read';
import { getFileCache } from '../src/file-cache';
import type { ToolContext } from '../src/types';

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return { cwd: tmpDir, abortSignal: new AbortController().signal, ...overrides };
}

// Minimal buffers carrying only the leading magic bytes of each format.
const PNG = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 1, 2, 3]);
const JPEG = Buffer.from([0xff, 0xd8, 0xff, 0xe0, 0, 0x10, 0x4a, 0x46]);
const GIF = Buffer.from([0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0, 0]);
const WEBP = Buffer.concat([
  Buffer.from('RIFF', 'latin1'),
  Buffer.from([0x24, 0, 0, 0]),
  Buffer.from('WEBP', 'latin1'),
]);
const BMP = Buffer.from([0x42, 0x4d, 0x10, 0, 0, 0]);

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-magic-'));
  getFileCache(tmpDir).clear();
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('detectImageFormatFromBuffer – magic-byte image detection', () => {
  it('detects png / jpeg / gif / webp / bmp from signatures', () => {
    assert.equal(detectImageFormatFromBuffer(PNG), 'image/png');
    assert.equal(detectImageFormatFromBuffer(JPEG), 'image/jpeg');
    assert.equal(detectImageFormatFromBuffer(GIF), 'image/gif');
    assert.equal(detectImageFormatFromBuffer(WEBP), 'image/webp');
    assert.equal(detectImageFormatFromBuffer(BMP), 'image/bmp');
  });

  it('returns null for non-image / too-short buffers', () => {
    assert.equal(detectImageFormatFromBuffer(Buffer.from('plain text')), null);
    assert.equal(detectImageFormatFromBuffer(Buffer.from([0x89])), null);
    assert.equal(detectImageFormatFromBuffer(Buffer.alloc(0)), null);
  });

  it('detectImageFormatFromBase64 decodes the prefix and sniffs it', () => {
    assert.equal(detectImageFormatFromBase64(PNG.toString('base64')), 'image/png');
    assert.equal(detectImageFormatFromBase64(JPEG.toString('base64')), 'image/jpeg');
    assert.equal(detectImageFormatFromBase64('not%%base64%%'), null);
  });
});

describe('encodeImageFile – magic bytes override a lying extension', () => {
  it('uses the real format when the extension is wrong', async () => {
    // A JPEG mislabeled as .png must be reported as image/jpeg.
    const p = path.join(tmpDir, 'actually-jpeg.png');
    fs.writeFileSync(p, JPEG);
    const { media_type, data } = await encodeImageFile(p);
    assert.equal(media_type, 'image/jpeg');
    assert.equal(data, JPEG.toString('base64'));
  });

  it('falls back to the extension map when content matches no signature', async () => {
    const p = path.join(tmpDir, 'pixel.png');
    const bytes = Buffer.from('fake-png-bytes');
    fs.writeFileSync(p, bytes);
    const { media_type } = await encodeImageFile(p);
    assert.equal(media_type, 'image/png');
  });
});

describe('encodeImageFile – BMP transcoded to PNG', () => {
  it('transcodes a BMP to PNG when sharp is available', async () => {
    const p = path.join(tmpDir, 'clip.bmp');
    fs.writeFileSync(p, BMP);
    const pngOut = PNG;
    const original = imageHooks.loadSharp;
    imageHooks.loadSharp = () => (_input: Buffer) => ({
      png: () => ({ toBuffer: async () => pngOut }),
      resize: () => ({ jpeg: () => ({ toBuffer: async () => pngOut }) }),
    });
    try {
      const { media_type, data } = await encodeImageFile(p);
      assert.equal(media_type, 'image/png');
      assert.equal(data, pngOut.toString('base64'));
    } finally {
      imageHooks.loadSharp = original;
    }
  });

  it('leaves BMP bytes when sharp is unavailable', async () => {
    const p = path.join(tmpDir, 'clip.bmp');
    fs.writeFileSync(p, BMP);
    const original = imageHooks.loadSharp;
    imageHooks.loadSharp = () => { throw new Error('no sharp'); };
    try {
      const { media_type, data } = await encodeImageFile(p);
      assert.equal(media_type, 'image/bmp');
      assert.equal(data, BMP.toString('base64'));
    } finally {
      imageHooks.loadSharp = original;
    }
  });

  it('.bmp is treated as a viewable image path', () => {
    assert.equal(isImagePath('/x/y.bmp'), true);
  });
});

describe('fileReadTool – base64 size boundary', () => {
  it('rejects an image whose base64 payload exceeds the API ceiling', async () => {
    const p = path.join(tmpDir, 'huge.png');
    const original = imageHooks.loadSharp;
    // A giant payload whose base64 still exceeds the 5 MB API ceiling after every
    // compression attempt, so execute() rejects it with an actionable error.
    const giant = Buffer.alloc(Math.ceil(API_IMAGE_MAX_BASE64_SIZE * 0.76));
    imageHooks.loadSharp = () => (_input: Buffer) => ({
      async metadata() { return { width: 1000, height: 1000, format: 'jpeg' }; },
      resize() { return this; },
      png() { return this; },
      jpeg() { return this; },
      async toBuffer() { return giant; },
    });
    // The on-disk file must exceed the raw target so compression actually runs.
    fs.writeFileSync(p, Buffer.alloc(Math.ceil(API_IMAGE_MAX_BASE64_SIZE * 0.8)));
    try {
      const result = await fileReadTool.execute({ file_path: p }, ctx());
      assert.equal(result.isError, true);
      assert.ok(/too large to send/.test(result.output), result.output);
    } finally {
      imageHooks.loadSharp = original;
    }
  });
});

describe('fileReadTool – PDF %PDF- header validation', () => {
  it('rejects a .pdf file missing the %PDF- magic header', async () => {
    const p = path.join(tmpDir, 'fake.pdf');
    fs.writeFileSync(p, Buffer.from('this is not really a pdf at all'));
    const result = await fileReadTool.execute({ file_path: p }, ctx());
    assert.equal(result.isError, true);
    assert.ok(/missing %PDF- header/.test(result.output), result.output);
  });
});

describe('parseImageToolOutput – normalize untrusted media_type to actual bytes', () => {
  it('corrects a mislabeled media_type using the magic bytes', () => {
    const block = JSON.stringify({
      type: 'image',
      source: { type: 'base64', media_type: 'image/png', data: JPEG.toString('base64') },
    });
    const parsed = parseImageToolOutput(block);
    assert.ok(parsed);
    assert.equal(parsed![0].source.media_type, 'image/jpeg');
  });

  it('leaves the declared media_type when bytes match no known signature', () => {
    const block = JSON.stringify({
      type: 'image',
      source: { type: 'base64', media_type: 'image/png', data: Buffer.from('nope').toString('base64') },
    });
    const parsed = parseImageToolOutput(block);
    assert.equal(parsed![0].source.media_type, 'image/png');
  });
});

describe('KNOWN_BINARY_EXTENSIONS – expanded coverage', () => {
  it('recognizes fonts, design/3D, flash, installers, native addons', () => {
    for (const ext of ['.ttf', '.woff2', '.psd', '.blend', '.fbx', '.swf', '.msi', '.deb', '.apk', '.node']) {
      assert.equal(isBinaryByExtension(ext), true, `${ext} should be binary`);
    }
  });

  it('fast-rejects a binary-extension file through file_read', async () => {
    const p = path.join(tmpDir, 'logo.ttf');
    fs.writeFileSync(p, Buffer.from([0, 1, 0, 0, 0]));
    const result = await fileReadTool.execute({ file_path: p }, ctx());
    assert.ok(/Binary file/.test(result.output), result.output);
  });
});
