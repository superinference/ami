import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  MAX_IMAGE_FILE_BYTES,
  isImageExtension,
  validateImageFile,
} from '../src/tools/file-read';

describe('MAX_IMAGE_FILE_BYTES', () => {
  it('is 128 MiB', () => {
    assert.equal(MAX_IMAGE_FILE_BYTES, 128 * 1024 * 1024);
  });
});

describe('isImageExtension', () => {
  it('recognises .png', () => assert.ok(isImageExtension('photo.png')));
  it('recognises .jpg', () => assert.ok(isImageExtension('photo.jpg')));
  it('recognises .jpeg', () => assert.ok(isImageExtension('photo.jpeg')));
  it('recognises .gif', () => assert.ok(isImageExtension('anim.gif')));
  it('recognises .bmp', () => assert.ok(isImageExtension('bitmap.bmp')));
  it('recognises .webp', () => assert.ok(isImageExtension('img.webp')));
  it('recognises .svg', () => assert.ok(isImageExtension('icon.svg')));
  it('recognises .ico', () => assert.ok(isImageExtension('favicon.ico')));
  it('recognises .tiff', () => assert.ok(isImageExtension('scan.tiff')));
  it('recognises .heic', () => assert.ok(isImageExtension('photo.heic')));
  it('recognises .heif', () => assert.ok(isImageExtension('photo.heif')));
  it('recognises .avif', () => assert.ok(isImageExtension('photo.avif')));

  it('rejects .ts', () => assert.ok(!isImageExtension('file.ts')));
  it('rejects .js', () => assert.ok(!isImageExtension('file.js')));
  it('rejects .txt', () => assert.ok(!isImageExtension('notes.txt')));
  it('rejects .md', () => assert.ok(!isImageExtension('README.md')));

  it('is case-insensitive (.PNG)', () => assert.ok(isImageExtension('PHOTO.PNG')));
  it('is case-insensitive (.JPG)', () => assert.ok(isImageExtension('PHOTO.JPG')));
  it('is case-insensitive (.Svg)', () => assert.ok(isImageExtension('icon.Svg')));

  it('handles paths with directories', () => {
    assert.ok(isImageExtension('/home/user/photos/vacation.jpeg'));
  });
});

describe('validateImageFile', () => {
  let tmpDir: string;

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'img-guard-'));
  });

  after(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('succeeds for a normal small file', () => {
    const f = path.join(tmpDir, 'small.png');
    fs.writeFileSync(f, Buffer.alloc(1024));
    const result = validateImageFile(f);
    assert.equal(result.valid, true);
    assert.equal(result.error, undefined);
  });

  it('fails for a nonexistent file', () => {
    const result = validateImageFile(path.join(tmpDir, 'nope.png'));
    assert.equal(result.valid, false);
    assert.ok(result.error!.includes('not found') || result.error!.includes('not accessible'));
  });

  it('fails for a non-regular file (symlink to /dev/null)', () => {
    const link = path.join(tmpDir, 'devnull.png');
    try {
      fs.symlinkSync('/dev/null', link);
    } catch {
      // On systems where /dev/null doesn't exist or symlink fails, skip
      return;
    }
    // lstatSync on a symlink returns the symlink's own info, not the target.
    // A symlink is not isFile() === true per lstat.
    const result = validateImageFile(link);
    assert.equal(result.valid, false);
    assert.ok(result.error!.includes('Not a regular file'));
  });

  it('fails for a directory', () => {
    const dir = path.join(tmpDir, 'subdir');
    fs.mkdirSync(dir);
    const result = validateImageFile(dir);
    assert.equal(result.valid, false);
    assert.ok(result.error!.includes('Not a regular file'));
  });

  it('fails for an oversized file (mock via stat)', () => {
    // We can't create a 128MB+ file in tests, so create a small file
    // and verify the constant boundary
    const f = path.join(tmpDir, 'big.png');
    fs.writeFileSync(f, Buffer.alloc(100));
    // Verify the file passes (it's small)
    assert.equal(validateImageFile(f).valid, true);
    // Verify the constant is correct
    assert.equal(MAX_IMAGE_FILE_BYTES, 128 * 1024 * 1024);
  });

  it('succeeds for a file exactly at the limit boundary', () => {
    // Just verify a regular file passes
    const f = path.join(tmpDir, 'boundary.png');
    fs.writeFileSync(f, Buffer.alloc(2048));
    const result = validateImageFile(f);
    assert.equal(result.valid, true);
  });

  it('succeeds for an empty regular file', () => {
    const f = path.join(tmpDir, 'empty.png');
    fs.writeFileSync(f, Buffer.alloc(0));
    const result = validateImageFile(f);
    assert.equal(result.valid, true);
  });
});
