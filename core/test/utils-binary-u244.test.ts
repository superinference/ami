import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  BINARY_CHECK_BYTES,
  KNOWN_BINARY_EXTENSIONS,
  isBinaryByExtension,
  hasBinaryExtension,
  isBinaryContent,
  checkBinaryContent,
} from '../src/utils/binary';

test('BINARY_CHECK_BYTES is the 8KB sniff window', () => {
  assert.equal(BINARY_CHECK_BYTES, 8192);
});

test('isBinaryByExtension normalizes case and leading dot membership', () => {
  assert.equal(isBinaryByExtension('.EXE'), true);
  assert.equal(isBinaryByExtension('.exe'), true);
  assert.equal(isBinaryByExtension('.ts'), false);
  assert.equal(KNOWN_BINARY_EXTENSIONS.has('.wasm'), true);
  // Viewable images are deliberately NOT in this set (handled by the image path);
  // their real binary content is still caught by the content sniff.
  assert.equal(isBinaryByExtension('.png'), false);
});

test('hasBinaryExtension derives the extension from a path', () => {
  assert.equal(hasBinaryExtension('/a/b/c.ZIP'), true);
  assert.equal(hasBinaryExtension('src/index.ts'), false);
  assert.equal(hasBinaryExtension('no-extension'), false);
});

test('isBinaryContent: empty buffer is text', () => {
  assert.equal(isBinaryContent(Buffer.alloc(0)), false);
});

test('isBinaryContent: a NUL byte is binary immediately', () => {
  assert.equal(isBinaryContent(Buffer.from('abc\u0000def', 'utf-8')), true);
});

test('isBinaryContent: plain ASCII (with tab/LF/CR) is text', () => {
  assert.equal(isBinaryContent(Buffer.from('hello\tworld\r\nline2\n', 'utf-8')), false);
});

test('isBinaryContent: high density of control bytes is binary', () => {
  // 20 control bytes (0x01) out of 20 => >10% non-printable.
  assert.equal(isBinaryContent(Buffer.from(new Array(20).fill(1))), true);
});

test('isBinaryContent: low density of control bytes stays text', () => {
  // 1 control byte in 100 => 1% <= 10%.
  const bytes = new Array(100).fill(65); // 'A'
  bytes[0] = 1;
  assert.equal(isBinaryContent(Buffer.from(bytes)), false);
});

test('isBinaryContent: only the first BINARY_CHECK_BYTES are scanned', () => {
  // A NUL beyond the 8KB window must NOT flip the verdict to binary.
  const buf = Buffer.concat([Buffer.alloc(BINARY_CHECK_BYTES, 65), Buffer.from([0])]);
  assert.equal(isBinaryContent(buf), false);
});

test('checkBinaryContent reads a file head and applies the heuristic', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-bin-'));
  try {
    const textFile = path.join(dir, 'a.txt');
    fs.writeFileSync(textFile, 'plain text content\n');
    assert.equal(await checkBinaryContent(textFile), false);

    const binFile = path.join(dir, 'b.bin');
    fs.writeFileSync(binFile, Buffer.from([0x00, 0x01, 0x02, 0x03]));
    assert.equal(await checkBinaryContent(binFile), true);

    const empty = path.join(dir, 'c.txt');
    fs.writeFileSync(empty, '');
    assert.equal(await checkBinaryContent(empty), false);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
