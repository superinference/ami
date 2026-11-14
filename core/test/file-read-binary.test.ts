import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  KNOWN_BINARY_EXTENSIONS,
  isBinaryByExtension,
  checkBinaryContent,
} from '../src/tools/file-read';
import { fileReadTool } from '../src/tools/file-read';

describe('KNOWN_BINARY_EXTENSIONS coverage', () => {
  const expected = [
    '.xlsm', '.xlsx', '.xls', '.docx', '.doc', '.pptx', '.ppt',
    '.odt', '.ods', '.odp',
    '.parquet', '.avro', '.orc', '.feather', '.arrow',
    '.protobuf', '.pb', '.msgpack',
    '.heic', '.heif', '.webm', '.flac', '.aac', '.m4a', '.opus', '.wma',
    '.epub', '.mobi',
    '.wasm', '.pyc', '.pyo', '.class', '.dex',
    '.sqlite', '.sqlite3', '.db', '.mdb',
    '.7z', '.rar', '.bz2', '.xz', '.lz4', '.zst',
    '.exe', '.dll', '.so', '.dylib', '.o', '.a', '.lib', '.bin',
    '.zip', '.gz', '.tar', '.jar', '.war', '.iso', '.img', '.dmg',
    '.mp3', '.mp4', '.avi', '.mov', '.mkv', '.wav', '.ogg',
  ];

  for (const ext of expected) {
    it(`includes ${ext}`, () => {
      assert.ok(KNOWN_BINARY_EXTENSIONS.has(ext), `${ext} missing from KNOWN_BINARY_EXTENSIONS`);
    });
  }
});

describe('isBinaryByExtension', () => {
  it('detects .xlsm as binary', () => assert.ok(isBinaryByExtension('.xlsm')));
  it('detects .parquet as binary', () => assert.ok(isBinaryByExtension('.parquet')));
  it('detects .epub as binary', () => assert.ok(isBinaryByExtension('.epub')));
  it('detects .heic as binary', () => assert.ok(isBinaryByExtension('.heic')));
  it('detects .wasm as binary', () => assert.ok(isBinaryByExtension('.wasm')));
  it('detects .sqlite as binary', () => assert.ok(isBinaryByExtension('.sqlite')));

  it('does NOT mark .txt as binary', () => assert.ok(!isBinaryByExtension('.txt')));
  it('does NOT mark .ts as binary', () => assert.ok(!isBinaryByExtension('.ts')));
  it('does NOT mark .py as binary', () => assert.ok(!isBinaryByExtension('.py')));
  it('does NOT mark .json as binary', () => assert.ok(!isBinaryByExtension('.json')));
  it('does NOT mark .md as binary', () => assert.ok(!isBinaryByExtension('.md')));
  it('does NOT mark .html as binary', () => assert.ok(!isBinaryByExtension('.html')));
  it('does NOT mark .css as binary', () => assert.ok(!isBinaryByExtension('.css')));

  it('is case-insensitive', () => {
    assert.ok(isBinaryByExtension('.XLSM'));
    assert.ok(isBinaryByExtension('.Parquet'));
  });
});

describe('checkBinaryContent — null-byte threshold', () => {
  let tmpDir: string;

  beforeEach(() => { tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-binary-')); });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('detects file with many null bytes as binary', async () => {
    const fp = path.join(tmpDir, 'data.bin');
    const buf = Buffer.alloc(1000);
    for (let i = 0; i < 200; i++) buf[i * 5] = 0; // 20% nulls
    fs.writeFileSync(fp, buf);
    assert.ok(await checkBinaryContent(fp));
  });

  it('does NOT detect a plain text file as binary', async () => {
    const fp = path.join(tmpDir, 'hello.txt');
    fs.writeFileSync(fp, 'Hello, world! This is a plain text file.\n'.repeat(50));
    assert.ok(!(await checkBinaryContent(fp)));
  });

  it('handles empty file', async () => {
    const fp = path.join(tmpDir, 'empty');
    fs.writeFileSync(fp, '');
    assert.ok(!(await checkBinaryContent(fp)));
  });

  it('ANY null byte makes a file binary (Claude parity)', async () => {
    const fp = path.join(tmpDir, 'edge.dat');
    const buf = Buffer.alloc(1000, 0x41); // all 'A'
    buf[500] = 0; // a single NUL is enough
    fs.writeFileSync(fp, buf);
    assert.ok(await checkBinaryContent(fp));
  });

  it('control-character-dense content (>10%, no NUL) is binary', async () => {
    const fp = path.join(tmpDir, 'ctrl.dat');
    const buf = Buffer.alloc(1000, 0x41); // 'A'
    // 150 non-whitespace control bytes (0x01) = 15% > 10% → binary. No NUL bytes.
    for (let i = 0; i < 150; i++) buf[i * 6] = 0x01;
    fs.writeFileSync(fp, buf);
    assert.ok(await checkBinaryContent(fp));
  });

  it('tabs/newlines/CR do NOT count toward the control-char density', async () => {
    const fp = path.join(tmpDir, 'ws.txt');
    // All whitespace control chars — must stay text.
    fs.writeFileSync(fp, Buffer.from('line\tone\nline\r\ntwo\n'.repeat(60)));
    assert.ok(!(await checkBinaryContent(fp)));
  });
});

describe('file_read tool rejects known binary extensions', () => {
  let tmpDir: string;

  beforeEach(() => { tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-fread-')); });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  const ctx = (cwd: string) => ({ cwd, abortSignal: new AbortController().signal });

  it('returns descriptive message for .xlsm', async () => {
    const fp = path.join(tmpDir, 'data.xlsm');
    fs.writeFileSync(fp, Buffer.from([0x50, 0x4B, 0x03, 0x04]));
    const result = await fileReadTool.execute({ file_path: fp }, ctx(tmpDir));
    assert.ok(!result.isError, 'should not be an error');
    assert.ok(result.output.includes('Binary file'));
    assert.ok(result.output.includes('.xlsm'));
  });

  it('returns descriptive message for .parquet', async () => {
    const fp = path.join(tmpDir, 'data.parquet');
    fs.writeFileSync(fp, Buffer.from([0x50, 0x41, 0x52, 0x31]));
    const result = await fileReadTool.execute({ file_path: fp }, ctx(tmpDir));
    assert.ok(result.output.includes('Binary file'));
    assert.ok(result.output.includes('.parquet'));
  });

  it('returns descriptive message for .epub', async () => {
    const fp = path.join(tmpDir, 'book.epub');
    fs.writeFileSync(fp, Buffer.from([0x50, 0x4B, 0x03, 0x04]));
    const result = await fileReadTool.execute({ file_path: fp }, ctx(tmpDir));
    assert.ok(result.output.includes('Binary file'));
    assert.ok(result.output.includes('.epub'));
  });

  it('reads .txt normally', async () => {
    const fp = path.join(tmpDir, 'readme.txt');
    fs.writeFileSync(fp, 'Hello world');
    const result = await fileReadTool.execute({ file_path: fp }, ctx(tmpDir));
    assert.ok(!result.isError);
    assert.ok(result.output.includes('Hello world'));
  });

  // Claude parity: a single embedded NUL byte classifies content as binary. A planted
  // "text" file with a NUL is the classic binary-smuggling vector, so file_read reports
  // it as a binary file rather than streaming raw bytes into the prompt.
  it('classifies text-with-single-null-byte as binary (Claude parity)', async () => {
    const fp = path.join(tmpDir, 'sparse-null.txt');
    const buf = Buffer.alloc(1000, 0x41); // 1000 bytes of 'A'
    buf[500] = 0; // one embedded NUL
    fs.writeFileSync(fp, buf);
    const result = await fileReadTool.execute({ file_path: fp }, ctx(tmpDir));
    assert.ok(!result.isError, 'should not be an error');
    assert.ok(
      result.output.includes('Binary file'),
      `Expected binary classification for a file containing an embedded NUL byte`,
    );
  });
});
