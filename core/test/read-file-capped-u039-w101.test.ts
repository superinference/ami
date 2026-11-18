import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { readFileCapped, READ_CHUNK_SIZE } from '../src/tools/read-edit-context';

/**
 * Unit coverage for the memory-bounded, truncation-aware incremental file reader
 * (U039 / w101). Parity intent: Claude Code's readCapped (readEditContext.ts) —
 * read a file into memory incrementally with a hard byte cap so a file that is
 * (or GROWS, past an earlier stat) beyond the cap is reported as `truncated`
 * instead of being slurped whole and risking an OOM.
 */
describe('readFileCapped', () => {
  let dir: string;
  before(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'rfc-u039-'));
  });
  after(() => {
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('reads the whole file when it is within the cap', async () => {
    const p = path.join(dir, 'small.txt');
    const content = 'hello world\nsecond line\n';
    fs.writeFileSync(p, content);
    const res = await readFileCapped(p, 1024);
    assert.ok(res);
    assert.equal(res.truncated, false);
    assert.equal(res.buffer.toString('utf8'), content);
  });

  it('returns null for a missing file (ENOENT → treat as absent)', async () => {
    const res = await readFileCapped(path.join(dir, 'does-not-exist.txt'), 1024);
    assert.equal(res, null);
  });

  it('reads an empty file as an empty, non-truncated buffer', async () => {
    const p = path.join(dir, 'empty.txt');
    fs.writeFileSync(p, '');
    const res = await readFileCapped(p, 1024);
    assert.ok(res);
    assert.equal(res.truncated, false);
    assert.equal(res.buffer.length, 0);
  });

  it('allows a file that is exactly the cap size (boundary — not truncated)', async () => {
    const p = path.join(dir, 'exact.bin');
    fs.writeFileSync(p, Buffer.alloc(100, 0x61));
    const res = await readFileCapped(p, 100);
    assert.ok(res);
    assert.equal(res.truncated, false);
    assert.equal(res.buffer.length, 100);
  });

  it('flags truncation and empties the buffer when the file exceeds the cap', async () => {
    const p = path.join(dir, 'big.bin');
    fs.writeFileSync(p, Buffer.alloc(101, 0x62));
    const res = await readFileCapped(p, 100);
    assert.ok(res);
    assert.equal(res.truncated, true);
    assert.equal(res.buffer.length, 0);
  });

  it('preserves raw (non-UTF / binary) bytes exactly for the caller to decode', async () => {
    const p = path.join(dir, 'binary.bin');
    const raw = Buffer.from([0x00, 0xff, 0xfe, 0x41, 0x0a, 0x80]);
    fs.writeFileSync(p, raw);
    const res = await readFileCapped(p, 1024);
    assert.ok(res);
    assert.equal(res.truncated, false);
    assert.deepEqual([...res.buffer], [...raw]);
  });

  it('reads a file larger than one chunk by growing the buffer incrementally', async () => {
    const p = path.join(dir, 'multi-chunk.bin');
    const size = READ_CHUNK_SIZE * 3 + 7;
    fs.writeFileSync(p, Buffer.alloc(size, 0x63));
    const res = await readFileCapped(p, size + 10);
    assert.ok(res);
    assert.equal(res.truncated, false);
    assert.equal(res.buffer.length, size);
  });

  it('detects over-cap during a multi-chunk read (growth past an earlier stat)', async () => {
    const p = path.join(dir, 'grew.bin');
    const size = READ_CHUNK_SIZE * 3;
    fs.writeFileSync(p, Buffer.alloc(size, 0x64));
    // Cap below the actual size: the read must stop and flag truncation rather
    // than materializing the whole file.
    const res = await readFileCapped(p, READ_CHUNK_SIZE);
    assert.ok(res);
    assert.equal(res.truncated, true);
    assert.equal(res.buffer.length, 0);
  });

  it('propagates a non-ENOENT open error (e.g. a directory target)', async () => {
    const sub = path.join(dir, 'a-directory');
    fs.mkdirSync(sub);
    await assert.rejects(() => readFileCapped(sub, 1024));
  });
});
