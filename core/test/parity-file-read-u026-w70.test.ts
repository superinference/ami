/**
 * U026 (w70) — file_read parity: memory-safe range/full reads with a byte
 * budget, errno-distinct access diagnostics, and a hard OOM ceiling.
 *
 * These drive the real fileReadTool.execute() end-to-end (temp files, real fs),
 * exercising the wiring into read-file-range.ts.
 */
import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileReadTool } from '../src/tools/file-read';
import { getFileCache } from '../src/file-cache';
import type { ToolContext } from '../src/types';

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-fr-w70-'));
  getFileCache(tmpDir).clear();
});
afterEach(() => {
  mock.restoreAll();
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Byte-accurate output truncation on a range read (truncateOnByteLimit)
// ---------------------------------------------------------------------------
describe('file_read range — byte-budget clip at a clean line boundary', () => {
  it('clips the window at the byte budget, flags it, and points at the next offset', async () => {
    const file = path.join(tmpDir, 'wide.txt');
    const line = 'x'.repeat(100);
    fs.writeFileSync(file, Array.from({ length: 10 }, () => line).join('\n') + '\n');

    const result = await fileReadTool.execute(
      { file_path: file, offset: 0, limit: 10 },
      // 250-byte output budget: fits 2 full 100-char lines (100+1+100=201),
      // a 3rd would overflow → clip after line 2.
      ctx({ outputBudget: { fileReadMaxSizeBytes: 250 } }),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('byte budget'), `expected a byte-budget notice, got:\n${result.output}`);
    assert.ok(result.output.includes('more lines follow'));
    // only 2 content lines survived the byte clip (clean line boundary)
    const contentLines = result.output.split('\n').filter(l => /^\d+\t/.test(l));
    assert.equal(contentLines.length, 2);
    // continuation points at the real number of lines shown, not offset+limit
    assert.ok(result.output.includes('offset=2'), `expected offset=2, got:\n${result.output}`);
  });

  it('adds no byte-budget notice when the window fits the budget', async () => {
    const file = path.join(tmpDir, 'narrow.txt');
    fs.writeFileSync(file, 'a\nb\nc\n');
    const result = await fileReadTool.execute(
      { file_path: file, offset: 0, limit: 10 },
      ctx({ outputBudget: { fileReadMaxSizeBytes: 100_000 } }),
    );
    assert.ok(!result.isError);
    assert.ok(!result.output.includes('byte budget'));
  });
});

// ---------------------------------------------------------------------------
// errno-distinct access diagnostics (EACCES/EPERM vs ENOENT)
// ---------------------------------------------------------------------------
describe('file_read — errno-distinct access diagnostics', () => {
  it('reports permission-denied distinctly (not "not found") for an unreadable file', async () => {
    const file = path.join(tmpDir, 'secret.txt');
    fs.writeFileSync(file, 'top secret\n');
    // Simulate an EACCES on the R_OK access probe (robust across root/CI).
    mock.method(fs.promises, 'access', async () => {
      throw Object.assign(new Error('permission denied'), { code: 'EACCES' });
    });
    const result = await fileReadTool.execute({ file_path: file }, ctx());
    assert.ok(result.isError);
    assert.ok(result.output.includes('permission denied'), result.output);
    assert.ok(!result.output.includes('not found'), result.output);
    // still surfaces the cwd note for self-correction
    assert.ok(result.output.includes('current working directory'));
  });

  it('still reports not-found for a genuinely missing file (ENOENT path unchanged)', async () => {
    const result = await fileReadTool.execute(
      { file_path: path.join(tmpDir, 'nope.txt') },
      ctx(),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('File not found or not readable'));
    assert.ok(!result.output.includes('permission denied'));
  });
});

// ---------------------------------------------------------------------------
// Large full reads: stream via the shared reader, with a hard OOM ceiling
// ---------------------------------------------------------------------------
describe('file_read — large full reads (streaming path + OOM ceiling)', () => {
  it('streams a >10MB full read via the shared reader (allow_large) without error', async () => {
    const file = path.join(tmpDir, 'big.txt');
    const fd = fs.openSync(file, 'w');
    // ~11 MB of newline-terminated lines → forces the streaming full-read branch.
    const chunk = Buffer.from((('line-' + 'y'.repeat(60) + '\n').repeat(1000)));
    const perChunk = chunk.length;
    let written = 0;
    while (written < 11 * 1024 * 1024) { fs.writeSync(fd, chunk); written += perChunk; }
    fs.closeSync(fd);

    const result = await fileReadTool.execute(
      { file_path: file, allow_large: true },
      ctx(),
    );
    assert.ok(!result.isError, result.output.slice(0, 200));
    // content is present (token budget may truncate the tail, but the head shows)
    assert.ok(result.output.includes('line-yyyy'));
  });

  it('rejects a full read above the hard 50MB OOM ceiling even with allow_large', async () => {
    const file = path.join(tmpDir, 'huge.txt');
    const fd = fs.openSync(file, 'w');
    // > 50 MB of newline-terminated lines → streaming reader throws FileTooLargeError.
    const chunk = Buffer.from(('a'.repeat(1023) + '\n').repeat(1000)); // ~1 MB
    for (let i = 0; i < 52; i++) fs.writeSync(fd, chunk);
    fs.closeSync(fd);

    const result = await fileReadTool.execute(
      { file_path: file, allow_large: true },
      ctx(),
    );
    assert.ok(result.isError, 'expected a too-large error');
    assert.ok(result.output.includes('exceeds the maximum allowed size'), result.output.slice(0, 200));
  });
});
