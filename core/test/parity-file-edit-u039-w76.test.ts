// Parity unit U039 (wave w76) — file_edit diff-cost cap uniformity + windowed
// not-found context preview.
//
// Covers two additive parity behaviors:
//  1. The EDIT_DIFF_SCAN_MAX_BYTES "degrade to a size-only summary" fallback,
//     previously only on the full-overwrite path, now applied UNIFORMLY to the
//     single-replace, replace_all, and multi-edit paths (parity: the reference's
//     MAX_SCAN_BYTES diff fallback across edit modes). RED before the fix: those
//     paths built an O(file) unified + structured diff regardless of size.
//  2. The not-found error preview now shows a bounded context WINDOW around the
//     closest fuzzy match with true 1-based line numbers + omission markers
//     (parity: the reference's scanForContext/sliceContext), instead of always dumping
//     the first 30 lines from the top.
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileEditTool } from '../src/tools/file-edit';
import type { ToolContext } from '../src/types';

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return { cwd: tmpDir, abortSignal: new AbortController().signal, ...overrides };
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-fe-w76-'));
});
afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// A filler line that pushes the file past the 10 MiB diff-scan cap.
const BIG_FILLER = 'x'.repeat(11 * 1024 * 1024);

describe('file_edit – diff-cost cap applied uniformly across edit modes', () => {
  it('single-replace on a >10 MiB file degrades to a size-only summary (no diff)', async () => {
    const file = path.join(tmpDir, 'big.txt');
    fs.writeFileSync(file, `NEEDLE = 0;\n${BIG_FILLER}\n`);

    const res = await fileEditTool.execute(
      { file_path: file, old_string: 'NEEDLE = 0;', new_string: 'NEEDLE = 1;' },
      ctx(),
    );
    assert.equal(res.isError, undefined);
    assert.match(res.output, /diff omitted: file exceeds the 10 MiB diff cap/);
    assert.ok(!res.output.includes('@@'), 'must not contain a unified-diff hunk header');
    assert.deepEqual(res.structuredPatch, []);
    // The edit still landed on disk.
    assert.ok(fs.readFileSync(file, 'utf-8').startsWith('NEEDLE = 1;'));
  });

  it('replace_all on a >10 MiB file degrades to a size-only summary (no diff)', async () => {
    const file = path.join(tmpDir, 'big-all.txt');
    fs.writeFileSync(file, `DUP\nDUP\n${BIG_FILLER}\n`);

    const res = await fileEditTool.execute(
      { file_path: file, old_string: 'DUP', new_string: 'REPL', replace_all: true },
      ctx(),
    );
    assert.equal(res.isError, undefined);
    assert.match(res.output, /Successfully replaced 2 occurrences/);
    assert.match(res.output, /diff omitted: file exceeds the 10 MiB diff cap/);
    assert.deepEqual(res.structuredPatch, []);
  });

  it('multi-edit on a >10 MiB file degrades to a size-only summary (no diff)', async () => {
    const file = path.join(tmpDir, 'big-multi.txt');
    fs.writeFileSync(file, `ALPHA\nBETA\n${BIG_FILLER}\n`);

    const res = await fileEditTool.execute(
      {
        file_path: file,
        edits: [
          { old_string: 'ALPHA', new_string: 'A1' },
          { old_string: 'BETA', new_string: 'B1' },
        ],
      },
      ctx(),
    );
    assert.equal(res.isError, false);
    assert.match(res.output, /2\/2 edits applied/);
    assert.match(res.output, /diff omitted: file exceeds the 10 MiB diff cap/);
    assert.deepEqual(res.structuredPatch, []);
    const after = fs.readFileSync(file, 'utf-8');
    assert.ok(after.startsWith('A1\nB1\n'));
  });

  it('a small file still shows a full diff (cap does not fire)', async () => {
    const file = path.join(tmpDir, 'small.txt');
    fs.writeFileSync(file, 'hello\nworld\n');

    const res = await fileEditTool.execute(
      { file_path: file, old_string: 'hello', new_string: 'hi' },
      ctx(),
    );
    assert.equal(res.isError, undefined);
    assert.ok(!res.output.includes('diff omitted'));
    assert.ok(res.output.includes('@@') || res.output.includes('-hello'), 'small-file diff present');
    assert.ok(Array.isArray(res.structuredPatch) && res.structuredPatch.length > 0);
  });
});

describe('file_edit – windowed not-found context preview', () => {
  it('shows a bounded window around the closest match with true line numbers', async () => {
    const lines: string[] = [];
    for (let i = 1; i <= 60; i++) {
      lines.push(i === 40 ? 'const targetVariable = 42;' : `const filler${i} = ${i};`);
    }
    const file = path.join(tmpDir, 'many.ts');
    fs.writeFileSync(file, lines.join('\n') + '\n');

    const res = await fileEditTool.execute(
      // One-char typo so fuzzy exact-match misses but findClosestLines points at L40.
      { file_path: file, old_string: 'const targetVariabl = 42;', new_string: 'x' },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('not found'));
    assert.ok(res.output.includes('Did you mean'));
    // Windowed context, not the from-the-top first-30 dump.
    assert.match(res.output, /around the closest match/);
    assert.match(res.output, /^40: const targetVariable = 42;$/m);
    assert.match(res.output, /\.\.\. \(\d+ lines above\)/);
    assert.match(res.output, /\.\.\. \(\d+ lines below\)/);
    assert.ok(!res.output.includes('first 30 lines'), 'should use the windowed preview, not the top dump');
  });

  it('falls back to the from-the-top preview when there is no close match', async () => {
    const file = path.join(tmpDir, 'nomatch.ts');
    fs.writeFileSync(file, 'aaaa\nbbbb\ncccc\n');

    const res = await fileEditTool.execute(
      { file_path: file, old_string: 'zzzzzzzzzzzz', new_string: 'x' },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('not found'));
    assert.match(res.output, /first 30 lines/);
  });
});
