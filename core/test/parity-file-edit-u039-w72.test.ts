// Unit U039 (wave w72) — additive parity gaps for core/core/src/tools/file-edit.ts:
//  - Multi-edit (edits[]) idempotent-duplicate recognition: an edit equivalent to
//    an earlier one in the same batch (identical inputs, or the same effect on the
//    original file) is SKIPPED with a clear note instead of being re-applied or
//    failing with a confusing "not found" once the earlier edit rewrote the region
//    (parity: the reference's areFileEditsEquivalent). The equivalence helper is the
//    ONE canonical core util (file-edit-equivalence.ts), reused here.
//  - Accurate 1-based edit location for the MULTI-edit path: the result now opens
//    with the same `[edit: file:LINE, +a -b]` location tag the single-replace path
//    emits, so the model/UI can locate the change without re-reading the file
//    (parity: the reference's readEditContext lineOffset, applied to both modes).
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileEditTool } from '../src/tools/file-edit';
import type { ToolContext } from '../src/types';

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    detachedMode: true,
    ...overrides,
  };
}

beforeEach(() => {
  tmpDir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'si-u039w72-')));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('fileEditTool – multi-edit accurate 1-based location tag', () => {
  it('opens the result with [edit: file:LINE, +a -b] at the true line', async () => {
    const file = path.join(tmpDir, 'loc.ts');
    fs.writeFileSync(file, 'line1\nline2\nline3\nline4\n');

    const res = await fileEditTool.execute(
      { file_path: file, edits: [{ old_string: 'line3', new_string: 'LINE3' }] },
      ctx({ filesRead: new Set([file]) }),
    );

    assert.equal(res.isError ?? false, false, res.output);
    // The change lands on line 3 of the original file.
    assert.match(res.output, /^\[edit: loc\.ts:3, \+1 -1\]/, res.output);
    assert.ok(res.output.includes('Successfully edited'));
  });
});

describe('fileEditTool – multi-edit duplicate recognition', () => {
  it('skips a literally-identical duplicate edit in the batch', async () => {
    const file = path.join(tmpDir, 'dup.ts');
    fs.writeFileSync(file, 'const a = 1;\nconst b = 2;\n');

    const res = await fileEditTool.execute(
      {
        file_path: file,
        edits: [
          { old_string: 'const a = 1;', new_string: 'const a = 99;' },
          { old_string: 'const a = 1;', new_string: 'const a = 99;' },
        ],
      },
      ctx({ filesRead: new Set([file]) }),
    );

    assert.equal(res.isError ?? false, false, res.output);
    // The edit is applied exactly once; the file is not double-mangled.
    assert.equal(fs.readFileSync(file, 'utf-8'), 'const a = 99;\nconst b = 2;\n');
    // The duplicate is reported as skipped, not as a "not found" failure.
    assert.match(res.output, /Skipped edits:/, res.output);
    assert.match(res.output, /Edit 2: skipped \(duplicate/, res.output);
    assert.ok(!/not found/.test(res.output), 'duplicate must not surface as "not found"');
    assert.match(res.output, /1\/2 edits applied/, res.output);
  });

  it('skips a semantically-equivalent duplicate (same effect, different inputs)', async () => {
    const file = path.join(tmpDir, 'sem.ts');
    // A single occurrence of foo, so replace_all and plain replace are equivalent.
    fs.writeFileSync(file, 'value = foo;\n');

    const res = await fileEditTool.execute(
      {
        file_path: file,
        edits: [
          { old_string: 'foo', new_string: 'bar' },
          { old_string: 'foo', new_string: 'bar', replace_all: true },
        ],
      },
      ctx({ filesRead: new Set([file]) }),
    );

    assert.equal(res.isError ?? false, false, res.output);
    assert.equal(fs.readFileSync(file, 'utf-8'), 'value = bar;\n');
    assert.match(res.output, /Edit 2: skipped \(duplicate/, res.output);
  });

  it('keeps distinct edits that target different content', async () => {
    const file = path.join(tmpDir, 'distinct.ts');
    fs.writeFileSync(file, 'alpha\nbeta\ngamma\n');

    const res = await fileEditTool.execute(
      {
        file_path: file,
        edits: [
          { old_string: 'alpha', new_string: 'A' },
          { old_string: 'gamma', new_string: 'G' },
        ],
      },
      ctx({ filesRead: new Set([file]) }),
    );

    assert.equal(res.isError ?? false, false, res.output);
    assert.equal(fs.readFileSync(file, 'utf-8'), 'A\nbeta\nG\n');
    assert.ok(!/Skipped edits:/.test(res.output), 'distinct edits must not be deduped');
    assert.match(res.output, /2\/2 edits applied/, res.output);
  });
});
