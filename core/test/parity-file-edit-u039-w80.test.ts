import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileEditTool } from '../src/tools/file-edit';
import { getFileCache } from '../src/file-cache';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// U039 / w80 — HARD read-before-edit enforcement on the string-replace and
// multi-edit paths (parity: Claude Code's FileEditTool `validateInput`,
// errorCode 6: "File has not been read yet. Read it first before writing to
// it.", plus its isPartialView rejection). AMI previously AUTO-READ on these
// paths — a leniency that let a blind edit through and contradicted the tool's
// own "You MUST file_read an existing file before changing it" contract. These
// tests lock the reference behavior: an existing file that the model never fully
// read cannot be edited via a targeted replace until it is read.
// ---------------------------------------------------------------------------

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

/** Mark a file as fully read, exactly as file_read seeds the shared cache. */
function markRead(file: string, filesRead: Set<string>): void {
  filesRead.add(file);
  getFileCache(tmpDir).setWritten(file, fs.readFileSync(file, 'utf-8'));
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-fe-w80-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('U039/w80 — hard read-before-edit on the string-replace path', () => {
  it('blocks a replace of an existing file that was never read', async () => {
    const file = path.join(tmpDir, 'a.ts');
    fs.writeFileSync(file, 'const x = 1;\n');
    const filesRead = new Set<string>();

    const r = await fileEditTool.execute(
      { file_path: file, old_string: 'const x = 1;', new_string: 'const x = 2;' },
      ctx({ filesRead }),
    );

    assert.equal(r.isError, true, r.output);
    assert.match(r.output, /must file_read .* before editing it/);
    // The on-disk file is untouched and filesRead is NOT silently seeded.
    assert.equal(fs.readFileSync(file, 'utf-8'), 'const x = 1;\n');
    assert.ok(!filesRead.has(file), 'a blocked edit must not auto-seed filesRead');
  });

  it('allows the replace once the file has been read', async () => {
    const file = path.join(tmpDir, 'b.ts');
    fs.writeFileSync(file, 'const x = 1;\n');
    const filesRead = new Set<string>();
    markRead(file, filesRead);

    const r = await fileEditTool.execute(
      { file_path: file, old_string: 'const x = 1;', new_string: 'const x = 2;' },
      ctx({ filesRead }),
    );

    assert.ok(!r.isError, r.output);
    assert.equal(fs.readFileSync(file, 'utf-8'), 'const x = 2;\n');
  });

  it('blocks a replace when only a PARTIAL (size-capped) view was read', async () => {
    const file = path.join(tmpDir, 'partial.ts');
    fs.writeFileSync(file, 'const x = 1;\n');
    const filesRead = new Set<string>([file]);
    // file_read of a size-capped file seeds filesRead but marks the view partial.
    getFileCache(tmpDir).markPartialView(file, fs.statSync(file).mtimeMs);

    const r = await fileEditTool.execute(
      { file_path: file, old_string: 'const x = 1;', new_string: 'const x = 2;' },
      ctx({ filesRead }),
    );

    assert.equal(r.isError, true, r.output);
    assert.match(r.output, /PARTIAL view/);
    assert.equal(fs.readFileSync(file, 'utf-8'), 'const x = 1;\n');
  });

  it('stays lenient when filesRead tracking is disabled (undefined)', async () => {
    const file = path.join(tmpDir, 'untracked.ts');
    fs.writeFileSync(file, 'const x = 1;\n');

    const r = await fileEditTool.execute(
      { file_path: file, old_string: 'const x = 1;', new_string: 'const x = 2;' },
      ctx(),
    );

    assert.ok(!r.isError, r.output);
    assert.equal(fs.readFileSync(file, 'utf-8'), 'const x = 2;\n');
  });
});

describe('U039/w80 — hard read-before-edit on the multi-edit (edits[]) path', () => {
  it('blocks an edits[] batch on an existing file that was never read', async () => {
    const file = path.join(tmpDir, 'multi.ts');
    fs.writeFileSync(file, 'a\nb\n');
    const filesRead = new Set<string>();

    const r = await fileEditTool.execute(
      { file_path: file, edits: [{ old_string: 'a', new_string: 'A' }] },
      ctx({ filesRead }),
    );

    assert.equal(r.isError, true, r.output);
    assert.match(r.output, /must file_read .* before editing it/);
    assert.equal(fs.readFileSync(file, 'utf-8'), 'a\nb\n');
    assert.ok(!filesRead.has(file), 'a blocked batch must not auto-seed filesRead');
  });

  it('allows the edits[] batch once the file has been read', async () => {
    const file = path.join(tmpDir, 'multi-ok.ts');
    fs.writeFileSync(file, 'a\nb\n');
    const filesRead = new Set<string>();
    markRead(file, filesRead);

    const r = await fileEditTool.execute(
      { file_path: file, edits: [{ old_string: 'a', new_string: 'A' }] },
      ctx({ filesRead }),
    );

    assert.ok(!r.isError, r.output);
    assert.equal(fs.readFileSync(file, 'utf-8'), 'A\nb\n');
  });
});
