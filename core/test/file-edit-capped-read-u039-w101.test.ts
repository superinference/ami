import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileEditTool, readHooks } from '../src/tools/file-edit';
import type { ToolContext } from '../src/types';

// ===========================================================================
// U039 / w101 — the overwrite path reads the existing file through the
// memory-bounded readFileCapped seam (readHooks.readCapped) instead of an
// unbounded fs.readFile.
//
// Parity intent: the reference's readCapped diff-read fallback — a file that is
// (or GROWS past the earlier stat, a TOCTOU window) beyond the cap is reported
// as truncated and never slurped whole, degrading to a size-limit error rather
// than risking an OOM. For a normal file the full content is read exactly as
// before (regression lock).
// ===========================================================================

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-edit-capped-u039-'));
});
afterEach(() => {
  mock.restoreAll();
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('U039/w101 — overwrite uses the memory-bounded capped read', () => {
  it('reads the full existing content through the capped read (regression)', async () => {
    const f = path.join(tmpDir, 'over.txt');
    fs.writeFileSync(f, 'original FIRST line\noriginal SECOND line\n');
    const r = await fileEditTool.execute(
      { file_path: f, content: 'brand NEW first line\noriginal SECOND line\n' },
      ctx({ filesRead: new Set([f]) }),
    );
    assert.equal(r.isError, undefined);
    // The diff must reflect the OLD content having been read (the removed line),
    // proving the capped read returned the real file bytes, not an empty buffer.
    assert.match(r.output, /original FIRST line/);
    assert.match(r.output, /brand NEW first line/);
    assert.equal(fs.readFileSync(f, 'utf8'), 'brand NEW first line\noriginal SECOND line\n');
  });

  it('blocks an overwrite when the file grew past the cap during the read', async () => {
    const f = path.join(tmpDir, 'grew.txt');
    fs.writeFileSync(f, 'small original content\n');
    // Simulate a file that grew beyond the 1 GiB cap AFTER the stat guard, so the
    // bounded read reports truncation (infeasible to reproduce with a real file).
    mock.method(readHooks, 'readCapped', async () => ({ buffer: Buffer.alloc(0), truncated: true }));
    const r = await fileEditTool.execute(
      { file_path: f, content: 'replacement\n' },
      ctx({ filesRead: new Set([f]) }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /exceeds the 1 GiB edit limit/);
    assert.equal(r.behavior, 'ask');
    // The on-disk file must be left untouched when the read is refused.
    assert.equal(fs.readFileSync(f, 'utf8'), 'small original content\n');
  });

  it('treats a null capped read (absent file) as a brand-new file create', async () => {
    const f = path.join(tmpDir, 'fresh.txt');
    // readFileCapped returns null on ENOENT; the overwrite path must create it.
    const r = await fileEditTool.execute(
      { file_path: f, content: 'created content\n' },
      ctx(),
    );
    assert.equal(r.isError, undefined);
    assert.match(r.output, /new file/);
    assert.equal(fs.readFileSync(f, 'utf8'), 'created content\n');
  });
});
