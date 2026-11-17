// Unit U039 — read-before-edit enforcement for the string-replace paths.
//
// Parity gap (references/claude/claude-code-main/src/tools/FileEditTool/prompt.ts
// L4-6, getPreReadInstruction): Claude Code's FileEditTool guarantees, for EVERY
// edit, that "This tool will error if you attempt an edit without reading the
// file". AMI previously enforced read-before-write only on the full-overwrite
// path (the H10 guard); the single-replace, replace_all, and multi-edit (edits[])
// paths silently auto-read instead, allowing a blind mutation. This unit extends
// the hard guard to every in-place edit mode.
//
// Red-green: revert `requireReadBeforeEdit` (+ its two call sites) in
// file-edit.ts and the "blocks …" tests below flip to green-on-old-behavior
// (the edit would succeed), so they fail — proving they discriminate the fix.
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileEditTool } from '../src/tools/file-edit';
import { fileReadTool } from '../src/tools/file-read';
import { getFileCache, releaseFileCache } from '../src/file-cache';
import type { ToolContext } from '../src/types';

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    filesRead: new Set<string>(),
    ...overrides,
  };
}

beforeEach(() => {
  tmpDir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'si-u039-rbe-')));
  getFileCache(tmpDir).clear();
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('fileEditTool – read-before-edit guard (string-replace paths)', () => {
  it('blocks a single replace on a non-empty file that was not read first', async () => {
    const fp = path.join(tmpDir, 'blind.ts');
    fs.writeFileSync(fp, 'const x = 1;\n');
    const result = await fileEditTool.execute(
      { file_path: fp, old_string: 'const x = 1;', new_string: 'const x = 2;' },
      ctx(),
    );
    assert.equal(result.isError, true, result.output);
    assert.match(result.output, /must file_read/);
    // The guard must prevent the mutation entirely.
    assert.equal(fs.readFileSync(fp, 'utf-8'), 'const x = 1;\n');
  });

  it('blocks a replace_all on a non-empty file that was not read first', async () => {
    const fp = path.join(tmpDir, 'blind-all.ts');
    fs.writeFileSync(fp, 'a\na\na\n');
    const result = await fileEditTool.execute(
      { file_path: fp, old_string: 'a', new_string: 'b', replace_all: true },
      ctx(),
    );
    assert.equal(result.isError, true, result.output);
    assert.match(result.output, /must file_read/);
    assert.equal(fs.readFileSync(fp, 'utf-8'), 'a\na\na\n');
  });

  it('blocks a multi-edit (edits[]) batch on a non-empty file that was not read first', async () => {
    const fp = path.join(tmpDir, 'blind-batch.ts');
    fs.writeFileSync(fp, 'one two\n');
    const result = await fileEditTool.execute(
      { file_path: fp, edits: [{ old_string: 'one', new_string: 'ONE' }] },
      ctx(),
    );
    assert.equal(result.isError, true, result.output);
    assert.match(result.output, /must file_read/);
    assert.equal(fs.readFileSync(fp, 'utf-8'), 'one two\n');
  });

  it('allows a replace once the file has been read (seeded in filesRead)', async () => {
    const fp = path.join(tmpDir, 'seen.ts');
    fs.writeFileSync(fp, 'const x = 1;\n');
    const result = await fileEditTool.execute(
      { file_path: fp, old_string: 'const x = 1;', new_string: 'const x = 2;' },
      ctx({ filesRead: new Set([fp]) }),
    );
    assert.ok(!result.isError, result.output);
    assert.equal(fs.readFileSync(fp, 'utf-8'), 'const x = 2;\n');
  });

  it('exempts an empty existing file (nothing unseen to clobber)', async () => {
    const fp = path.join(tmpDir, 'empty.ts');
    fs.writeFileSync(fp, '');
    const result = await fileEditTool.execute(
      { file_path: fp, old_string: 'x', new_string: 'y' },
      ctx(),
    );
    // Not blocked by the read guard; it fails later on its own merits (not found).
    assert.equal(result.isError, true, result.output);
    assert.doesNotMatch(result.output, /must file_read/);
  });

  it('skips the guard entirely when filesRead tracking is disabled (undefined)', async () => {
    const fp = path.join(tmpDir, 'untracked.ts');
    fs.writeFileSync(fp, 'const x = 1;\n');
    const result = await fileEditTool.execute(
      { file_path: fp, old_string: 'const x = 1;', new_string: 'const x = 2;' },
      ctx({ filesRead: undefined }),
    );
    assert.ok(!result.isError, result.output);
    assert.equal(fs.readFileSync(fp, 'utf-8'), 'const x = 2;\n');
  });
});

describe('fileEditTool – read-before-edit end-to-end (real file_read → file_edit)', () => {
  it('a real file_read satisfies the guard for a subsequent replace', async () => {
    const fp = path.join(tmpDir, 'e2e.ts');
    fs.writeFileSync(fp, 'hello world\n');
    const context = ctx();

    // First the model reads the file through the REAL read tool …
    const read = await fileReadTool.execute({ file_path: fp }, context);
    assert.ok(!read.isError, read.output);

    // … then the edit is permitted and lands.
    const edit = await fileEditTool.execute(
      { file_path: fp, old_string: 'hello world', new_string: 'goodbye world' },
      context,
    );
    assert.ok(!edit.isError, edit.output);
    assert.equal(fs.readFileSync(fp, 'utf-8'), 'goodbye world\n');
  });

  it('a cache-hit re-read still registers filesRead so a later edit is not blocked', async () => {
    // Regression: the full-reread "File unchanged" cache short-circuit used to
    // return without adding to context.filesRead, so a read→(cache hit)→edit in a
    // fresh context (e.g. a forked sub-agent inheriting a clone-on-fork cache) was
    // wrongly blocked by the new guard. The read must register even on a cache hit.
    const fp = path.join(tmpDir, 'cached.txt');
    fs.writeFileSync(fp, 'original line\n');

    // Parent reads it → shared cache primed with current bytes + mtime.
    await fileReadTool.execute({ file_path: fp }, ctx());

    // A sub-agent (its own context + isolated clone-on-fork cache) re-reads: this
    // hits the unchanged-file cache and returns the stub.
    const childCtx = ctx({ agentId: 'sub-u039' });
    const reread = await fileReadTool.execute({ file_path: fp }, childCtx);
    assert.match(reread.output, /unchanged/i);
    assert.ok(childCtx.filesRead!.has(fp), 'a cache-hit read must still register filesRead');

    // The child edit is therefore permitted.
    const edit = await fileEditTool.execute(
      { file_path: fp, old_string: 'original line', new_string: 'rewritten by child' },
      childCtx,
    );
    assert.ok(!edit.isError, edit.output);

    releaseFileCache(tmpDir, 'sub-u039');
  });
});
