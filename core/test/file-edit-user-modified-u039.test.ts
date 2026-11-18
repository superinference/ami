import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileEditTool } from '../src/tools/file-edit';
import type { ToolContext } from '../src/types';

// ===========================================================================
// U039 — file_edit result metadata.userModified reflects the approve-with-edits
// (accept-with-edits) permission signal.
//
// Parity target: Claude Code's FileEditTool outputSchema `userModified` +
// hooks/useDiffInIDE.ts computeEditsFromContents, which records that the applied
// bytes are the USER-corrected call, not the model's original proposal. AMI's
// permission layer rewrites the edit INPUT the user approved (updatedInput /
// approve-with-edited-input) and sets ToolContext.userModified; file_edit must
// surface that on metadata.userModified instead of hardcoding false.
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
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-edit-usermod-u039-'));
});
afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('U039 — file_edit metadata.userModified tracks approve-with-edits', () => {
  it('write/create: userModified true when the user edited the input', async () => {
    const f = path.join(tmpDir, 'new.txt');
    const r = await fileEditTool.execute(
      { file_path: f, content: 'hello\nworld\n' },
      ctx({ userModified: true }),
    );
    assert.equal(r.isError, undefined);
    assert.equal((r.metadata as Record<string, unknown>).userModified, true);
  });

  it('write/create: userModified false when the user did NOT edit the input', async () => {
    const f = path.join(tmpDir, 'new2.txt');
    const r = await fileEditTool.execute(
      { file_path: f, content: 'hello\nworld\n' },
      ctx(),
    );
    assert.equal(r.isError, undefined);
    assert.equal((r.metadata as Record<string, unknown>).userModified, false);
  });

  it('overwrite of an existing file: userModified true', async () => {
    const f = path.join(tmpDir, 'over.txt');
    fs.writeFileSync(f, 'original content line\nsecond line\n');
    const r = await fileEditTool.execute(
      { file_path: f, content: 'replaced content line\nsecond line\n' },
      ctx({ filesRead: new Set([f]), userModified: true }),
    );
    assert.equal(r.isError, undefined);
    assert.equal((r.metadata as Record<string, unknown>).userModified, true);
  });

  it('single string replace: userModified true', async () => {
    const f = path.join(tmpDir, 'a.txt');
    fs.writeFileSync(f, 'alpha beta gamma\n');
    const r = await fileEditTool.execute(
      { file_path: f, old_string: 'beta', new_string: 'delta' },
      ctx({ userModified: true }),
    );
    assert.equal(r.isError, undefined);
    assert.equal((r.metadata as Record<string, unknown>).userModified, true);
  });

  it('single string replace: userModified false by default', async () => {
    const f = path.join(tmpDir, 'a2.txt');
    fs.writeFileSync(f, 'alpha beta gamma\n');
    const r = await fileEditTool.execute(
      { file_path: f, old_string: 'beta', new_string: 'delta' },
      ctx(),
    );
    assert.equal(r.isError, undefined);
    assert.equal((r.metadata as Record<string, unknown>).userModified, false);
  });

  it('replace_all: userModified true', async () => {
    const f = path.join(tmpDir, 'ra.txt');
    fs.writeFileSync(f, 'x foo y foo z foo\n');
    const r = await fileEditTool.execute(
      { file_path: f, old_string: 'foo', new_string: 'bar', replace_all: true },
      ctx({ userModified: true }),
    );
    assert.equal(r.isError, undefined);
    assert.equal((r.metadata as Record<string, unknown>).replaceAll, true);
    assert.equal((r.metadata as Record<string, unknown>).userModified, true);
  });

  it('multi-edit (edits[]): userModified true', async () => {
    const f = path.join(tmpDir, 'multi.txt');
    fs.writeFileSync(f, 'one\ntwo\nthree\n');
    const r = await fileEditTool.execute(
      {
        file_path: f,
        edits: [
          { old_string: 'one', new_string: 'ONE' },
          { old_string: 'three', new_string: 'THREE' },
        ],
      },
      ctx({ userModified: true }),
    );
    assert.equal(r.isError, false);
    assert.equal((r.metadata as Record<string, unknown>).editMode, 'multiple');
    assert.equal((r.metadata as Record<string, unknown>).userModified, true);
  });
});
