import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileEditTool } from '../src/tools/file-edit';
import type { ToolContext, GitDiffSummary } from '../src/types';

// ---------------------------------------------------------------------------
// U039 / w79 — structured edit-result metadata (parity: the reference's
// FileEditTool outputSchema): gitDiff, editMode, replaceAll, userModified.
// Plus the documented intentional divergence from line-reference ("hashline")
// edits (string-match addressing is AMI's canonical edit mode).
// ---------------------------------------------------------------------------

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-fe-w79-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

function meta(r: { metadata?: Record<string, unknown> }): Record<string, unknown> {
  assert.ok(r.metadata, 'a successful edit must carry structured metadata');
  return r.metadata as Record<string, unknown>;
}
function gitDiff(r: { metadata?: Record<string, unknown> }): GitDiffSummary {
  const g = meta(r).gitDiff as GitDiffSummary | undefined;
  assert.ok(g, 'expected a gitDiff summary in metadata');
  return g;
}

describe('fileEditTool – outputSchema (Claude FileEditTool contract parity)', () => {
  it('declares an object outputSchema with gitDiff/editMode/replaceAll/userModified', () => {
    const schema = fileEditTool.outputSchema;
    assert.ok(schema, 'file_edit must declare an outputSchema');
    assert.equal(schema!.type, 'object');
    const props = schema!.properties!;
    for (const key of ['filePath', 'editMode', 'replaceAll', 'userModified', 'structuredPatch', 'gitDiff']) {
      assert.ok(Object.prototype.hasOwnProperty.call(props, key), `outputSchema must describe "${key}"`);
    }
    const gd = (props.gitDiff as { properties?: Record<string, unknown> }).properties!;
    for (const key of ['filename', 'status', 'additions', 'deletions', 'changes', 'patch']) {
      assert.ok(Object.prototype.hasOwnProperty.call(gd, key), `gitDiff must describe "${key}"`);
    }
  });

  it('documents the intentional divergence from line-reference (hashline) edits', () => {
    const d = fileEditTool.description.toLowerCase();
    assert.ok(d.includes('hashline') || d.includes('line-reference') || d.includes('line reference'),
      'description must document the string-match vs line-reference addressing choice');
    assert.ok(d.includes('string match') || d.includes('exact string'),
      'description must state string-match is the canonical addressing scheme');
  });
});

describe('fileEditTool – edit result metadata across modes', () => {
  it('single-replace: editMode single, modified gitDiff with changes = additions + deletions', async () => {
    const file = path.join(tmpDir, 'a.ts');
    fs.writeFileSync(file, 'const x = 1;\nconst y = 2;\n');
    const filesRead = new Set([file]);
    const r = await fileEditTool.execute(
      { file_path: file, old_string: 'const x = 1;', new_string: 'const x = 42;' },
      ctx({ filesRead }),
    );
    assert.ok(!r.isError, r.output);
    const m = meta(r);
    assert.equal(m.filePath, file);
    assert.equal(m.editMode, 'single');
    assert.equal(m.replaceAll, false);
    assert.equal(m.userModified, false);
    const g = gitDiff(r);
    assert.equal(g.filename, file);
    assert.equal(g.status, 'modified');
    assert.equal(g.changes, g.additions + g.deletions);
    assert.ok(g.additions >= 1 && g.deletions >= 1, 'a one-line change adds and removes a line');
    assert.ok(g.patch.length > 0, 'gitDiff carries the unified patch');
  });

  it('write/create: editMode single, status added, deletions 0, additions = line count', async () => {
    const file = path.join(tmpDir, 'new.txt');
    const r = await fileEditTool.execute(
      { file_path: file, content: 'line one\nline two\nline three\n' },
      ctx({ filesRead: new Set() }),
    );
    assert.ok(!r.isError, r.output);
    assert.equal(meta(r).editMode, 'single');
    const g = gitDiff(r);
    assert.equal(g.status, 'added');
    assert.equal(g.deletions, 0);
    assert.equal(g.additions, 3);
    assert.equal(g.changes, 3);
  });

  it('full overwrite of an existing file: status modified', async () => {
    const file = path.join(tmpDir, 'over.txt');
    fs.writeFileSync(file, 'old a\nold b\nold c\n');
    const filesRead = new Set([file]);
    const r = await fileEditTool.execute(
      { file_path: file, content: 'brand new\n' },
      ctx({ filesRead }),
    );
    assert.ok(!r.isError, r.output);
    assert.equal(meta(r).editMode, 'single');
    assert.equal(gitDiff(r).status, 'modified');
  });

  it('replace_all: editMode single, replaceAll true', async () => {
    const file = path.join(tmpDir, 'all.ts');
    fs.writeFileSync(file, 'foo\nfoo\nbar\nfoo\n');
    const filesRead = new Set([file]);
    const r = await fileEditTool.execute(
      { file_path: file, old_string: 'foo', new_string: 'baz', replace_all: true },
      ctx({ filesRead }),
    );
    assert.ok(!r.isError, r.output);
    const m = meta(r);
    assert.equal(m.editMode, 'single');
    assert.equal(m.replaceAll, true);
    assert.equal(gitDiff(r).status, 'modified');
  });

  it('multi-edit (edits[]): editMode multiple with a modified gitDiff', async () => {
    const file = path.join(tmpDir, 'multi.ts');
    fs.writeFileSync(file, 'alpha\nbeta\ngamma\n');
    const filesRead = new Set([file]);
    const r = await fileEditTool.execute(
      {
        file_path: file,
        edits: [
          { old_string: 'alpha', new_string: 'ALPHA' },
          { old_string: 'gamma', new_string: 'GAMMA' },
        ],
      },
      ctx({ filesRead }),
    );
    assert.ok(!r.isError, r.output);
    const m = meta(r);
    assert.equal(m.editMode, 'multiple');
    assert.equal(m.replaceAll, false);
    assert.equal(m.userModified, false);
    const g = gitDiff(r);
    assert.equal(g.status, 'modified');
    assert.equal(g.changes, g.additions + g.deletions);
  });

  it('error results do NOT carry success metadata', async () => {
    const file = path.join(tmpDir, 'err.ts');
    fs.writeFileSync(file, 'const x = 1;\n');
    const filesRead = new Set([file]);
    const r = await fileEditTool.execute(
      { file_path: file, old_string: 'NOT PRESENT', new_string: 'whatever' },
      ctx({ filesRead }),
    );
    assert.ok(r.isError);
    assert.equal(r.metadata, undefined, 'a failed edit must not emit the success output contract');
  });

  it('end-to-end: on-disk content and metadata agree after a real edit pipeline', async () => {
    const file = path.join(tmpDir, 'e2e.ts');
    fs.writeFileSync(file, 'export const n = 0;\n');
    const filesRead = new Set([file]);
    const r = await fileEditTool.execute(
      { file_path: file, old_string: 'const n = 0;', new_string: 'const n = 123;' },
      ctx({ filesRead }),
    );
    assert.ok(!r.isError, r.output);
    // Real write happened.
    assert.equal(fs.readFileSync(file, 'utf-8'), 'export const n = 123;\n');
    // Metadata describes exactly that one-line modification.
    const g = gitDiff(r);
    assert.equal(g.status, 'modified');
    assert.ok(g.patch.includes('const n = 123;'), 'patch reflects the new content');
    assert.equal(g.changes, g.additions + g.deletions);
  });
});
