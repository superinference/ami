import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { multiEditTool } from '../src/tools/multi-edit';
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
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-multi-edit-unit-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Tool definition
// ---------------------------------------------------------------------------

describe('multiEditTool – definition', () => {
  it('has the correct name', () => {
    assert.equal(multiEditTool.name, 'multi_edit');
  });

  it('is not read-only', () => {
    assert.equal(multiEditTool.isReadOnly, false);
  });

  it('schema requires file_path and edits', () => {
    const req = multiEditTool.inputSchema.required;
    assert.ok(req?.includes('file_path'));
    assert.ok(req?.includes('edits'));
  });
});

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

describe('multiEditTool – validation', () => {
  it('rejects empty file_path', async () => {
    const result = await multiEditTool.execute(
      { file_path: '', edits: [{ old_string: 'a', new_string: 'b' }] },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('file_path must not be empty'));
  });

  it('rejects missing edits', async () => {
    const result = await multiEditTool.execute(
      { file_path: path.join(tmpDir, 'f.txt'), edits: [] },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('non-empty array'));
  });

  it('rejects non-array edits', async () => {
    const result = await multiEditTool.execute(
      { file_path: path.join(tmpDir, 'f.txt'), edits: 'not an array' },
      ctx(),
    );
    assert.equal(result.isError, true);
  });

  it('rejects when file does not exist', async () => {
    const result = await multiEditTool.execute(
      { file_path: '/nonexistent.txt', edits: [{ old_string: 'a', new_string: 'b' }] },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Cannot read file'));
  });
});

// ---------------------------------------------------------------------------
// Edits
// ---------------------------------------------------------------------------

describe('multiEditTool – edits', () => {
  it('applies multiple edits successfully', async () => {
    const file = path.join(tmpDir, 'test.ts');
    fs.writeFileSync(file, 'const a = 1;\nconst b = 2;\nconst c = 3;\n');

    const result = await multiEditTool.execute({
      file_path: file,
      edits: [
        { old_string: 'const a = 1;', new_string: 'const a = 10;' },
        { old_string: 'const c = 3;', new_string: 'const c = 30;' },
      ],
    }, ctx());

    assert.ok(!result.isError);
    assert.ok(result.output.includes('2/2 edits applied'));
    const content = fs.readFileSync(file, 'utf-8');
    assert.ok(content.includes('const a = 10;'));
    assert.ok(content.includes('const c = 30;'));
  });

  it('applies edits sequentially', async () => {
    const file = path.join(tmpDir, 'seq.ts');
    fs.writeFileSync(file, 'AAA\n');

    const result = await multiEditTool.execute({
      file_path: file,
      edits: [
        { old_string: 'AAA', new_string: 'BBB' },
        { old_string: 'BBB', new_string: 'CCC' },
      ],
    }, ctx());

    assert.ok(!result.isError);
    assert.equal(fs.readFileSync(file, 'utf-8'), 'CCC\n');
  });

  it('reports partial success with failed edits', async () => {
    const file = path.join(tmpDir, 'partial.ts');
    fs.writeFileSync(file, 'alpha\nbeta\n');

    const result = await multiEditTool.execute({
      file_path: file,
      edits: [
        { old_string: 'alpha', new_string: 'ALPHA' },
        { old_string: 'nonexistent', new_string: 'nope' },
      ],
    }, ctx());

    assert.ok(!result.isError); // partial success is not an error
    assert.ok(result.output.includes('1/2 edits applied'));
    assert.ok(result.output.includes('Failed edits'));
  });

  it('skips edits with empty old_string', async () => {
    const file = path.join(tmpDir, 'empty.ts');
    fs.writeFileSync(file, 'content\n');

    const result = await multiEditTool.execute({
      file_path: file,
      edits: [
        { old_string: '', new_string: 'x' },
        { old_string: 'content', new_string: 'replaced' },
      ],
    }, ctx());

    assert.ok(!result.isError);
    assert.ok(result.output.includes('1/2 edits applied'));
  });

  it('skips edits with identical old_string and new_string', async () => {
    const file = path.join(tmpDir, 'identical.ts');
    fs.writeFileSync(file, 'same\n');

    const result = await multiEditTool.execute({
      file_path: file,
      edits: [{ old_string: 'same', new_string: 'same' }],
    }, ctx());

    assert.equal(result.isError, true);
    assert.ok(result.output.includes('identical'));
  });

  it('fails completely when no edits succeed', async () => {
    const file = path.join(tmpDir, 'nope.ts');
    fs.writeFileSync(file, 'content\n');

    const result = await multiEditTool.execute({
      file_path: file,
      edits: [{ old_string: 'nonexistent', new_string: 'x' }],
    }, ctx());

    assert.equal(result.isError, true);
    assert.ok(result.output.includes('No edits applied'));
  });

  it('shows "did you mean" hints for failed edits', async () => {
    const file = path.join(tmpDir, 'hints.ts');
    fs.writeFileSync(file, 'const fooBar = 1;\n');

    const result = await multiEditTool.execute({
      file_path: file,
      edits: [{ old_string: 'const fooBaz = 1;', new_string: 'x' }],
    }, ctx());

    assert.equal(result.isError, true);
    assert.ok(result.output.includes('did you mean'));
  });

  it('handles multiple matches error', async () => {
    const file = path.join(tmpDir, 'dup.ts');
    fs.writeFileSync(file, 'foo\nbar\nfoo\n');

    const result = await multiEditTool.execute({
      file_path: file,
      edits: [{ old_string: 'foo', new_string: 'baz' }],
    }, ctx());

    assert.equal(result.isError, true);
    assert.ok(result.output.includes('times'));
  });
});

// ---------------------------------------------------------------------------
// Relative paths
// ---------------------------------------------------------------------------

describe('multiEditTool – relative paths', () => {
  it('resolves relative paths against cwd', async () => {
    const file = path.join(tmpDir, 'rel.ts');
    fs.writeFileSync(file, 'old\n');

    const result = await multiEditTool.execute({
      file_path: 'rel.ts',
      edits: [{ old_string: 'old', new_string: 'new' }],
    }, ctx());

    assert.ok(!result.isError);
    assert.equal(fs.readFileSync(file, 'utf-8'), 'new\n');
  });
});
