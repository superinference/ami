import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileWriteTool } from '../src/tools/file-write';
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
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-file-write-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Tool definition
// ---------------------------------------------------------------------------

describe('fileWriteTool – definition', () => {
  it('has the correct name', () => {
    assert.equal(fileWriteTool.name, 'file_write');
  });

  it('is not read-only', () => {
    assert.equal(fileWriteTool.isReadOnly, false);
  });

  it('schema requires file_path and content', () => {
    const req = fileWriteTool.inputSchema.required;
    assert.ok(req?.includes('file_path'));
    assert.ok(req?.includes('content'));
  });
});

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

describe('fileWriteTool – validation', () => {
  it('rejects empty file_path', async () => {
    const result = await fileWriteTool.execute({ file_path: '', content: 'x' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('file_path must not be empty'));
  });

  it('rejects whitespace-only file_path', async () => {
    const result = await fileWriteTool.execute({ file_path: '   ', content: 'x' }, ctx());
    assert.equal(result.isError, true);
  });

  it('rejects null content', async () => {
    const result = await fileWriteTool.execute(
      { file_path: path.join(tmpDir, 'f.txt'), content: null },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('content must be provided'));
  });

  it('rejects undefined content', async () => {
    const result = await fileWriteTool.execute(
      { file_path: path.join(tmpDir, 'f.txt'), content: undefined },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('content must be provided'));
  });
});

// ---------------------------------------------------------------------------
// Writing new files
// ---------------------------------------------------------------------------

describe('fileWriteTool – new files', () => {
  it('creates a new file with content', async () => {
    const file = path.join(tmpDir, 'new.txt');
    const result = await fileWriteTool.execute(
      { file_path: file, content: 'hello world\n' },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('new file'));
    assert.equal(fs.readFileSync(file, 'utf-8'), 'hello world\n');
  });

  it('creates parent directories if they do not exist', async () => {
    const file = path.join(tmpDir, 'deep', 'nested', 'dir', 'file.txt');
    const result = await fileWriteTool.execute(
      { file_path: file, content: 'nested\n' },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.equal(fs.readFileSync(file, 'utf-8'), 'nested\n');
  });

  it('shows diff lines with + prefix for new files', async () => {
    const file = path.join(tmpDir, 'diff.txt');
    const result = await fileWriteTool.execute(
      { file_path: file, content: 'line1\nline2\n' },
      ctx(),
    );
    assert.ok(result.output.includes('+line1'));
    assert.ok(result.output.includes('+line2'));
    assert.ok(result.output.includes('/dev/null'));
  });

  it('allows writing empty content', async () => {
    const file = path.join(tmpDir, 'empty.txt');
    const result = await fileWriteTool.execute(
      { file_path: file, content: '' },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.equal(fs.readFileSync(file, 'utf-8'), '');
  });
});

// ---------------------------------------------------------------------------
// Overwriting existing files
// ---------------------------------------------------------------------------

describe('fileWriteTool – overwriting', () => {
  it('overwrites existing file content', async () => {
    const file = path.join(tmpDir, 'existing.txt');
    fs.writeFileSync(file, 'old content\n');

    const result = await fileWriteTool.execute(
      { file_path: file, content: 'new content\n' },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.equal(fs.readFileSync(file, 'utf-8'), 'new content\n');
    // Should show diff with - for old and + for new
    assert.ok(result.output.includes('-old content'));
    assert.ok(result.output.includes('+new content'));
  });

  it('shows line count in output', async () => {
    const file = path.join(tmpDir, 'count.txt');
    const result = await fileWriteTool.execute(
      { file_path: file, content: 'a\nb\nc\n' },
      ctx(),
    );
    assert.ok(result.output.includes('4 lines')); // 'a\nb\nc\n' => 4 lines
  });
});

// ---------------------------------------------------------------------------
// Relative paths
// ---------------------------------------------------------------------------

describe('fileWriteTool – relative paths', () => {
  it('resolves relative paths against cwd', async () => {
    const result = await fileWriteTool.execute(
      { file_path: 'rel.txt', content: 'relative\n' },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.equal(fs.readFileSync(path.join(tmpDir, 'rel.txt'), 'utf-8'), 'relative\n');
  });
});
