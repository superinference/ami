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

describe('fileWriteTool – workspace boundary', () => {
  it('rejects paths outside workspace', async () => {
    const result = await fileWriteTool.execute(
      { file_path: '/tmp/outside-workspace.txt', content: 'nope' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('outside the workspace'));
  });

  it('allows writing to cwd itself', async () => {
    const file = path.join(tmpDir, 'inroot.txt');
    const result = await fileWriteTool.execute(
      { file_path: file, content: 'in root\n' },
      ctx(),
    );
    assert.ok(!result.isError);
  });
});

// ---------------------------------------------------------------------------
// Read-before-write enforcement
// ---------------------------------------------------------------------------

describe('fileWriteTool – read-before-write enforcement', () => {
  it('blocks overwrite when filesRead is set but file was not read', async () => {
    const file = path.join(tmpDir, 'existing-unread.txt');
    fs.writeFileSync(file, 'original\n');

    const result = await fileWriteTool.execute(
      { file_path: file, content: 'overwritten\n' },
      ctx({ filesRead: new Set() }),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('must read'));
  });

  it('allows overwrite when file was previously read', async () => {
    const file = path.join(tmpDir, 'existing-read.txt');
    fs.writeFileSync(file, 'original\n');
    const filesRead = new Set([file]);

    const result = await fileWriteTool.execute(
      { file_path: file, content: 'overwritten\n' },
      ctx({ filesRead }),
    );
    assert.ok(!result.isError);
    assert.equal(fs.readFileSync(file, 'utf-8'), 'overwritten\n');
  });

  it('allows writing new files without prior read', async () => {
    const file = path.join(tmpDir, 'brand-new.txt');

    const result = await fileWriteTool.execute(
      { file_path: file, content: 'new content\n' },
      ctx({ filesRead: new Set() }),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('new file'));
  });

  it('skips enforcement when filesRead is undefined', async () => {
    const file = path.join(tmpDir, 'no-tracking.txt');
    fs.writeFileSync(file, 'original\n');

    const result = await fileWriteTool.execute(
      { file_path: file, content: 'overwritten\n' },
      ctx(),
    );
    assert.ok(!result.isError);
  });

  it('does not modify file when blocked by read-before-write', async () => {
    const file = path.join(tmpDir, 'protected.txt');
    fs.writeFileSync(file, 'original\n');

    await fileWriteTool.execute(
      { file_path: file, content: 'should-not-write\n' },
      ctx({ filesRead: new Set() }),
    );
    assert.equal(fs.readFileSync(file, 'utf-8'), 'original\n');
  });
});

// ---------------------------------------------------------------------------
// CRLF-aware file writing
// ---------------------------------------------------------------------------

describe('fileWriteTool – CRLF handling', () => {
  it('preserves CRLF when overwriting CRLF file', async () => {
    const file = path.join(tmpDir, 'crlf.txt');
    fs.writeFileSync(file, 'old1\r\nold2\r\n');

    const result = await fileWriteTool.execute(
      { file_path: file, content: 'new1\nnew2\n' },
      ctx(),
    );
    assert.ok(!result.isError);
    const content = fs.readFileSync(file, 'utf-8');
    assert.ok(content.includes('\r\n'), 'Should preserve CRLF');
    assert.equal(content, 'new1\r\nnew2\r\n');
  });

  it('preserves LF when overwriting LF file', async () => {
    const file = path.join(tmpDir, 'lf.txt');
    fs.writeFileSync(file, 'old1\nold2\n');

    const result = await fileWriteTool.execute(
      { file_path: file, content: 'new1\nnew2\n' },
      ctx(),
    );
    assert.ok(!result.isError);
    const content = fs.readFileSync(file, 'utf-8');
    assert.ok(!content.includes('\r\n'));
    assert.equal(content, 'new1\nnew2\n');
  });

  it('uses content as-is for new files', async () => {
    const file = path.join(tmpDir, 'brand-new-crlf.txt');

    const result = await fileWriteTool.execute(
      { file_path: file, content: 'line1\nline2\n' },
      ctx(),
    );
    assert.ok(!result.isError);
    const content = fs.readFileSync(file, 'utf-8');
    assert.equal(content, 'line1\nline2\n');
  });
});

describe('fileWriteTool – large file diff truncation', () => {
  it('truncates new file diff at 20 lines', async () => {
    const file = path.join(tmpDir, 'large-new.txt');
    const lines = Array.from({ length: 30 }, (_, i) => `line${i + 1}`);
    const result = await fileWriteTool.execute(
      { file_path: file, content: lines.join('\n') + '\n' },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('more lines'));
  });

  it('truncates overwrite diff at 20 lines for both old and new', async () => {
    const file = path.join(tmpDir, 'large-overwrite.txt');
    const oldLines = Array.from({ length: 25 }, (_, i) => `old${i + 1}`);
    fs.writeFileSync(file, oldLines.join('\n') + '\n');

    const newLines = Array.from({ length: 25 }, (_, i) => `new${i + 1}`);
    const result = await fileWriteTool.execute(
      { file_path: file, content: newLines.join('\n') + '\n' },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('more removed'));
    assert.ok(result.output.includes('more added'));
  });
});
