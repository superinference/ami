import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileReadTool } from '../src/tools/file-read';
import { getFileCache } from '../src/file-cache';
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
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-file-read-'));
  // Clear file cache for the temp dir so each test starts fresh
  getFileCache(tmpDir).clear();
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Tool definition
// ---------------------------------------------------------------------------

describe('fileReadTool – definition', () => {
  it('has the correct name', () => {
    assert.equal(fileReadTool.name, 'file_read');
  });

  it('is read-only and concurrency-safe', () => {
    assert.equal(fileReadTool.isReadOnly, true);
    assert.equal(fileReadTool.isConcurrencySafe, true);
  });

  it('schema requires file_path', () => {
    assert.ok(fileReadTool.inputSchema.required?.includes('file_path'));
  });
});

// ---------------------------------------------------------------------------
// Basic file reading
// ---------------------------------------------------------------------------

describe('fileReadTool – basic reading', () => {
  it('reads a simple text file with line numbers', async () => {
    const file = path.join(tmpDir, 'hello.txt');
    fs.writeFileSync(file, 'hello\nworld\n');

    const result = await fileReadTool.execute({ file_path: file }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('hello'));
    assert.ok(result.output.includes('world'));
    assert.ok(result.output.includes('File:'));
    assert.ok(result.output.includes('3 lines')); // 'hello\nworld\n' splits to 3 lines
  });

  it('returns error for empty file_path', async () => {
    const result = await fileReadTool.execute({ file_path: '' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('must not be empty'));
  });

  it('returns error for non-existent file', async () => {
    const result = await fileReadTool.execute({ file_path: path.join(tmpDir, 'no-such-file.txt') }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('not found') || result.output.includes('not readable'));
  });

  it('returns error when path is a directory', async () => {
    const result = await fileReadTool.execute({ file_path: tmpDir }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('directory'));
  });

  it('returns (empty file) for a zero-length file', async () => {
    const file = path.join(tmpDir, 'empty.txt');
    fs.writeFileSync(file, '');

    const result = await fileReadTool.execute({ file_path: file }, ctx());
    assert.ok(result.output.includes('empty file'));
  });
});

// ---------------------------------------------------------------------------
// Relative paths
// ---------------------------------------------------------------------------

describe('fileReadTool – relative paths', () => {
  it('resolves relative path against cwd', async () => {
    const file = path.join(tmpDir, 'rel.txt');
    fs.writeFileSync(file, 'relative content\n');

    const result = await fileReadTool.execute({ file_path: 'rel.txt' }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('relative content'));
  });
});

// ---------------------------------------------------------------------------
// Offset and limit
// ---------------------------------------------------------------------------

describe('fileReadTool – offset and limit', () => {
  it('respects offset (0-based line number)', async () => {
    const file = path.join(tmpDir, 'lines.txt');
    fs.writeFileSync(file, 'line1\nline2\nline3\nline4\nline5\n');

    const result = await fileReadTool.execute(
      { file_path: file, offset: 2, limit: 2 },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('line3'));
    assert.ok(result.output.includes('line4'));
    assert.ok(result.output.includes('Showing lines'));
    // Should NOT include line1 or line2
    const lines = result.output.split('\n');
    const contentLines = lines.filter(l => /\t/.test(l));
    assert.equal(contentLines.length, 2);
  });

  it('defaults to offset=0 and limit=2000', async () => {
    const file = path.join(tmpDir, 'small.txt');
    fs.writeFileSync(file, 'one\ntwo\nthree\n');

    const result = await fileReadTool.execute({ file_path: file }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('one'));
    assert.ok(result.output.includes('two'));
    assert.ok(result.output.includes('three'));
  });

  it('handles offset beyond file length', async () => {
    const file = path.join(tmpDir, 'short.txt');
    fs.writeFileSync(file, 'only line\n');

    const result = await fileReadTool.execute(
      { file_path: file, offset: 100 },
      ctx(),
    );
    assert.ok(!result.isError);
    // No content lines should be shown
  });
});

// ---------------------------------------------------------------------------
// Binary detection
// ---------------------------------------------------------------------------

describe('fileReadTool – binary detection', () => {
  it('returns [Binary file] for files with null bytes', async () => {
    const file = path.join(tmpDir, 'binary.bin');
    const buf = Buffer.alloc(100);
    buf[50] = 0; // null byte
    buf.write('text', 0);
    fs.writeFileSync(file, buf);

    const result = await fileReadTool.execute({ file_path: file }, ctx());
    assert.ok(result.output.includes('Binary file'));
  });
});

// ---------------------------------------------------------------------------
// Image files
// ---------------------------------------------------------------------------

describe('fileReadTool – image files', () => {
  for (const ext of ['.png', '.jpg', '.jpeg', '.gif', '.webp']) {
    it(`returns base64 for ${ext} files`, async () => {
      const file = path.join(tmpDir, `image${ext}`);
      fs.writeFileSync(file, Buffer.from('fake image data'));

      const result = await fileReadTool.execute({ file_path: file }, ctx());
      assert.ok(!result.isError);
      const parsed = JSON.parse(result.output);
      assert.equal(parsed.type, 'image');
      assert.equal(parsed.source.type, 'base64');
      assert.ok(parsed.source.data.length > 0);
    });
  }
});

// ---------------------------------------------------------------------------
// File cache
// ---------------------------------------------------------------------------

describe('fileReadTool – file cache', () => {
  it('returns short summary on second read of unchanged file', async () => {
    const file = path.join(tmpDir, 'cached.txt');
    fs.writeFileSync(file, 'content\n');

    // First read — full content
    const r1 = await fileReadTool.execute({ file_path: file }, ctx());
    assert.ok(r1.output.includes('content'));

    // Second read — should show cached summary
    const r2 = await fileReadTool.execute({ file_path: file }, ctx());
    assert.ok(r2.output.includes('unchanged'));
  });
});

// ---------------------------------------------------------------------------
// Line number formatting
// ---------------------------------------------------------------------------

describe('fileReadTool – line number formatting', () => {
  it('uses 1-based line numbers', async () => {
    const file = path.join(tmpDir, 'nums.txt');
    fs.writeFileSync(file, 'first\nsecond\nthird\n');

    const result = await fileReadTool.execute({ file_path: file }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('1\tfirst'));
    assert.ok(result.output.includes('2\tsecond'));
    assert.ok(result.output.includes('3\tthird'));
  });
});

// ---------------------------------------------------------------------------
// Read-before-write tracking (filesRead)
// ---------------------------------------------------------------------------

describe('fileReadTool – filesRead tracking', () => {
  it('adds resolved path to filesRead set after successful read', async () => {
    const file = path.join(tmpDir, 'tracked.txt');
    fs.writeFileSync(file, 'content\n');
    const filesRead = new Set<string>();

    await fileReadTool.execute({ file_path: file }, ctx({ filesRead }));
    assert.ok(filesRead.has(file));
  });

  it('adds path for image files', async () => {
    const file = path.join(tmpDir, 'img.png');
    fs.writeFileSync(file, Buffer.from('fake'));
    const filesRead = new Set<string>();

    await fileReadTool.execute({ file_path: file }, ctx({ filesRead }));
    assert.ok(filesRead.has(file));
  });

  it('adds path even for binary files', async () => {
    const file = path.join(tmpDir, 'bin.dat');
    const buf = Buffer.alloc(100);
    buf[50] = 0;
    fs.writeFileSync(file, buf);
    const filesRead = new Set<string>();

    await fileReadTool.execute({ file_path: file }, ctx({ filesRead }));
    assert.ok(filesRead.has(file));
  });

  it('does not crash when filesRead is undefined', async () => {
    const file = path.join(tmpDir, 'notrack.txt');
    fs.writeFileSync(file, 'data\n');

    const result = await fileReadTool.execute({ file_path: file }, ctx());
    assert.ok(!result.isError);
  });

  it('does not add path for non-existent files', async () => {
    const filesRead = new Set<string>();
    await fileReadTool.execute(
      { file_path: path.join(tmpDir, 'ghost.txt') },
      ctx({ filesRead }),
    );
    assert.equal(filesRead.size, 0);
  });

  it('resolves relative paths before adding to filesRead', async () => {
    const file = path.join(tmpDir, 'rel-track.txt');
    fs.writeFileSync(file, 'data\n');
    const filesRead = new Set<string>();

    await fileReadTool.execute({ file_path: 'rel-track.txt' }, ctx({ filesRead }));
    assert.ok(filesRead.has(file));
  });
});
