import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { listDirTool } from '../src/tools/list-dir';
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
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-list-dir-'));
  fs.writeFileSync(path.join(tmpDir, 'file1.txt'), '');
  fs.writeFileSync(path.join(tmpDir, 'file2.ts'), '');
  fs.mkdirSync(path.join(tmpDir, 'subdir'));
  fs.mkdirSync(path.join(tmpDir, 'another'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Tool definition
// ---------------------------------------------------------------------------

describe('listDirTool – definition', () => {
  it('has the correct name', () => {
    assert.equal(listDirTool.name, 'list_dir');
  });

  it('is read-only and concurrency-safe', () => {
    assert.equal(listDirTool.isReadOnly, true);
    assert.equal(listDirTool.isConcurrencySafe, true);
  });

  it('schema requires path', () => {
    assert.ok(listDirTool.inputSchema.required?.includes('path'));
  });
});

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

describe('listDirTool – validation', () => {
  it('rejects empty path', async () => {
    const result = await listDirTool.execute({ path: '' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('must not be empty'));
  });

  it('rejects whitespace-only path', async () => {
    const result = await listDirTool.execute({ path: '   ' }, ctx());
    assert.equal(result.isError, true);
  });
});

// ---------------------------------------------------------------------------
// Listing
// ---------------------------------------------------------------------------

describe('listDirTool – listing', () => {
  it('lists directories and files', async () => {
    const result = await listDirTool.execute({ path: tmpDir }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('[dir]'));
    assert.ok(result.output.includes('[file]'));
    assert.ok(result.output.includes('subdir/'));
    assert.ok(result.output.includes('another/'));
    assert.ok(result.output.includes('file1.txt'));
    assert.ok(result.output.includes('file2.ts'));
  });

  it('shows directories before files', async () => {
    const result = await listDirTool.execute({ path: tmpDir }, ctx());
    const lines = result.output.split('\n').filter(l => l.includes('['));
    const firstFileIdx = lines.findIndex(l => l.includes('[file]'));
    const lastDirIdx = lines.length - 1 - [...lines].reverse().findIndex(l => l.includes('[dir]'));
    assert.ok(lastDirIdx < firstFileIdx, 'Directories should appear before files');
  });

  it('sorts entries alphabetically within groups', async () => {
    const result = await listDirTool.execute({ path: tmpDir }, ctx());
    const lines = result.output.split('\n').filter(l => l.includes('[dir]'));
    // another/ should come before subdir/
    const anotherIdx = lines.findIndex(l => l.includes('another'));
    const subdirIdx = lines.findIndex(l => l.includes('subdir'));
    assert.ok(anotherIdx < subdirIdx, 'another/ should be before subdir/');
  });

  it('shows entry count', async () => {
    const result = await listDirTool.execute({ path: tmpDir }, ctx());
    assert.ok(result.output.includes('4 entries'));
  });

  it('handles empty directories', async () => {
    const emptyDir = path.join(tmpDir, 'empty');
    fs.mkdirSync(emptyDir);
    const result = await listDirTool.execute({ path: emptyDir }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('0 entries'));
  });
});

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

describe('listDirTool – error handling', () => {
  it('returns error for non-existent directory', async () => {
    const result = await listDirTool.execute({ path: '/nonexistent/dir' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('not found'));
  });

  it('returns error when path is a file', async () => {
    const file = path.join(tmpDir, 'file1.txt');
    const result = await listDirTool.execute({ path: file }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('not a directory'));
  });
});

// ---------------------------------------------------------------------------
// Relative paths
// ---------------------------------------------------------------------------

describe('listDirTool – relative paths', () => {
  it('resolves relative paths against cwd', async () => {
    const result = await listDirTool.execute({ path: 'subdir' }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('0 entries'));
  });
});
