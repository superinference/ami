import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileReadTool } from '../src/tools/file-read';
import { getFileCache } from '../src/file-cache';
import { resolveFilePath, resolveSearchPath, formatDirectoryList, MAX_DIRS_TO_LIST } from '../src/tools/tool-utils';
import { memoryAgeDays } from '../src/memory';
import { formatFileSize } from '../src/utils/format';
import type { ToolContext } from '../src/types';

let workspace: string;
let outside: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: workspace,
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

beforeEach(() => {
  workspace = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u026-ws-'));
  outside = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u026-out-'));
  getFileCache(workspace).clear();
});

afterEach(() => {
  fs.rmSync(workspace, { recursive: true, force: true });
  fs.rmSync(outside, { recursive: true, force: true });
});

describe('U026 — formatFileSize in file-read size messages', () => {
  it('reports a large file size via the scaling formatter (MB), not raw KB', async () => {
    // 1.5 MiB of newline-delimited text triggers the >256KB partial-view path.
    const line = 'x'.repeat(63) + '\n'; // 64 bytes/line
    const count = Math.ceil((1.5 * 1024 * 1024) / 64);
    const big = path.join(workspace, 'big.txt');
    fs.writeFileSync(big, line.repeat(count));
    const size = fs.statSync(big).size;

    const res = await fileReadTool.execute({ file_path: big }, ctx());
    assert.ok(res.output.includes('PARTIAL VIEW'), res.output.slice(0, 120));
    // Scaled, human-readable size appears verbatim; the old KB-only path would
    // have printed a five-digit "1536KB" instead.
    assert.ok(res.output.includes(formatFileSize(size)), `expected ${formatFileSize(size)} in: ${res.output.slice(0, 160)}`);
    assert.ok(res.output.includes('MB'), res.output.slice(0, 160));
  });
});

describe('U026 — denial message enumerates allowed roots', () => {
  it('appends the session cwd as an accessible root to the out-of-workspace error', () => {
    const target = path.join(outside, 'secret.txt');
    const { error } = resolveFilePath(target, workspace, []);
    assert.ok(error, 'expected an out-of-workspace error');
    assert.ok(error!.output.includes('outside the workspace directory'), error!.output);
    assert.ok(error!.output.includes(`Accessible roots for this session: '${workspace}'`), error!.output);
  });

  it('truncates a long root list with "and N more"', () => {
    const extraRoots = Array.from({ length: 6 }, (_, i) => path.join(outside, `root-${i}`));
    const { error } = resolveFilePath('/nonexistent-u026/secret', workspace, extraRoots);
    assert.ok(error, 'expected an out-of-workspace error');
    // 1 cwd + 6 roots = 7; MAX_DIRS_TO_LIST=5 shown, 2 summarized.
    assert.ok(error!.output.includes('and 2 more'), error!.output);
  });

  it('also enriches the glob/list-dir search-path denial', () => {
    const { error } = resolveSearchPath(outside, workspace, []);
    assert.ok(typeof error === 'string' && error.includes('Accessible roots for this session:'), String(error));
  });
});

describe('U026 — formatDirectoryList helper', () => {
  it('quotes, de-duplicates and truncates', () => {
    assert.equal(formatDirectoryList([]), '');
    assert.equal(formatDirectoryList(['a', 'a', 'b']), "'a', 'b'");
    const many = ['a', 'b', 'c', 'd', 'e', 'f', 'g'];
    assert.equal(formatDirectoryList(many, MAX_DIRS_TO_LIST), "'a', 'b', 'c', 'd', 'e', and 2 more");
  });
});

describe('U026 — clamped memoryAgeDays is the single age source', () => {
  it('clamps a future mtime to 0 instead of a negative age', () => {
    const future = Date.now() + 5 * 86400000;
    assert.equal(memoryAgeDays(future), 0);
  });

  it('computes a positive age for a past mtime', () => {
    const past = Date.now() - 5 * 86400000;
    assert.equal(memoryAgeDays(past), 5);
  });

  it('file-read uses the clamped helper for the memory staleness note', async () => {
    const memDir = path.join(workspace, '.ami', 'memory');
    fs.mkdirSync(memDir, { recursive: true });
    const memFile = path.join(memDir, 'notes.md');
    fs.writeFileSync(memFile, 'old memory content\n');
    const old = (Date.now() - 5 * 86400000) / 1000;
    fs.utimesSync(memFile, old, old);
    getFileCache(workspace).clear();

    const res = await fileReadTool.execute({ file_path: memFile }, ctx());
    assert.ok(res.output.includes('last modified 5 days ago'), res.output.slice(0, 160));
    assert.ok(!res.output.includes('modified -'), 'must never render a negative age');
  });
});
