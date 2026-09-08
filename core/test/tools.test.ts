import { describe, it, before, after, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { execSync } from 'node:child_process';

import { fileReadTool } from '../src/tools/file-read';
import { fileEditTool } from '../src/tools/file-edit';
import { bashTool } from '../src/tools/bash';
import { grepTool } from '../src/tools/grep';
import { globTool } from '../src/tools/glob';
import { listDirTool } from '../src/tools/list-dir';
import { scoutTool } from '../src/tools/scout';
import { createDefaultTools, ToolRegistry } from '../src/tools/index';
import { getFileCache } from '../src/file-cache';
import type { ToolContext } from '../src/types';

function makeContext(cwd: string): ToolContext {
  return {
    cwd,
    abortSignal: new AbortController().signal,
  };
}

function makeTempDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'si-test-'));
}

async function withStatFailureFor(filePath: string, fn: () => Promise<void>): Promise<void> {
  const orig = fs.statSync;
  const target = path.resolve(filePath);
  (fs as typeof fs).statSync = ((p: fs.PathLike, opts?: unknown) => {
    if (path.resolve(String(p)) === target) {
      throw Object.assign(new Error('EIO: stat failed'), { code: 'EIO' });
    }
    return (orig as Function).call(fs, p, opts);
  }) as typeof fs.statSync;
  try {
    await fn();
  } finally {
    (fs as typeof fs).statSync = orig;
  }
}

// ---------------------------------------------------------------------------
// file_read
// ---------------------------------------------------------------------------
describe('file_read tool', () => {
  let tmpDir: string;

  before(() => {
    tmpDir = makeTempDir();
  });

  after(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('reads a file and returns numbered lines', async () => {
    const fp = path.join(tmpDir, 'hello.txt');
    fs.writeFileSync(fp, 'line one\nline two\nline three\n');

    const result = await fileReadTool.execute({ file_path: fp }, makeContext(tmpDir));
    assert.equal(result.isError, undefined);
    assert.ok(result.output.includes('hello.txt'));
    // Line numbers should be 1-based
    assert.ok(result.output.includes('1\tline one'));
    assert.ok(result.output.includes('2\tline two'));
    assert.ok(result.output.includes('3\tline three'));
  });

  it('supports offset and limit', async () => {
    const fp = path.join(tmpDir, 'lines.txt');
    const lines = Array.from({ length: 20 }, (_, i) => `line ${i + 1}`).join('\n');
    fs.writeFileSync(fp, lines);

    const result = await fileReadTool.execute(
      { file_path: fp, offset: 5, limit: 3 },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, undefined);
    assert.ok(result.output.includes('line 6'));
    assert.ok(result.output.includes('line 7'));
    assert.ok(result.output.includes('line 8'));
    // Should not include line 9
    assert.ok(!result.output.includes('9\tline 9'));
    // Should have the range content (lines 6-8)
    assert.ok(result.output.includes('line 6') && result.output.includes('line 8'));
  });

  it('detects binary files', async () => {
    const fp = path.join(tmpDir, 'binary.bin');
    const buf = Buffer.alloc(100);
    buf[50] = 0; // null byte
    buf.write('hello', 0);
    fs.writeFileSync(fp, buf);

    const result = await fileReadTool.execute({ file_path: fp }, makeContext(tmpDir));
    assert.equal(result.isError, undefined);
    assert.ok(result.output.includes('Binary file'));
  });

  it('returns error for missing file', async () => {
    const result = await fileReadTool.execute(
      { file_path: path.join(tmpDir, 'nonexistent.txt') },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Error'));
    assert.ok(result.output.includes('not found') || result.output.includes('not readable'));
  });

  it('returns error for empty file_path', async () => {
    const result = await fileReadTool.execute({ file_path: '' }, makeContext(tmpDir));
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('file_path must not be empty'));
  });

  it('returns "(empty file)" for empty files', async () => {
    const fp = path.join(tmpDir, 'empty.txt');
    fs.writeFileSync(fp, '');

    const result = await fileReadTool.execute({ file_path: fp }, makeContext(tmpDir));
    assert.equal(result.isError, undefined);
    assert.ok(result.output.includes('(empty file)'));
  });

  it('returns error when reading a directory', async () => {
    const dir = path.join(tmpDir, 'subdir');
    fs.mkdirSync(dir);

    const result = await fileReadTool.execute({ file_path: dir }, makeContext(tmpDir));
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('directory'));
  });

  it('resolves relative paths against cwd', async () => {
    const fp = path.join(tmpDir, 'relative.txt');
    fs.writeFileSync(fp, 'relative content');

    const result = await fileReadTool.execute(
      { file_path: 'relative.txt' },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, undefined);
    assert.ok(result.output.includes('relative content'));
  });
});

// ---------------------------------------------------------------------------
// file_edit — write / create (replaces file_write)
// ---------------------------------------------------------------------------
describe('file_edit tool — write and create', () => {
  let tmpDir: string;

  before(() => {
    tmpDir = makeTempDir();
  });

  after(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('writes content to a new file', async () => {
    const fp = path.join(tmpDir, 'output.txt');
    const result = await fileEditTool.execute(
      { file_path: fp, content: 'hello world' },
      makeContext(tmpDir),
    );
    assert.notEqual(result.isError, true);
    assert.ok(result.output.includes('Successfully wrote') || result.output.includes('Created new file'));
    const content = fs.readFileSync(fp, 'utf-8');
    assert.equal(content, 'hello world');
  });

  it('creates parent directories automatically', async () => {
    const fp = path.join(tmpDir, 'a', 'b', 'c', 'deep.txt');
    const result = await fileEditTool.execute(
      { file_path: fp, new_string: 'deep content' },
      makeContext(tmpDir),
    );
    assert.notEqual(result.isError, true);
    const content = fs.readFileSync(fp, 'utf-8');
    assert.equal(content, 'deep content');
  });

  it('overwrites existing file', async () => {
    const fp = path.join(tmpDir, 'overwrite.txt');
    fs.writeFileSync(fp, 'original');
    const result = await fileEditTool.execute(
      { file_path: fp, old_string: '', new_string: 'replaced' },
      makeContext(tmpDir),
    );
    assert.notEqual(result.isError, true);
    assert.equal(fs.readFileSync(fp, 'utf-8'), 'replaced');
  });

  it('returns error for empty file_path', async () => {
    const result = await fileEditTool.execute(
      { file_path: '', content: 'data' },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('file_path must not be empty'));
  });

  it('returns error when content is missing', async () => {
    const result = await fileEditTool.execute(
      { file_path: path.join(tmpDir, 'x.txt') },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('must be provided'));
  });

  it('caches the written file so a subsequent get() hits', async () => {
    const fp = path.join(tmpDir, 'cached-write.txt');
    const result = await fileEditTool.execute(
      { file_path: fp, content: 'cached' },
      makeContext(tmpDir),
    );
    assert.notEqual(result.isError, true);
    const cached = getFileCache(tmpDir).get(fp);
    assert.ok(cached);
    assert.equal(cached!.content, 'cached');
  });

    it('populates an empty existing file', async () => {
      const fp = path.join(tmpDir, 'empty.txt');
      fs.writeFileSync(fp, '');
      const result = await fileEditTool.execute(
        { file_path: fp, content: 'now filled' },
        makeContext(tmpDir),
      );
      assert.notEqual(result.isError, true);
      assert.ok(result.output.includes('Populated empty file') || result.output.includes('Successfully wrote'));
      assert.equal(fs.readFileSync(fp, 'utf-8'), 'now filled');
    });

    it('requires file_read before overwriting a non-empty file when filesRead is tracked', async () => {
      const fp = path.join(tmpDir, 'need-read.txt');
      fs.writeFileSync(fp, 'keep me');
      const ctx = { ...makeContext(tmpDir), filesRead: new Set<string>() };
      const result = await fileEditTool.execute(
        { file_path: fp, content: 'overwrite' },
        ctx,
      );
      assert.equal(result.isError, true);
      assert.ok(result.output.includes('must read'));
      assert.equal(fs.readFileSync(fp, 'utf-8'), 'keep me');
    });

    it('rejects Jupyter notebooks', async () => {
      const result = await fileEditTool.execute(
        { file_path: path.join(tmpDir, 'nb.ipynb'), content: '{}' },
        makeContext(tmpDir),
      );
      assert.equal(result.isError, true);
      assert.ok(result.output.includes('notebook_edit'));
    });

  it('does not report isError when post-write stat fails', async () => {
    const fp = path.join(tmpDir, 'stat-fail-write.txt');
    await withStatFailureFor(fp, async () => {
      const result = await fileEditTool.execute(
        { file_path: fp, content: 'still wrote' },
        makeContext(tmpDir),
      );
      assert.notEqual(result.isError, true);
      assert.ok(result.output.includes('Successfully wrote') || result.output.includes('Created new file'));
    });
    assert.equal(fs.readFileSync(fp, 'utf-8'), 'still wrote');
  });

  it('rejects content that looks like a secret', async () => {
    const result = await fileEditTool.execute(
      { file_path: path.join(tmpDir, 'secret.txt'), content: 'key: sk-abcdefghijklmnopqrstuvwxyz1234567890abcdef' },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('secrets'));
  });

  it('rejects overwrite when the file changed after the last read', async () => {
    const fp = path.join(tmpDir, 'stale-write.txt');
    fs.writeFileSync(fp, 'original\n');
    const stat = fs.statSync(fp);
    getFileCache(tmpDir).set(fp, 'original\n', stat.mtimeMs);
    fs.utimesSync(fp, Date.now() / 1000, (stat.mtimeMs + 2000) / 1000);
    const result = await fileEditTool.execute(
      { file_path: fp, content: 'new\n' },
      { ...makeContext(tmpDir), filesRead: new Set([fp]) },
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('modified since'));
  });
});

// ---------------------------------------------------------------------------
// file_edit
// ---------------------------------------------------------------------------
describe('file_edit tool', () => {
  let tmpDir: string;

  before(() => {
    tmpDir = makeTempDir();
  });

  after(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('performs a search and replace edit', async () => {
    const fp = path.join(tmpDir, 'edit.txt');
    fs.writeFileSync(fp, 'Hello World\nFoo Bar\nBaz\n');

    const result = await fileEditTool.execute(
      { file_path: fp, old_string: 'Foo Bar', new_string: 'Foo Baz' },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, undefined);
    assert.ok(result.output.includes('Successfully edited'));
    const content = fs.readFileSync(fp, 'utf-8');
    assert.ok(content.includes('Foo Baz'));
    assert.ok(!content.includes('Foo Bar'));
  });

  it('returns error when old_string is not found', async () => {
    const fp = path.join(tmpDir, 'nofind.txt');
    fs.writeFileSync(fp, 'some content');

    const result = await fileEditTool.execute(
      { file_path: fp, old_string: 'MISSING', new_string: 'x' },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('not found'));
  });

  it('returns error when old_string is not unique (multiple matches)', async () => {
    const fp = path.join(tmpDir, 'multi.txt');
    fs.writeFileSync(fp, 'abc\nabc\nabc\n');

    const result = await fileEditTool.execute(
      { file_path: fp, old_string: 'abc', new_string: 'xyz' },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('found 3 times'));
    assert.ok(result.output.includes('must be unique'));
  });

  it('returns error when old_string equals new_string', async () => {
    const fp = path.join(tmpDir, 'same.txt');
    fs.writeFileSync(fp, 'content');

    const result = await fileEditTool.execute(
      { file_path: fp, old_string: 'content', new_string: 'content' },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('identical'));
  });

  it('returns error when new_string is missing in replace mode', async () => {
    const fp = path.join(tmpDir, 'no-new.txt');
    fs.writeFileSync(fp, 'keep\n');
    const result = await fileEditTool.execute(
      { file_path: fp, old_string: 'keep' },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('new_string must be provided'));
  });

  it('returns error for missing file', async () => {
    const result = await fileEditTool.execute(
      { file_path: path.join(tmpDir, 'ghost.txt'), old_string: 'a', new_string: 'b' },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Error'));
  });

  it('returns a snippet showing context around the change', async () => {
    const fp = path.join(tmpDir, 'snippet.txt');
    const lines = Array.from({ length: 20 }, (_, i) => `line ${i + 1}`).join('\n');
    fs.writeFileSync(fp, lines);

    const result = await fileEditTool.execute(
      { file_path: fp, old_string: 'line 10', new_string: 'LINE TEN' },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, undefined);
    // The snippet should show surrounding context with line numbers
    assert.ok(result.output.includes('LINE TEN'));
  });

  it('caches content after a successful unique replace', async () => {
    const fp = path.join(tmpDir, 'edit-cached.txt');
    fs.writeFileSync(fp, 'Hello World\n');
    const result = await fileEditTool.execute(
      { file_path: fp, old_string: 'World', new_string: 'there' },
      makeContext(tmpDir),
    );
    assert.notEqual(result.isError, true);
    const cached = getFileCache(tmpDir).get(fp);
    assert.ok(cached);
    assert.ok(cached!.content.includes('there'));
  });

  it('does not report isError when post-write stat fails', async () => {
    const fp = path.join(tmpDir, 'edit-stat-fail.txt');
    fs.writeFileSync(fp, 'Hello World\n');
    await withStatFailureFor(fp, async () => {
      const result = await fileEditTool.execute(
        { file_path: fp, old_string: 'World', new_string: 'there' },
        makeContext(tmpDir),
      );
      assert.notEqual(result.isError, true);
      assert.ok(result.output.includes('Successfully edited'));
    });
    assert.ok(fs.readFileSync(fp, 'utf-8').includes('there'));
  });

  it('caches a newly created file (empty old_string)', async () => {
    const fp = path.join(tmpDir, 'created-via-edit.txt');
    const result = await fileEditTool.execute(
      { file_path: fp, old_string: '', new_string: 'brand new' },
      makeContext(tmpDir),
    );
    assert.notEqual(result.isError, true);
    assert.ok(result.output.includes('Created new file'));
    const cached = getFileCache(tmpDir).get(fp);
    assert.ok(cached);
    assert.equal(cached!.content, 'brand new');
  });
});

// ---------------------------------------------------------------------------
// file_edit — sequential edits (replaces multi_edit)
// ---------------------------------------------------------------------------
describe('file_edit tool — sequential edits', () => {
  let tmpDir: string;

  before(() => {
    tmpDir = makeTempDir();
  });

  after(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('applies sequential edits and succeeds even if post-write stat fails', async () => {
    const fp = path.join(tmpDir, 'multi.txt');
    fs.writeFileSync(fp, 'one two three\n');
    await withStatFailureFor(fp, async () => {
      const result = await fileEditTool.execute(
        { file_path: fp, edits: [{ old_string: 'one', new_string: 'ONE' }, { old_string: 'two', new_string: 'TWO' }] },
        makeContext(tmpDir),
      );
      assert.notEqual(result.isError, true);
      assert.ok(result.output.includes('Successfully edited'));
    });
    assert.equal(fs.readFileSync(fp, 'utf-8'), 'ONE TWO three\n');
  });

  it('caches after a successful sequential edit', async () => {
    const fp = path.join(tmpDir, 'multi-cache.txt');
    fs.writeFileSync(fp, 'foo bar\n');
    const result = await fileEditTool.execute(
      { file_path: fp, edits: [{ old_string: 'foo', new_string: 'baz' }] },
      makeContext(tmpDir),
    );
    assert.notEqual(result.isError, true);
    const cached = getFileCache(tmpDir).get(fp);
    assert.ok(cached);
    assert.ok(cached!.content.includes('baz'));
  });

  it('applies replace_all within sequential edits', async () => {
    const fp = path.join(tmpDir, 'replace-all.txt');
    fs.writeFileSync(fp, 'aa aa aa\n');
    const result = await fileEditTool.execute(
      { file_path: fp, edits: [{ old_string: 'aa', new_string: 'bb', replace_all: true }] },
      makeContext(tmpDir),
    );
    assert.notEqual(result.isError, true);
    assert.equal(fs.readFileSync(fp, 'utf-8'), 'bb bb bb\n');
  });

  it('returns error for a malformed edits entry', async () => {
    const fp = path.join(tmpDir, 'bad-edits.txt');
    fs.writeFileSync(fp, 'x\n');
    const result = await fileEditTool.execute(
      { file_path: fp, edits: ['not-an-object'] },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('each edit must be an object'));
  });

  it('returns error for an empty edits array', async () => {
    const fp = path.join(tmpDir, 'empty-edits.txt');
    fs.writeFileSync(fp, 'x\n');
    const result = await fileEditTool.execute(
      { file_path: fp, edits: [] },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('non-empty'));
  });

  it('rejects sequential edits that would write a secret', async () => {
    const fp = path.join(tmpDir, 'seq-secret.txt');
    fs.writeFileSync(fp, 'token = PLACEHOLDER\n');
    const result = await fileEditTool.execute(
      {
        file_path: fp,
        edits: [{ old_string: 'PLACEHOLDER', new_string: 'sk-abcdefghijklmnopqrstuvwxyz1234567890abcdef' }],
      },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('secrets'));
  });
});

// ---------------------------------------------------------------------------
// bash
// ---------------------------------------------------------------------------
describe('bash tool', () => {
  let tmpDir: string;

  before(() => {
    tmpDir = makeTempDir();
  });

  after(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('runs a simple command and captures output', async () => {
    const result = await bashTool.execute(
      { command: 'echo hello' },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('hello'));
  });

  it('captures exit code for failing commands', async () => {
    const result = await bashTool.execute(
      { command: 'exit 42' },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('[exit code: 42'));
  });

  it('captures stderr', async () => {
    const result = await bashTool.execute(
      { command: 'echo error_msg >&2; exit 1' },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('error_msg'));
  });

  it('respects timeout', async () => {
    const result = await bashTool.execute(
      { command: 'sleep 30', timeout: 200 },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('timed out'));
  });

  it('returns error for empty command', async () => {
    const result = await bashTool.execute({ command: '' }, makeContext(tmpDir));
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('command must not be empty'));
  });

  it('uses provided cwd', async () => {
    const result = await bashTool.execute(
      { command: 'pwd' },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, false);
    assert.ok(result.output.includes(tmpDir));
  });

  it('handles abort signal', async () => {
    const ac = new AbortController();
    const ctx: ToolContext = { cwd: tmpDir, abortSignal: ac.signal };

    // Abort after a short delay
    setTimeout(() => ac.abort(), 100);
    const result = await bashTool.execute({ command: 'sleep 30' }, ctx);
    assert.equal(result.isError, true);
  });

  it('handles already-aborted signal', async () => {
    const ac = new AbortController();
    ac.abort();
    const ctx: ToolContext = { cwd: tmpDir, abortSignal: ac.signal };

    const result = await bashTool.execute({ command: 'echo hi' }, ctx);
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('aborted') || result.output.includes('Aborted'));
  });
});

// ---------------------------------------------------------------------------
// grep
// ---------------------------------------------------------------------------
describe('grep tool', () => {
  let tmpDir: string;

  before(() => {
    tmpDir = makeTempDir();
    fs.writeFileSync(path.join(tmpDir, 'a.txt'), 'hello world\nfoo bar\nhello again\n');
    fs.writeFileSync(path.join(tmpDir, 'b.txt'), 'nothing here\nworld peace\n');
    fs.mkdirSync(path.join(tmpDir, 'sub'));
    fs.writeFileSync(path.join(tmpDir, 'sub', 'c.ts'), 'const hello = 1;\nconst world = 2;\n');
  });

  after(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('finds pattern across files', async () => {
    const result = await grepTool.execute(
      { pattern: 'hello', path: tmpDir },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, undefined);
    // Should find in a.txt and sub/c.ts
    assert.ok(result.output.includes('hello'));
  });

  it('returns no matches message when pattern not found', async () => {
    const result = await grepTool.execute(
      { pattern: 'ZZZZNOTFOUND', path: tmpDir },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, undefined);
    assert.ok(result.output.includes('No matches found'));
  });

  it('supports include filter', async () => {
    const result = await grepTool.execute(
      { pattern: 'hello', path: tmpDir, include: '*.ts' },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, undefined);
    // Should only find in the .ts file
    assert.ok(result.output.includes('hello'));
    assert.ok(!result.output.includes('a.txt'));
  });

  it('returns error for empty pattern', async () => {
    const result = await grepTool.execute({ pattern: '' }, makeContext(tmpDir));
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('pattern must not be empty'));
  });
});

// ---------------------------------------------------------------------------
// glob
// ---------------------------------------------------------------------------
describe('glob tool', () => {
  let tmpDir: string;

  before(() => {
    tmpDir = makeTempDir();
    fs.writeFileSync(path.join(tmpDir, 'file1.ts'), '');
    fs.writeFileSync(path.join(tmpDir, 'file2.ts'), '');
    fs.writeFileSync(path.join(tmpDir, 'file3.js'), '');
    fs.mkdirSync(path.join(tmpDir, 'src'));
    fs.writeFileSync(path.join(tmpDir, 'src', 'main.ts'), '');
    fs.writeFileSync(path.join(tmpDir, 'src', 'util.js'), '');
  });

  after(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('finds files matching a glob pattern', async () => {
    const result = await globTool.execute(
      { pattern: '**/*.ts', path: tmpDir },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, undefined);
    assert.ok(result.output.includes('file1.ts'));
    assert.ok(result.output.includes('file2.ts'));
    assert.ok(result.output.includes('main.ts'));
    // Should not include .js files
    assert.ok(!result.output.includes('file3.js'));
    assert.ok(!result.output.includes('util.js'));
  });

  it('returns no files message when nothing matches', async () => {
    const result = await globTool.execute(
      { pattern: '**/*.py', path: tmpDir },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, undefined);
    assert.ok(result.output.includes('No files found'));
  });

  it('results are sorted alphabetically', async () => {
    const result = await globTool.execute(
      { pattern: '*.ts', path: tmpDir },
      makeContext(tmpDir),
    );
    const lines = result.output.trim().split('\n');
    assert.ok(lines.length >= 2);
    // Verify sorted order
    for (let i = 1; i < lines.length; i++) {
      assert.ok(lines[i - 1].localeCompare(lines[i]) <= 0);
    }
  });

  it('returns error for empty pattern', async () => {
    const result = await globTool.execute({ pattern: '' }, makeContext(tmpDir));
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('pattern must not be empty'));
  });
});

// ---------------------------------------------------------------------------
// list_dir
// ---------------------------------------------------------------------------
describe('list_dir tool', () => {
  let tmpDir: string;

  before(() => {
    tmpDir = makeTempDir();
    fs.writeFileSync(path.join(tmpDir, 'alpha.txt'), '');
    fs.writeFileSync(path.join(tmpDir, 'beta.js'), '');
    fs.mkdirSync(path.join(tmpDir, 'subdir1'));
    fs.mkdirSync(path.join(tmpDir, 'subdir2'));
    fs.writeFileSync(path.join(tmpDir, 'subdir1', 'inner.txt'), '');
  });

  after(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('lists directory contents with type indicators', async () => {
    const result = await listDirTool.execute(
      { path: tmpDir },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, undefined);
    assert.ok(result.output.includes('[dir]'));
    assert.ok(result.output.includes('[file]'));
    assert.ok(result.output.includes('subdir1/'));
    assert.ok(result.output.includes('subdir2/'));
    assert.ok(result.output.includes('alpha.txt'));
    assert.ok(result.output.includes('beta.js'));
  });

  it('directories appear before files', async () => {
    const result = await listDirTool.execute(
      { path: tmpDir },
      makeContext(tmpDir),
    );
    const lines = result.output.split('\n').filter(l => l.startsWith('['));
    const firstFileIndex = lines.findIndex(l => l.startsWith('[file]'));
    const lastDirIndex = lines.length - 1 - [...lines].reverse().findIndex(l => l.startsWith('[dir]'));
    if (firstFileIndex >= 0 && lastDirIndex >= 0) {
      assert.ok(lastDirIndex < firstFileIndex, 'directories should appear before files');
    }
  });

  it('shows entry count in header', async () => {
    const result = await listDirTool.execute(
      { path: tmpDir },
      makeContext(tmpDir),
    );
    assert.ok(result.output.includes('4 entries'));
  });

  it('returns error for non-existent directory', async () => {
    const result = await listDirTool.execute(
      { path: path.join(tmpDir, 'nonexistent') },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Error'));
  });

  it('returns error for file path (not a directory)', async () => {
    const result = await listDirTool.execute(
      { path: path.join(tmpDir, 'alpha.txt') },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('not a directory'));
  });

  it('returns error for empty path', async () => {
    const result = await listDirTool.execute({ path: '' }, makeContext(tmpDir));
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('path must not be empty'));
  });
});

describe('scout tool', () => {
  let tmpDir: string;
  let upstream: string;

  before(() => {
    tmpDir = makeTempDir();
    upstream = path.join(tmpDir, 'upstream');
    fs.mkdirSync(upstream);
    fs.writeFileSync(path.join(upstream, 'README.md'), 'hello scout\n');
    execSync('git init && git add README.md && git -c user.email=t@t -c user.name=t commit -m init', {
      cwd: upstream, stdio: 'ignore',
    });
  });

  after(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns the cache path so agents can file_read cloned files', async () => {
    const result = await scoutTool.execute({ repo: upstream }, makeContext(tmpDir));
    assert.notEqual(result.isError, true, result.output);
    assert.ok(result.output.includes('### Cache path:'), result.output.slice(0, 400));
    const match = result.output.match(/### Cache path:\n([^\n]+)/);
    assert.ok(match, 'expected cache path line');
    const cachePath = match![1];
    assert.ok(fs.existsSync(cachePath), `cache dir missing: ${cachePath}`);
    assert.ok(fs.existsSync(path.join(cachePath, 'README.md')));
    assert.ok(result.output.includes('file_read'));
  });
});

// ---------------------------------------------------------------------------
// ToolRegistry
// ---------------------------------------------------------------------------
describe('ToolRegistry', () => {
  it('createDefaultTools returns all 40 tools', () => {
    const registry = createDefaultTools('/tmp');
    const tools = registry.getAll();
    assert.equal(tools.length, 40);
    const names = tools.map(t => t.name).sort();
    assert.deepEqual(names, ['AskUserQuestion', 'agentic_fetch', 'bash', 'brief', 'config', 'cron_create', 'cron_delete', 'cron_list', 'diagnostics', 'enter_worktree', 'exit_worktree', 'file_edit', 'file_read', 'find_references', 'git_commit', 'glob', 'grep', 'list_dir', 'notebook_edit', 'plan_mode', 'schedule_wakeup', 'scout', 'search_symbols', 'send_message', 'skill', 'sleep', 'structured_output', 'task', 'task_complete', 'task_kill', 'task_list', 'task_output', 'task_tracker', 'team_create', 'team_delete', 'todo_write', 'tool_search', 'web_fetch', 'web_search', 'workflow']);
  });

  it('get returns a specific tool', () => {
    const registry = createDefaultTools('/tmp');
    const tool = registry.get('file_read');
    assert.ok(tool);
    assert.equal(tool!.name, 'file_read');
  });

  it('get returns undefined for unknown tool', () => {
    const registry = createDefaultTools('/tmp');
    const tool = registry.get('unknown_tool');
    assert.equal(tool, undefined);
  });

  it('register adds a custom tool', () => {
    const registry = new ToolRegistry();
    const customTool = {
      name: 'custom',
      description: 'A custom tool',
      inputSchema: { type: 'object' as const, properties: {} },
      isReadOnly: true,
      execute: async () => ({ output: 'ok' }),
    };
    registry.register(customTool);
    assert.equal(registry.get('custom')?.name, 'custom');
    assert.equal(registry.getAll().length, 1);
  });

  it('toOpenAIFormat produces correct structure', () => {
    const registry = createDefaultTools('/tmp');
    const oai = registry.toOpenAIFormat();
    assert.ok(oai.length > 0);
    for (const entry of oai) {
      assert.equal(entry.type, 'function');
      assert.ok(entry.function.name);
      assert.ok(entry.function.description);
      assert.ok(entry.function.parameters);
    }
  });

  it('tools have correct isReadOnly flags', () => {
    const registry = createDefaultTools('/tmp');
    const readOnlyTools = ['file_read', 'grep', 'glob', 'list_dir', 'web_fetch', 'web_search', 'search_symbols'];
    const writableTools = ['bash', 'file_edit', 'notebook_edit', 'git_commit', 'task_tracker'];

    for (const name of readOnlyTools) {
      assert.equal(registry.get(name)!.isReadOnly, true, `${name} should be read-only`);
    }
    for (const name of writableTools) {
      assert.equal(registry.get(name)!.isReadOnly, false, `${name} should be writable`);
    }
  });
});
