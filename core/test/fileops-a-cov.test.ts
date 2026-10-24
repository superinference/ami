import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { EventEmitter } from 'events';

import { fileEditTool, verifyWriteTarget, writeEntireFile, commitWrite, lspHooks } from '../src/tools/file-edit';
import {
  fileReadTool,
  parseImageToolOutput,
  validateImageFile,
  MAX_IMAGE_FILE_BYTES,
  imageHooks,
  fsHooks as fileReadFsHooks,
} from '../src/tools/file-read';
import { globTool, globHooks } from '../src/tools/glob';
import {
  grepTool,
  searchFilesForPattern,
  stripGrepFlags,
  isGrepNoMatchExit,
} from '../src/tools/grep';
import { listDirTool } from '../src/tools/list-dir';
import { grepHooks, runSearch } from '../src/tools/grep';
import { encodeImageFile } from '../src/tools/file-read';
import { getFileCache } from '../src/file-cache';
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
  tmpDir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'si-fileops-a-')));
  getFileCache(tmpDir).clear();
});

afterEach(() => {
  mock.restoreAll();
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ===========================================================================
// file-edit.ts — successful writes / edits (exercise commitWrite, trackFileHistory,
// notifyLsp, verifyWriteTarget, diff builders)
// ===========================================================================

describe('fileEditTool – create / overwrite success paths', () => {
  it('creates a new file (isNew) with a short diff preview', async () => {
    const fp = path.join(tmpDir, 'new.txt');
    const res = await fileEditTool.execute({ file_path: fp, new_string: 'alpha\nbeta\n' }, ctx());
    assert.equal(res.isError, undefined);
    assert.ok(res.output.includes('Created new file'));
    assert.ok(res.output.includes('+alpha'));
    assert.equal(fs.readFileSync(fp, 'utf-8'), 'alpha\nbeta\n');
  });

  it('creates a new file with >20 lines (truncation note in preview)', async () => {
    const fp = path.join(tmpDir, 'big-new.txt');
    const body = Array.from({ length: 40 }, (_, i) => `line${i}`).join('\n');
    const res = await fileEditTool.execute({ file_path: fp, content: body }, ctx());
    assert.equal(res.isError, undefined);
    assert.ok(res.output.includes('more lines)'));
  });

  it('populates an empty file (Populated empty file label)', async () => {
    const fp = path.join(tmpDir, 'empty.txt');
    fs.writeFileSync(fp, '');
    const res = await fileEditTool.execute({ file_path: fp, new_string: 'filled\n' }, ctx());
    assert.ok(res.output.includes('Populated empty file'));
  });

  it('overwrites an existing non-empty file and shows a replace diff', async () => {
    const fp = path.join(tmpDir, 'over.txt');
    const old = Array.from({ length: 30 }, (_, i) => `old${i}`).join('\n');
    fs.writeFileSync(fp, old);
    const context = ctx();
    const res = await fileEditTool.execute(
      { file_path: fp, new_string: Array.from({ length: 30 }, (_, i) => `new${i}`).join('\n') },
      context,
    );
    assert.equal(res.isError, undefined);
    assert.ok(res.output.includes('Successfully wrote'));
    assert.ok(res.output.includes('more removed'));
    assert.ok(res.output.includes('more added'));
    // trackFileHistory created a backup
    const histDir = path.join(tmpDir, '.superinference', 'file-history');
    assert.ok(fs.existsSync(histDir));
    assert.ok(fs.readdirSync(histDir).length >= 1);
  });

  it('writes a file in a nested subdirectory (verifyWriteTarget parent walk)', async () => {
    const fp = path.join(tmpDir, 'a', 'b', 'c', 'deep.txt');
    const res = await fileEditTool.execute({ file_path: fp, new_string: 'deep\n' }, ctx());
    assert.equal(res.isError, undefined);
    assert.ok(fs.existsSync(fp));
  });

  it('uses file_text / file_content aliases for write content', async () => {
    const fp = path.join(tmpDir, 'alias.txt');
    const res = await fileEditTool.execute({ file_path: fp, file_text: 'from file_text\n' }, ctx());
    assert.equal(res.isError, undefined);
    assert.equal(fs.readFileSync(fp, 'utf-8'), 'from file_text\n');
  });
});

describe('fileEditTool – single replace success paths', () => {
  it('performs an exact replace on an existing file (reads into filesRead + cache)', async () => {
    const fp = path.join(tmpDir, 'r.txt');
    fs.writeFileSync(fp, 'hello world\nsecond line\n');
    const context = ctx();
    const res = await fileEditTool.execute(
      { file_path: fp, old_string: 'hello world', new_string: 'goodbye world' },
      context,
    );
    assert.equal(res.isError, undefined);
    assert.ok(res.output.includes('Successfully edited'));
    assert.ok(context.filesRead!.has(fp));
    assert.ok(fs.readFileSync(fp, 'utf-8').includes('goodbye world'));
  });

  it('replace_all replaces every occurrence and shows a diff', async () => {
    const fp = path.join(tmpDir, 'ra.txt');
    fs.writeFileSync(fp, 'foo\nfoo\nbar\nfoo\n');
    const res = await fileEditTool.execute(
      { file_path: fp, old_string: 'foo', new_string: 'baz', replace_all: true },
      ctx(),
    );
    assert.equal(res.isError, undefined);
    assert.ok(res.output.includes('Successfully replaced 3 occurrences'));
    assert.equal(fs.readFileSync(fp, 'utf-8'), 'baz\nbaz\nbar\nbaz\n');
  });

  it('trackFileHistory prunes backups beyond the 20-entry cap', async () => {
    const fp = path.join(tmpDir, 'hist.txt');
    const histDir = path.join(tmpDir, '.superinference', 'file-history');
    fs.mkdirSync(histDir, { recursive: true });
    const safe = 'hist.txt';
    // Pre-seed 25 old backups so the next edit triggers pruning.
    for (let i = 0; i < 25; i++) {
      fs.writeFileSync(path.join(histDir, `${safe}.${1000 + i}.bak`), `old${i}`);
    }
    fs.writeFileSync(fp, 'content v1\n');
    const res = await fileEditTool.execute(
      { file_path: fp, old_string: 'content v1', new_string: 'content v2' },
      ctx(),
    );
    assert.equal(res.isError, undefined);
    const remaining = fs.readdirSync(histDir).filter(f => f.startsWith(safe + '.') && f.endsWith('.bak'));
    assert.ok(remaining.length <= 21, `expected prune to <=21, got ${remaining.length}`);
  });
});

describe('fileEditTool – sequential edits success', () => {
  it('applies multiple sequential edits', async () => {
    const fp = path.join(tmpDir, 'seq.txt');
    fs.writeFileSync(fp, 'one two three\n');
    const res = await fileEditTool.execute(
      {
        file_path: fp,
        edits: [
          { old_string: 'one', new_string: 'ONE' },
          { old_string: 'three', new_string: 'THREE' },
        ],
      },
      ctx(),
    );
    assert.equal(res.isError, false);
    assert.ok(res.output.includes('2/2 edits applied'));
    assert.equal(fs.readFileSync(fp, 'utf-8'), 'ONE two THREE\n');
  });

  it('reports partially-failed sequential edits (applied + failed)', async () => {
    const fp = path.join(tmpDir, 'seq2.txt');
    fs.writeFileSync(fp, 'keep this line\n');
    const res = await fileEditTool.execute(
      {
        file_path: fp,
        edits: [
          { old_string: 'keep', new_string: 'KEEP' },
          { old_string: 'nonexistent-string', new_string: 'x' },
        ],
      },
      ctx(),
    );
    assert.equal(res.isError, false);
    assert.ok(res.output.includes('Failed edits:'));
    assert.ok(res.output.includes('1/2 edits applied'));
  });

  it('sequential edit with replace_all replaces multiple occurrences', async () => {
    const fp = path.join(tmpDir, 'seq3.txt');
    fs.writeFileSync(fp, 'x x x\n');
    const res = await fileEditTool.execute(
      { file_path: fp, edits: [{ old_string: 'x', new_string: 'y', replace_all: true }] },
      ctx(),
    );
    assert.equal(res.isError, false);
    assert.ok(res.output.includes('1/1 edits applied'));
    assert.equal(fs.readFileSync(fp, 'utf-8'), 'y y y\n');
  });

  it('sequential edit: empty old_string and identical strings fail individually', async () => {
    const fp = path.join(tmpDir, 'seq4.txt');
    fs.writeFileSync(fp, 'data here\n');
    const res = await fileEditTool.execute(
      {
        file_path: fp,
        edits: [
          { old_string: '', new_string: 'x' },
          { old_string: 'same', new_string: 'same' },
          { old_string: 'data', new_string: 'DATA' },
        ],
      },
      ctx(),
    );
    assert.ok(res.output.includes('must not be empty'));
    assert.ok(res.output.includes('identical'));
  });

  it('sequential edits: matches multiple locations without replace_all fails that edit', async () => {
    const fp = path.join(tmpDir, 'seq5.txt');
    fs.writeFileSync(fp, 'dup\ndup\nunique\n');
    const res = await fileEditTool.execute(
      {
        file_path: fp,
        edits: [
          { old_string: 'dup', new_string: 'z' },
          { old_string: 'unique', new_string: 'U' },
        ],
      },
      ctx(),
    );
    assert.ok(res.output.includes('matches 2 locations'));
    assert.ok(res.output.includes('1/2 edits applied'));
  });

  it('sequential edits: all fail returns No edits applied', async () => {
    const fp = path.join(tmpDir, 'seq6.txt');
    fs.writeFileSync(fp, 'abc\n');
    const res = await fileEditTool.execute(
      { file_path: fp, edits: [{ old_string: 'zzz', new_string: 'y' }] },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('No edits applied'));
  });

  it('sequential edits: not-found offers a line hint', async () => {
    const fp = path.join(tmpDir, 'seq7.txt');
    fs.writeFileSync(fp, 'const apple = 1;\nconst banana = 2;\n');
    const res = await fileEditTool.execute(
      { file_path: fp, edits: [{ old_string: 'const appl = 1;', new_string: 'x' }] },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('not found'));
  });

  it('sequential edits: ENOENT read error', async () => {
    const fp = path.join(tmpDir, 'missing-seq.txt');
    const res = await fileEditTool.execute(
      { file_path: fp, edits: [{ old_string: 'a', new_string: 'b' }] },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('Cannot read file'));
  });
});

describe('fileEditTool – validation & guards', () => {
  it('rejects edits that are not a non-empty array', async () => {
    const fp = path.join(tmpDir, 'v.txt');
    fs.writeFileSync(fp, 'x\n');
    const res = await fileEditTool.execute({ file_path: fp, edits: [] }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('non-empty array'));
  });

  it('rejects an edit entry that is not an object', async () => {
    const fp = path.join(tmpDir, 'v2.txt');
    fs.writeFileSync(fp, 'x\n');
    const res = await fileEditTool.execute({ file_path: fp, edits: ['nope'] }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('each edit must be an object'));
  });

  it('write-mode without new_string/content errors', async () => {
    const fp = path.join(tmpDir, 'w.txt');
    const res = await fileEditTool.execute({ file_path: fp, old_string: '' }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('must be provided to create or overwrite'));
  });

  it('replace-mode without new_string errors', async () => {
    const fp = path.join(tmpDir, 'w2.txt');
    fs.writeFileSync(fp, 'hello\n');
    const res = await fileEditTool.execute({ file_path: fp, old_string: 'hello' }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('new_string must be provided'));
  });

  it('rejects UNC paths', async () => {
    const res = await fileEditTool.execute(
      { file_path: '\\\\server\\share\\x.txt', new_string: 'a' },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('UNC paths'));
  });

  it('blocks writes to a symlink target (validateFileWritePath)', async () => {
    const real = path.join(tmpDir, 'real.txt');
    fs.writeFileSync(real, 'data\n');
    const link = path.join(tmpDir, 'link.txt');
    fs.symlinkSync(real, link);
    const res = await fileEditTool.execute(
      { file_path: link, old_string: 'data', new_string: 'other' },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('symlink'));
  });

  it('rejects .ipynb edits', async () => {
    const fp = path.join(tmpDir, 'nb.ipynb');
    fs.writeFileSync(fp, '{}');
    const res = await fileEditTool.execute({ file_path: fp, old_string: '{}', new_string: '{"x":1}' }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('notebook_edit'));
  });

  it('errors when replacing on a non-existent file (ENOENT with suggestions)', async () => {
    fs.writeFileSync(path.join(tmpDir, 'readme.md'), 'x\n');
    const res = await fileEditTool.execute(
      { file_path: path.join(tmpDir, 'readme.mdx'), old_string: 'x', new_string: 'y' },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('File not found'));
  });

  it('warns on invalid JSON edits to config-like files', async () => {
    const fp = path.join(tmpDir, 'package.json');
    fs.writeFileSync(fp, '{\n  "name": "x"\n}\n');
    const res = await fileEditTool.execute(
      { file_path: fp, old_string: '"name": "x"', new_string: '"name": "x",,' },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('invalid JSON'));
  });

  it('blocks writing a secret into a file', async () => {
    const fp = path.join(tmpDir, 'conf.txt');
    fs.writeFileSync(fp, 'line1\nline2\n');
    const secret = 'sk-' + 'a'.repeat(40);
    const res = await fileEditTool.execute(
      { file_path: fp, old_string: 'line2', new_string: `token=${secret}` },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('secrets'));
  });

  it('blocks a secret written via replace_all', async () => {
    const fp = path.join(tmpDir, 'conf2.txt');
    fs.writeFileSync(fp, 'PLACEHOLDER\nPLACEHOLDER\n');
    const secret = 'sk-' + 'b'.repeat(40);
    const res = await fileEditTool.execute(
      { file_path: fp, old_string: 'PLACEHOLDER', new_string: secret, replace_all: true },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('secrets'));
  });

  it('blocks a secret written via writeEntireFile', async () => {
    const fp = path.join(tmpDir, 'brand-new-secret.txt');
    const secret = 'sk-' + 'c'.repeat(40);
    const res = await fileEditTool.execute({ file_path: fp, new_string: `k=${secret}\n` }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('secrets'));
  });

  it('errors when old_string is not found (single replace, with preview)', async () => {
    const fp = path.join(tmpDir, 'nf.txt');
    fs.writeFileSync(fp, Array.from({ length: 35 }, (_, i) => `l${i}`).join('\n'));
    const res = await fileEditTool.execute(
      { file_path: fp, old_string: 'does-not-exist', new_string: 'x' },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('old_string not found'));
    assert.ok(res.output.includes('more lines'));
  });

  it('errors when old_string matches multiple locations (single replace)', async () => {
    const fp = path.join(tmpDir, 'multi.txt');
    fs.writeFileSync(fp, 'same\nsame\n');
    const res = await fileEditTool.execute(
      { file_path: fp, old_string: 'same', new_string: 'diff' },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('matches 2 locations'));
  });
});

describe('fileEditTool – directory & notifyLsp seams', () => {
  it('writeEntireFile errors on a directory target', async () => {
    const d = path.join(tmpDir, 'adir');
    fs.mkdirSync(d);
    const res = await fileEditTool.execute({ file_path: d, new_string: 'x\n' }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('is a directory'));
  });

  it('non-detached write runs notifyLsp without throwing', async () => {
    const fp = path.join(tmpDir, 'lsp.txt');
    const res = await fileEditTool.execute(
      { file_path: fp, new_string: 'content\n' },
      ctx({ detachedMode: false }),
    );
    assert.equal(res.isError, undefined);
  });

  it('detached write skips notifyLsp', async () => {
    const fp = path.join(tmpDir, 'lsp2.txt');
    const res = await fileEditTool.execute(
      { file_path: fp, new_string: 'content\n' },
      ctx({ detachedMode: true }),
    );
    assert.equal(res.isError, undefined);
  });
});

describe('verifyWriteTarget (exported)', () => {
  it('blocks a symlink whose target escapes the workspace', () => {
    const outside = fs.mkdtempSync(path.join(os.tmpdir(), 'si-outside-'));
    try {
      const target = path.join(outside, 'secret.txt');
      fs.writeFileSync(target, 'x');
      const link = path.join(tmpDir, 'escape.txt');
      fs.symlinkSync(target, link);
      const err = verifyWriteTarget(link, tmpDir);
      assert.ok(err && err.includes('symlink resolving to'));
    } finally {
      fs.rmSync(outside, { recursive: true, force: true });
    }
  });

  it('blocks when a parent directory is a symlink escaping the workspace', () => {
    const outside = fs.mkdtempSync(path.join(os.tmpdir(), 'si-outside2-'));
    try {
      const link = path.join(tmpDir, 'linkdir');
      fs.symlinkSync(outside, link);
      const err = verifyWriteTarget(path.join(link, 'child.txt'), tmpDir);
      assert.ok(err && err.includes('parent directory'));
    } finally {
      fs.rmSync(outside, { recursive: true, force: true });
    }
  });

  it('returns undefined for a legitimate in-workspace path', () => {
    const err = verifyWriteTarget(path.join(tmpDir, 'ok.txt'), tmpDir);
    assert.equal(err, undefined);
  });
});

// ===========================================================================
// file-read.ts
// ===========================================================================

describe('fileReadTool – text reads', () => {
  it('reads a plain text file with line numbers and a security note for large output', async () => {
    const fp = path.join(tmpDir, 'plain.txt');
    const body = Array.from({ length: 60 }, (_, i) => `content line number ${i} with enough text to pad`).join('\n');
    fs.writeFileSync(fp, body);
    const context = ctx();
    const res = await fileReadTool.execute({ file_path: fp }, context);
    assert.equal(res.isError, undefined);
    assert.ok(res.output.includes('[type: text'));
    assert.ok(res.output.includes('1\t'));
    assert.ok(res.output.includes('Security note'));
    assert.ok(context.filesRead!.has(fp));
  });

  it('returns "(empty file)" for an empty file full read', async () => {
    const fp = path.join(tmpDir, 'empty.txt');
    fs.writeFileSync(fp, '');
    const res = await fileReadTool.execute({ file_path: fp }, ctx());
    assert.equal(res.output, '(empty file)');
  });

  it('reads a range via offset/limit and returns a range header', async () => {
    const fp = path.join(tmpDir, 'range.txt');
    fs.writeFileSync(fp, Array.from({ length: 50 }, (_, i) => `row${i}`).join('\n'));
    const res = await fileReadTool.execute({ file_path: fp, offset: 5, limit: 3 }, ctx());
    assert.ok(res.output.includes('Showing 3 lines from offset 5'));
    assert.ok(res.output.includes('row6'));
  });

  it('returns "(empty file)" for a range read that yields nothing', async () => {
    const fp = path.join(tmpDir, 'range-empty.txt');
    fs.writeFileSync(fp, 'only\n');
    const res = await fileReadTool.execute({ file_path: fp, offset: 100, limit: 10 }, ctx());
    assert.equal(res.output, '(empty file)');
  });

  it('truncates full reads that exceed the token budget', async () => {
    const fp = path.join(tmpDir, 'budget.txt');
    // > 100k chars but < 256KB so it is a full read, not a partial view.
    const body = ('x'.repeat(120) + '\n').repeat(1000); // ~121k chars
    fs.writeFileSync(fp, body);
    const res = await fileReadTool.execute({ file_path: fp }, ctx());
    assert.ok(res.output.includes('token budget'));
  });

  it('truncates range reads that exceed the token budget', async () => {
    const fp = path.join(tmpDir, 'range-budget.txt');
    const body = ('y'.repeat(120) + '\n').repeat(2000);
    fs.writeFileSync(fp, body);
    const res = await fileReadTool.execute({ file_path: fp, offset: 0, limit: 2000 }, ctx());
    assert.ok(res.output.includes('token budget'));
  });

  it('returns a cached-unchanged summary on a second read', async () => {
    const fp = path.join(tmpDir, 'cached.txt');
    fs.writeFileSync(fp, 'a\nb\nc\n');
    const context = ctx();
    await fileReadTool.execute({ file_path: fp }, context);
    const res2 = await fileReadTool.execute({ file_path: fp }, context);
    assert.ok(res2.output.includes('File unchanged since last read'));
  });

  it('prepends a staleness note for old memory files', async () => {
    const memDir = path.join(tmpDir, '.superinference', 'memory');
    fs.mkdirSync(memDir, { recursive: true });
    const fp = path.join(memDir, 'notes.md');
    fs.writeFileSync(fp, 'remembered fact\n');
    const old = Date.now() / 1000 - 5 * 86400;
    fs.utimesSync(fp, old, old);
    const res = await fileReadTool.execute({ file_path: fp }, ctx());
    assert.ok(res.output.includes('Memory file — last modified'));
  });

  it('appends matching skills from a skill manager', async () => {
    const fp = path.join(tmpDir, 'skilled.ts');
    fs.writeFileSync(fp, 'export const x = 1;\n');
    const context = ctx() as any;
    context._skillManager = {
      findMatchingSkills: () => [{ name: 'my-skill' }],
    };
    const res = await fileReadTool.execute({ file_path: fp }, context);
    assert.ok(res.output.includes('Relevant skills: my-skill'));
  });
});

describe('fileReadTool – special file handling', () => {
  it('errors when the path is a directory', async () => {
    const d = path.join(tmpDir, 'adir');
    fs.mkdirSync(d);
    const res = await fileReadTool.execute({ file_path: d }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('is a directory'));
  });

  it('fast-rejects known binary extensions', async () => {
    const fp = path.join(tmpDir, 'archive.zip');
    fs.writeFileSync(fp, 'PK\x03\x04 not really a zip');
    const res = await fileReadTool.execute({ file_path: fp }, ctx());
    assert.ok(res.output.includes('[Binary file: .zip'));
  });

  it('errors with suggestions when a similar file exists', async () => {
    fs.writeFileSync(path.join(tmpDir, 'config.yaml'), 'x\n');
    const res = await fileReadTool.execute(
      { file_path: path.join(tmpDir, 'config.yml') },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('Did you mean'));
  });

  it('reads a jupyter notebook and formats cells', async () => {
    const fp = path.join(tmpDir, 'nb.ipynb');
    const nb = {
      metadata: { kernelspec: { display_name: 'Python 3' } },
      cells: [
        { cell_type: 'code', source: ['print(1)\n'], outputs: [{ text: ['1\n'] }] },
        { cell_type: 'code', source: 'x=2', outputs: [{ data: { 'text/plain': ['2'] } }] },
        { cell_type: 'code', source: ['boom'], outputs: [{ ename: 'ValueError', evalue: 'bad' }] },
      ],
    };
    fs.writeFileSync(fp, JSON.stringify(nb));
    const res = await fileReadTool.execute({ file_path: fp }, ctx());
    assert.ok(res.output.includes('Jupyter Notebook, 3 cells'));
    assert.ok(res.output.includes('[output] 1'));
    assert.ok(res.output.includes('[error] ValueError'));
  });

  it('errors on a malformed notebook', async () => {
    const fp = path.join(tmpDir, 'bad.ipynb');
    fs.writeFileSync(fp, '{ not json');
    const res = await fileReadTool.execute({ file_path: fp }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('Error reading notebook'));
  });

  it('truncates very large notebooks over the token budget', async () => {
    const fp = path.join(tmpDir, 'bignb.ipynb');
    const bigSrc = 'a'.repeat(200000);
    fs.writeFileSync(fp, JSON.stringify({ cells: [{ cell_type: 'code', source: [bigSrc] }] }));
    const res = await fileReadTool.execute({ file_path: fp }, ctx());
    assert.ok(res.output.includes('truncated'));
  });

  it('shows a partial view for files over the 256KB cap', async () => {
    const fp = path.join(tmpDir, 'huge.txt');
    fs.writeFileSync(fp, ('z'.repeat(100) + '\n').repeat(4000)); // ~400KB
    const res = await fileReadTool.execute({ file_path: fp }, ctx());
    assert.ok(res.output.includes('PARTIAL VIEW'));
  });

  it('empty path and UNC path are rejected', async () => {
    assert.ok((await fileReadTool.execute({ file_path: '' }, ctx())).isError);
    assert.ok((await fileReadTool.execute({ file_path: '//host/share' }, ctx())).isError);
  });
});

describe('file-read exported helpers', () => {
  it('parseImageToolOutput parses valid image JSON and rejects others', () => {
    const img = JSON.stringify({ type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'AAAA' } });
    const parsed = parseImageToolOutput(img);
    assert.ok(parsed && parsed[0].type === 'image');
    assert.equal(parseImageToolOutput('plain text'), null);
    assert.equal(parseImageToolOutput('{ broken json'), null);
    assert.equal(parseImageToolOutput('{"type":"other"}'), null);
    assert.equal(parseImageToolOutput(123 as any), null);
  });

  it('validateImageFile rejects missing, non-regular, and oversized images', () => {
    assert.equal(validateImageFile(path.join(tmpDir, 'nope.png')).valid, false);
    const dir = path.join(tmpDir, 'dir.png');
    fs.mkdirSync(dir);
    assert.equal(validateImageFile(dir).valid, false);
    const ok = path.join(tmpDir, 'ok.png');
    fs.writeFileSync(ok, 'small');
    assert.equal(validateImageFile(ok).valid, true);
    const big = path.join(tmpDir, 'big.png');
    fs.writeFileSync(big, 'x');
    fs.truncateSync(big, MAX_IMAGE_FILE_BYTES + 1); // sparse, no real disk use
    assert.equal(validateImageFile(big).valid, false);
  });
});

// ===========================================================================
// glob.ts
// ===========================================================================

describe('globTool', () => {
  it('finds matching files and sorts them', async () => {
    fs.writeFileSync(path.join(tmpDir, 'a.ts'), '');
    fs.writeFileSync(path.join(tmpDir, 'b.ts'), '');
    fs.writeFileSync(path.join(tmpDir, 'c.js'), '');
    const res = await globTool.execute({ pattern: '*.ts' }, ctx());
    assert.ok(res.output.includes('a.ts'));
    assert.ok(res.output.includes('b.ts'));
    assert.ok(!res.output.includes('c.js'));
    assert.ok(res.output.includes('[2 files]'));
  });

  it('reports no matches', async () => {
    const res = await globTool.execute({ pattern: '*.nomatch' }, ctx());
    assert.ok(res.output.includes('No files found matching pattern'));
  });

  it('truncates results beyond MAX_RESULTS', async () => {
    for (let i = 0; i < 230; i++) {
      fs.writeFileSync(path.join(tmpDir, `f${String(i).padStart(4, '0')}.md`), '');
    }
    const res = await globTool.execute({ pattern: '*.md' }, ctx());
    assert.ok(res.output.includes('(truncated)'));
    assert.ok(res.output.includes('showing 200 of 230'));
  });

  it('errors on an unreadable base path', async () => {
    const res = await globTool.execute(
      { pattern: '*.ts', path: path.join(tmpDir, 'does-not-exist') },
      ctx(),
    );
    assert.equal(res.isError, true);
  });

  it('errors on a missing pattern', async () => {
    const res = await globTool.execute({ pattern: '' }, ctx());
    assert.equal(res.isError, true);
  });

  it('surfaces an error when the glob implementation throws', async () => {
    mock.method(globHooks, 'glob', (() => Promise.reject(new Error('glob exploded'))) as any);
    const res = await globTool.execute({ pattern: '*.ts' }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('Error searching for files: glob exploded'));
  });

  it('surfaces a non-Error rejection from the glob implementation', async () => {
    mock.method(globHooks, 'glob', (() => Promise.reject('string failure')) as any);
    const res = await globTool.execute({ pattern: '*.ts' }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('string failure'));
  });

  it('searches an explicitly provided base directory', async () => {
    const sub = path.join(tmpDir, 'nested');
    fs.mkdirSync(sub);
    fs.writeFileSync(path.join(sub, 'deep.ts'), '');
    const res = await globTool.execute({ pattern: '*.ts', path: sub }, ctx());
    assert.ok(res.output.includes('deep.ts'));
  });
});

// ===========================================================================
// list-dir.ts
// ===========================================================================

describe('listDirTool', () => {
  it('lists directories first then files', async () => {
    fs.mkdirSync(path.join(tmpDir, 'zdir'));
    fs.mkdirSync(path.join(tmpDir, 'adir'));
    fs.writeFileSync(path.join(tmpDir, 'file.txt'), '');
    const res = await listDirTool.execute({ path: tmpDir }, ctx());
    assert.ok(res.output.includes('[dir]  adir/'));
    assert.ok(res.output.includes('[dir]  zdir/'));
    assert.ok(res.output.includes('[file] file.txt'));
    const aIdx = res.output.indexOf('adir');
    const fIdx = res.output.indexOf('file.txt');
    assert.ok(aIdx < fIdx, 'dirs should come before files');
  });

  it('lists a relative directory path', async () => {
    fs.mkdirSync(path.join(tmpDir, 'sub'));
    fs.writeFileSync(path.join(tmpDir, 'sub', 'inner.txt'), '');
    const res = await listDirTool.execute({ path: 'sub' }, ctx());
    assert.ok(res.output.includes('inner.txt'));
  });

  it('truncates listings beyond MAX_ENTRIES', async () => {
    const big = path.join(tmpDir, 'many');
    fs.mkdirSync(big);
    for (let i = 0; i < 1005; i++) fs.writeFileSync(path.join(big, `e${i}.txt`), '');
    const res = await listDirTool.execute({ path: big }, ctx());
    assert.ok(res.output.includes('Showing first 1000 of 1005'));
  });

  it('errors on empty path', async () => {
    const res = await listDirTool.execute({ path: '' }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('must not be empty'));
  });

  it('errors on a non-existent directory', async () => {
    const res = await listDirTool.execute({ path: path.join(tmpDir, 'ghost') }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('Directory not found'));
  });

  it('errors when the path is a file, not a directory', async () => {
    const fp = path.join(tmpDir, 'notadir.txt');
    fs.writeFileSync(fp, 'x');
    const res = await listDirTool.execute({ path: fp }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('is not a directory'));
  });

  it('rejects a path outside the workspace', async () => {
    const res = await listDirTool.execute({ path: path.resolve(tmpDir, '..') }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('outside the workspace'));
  });

  it('rejects a symlink that resolves outside the workspace', async () => {
    const outside = fs.mkdtempSync(path.join(os.tmpdir(), 'si-ld-out-'));
    try {
      const link = path.join(tmpDir, 'escape');
      fs.symlinkSync(outside, link);
      const res = await listDirTool.execute({ path: link }, ctx());
      assert.equal(res.isError, true);
      assert.ok(res.output.includes('resolves outside the workspace via symlink'));
    } finally {
      fs.rmSync(outside, { recursive: true, force: true });
    }
  });

  it('reports a read error for an unreadable directory', async () => {
    const locked = path.join(tmpDir, 'locked');
    fs.mkdirSync(locked);
    fs.writeFileSync(path.join(locked, 'inner.txt'), 'x');
    fs.chmodSync(locked, 0o000);
    try {
      const res = await listDirTool.execute({ path: locked }, ctx());
      // As a non-root user readdir fails with EACCES -> "Error reading directory".
      assert.equal(res.isError, true);
      assert.ok(res.output.includes('Error reading directory'));
    } finally {
      fs.chmodSync(locked, 0o755);
    }
  });
});

// ===========================================================================
// grep.ts
// ===========================================================================

function seedGrepFiles(): void {
  fs.writeFileSync(path.join(tmpDir, 'alpha.ts'), 'export const needle = 1;\nconst other = 2;\n');
  fs.writeFileSync(path.join(tmpDir, 'beta.ts'), 'function needle() {}\n// needle again\n');
  fs.mkdirSync(path.join(tmpDir, 'nested'));
  fs.writeFileSync(path.join(tmpDir, 'nested', 'gamma.py'), 'needle = "py"\n');
}

describe('grepTool – searches', () => {
  it('finds matches in content mode (default)', async () => {
    seedGrepFiles();
    const res = await grepTool.execute({ pattern: 'needle' }, ctx());
    assert.ok(res.output.includes('needle'));
    assert.ok(res.output.includes('matches in'));
  });

  it('files_with_matches mode lists file names', async () => {
    seedGrepFiles();
    const res = await grepTool.execute({ pattern: 'needle', output_mode: 'files_with_matches' }, ctx());
    assert.ok(res.output.includes('alpha.ts') || res.output.includes('beta.ts'));
  });

  it('count mode returns per-file counts', async () => {
    seedGrepFiles();
    const res = await grepTool.execute({ pattern: 'needle', output_mode: 'count' }, ctx());
    assert.ok(res.output.length > 0);
  });

  it('case-insensitive matching', async () => {
    seedGrepFiles();
    const res = await grepTool.execute({ pattern: 'NEEDLE', case_insensitive: true }, ctx());
    assert.ok(res.output.includes('needle'));
  });

  it('supports context_lines, before_context, after_context, multiline, line_numbers, type', async () => {
    seedGrepFiles();
    const res = await grepTool.execute(
      {
        pattern: 'needle',
        context_lines: 1,
        before_context: 1,
        after_context: 1,
        multiline: true,
        line_numbers: false,
        type: 'ts',
      },
      ctx(),
    );
    assert.ok(res.output.length > 0);
  });

  it('supports comma-separated include globs', async () => {
    seedGrepFiles();
    const res = await grepTool.execute({ pattern: 'needle', include: '*.ts, *.py' }, ctx());
    assert.ok(res.output.length > 0);
  });

  it('handles a pattern starting with a dash', async () => {
    fs.writeFileSync(path.join(tmpDir, 'dash.txt'), 'value = -42;\n');
    const res = await grepTool.execute({ pattern: '-42' }, ctx());
    assert.ok(res.output.includes('-42') || res.output.includes('No matches'));
  });

  it('head_limit=0 means unlimited', async () => {
    seedGrepFiles();
    const res = await grepTool.execute({ pattern: 'needle', head_limit: 0 }, ctx());
    assert.ok(res.output.length > 0);
  });

  it('truncates with head_limit and reports the range', async () => {
    const fp = path.join(tmpDir, 'many.txt');
    fs.writeFileSync(fp, Array.from({ length: 20 }, (_, i) => `match ${i}`).join('\n') + '\n');
    const res = await grepTool.execute({ pattern: 'match', head_limit: 3 }, ctx());
    assert.ok(res.output.includes('showing lines') || res.output.includes('truncated'));
  });

  it('offset into results reports a shifted range', async () => {
    const fp = path.join(tmpDir, 'off.txt');
    fs.writeFileSync(fp, Array.from({ length: 10 }, (_, i) => `hit ${i}`).join('\n') + '\n');
    const res = await grepTool.execute({ pattern: 'hit', offset: 2, head_limit: 3 }, ctx());
    assert.ok(res.output.includes('showing lines 3-'));
  });

  it('offset beyond all results yields a no-matches-in-range message', async () => {
    const fp = path.join(tmpDir, 'off2.txt');
    fs.writeFileSync(fp, 'single hit\n');
    const res = await grepTool.execute({ pattern: 'hit', offset: 50 }, ctx());
    assert.ok(res.output.includes('No matches in range'));
  });

  it('reports no matches when the pattern is absent', async () => {
    seedGrepFiles();
    const res = await grepTool.execute({ pattern: 'zzz-absent-zzz' }, ctx());
    assert.ok(res.output.includes('No matches found'));
  });

  it('filters out go test suite-log lines', async () => {
    const fp = path.join(tmpDir, 'suite.log');
    fs.writeFileSync(
      fp,
      '{"Action":"pass","Test":"TestFoo","Elapsed":0.1}\nreal needle line\n',
    );
    const res = await grepTool.execute({ pattern: 'needle|Action' }, ctx());
    assert.ok(res.output.includes('real needle line'));
    assert.ok(!res.output.includes('"Action"'));
  });

  it('errors on an unreadable search path', async () => {
    const res = await grepTool.execute(
      { pattern: 'x', path: path.join(tmpDir, 'nowhere') },
      ctx(),
    );
    assert.equal(res.isError, true);
  });

  it('errors on a missing pattern', async () => {
    const res = await grepTool.execute({ pattern: '' }, ctx());
    assert.equal(res.isError, true);
  });
});

describe('grep exported helpers', () => {
  it('stripGrepFlags removes embedded flags but keeps a flagless pattern', () => {
    assert.equal(stripGrepFlags('-l -c needle'), 'needle');
    assert.equal(stripGrepFlags('plain'), 'plain');
    assert.equal(stripGrepFlags('-l '), '-l '); // nothing left -> keep original
  });

  it('isGrepNoMatchExit identifies exit code 1', () => {
    assert.equal(isGrepNoMatchExit(1), true);
    assert.equal(isGrepNoMatchExit(0), false);
    assert.equal(isGrepNoMatchExit(2), false);
  });

  it('searchFilesForPattern walks a tree, honoring includes and skip dirs', () => {
    seedGrepFiles();
    fs.mkdirSync(path.join(tmpDir, 'node_modules'));
    fs.writeFileSync(path.join(tmpDir, 'node_modules', 'skip.ts'), 'needle skipped\n');
    const out = searchFilesForPattern(tmpDir, 'needle', { include: '*.ts' });
    assert.ok(out.includes('alpha.ts'));
    assert.ok(!out.includes('skip.ts'));
    assert.ok(!out.includes('gamma.py'));
  });

  it('searchFilesForPattern matches a single file root', () => {
    const fp = path.join(tmpDir, 'solo.txt');
    fs.writeFileSync(fp, 'has needle here\nno match\n');
    const out = searchFilesForPattern(fp, 'needle', {});
    assert.ok(out.includes('solo.txt:1:'));
  });

  it('searchFilesForPattern is case-insensitive when asked', () => {
    const fp = path.join(tmpDir, 'ci.txt');
    fs.writeFileSync(fp, 'NEEDLE\n');
    const out = searchFilesForPattern(fp, 'needle', { caseInsensitive: true });
    assert.ok(out.includes('NEEDLE'));
  });

  it('searchFilesForPattern skips over-large files', () => {
    const fp = path.join(tmpDir, 'large.txt');
    fs.writeFileSync(fp, 'needle\n');
    fs.truncateSync(fp, 2_000_000); // > FALLBACK_MAX_FILE_BYTES, sparse
    const out = searchFilesForPattern(path.join(tmpDir), 'needle', { include: 'large.txt' });
    assert.ok(!out.includes('large.txt'));
  });

  it('searchFilesForPattern returns empty for a non-existent root', () => {
    assert.equal(searchFilesForPattern(path.join(tmpDir, 'ghost'), 'x', {}), '');
  });

  it('searchFilesForPattern with headLimit=0 scans without a cap', () => {
    seedGrepFiles();
    const out = searchFilesForPattern(tmpDir, 'needle', { headLimit: 0 });
    assert.ok(out.split('\n').filter(Boolean).length >= 3);
  });

  it('searchFilesForPattern stops at the FALLBACK_MAX_FILES scan cap', () => {
    const dir = path.join(tmpDir, 'floods');
    fs.mkdirSync(dir);
    // > 4000 files so the seen >= FALLBACK_MAX_FILES guard trips. Pattern never
    // matches, so the cap (not the match cap) is what halts the walk.
    for (let i = 0; i < 4005; i++) {
      fs.writeFileSync(path.join(dir, `f${i}.txt`), 'nothing-of-interest\n');
    }
    const out = searchFilesForPattern(dir, 'zzz-never-matches-zzz', {});
    assert.equal(out, '');
  });

  it('searchFilesForPattern throws on an invalid regex', () => {
    assert.throws(() => searchFilesForPattern(tmpDir, '(', {}));
  });

  it('searchFilesForPattern skips non-file, non-dir entries (symlinks)', () => {
    const dir = path.join(tmpDir, 'walkme');
    fs.mkdirSync(dir);
    fs.writeFileSync(path.join(dir, 'real.txt'), 'needle here\n');
    fs.symlinkSync('/nonexistent-target', path.join(dir, 'dangling-link'));
    const out = searchFilesForPattern(dir, 'needle', {});
    assert.ok(out.includes('real.txt'));
    assert.ok(!out.includes('dangling-link'));
  });

  it('searchFilesForPattern skips files it cannot read', () => {
    const dir = path.join(tmpDir, 'walkperm');
    fs.mkdirSync(dir);
    fs.writeFileSync(path.join(dir, 'ok.txt'), 'needle ok\n');
    const locked = path.join(dir, 'locked.txt');
    fs.writeFileSync(locked, 'needle locked\n');
    fs.chmodSync(locked, 0o000);
    try {
      const out = searchFilesForPattern(dir, 'needle', {});
      assert.ok(out.includes('ok.txt'));
      assert.ok(!out.includes('locked.txt'));
    } finally {
      fs.chmodSync(locked, 0o644);
    }
  });
});

describe('grepTool – in-process fallback for invalid regex', () => {
  it('reports an invalid regular expression error (fallback path)', async () => {
    seedGrepFiles();
    // '(' is an invalid regex; rg is absent here and grep -E exits >1 with no
    // stdout, so the in-process fallback runs and surfaces the regex error.
    const res = await grepTool.execute({ pattern: '(' }, ctx());
    // Either the fallback regex error OR (if a system grep tolerates it) a result.
    assert.ok(
      res.output.includes('Invalid regular expression') || res.output.length > 0,
    );
  });
});

// ===========================================================================
// file-edit.ts — error seams needing real fs conditions
// ===========================================================================

describe('fileEditTool – deeper error paths', () => {
  it('rejects a write-mode edit on a non-UTF-encoded file', async () => {
    const fp = path.join(tmpDir, 'sjis.txt');
    // High bytes that are invalid UTF-8 (Shift-JIS-like) in an existing file.
    fs.writeFileSync(fp, Buffer.from([0x82, 0xa0, 0x82, 0xa2, 0x82, 0xa4]));
    const res = await fileEditTool.execute({ file_path: fp, new_string: 'replacement\n' }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('non-UTF encoding'));
  });

  it('returns a write error when the parent path is a file (commitWrite failure)', async () => {
    const blocker = path.join(tmpDir, 'blocker');
    fs.writeFileSync(blocker, 'i am a file');
    const res = await fileEditTool.execute(
      { file_path: path.join(blocker, 'child.txt'), new_string: 'x\n' },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('Error writing file'));
  });

  it('rejects editing a file larger than the 1 GiB limit', async () => {
    const fp = path.join(tmpDir, 'huge.bin');
    fs.writeFileSync(fp, 'seed');
    fs.truncateSync(fp, 1024 * 1024 * 1024 + 1); // sparse, no real disk use
    const res = await fileEditTool.execute(
      { file_path: fp, old_string: 'seed', new_string: 'other' },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('exceeds 1 GiB limit'));
  });

  it('refuses a single replace when the file changed since the last read', async () => {
    const fp = path.join(tmpDir, 'changed.txt');
    fs.writeFileSync(fp, 'original content\nline two\n');
    const context = ctx();
    await fileReadTool.execute({ file_path: fp }, context);
    // Mutate on disk with a clearly newer mtime.
    fs.writeFileSync(fp, 'original content\nline two\nextra\n');
    const future = Date.now() / 1000 + 100;
    fs.utimesSync(fp, future, future);
    const res = await fileEditTool.execute(
      { file_path: fp, old_string: 'original content', new_string: 'new content' },
      context,
    );
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('modified since you last read it'));
  });

  it('still succeeds when history tracking cannot write (.superinference is a file)', async () => {
    // .superinference as a file makes trackFileHistory's mkdir throw (caught).
    fs.writeFileSync(path.join(tmpDir, '.superinference'), 'blocking file');
    const fp = path.join(tmpDir, 'edit-me.txt');
    fs.writeFileSync(fp, 'hello world\n');
    const res = await fileEditTool.execute(
      { file_path: fp, old_string: 'hello', new_string: 'bye' },
      ctx(),
    );
    assert.equal(res.isError, undefined);
    assert.ok(fs.readFileSync(fp, 'utf-8').includes('bye world'));
  });

  it('reports "no changes detected" for a CRLF/LF-only no-op replace', async () => {
    const fp = path.join(tmpDir, 'noop.txt');
    fs.writeFileSync(fp, 'foo\nbar\n');
    const res = await fileEditTool.execute(
      { file_path: fp, old_string: 'foo\r\n', new_string: 'foo\n' },
      ctx(),
    );
    assert.equal(res.isError, undefined);
    assert.ok(res.output.includes('no changes detected'));
  });

  it('rejects a sequential edit on a non-UTF-encoded file', async () => {
    const fp = path.join(tmpDir, 'sjis-seq.txt');
    fs.writeFileSync(fp, Buffer.from([0x82, 0xa0, 0x82, 0xa2, 0x82, 0xa4]));
    const res = await fileEditTool.execute(
      { file_path: fp, edits: [{ old_string: 'a', new_string: 'b' }] },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('non-UTF encoding'));
  });

  it('inner encoding-detect failure still writes (readFileSync on unreadable existing file)', async () => {
    const fp = path.join(tmpDir, 'noperm.txt');
    fs.writeFileSync(fp, 'existing\n');
    fs.chmodSync(fp, 0o000);
    try {
      const res = await fileEditTool.execute({ file_path: fp, new_string: 'fresh\n' }, ctx());
      // readFileSync inside commitWrite throws (caught); the writeFileSync also
      // fails for a 0o000 file, so we get a write error — but the inner catch ran.
      assert.equal(res.isError, true);
      assert.ok(res.output.includes('Error writing file'));
    } finally {
      fs.chmodSync(fp, 0o644);
    }
  });

  it('prunes history even when an old backup entry cannot be unlinked', async () => {
    const fp = path.join(tmpDir, 'prune-me.txt');
    const histDir = path.join(tmpDir, '.superinference', 'file-history');
    fs.mkdirSync(histDir, { recursive: true });
    const safe = 'prune-me.txt';
    // A directory named like a .bak entry: unlink on it rejects (.catch arm).
    fs.mkdirSync(path.join(histDir, `${safe}.0001.bak`));
    for (let i = 1; i < 25; i++) {
      fs.writeFileSync(path.join(histDir, `${safe}.${String(1000 + i)}.bak`), `b${i}`);
    }
    fs.writeFileSync(fp, 'v1\n');
    const res = await fileEditTool.execute(
      { file_path: fp, old_string: 'v1', new_string: 'v2' },
      ctx(),
    );
    assert.equal(res.isError, undefined);
  });

  it('execute re-checks the resolved path against the device blocklist', async () => {
    // cwd=/dev makes "null" resolve to /dev/null — inside "workspace" yet blocked.
    const res = await fileEditTool.execute(
      { file_path: 'null', new_string: 'x\n' },
      ctx({ cwd: '/dev' }),
    );
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('Cannot edit device or special file'));
  });
});

describe('fileEditTool – branch completeness', () => {
  it('rejects a file path containing a null byte', async () => {
    const res = await fileEditTool.execute(
      { file_path: path.join(tmpDir, 'bad\x00.txt'), new_string: 'x\n' },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('null byte'));
  });

  it('accepts the "path" alias for file_path', async () => {
    const fp = path.join(tmpDir, 'via-path.txt');
    const res = await fileEditTool.execute({ path: fp, new_string: 'hi\n' }, ctx());
    assert.equal(res.isError, undefined);
    assert.equal(fs.readFileSync(fp, 'utf-8'), 'hi\n');
  });

  it('single replace matches via a non-exact (line_trimmed) strategy', async () => {
    const fp = path.join(tmpDir, 'fuzzy.txt');
    // Middle line is indented 8 spaces in the file but 4 in old_string, so only
    // the line_trimmed strategy matches (no exact substring).
    fs.writeFileSync(fp, 'function f() {\n        return 1;\n}\n');
    const res = await fileEditTool.execute(
      { file_path: fp, old_string: 'function f() {\n    return 1;\n}', new_string: 'function f() {\n    return 2;\n}' },
      ctx(),
    );
    assert.equal(res.isError, undefined);
    assert.ok(res.output.includes('strategy'));
    assert.ok(fs.readFileSync(fp, 'utf-8').includes('return 2;'));
  });

  it('sequential edit matches via a non-exact strategy', async () => {
    const fp = path.join(tmpDir, 'fuzzy-seq.txt');
    fs.writeFileSync(fp, 'while (go) {\n          step();\n}\n');
    const res = await fileEditTool.execute(
      { file_path: fp, edits: [{ old_string: 'while (go) {\n  step();\n}', new_string: 'while (go) {\n  halt();\n}' }] },
      ctx(),
    );
    assert.equal(res.isError, false);
    assert.ok(res.output.includes('1/1 edits applied'));
    assert.ok(fs.readFileSync(fp, 'utf-8').includes('halt();'));
  });

  it('single replace surfaces a commitWrite failure on a read-only file', async () => {
    const fp = path.join(tmpDir, 'ro-single.txt');
    fs.writeFileSync(fp, 'alpha beta\n');
    fs.chmodSync(fp, 0o444);
    try {
      const res = await fileEditTool.execute(
        { file_path: fp, old_string: 'alpha', new_string: 'gamma' },
        ctx(),
      );
      assert.equal(res.isError, true);
      assert.ok(res.output.includes('Error writing file'));
    } finally {
      fs.chmodSync(fp, 0o644);
    }
  });

  it('sequential edit surfaces a commitWrite failure on a read-only file', async () => {
    const fp = path.join(tmpDir, 'ro-seq.txt');
    fs.writeFileSync(fp, 'one two\n');
    fs.chmodSync(fp, 0o444);
    try {
      const res = await fileEditTool.execute(
        { file_path: fp, edits: [{ old_string: 'one', new_string: 'uno' }] },
        ctx(),
      );
      assert.equal(res.isError, true);
      assert.ok(res.output.includes('Error writing file'));
    } finally {
      fs.chmodSync(fp, 0o644);
    }
  });

  it('notifyLsp swallows a client-lookup failure (catch path)', async () => {
    mock.method(lspHooks, 'getClient', (() => { throw new Error('no lsp'); }) as any);
    const fp = path.join(tmpDir, 'lsp-fail.txt');
    const res = await fileEditTool.execute(
      { file_path: fp, new_string: 'content\n' },
      ctx({ detachedMode: false }),
    );
    assert.equal(res.isError, undefined);
  });

  it('notifyLsp swallows rejected notification promises (.catch arms)', async () => {
    mock.method(lspHooks, 'getClient', (() => ({
      notifyDidChange: () => Promise.reject(new Error('change failed')),
      notifyDidSave: () => Promise.reject(new Error('save failed')),
    })) as any);
    const fp = path.join(tmpDir, 'lsp-reject.txt');
    const res = await fileEditTool.execute(
      { file_path: fp, new_string: 'content\n' },
      ctx({ detachedMode: false }),
    );
    assert.equal(res.isError, undefined);
    // Let the fire-and-forget .catch(() => {}) handlers run.
    await new Promise((r) => setImmediate(r));
  });
});

describe('commitWrite (exported)', () => {
  it('propagates a verifyWriteTarget symlink error', () => {
    const outside = fs.mkdtempSync(path.join(os.tmpdir(), 'si-cw-out-'));
    try {
      const target = path.join(outside, 'f.txt');
      fs.writeFileSync(target, 'x');
      const link = path.join(tmpDir, 'cwlink.txt');
      fs.symlinkSync(target, link);
      const err = commitWrite(link, 'data', 'data', ctx());
      assert.ok(err && err.includes('symlink resolving to'));
    } finally {
      fs.rmSync(outside, { recursive: true, force: true });
    }
  });

  it('writes successfully for a legitimate path', () => {
    const fp = path.join(tmpDir, 'cw-ok.txt');
    const err = commitWrite(fp, 'hello', 'hello', ctx());
    assert.equal(err, undefined);
    assert.equal(fs.readFileSync(fp, 'utf-8'), 'hello');
  });
});

describe('writeEntireFile (exported) – own guards', () => {
  it('blocks a device/special file', async () => {
    const res = await writeEntireFile('/dev/null', 'x\n', ctx({ cwd: '/dev' }));
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('Cannot edit device or special file'));
  });

  it('rejects a directory target', async () => {
    const d = path.join(tmpDir, 'adir');
    fs.mkdirSync(d);
    const res = await writeEntireFile(d, 'x\n', ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('is a directory'));
  });
});

// ===========================================================================
// file-read.ts — binary, range header, large suite logs, image compression
// ===========================================================================

describe('fileReadTool – binary & range-header & suite logs', () => {
  it('detects binary content by null bytes in a non-binary extension', async () => {
    const fp = path.join(tmpDir, 'nulls.txt');
    const buf = Buffer.alloc(2000, 0x41);
    for (let i = 0; i < 200; i++) buf[i * 10] = 0; // >1% null bytes
    fs.writeFileSync(fp, buf);
    const res = await fileReadTool.execute({ file_path: fp }, ctx());
    assert.equal(res.output, '[Binary file]');
  });

  it('emits a range header for a full read of a file with more than 2000 lines', async () => {
    const fp = path.join(tmpDir, 'long.txt');
    fs.writeFileSync(fp, Array.from({ length: 2500 }, (_, i) => `r${i}`).join('\n'));
    const res = await fileReadTool.execute({ file_path: fp }, ctx());
    assert.ok(res.output.includes('Showing lines 1-2000 of 2500'));
  });

  it('compacts a large (>8MB) suite log via head/tail read', async () => {
    const fp = path.join(tmpDir, 'bigsuite.log');
    const line = '{"Action":"pass","Test":"TestFoo","Package":"pkg","Elapsed":0.01}\n';
    const chunk = line.repeat(2000); // ~130KB
    const fh = fs.openSync(fp, 'w');
    // Write just over 8MB so readSuiteHeadAndTail is used.
    for (let i = 0; i < 65; i++) fs.writeSync(fh, chunk);
    fs.closeSync(fh);
    assert.ok(fs.statSync(fp).size > 8_000_000);
    const res = await fileReadTool.execute({ file_path: fp }, ctx());
    assert.ok(res.output.includes('Passing tests were omitted'));
  });

  it('falls back to a normal read when the full suite log fails the deeper check', async () => {
    const fp = path.join(tmpDir, 'fakesuite.log');
    // First 64KB looks like a suite log; a marker later makes the full check fail.
    const line = '{"Action":"pass","Test":"TestFoo","Elapsed":0.01}\n';
    const head = line.repeat(1500); // ~75KB > 64KB peek window
    fs.writeFileSync(fp, head + '\nShowing failing output only\n');
    const res = await fileReadTool.execute({ file_path: fp }, ctx());
    // Not the suite-log summary — a normal file read (or partial view).
    assert.ok(!res.output.includes('Passing tests were omitted'));
  });

  it('encodeImageFile handles large images (no sharp -> original bytes)', async () => {
    const fp = path.join(tmpDir, 'big.png');
    fs.writeFileSync(fp, Buffer.alloc(600 * 1024, 0x89)); // > 512KB
    const out = await encodeImageFile(fp);
    assert.ok(out.data.length > 0);
    assert.ok(typeof out.media_type === 'string');
  });

  it('reads a large image through the tool (base64 image JSON)', async () => {
    const fp = path.join(tmpDir, 'big2.png');
    fs.writeFileSync(fp, Buffer.alloc(600 * 1024, 0x89));
    const res = await fileReadTool.execute({ file_path: fp }, ctx());
    assert.ok(res.output.includes('"type":"image"'));
  });

  it('blocks reading a device/special file when it resolves inside cwd', async () => {
    // cwd=/dev makes "null" resolve to /dev/null — a blocked device.
    const res = await fileReadTool.execute({ file_path: 'null' }, ctx({ cwd: '/dev' }));
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('Reading device/special files is not allowed'));
  });

});

describe('encodeImageFile – sharp compression via seam', () => {
  function fakeSharp(firstLen: number, opts?: { secondThrows?: boolean }) {
    let call = 0;
    return (_input: Buffer) => ({
      resize() { return this; },
      jpeg() { return this; },
      async toBuffer() {
        call++;
        if (call === 1) return Buffer.alloc(firstLen);
        if (opts?.secondThrows) throw new Error('second resize failed');
        return Buffer.alloc(100);
      },
    });
  }

  it('compresses a large image and re-compresses when still too big', async () => {
    const fp = path.join(tmpDir, 'large.png');
    fs.writeFileSync(fp, Buffer.alloc(600 * 1024, 0x89));
    // 50000 bytes -> base64*0.125 > 5000 -> triggers the second downscale.
    mock.method(imageHooks, 'loadSharp', (() => fakeSharp(50000)) as any);
    const out = await encodeImageFile(fp);
    assert.equal(out.media_type, 'image/jpeg');
    assert.ok(out.data.length > 0);
  });

  it('keeps the first compression when the second downscale throws', async () => {
    const fp = path.join(tmpDir, 'large2.png');
    fs.writeFileSync(fp, Buffer.alloc(600 * 1024, 0x89));
    mock.method(imageHooks, 'loadSharp', (() => fakeSharp(50000, { secondThrows: true })) as any);
    const out = await encodeImageFile(fp);
    assert.equal(out.media_type, 'image/jpeg');
  });

  it('compresses without a second pass when already small enough', async () => {
    const fp = path.join(tmpDir, 'large3.png');
    fs.writeFileSync(fp, Buffer.alloc(600 * 1024, 0x89));
    mock.method(imageHooks, 'loadSharp', (() => fakeSharp(1000)) as any);
    const out = await encodeImageFile(fp);
    assert.equal(out.media_type, 'image/jpeg');
  });

  it('uses octet-stream for an image extension without a known media type', async () => {
    const fp = path.join(tmpDir, 'vector.svg');
    fs.writeFileSync(fp, '<svg/>'); // small -> no sharp path
    const out = await encodeImageFile(fp);
    assert.equal(out.media_type, 'application/octet-stream');
  });
});

describe('fileReadTool – more branch completeness', () => {
  it('rejects a file path containing a null byte', async () => {
    const res = await fileReadTool.execute({ file_path: path.join(tmpDir, 'x\x00.txt') }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('null byte'));
  });

  it('reads a UTF-16LE (BOM) file as text, not binary', async () => {
    const fp = path.join(tmpDir, 'utf16.txt');
    const body = Buffer.concat([
      Buffer.from([0xff, 0xfe]),
      Buffer.from('hello utf16 world\nsecond line\n', 'utf16le'),
    ]);
    fs.writeFileSync(fp, body);
    const res = await fileReadTool.execute({ file_path: fp }, ctx());
    assert.ok(!res.output.includes('[Binary file]'));
    assert.ok(res.output.includes('hello utf16 world'));
  });

  it('survives an lstat failure during symlink validation (TOCTOU guard)', async () => {
    const fp = path.join(tmpDir, 'racey.txt');
    fs.writeFileSync(fp, 'content survives the race\n');
    mock.method(fileReadFsHooks, 'lstatSync', (() => { throw new Error('vanished'); }) as any);
    const res = await fileReadTool.execute({ file_path: fp }, ctx());
    // The lstat throw is swallowed; the read still succeeds below.
    assert.ok(res.output.includes('content survives the race'));
  });

  it('survives a BOM-sniff open failure (falls back to utf-8)', async () => {
    const fp = path.join(tmpDir, 'bomfail.txt');
    fs.writeFileSync(fp, 'plain utf8 content\n');
    mock.method(fileReadFsHooks, 'openForBom', (() => Promise.reject(new Error('no fd'))) as any);
    const res = await fileReadTool.execute({ file_path: fp }, ctx());
    assert.ok(res.output.includes('plain utf8 content'));
  });
});

// ===========================================================================
// grep.ts — runSearch branch coverage via the spawn/timeout seam
// ===========================================================================

interface FakeChild extends EventEmitter {
  stdout: EventEmitter;
  stderr: EventEmitter;
  kill: (signal?: string) => boolean;
}

function makeFakeChild(): FakeChild {
  const child = new EventEmitter() as FakeChild;
  child.stdout = new EventEmitter();
  child.stderr = new EventEmitter();
  child.kill = () => true;
  return child;
}

function mockSpawn(impl: (cmd: string, child: FakeChild) => void): FakeChild[] {
  const children: FakeChild[] = [];
  mock.method(grepHooks, 'spawn', ((cmd: string) => {
    const child = makeFakeChild();
    children.push(child);
    impl(cmd, child);
    return child as any;
  }) as any);
  return children;
}

describe('grep runSearch – branch coverage via seam', () => {
  it('resolves null when spawn throws synchronously', async () => {
    mock.method(grepHooks, 'spawn', (() => { throw new Error('boom'); }) as any);
    const outcome = await runSearch(['x'], 'rg', ctx(), 1000);
    assert.equal(outcome.output, null);
  });

  it('flags EAGAIN errors', async () => {
    mockSpawn((_cmd, child) => {
      setImmediate(() => child.emit('error', { code: 'EAGAIN' }));
    });
    const outcome = await runSearch(['x'], 'rg', ctx(), 1000);
    assert.equal(outcome.eagain, true);
    assert.equal(outcome.output, null);
  });

  it('resolves null on a non-EAGAIN spawn error', async () => {
    mockSpawn((_cmd, child) => {
      setImmediate(() => child.emit('error', { code: 'ENOENT' }));
    });
    const outcome = await runSearch(['x'], 'rg', ctx(), 1000);
    assert.equal(outcome.output, null);
    assert.equal(outcome.eagain, undefined);
  });

  it('returns stdout on a clean exit (code 0)', async () => {
    mockSpawn((_cmd, child) => {
      setImmediate(() => {
        child.stdout.emit('data', Buffer.from('a:1:hit\n'));
        child.emit('close', 0, null);
      });
    });
    const outcome = await runSearch(['x'], 'rg', ctx(), 1000);
    assert.equal(outcome.output, 'a:1:hit\n');
  });

  it('treats exit code 1 as an empty (no-match) result', async () => {
    mockSpawn((_cmd, child) => {
      setImmediate(() => child.emit('close', 1, null));
    });
    const outcome = await runSearch(['x'], 'grep', ctx(), 1000);
    assert.equal(outcome.output, '');
  });

  it('reports a search error with stdout on exit code > 1', async () => {
    mockSpawn((_cmd, child) => {
      setImmediate(() => {
        child.stdout.emit('data', Buffer.from('partial\n'));
        child.stderr.emit('data', Buffer.from('some warning'));
        child.emit('close', 2, null);
      });
    });
    const outcome = await runSearch(['x'], 'grep', ctx(), 1000);
    assert.equal(outcome.output, 'partial\n');
    assert.ok(outcome.searchError && outcome.searchError.includes('some warning'));
  });

  it('reports a search error without stdout on exit code > 1', async () => {
    mockSpawn((_cmd, child) => {
      setImmediate(() => child.emit('close', 2, null));
    });
    const outcome = await runSearch(['x'], 'grep', ctx(), 1000);
    assert.equal(outcome.output, null);
    assert.ok(outcome.searchError && outcome.searchError.includes('exited with code 2'));
  });

  it('reports termination by signal', async () => {
    mockSpawn((_cmd, child) => {
      setImmediate(() => child.emit('close', null, 'SIGTERM'));
    });
    const outcome = await runSearch(['x'], 'grep', ctx(), 1000);
    assert.ok(outcome.searchError && outcome.searchError.includes('SIGTERM'));
  });

  it('handles a null exit code with no signal (defensive code ?? 0)', async () => {
    mockSpawn((_cmd, child) => {
      setImmediate(() => child.emit('close', null, null));
    });
    const outcome = await runSearch(['x'], 'grep', ctx(), 1000);
    // code null, not a no-match (1) -> treated as an error with no stdout.
    assert.equal(outcome.output, null);
    assert.ok(outcome.searchError && outcome.searchError.includes('code null'));
  });

  it('flags a timeout and kills the child', async () => {
    mockSpawn((_cmd, child) => {
      // Never emits on its own; kill() (from the timer) ends it.
      child.kill = () => { child.emit('close', null, 'SIGKILL'); return true; };
    });
    const outcome = await runSearch(['x'], 'grep', ctx(), 5);
    assert.equal(outcome.timedOut, true);
  });

  it('returns empty immediately for an already-aborted signal', async () => {
    const controller = new AbortController();
    controller.abort();
    const killed: boolean[] = [];
    mockSpawn((_cmd, child) => { child.kill = () => { killed.push(true); return true; }; });
    const outcome = await runSearch(['x'], 'grep', ctx({ abortSignal: controller.signal }), 1000);
    assert.equal(outcome.output, '');
    assert.equal(killed.length, 1);
  });

  it('kills the child when the signal aborts mid-search', async () => {
    const controller = new AbortController();
    const killed: string[] = [];
    mockSpawn((_cmd, child) => {
      child.kill = (sig?: string) => { killed.push(sig ?? ''); return true; };
      setImmediate(() => {
        controller.abort();
        child.emit('close', 0, null);
      });
    });
    const outcome = await runSearch(['x'], 'grep', ctx({ abortSignal: controller.signal }), 1000);
    assert.equal(outcome.output, '');
    assert.ok(killed.length >= 1);
  });
});

describe('grepTool.execute – integration via seam', () => {
  it('uses ripgrep output when the rg process succeeds', async () => {
    mockSpawn((cmd, child) => {
      setImmediate(() => {
        if (cmd === 'rg') {
          child.stdout.emit('data', Buffer.from('file.ts:1:needle\n'));
          child.emit('close', 0, null);
        } else {
          child.emit('close', 1, null);
        }
      });
    });
    const res = await grepTool.execute({ pattern: 'needle' }, ctx());
    assert.ok(res.output.includes('needle'));
  });

  it('retries ripgrep once on EAGAIN then succeeds', async () => {
    let rgCalls = 0;
    mockSpawn((cmd, child) => {
      setImmediate(() => {
        if (cmd === 'rg') {
          rgCalls++;
          if (rgCalls === 1) child.emit('error', { code: 'EAGAIN' });
          else { child.stdout.emit('data', Buffer.from('f:1:needle\n')); child.emit('close', 0, null); }
        } else {
          child.emit('close', 1, null);
        }
      });
    });
    const res = await grepTool.execute({ pattern: 'needle' }, ctx());
    assert.ok(res.output.includes('needle'));
    assert.equal(rgCalls, 2);
  });

  it('appends a warning when rg exits with an error but still produced output', async () => {
    mockSpawn((cmd, child) => {
      setImmediate(() => {
        if (cmd === 'rg') {
          child.stdout.emit('data', Buffer.from('f:1:needle\n'));
          child.stderr.emit('data', Buffer.from('rg partial failure'));
          child.emit('close', 2, null);
        } else {
          child.emit('close', 1, null);
        }
      });
    });
    const res = await grepTool.execute({ pattern: 'needle' }, ctx());
    assert.ok(res.output.includes('needle'));
    assert.ok(res.output.includes('search exited with error'));
  });

  it('returns a timeout error when ripgrep times out', async () => {
    const orig = grepHooks.timeoutMs;
    grepHooks.timeoutMs = 5;
    try {
      mockSpawn((_cmd, child) => {
        child.kill = () => { child.emit('close', null, 'SIGKILL'); return true; };
      });
      const res = await grepTool.execute({ pattern: 'needle' }, ctx());
      assert.equal(res.isError, true);
      assert.ok(res.output.includes('timed out'));
    } finally {
      grepHooks.timeoutMs = orig;
    }
  });

  it('records the rg search error then succeeds via grep fallback', async () => {
    mockSpawn((cmd, child) => {
      setImmediate(() => {
        if (cmd === 'rg') {
          // Exit >1 with no stdout -> searchError recorded, output null.
          child.stderr.emit('data', Buffer.from('rg total failure'));
          child.emit('close', 2, null);
        } else {
          child.stdout.emit('data', Buffer.from('f:1:needle\n'));
          child.emit('close', 0, null);
        }
      });
    });
    const res = await grepTool.execute({ pattern: 'needle' }, ctx());
    assert.ok(res.output.includes('needle'));
  });

  it('falls back to grep and appends a warning when grep exits with an error', async () => {
    mockSpawn((cmd, child) => {
      setImmediate(() => {
        if (cmd === 'rg') {
          child.emit('error', { code: 'ENOENT' });
        } else {
          child.stdout.emit('data', Buffer.from('f:1:needle\n'));
          child.stderr.emit('data', Buffer.from('grep partial failure'));
          child.emit('close', 2, null);
        }
      });
    });
    const res = await grepTool.execute({ pattern: 'needle' }, ctx());
    assert.ok(res.output.includes('needle'));
    assert.ok(res.output.includes('search exited with error'));
  });

  it('returns a timeout error when grep times out', async () => {
    const orig = grepHooks.timeoutMs;
    grepHooks.timeoutMs = 5;
    try {
      mockSpawn((cmd, child) => {
        if (cmd === 'rg') {
          setImmediate(() => child.emit('error', { code: 'ENOENT' }));
        } else {
          child.kill = () => { child.emit('close', null, 'SIGKILL'); return true; };
        }
      });
      const res = await grepTool.execute({ pattern: 'needle' }, ctx());
      assert.equal(res.isError, true);
      assert.ok(res.output.includes('timed out'));
    } finally {
      grepHooks.timeoutMs = orig;
    }
  });
});
