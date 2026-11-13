import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { listDirTool } from '../src/tools/list-dir';
import { notebookEditTool } from '../src/tools/notebook-edit';
import { createDefaultTools } from '../src/tools/index';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// POV: core tools list_dir + notebook_edit — behavioral over real temp files.
// Every assertion is computed from the real tool's observed output / on-disk
// effect, never by reading source text.
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
  // realpathSync so comparisons survive /tmp symlink indirection on some OSes.
  tmpDir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'pov-nb-ld-')));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ===========================================================================
// createDefaultTools registration — the tools are wired under their real names
// ===========================================================================

describe('createDefaultTools — list_dir / notebook_edit registration', () => {
  it('registers list_dir and notebook_edit under the exact tool names', () => {
    const reg = createDefaultTools(tmpDir);
    assert.equal(reg.get('list_dir'), listDirTool);
    assert.equal(reg.get('notebook_edit'), notebookEditTool);
  });

  it('exposes the documented read-only / concurrency shape for list_dir', () => {
    const reg = createDefaultTools(tmpDir);
    const t = reg.get('list_dir')!;
    assert.equal(t.isReadOnly, true);
    assert.equal(t.isConcurrencySafe, true);
    assert.ok(t.inputSchema.required?.includes('path'));
  });

  it('exposes the documented mutating shape for notebook_edit', () => {
    const reg = createDefaultTools(tmpDir);
    const t = reg.get('notebook_edit')!;
    assert.equal(t.isReadOnly, false);
    assert.ok(t.inputSchema.required?.includes('notebook_path'));
    assert.ok(t.inputSchema.required?.includes('new_source'));
  });

  it('both tools appear in getAll() exactly once', () => {
    const names = createDefaultTools(tmpDir).getAll().map(t => t.name);
    assert.equal(names.filter(n => n === 'list_dir').length, 1);
    assert.equal(names.filter(n => n === 'notebook_edit').length, 1);
  });
});

// ===========================================================================
// list_dir — directory listing behavior
// ===========================================================================

describe('list_dir — listing behavior', () => {
  it('lists files and subdirs with [dir]/[file] markers and a count header', async () => {
    fs.writeFileSync(path.join(tmpDir, 'b.txt'), 'x');
    fs.writeFileSync(path.join(tmpDir, 'a.txt'), 'x');
    fs.mkdirSync(path.join(tmpDir, 'sub'));

    const res = await listDirTool.execute({ path: tmpDir }, ctx());
    assert.ok(!res.isError, res.output);

    const lines = res.output.split('\n');
    assert.equal(lines[0], `Contents of ${tmpDir}: (3 entries)`);

    const body = res.output;
    assert.ok(body.includes('[dir]  sub/'));
    assert.ok(body.includes('[file] a.txt'));
    assert.ok(body.includes('[file] b.txt'));
  });

  it('orders directories before files, each group alphabetical', async () => {
    fs.mkdirSync(path.join(tmpDir, 'zdir'));
    fs.mkdirSync(path.join(tmpDir, 'adir'));
    fs.writeFileSync(path.join(tmpDir, 'zfile'), 'x');
    fs.writeFileSync(path.join(tmpDir, 'afile'), 'x');

    const res = await listDirTool.execute({ path: tmpDir }, ctx());
    const rendered = res.output.split('\n').filter(l => l.startsWith('[dir]') || l.startsWith('[file]'));
    assert.deepEqual(rendered, [
      '[dir]  adir/',
      '[dir]  zdir/',
      '[file] afile',
      '[file] zfile',
    ]);
  });

  it('includes hidden dotfiles and dot-directories in the listing', async () => {
    fs.writeFileSync(path.join(tmpDir, '.hidden'), 'x');
    fs.mkdirSync(path.join(tmpDir, '.config'));
    fs.writeFileSync(path.join(tmpDir, 'visible'), 'x');

    const res = await listDirTool.execute({ path: tmpDir }, ctx());
    assert.ok(res.output.includes('[file] .hidden'));
    assert.ok(res.output.includes('[dir]  .config/'));
    assert.ok(res.output.includes('[file] visible'));
    assert.equal(res.output.split('\n')[0], `Contents of ${tmpDir}: (3 entries)`);
  });

  it('reports 0 entries for an empty directory', async () => {
    const empty = path.join(tmpDir, 'empty');
    fs.mkdirSync(empty);
    const res = await listDirTool.execute({ path: empty }, ctx());
    assert.ok(!res.isError);
    assert.equal(res.output.split('\n')[0], `Contents of ${empty}: (0 entries)`);
  });

  it('resolves a relative path against cwd', async () => {
    const sub = path.join(tmpDir, 'nested');
    fs.mkdirSync(sub);
    fs.writeFileSync(path.join(sub, 'f'), 'x');
    const res = await listDirTool.execute({ path: 'nested' }, ctx());
    assert.ok(!res.isError);
    assert.ok(res.output.includes('[file] f'));
    assert.equal(res.output.split('\n')[0], `Contents of ${sub}: (1 entries)`);
  });

  it('truncates to 1000 entries and announces the overflow', async () => {
    const big = path.join(tmpDir, 'big');
    fs.mkdirSync(big);
    for (let i = 0; i < 1005; i++) {
      fs.writeFileSync(path.join(big, `f${String(i).padStart(4, '0')}`), '');
    }
    const res = await listDirTool.execute({ path: big }, ctx());
    assert.ok(!res.isError);
    assert.equal(res.output.split('\n')[0], `Contents of ${big}: (1005 entries)`);
    assert.ok(res.output.includes('Showing first 1000 of 1005 entries'));
    const shown = res.output.split('\n').filter(l => l.startsWith('[file]')).length;
    assert.equal(shown, 1000);
  });
});

describe('list_dir — error paths', () => {
  it('rejects an empty path', async () => {
    const res = await listDirTool.execute({ path: '   ' }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('must not be empty'));
  });

  it('errors when the directory does not exist', async () => {
    const res = await listDirTool.execute({ path: path.join(tmpDir, 'nope') }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('Directory not found'));
  });

  it('errors when the path is a file, not a directory', async () => {
    const f = path.join(tmpDir, 'afile.txt');
    fs.writeFileSync(f, 'x');
    const res = await listDirTool.execute({ path: f }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('is not a directory'));
  });

  it('blocks a path outside the workspace', async () => {
    const outside = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'pov-outside-')));
    try {
      const res = await listDirTool.execute({ path: outside }, ctx());
      assert.equal(res.isError, true);
      assert.ok(res.output.includes('outside the workspace'));
    } finally {
      fs.rmSync(outside, { recursive: true, force: true });
    }
  });

  it('permits an out-of-workspace dir that is a granted read root', async () => {
    const extra = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'pov-root-')));
    fs.writeFileSync(path.join(extra, 'bundled'), 'x');
    try {
      const res = await listDirTool.execute(
        { path: extra },
        ctx({ allowedReadRoots: [extra] }),
      );
      assert.ok(!res.isError, res.output);
      assert.ok(res.output.includes('[file] bundled'));
    } finally {
      fs.rmSync(extra, { recursive: true, force: true });
    }
  });

  it('rejects a symlink that escapes the workspace', async () => {
    const outside = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'pov-escape-')));
    try {
      const link = path.join(tmpDir, 'escape');
      fs.symlinkSync(outside, link);
      const res = await listDirTool.execute({ path: link }, ctx());
      assert.equal(res.isError, true);
      assert.ok(res.output.includes('symlink'));
    } finally {
      fs.rmSync(outside, { recursive: true, force: true });
    }
  });
});

// ===========================================================================
// notebook_edit — behavioral edits reflected on disk
// ===========================================================================

function writeNotebook(
  name: string,
  cells: Array<{ cell_type: string; source: string; id?: string }>,
  nbformat = 4,
  nbformat_minor = 5,
): string {
  const nb = {
    nbformat,
    nbformat_minor,
    metadata: {},
    cells: cells.map(c => ({
      cell_type: c.cell_type,
      source: c.source,
      metadata: {},
      ...(c.id ? { id: c.id } : {}),
      ...(c.cell_type === 'code' ? { execution_count: 7, outputs: [{ x: 1 }] } : {}),
    })),
  };
  const file = path.join(tmpDir, name);
  fs.writeFileSync(file, JSON.stringify(nb));
  return file;
}

function readNotebook(file: string) {
  return JSON.parse(fs.readFileSync(file, 'utf-8'));
}

describe('notebook_edit — replace reflected on disk', () => {
  it('replaces a code cell and resets its execution state', async () => {
    const file = writeNotebook('r.ipynb', [
      { cell_type: 'code', source: 'old' },
      { cell_type: 'markdown', source: '# t' },
    ]);
    const res = await notebookEditTool.execute(
      { notebook_path: file, cell_number: 0, new_source: 'new', edit_mode: 'replace' },
      ctx(),
    );
    assert.ok(!res.isError, res.output);
    assert.ok(res.output.includes('Replaced cell 0'));
    const nb = readNotebook(file);
    assert.equal(nb.cells[0].source, 'new');
    assert.equal(nb.cells[0].execution_count, null);
    assert.deepEqual(nb.cells[0].outputs, []);
    // untouched cell preserved
    assert.equal(nb.cells[1].source, '# t');
  });

  it('writes the notebook back as JSON with one-space indentation', async () => {
    const file = writeNotebook('indent.ipynb', [{ cell_type: 'code', source: 'x' }]);
    await notebookEditTool.execute(
      { notebook_path: file, cell_number: 0, new_source: 'y' },
      ctx(),
    );
    const raw = fs.readFileSync(file, 'utf-8');
    const nb = JSON.parse(raw);
    assert.equal(raw, JSON.stringify(nb, null, 1));
    assert.ok(raw.includes('\n '));
  });
});

describe('notebook_edit — cell_id selection', () => {
  it('selects a cell by its exact id for replace', async () => {
    const file = writeNotebook('byid.ipynb', [
      { cell_type: 'code', source: 'zero', id: 'aaa' },
      { cell_type: 'code', source: 'one', id: 'bbb' },
    ]);
    const res = await notebookEditTool.execute(
      { notebook_path: file, cell_id: 'bbb', new_source: 'patched' },
      ctx(),
    );
    assert.ok(!res.isError, res.output);
    const nb = readNotebook(file);
    assert.equal(nb.cells[1].source, 'patched');
    assert.equal(nb.cells[0].source, 'zero');
  });

  it('supports the cell-N positional alias', async () => {
    const file = writeNotebook('alias.ipynb', [
      { cell_type: 'code', source: 'zero' },
      { cell_type: 'code', source: 'one' },
    ]);
    const res = await notebookEditTool.execute(
      { notebook_path: file, cell_id: 'cell-1', new_source: 'via-alias' },
      ctx(),
    );
    assert.ok(!res.isError, res.output);
    const nb = readNotebook(file);
    assert.equal(nb.cells[1].source, 'via-alias');
  });

  it('errors and lists available cells when cell_id is unknown', async () => {
    const file = writeNotebook('nocell.ipynb', [
      { cell_type: 'code', source: 'zero', id: 'known' },
    ]);
    const res = await notebookEditTool.execute(
      { notebook_path: file, cell_id: 'ghost', new_source: 'x' },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('not found'));
    assert.ok(res.output.includes('known'));
  });

  it('inserts AFTER the matched cell when cell_id is used with insert', async () => {
    const file = writeNotebook('insafter.ipynb', [
      { cell_type: 'code', source: 'zero', id: 'a' },
      { cell_type: 'code', source: 'two', id: 'b' },
    ]);
    const res = await notebookEditTool.execute(
      { notebook_path: file, cell_id: 'a', new_source: 'one', edit_mode: 'insert' },
      ctx(),
    );
    assert.ok(!res.isError, res.output);
    const nb = readNotebook(file);
    assert.equal(nb.cells.length, 3);
    assert.equal(nb.cells[0].source, 'zero');
    assert.equal(nb.cells[1].source, 'one');
    assert.equal(nb.cells[2].source, 'two');
  });
});

describe('notebook_edit — insert and delete reflected on disk', () => {
  it('inserts a markdown cell at an index without execution fields', async () => {
    const file = writeNotebook('ins.ipynb', [{ cell_type: 'code', source: 'a' }]);
    const res = await notebookEditTool.execute(
      { notebook_path: file, cell_number: 0, new_source: '# md', edit_mode: 'insert', cell_type: 'markdown' },
      ctx(),
    );
    assert.ok(!res.isError, res.output);
    const nb = readNotebook(file);
    assert.equal(nb.cells.length, 2);
    assert.equal(nb.cells[0].cell_type, 'markdown');
    assert.equal(nb.cells[0].source, '# md');
    assert.equal(nb.cells[0].execution_count, undefined);
  });

  it('deletes a cell and leaves the rest contiguous', async () => {
    const file = writeNotebook('del.ipynb', [
      { cell_type: 'code', source: 'a' },
      { cell_type: 'code', source: 'b' },
      { cell_type: 'code', source: 'c' },
    ]);
    const res = await notebookEditTool.execute(
      { notebook_path: file, cell_number: 1, new_source: '', edit_mode: 'delete' },
      ctx(),
    );
    assert.ok(!res.isError, res.output);
    assert.ok(res.output.includes('Notebook now has 2 cell(s)'));
    const nb = readNotebook(file);
    assert.deepEqual(nb.cells.map((c: { source: string }) => c.source), ['a', 'c']);
  });
});

describe('notebook_edit — read-before-write gate', () => {
  it('blocks an edit when filesRead is tracked but the notebook was not read', async () => {
    const file = writeNotebook('gate.ipynb', [{ cell_type: 'code', source: 'a' }]);
    const res = await notebookEditTool.execute(
      { notebook_path: file, cell_number: 0, new_source: 'b' },
      ctx({ filesRead: new Set<string>() }),
    );
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('must read'));
    // on-disk content untouched
    assert.equal(readNotebook(file).cells[0].source, 'a');
  });

  it('allows the edit once the resolved path is in filesRead', async () => {
    const file = writeNotebook('gate2.ipynb', [{ cell_type: 'code', source: 'a' }]);
    const resolved = fs.realpathSync(file);
    const res = await notebookEditTool.execute(
      { notebook_path: file, cell_number: 0, new_source: 'b' },
      ctx({ filesRead: new Set<string>([resolved]) }),
    );
    assert.ok(!res.isError, res.output);
    assert.equal(readNotebook(file).cells[0].source, 'b');
  });
});

describe('notebook_edit — validation guards', () => {
  it('rejects a non-.ipynb path', async () => {
    const f = path.join(tmpDir, 'nb.txt');
    fs.writeFileSync(f, '{}');
    const res = await notebookEditTool.execute({ notebook_path: f, new_source: 'x' }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('.ipynb'));
  });

  it('rejects an invalid edit_mode', async () => {
    const file = writeNotebook('bad.ipynb', [{ cell_type: 'code', source: 'a' }]);
    const res = await notebookEditTool.execute(
      { notebook_path: file, new_source: 'x', edit_mode: 'frobnicate' },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('edit_mode'));
  });

  it('rejects a corrupt notebook that is not valid JSON', async () => {
    const f = path.join(tmpDir, 'corrupt.ipynb');
    fs.writeFileSync(f, 'definitely not json');
    const res = await notebookEditTool.execute({ notebook_path: f, new_source: 'x' }, ctx());
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('not valid JSON'));
  });
});
