import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { notebookEditTool } from '../src/tools/notebook-edit';
import type { ToolContext } from '../src/types';

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

function writeNotebook(name: string, cells: Array<{ cell_type: string; source: string }>, nbformat = 4, nbformat_minor = 5): string {
  const nb = {
    nbformat,
    nbformat_minor,
    metadata: {},
    cells: cells.map(c => ({
      cell_type: c.cell_type,
      source: c.source,
      metadata: {},
      ...(c.cell_type === 'code' ? { execution_count: null, outputs: [] } : {}),
    })),
  };
  const file = path.join(tmpDir, name);
  fs.writeFileSync(file, JSON.stringify(nb));
  return file;
}

function readNotebook(file: string) {
  return JSON.parse(fs.readFileSync(file, 'utf-8'));
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-notebook-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Tool definition
// ---------------------------------------------------------------------------

describe('notebookEditTool – definition', () => {
  it('has the correct name', () => {
    assert.equal(notebookEditTool.name, 'notebook_edit');
  });

  it('is not read-only', () => {
    assert.equal(notebookEditTool.isReadOnly, false);
  });

  it('schema requires notebook_path and new_source', () => {
    const req = notebookEditTool.inputSchema.required;
    assert.ok(req?.includes('notebook_path'));
    assert.ok(req?.includes('new_source'));
  });
});

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

describe('notebookEditTool – validation', () => {
  it('rejects empty notebook_path', async () => {
    const result = await notebookEditTool.execute(
      { notebook_path: '', new_source: 'x' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('must not be empty'));
  });

  it('rejects invalid edit_mode', async () => {
    const file = writeNotebook('test.ipynb', [{ cell_type: 'code', source: 'x' }]);
    const result = await notebookEditTool.execute(
      { notebook_path: file, new_source: 'x', edit_mode: 'invalid' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('edit_mode'));
  });

  it('rejects non-.ipynb files', async () => {
    const file = path.join(tmpDir, 'test.txt');
    fs.writeFileSync(file, '{}');
    const result = await notebookEditTool.execute(
      { notebook_path: file, new_source: 'x' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('.ipynb'));
  });

  it('rejects non-existent notebook', async () => {
    const result = await notebookEditTool.execute(
      { notebook_path: path.join(tmpDir, 'nope.ipynb'), new_source: 'x' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('not found'));
  });

  it('rejects invalid JSON', async () => {
    const file = path.join(tmpDir, 'bad.ipynb');
    fs.writeFileSync(file, 'not json');
    const result = await notebookEditTool.execute(
      { notebook_path: file, new_source: 'x' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('not valid JSON'));
  });

  it('rejects notebook without cells array', async () => {
    const file = path.join(tmpDir, 'nocells.ipynb');
    fs.writeFileSync(file, JSON.stringify({ nbformat: 4, metadata: {} }));
    const result = await notebookEditTool.execute(
      { notebook_path: file, new_source: 'x' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('missing "cells"'));
  });
});

// ---------------------------------------------------------------------------
// Replace mode
// ---------------------------------------------------------------------------

describe('notebookEditTool – replace', () => {
  it('replaces cell content at specified index', async () => {
    const file = writeNotebook('replace.ipynb', [
      { cell_type: 'code', source: 'old code' },
      { cell_type: 'markdown', source: '# Title' },
    ]);

    const result = await notebookEditTool.execute(
      { notebook_path: file, cell_number: 0, new_source: 'new code', edit_mode: 'replace' },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('Replaced cell 0'));

    const nb = readNotebook(file);
    assert.equal(nb.cells[0].source, 'new code');
    assert.equal(nb.cells[0].execution_count, null);
    assert.deepEqual(nb.cells[0].outputs, []);
  });

  it('defaults to cell 0 when cell_number not specified', async () => {
    const file = writeNotebook('default.ipynb', [
      { cell_type: 'code', source: 'original' },
    ]);

    const result = await notebookEditTool.execute(
      { notebook_path: file, new_source: 'replaced' },
      ctx(),
    );
    assert.ok(!result.isError);
    const nb = readNotebook(file);
    assert.equal(nb.cells[0].source, 'replaced');
  });

  it('auto-converts out-of-range replace to insert at end', async () => {
    const file = writeNotebook('oor.ipynb', [
      { cell_type: 'code', source: 'x' },
    ]);
    const result = await notebookEditTool.execute(
      { notebook_path: file, cell_number: 5, new_source: 'y', edit_mode: 'replace' },
      ctx(),
    );
    // Out-of-range replace is auto-converted to insert
    assert.ok(!result.isError, `Expected success (auto-insert), got: ${result.output}`);
    assert.ok(result.output.includes('Inserted'));
    const nb = readNotebook(file);
    assert.equal(nb.cells.length, 2);
    assert.equal(nb.cells[1].source, 'y');
  });

  it('can change cell type during replace', async () => {
    const file = writeNotebook('changetype.ipynb', [
      { cell_type: 'code', source: 'x = 1' },
    ]);
    const result = await notebookEditTool.execute(
      { notebook_path: file, cell_number: 0, new_source: '# Header', cell_type: 'markdown', edit_mode: 'replace' },
      ctx(),
    );
    assert.ok(!result.isError);
    const nb = readNotebook(file);
    assert.equal(nb.cells[0].cell_type, 'markdown');
    assert.equal(nb.cells[0].source, '# Header');
  });
});

// ---------------------------------------------------------------------------
// Insert mode
// ---------------------------------------------------------------------------

describe('notebookEditTool – insert', () => {
  it('inserts a new cell at specified index', async () => {
    const file = writeNotebook('insert.ipynb', [
      { cell_type: 'code', source: 'first' },
      { cell_type: 'code', source: 'second' },
    ]);

    const result = await notebookEditTool.execute(
      { notebook_path: file, cell_number: 1, new_source: 'inserted', edit_mode: 'insert', cell_type: 'code' },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('Inserted'));

    const nb = readNotebook(file);
    assert.equal(nb.cells.length, 3);
    assert.equal(nb.cells[1].source, 'inserted');
  });

  it('inserts at end when cell_number not specified', async () => {
    const file = writeNotebook('insertend.ipynb', [
      { cell_type: 'code', source: 'first' },
    ]);

    const result = await notebookEditTool.execute(
      { notebook_path: file, new_source: 'appended', edit_mode: 'insert' },
      ctx(),
    );
    assert.ok(!result.isError);
    const nb = readNotebook(file);
    assert.equal(nb.cells.length, 2);
    assert.equal(nb.cells[1].source, 'appended');
  });

  it('defaults to code cell type', async () => {
    const file = writeNotebook('insertdefault.ipynb', [
      { cell_type: 'code', source: 'x' },
    ]);
    const result = await notebookEditTool.execute(
      { notebook_path: file, new_source: 'y = 1', edit_mode: 'insert' },
      ctx(),
    );
    assert.ok(!result.isError);
    const nb = readNotebook(file);
    assert.equal(nb.cells[1].cell_type, 'code');
    assert.equal(nb.cells[1].execution_count, null);
    assert.deepEqual(nb.cells[1].outputs, []);
  });

  it('generates cell id for nbformat >= 4.5', async () => {
    const file = writeNotebook('withid.ipynb', [
      { cell_type: 'code', source: 'x' },
    ], 4, 5);
    const result = await notebookEditTool.execute(
      { notebook_path: file, new_source: 'new', edit_mode: 'insert' },
      ctx(),
    );
    assert.ok(!result.isError);
    const nb = readNotebook(file);
    assert.ok(nb.cells[1].id, 'Inserted cell should have an id');
  });

  it('does NOT generate cell id for nbformat < 4.5', async () => {
    const file = writeNotebook('noid.ipynb', [
      { cell_type: 'code', source: 'x' },
    ], 4, 4);
    const result = await notebookEditTool.execute(
      { notebook_path: file, new_source: 'new', edit_mode: 'insert' },
      ctx(),
    );
    assert.ok(!result.isError);
    const nb = readNotebook(file);
    assert.equal(nb.cells[1].id, undefined);
  });

  it('inserts markdown cell when specified', async () => {
    const file = writeNotebook('insertmd.ipynb', [
      { cell_type: 'code', source: 'x' },
    ]);
    const result = await notebookEditTool.execute(
      { notebook_path: file, new_source: '# Hello', edit_mode: 'insert', cell_type: 'markdown' },
      ctx(),
    );
    assert.ok(!result.isError);
    const nb = readNotebook(file);
    assert.equal(nb.cells[1].cell_type, 'markdown');
    // Markdown cells should NOT have execution_count or outputs
    assert.equal(nb.cells[1].execution_count, undefined);
  });

  it('rejects out-of-range cell_number for insert', async () => {
    const file = writeNotebook('insoor.ipynb', [
      { cell_type: 'code', source: 'x' },
    ]);
    const result = await notebookEditTool.execute(
      { notebook_path: file, cell_number: 10, new_source: 'y', edit_mode: 'insert' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('out of range'));
  });
});

// ---------------------------------------------------------------------------
// Delete mode
// ---------------------------------------------------------------------------

describe('notebookEditTool – delete', () => {
  it('deletes a cell at specified index', async () => {
    const file = writeNotebook('delete.ipynb', [
      { cell_type: 'code', source: 'first' },
      { cell_type: 'code', source: 'second' },
      { cell_type: 'code', source: 'third' },
    ]);

    const result = await notebookEditTool.execute(
      { notebook_path: file, cell_number: 1, new_source: '', edit_mode: 'delete' },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('Deleted cell 1'));

    const nb = readNotebook(file);
    assert.equal(nb.cells.length, 2);
    assert.equal(nb.cells[0].source, 'first');
    assert.equal(nb.cells[1].source, 'third');
  });

  it('rejects out-of-range cell_number for delete', async () => {
    const file = writeNotebook('deloor.ipynb', [
      { cell_type: 'code', source: 'x' },
    ]);
    const result = await notebookEditTool.execute(
      { notebook_path: file, cell_number: 5, new_source: '', edit_mode: 'delete' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('out of range'));
  });

  it('rejects negative cell_number for delete', async () => {
    const file = writeNotebook('delneg.ipynb', [
      { cell_type: 'code', source: 'x' },
    ]);
    const result = await notebookEditTool.execute(
      { notebook_path: file, cell_number: -1, new_source: '', edit_mode: 'delete' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('out of range'));
  });
});

// ---------------------------------------------------------------------------
// Relative paths
// ---------------------------------------------------------------------------

describe('notebookEditTool – relative paths', () => {
  it('resolves relative notebook path against cwd', async () => {
    const file = writeNotebook('relative.ipynb', [
      { cell_type: 'code', source: 'original' },
    ]);
    const result = await notebookEditTool.execute(
      { notebook_path: 'relative.ipynb', cell_number: 0, new_source: 'updated' },
      ctx(),
    );
    assert.ok(!result.isError);
    const nb = readNotebook(file);
    assert.equal(nb.cells[0].source, 'updated');
  });
});
