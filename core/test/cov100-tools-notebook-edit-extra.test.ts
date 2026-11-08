import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { notebookEditTool } from '../src/tools/notebook-edit';
import type { ToolContext } from '../src/types';

// Targets the uncovered first OR-arm of the cell-id generation guard on
// notebook-edit.ts line 242: `notebook.nbformat > 4`. Existing tests only
// use nbformat 4 (so only the second arm `nbformat === 4 && minor >= 5`
// ever evaluates true). A notebook with nbformat 5 drives the first arm true
// even when nbformat_minor is 0, so an inserted cell must still get an id.

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

function writeRawNotebook(name: string, nb: unknown): string {
  const file = path.join(tmpDir, name);
  fs.writeFileSync(file, JSON.stringify(nb));
  return file;
}

function readNotebook(file: string) {
  return JSON.parse(fs.readFileSync(file, 'utf-8'));
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-notebook-cov-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('notebookEditTool – nbformat > 4 id generation (line 242 first OR arm)', () => {
  it('generates a cell id on insert when nbformat is 5 even though nbformat_minor is 0', async () => {
    // nbformat 5, minor 0: the SECOND arm (=== 4 && minor >= 5) is false,
    // so only the first arm `nbformat > 4` can make the guard true.
    const file = writeRawNotebook('v5.ipynb', {
      nbformat: 5,
      nbformat_minor: 0,
      metadata: {},
      cells: [
        { cell_type: 'code', source: 'a = 1', metadata: {}, execution_count: null, outputs: [] },
      ],
    });

    const result = await notebookEditTool.execute(
      { notebook_path: file, new_source: 'b = 2', edit_mode: 'insert' },
      ctx(),
    );

    assert.ok(!result.isError, `Expected success, got: ${result.output}`);

    const nb = readNotebook(file);
    assert.equal(nb.cells.length, 2, 'cell should be appended');
    const inserted = nb.cells[1];
    assert.equal(inserted.source, 'b = 2');
    assert.equal(typeof inserted.id, 'string', 'inserted cell must receive a generated id');
    assert.ok(inserted.id.length > 0, 'generated id must be non-empty');
  });

  it('generates a cell id on auto-insert replace when nbformat is 5 and minor 0', async () => {
    // Out-of-range replace auto-converts to insert; the same first OR arm
    // governs id generation on the replace path (source line 271-273).
    const file = writeRawNotebook('v5replace.ipynb', {
      nbformat: 5,
      nbformat_minor: 0,
      metadata: {},
      cells: [
        { cell_type: 'code', source: 'a = 1', metadata: {}, execution_count: null, outputs: [] },
      ],
    });

    const result = await notebookEditTool.execute(
      { notebook_path: file, cell_number: 5, new_source: 'appended', edit_mode: 'replace' },
      ctx(),
    );

    assert.ok(!result.isError, `Expected auto-insert success, got: ${result.output}`);

    const nb = readNotebook(file);
    const inserted = nb.cells[nb.cells.length - 1];
    assert.equal(inserted.source, 'appended');
    assert.equal(typeof inserted.id, 'string', 'auto-inserted cell must receive a generated id');
    assert.ok(inserted.id.length > 0, 'generated id must be non-empty');
  });
});
