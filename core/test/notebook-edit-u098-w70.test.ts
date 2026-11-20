import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { notebookEditTool } from '../src/tools/notebook-edit';
import type { ToolContext } from '../src/types';

let tmpDir: string;
let nbPath: string;

function ctx(): ToolContext {
  return { cwd: tmpDir, abortSignal: new AbortController().signal, filesRead: new Set([nbPath]) };
}

function writeNotebook(cells: Array<{ cell_type: string; source: string[]; id?: string }>): void {
  fs.writeFileSync(nbPath, JSON.stringify({
    nbformat: 4,
    nbformat_minor: 5,
    metadata: {},
    cells: cells.map(c => ({ ...c, metadata: {}, execution_count: null, outputs: [] })),
  }, null, 1));
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-nb-w70-'));
  nbPath = path.join(tmpDir, 'nb.ipynb');
});
afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

describe('notebook_edit – per-mode cell preview (U098-w70)', () => {
  it('surfaces the NEW cell source on insert (distinct from a replace diff)', async () => {
    writeNotebook([{ cell_type: 'code', source: ['a = 1\n'], id: 'c0' }]);
    const result = await notebookEditTool.execute(
      { notebook_path: nbPath, edit_mode: 'insert', new_source: 'b = 2\nprint(b)\n', cell_type: 'code' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.ok(/Inserted new/.test(result.output), result.output);
    // The inserted cell body is surfaced to the model/user.
    assert.ok(result.output.includes('Inserted cell source:'), result.output);
    assert.ok(result.output.includes('b = 2'), result.output);
    assert.ok(result.output.includes('print(b)'), result.output);
    // Insert is NOT a unified diff (parity: the reference highlights the source, no @@ hunks).
    assert.ok(!result.output.includes('@@'), result.output);
  });

  it('surfaces the DELETED cell source on delete', async () => {
    writeNotebook([
      { cell_type: 'code', source: ['keep = 0\n'], id: 'c0' },
      { cell_type: 'markdown', source: ['# gone\n', 'bye\n'], id: 'c1' },
    ]);
    const result = await notebookEditTool.execute(
      { notebook_path: nbPath, cell_id: 'c1', edit_mode: 'delete', new_source: '' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.ok(/Deleted cell 1/.test(result.output), result.output);
    // The removed cell body is surfaced so the user can see what was deleted.
    assert.ok(result.output.includes('Deleted cell source:'), result.output);
    assert.ok(result.output.includes('# gone'), result.output);
    assert.ok(result.output.includes('bye'), result.output);
    assert.ok(!result.output.includes('@@'), result.output);
  });

  it('still emits a unified diff (not a source block) on replace', async () => {
    writeNotebook([{ cell_type: 'code', source: ['x = 1\n'], id: 'c0' }]);
    const result = await notebookEditTool.execute(
      { notebook_path: nbPath, cell_number: 0, new_source: 'x = 2\n' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.ok(/Replaced cell 0/.test(result.output), result.output);
    assert.ok(result.output.includes('-x = 1'), result.output);
    assert.ok(result.output.includes('+x = 2'), result.output);
    // Replace keeps the structured diff, not the plain source-preview headers.
    assert.ok(!result.output.includes('Inserted cell source:'), result.output);
    assert.ok(!result.output.includes('Deleted cell source:'), result.output);
  });

  it('shows the inserted source when a replace at/after the end auto-converts to insert', async () => {
    writeNotebook([{ cell_type: 'code', source: ['only\n'], id: 'c0' }]);
    const result = await notebookEditTool.execute(
      { notebook_path: nbPath, cell_number: 5, new_source: 'appended = True\n', cell_type: 'code' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.ok(/Inserted new/.test(result.output), result.output);
    assert.ok(result.output.includes('Inserted cell source:'), result.output);
    assert.ok(result.output.includes('appended = True'), result.output);
  });
});
