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
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-nb-diff-'));
  nbPath = path.join(tmpDir, 'nb.ipynb');
});
afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

describe('notebook_edit – cell diff preview on replace', () => {
  it('appends a unified diff of old vs new cell source', async () => {
    writeNotebook([{ cell_type: 'code', source: ['x = 1\n', 'print(x)\n'], id: 'c0' }]);
    const result = await notebookEditTool.execute(
      { notebook_path: nbPath, cell_number: 0, new_source: 'x = 2\nprint(x)\n' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.ok(/Replaced cell 0/.test(result.output), result.output);
    // The diff must show the changed line, not the whole file as -/+.
    assert.ok(result.output.includes('--- cell 0'), result.output);
    assert.ok(result.output.includes('-x = 1'), result.output);
    assert.ok(result.output.includes('+x = 2'), result.output);
    // Unchanged line stays as context (leading space), not removed+added.
    assert.ok(!result.output.includes('-print(x)'), result.output);
  });

  it('omits the diff block when the new source is identical', async () => {
    writeNotebook([{ cell_type: 'code', source: ['same\n'], id: 'c0' }]);
    const result = await notebookEditTool.execute(
      { notebook_path: nbPath, cell_number: 0, new_source: 'same\n' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.ok(!result.output.includes('@@'), result.output);
    assert.ok(!result.output.includes('no changes detected'), result.output);
  });

  it('does not emit a diff for insert mode', async () => {
    writeNotebook([{ cell_type: 'code', source: ['a\n'], id: 'c0' }]);
    const result = await notebookEditTool.execute(
      { notebook_path: nbPath, edit_mode: 'insert', new_source: 'b\n' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.ok(/Inserted new/.test(result.output), result.output);
    assert.ok(!result.output.includes('@@'), result.output);
  });
});
