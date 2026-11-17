import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { notebookEditTool } from '../src/tools/notebook-edit';
import { toolDefinitionsToOpenAI } from '../src/provider';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// U098-w76 — NotebookEditTool parity:
//  - wrong-file-type error redirects to file_edit (gap: errorCode-2 affordance)
//  - notebook language derived from metadata.language_info (+ cell id surfaced)
//  - structured outputSchema + ToolResult.metadata attribution (original/updated
//    file, language, cell_language, edit_mode, cell_id) advertised via the provider
//  - per-cell highlight language (markdown vs the notebook language)
// ---------------------------------------------------------------------------

let tmpDir: string;
let nbPath: string;

function ctx(): ToolContext {
  return { cwd: tmpDir, abortSignal: new AbortController().signal, filesRead: new Set([nbPath]) };
}

interface CellSpec { cell_type: string; source: string[]; id?: string }

function writeNotebook(cells: CellSpec[], languageInfoName?: string): void {
  fs.writeFileSync(nbPath, JSON.stringify({
    nbformat: 4,
    nbformat_minor: 5,
    metadata: languageInfoName ? { language_info: { name: languageInfoName } } : {},
    cells: cells.map(c => ({ ...c, metadata: {}, execution_count: null, outputs: [] })),
  }, null, 1));
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-nb-w76-'));
  nbPath = path.join(tmpDir, 'nb.ipynb');
});
afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

describe('notebook_edit – wrong file-type redirects to file_edit (U098-w76)', () => {
  it('appends a file_edit recovery hint for a non-.ipynb path', async () => {
    const txt = path.join(tmpDir, 'notes.txt');
    fs.writeFileSync(txt, 'hello\n');
    const result = await notebookEditTool.execute(
      { notebook_path: txt, new_source: 'x', cell_number: 0 },
      { cwd: tmpDir, abortSignal: new AbortController().signal, filesRead: new Set([txt]) },
    );
    assert.ok(result.isError, result.output);
    assert.ok(/must be a Jupyter notebook/i.test(result.output), result.output);
    // The affordance hint — the model is told exactly how to recover (parity).
    assert.ok(/use the file_edit tool/i.test(result.output), result.output);
  });
});

describe('notebook_edit – language derivation + cell id (U098-w76)', () => {
  it('reports the notebook language from metadata.language_info on replace', async () => {
    writeNotebook([{ cell_type: 'code', source: ['x = 1\n'], id: 'c0' }], 'julia');
    const result = await notebookEditTool.execute(
      { notebook_path: nbPath, cell_number: 0, new_source: 'x = 2\n' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.ok(result.output.includes('language: julia'), result.output);
    // Cell id surfaced so the model can correlate the edit.
    assert.ok(result.output.includes('cell id: c0'), result.output);
    assert.equal((result.metadata as Record<string, unknown>).language, 'julia');
    assert.equal((result.metadata as Record<string, unknown>).cell_id, 'c0');
  });

  it("defaults the language to 'python' when language_info is absent", async () => {
    writeNotebook([{ cell_type: 'code', source: ['x = 1\n'], id: 'c0' }]);
    const result = await notebookEditTool.execute(
      { notebook_path: nbPath, cell_number: 0, new_source: 'x = 9\n' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.ok(result.output.includes('language: python'), result.output);
    assert.equal((result.metadata as Record<string, unknown>).language, 'python');
  });
});

describe('notebook_edit – structured metadata attribution (U098-w76)', () => {
  it('carries before/after content + resolved fields on replace', async () => {
    writeNotebook([{ cell_type: 'code', source: ['x = 1\n'], id: 'c0' }], 'python');
    const before = fs.readFileSync(nbPath, 'utf-8');
    const result = await notebookEditTool.execute(
      { notebook_path: nbPath, cell_number: 0, new_source: 'x = 42\n' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    const md = result.metadata as Record<string, unknown>;
    assert.equal(md.edit_mode, 'replace');
    assert.equal(md.cell_type, 'code');
    assert.equal(md.cell_language, 'python');
    assert.equal(md.cell_id, 'c0');
    assert.equal(md.error, '');
    assert.equal(md.new_source, 'x = 42\n');
    assert.equal(md.original_file, before);
    assert.equal(md.notebook_path, fs.realpathSync(nbPath));
    // updated_file reflects the post-edit notebook (the new source is present).
    assert.ok(typeof md.updated_file === 'string' && (md.updated_file as string).includes('x = 42'), String(md.updated_file));
    assert.notEqual(md.updated_file, md.original_file);
  });

  it('surfaces the generated cell id + insert mode on insert', async () => {
    writeNotebook([{ cell_type: 'code', source: ['a = 1\n'], id: 'c0' }]);
    const result = await notebookEditTool.execute(
      { notebook_path: nbPath, edit_mode: 'insert', new_source: '# heading\n', cell_type: 'markdown' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    const md = result.metadata as Record<string, unknown>;
    assert.equal(md.edit_mode, 'insert');
    assert.equal(md.cell_type, 'markdown');
    // A markdown cell highlights as markdown, not the notebook language (gap 5).
    assert.equal(md.cell_language, 'markdown');
    assert.ok(typeof md.cell_id === 'string' && (md.cell_id as string).length > 0, String(md.cell_id));
    assert.ok(result.output.includes(`cell id: ${md.cell_id}`), result.output);
  });

  it('reports the deleted cell id + empty new_source on delete', async () => {
    writeNotebook([
      { cell_type: 'code', source: ['keep = 0\n'], id: 'c0' },
      { cell_type: 'markdown', source: ['# gone\n'], id: 'c1' },
    ]);
    const result = await notebookEditTool.execute(
      { notebook_path: nbPath, cell_id: 'c1', edit_mode: 'delete', new_source: '' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    const md = result.metadata as Record<string, unknown>;
    assert.equal(md.edit_mode, 'delete');
    assert.equal(md.cell_id, 'c1');
    assert.equal(md.cell_type, 'markdown');
    assert.equal(md.new_source, '');
  });
});

describe('notebook_edit – outputSchema advertised to the provider (U098-w76)', () => {
  it('declares an object-rooted outputSchema that the provider exposes', () => {
    assert.equal(notebookEditTool.outputSchema?.type, 'object');
    const [out] = toolDefinitionsToOpenAI([notebookEditTool]);
    assert.deepEqual(out.function.outputSchema, notebookEditTool.outputSchema);
    // The attribution fields are part of the declared contract.
    const props = (notebookEditTool.outputSchema as { properties: Record<string, unknown> }).properties;
    for (const key of ['original_file', 'updated_file', 'language', 'cell_language', 'edit_mode', 'cell_id']) {
      assert.ok(key in props, `outputSchema missing ${key}`);
    }
  });
});
