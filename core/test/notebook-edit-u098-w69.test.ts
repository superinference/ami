import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { notebookEditTool } from '../src/tools/notebook-edit';
import { trackFileHistory, FILE_HISTORY_MAX_SNAPSHOTS } from '../src/tools/file-history';
import type { ToolContext } from '../src/types';

// U098 (wave 69) — additive parity improvements for notebook_edit:
//   - pre-edit file-history snapshot (recovery/undo fallback) via shared helper
//   - strict input schema (additionalProperties:false) rejects unknown params
//   - searchHint discovery metadata ("jupyter"/"ipynb")
//   - enriched edit_mode semantics in the schema description

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

interface Cell { cell_type: string; source: string | string[]; id?: string }

function notebookJson(cells: Cell[], nbformat = 4, nbformat_minor = 5): string {
  const nb = {
    nbformat,
    nbformat_minor,
    metadata: {},
    cells: cells.map(c => ({
      cell_type: c.cell_type,
      source: c.source,
      metadata: {},
      ...(c.id ? { id: c.id } : {}),
      ...(c.cell_type === 'code' ? { execution_count: null, outputs: [] } : {}),
    })),
  };
  return JSON.stringify(nb, null, 1);
}

function writeNotebook(name: string, cells: Cell[]): string {
  const file = path.join(tmpDir, name);
  fs.writeFileSync(file, notebookJson(cells));
  return file;
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-nb-w69-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('notebook_edit w69 — pre-edit file-history snapshot', () => {
  it('writes the ORIGINAL notebook content to .ami/file-history before a replace', async () => {
    const original = notebookJson([{ cell_type: 'code', source: 'print(1)' }]);
    const file = writeNotebook('nb.ipynb', [{ cell_type: 'code', source: 'print(1)' }]);

    const res = await notebookEditTool.execute(
      { notebook_path: file, cell_number: 0, new_source: 'print(2)' },
      ctx(),
    );
    assert.equal(res.isError, undefined);

    const histDir = path.join(tmpDir, '.ami', 'file-history');
    const entries = fs.readdirSync(histDir).filter(e => e.startsWith('nb.ipynb.') && e.endsWith('.bak'));
    assert.equal(entries.length, 1, 'exactly one snapshot written');
    // Snapshot holds the pre-edit content, NOT the new content.
    const snap = fs.readFileSync(path.join(histDir, entries[0]), 'utf-8');
    assert.equal(snap, original);
    assert.match(snap, /print\(1\)/);
    assert.doesNotMatch(snap, /print\(2\)/);
  });

  it('snapshots before delete as well', async () => {
    const file = writeNotebook('d.ipynb', [
      { cell_type: 'code', source: 'a' },
      { cell_type: 'code', source: 'b' },
    ]);
    await notebookEditTool.execute(
      { notebook_path: file, cell_number: 1, new_source: '', edit_mode: 'delete' },
      ctx(),
    );
    const histDir = path.join(tmpDir, '.ami', 'file-history');
    const entries = fs.readdirSync(histDir).filter(e => e.endsWith('.bak'));
    assert.equal(entries.length, 1);
  });

  it('shared trackFileHistory prunes beyond FILE_HISTORY_MAX_SNAPSHOTS', async () => {
    for (let i = 0; i < FILE_HISTORY_MAX_SNAPSHOTS + 5; i++) {
      await trackFileHistory(path.join(tmpDir, 'x.ipynb'), `v${i}`, tmpDir);
      // ensure distinct timestamps so sort order is stable
      await new Promise(r => setTimeout(r, 2));
    }
    const histDir = path.join(tmpDir, '.ami', 'file-history');
    const entries = fs.readdirSync(histDir).filter(e => e.endsWith('.bak'));
    assert.equal(entries.length, FILE_HISTORY_MAX_SNAPSHOTS);
  });
});

describe('notebook_edit w69 — strict input schema', () => {
  it('declares additionalProperties:false so unknown params are rejected by the validator', () => {
    assert.equal(notebookEditTool.inputSchema.additionalProperties, false);
  });
});

describe('notebook_edit w69 — discovery + description metadata', () => {
  it('exposes a searchHint mentioning jupyter/ipynb', () => {
    assert.ok(notebookEditTool.searchHint, 'searchHint present');
    assert.match(notebookEditTool.searchHint!, /jupyter/);
    assert.match(notebookEditTool.searchHint!, /ipynb/);
  });

  it('edit_mode description teaches insert-after + cell_type-required semantics', () => {
    const desc = (notebookEditTool.inputSchema.properties!.edit_mode as { description: string }).description;
    assert.match(desc, /after/i);
    assert.match(desc, /requires cell_type/i);
    assert.match(desc, /end of the notebook/i);
  });
});
