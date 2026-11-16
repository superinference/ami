import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { notebookEditTool } from '../src/tools/notebook-edit';
import type { ToolContext } from '../src/types';

// U098 — parity improvements for core/core/src/tools/notebook-edit.ts:
//   G1  require cell_type on edit_mode=insert (Claude errorCode 5)
//   G2  cell_id resolution order (real id wins over positional cell-N alias;
//       alias regex strictly anchored /^cell-(\d+)$/)
//   G4  preserve original encoding (BOM/UTF-16) + line endings on write
//   G5  enriched schema descriptions (absolute path, insert-required cell_type, …)
//   G6  durable fsync-flushed atomic write (no temp files left behind)

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

function readNotebook(file: string) {
  return JSON.parse(fs.readFileSync(file).toString('utf-8').replace(/^﻿/, ''));
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-nb-u098-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// G1 — cell_type required for insert
// ---------------------------------------------------------------------------

describe('notebook_edit U098 — cell_type required on insert', () => {
  it('rejects insert when cell_type is omitted', async () => {
    const file = writeNotebook('ins.ipynb', [{ cell_type: 'code', source: 'x' }]);
    const res = await notebookEditTool.execute(
      { notebook_path: file, new_source: 'y', edit_mode: 'insert' },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.match(res.output, /cell_type is required when using edit_mode=insert/);
    // The notebook must be untouched.
    assert.equal(readNotebook(file).cells.length, 1);
  });

  it('allows insert when cell_type is provided', async () => {
    const file = writeNotebook('ins2.ipynb', [{ cell_type: 'code', source: 'x' }]);
    const res = await notebookEditTool.execute(
      { notebook_path: file, new_source: '# md', edit_mode: 'insert', cell_type: 'markdown' },
      ctx(),
    );
    assert.ok(!res.isError, res.output);
    const nb = readNotebook(file);
    assert.equal(nb.cells.length, 2);
    assert.equal(nb.cells[1].cell_type, 'markdown');
  });
});

// ---------------------------------------------------------------------------
// G2 — cell_id resolution order
// ---------------------------------------------------------------------------

describe('notebook_edit U098 — cell_id resolution order', () => {
  it('matches the real cell id before the positional alias', async () => {
    // index 0 has an unrelated id; index 1 has the id "cell-0", which also
    // looks like the positional alias for index 0. The real-id match must win,
    // so selecting "cell-0" edits index 1, NOT index 0.
    const file = writeNotebook('order.ipynb', [
      { cell_type: 'code', source: 'zero', id: 'realzero' },
      { cell_type: 'code', source: 'one', id: 'cell-0' },
    ]);
    const res = await notebookEditTool.execute(
      { notebook_path: file, cell_id: 'cell-0', new_source: 'patched' },
      ctx(),
    );
    assert.ok(!res.isError, res.output);
    const nb = readNotebook(file);
    assert.equal(nb.cells[1].source, 'patched');
    assert.equal(nb.cells[0].source, 'zero');
  });

  it('falls back to the strict cell-N alias only for anchored matches', async () => {
    const file = writeNotebook('alias.ipynb', [
      { cell_type: 'code', source: 'zero' },
      { cell_type: 'code', source: 'one' },
    ]);
    const res = await notebookEditTool.execute(
      { notebook_path: file, cell_id: 'cell-1', new_source: 'via-alias' },
      ctx(),
    );
    assert.ok(!res.isError, res.output);
    assert.equal(readNotebook(file).cells[1].source, 'via-alias');
  });

  it('does not mis-parse a non-anchored cell-like id as an index', async () => {
    const file = writeNotebook('nonanchor.ipynb', [
      { cell_type: 'code', source: 'zero', id: 'keep' },
    ]);
    const res = await notebookEditTool.execute(
      { notebook_path: file, cell_id: 'cell-2x', new_source: 'nope' },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.match(res.output, /not found/);
    assert.match(res.output, /keep/);
    assert.equal(readNotebook(file).cells[0].source, 'zero');
  });

  it('errors when the cell-N alias index is out of range', async () => {
    const file = writeNotebook('oor.ipynb', [{ cell_type: 'code', source: 'zero' }]);
    const res = await notebookEditTool.execute(
      { notebook_path: file, cell_id: 'cell-9', new_source: 'nope' },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.match(res.output, /does not exist/);
  });
});

// ---------------------------------------------------------------------------
// G4 — encoding + line-ending preservation
// ---------------------------------------------------------------------------

describe('notebook_edit U098 — encoding and line-ending preservation', () => {
  it('preserves CRLF line endings on write', async () => {
    const file = path.join(tmpDir, 'crlf.ipynb');
    const lf = notebookJson([{ cell_type: 'code', source: 'old' }]);
    fs.writeFileSync(file, lf.replace(/\n/g, '\r\n'));

    const res = await notebookEditTool.execute(
      { notebook_path: file, cell_number: 0, new_source: 'new' },
      ctx(),
    );
    assert.ok(!res.isError, res.output);
    const bytes = fs.readFileSync(file).toString('utf-8');
    assert.ok(bytes.includes('\r\n'), 'CRLF line endings must be preserved');
    assert.equal(readNotebook(file).cells[0].source, 'new');
  });

  it('preserves a UTF-8 BOM on write', async () => {
    const file = path.join(tmpDir, 'bom.ipynb');
    const json = notebookJson([{ cell_type: 'code', source: 'old' }]);
    fs.writeFileSync(file, Buffer.concat([Buffer.from([0xEF, 0xBB, 0xBF]), Buffer.from(json, 'utf-8')]));

    const res = await notebookEditTool.execute(
      { notebook_path: file, cell_number: 0, new_source: 'new' },
      ctx(),
    );
    assert.ok(!res.isError, res.output);
    const buf = fs.readFileSync(file);
    assert.deepEqual([buf[0], buf[1], buf[2]], [0xEF, 0xBB, 0xBF], 'BOM must be preserved');
    assert.equal(readNotebook(file).cells[0].source, 'new');
  });
});

// ---------------------------------------------------------------------------
// G6 — durable atomic write leaves no temp files behind
// ---------------------------------------------------------------------------

describe('notebook_edit U098 — durable write', () => {
  it('leaves no temp files behind after a successful write', async () => {
    const file = writeNotebook('durable.ipynb', [{ cell_type: 'code', source: 'old' }]);
    const res = await notebookEditTool.execute(
      { notebook_path: file, cell_number: 0, new_source: 'new' },
      ctx(),
    );
    assert.ok(!res.isError, res.output);
    const leftovers = fs.readdirSync(tmpDir).filter(f => f.endsWith('.tmp'));
    assert.deepEqual(leftovers, [], 'no .tmp files should remain');
  });
});

// ---------------------------------------------------------------------------
// G5 — enriched schema descriptions
// ---------------------------------------------------------------------------

describe('notebook_edit U098 — enriched descriptions', () => {
  it('documents absolute-path requirement and insert-required cell_type', () => {
    const props = notebookEditTool.inputSchema.properties as Record<string, { description?: string }>;
    assert.match(props.notebook_path.description ?? '', /absolute path/i);
    assert.match(props.cell_type.description ?? '', /Required when edit_mode is insert/i);
    assert.match(props.cell_id.description ?? '', /inserted after/i);
    assert.match(notebookEditTool.description, /Jupyter/);
  });
});
