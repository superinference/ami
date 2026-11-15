import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { notebookEditTool } from '../src/tools/notebook-edit';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// Parity batch "c" for core notebook_edit:
//  1. encoding + line-ending preservation on write-back (durable atomic write)
//  2. cell_id resolution ordering — a real id match wins over the cell-N alias
//  3. wrong-file-type error carries a recovery hint to the file_edit tool
//
// Behavioral/red-green: each assertion is computed from the real tool's
// on-disk effect or output, never from source text.
// ---------------------------------------------------------------------------

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

interface Cell { cell_type: string; source: string; id?: string }

function buildNotebook(cells: Cell[]): string {
  return JSON.stringify({
    nbformat: 4,
    nbformat_minor: 5,
    metadata: {},
    cells: cells.map(c => ({
      cell_type: c.cell_type,
      source: c.source,
      metadata: {},
      ...(c.id ? { id: c.id } : {}),
      ...(c.cell_type === 'code' ? { execution_count: null, outputs: [] } : {}),
    })),
  }, null, 1);
}

beforeEach(() => {
  tmpDir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'parity-nb-c-')));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ===========================================================================
// 1. Encoding + line-ending preservation
// ===========================================================================

describe('notebook_edit — preserves original line endings', () => {
  it('keeps CRLF line endings on write-back (does not normalize to LF)', async () => {
    const file = path.join(tmpDir, 'crlf.ipynb');
    const crlf = buildNotebook([{ cell_type: 'code', source: 'x' }]).replace(/\n/g, '\r\n');
    fs.writeFileSync(file, crlf, 'utf-8');

    const res = await notebookEditTool.execute(
      { notebook_path: file, cell_number: 0, new_source: 'y', edit_mode: 'replace' },
      ctx(),
    );
    assert.ok(!res.isError, res.output);

    const raw = fs.readFileSync(file, 'utf-8');
    assert.ok(raw.includes('\r\n'), 'expected CRLF to be preserved');
    assert.ok(!/[^\r]\n/.test(raw), 'expected no bare LF line endings');
    // Content round-trips to valid JSON with the edit applied.
    const nb = JSON.parse(raw);
    assert.equal(nb.cells[0].source, 'y');
  });

  it('keeps LF line endings on an LF notebook', async () => {
    const file = path.join(tmpDir, 'lf.ipynb');
    fs.writeFileSync(file, buildNotebook([{ cell_type: 'code', source: 'x' }]), 'utf-8');

    await notebookEditTool.execute(
      { notebook_path: file, cell_number: 0, new_source: 'y' },
      ctx(),
    );
    const raw = fs.readFileSync(file, 'utf-8');
    assert.ok(!raw.includes('\r\n'), 'LF notebook must not gain CRLF');
  });
});

describe('notebook_edit — preserves original character encoding', () => {
  it('keeps a UTF-8 BOM on write-back', async () => {
    const file = path.join(tmpDir, 'bom.ipynb');
    const body = Buffer.from(buildNotebook([{ cell_type: 'code', source: 'x' }]), 'utf-8');
    fs.writeFileSync(file, Buffer.concat([Buffer.from([0xEF, 0xBB, 0xBF]), body]));

    const res = await notebookEditTool.execute(
      { notebook_path: file, cell_number: 0, new_source: 'y' },
      ctx(),
    );
    assert.ok(!res.isError, res.output);

    const buf = fs.readFileSync(file);
    assert.equal(buf[0], 0xEF, 'BOM byte 0 must survive');
    assert.equal(buf[1], 0xBB, 'BOM byte 1 must survive');
    assert.equal(buf[2], 0xBF, 'BOM byte 2 must survive');
    const nb = JSON.parse(buf.subarray(3).toString('utf-8'));
    assert.equal(nb.cells[0].source, 'y');
  });

  it('keeps UTF-16LE encoding on write-back', async () => {
    const file = path.join(tmpDir, 'u16.ipynb');
    const body = Buffer.from(buildNotebook([{ cell_type: 'code', source: 'x' }]), 'utf16le');
    fs.writeFileSync(file, Buffer.concat([Buffer.from([0xFF, 0xFE]), body]));

    const res = await notebookEditTool.execute(
      { notebook_path: file, cell_number: 0, new_source: 'y' },
      ctx(),
    );
    assert.ok(!res.isError, res.output);

    const buf = fs.readFileSync(file);
    assert.equal(buf[0], 0xFF, 'UTF-16LE BOM byte 0 must survive');
    assert.equal(buf[1], 0xFE, 'UTF-16LE BOM byte 1 must survive');
    const nb = JSON.parse(buf.subarray(2).toString('utf16le'));
    assert.equal(nb.cells[0].source, 'y');
  });

  it('two sequential edits succeed (write-back caches the decoded form, no false "modified")', async () => {
    const file = path.join(tmpDir, 'seq.ipynb');
    const crlf = buildNotebook([{ cell_type: 'code', source: 'x' }]).replace(/\n/g, '\r\n');
    fs.writeFileSync(file, crlf, 'utf-8');
    const c = ctx();

    const first = await notebookEditTool.execute(
      { notebook_path: file, cell_number: 0, new_source: 'y' },
      c,
    );
    assert.ok(!first.isError, first.output);
    // A second edit through the same context must not trip the file-cache
    // "modified since last read" guard — the write-back must cache content that
    // matches what the decodeBuffer-based freshness check reads from disk.
    const second = await notebookEditTool.execute(
      { notebook_path: file, cell_number: 0, new_source: 'z' },
      c,
    );
    assert.ok(!second.isError, second.output);
    assert.equal(JSON.parse(fs.readFileSync(file, 'utf-8')).cells[0].source, 'z');
  });
});

// ===========================================================================
// 2. cell_id resolution ordering: real id wins over cell-N positional alias
// ===========================================================================

describe('notebook_edit — cell_id real-id match wins over cell-N alias', () => {
  it('selects the cell whose genuine id is "cell-1", not positional index 1', async () => {
    const file = path.join(tmpDir, 'collision.ipynb');
    // Cell 0 has the genuine legal nbformat id "cell-1"; cell 1 is a decoy.
    fs.writeFileSync(file, buildNotebook([
      { cell_type: 'code', source: 'zero', id: 'cell-1' },
      { cell_type: 'code', source: 'one', id: 'real-one' },
    ]), 'utf-8');

    const res = await notebookEditTool.execute(
      { notebook_path: file, cell_id: 'cell-1', new_source: 'patched' },
      ctx(),
    );
    assert.ok(!res.isError, res.output);

    const nb = JSON.parse(fs.readFileSync(file, 'utf-8'));
    // The id-named cell (index 0) is edited; the positional-index-1 cell is NOT.
    assert.equal(nb.cells[0].source, 'patched');
    assert.equal(nb.cells[1].source, 'one');
  });

  it('still honors the cell-N positional alias when no cell owns that id', async () => {
    const file = path.join(tmpDir, 'alias.ipynb');
    fs.writeFileSync(file, buildNotebook([
      { cell_type: 'code', source: 'zero' },
      { cell_type: 'code', source: 'one' },
    ]), 'utf-8');

    const res = await notebookEditTool.execute(
      { notebook_path: file, cell_id: 'cell-1', new_source: 'via-alias' },
      ctx(),
    );
    assert.ok(!res.isError, res.output);
    const nb = JSON.parse(fs.readFileSync(file, 'utf-8'));
    assert.equal(nb.cells[1].source, 'via-alias');
  });
});

// ===========================================================================
// 3. Wrong-file-type error carries a recovery hint
// ===========================================================================

describe('notebook_edit — wrong file type recovery hint', () => {
  it('points the caller to the file_edit tool when the path is not .ipynb', async () => {
    const file = path.join(tmpDir, 'notes.txt');
    fs.writeFileSync(file, 'hello', 'utf-8');

    const res = await notebookEditTool.execute(
      { notebook_path: file, new_source: 'x' },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.match(res.output, /Jupyter notebook/);
    assert.match(res.output, /file_edit/);
  });
});
