import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { notebookEditTool } from '../src/tools/notebook-edit';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// U098-w81 — NotebookEditTool parity gaps:
//   1. semanticNumber coercion of a quoted cell_number ("2" → 2), with an
//      explicit error for a non-numeric cell_number (parity: utils/coerce.ts
//      coerceNumber replacing the raw `as number` cast).
//   2. Write-path glob guard: a notebook_path containing `*`/`?` is rejected
//      with an actionable "specify an exact file path" message, mirroring
//      file-edit.ts (parity: the reference's pathValidation glob-in-write rejection).
// ---------------------------------------------------------------------------

let tmpDir: string;
let nbPath: string;

function ctx(overrides: Partial<ToolContext> = {}): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    filesRead: new Set([nbPath]),
    ...overrides,
  } as ToolContext;
}

function writeNotebook(): void {
  fs.writeFileSync(nbPath, JSON.stringify({
    nbformat: 4,
    nbformat_minor: 5,
    metadata: { language_info: { name: 'python' } },
    cells: [
      { cell_type: 'code', source: ['x = 1\n'], metadata: {}, execution_count: null, outputs: [], id: 'c0' },
      { cell_type: 'code', source: ['y = 2\n'], metadata: {}, execution_count: null, outputs: [], id: 'c1' },
    ],
  }, null, 1));
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-nb-w81-'));
  nbPath = path.join(tmpDir, 'nb.ipynb');
  writeNotebook();
});
afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('notebook_edit – quoted cell_number coercion (U098-w81)', () => {
  it('coerces a quoted numeric cell_number ("1") to the real index and edits that cell', async () => {
    const result = await notebookEditTool.execute(
      { notebook_path: nbPath, cell_number: '1', new_source: 'y = 99\n' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.match(result.output, /Replaced cell 1/);
    const nb = JSON.parse(fs.readFileSync(nbPath, 'utf8'));
    // Cell 1 was changed; cell 0 untouched (proves "1" was NOT misread as 0 or NaN).
    assert.equal(nb.cells[1].source, 'y = 99\n');
    assert.deepEqual(nb.cells[0].source, ['x = 1\n']);
  });

  it('rejects a non-numeric cell_number with an explicit error (no silent fall-through)', async () => {
    const result = await notebookEditTool.execute(
      { notebook_path: nbPath, cell_number: 'abc', new_source: 'z = 0\n' },
      ctx(),
    );
    assert.ok(result.isError, 'non-numeric cell_number must error');
    assert.match(result.output, /cell_number must be a number/);
    // Notebook is untouched.
    const nb = JSON.parse(fs.readFileSync(nbPath, 'utf8'));
    assert.equal(nb.cells.length, 2);
    assert.deepEqual(nb.cells[0].source, ['x = 1\n']);
  });

  it('still accepts a genuine numeric cell_number unchanged', async () => {
    const result = await notebookEditTool.execute(
      { notebook_path: nbPath, cell_number: 0, new_source: 'x = 7\n' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    const nb = JSON.parse(fs.readFileSync(nbPath, 'utf8'));
    assert.equal(nb.cells[0].source, 'x = 7\n');
  });
});

describe('notebook_edit – write-path glob guard (U098-w81)', () => {
  it('rejects a notebook_path containing a "*" wildcard with an actionable message', async () => {
    const globPath = path.join(tmpDir, '*.ipynb');
    const result = await notebookEditTool.execute(
      { notebook_path: globPath, cell_number: 0, new_source: 'x = 1\n' },
      ctx({ filesRead: new Set([globPath]) }),
    );
    assert.ok(result.isError, 'glob path must error');
    assert.match(result.output, /Glob patterns are not allowed in write operations/);
    assert.match(result.output, /exact file path/);
  });

  it('rejects a notebook_path containing a "?" wildcard', async () => {
    const globPath = path.join(tmpDir, 'nb?.ipynb');
    const result = await notebookEditTool.execute(
      { notebook_path: globPath, cell_number: 0, new_source: 'x = 1\n' },
      ctx({ filesRead: new Set([globPath]) }),
    );
    assert.ok(result.isError, 'glob path must error');
    assert.match(result.output, /Glob patterns are not allowed in write operations/);
  });

  it('allows a legitimate bracket filename (superset divergence from Claude)', async () => {
    const bracketPath = path.join(tmpDir, '[id].ipynb');
    fs.writeFileSync(bracketPath, fs.readFileSync(nbPath, 'utf8'));
    const result = await notebookEditTool.execute(
      { notebook_path: bracketPath, cell_number: 0, new_source: 'x = 5\n' },
      ctx({ filesRead: new Set([bracketPath]) }),
    );
    assert.ok(!result.isError, result.output);
  });
});
