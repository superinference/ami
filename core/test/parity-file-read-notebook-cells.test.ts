import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileReadTool } from '../src/tools/file-read';
import { getFileCache } from '../src/file-cache';
import type { ToolContext } from '../src/types';

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

function writeNotebook(name: string, nb: unknown): string {
  const fp = path.join(tmpDir, name);
  fs.writeFileSync(fp, JSON.stringify(nb));
  return fp;
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-fr-nbcells-'));
  getFileCache(tmpDir).clear();
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
  mock.restoreAll();
});

// ---------------------------------------------------------------------------
// Gap: execution_count surfaced per code cell in the formatted output
// ---------------------------------------------------------------------------
describe('fileReadTool – notebook execution_count surfacing', () => {
  it('surfaces a code cell execution_count in the cell header', async () => {
    const fp = writeNotebook('exec.ipynb', {
      metadata: { kernelspec: { name: 'python3' } },
      cells: [
        { cell_type: 'code', execution_count: 7, source: ['print(1)\n'], outputs: [] },
      ],
    });
    const result = await fileReadTool.execute({ file_path: fp }, ctx());
    assert.equal(result.isError, undefined);
    assert.match(result.output, /exec(ution)?[:\s]*7/i);
  });

  it('does not attach an execution count for markdown cells', async () => {
    const fp = writeNotebook('md.ipynb', {
      metadata: {},
      cells: [{ cell_type: 'markdown', source: ['# Title\n'] }],
    });
    const result = await fileReadTool.execute({ file_path: fp }, ctx());
    assert.doesNotMatch(result.output, /exec/i);
  });
});

// ---------------------------------------------------------------------------
// Gap: per-cell language from notebook.metadata.language_info.name
// ---------------------------------------------------------------------------
describe('fileReadTool – notebook per-cell language', () => {
  it('surfaces a non-python code-cell language from language_info.name', async () => {
    const fp = writeNotebook('r.ipynb', {
      metadata: { language_info: { name: 'r' } },
      cells: [{ cell_type: 'code', execution_count: 1, source: ['x <- 1\n'], outputs: [] }],
    });
    const result = await fileReadTool.execute({ file_path: fp }, ctx());
    assert.match(result.output, /\br\b/);
    assert.match(result.output, /lang/i);
  });

  it('does not annotate python code cells with a language tag', async () => {
    const fp = writeNotebook('py.ipynb', {
      metadata: { language_info: { name: 'python' } },
      cells: [{ cell_type: 'code', execution_count: 1, source: ['y = 2\n'], outputs: [] }],
    });
    const result = await fileReadTool.execute({ file_path: fp }, ctx());
    assert.doesNotMatch(result.output, /lang/i);
  });
});

// ---------------------------------------------------------------------------
// Gap: cell_id parameter to read a single notebook cell by id
// ---------------------------------------------------------------------------
describe('fileReadTool – notebook cell_id single-cell read', () => {
  it('reads only the cell matching cell_id (by explicit id)', async () => {
    const fp = writeNotebook('ids.ipynb', {
      metadata: {},
      cells: [
        { cell_type: 'code', id: 'aaa', execution_count: 1, source: ['first()\n'], outputs: [] },
        { cell_type: 'code', id: 'bbb', execution_count: 2, source: ['second()\n'], outputs: [] },
      ],
    });
    const result = await fileReadTool.execute({ file_path: fp, cell_id: 'bbb' }, ctx());
    assert.equal(result.isError, undefined);
    assert.match(result.output, /second\(\)/);
    assert.doesNotMatch(result.output, /first\(\)/);
  });

  it('reads a cell by its synthetic cell-N id when no explicit id is present', async () => {
    const fp = writeNotebook('synth.ipynb', {
      metadata: {},
      cells: [
        { cell_type: 'code', source: ['alpha()\n'], outputs: [] },
        { cell_type: 'code', source: ['beta()\n'], outputs: [] },
      ],
    });
    const result = await fileReadTool.execute({ file_path: fp, cell_id: 'cell-1' }, ctx());
    assert.match(result.output, /beta\(\)/);
    assert.doesNotMatch(result.output, /alpha\(\)/);
  });

  it('errors when cell_id does not match any cell', async () => {
    const fp = writeNotebook('nomatch.ipynb', {
      metadata: {},
      cells: [{ cell_type: 'code', id: 'aaa', source: ['x()\n'], outputs: [] }],
    });
    const result = await fileReadTool.execute({ file_path: fp, cell_id: 'zzz' }, ctx());
    assert.equal(result.isError, true);
    assert.match(result.output, /not found/i);
    assert.match(result.output, /zzz/);
  });

  it('includes large outputs when reading a single cell by id (bypasses the large-output guard)', async () => {
    const big = 'DATA'.repeat(5000); // >10000 chars → exceeds NOTEBOOK_LARGE_OUTPUT_THRESHOLD
    const fp = writeNotebook('big.ipynb', {
      metadata: {},
      cells: [
        {
          cell_type: 'code',
          id: 'bigcell',
          execution_count: 3,
          source: ['make_big()\n'],
          outputs: [{ output_type: 'stream', text: [big] }],
        },
      ],
    });
    // Full read: large-output guard replaces the payload with a jq hint.
    const full = await fileReadTool.execute({ file_path: fp }, ctx());
    assert.match(full.output, /too large to include/i);
    // Single-cell read: the big output is included verbatim.
    const single = await fileReadTool.execute({ file_path: fp, cell_id: 'bigcell' }, ctx());
    assert.doesNotMatch(single.output, /too large to include/i);
    assert.match(single.output, /DATA/);
  });
});
