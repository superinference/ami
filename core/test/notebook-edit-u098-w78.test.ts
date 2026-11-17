import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { notebookEditTool } from '../src/tools/notebook-edit';
import { lspHooks } from '../src/tools/file-edit';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// U098-w78 — NotebookEditTool: cross-turn LSP diagnostic deduplication via the
// bounded-LRU DiagnosticTracker, with PER-FILE INVALIDATION ON EDIT.
//
// Parity (LSPDiagnosticRegistry.ts L51-56, L290-312, L372-379): Claude invalidates
// a file's delivered-diagnostics on edit so the next publish surfaces its issues
// again, and caps retention with a bounded LRU. AMI's DiagnosticTracker already
// provides the bounded-LRU baseline (MAX_TRACKED_BASELINE_FILES=500 + LRU
// eviction); file_edit snapshots the pre-edit baseline via captureLspBaseline so
// a post-edit `diagnostics` query reports ONLY the issues the edit introduced.
// notebook_edit did NOT participate — a notebook edit never invalidated/rolled the
// per-file baseline, so cross-turn dedup was wrong for .ipynb edits. This verifies
// notebook_edit now captures the baseline before its write, exactly like file_edit,
// reusing the SAME shared helper (no duplicate LSP-baseline logic).
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
    cells: [{ cell_type: 'code', source: ['x = 1\n'], metadata: {}, execution_count: null, outputs: [], id: 'c0' }],
  }, null, 1));
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-nb-w78-'));
  nbPath = path.join(tmpDir, 'nb.ipynb');
  writeNotebook();
});
afterEach(() => {
  mock.restoreAll();
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('notebook_edit – pre-edit LSP diagnostics baseline (U098-w78)', () => {
  it('invalidates the per-file diagnostics baseline before a replace edit', async () => {
    const calls: string[] = [];
    mock.method(lspHooks, 'getClient', (() => ({
      notifyDidChange: () => Promise.resolve(),
      notifyDidSave: () => Promise.resolve(),
      diagnostics: {
        beforeFileEdited: (p: string) => { calls.push(p); return Promise.resolve(); },
      },
    })) as any);

    const result = await notebookEditTool.execute(
      { notebook_path: nbPath, cell_number: 0, new_source: 'x = 2\n' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    // The baseline was snapshotted for THIS notebook, before the write — the
    // per-file invalidation that makes cross-turn diagnostic dedup correct.
    assert.deepEqual(calls, [fs.realpathSync(nbPath)]);
  });

  it('invalidates the baseline on insert and delete edits too', async () => {
    const calls: string[] = [];
    mock.method(lspHooks, 'getClient', (() => ({
      notifyDidChange: () => Promise.resolve(),
      notifyDidSave: () => Promise.resolve(),
      diagnostics: { beforeFileEdited: (p: string) => { calls.push(p); return Promise.resolve(); } },
    })) as any);

    const ins = await notebookEditTool.execute(
      { notebook_path: nbPath, edit_mode: 'insert', cell_type: 'code', new_source: 'y = 2\n' },
      ctx(),
    );
    assert.ok(!ins.isError, ins.output);
    const del = await notebookEditTool.execute(
      { notebook_path: nbPath, cell_id: 'c0', edit_mode: 'delete', new_source: '' },
      ctx(),
    );
    assert.ok(!del.isError, del.output);
    assert.equal(calls.length, 2);
    assert.ok(calls.every((c) => c === fs.realpathSync(nbPath)), calls.join(','));
  });

  it('does NOT touch the LSP in detached mode (parity with file_edit)', async () => {
    let touched = false;
    mock.method(lspHooks, 'getClient', (() => { touched = true; return {}; }) as any);
    const result = await notebookEditTool.execute(
      { notebook_path: nbPath, cell_number: 0, new_source: 'x = 3\n' },
      ctx({ detachedMode: true }),
    );
    assert.ok(!result.isError, result.output);
    assert.equal(touched, false);
  });

  it('swallows a client-lookup failure — the edit still succeeds', async () => {
    mock.method(lspHooks, 'getClient', (() => { throw new Error('no lsp'); }) as any);
    const result = await notebookEditTool.execute(
      { notebook_path: nbPath, cell_number: 0, new_source: 'x = 4\n' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.ok(fs.readFileSync(nbPath, 'utf-8').includes('x = 4'));
  });
});
