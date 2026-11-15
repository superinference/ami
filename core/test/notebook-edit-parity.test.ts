import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { notebookEditTool } from '../src/tools/notebook-edit';
import type { ToolContext } from '../src/types';

// ===========================================================================
// notebook-edit.ts — core parity: atomic temp+rename write, writability
// pre-check, and invalid cell_type validation.
// ===========================================================================

describe('notebook-edit — parity (atomic write + guards)', () => {
  let tmpDir: string;

  function ctx(overrides?: Partial<ToolContext>): ToolContext {
    return {
      cwd: tmpDir,
      abortSignal: new AbortController().signal,
      // Run in detached mode so the LSP notify block (non-detached) is skipped.
      detachedMode: true,
      ...overrides,
    } as ToolContext;
  }

  function writeNotebook(name: string, cells: unknown[]): string {
    const file = path.join(tmpDir, name);
    fs.writeFileSync(
      file,
      JSON.stringify({ nbformat: 4, nbformat_minor: 5, metadata: {}, cells }),
    );
    return file;
  }

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-nb-parity-'));
  });

  afterEach(() => {
    // Restore writable/traversable perms throughout the tree so rm can delete it,
    // even after a test left a read-only dir/file behind.
    const restore = (p: string): void => {
      let st: fs.Stats;
      try {
        st = fs.lstatSync(p);
      } catch {
        return;
      }
      fs.chmodSync(p, st.isDirectory() ? 0o755 : 0o644);
      if (st.isDirectory()) {
        for (const e of fs.readdirSync(p)) restore(path.join(p, e));
      }
    };
    restore(tmpDir);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  // -- Atomic write: happy path writes valid JSON and leaves no temp litter. ----
  it('writes valid JSON via atomic rename and leaves no temp files behind', async () => {
    const file = writeNotebook('ok.ipynb', [
      { cell_type: 'code', source: 'a', metadata: {}, execution_count: null, outputs: [] },
    ]);

    const result = await notebookEditTool.execute!(
      { notebook_path: file, cell_number: 0, new_source: 'print(1)', edit_mode: 'replace' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);

    // Target holds valid JSON with the new source.
    const parsed = JSON.parse(fs.readFileSync(file, 'utf-8'));
    assert.equal(
      Array.isArray(parsed.cells[0].source)
        ? parsed.cells[0].source.join('')
        : parsed.cells[0].source,
      'print(1)',
    );

    // No `.ok.ipynb.<pid>.<rand>.tmp` litter remains in the directory.
    const litter = fs.readdirSync(tmpDir).filter((f) => f.includes('.tmp'));
    assert.deepEqual(litter, [], `unexpected temp litter: ${litter.join(', ')}`);
  });

  // -- Writability pre-check: a read-only FILE in a writable dir must still fail. -
  it('fails on a read-only notebook file (atomic rename would otherwise clobber it)', async () => {
    const file = writeNotebook('ro.ipynb', [
      { cell_type: 'code', source: 'a', metadata: {}, execution_count: null, outputs: [] },
    ]);
    const before = fs.readFileSync(file, 'utf-8');
    fs.chmodSync(file, 0o444);

    const result = await notebookEditTool.execute!(
      { notebook_path: file, cell_number: 0, new_source: 'x', edit_mode: 'replace' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /Error writing notebook/);
    assert.match(result.output, /read-only or not writable/);

    // The read-only file is untouched (not clobbered by a rename).
    fs.chmodSync(file, 0o644);
    assert.equal(fs.readFileSync(file, 'utf-8'), before);
  });

  // -- Atomic write failure path: writable file inside a read-only directory. ----
  // Exercises the writeFile/rename catch + best-effort temp cleanup.
  it('reports a write error when the target directory is not writable', async () => {
    const sub = fs.mkdtempSync(path.join(tmpDir, 'rodir-'));
    const file = path.join(sub, 'n.ipynb');
    fs.writeFileSync(
      file,
      JSON.stringify({
        nbformat: 4,
        nbformat_minor: 5,
        metadata: {},
        cells: [{ cell_type: 'code', source: 'a', metadata: {}, execution_count: null, outputs: [] }],
      }),
    );
    // File itself stays writable (W_OK pre-check passes); the DIR is read-only so
    // the temp writeFile/rename fails.
    fs.chmodSync(sub, 0o555);

    const result = await notebookEditTool.execute!(
      { notebook_path: file, cell_number: 0, new_source: 'x', edit_mode: 'replace' },
      ctx({ cwd: sub }),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /Error writing notebook/);

    // No temp file leaked into the (now restored) directory.
    fs.chmodSync(sub, 0o755);
    const litter = fs.readdirSync(sub).filter((f) => f.includes('.tmp'));
    assert.deepEqual(litter, [], `unexpected temp litter: ${litter.join(', ')}`);
  });

  // -- Invalid cell_type validation. --------------------------------------------
  it('rejects an invalid cell_type and names the valid options', async () => {
    const file = writeNotebook('bad.ipynb', [
      { cell_type: 'code', source: 'a', metadata: {}, execution_count: null, outputs: [] },
    ]);
    const result = await notebookEditTool.execute!(
      { notebook_path: file, cell_number: 0, new_source: 'x', cell_type: 'raw', edit_mode: 'replace' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /cell_type must be "code" or "markdown"/);
    assert.match(result.output, /raw/);
  });

  it('accepts a valid markdown cell_type on insert', async () => {
    const file = writeNotebook('mdok.ipynb', [
      { cell_type: 'code', source: 'a', metadata: {}, execution_count: null, outputs: [] },
    ]);
    const result = await notebookEditTool.execute!(
      { notebook_path: file, new_source: '# title', cell_type: 'markdown', edit_mode: 'insert' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    const parsed = JSON.parse(fs.readFileSync(file, 'utf-8'));
    assert.equal(parsed.cells[parsed.cells.length - 1].cell_type, 'markdown');
  });
});
