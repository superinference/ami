import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { notebookEditTool } from '../src/tools/notebook-edit';
import type { ToolContext } from '../src/types';
import { getLSPClient } from '../src/lsp';
import { getFileCache } from '../src/file-cache';
import {
  fuzzyFindAndReplace,
  reindentReplacement,
  findClosestLines,
} from '../src/tools/fuzzy-match';
import { suggestSimilarPaths } from '../src/tools/path-suggest';
import { syntheticOutputTool } from '../src/tools/synthetic-output';
import { spillToolText, buildSpillBanner } from '../src/tools/spill-output';

// ===========================================================================
// notebook-edit.ts — uncovered branches/functions
// ===========================================================================

describe('notebook-edit — uncovered paths', () => {
  let tmpDir: string;

  function ctx(overrides?: Partial<ToolContext>): ToolContext {
    return {
      cwd: tmpDir,
      abortSignal: new AbortController().signal,
      ...overrides,
    } as ToolContext;
  }

  function writeRawNotebook(
    name: string,
    notebook: unknown,
  ): string {
    const file = path.join(tmpDir, name);
    fs.writeFileSync(file, JSON.stringify(notebook));
    return file;
  }

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-nb-cov-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
    mock.restoreAll();
  });

  it('rejects UNC paths (// prefix) — NTLM credential-leak guard', async () => {
    const result = await notebookEditTool.execute!(
      { notebook_path: '//server/share/evil.ipynb', new_source: 'x' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /UNC paths are not allowed/);
  });

  it('rejects UNC paths (backslash prefix)', async () => {
    const result = await notebookEditTool.execute!(
      { notebook_path: '\\\\server\\share\\evil.ipynb', new_source: 'x' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /UNC paths are not allowed/);
  });

  it('reports a non-ENOENT read error (target is a directory)', async () => {
    // A directory whose name ends in .ipynb: passes write-path + extension
    // checks, but readFile fails with EISDIR (not ENOENT).
    const dir = path.join(tmpDir, 'adir.ipynb');
    fs.mkdirSync(dir);
    const result = await notebookEditTool.execute!(
      { notebook_path: dir, new_source: 'x' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /Error reading notebook/);
    assert.ok(!/not found/.test(result.output), 'EISDIR must not be reported as ENOENT');
  });

  it('resolves cell_id by matching a real cell id', async () => {
    const file = writeRawNotebook('byid.ipynb', {
      nbformat: 4,
      nbformat_minor: 5,
      metadata: {},
      cells: [
        { cell_type: 'code', source: 'a', metadata: {}, id: 'aaa', execution_count: null, outputs: [] },
        { cell_type: 'code', source: 'b', metadata: {}, id: 'bbb', execution_count: null, outputs: [] },
      ],
    });
    const result = await notebookEditTool.execute!(
      { notebook_path: file, cell_id: 'bbb', new_source: 'B2', edit_mode: 'replace' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.match(result.output, /Replaced cell 1/);
    const nb = JSON.parse(fs.readFileSync(file, 'utf-8'));
    assert.equal(nb.cells[1].source, 'B2');
  });

  it('errors with an available-cells list when cell_id is not found', async () => {
    const file = writeRawNotebook('missingid.ipynb', {
      nbformat: 4,
      nbformat_minor: 5,
      metadata: {},
      cells: [
        { cell_type: 'code', source: 'a', metadata: {}, id: 'aaa', execution_count: null, outputs: [] },
        { cell_type: 'markdown', source: 'b', metadata: {} }, // no id
      ],
    });
    const result = await notebookEditTool.execute!(
      { notebook_path: file, cell_id: 'zzz', new_source: 'x', edit_mode: 'replace' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /cell_id "zzz" not found/);
    assert.match(result.output, /0:aaa/);
    assert.match(result.output, /1:\(no id\)/);
  });

  it('resolves the cell-N alias form', async () => {
    const file = writeRawNotebook('alias.ipynb', {
      nbformat: 4,
      nbformat_minor: 5,
      metadata: {},
      cells: [
        { cell_type: 'code', source: 'a', metadata: {}, execution_count: null, outputs: [] },
        { cell_type: 'code', source: 'b', metadata: {}, execution_count: null, outputs: [] },
      ],
    });
    const result = await notebookEditTool.execute!(
      { notebook_path: file, cell_id: 'cell-1', new_source: 'B!', edit_mode: 'replace' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    const nb = JSON.parse(fs.readFileSync(file, 'utf-8'));
    assert.equal(nb.cells[1].source, 'B!');
  });

  it('treats an out-of-range cell-N alias as a non-matching id', async () => {
    const file = writeRawNotebook('aliasoor.ipynb', {
      nbformat: 4,
      nbformat_minor: 5,
      metadata: {},
      cells: [{ cell_type: 'code', source: 'a', metadata: {}, execution_count: null, outputs: [] }],
    });
    // "cell-9" parses but 9 >= length, so the alias shortcut is skipped and the
    // id lookup runs — which does not find a literal id "cell-9".
    const result = await notebookEditTool.execute!(
      { notebook_path: file, cell_id: 'cell-9', new_source: 'x', edit_mode: 'replace' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /not found/);
  });

  it('treats a non-numeric cell-N alias as a non-matching id', async () => {
    const file = writeRawNotebook('aliasnan.ipynb', {
      nbformat: 4,
      nbformat_minor: 5,
      metadata: {},
      cells: [{ cell_type: 'code', source: 'a', metadata: {}, execution_count: null, outputs: [] }],
    });
    const result = await notebookEditTool.execute!(
      { notebook_path: file, cell_id: 'cell-abc', new_source: 'x', edit_mode: 'replace' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /not found/);
  });

  it('inserts AFTER the matched cell when cell_id + insert are combined', async () => {
    const file = writeRawNotebook('insafter.ipynb', {
      nbformat: 4,
      nbformat_minor: 5,
      metadata: {},
      cells: [
        { cell_type: 'code', source: 'a', metadata: {}, id: 'aaa', execution_count: null, outputs: [] },
        { cell_type: 'code', source: 'c', metadata: {}, id: 'ccc', execution_count: null, outputs: [] },
      ],
    });
    const result = await notebookEditTool.execute!(
      { notebook_path: file, cell_id: 'aaa', new_source: 'INSERTED', edit_mode: 'insert' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    const nb = JSON.parse(fs.readFileSync(file, 'utf-8'));
    assert.equal(nb.cells.length, 3);
    assert.equal(nb.cells[1].source, 'INSERTED'); // inserted after index 0
  });

  it('generates an id for nbformat > 4 notebooks on insert', async () => {
    const file = writeRawNotebook('nb5.ipynb', {
      nbformat: 5,
      nbformat_minor: 0,
      metadata: {},
      cells: [{ cell_type: 'code', source: 'a', metadata: {}, execution_count: null, outputs: [] }],
    });
    const result = await notebookEditTool.execute!(
      { notebook_path: file, new_source: 'new', edit_mode: 'insert' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    const nb = JSON.parse(fs.readFileSync(file, 'utf-8'));
    assert.ok(nb.cells[1].id, 'nbformat>4 insert should get an id');
  });

  it('auto-converts an out-of-range replace to insert on an nbformat>4 notebook (id generated)', async () => {
    const file = writeRawNotebook('nb5replace.ipynb', {
      nbformat: 5,
      nbformat_minor: 0,
      metadata: {},
      cells: [{ cell_type: 'code', source: 'a', metadata: {}, execution_count: null, outputs: [] }],
    });
    const result = await notebookEditTool.execute!(
      { notebook_path: file, cell_number: 9, new_source: 'z', cell_type: 'markdown', edit_mode: 'replace' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.match(result.output, /Inserted/);
    const nb = JSON.parse(fs.readFileSync(file, 'utf-8'));
    assert.equal(nb.cells[1].cell_type, 'markdown');
    assert.ok(nb.cells[1].id, 'auto-insert on nbformat>4 should get an id');
  });

  it('rejects a notebook_path containing a null byte', async () => {
    const result = await notebookEditTool.execute!(
      { notebook_path: 'bad\x00.ipynb', new_source: 'x' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /null byte/);
  });

  it('leaves the cell type unchanged when the requested type matches the current type', async () => {
    const file = writeRawNotebook('samectype.ipynb', {
      nbformat: 4,
      nbformat_minor: 5,
      metadata: {},
      cells: [{ cell_type: 'code', source: 'a', metadata: {}, execution_count: null, outputs: [] }],
    });
    const result = await notebookEditTool.execute!(
      { notebook_path: file, cell_number: 0, new_source: 'b', cell_type: 'code', edit_mode: 'replace' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    const nb = JSON.parse(fs.readFileSync(file, 'utf-8'));
    assert.equal(nb.cells[0].cell_type, 'code');
    assert.equal(nb.cells[0].source, 'b');
  });

  it('treats a null cell_number as unspecified (insert appends at the end)', async () => {
    const file = writeRawNotebook('nullnum.ipynb', {
      nbformat: 4,
      nbformat_minor: 5,
      metadata: {},
      cells: [{ cell_type: 'code', source: 'a', metadata: {}, execution_count: null, outputs: [] }],
    });
    const result = await notebookEditTool.execute!(
      { notebook_path: file, cell_number: null, new_source: 'appended', edit_mode: 'insert' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    const nb = JSON.parse(fs.readFileSync(file, 'utf-8'));
    assert.equal(nb.cells.length, 2);
    assert.equal(nb.cells[1].source, 'appended');
  });

  it('rejects a path outside the workspace', async () => {
    const outside = fs.mkdtempSync(path.join(os.tmpdir(), 'si-nb-outside-'));
    try {
      const file = path.join(outside, 'x.ipynb');
      fs.writeFileSync(file, JSON.stringify({ nbformat: 4, nbformat_minor: 5, metadata: {}, cells: [] }));
      const result = await notebookEditTool.execute!(
        { notebook_path: file, new_source: 'x' },
        ctx(),
      );
      assert.equal(result.isError, true);
      assert.match(result.output, /outside the workspace/);
    } finally {
      fs.rmSync(outside, { recursive: true, force: true });
    }
  });

  it('rejects a notebook whose JSON is literally null', async () => {
    const file = path.join(tmpDir, 'nulljson.ipynb');
    fs.writeFileSync(file, 'null');
    const result = await notebookEditTool.execute!(
      { notebook_path: file, new_source: 'x' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /missing "cells" array/);
  });

  it('lets cell_number take precedence when both cell_number and cell_id are given', async () => {
    const file = writeRawNotebook('both.ipynb', {
      nbformat: 4,
      nbformat_minor: 5,
      metadata: {},
      cells: [
        { cell_type: 'code', source: 'a', metadata: {}, id: 'aaa', execution_count: null, outputs: [] },
        { cell_type: 'code', source: 'b', metadata: {}, id: 'bbb', execution_count: null, outputs: [] },
      ],
    });
    // cell_id "aaa" (index 0) is ignored because cell_number 1 is provided.
    const result = await notebookEditTool.execute!(
      { notebook_path: file, cell_number: 1, cell_id: 'aaa', new_source: 'B2', edit_mode: 'replace' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    const nb = JSON.parse(fs.readFileSync(file, 'utf-8'));
    assert.equal(nb.cells[1].source, 'B2');
    assert.equal(nb.cells[0].source, 'a');
  });

  it('rejects a negative cell_number in replace mode', async () => {
    const file = writeRawNotebook('negreplace.ipynb', {
      nbformat: 4,
      nbformat_minor: 5,
      metadata: {},
      cells: [{ cell_type: 'code', source: 'a', metadata: {}, execution_count: null, outputs: [] }],
    });
    const result = await notebookEditTool.execute!(
      { notebook_path: file, cell_number: -1, new_source: 'x', edit_mode: 'replace' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /out of range/);
  });

  it('skips LSP notifications in detached mode', async () => {
    const file = writeRawNotebook('detached.ipynb', {
      nbformat: 4,
      nbformat_minor: 5,
      metadata: {},
      cells: [{ cell_type: 'code', source: 'a', metadata: {}, execution_count: null, outputs: [] }],
    });
    const result = await notebookEditTool.execute!(
      { notebook_path: file, cell_number: 0, new_source: 'edited', edit_mode: 'replace' },
      ctx({ detachedMode: true }),
    );
    assert.ok(!result.isError, result.output);
    const nb = JSON.parse(fs.readFileSync(file, 'utf-8'));
    assert.equal(nb.cells[0].source, 'edited');
  });

  it('requires the notebook be read first when filesRead tracking is active', async () => {
    const file = writeRawNotebook('unread.ipynb', {
      nbformat: 4,
      nbformat_minor: 5,
      metadata: {},
      cells: [{ cell_type: 'code', source: 'a', metadata: {}, execution_count: null, outputs: [] }],
    });
    const result = await notebookEditTool.execute!(
      { notebook_path: file, cell_number: 0, new_source: 'x', edit_mode: 'replace' },
      ctx({ filesRead: new Set<string>() }),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /must read .* with file_read before editing/);
  });

  it('allows the edit when the notebook is recorded in filesRead', async () => {
    const file = writeRawNotebook('wasread.ipynb', {
      nbformat: 4,
      nbformat_minor: 5,
      metadata: {},
      cells: [{ cell_type: 'code', source: 'a', metadata: {}, execution_count: null, outputs: [] }],
    });
    const real = fs.realpathSync(file);
    const result = await notebookEditTool.execute!(
      { notebook_path: file, cell_number: 0, new_source: 'edited', edit_mode: 'replace' },
      ctx({ filesRead: new Set<string>([real]) }),
    );
    assert.ok(!result.isError, result.output);
    const nb = JSON.parse(fs.readFileSync(file, 'utf-8'));
    assert.equal(nb.cells[0].source, 'edited');
  });

  it('refuses to edit when the notebook changed on disk since the last read', async () => {
    const file = writeRawNotebook('stale.ipynb', {
      nbformat: 4,
      nbformat_minor: 5,
      metadata: {},
      cells: [{ cell_type: 'code', source: 'a', metadata: {}, execution_count: null, outputs: [] }],
    });
    const real = fs.realpathSync(file);
    // Seed the cache with a stale mtime so hasChanged() returns true.
    getFileCache(tmpDir).set(real, fs.readFileSync(real, 'utf-8'), 1);
    const result = await notebookEditTool.execute!(
      { notebook_path: file, cell_number: 0, new_source: 'x', edit_mode: 'replace' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /modified since last read/);
    // The stale entry is evicted so the next read returns full content.
    assert.equal(getFileCache(tmpDir).get(real), null);
  });

  it('reports a write failure (read-only notebook file)', async () => {
    const file = writeRawNotebook('ro.ipynb', {
      nbformat: 4,
      nbformat_minor: 5,
      metadata: {},
      cells: [{ cell_type: 'code', source: 'a', metadata: {}, execution_count: null, outputs: [] }],
    });
    fs.chmodSync(file, 0o444);
    try {
      const result = await notebookEditTool.execute!(
        { notebook_path: file, cell_number: 0, new_source: 'x', edit_mode: 'replace' },
        ctx(),
      );
      assert.equal(result.isError, true);
      assert.match(result.output, /Error writing notebook/);
    } finally {
      fs.chmodSync(file, 0o644);
    }
  });

  it('swallows LSP notification rejections (non-detached mode)', async () => {
    const file = writeRawNotebook('lsp.ipynb', {
      nbformat: 4,
      nbformat_minor: 5,
      metadata: {},
      cells: [{ cell_type: 'code', source: 'a', metadata: {}, execution_count: null, outputs: [] }],
    });
    // Make the shared LSP singleton's notifications reject so the fire-and-forget
    // `.catch(() => {})` handlers execute. The edit must still succeed.
    const lsp = getLSPClient();
    mock.method(lsp, 'notifyDidChange', () => Promise.reject(new Error('boom-change')));
    mock.method(lsp, 'notifyDidSave', () => Promise.reject(new Error('boom-save')));

    const result = await notebookEditTool.execute!(
      { notebook_path: file, cell_number: 0, new_source: 'edited', edit_mode: 'replace' },
      ctx({ detachedMode: false } as Partial<ToolContext>),
    );
    assert.ok(!result.isError, result.output);
    // Let the rejected-promise microtasks (and their catch handlers) flush.
    await new Promise((r) => setTimeout(r, 20));
  });
});

// ===========================================================================
// fuzzy-match.ts — uncovered branches/functions
// ===========================================================================

describe('fuzzy-match — uncovered paths', () => {
  it('calls every strategy when nothing matches', () => {
    // exact/line_trimmed/whitespace_normalized/indentation_flexible/escape_normalized
    // are all invoked; each returns zero matches.
    const result = fuzzyFindAndReplace('alpha beta gamma', 'nonexistent-token', 'x');
    assert.equal(result.newContent, null);
    assert.equal(result.strategy, null);
    assert.equal(result.matchCount, 0);
    assert.match(result.error ?? '', /not found/);
  });

  it('wins via escape_normalized using a literal backslash-n pattern', () => {
    const content = 'line1\nline2';
    const result = fuzzyFindAndReplace(content, 'line1\\nline2', 'REPL');
    assert.equal(result.error, null);
    assert.equal(result.strategy, 'escape_normalized');
    assert.equal(result.newContent, 'REPL');
  });

  it('escape_normalized maps offsets through a length-changing content (backslash-n in content)', () => {
    // Content itself contains a literal "\n" sequence, so normalizeEscapes
    // shortens it and buildPositionMap must walk to remap offsets.
    const content = 'pre a\\nb post';
    const result = fuzzyFindAndReplace(content, 'a\nb', 'X');
    assert.equal(result.error, null);
    assert.equal(result.strategy, 'escape_normalized');
    assert.equal(result.newContent, 'pre X post');
  });

  it('escape_normalized remaps offsets around an em-dash expansion and a smart quote', () => {
    // Em-dash expands 1->2 chars and backslash-n contracts 2->1, forcing the
    // full buildPositionMap walk through both branches plus the smart-quote arm.
    const content = "x\\ny—z’q\\n";
    const result = fuzzyFindAndReplace(content, 'y--z', 'MID');
    assert.equal(result.error, null);
    assert.equal(result.strategy, 'escape_normalized');
    assert.match(result.newContent ?? '', /MID/);
  });

  it('invokes indentation_flexible (and escape_normalized) on a multi-line no-match', () => {
    // No strategy matches, so every strategy function — including
    // findIndentationFlexible and findEscapeNormalized — is invoked over
    // multiple content lines.
    const content = '    alpha\n    beta\n    gamma';
    const result = fuzzyFindAndReplace(content, 'delta\nepsilon', 'X');
    assert.equal(result.newContent, null);
    assert.equal(result.strategy, null);
    assert.match(result.error ?? '', /not found/);
  });

  it('reports multiple matches for a non-exact strategy', () => {
    const content = '   dup\n   dup';
    const result = fuzzyFindAndReplace(content, 'dup', 'x');
    assert.ok(result.matchCount >= 2);
    assert.equal(result.newContent, null);
    assert.match(result.error ?? '', /locations/);
  });

  it('preserves curly double quotes in the replacement', () => {
    const content = 'say “hi” now';
    const result = fuzzyFindAndReplace(content, 'say “hi” now', 'say "bye" now');
    assert.equal(result.error, null);
    assert.equal(result.strategy, 'exact');
    // ASCII double quotes in newString were converted back to curly quotes.
    assert.ok(/“bye”/.test(result.newContent ?? ''), result.newContent ?? '');
  });

  it('preserves curly single quotes in the replacement', () => {
    const content = 'it‘s’ here';
    const result = fuzzyFindAndReplace(content, 'it‘s’ here', "it's' there");
    assert.equal(result.error, null);
    assert.ok(/‘/.test(result.newContent ?? '') || /’/.test(result.newContent ?? ''), result.newContent ?? '');
  });

  it('does not re-curl when the replacement already carries a curly quote', () => {
    const content = 'q “x”';
    const result = fuzzyFindAndReplace(content, 'q “x”', 'q “y” and "z"');
    assert.equal(result.error, null);
    // Replacement already included a curly double quote, so ASCII " is left alone.
    assert.ok((result.newContent ?? '').includes('"z"'));
  });

  it('reindentReplacement removes indentation when the file is less-indented (multi-line)', () => {
    const fileRegion = 'a\n  b\n  c';
    const oldString = '    a\n      b\n      c';
    const newString = '    a\n      b\n      c';
    const result = reindentReplacement(fileRegion, oldString, newString);
    const lines = result.split('\n');
    assert.equal(lines[0], 'a');
    // Each subsequent line had 4 spaces of indent removed.
    assert.equal(lines[1], '  b');
    assert.equal(lines[2], '  c');
  });

  it('reindentReplacement leaves deeper lines unchanged when delta is zero after first line', () => {
    // Same base indent -> returns newString verbatim (early-return branch).
    const result = reindentReplacement('  a\n    b', '  a\n    b', '  A\n    B');
    assert.equal(result, '  A\n    B');
  });

  it('exercises findExact empty-pattern guard while line_trimmed wins on a blank line', () => {
    // findExact returns [] for an empty pattern (its length guard), so the
    // search proceeds; line_trimmed then matches the single blank line uniquely.
    const result = fuzzyFindAndReplace('abc\n\ndef', '', 'INSERTED');
    assert.equal(result.error, null);
    assert.ok((result.newContent ?? '').includes('INSERTED'));
  });

  it('maps byte offsets for a line-strategy match that is not the first line', () => {
    // exact fails (indent between lines), line_trimmed matches at content line 1,
    // forcing findByLine to sum the lengths of preceding lines.
    const content = 'first line\n    second\n    third';
    const result = fuzzyFindAndReplace(content, 'second\nthird', 'SECOND\nTHIRD');
    assert.equal(result.error, null);
    assert.match(result.newContent ?? '', /first line/);
    assert.match(result.newContent ?? '', /SECOND/);
    assert.match(result.newContent ?? '', /THIRD/);
  });

  it('reindentReplacement replaces the first line when it lacks the old base indent', () => {
    // newString's first line ("other") does not start with oldBaseIndent ("    "),
    // so the else arm (fileBaseIndent + trimStart) runs.
    const result = reindentReplacement('      x', '    x', 'other');
    assert.equal(result, '      other');
  });

  it('reindentReplacement leaves non-first lines unchanged when indent width is equal but characters differ', () => {
    // file base indent is a single space, old base indent is a single tab: not
    // string-equal (no early return) but deltaLen === 0, so deeper lines are
    // returned verbatim.
    const fileRegion = ' a\n    deep';
    const oldString = '\ta\n    deep';
    const newString = '\tA\n    DEEP';
    const result = reindentReplacement(fileRegion, oldString, newString);
    const lines = result.split('\n');
    assert.equal(lines[1], '    DEEP'); // unchanged (deltaLen 0)
  });

  it('findClosestLines sorts multiple candidates by similarity', () => {
    const content = 'fooBarBaz\nfooBarQux\nfooBarBat\ntotallyDifferent';
    const results = findClosestLines(content, ['fooBarBaz'], 5);
    assert.ok(results.length >= 2);
    for (let i = 1; i < results.length; i++) {
      assert.ok(results[i - 1].similarity >= results[i].similarity);
    }
  });
});

// ===========================================================================
// path-suggest.ts — uncovered branches/functions
// ===========================================================================

describe('path-suggest — uncovered paths', () => {
  let tmpDir: string;
  let origCwd: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-psug-'));
    origCwd = process.cwd();
  });

  afterEach(() => {
    process.chdir(origCwd);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('resolves a relative missing path against cwd', () => {
    fs.writeFileSync(path.join(tmpDir, 'config.ts'), '');
    // Relative input exercises the `path.resolve(cwd, missingPath)` branch.
    const suggestions = suggestSimilarPaths('configg.ts', tmpDir);
    assert.ok(suggestions.some((s) => s.endsWith('config.ts')), JSON.stringify(suggestions));
  });

  it('sorts multiple candidates by edit distance', () => {
    // Two files within edit distance of the query forces the sort comparator.
    fs.writeFileSync(path.join(tmpDir, 'foo1.ts'), '');
    fs.writeFileSync(path.join(tmpDir, 'foo2.ts'), '');
    fs.writeFileSync(path.join(tmpDir, 'fo.ts'), '');
    const suggestions = suggestSimilarPaths(path.join(tmpDir, 'foo.ts'), tmpDir);
    assert.ok(suggestions.length >= 2, JSON.stringify(suggestions));
    assert.ok(suggestions.every((s) => s.startsWith(tmpDir)));
  });
});

// ===========================================================================
// synthetic-output.ts — uncovered branches
// ===========================================================================

describe('synthetic-output — uncovered paths', () => {
  it('returns an error when the data is not JSON-serializable (BigInt)', async () => {
    const result = await syntheticOutputTool.execute!(
      { data: { n: 10n } },
      {} as ToolContext,
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /Error serializing structured output/);
  });

  it('returns an error for circular data', async () => {
    const circular: Record<string, unknown> = {};
    circular.self = circular;
    const result = await syntheticOutputTool.execute!(
      { data: circular },
      {} as ToolContext,
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /Error serializing structured output/);
  });
});

// ===========================================================================
// spill-output.ts — remaining branches
// ===========================================================================

describe('spill-output — uncovered paths', () => {
  it('buildSpillBanner reports ENOSPC distinctly', () => {
    const banner = buildSpillBanner(123, null, 'ENOSPC');
    assert.match(banner, /disk full — ENOSPC/);
    assert.match(banner, /123 chars/);
  });

  it('buildSpillBanner reports a generic persist failure', () => {
    const banner = buildSpillBanner(5, null, 'EACCES');
    assert.match(banner, /could not persist to disk/);
  });

  it('buildSpillBanner reports the spill file path when persisted', () => {
    const banner = buildSpillBanner(9, '/tmp/x.txt', undefined);
    assert.match(banner, /persisted to \/tmp\/x\.txt/);
  });

  it('falls back to process.cwd() when cwd is empty', () => {
    const prev = process.cwd();
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-spill-cwd-'));
    process.chdir(dir);
    try {
      const text = 'c'.repeat(3000);
      const result = spillToolText('', 'cwdfallback', text, 1000);
      assert.ok(result.length < text.length);
      // The spill directory was created under process.cwd() (the empty-cwd fallback).
      const spillDir = path.join(dir, '.superinference', 'tool-results');
      assert.ok(fs.existsSync(spillDir));
      assert.match(result, /persisted to/);
    } finally {
      process.chdir(prev);
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('backs off a lone high surrogate at the head boundary', () => {
    // limit=150 -> headLen = floor(150*0.67) = 100, so index 99 is the last head
    // char. Put the emoji's high surrogate exactly at index 99 so the back-off
    // branch (charCodeAt(headLen-1) is a high surrogate) fires.
    const emoji = '\u{1F600}'; // high surrogate 0xD83D, low 0xDE00
    const text = 'a'.repeat(99) + emoji + 'b'.repeat(2000);
    assert.ok(text.charCodeAt(99) >= 0xd800 && text.charCodeAt(99) <= 0xdbff);
    const result = spillToolText(os.tmpdir(), 'surr-head', text, 150);
    for (let i = 0; i < result.length; i++) {
      const code = result.charCodeAt(i);
      if (code >= 0xd800 && code <= 0xdbff) {
        const next = result.charCodeAt(i + 1);
        assert.ok(next >= 0xdc00 && next <= 0xdfff, `lone high surrogate at ${i}`);
      }
    }
  });

  it('skips a lone low surrogate at the tail boundary', () => {
    // limit=900 -> headLen = floor(603) = 603, tailLen = 900-603-200 = 97, so
    // tailStart = length-97. Arrange an emoji's low surrogate exactly there so
    // the tail skip branch (charCodeAt(tailStart) is a low surrogate) fires.
    const emoji = '\u{1F680}'; // high 0xD83D, low 0xDE80
    const tail = 97;
    const totalLen = 4000;
    // tailStart = totalLen - tail. Put the high surrogate at tailStart-1 and the
    // low surrogate at tailStart so the boundary lands on the low surrogate.
    const prefixLen = totalLen - tail - 1;
    const text = 'a'.repeat(prefixLen) + emoji + 'z'.repeat(tail - 1);
    assert.equal(text.length, totalLen);
    const tailStart = totalLen - tail;
    assert.ok(text.charCodeAt(tailStart) >= 0xdc00 && text.charCodeAt(tailStart) <= 0xdfff);
    const result = spillToolText(os.tmpdir(), 'surr-tail', text, 900);
    for (let i = 0; i < result.length; i++) {
      const code = result.charCodeAt(i);
      if (code >= 0xdc00 && code <= 0xdfff) {
        const prev = i > 0 ? result.charCodeAt(i - 1) : 0;
        assert.ok(prev >= 0xd800 && prev <= 0xdbff, `lone low surrogate at ${i}`);
      }
    }
  });
});
