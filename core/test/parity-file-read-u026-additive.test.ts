import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  fileReadTool,
  convertLeadingTabsToSpaces,
  FILE_UNCHANGED_STUB,
  FILE_NOT_FOUND_CWD_NOTE,
} from '../src/tools/file-read';
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

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-fr-u026-add-'));
  getFileCache(tmpDir).clear();
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Gap: tool description advertises Jupyter notebook (.ipynb) + PDF support
// (so the model routes .ipynb/.pdf reads to the richer built-in path, not bash)
// ---------------------------------------------------------------------------
describe('fileReadTool – description advertises notebook/PDF/directory capabilities', () => {
  it('mentions Jupyter notebooks (.ipynb) with cells + outputs', () => {
    assert.match(fileReadTool.description, /Jupyter notebooks? \(\.ipynb\)/);
    assert.match(fileReadTool.description, /cells/i);
  });

  it('mentions PDF reading with the large-PDF pages requirement + 20-page cap', () => {
    assert.match(fileReadTool.description, /PDF files? \(\.pdf\)/);
    assert.match(fileReadTool.description, /pages parameter/);
    assert.match(fileReadTool.description, /20 pages/);
  });

  it('tells the model to use ls via bash for a directory', () => {
    assert.match(fileReadTool.description, /ls command via the bash tool/);
  });
});

// ---------------------------------------------------------------------------
// Gap: directory-read error points the model to list a directory via bash/ls
// ---------------------------------------------------------------------------
describe('fileReadTool – directory error redirects to bash ls', () => {
  it('redirects a directory read to an ls command via bash instead of dead-ending', async () => {
    const result = await fileReadTool.execute({ file_path: tmpDir }, ctx());
    assert.equal(result.isError, true);
    assert.match(result.output, /is a directory, not a file/);
    assert.match(result.output, /ls command via the bash tool/);
  });
});

// ---------------------------------------------------------------------------
// Gap: "File unchanged" stub carries actionable guidance (reuse earlier result)
// ---------------------------------------------------------------------------
describe('fileReadTool – FILE_UNCHANGED_STUB actionable guidance', () => {
  it('the stub constant tells the model to reuse the earlier tool_result', () => {
    assert.match(FILE_UNCHANGED_STUB, /refer to that instead of re-reading/);
  });

  it('a full re-read of an unchanged file returns the actionable stub', async () => {
    const file = path.join(tmpDir, 'cached.txt');
    fs.writeFileSync(file, 'alpha\nbeta\n');
    await fileReadTool.execute({ file_path: file }, ctx());
    const r2 = await fileReadTool.execute({ file_path: file }, ctx());
    assert.ok(r2.output.includes(FILE_UNCHANGED_STUB), r2.output);
    assert.match(r2.output, /refer to that instead of re-reading/);
    // AMI differentiator preserved: the path + line count still travel with it.
    assert.ok(r2.output.includes(file), r2.output);
  });

  it('an identical repeat range read returns the actionable stub', async () => {
    const file = path.join(tmpDir, 'ranged.txt');
    fs.writeFileSync(file, Array.from({ length: 20 }, (_, i) => `line ${i + 1}`).join('\n') + '\n');
    await fileReadTool.execute({ file_path: file, offset: 2, limit: 3 }, ctx());
    const r2 = await fileReadTool.execute({ file_path: file, offset: 2, limit: 3 }, ctx());
    assert.ok(r2.output.includes(FILE_UNCHANGED_STUB), r2.output);
    assert.match(r2.output, /lines 3-5/);
  });
});

// ---------------------------------------------------------------------------
// Gap: file-not-found error unconditionally includes a cwd note
// ---------------------------------------------------------------------------
describe('fileReadTool – FILE_NOT_FOUND_CWD_NOTE on not-found', () => {
  it('appends the cwd note even with no similar/under-cwd suggestion', async () => {
    const missing = path.join(tmpDir, 'definitely-no-such-file-xyz.txt');
    const result = await fileReadTool.execute({ file_path: missing }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes(`${FILE_NOT_FOUND_CWD_NOTE} ${tmpDir}.`), result.output);
  });

  it('appends the cwd note even when a similar-name suggestion is also present', async () => {
    // A near-miss sibling file in cwd triggers the suggestSimilarPaths hint; the
    // unconditional cwd note must still be present alongside it.
    fs.writeFileSync(path.join(tmpDir, 'report.txt'), 'x\n');
    const typo = path.join(tmpDir, 'reports.txt');
    const result = await fileReadTool.execute({ file_path: typo }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes(`${FILE_NOT_FOUND_CWD_NOTE} ${tmpDir}.`), result.output);
  });
});

// ---------------------------------------------------------------------------
// Gap: normalize leading tabs to spaces in rendered file content
// ---------------------------------------------------------------------------
describe('convertLeadingTabsToSpaces', () => {
  it('replaces each leading tab with 2 spaces', () => {
    assert.equal(convertLeadingTabsToSpaces('\tfoo'), '  foo');
    assert.equal(convertLeadingTabsToSpaces('\t\tbar'), '    bar');
  });

  it('normalizes leading tabs per line across a multi-line string', () => {
    assert.equal(convertLeadingTabsToSpaces('\tfoo\n\t\tbar'), '  foo\n    bar');
  });

  it('fast-paths (returns the same string) when there is no tab', () => {
    const s = 'no tabs here';
    assert.equal(convertLeadingTabsToSpaces(s), s);
  });

  it('leaves NON-leading (mid-line) tabs untouched', () => {
    assert.equal(convertLeadingTabsToSpaces('a\tb'), 'a\tb');
    assert.equal(convertLeadingTabsToSpaces('\ta\tb'), '  a\tb');
  });
});

describe('fileReadTool – rendered content has tab-width-independent indentation', () => {
  it('full read converts leading tabs to spaces in the content (gutter tab preserved)', async () => {
    const file = path.join(tmpDir, 'indent.py');
    fs.writeFileSync(file, 'def f():\n\tif x:\n\t\treturn 1\n');
    const result = await fileReadTool.execute({ file_path: file }, ctx());
    assert.ok(result.output.includes('  if x:'), result.output);
    assert.ok(result.output.includes('    return 1'), result.output);
    // The raw tab-prefixed source lines must NOT survive into the output.
    assert.ok(!result.output.includes('\tif x:'), result.output);
    assert.ok(!result.output.includes('\t\treturn 1'), result.output);
  });

  it('range read converts leading tabs to spaces in the content', async () => {
    const file = path.join(tmpDir, 'indent-range.py');
    fs.writeFileSync(file, 'def f():\n\tif x:\n\t\treturn 1\nq = 2\n');
    const result = await fileReadTool.execute({ file_path: file, offset: 1, limit: 2 }, ctx());
    assert.ok(result.output.includes('  if x:'), result.output);
    assert.ok(result.output.includes('    return 1'), result.output);
    assert.ok(!result.output.includes('\tif x:'), result.output);
  });
});
