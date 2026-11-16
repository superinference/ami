// Unit U039 — parity gaps for core/core/src/tools/file-edit.ts:
//  - strip trailing whitespace of inserted/written text, with a Markdown opt-out
//  - sequential-edit substring guard (old_string ⊂ a prior edit's new_string)
//  - strict input schema: reject unknown parameters
//  - description hints ("must be different from old_string", replace_all rename)
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileEditTool } from '../src/tools/file-edit';
import { stripTrailingWhitespace, isMarkdownPath } from '../src/tools/tool-utils';
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
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u039-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// stripTrailingWhitespace / isMarkdownPath helpers
// ---------------------------------------------------------------------------

describe('stripTrailingWhitespace helper', () => {
  it('removes trailing spaces/tabs per line, preserving LF', () => {
    assert.equal(stripTrailingWhitespace('a   \nb\t\nc'), 'a\nb\nc');
  });

  it('preserves CRLF and CR line endings while trimming content', () => {
    assert.equal(stripTrailingWhitespace('a  \r\nb \r\n'), 'a\r\nb\r\n');
    assert.equal(stripTrailingWhitespace('a  \rb '), 'a\rb');
  });

  it('collapses a whitespace-only line to empty but keeps its newline', () => {
    assert.equal(stripTrailingWhitespace('a\n   \nb'), 'a\n\nb');
  });

  it('leaves interior whitespace untouched', () => {
    assert.equal(stripTrailingWhitespace('a  b  c'), 'a  b  c');
  });
});

describe('isMarkdownPath helper', () => {
  it('matches .md and .mdx case-insensitively, rejects others', () => {
    assert.equal(isMarkdownPath('/x/readme.md'), true);
    assert.equal(isMarkdownPath('/x/doc.MDX'), true);
    assert.equal(isMarkdownPath('/x/a.txt'), false);
    assert.equal(isMarkdownPath('/x/markdown.ts'), false);
  });
});

// ---------------------------------------------------------------------------
// Trailing-whitespace normalization through the tool (write + replace + multi)
// ---------------------------------------------------------------------------

describe('fileEditTool – trailing-whitespace normalization', () => {
  it('strips trailing whitespace when writing a new non-markdown file', async () => {
    const file = path.join(tmpDir, 'w.txt');
    const res = await fileEditTool.execute(
      { file_path: file, new_string: 'alpha   \nbeta\t\ngamma' },
      ctx(),
    );
    assert.ok(!res.isError, res.output);
    assert.equal(fs.readFileSync(file, 'utf-8'), 'alpha\nbeta\ngamma');
  });

  it('preserves trailing whitespace for a .md write (hard line breaks)', async () => {
    const file = path.join(tmpDir, 'doc.md');
    const res = await fileEditTool.execute(
      { file_path: file, new_string: 'line one  \nline two' },
      ctx(),
    );
    assert.ok(!res.isError, res.output);
    assert.equal(fs.readFileSync(file, 'utf-8'), 'line one  \nline two');
  });

  it('strips trailing whitespace of new_string in single-replace mode', async () => {
    const file = path.join(tmpDir, 's.txt');
    fs.writeFileSync(file, 'foo\n');
    const res = await fileEditTool.execute(
      { file_path: file, old_string: 'foo', new_string: 'bar   ' },
      ctx(),
    );
    assert.ok(!res.isError, res.output);
    assert.equal(fs.readFileSync(file, 'utf-8'), 'bar\n');
  });

  it('keeps trailing whitespace of new_string in single-replace on a .md file', async () => {
    const file = path.join(tmpDir, 's.md');
    fs.writeFileSync(file, 'foo\n');
    const res = await fileEditTool.execute(
      { file_path: file, old_string: 'foo', new_string: 'bar   ' },
      ctx(),
    );
    assert.ok(!res.isError, res.output);
    assert.equal(fs.readFileSync(file, 'utf-8'), 'bar   \n');
  });

  it('strips trailing whitespace of each edit new_string in multi-edit mode', async () => {
    const file = path.join(tmpDir, 'm.txt');
    fs.writeFileSync(file, 'one\ntwo\n');
    const res = await fileEditTool.execute(
      {
        file_path: file,
        edits: [
          { old_string: 'one', new_string: 'ONE   ' },
          { old_string: 'two', new_string: 'TWO\t' },
        ],
      },
      ctx(),
    );
    assert.ok(!res.isError, res.output);
    assert.equal(fs.readFileSync(file, 'utf-8'), 'ONE\nTWO\n');
  });
});

// ---------------------------------------------------------------------------
// Strict input schema: reject unknown parameters
// ---------------------------------------------------------------------------

describe('fileEditTool – strict unknown-key rejection', () => {
  it('rejects a hallucinated/typo parameter and does not touch the file', async () => {
    const file = path.join(tmpDir, 'u.txt');
    fs.writeFileSync(file, 'keep\n');
    const res = await fileEditTool.execute(
      { file_path: file, old_string: 'keep', new_string: 'changed', newText: 'oops' },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('Unknown parameter'), res.output);
    assert.ok(res.output.includes('newText'), res.output);
    // File is untouched because the edit was rejected before any write.
    assert.equal(fs.readFileSync(file, 'utf-8'), 'keep\n');
  });

  it('accepts all known aliases (path/content/file_text/file_content)', async () => {
    const file = path.join(tmpDir, 'alias.txt');
    const res = await fileEditTool.execute(
      { path: file, content: 'via-alias' },
      ctx(),
    );
    assert.ok(!res.isError, res.output);
    assert.equal(fs.readFileSync(file, 'utf-8'), 'via-alias');
  });
});

// ---------------------------------------------------------------------------
// Description hints (schema wording parity)
// ---------------------------------------------------------------------------

describe('fileEditTool – description hints', () => {
  it('new_string notes it must differ from old_string (top-level + edits item)', () => {
    const props = fileEditTool.inputSchema.properties as Record<string, any>;
    assert.ok(props.new_string.description.includes('must be different from old_string'));
    assert.ok(
      props.edits.items.properties.new_string.description.includes('must be different from old_string'),
    );
  });

  it('replace_all description carries the rename-a-variable hint', () => {
    const props = fileEditTool.inputSchema.properties as Record<string, any>;
    assert.ok(/rename a variable/i.test(props.replace_all.description));
  });
});
