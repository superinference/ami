import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileEditTool } from '../src/tools/file-edit';
import type { ToolContext } from '../src/types';
import { fuzzyFindAndReplace } from '../src/tools/fuzzy-match';

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-edit-uniq-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// fuzzyFindAndReplace — uniqueness messages
// ---------------------------------------------------------------------------

describe('fuzzyFindAndReplace — uniqueness validation', () => {
  it('single match succeeds with newContent', () => {
    const result = fuzzyFindAndReplace('hello world', 'hello', 'goodbye');
    assert.equal(result.error, null);
    assert.equal(result.matchCount, 1);
    assert.equal(result.newContent, 'goodbye world');
  });

  it('zero matches returns matchCount=0 and error', () => {
    const result = fuzzyFindAndReplace('hello world', 'missing', 'new');
    assert.equal(result.matchCount, 0);
    assert.ok(result.error);
    assert.ok(result.error.includes('not found'));
  });

  it('multiple matches returns error with count', () => {
    const content = 'foo bar foo baz foo';
    const result = fuzzyFindAndReplace(content, 'foo', 'qux');
    assert.equal(result.matchCount, 3);
    assert.ok(result.error);
    assert.ok(result.error.includes('3 locations'));
  });

  it('error message mentions replace_all: true', () => {
    const content = 'abc\nabc\n';
    const result = fuzzyFindAndReplace(content, 'abc', 'xyz');
    assert.ok(result.error);
    assert.ok(result.error.includes('replace_all: true'));
  });

  it('error message suggests adding more context', () => {
    const content = 'abc\nabc\n';
    const result = fuzzyFindAndReplace(content, 'abc', 'xyz');
    assert.ok(result.error);
    assert.ok(result.error.includes('more surrounding context'));
  });

  it('2 matches reports count=2', () => {
    const result = fuzzyFindAndReplace('aa bb aa', 'aa', 'cc');
    assert.equal(result.matchCount, 2);
  });

  it('5 matches reports count=5', () => {
    const content = Array(5).fill('dup').join(' x ');
    const result = fuzzyFindAndReplace(content, 'dup', 'new');
    assert.equal(result.matchCount, 5);
  });

  it('10 matches reports count=10', () => {
    const content = Array(10).fill('pattern').join('\n');
    const result = fuzzyFindAndReplace(content, 'pattern', 'replaced');
    assert.equal(result.matchCount, 10);
  });
});

// ---------------------------------------------------------------------------
// fileEditTool — single-edit uniqueness
// ---------------------------------------------------------------------------

describe('fileEditTool — single-edit uniqueness', () => {
  it('single match succeeds', async () => {
    const file = path.join(tmpDir, 'single.txt');
    fs.writeFileSync(file, 'alpha beta gamma\n');
    const result = await fileEditTool.execute(
      { file_path: file, old_string: 'beta', new_string: 'BETA' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.equal(fs.readFileSync(file, 'utf-8'), 'alpha BETA gamma\n');
  });

  it('zero matches returns not-found error', async () => {
    const file = path.join(tmpDir, 'zero.txt');
    fs.writeFileSync(file, 'alpha beta gamma\n');
    const result = await fileEditTool.execute(
      { file_path: file, old_string: 'missing', new_string: 'new' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('not found'));
  });

  it('multiple matches returns error with count', async () => {
    const file = path.join(tmpDir, 'multi.txt');
    fs.writeFileSync(file, 'line one\nline two\nline three\n');
    const result = await fileEditTool.execute(
      { file_path: file, old_string: 'line', new_string: 'row' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('3 locations'), `Expected '3 locations' in: ${result.output}`);
  });

  it('error message suggests replace_all: true', async () => {
    const file = path.join(tmpDir, 'suggest.txt');
    fs.writeFileSync(file, 'dup dup\n');
    const result = await fileEditTool.execute(
      { file_path: file, old_string: 'dup', new_string: 'unique' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('replace_all: true'));
  });

  it('error message suggests more context', async () => {
    const file = path.join(tmpDir, 'context.txt');
    fs.writeFileSync(file, 'x y\nx y\n');
    const result = await fileEditTool.execute(
      { file_path: file, old_string: 'x y', new_string: 'a b' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('more surrounding context') || result.output.includes('more context'));
  });

  it('replace_all: true allows multiple matches', async () => {
    const file = path.join(tmpDir, 'replall.txt');
    fs.writeFileSync(file, 'old old old\n');
    const result = await fileEditTool.execute(
      { file_path: file, old_string: 'old', new_string: 'new', replace_all: true },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.equal(fs.readFileSync(file, 'utf-8'), 'new new new\n');
  });

  it('replace_all reports correct occurrence count', async () => {
    const file = path.join(tmpDir, 'count.txt');
    fs.writeFileSync(file, 'abc abc abc abc abc\n');
    const result = await fileEditTool.execute(
      { file_path: file, old_string: 'abc', new_string: 'xyz', replace_all: true },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.ok(result.output.includes('5 occurrences'));
  });

  it('empty old_string enters write mode — not an edit error', async () => {
    const file = path.join(tmpDir, 'empty.txt');
    const result = await fileEditTool.execute(
      { file_path: file, old_string: '', new_string: 'created content' },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(fs.existsSync(file));
    assert.equal(fs.readFileSync(file, 'utf-8'), 'created content');
  });
});

// ---------------------------------------------------------------------------
// fileEditTool — sequential edits uniqueness
// ---------------------------------------------------------------------------

describe('fileEditTool — sequential edits uniqueness', () => {
  it('ambiguous edit in sequence fails with count', async () => {
    const file = path.join(tmpDir, 'seq.txt');
    fs.writeFileSync(file, 'foo bar\nfoo baz\n');
    const result = await fileEditTool.execute(
      {
        file_path: file,
        edits: [
          { old_string: 'foo', new_string: 'qux' },
        ],
      },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('2 locations'), `Expected '2 locations' in: ${result.output}`);
  });

  it('sequential edit with replace_all: true succeeds', async () => {
    const file = path.join(tmpDir, 'seq-all.txt');
    fs.writeFileSync(file, 'x y\nx y\nx y\n');
    const result = await fileEditTool.execute(
      {
        file_path: file,
        edits: [
          { old_string: 'x y', new_string: 'a b', replace_all: true },
        ],
      },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.equal(fs.readFileSync(file, 'utf-8'), 'a b\na b\na b\n');
  });

  it('sequential edit suggests replace_all in error', async () => {
    const file = path.join(tmpDir, 'seq-suggest.txt');
    fs.writeFileSync(file, 'dup\ndup\n');
    const result = await fileEditTool.execute(
      {
        file_path: file,
        edits: [
          { old_string: 'dup', new_string: 'uniq' },
        ],
      },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('replace_all: true'));
  });

  it('sequential edit with empty old_string is rejected', async () => {
    const file = path.join(tmpDir, 'seq-empty.txt');
    fs.writeFileSync(file, 'content\n');
    const result = await fileEditTool.execute(
      {
        file_path: file,
        edits: [
          { old_string: '', new_string: 'new' },
        ],
      },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('old_string must not be empty'));
  });
});
