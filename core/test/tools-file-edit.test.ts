import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileEditTool } from '../src/tools/file-edit';
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
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-file-edit-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Tool definition
// ---------------------------------------------------------------------------

describe('fileEditTool – definition', () => {
  it('has the correct name', () => {
    assert.equal(fileEditTool.name, 'file_edit');
  });

  it('is not read-only', () => {
    assert.equal(fileEditTool.isReadOnly, false);
  });

  it('schema requires file_path, old_string, new_string', () => {
    const req = fileEditTool.inputSchema.required;
    assert.ok(req?.includes('file_path'));
    assert.ok(req?.includes('old_string'));
    assert.ok(req?.includes('new_string'));
  });
});

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

describe('fileEditTool – validation', () => {
  it('rejects empty file_path', async () => {
    const result = await fileEditTool.execute(
      { file_path: '', old_string: 'a', new_string: 'b' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('file_path must not be empty'));
  });

  it('rejects null old_string', async () => {
    const result = await fileEditTool.execute(
      { file_path: '/tmp/f.txt', old_string: null, new_string: 'b' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('old_string must be provided'));
  });

  it('rejects empty old_string', async () => {
    const result = await fileEditTool.execute(
      { file_path: '/tmp/f.txt', old_string: '', new_string: 'b' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('old_string must not be empty'));
  });

  it('rejects null new_string', async () => {
    const result = await fileEditTool.execute(
      { file_path: '/tmp/f.txt', old_string: 'a', new_string: null },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('new_string must be provided'));
  });

  it('rejects identical old_string and new_string', async () => {
    const result = await fileEditTool.execute(
      { file_path: '/tmp/f.txt', old_string: 'same', new_string: 'same' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('identical'));
  });
});

// ---------------------------------------------------------------------------
// Successful edits
// ---------------------------------------------------------------------------

describe('fileEditTool – successful edits', () => {
  it('replaces exact match in file', async () => {
    const file = path.join(tmpDir, 'test.ts');
    fs.writeFileSync(file, 'const x = 1;\nconst y = 2;\n');

    const result = await fileEditTool.execute(
      { file_path: file, old_string: 'const x = 1;', new_string: 'const x = 10;' },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('Successfully edited'));
    const content = fs.readFileSync(file, 'utf-8');
    assert.ok(content.includes('const x = 10;'));
    assert.ok(content.includes('const y = 2;'));
  });

  it('handles multi-line replacements', async () => {
    const file = path.join(tmpDir, 'multi.ts');
    fs.writeFileSync(file, 'line1\nline2\nline3\n');

    const result = await fileEditTool.execute(
      { file_path: file, old_string: 'line1\nline2', new_string: 'replaced1\nreplaced2' },
      ctx(),
    );
    assert.ok(!result.isError);
    const content = fs.readFileSync(file, 'utf-8');
    assert.ok(content.includes('replaced1\nreplaced2'));
    assert.ok(content.includes('line3'));
  });

  it('resolves relative paths', async () => {
    const file = path.join(tmpDir, 'rel.ts');
    fs.writeFileSync(file, 'old content\n');

    const result = await fileEditTool.execute(
      { file_path: 'rel.ts', old_string: 'old content', new_string: 'new content' },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.equal(fs.readFileSync(file, 'utf-8'), 'new content\n');
  });

  it('shows unified diff in output', async () => {
    const file = path.join(tmpDir, 'diff.ts');
    fs.writeFileSync(file, 'alpha\nbeta\ngamma\n');

    const result = await fileEditTool.execute(
      { file_path: file, old_string: 'beta', new_string: 'BETA' },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('-beta'));
    assert.ok(result.output.includes('+BETA'));
  });
});

// ---------------------------------------------------------------------------
// Error cases
// ---------------------------------------------------------------------------

describe('fileEditTool – error cases', () => {
  it('returns error when file does not exist', async () => {
    const result = await fileEditTool.execute(
      { file_path: '/nonexistent/file.ts', old_string: 'a', new_string: 'b' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Error reading file'));
  });

  it('returns error when old_string not found', async () => {
    const file = path.join(tmpDir, 'notfound.ts');
    fs.writeFileSync(file, 'hello world\n');

    const result = await fileEditTool.execute(
      { file_path: file, old_string: 'nonexistent', new_string: 'replacement' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('not found'));
  });

  it('provides "did you mean" hints when old_string not found', async () => {
    const file = path.join(tmpDir, 'hints.ts');
    fs.writeFileSync(file, 'const fooBar = 1;\nconst bazQux = 2;\n');

    const result = await fileEditTool.execute(
      { file_path: file, old_string: 'const fooBaz = 1;', new_string: 'x' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Did you mean'));
  });

  it('returns error for multiple matches', async () => {
    const file = path.join(tmpDir, 'dup.ts');
    fs.writeFileSync(file, 'hello\nworld\nhello\n');

    const result = await fileEditTool.execute(
      { file_path: file, old_string: 'hello', new_string: 'hi' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('found') && result.output.includes('times'));
  });
});

// ---------------------------------------------------------------------------
// Fuzzy matching
// ---------------------------------------------------------------------------

describe('fileEditTool – fuzzy matching', () => {
  it('matches with trimmed whitespace (line_trimmed strategy)', async () => {
    const file = path.join(tmpDir, 'fuzzy.ts');
    fs.writeFileSync(file, '  const x = 1;  \n');

    const result = await fileEditTool.execute(
      { file_path: file, old_string: 'const x = 1;', new_string: 'const x = 2;' },
      ctx(),
    );
    assert.ok(!result.isError);
    // Should note the fuzzy strategy used
    const content = fs.readFileSync(file, 'utf-8');
    assert.ok(content.includes('const x = 2;'));
  });
});
