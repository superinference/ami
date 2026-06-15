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

  it('rejects empty old_string on non-empty file', async () => {
    const file = path.join(tmpDir, 'nonempty.txt');
    fs.writeFileSync(file, 'existing content\n');

    const result = await fileEditTool.execute(
      { file_path: file, old_string: '', new_string: 'b' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('old_string is empty but file has content'));
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
      { file_path: path.join(tmpDir, 'nonexistent', 'file.ts'), old_string: 'a', new_string: 'b' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('File not found') || result.output.includes('Error reading file'));
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

// ---------------------------------------------------------------------------
// Read-before-write enforcement
// ---------------------------------------------------------------------------

describe('fileEditTool – read-before-write enforcement', () => {
  it('blocks edit when filesRead is set but file was not read', async () => {
    const file = path.join(tmpDir, 'unread.ts');
    fs.writeFileSync(file, 'const x = 1;\n');

    const result = await fileEditTool.execute(
      { file_path: file, old_string: 'const x = 1;', new_string: 'const x = 2;' },
      ctx({ filesRead: new Set() }),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('must read'));
  });

  it('allows edit when file was previously read', async () => {
    const file = path.join(tmpDir, 'wasread.ts');
    fs.writeFileSync(file, 'const x = 1;\n');
    const filesRead = new Set([file]);

    const result = await fileEditTool.execute(
      { file_path: file, old_string: 'const x = 1;', new_string: 'const x = 2;' },
      ctx({ filesRead }),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('Successfully edited'));
  });

  it('skips enforcement when filesRead is undefined', async () => {
    const file = path.join(tmpDir, 'notracking.ts');
    fs.writeFileSync(file, 'const x = 1;\n');

    const result = await fileEditTool.execute(
      { file_path: file, old_string: 'const x = 1;', new_string: 'const x = 2;' },
      ctx(),
    );
    assert.ok(!result.isError);
  });

  it('resolves relative path for enforcement check', async () => {
    const file = path.join(tmpDir, 'relative.ts');
    fs.writeFileSync(file, 'old\n');
    const filesRead = new Set([file]);

    const result = await fileEditTool.execute(
      { file_path: 'relative.ts', old_string: 'old', new_string: 'new' },
      ctx({ filesRead }),
    );
    assert.ok(!result.isError);
  });

  it('does not modify file when blocked by read-before-write', async () => {
    const file = path.join(tmpDir, 'protected.ts');
    fs.writeFileSync(file, 'original\n');

    await fileEditTool.execute(
      { file_path: file, old_string: 'original', new_string: 'modified' },
      ctx({ filesRead: new Set() }),
    );
    assert.equal(fs.readFileSync(file, 'utf-8'), 'original\n');
  });
});

// ---------------------------------------------------------------------------
// CRLF-aware file editing
// ---------------------------------------------------------------------------

describe('fileEditTool – CRLF handling', () => {
  it('preserves CRLF line endings when editing', async () => {
    const file = path.join(tmpDir, 'crlf.ts');
    fs.writeFileSync(file, 'line1\r\nline2\r\nline3\r\n');

    const result = await fileEditTool.execute(
      { file_path: file, old_string: 'line2', new_string: 'replaced' },
      ctx(),
    );
    assert.ok(!result.isError);
    const content = fs.readFileSync(file, 'utf-8');
    assert.ok(content.includes('\r\n'), 'Should preserve CRLF endings');
    assert.ok(content.includes('replaced'));
    assert.ok(!content.includes('line2'));
  });

  it('preserves LF line endings when editing', async () => {
    const file = path.join(tmpDir, 'lf.ts');
    fs.writeFileSync(file, 'line1\nline2\nline3\n');

    const result = await fileEditTool.execute(
      { file_path: file, old_string: 'line2', new_string: 'replaced' },
      ctx(),
    );
    assert.ok(!result.isError);
    const content = fs.readFileSync(file, 'utf-8');
    assert.ok(!content.includes('\r\n'), 'Should NOT introduce CRLF');
    assert.ok(content.includes('replaced'));
  });

  it('matches CRLF content when model sends LF-only old_string', async () => {
    const file = path.join(tmpDir, 'crossmatch.ts');
    fs.writeFileSync(file, 'alpha\r\nbeta\r\ngamma\r\n');

    const result = await fileEditTool.execute(
      { file_path: file, old_string: 'alpha\nbeta', new_string: 'ALPHA\nBETA' },
      ctx(),
    );
    assert.ok(!result.isError);
    const content = fs.readFileSync(file, 'utf-8');
    assert.ok(content.includes('ALPHA\r\nBETA'), 'Should convert newlines to match file');
  });

  it('preserves CRLF in multi-line replacement', async () => {
    const file = path.join(tmpDir, 'multiline-crlf.ts');
    fs.writeFileSync(file, 'const a = 1;\r\nconst b = 2;\r\nconst c = 3;\r\n');

    const result = await fileEditTool.execute(
      { file_path: file, old_string: 'const b = 2;', new_string: 'const b = 20;\nconst d = 40;' },
      ctx(),
    );
    assert.ok(!result.isError);
    const content = fs.readFileSync(file, 'utf-8');
    assert.ok(content.includes('const b = 20;\r\nconst d = 40;'));
  });
});
