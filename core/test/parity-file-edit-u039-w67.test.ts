// Unit U039 (wave w67) — parity gaps for core/core/src/tools/file-edit.ts:
//  - AMI settings-file schema validation on edit (not just JSON syntax): block a
//    valid→schema-invalid edit, with per-field errors, unknown-key suggestions,
//    an env-protection note, and the recognized-keys list
//  - honor the model's explicit line endings on a full-content write (no
//    re-conversion to the old file's EOL)
//  - trailing-newline-aware line counting for the "wrote N lines" display
//  - empty-content '(No content)' sentinel on a zero-length write
//  - glob-pattern rejection in write operations (actionable message)
//  - ~ expansion before path resolution (tool path == permission-checked path)
//  - FILE_NOT_FOUND_CWD_NOTE + suggestPathUnderCwd in the file-not-found error
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileEditTool } from '../src/tools/file-edit';
import { countContentLines, FILE_NOT_FOUND_CWD_NOTE } from '../src/tools/tool-utils';
import type { ToolContext } from '../src/types';

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    filesRead: new Set<string>(),
    ...overrides,
  };
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u039w67-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// countContentLines helper — trailing newline is a terminator, not a new line
// ---------------------------------------------------------------------------

describe('countContentLines helper', () => {
  it('treats a trailing newline as a terminator (no off-by-one)', () => {
    assert.equal(countContentLines(''), 0);
    assert.equal(countContentLines('a'), 1);
    assert.equal(countContentLines('a\n'), 1);
    assert.equal(countContentLines('a\nb'), 2);
    assert.equal(countContentLines('a\nb\n'), 2);
  });
});

// ---------------------------------------------------------------------------
// AMI settings-file schema validation on edit
// ---------------------------------------------------------------------------

describe('fileEditTool – AMI settings-file schema validation', () => {
  function writeAmiConfig(contents: string): string {
    const dir = path.join(tmpDir, '.ami');
    fs.mkdirSync(dir, { recursive: true });
    const file = path.join(dir, 'config.json');
    fs.writeFileSync(file, contents);
    return file;
  }

  it('blocks a valid→schema-invalid edit (wrong-typed field)', async () => {
    const file = writeAmiConfig('{ "model": "gpt" }');
    const result = await fileEditTool.execute(
      { file_path: file, old_string: '"model": "gpt"', new_string: '"model": "gpt", "maxTurns": "lots"' },
      ctx(),
    );
    assert.equal(result.isError, true, result.output);
    assert.match(result.output, /settings schema/);
    assert.match(result.output, /maxTurns/);
    // The on-disk file must be untouched.
    assert.equal(fs.readFileSync(file, 'utf-8'), '{ "model": "gpt" }');
  });

  it('surfaces an unknown-key "did you mean" suggestion and the recognized-keys list', async () => {
    const file = writeAmiConfig('{ "model": "gpt" }');
    const result = await fileEditTool.execute(
      { file_path: file, old_string: '"model": "gpt"', new_string: '"model": "gpt", "modle": "x"' },
      ctx(),
    );
    assert.equal(result.isError, true, result.output);
    assert.match(result.output, /unknown key "modle"/);
    assert.match(result.output, /did you mean "model"/);
    assert.match(result.output, /Recognized settings keys:/);
  });

  it('appends an env-protection note when a failing edit touches the env block', async () => {
    const file = writeAmiConfig('{ "model": "gpt" }');
    const result = await fileEditTool.execute(
      {
        file_path: file,
        old_string: '"model": "gpt"',
        new_string: '"model": "gpt", "env": { "X": "1" }, "maxTurns": "bad"',
      },
      ctx(),
    );
    assert.equal(result.isError, true, result.output);
    assert.match(result.output, /do not modify the "env" block/);
  });

  it('allows a valid→valid settings edit', async () => {
    const file = writeAmiConfig('{ "model": "gpt" }');
    const result = await fileEditTool.execute(
      { file_path: file, old_string: '"model": "gpt"', new_string: '"model": "claude", "maxTurns": 5' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.match(fs.readFileSync(file, 'utf-8'), /"maxTurns": 5/);
  });

  it('allows an edit when the baseline was already schema-invalid (repair-in-progress)', async () => {
    const file = writeAmiConfig('{ "maxTurns": "bad" }');
    const result = await fileEditTool.execute(
      { file_path: file, old_string: '"maxTurns": "bad"', new_string: '"maxTurns": "still-bad", "modle": "x"' },
      ctx(),
    );
    // Schema guard must NOT block a file that was already invalid.
    assert.ok(!result.isError, result.output);
  });

  it('does NOT schema-check a config.json outside an .ami directory', async () => {
    const file = path.join(tmpDir, 'config.json');
    fs.writeFileSync(file, '{ "model": "gpt" }');
    const result = await fileEditTool.execute(
      { file_path: file, old_string: '"model": "gpt"', new_string: '"model": "gpt", "maxTurns": "bad"' },
      ctx(),
    );
    // Plain config.json (not AMI settings) is only JSON-syntax-checked.
    assert.ok(!result.isError, result.output);
  });
});

// ---------------------------------------------------------------------------
// Full-content write: honor the model's line endings (gap 19)
// ---------------------------------------------------------------------------

describe('fileEditTool – full-content write honors model line endings', () => {
  it('writes LF content verbatim when overwriting a CRLF file', async () => {
    const file = path.join(tmpDir, 'eol.sh');
    fs.writeFileSync(file, 'echo a\r\necho b\r\n');
    const result = await fileEditTool.execute(
      // No old_string → full overwrite. filesRead undefined skips read-before-write.
      { file_path: file, new_string: 'echo x\necho y\n' },
      { cwd: tmpDir, abortSignal: new AbortController().signal },
    );
    assert.ok(!result.isError, result.output);
    // The model sent LF; the file must NOT be re-converted back to CRLF.
    assert.equal(fs.readFileSync(file, 'utf-8'), 'echo x\necho y\n');
  });
});

// ---------------------------------------------------------------------------
// New-file write: trailing-newline line count + empty-content sentinel
// ---------------------------------------------------------------------------

describe('fileEditTool – new-file write display', () => {
  it('reports a newline-terminated file as N lines, not N+1', async () => {
    const file = path.join(tmpDir, 'two.txt');
    const result = await fileEditTool.execute(
      { file_path: file, new_string: 'a\nb\n' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.match(result.output, /new file, 2 lines/);
    assert.ok(!/new file, 3 lines/.test(result.output), result.output);
  });

  it('renders a (No content) sentinel for a zero-length write', async () => {
    const file = path.join(tmpDir, 'empty.txt');
    const result = await fileEditTool.execute(
      { file_path: file, new_string: '' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.match(result.output, /\(No content\)/);
    assert.match(result.output, /new file, 0 lines/);
    assert.equal(fs.readFileSync(file, 'utf-8'), '');
  });
});

// ---------------------------------------------------------------------------
// Glob-pattern rejection in write operations (gap 28)
// ---------------------------------------------------------------------------

describe('fileEditTool – glob-pattern rejection', () => {
  it('rejects a * wildcard path with an actionable message', async () => {
    const result = await fileEditTool.execute(
      { file_path: path.join(tmpDir, 'foo*.txt'), new_string: 'x' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /Glob patterns are not allowed in write operations/);
  });

  it('rejects a ? wildcard path', async () => {
    const result = await fileEditTool.execute(
      { file_path: path.join(tmpDir, 'who?.txt'), new_string: 'x' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /Glob patterns are not allowed/);
  });

  it('still allows a legitimate bracket filename (superset divergence)', async () => {
    const file = path.join(tmpDir, '[id].tsx');
    const result = await fileEditTool.execute(
      { file_path: file, new_string: 'export default 1\n' },
      ctx(),
    );
    assert.ok(!result.isError, result.output);
    assert.ok(fs.existsSync(file));
  });
});

// ---------------------------------------------------------------------------
// ~ expansion before path resolution (gap 16)
// ---------------------------------------------------------------------------

describe('fileEditTool – ~ expansion before resolution', () => {
  it('expands ~ to the home dir (not a literal ~ dir under cwd)', async () => {
    const result = await fileEditTool.execute(
      { file_path: '~/si-parity-should-not-write.txt', new_string: 'x' },
      ctx(),
    );
    // Home is outside the tmp workspace → blocked. Crucially, no literal "~"
    // directory is created under cwd (the pre-expansion bug).
    assert.equal(result.isError, true, result.output);
    assert.equal(fs.existsSync(path.join(tmpDir, '~')), false);
  });
});

// ---------------------------------------------------------------------------
// File-not-found error carries cwd note + path-under-cwd suggestion
// ---------------------------------------------------------------------------

describe('fileEditTool – file-not-found affordances', () => {
  it('appends the working-directory note to a not-found edit error', async () => {
    const missing = path.join(tmpDir, 'nope.ts');
    const result = await fileEditTool.execute(
      { file_path: missing, old_string: 'a', new_string: 'b' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /File not found/);
    assert.ok(result.output.includes(`${FILE_NOT_FOUND_CWD_NOTE} ${tmpDir}`), result.output);
  });

  it('suggests the corrected path under cwd when the repo folder was dropped', async () => {
    const repo = path.join(tmpDir, 'repo');
    fs.mkdirSync(repo, { recursive: true });
    fs.writeFileSync(path.join(repo, 'data.ts'), 'original\n');
    // Model dropped the "repo" segment: <tmp>/data.ts instead of <tmp>/repo/data.ts.
    const dropped = path.join(tmpDir, 'data.ts');
    const result = await fileEditTool.execute(
      { file_path: dropped, old_string: 'original', new_string: 'changed' },
      ctx({ cwd: repo, allowedReadRoots: [tmpDir] }),
    );
    assert.equal(result.isError, true, result.output);
    assert.match(result.output, /under the working directory/);
    assert.ok(result.output.includes(path.join(repo, 'data.ts')), result.output);
  });
});
