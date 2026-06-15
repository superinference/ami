import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileReadTool } from '../src/tools/file-read';
import { fileEditTool } from '../src/tools/file-edit';
import { grepTool } from '../src/tools/grep';
import { webFetchTool } from '../src/tools/web-fetch';
import { webSearchTool } from '../src/tools/web-search';
import { notebookEditTool } from '../src/tools/notebook-edit';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    filesRead: new Set<string>(),
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Notebook (.ipynb) reading
// ---------------------------------------------------------------------------

describe('file_read – notebook (.ipynb) support', () => {
  it('parses a basic notebook with code and markdown cells', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-nb-'));
    const nbPath = path.join(tmpDir, 'test.ipynb');
    const notebook = {
      cells: [
        {
          cell_type: 'markdown',
          source: ['# Hello Notebook'],
          metadata: {},
          id: 'md-1',
        },
        {
          cell_type: 'code',
          source: ['print("hello")'],
          metadata: {},
          id: 'code-1',
          outputs: [
            { output_type: 'stream', text: ['hello\n'] },
          ],
        },
      ],
      metadata: {
        kernelspec: { display_name: 'Python 3', name: 'python3' },
      },
      nbformat: 4,
      nbformat_minor: 5,
    };
    fs.writeFileSync(nbPath, JSON.stringify(notebook));

    const result = await fileReadTool.execute({ file_path: nbPath }, ctx({ cwd: tmpDir }));
    assert.ok(!result.isError);
    assert.ok(result.output.includes('Jupyter Notebook'));
    assert.ok(result.output.includes('2 cells'));
    assert.ok(result.output.includes('Python 3'));
    assert.ok(result.output.includes('# Hello Notebook'));
    assert.ok(result.output.includes('print("hello")'));
    assert.ok(result.output.includes('[output] hello'));
    assert.ok(result.output.includes('md-1'));
    assert.ok(result.output.includes('code-1'));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('handles notebook with error output', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-nb-'));
    const nbPath = path.join(tmpDir, 'error.ipynb');
    const notebook = {
      cells: [
        {
          cell_type: 'code',
          source: ['1/0'],
          metadata: {},
          outputs: [
            { output_type: 'error', ename: 'ZeroDivisionError', evalue: 'division by zero' },
          ],
        },
      ],
      metadata: {},
      nbformat: 4,
      nbformat_minor: 5,
    };
    fs.writeFileSync(nbPath, JSON.stringify(notebook));

    const result = await fileReadTool.execute({ file_path: nbPath }, ctx({ cwd: tmpDir }));
    assert.ok(!result.isError);
    assert.ok(result.output.includes('[error] ZeroDivisionError'));
    assert.ok(result.output.includes('division by zero'));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('handles notebook with text/plain data output', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-nb-'));
    const nbPath = path.join(tmpDir, 'data.ipynb');
    const notebook = {
      cells: [
        {
          cell_type: 'code',
          source: ['42'],
          metadata: {},
          outputs: [
            { output_type: 'execute_result', data: { 'text/plain': ['42'] } },
          ],
        },
      ],
      metadata: {},
      nbformat: 4,
      nbformat_minor: 5,
    };
    fs.writeFileSync(nbPath, JSON.stringify(notebook));

    const result = await fileReadTool.execute({ file_path: nbPath }, ctx({ cwd: tmpDir }));
    assert.ok(!result.isError);
    assert.ok(result.output.includes('[output] 42'));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('generates cell IDs when missing', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-nb-'));
    const nbPath = path.join(tmpDir, 'noid.ipynb');
    const notebook = {
      cells: [
        { cell_type: 'code', source: ['x = 1'], metadata: {}, outputs: [] },
      ],
      metadata: {},
      nbformat: 4,
      nbformat_minor: 5,
    };
    fs.writeFileSync(nbPath, JSON.stringify(notebook));

    const result = await fileReadTool.execute({ file_path: nbPath }, ctx({ cwd: tmpDir }));
    assert.ok(!result.isError);
    assert.ok(result.output.includes('cell-0'));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('handles malformed notebook JSON', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-nb-'));
    const nbPath = path.join(tmpDir, 'bad.ipynb');
    fs.writeFileSync(nbPath, '{ not valid json }}}');

    const result = await fileReadTool.execute({ file_path: nbPath }, ctx({ cwd: tmpDir }));
    assert.ok(result.isError);
    assert.ok(result.output.includes('Error reading notebook'));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// file_edit – .ipynb guard
// ---------------------------------------------------------------------------

describe('file_edit – .ipynb guard', () => {
  it('rejects editing .ipynb files', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-edit-'));
    const nbPath = path.join(tmpDir, 'test.ipynb');
    fs.writeFileSync(nbPath, '{}');

    const c = ctx({ cwd: tmpDir });
    c.filesRead!.add(nbPath);

    const result = await fileEditTool.execute(
      { file_path: nbPath, old_string: 'foo', new_string: 'bar' },
      c,
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('notebook_edit'));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// web_fetch – domain filtering
// ---------------------------------------------------------------------------

describe('web_fetch – domain filtering', () => {
  it('schema includes allowed_domains and blocked_domains', () => {
    assert.ok('allowed_domains' in webFetchTool.inputSchema.properties);
    assert.ok('blocked_domains' in webFetchTool.inputSchema.properties);
  });

  it('blocks URL not in allowed_domains', async () => {
    const result = await webFetchTool.execute(
      { url: 'https://example.com', allowed_domains: ['trusted.com'] },
      ctx(),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('not in the allowed domains'));
  });

  it('blocks URL in blocked_domains', async () => {
    const result = await webFetchTool.execute(
      { url: 'https://evil.com/page', blocked_domains: ['evil.com'] },
      ctx(),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('blocked'));
  });

  it('allows URL when domain matches allowed_domains', async () => {
    const result = await webFetchTool.execute(
      { url: 'https://api.trusted.com/data', allowed_domains: ['trusted.com'] },
      ctx({ _allowLocalhostForTesting: true } as any),
    );
    // Should not be domain-blocked (may fail for other reasons like network)
    if (result.isError) {
      assert.ok(!result.output.includes('not in the allowed domains'));
    }
  });
});

// ---------------------------------------------------------------------------
// web_search – domain filtering
// ---------------------------------------------------------------------------

describe('web_search – domain filtering', () => {
  it('schema includes allowed_domains and blocked_domains', () => {
    assert.ok('allowed_domains' in webSearchTool.inputSchema.properties);
    assert.ok('blocked_domains' in webSearchTool.inputSchema.properties);
  });
});

// ---------------------------------------------------------------------------
// grep – context lines and max columns
// ---------------------------------------------------------------------------

describe('grep – new features', () => {
  it('schema includes context_lines', () => {
    assert.ok('context_lines' in grepTool.inputSchema.properties);
  });

  it('schema includes output_mode', () => {
    assert.ok('output_mode' in grepTool.inputSchema.properties);
  });

  it('schema includes case_insensitive', () => {
    assert.ok('case_insensitive' in grepTool.inputSchema.properties);
  });

  it('searches with context lines', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-grep-'));
    const f = path.join(tmpDir, 'test.txt');
    fs.writeFileSync(f, 'line1\nline2\nMATCH\nline4\nline5\n');

    const result = await grepTool.execute(
      { pattern: 'MATCH', path: tmpDir, context_lines: 1 },
      ctx({ cwd: tmpDir }),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('MATCH'));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('respects case_insensitive', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-grep-'));
    fs.writeFileSync(path.join(tmpDir, 'test.txt'), 'Hello World\nhello world\nHELLO WORLD\n');

    const result = await grepTool.execute(
      { pattern: 'hello', path: tmpDir, case_insensitive: true },
      ctx({ cwd: tmpDir }),
    );
    assert.ok(!result.isError);
    // All variants should match (case insensitive)
    assert.ok(result.output.includes('Hello World'));
    assert.ok(result.output.includes('hello world'));
    assert.ok(result.output.includes('HELLO WORLD'));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('supports files_with_matches output mode', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-grep-'));
    fs.writeFileSync(path.join(tmpDir, 'test.txt'), 'findme here\n');

    const result = await grepTool.execute(
      { pattern: 'findme', path: tmpDir, output_mode: 'files_with_matches' },
      ctx({ cwd: tmpDir }),
    );
    assert.ok(!result.isError);
    // Output should contain file path (not the match text)
    assert.ok(result.output.includes('test.txt'));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('supports count output mode', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-grep-'));
    fs.writeFileSync(path.join(tmpDir, 'test.txt'), 'a\na\na\nb\n');

    const result = await grepTool.execute(
      { pattern: 'a', path: tmpDir, output_mode: 'count' },
      ctx({ cwd: tmpDir }),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('3'));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('schema includes multiline parameter', () => {
    assert.ok('multiline' in grepTool.inputSchema.properties);
  });
});

// ---------------------------------------------------------------------------
// file_read – UNC path blocking
// ---------------------------------------------------------------------------

describe('file_read – UNC path blocking', () => {
  it('blocks UNC paths with backslashes', async () => {
    const result = await fileReadTool.execute(
      { file_path: '\\\\server\\share\\file.txt' },
      ctx(),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('UNC'));
  });

  it('blocks UNC paths with forward slashes', async () => {
    const result = await fileReadTool.execute(
      { file_path: '//server/share/file.txt' },
      ctx(),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('UNC'));
  });

  it('allows normal absolute paths', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-unc-'));
    const f = path.join(tmpDir, 'test.txt');
    fs.writeFileSync(f, 'hello');

    const result = await fileReadTool.execute({ file_path: f }, ctx({ cwd: tmpDir }));
    assert.ok(!result.isError);
    assert.ok(result.output.includes('hello'));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// notebook_edit – read-before-edit check
// ---------------------------------------------------------------------------

describe('notebook_edit – read-before-edit check', () => {
  it('rejects edit when notebook was not read first', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-nbedit-'));
    const nbPath = path.join(tmpDir, 'test.ipynb');
    const notebook = {
      cells: [{ cell_type: 'code', source: ['x = 1'], metadata: {}, id: 'c1', outputs: [] }],
      metadata: {}, nbformat: 4, nbformat_minor: 5,
    };
    fs.writeFileSync(nbPath, JSON.stringify(notebook));

    const result = await notebookEditTool.execute(
      { notebook_path: nbPath, cell_id: 'c1', new_source: 'x = 2' },
      ctx({ cwd: tmpDir }),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('read'));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('allows edit when notebook was read first', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-nbedit-'));
    const nbPath = path.join(tmpDir, 'test.ipynb');
    const notebook = {
      cells: [{ cell_type: 'code', source: ['x = 1'], metadata: {}, id: 'c1', outputs: [] }],
      metadata: {}, nbformat: 4, nbformat_minor: 5,
    };
    fs.writeFileSync(nbPath, JSON.stringify(notebook));

    const filesRead = new Set<string>();
    filesRead.add(fs.realpathSync(nbPath));
    const result = await notebookEditTool.execute(
      { notebook_path: nbPath, cell_id: 'c1', new_source: 'x = 2' },
      ctx({ cwd: tmpDir, filesRead }),
    );
    assert.ok(!result.isError);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// grep – offset and head_limit pagination
// ---------------------------------------------------------------------------

describe('grep – offset and head_limit', () => {
  it('schema includes head_limit and offset', () => {
    assert.ok('head_limit' in grepTool.inputSchema.properties);
    assert.ok('offset' in grepTool.inputSchema.properties);
  });

  it('respects head_limit', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-grep-'));
    const lines = Array.from({ length: 20 }, (_, i) => `match_line_${i}`).join('\n');
    fs.writeFileSync(path.join(tmpDir, 'test.txt'), lines);

    const result = await grepTool.execute(
      { pattern: 'match_line', path: tmpDir, head_limit: 5 },
      ctx({ cwd: tmpDir }),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('match_line_0'));
    assert.ok(result.output.includes('showing'));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('respects offset', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-grep-'));
    const lines = Array.from({ length: 10 }, (_, i) => `line_${i}`).join('\n');
    fs.writeFileSync(path.join(tmpDir, 'test.txt'), lines);

    const result = await grepTool.execute(
      { pattern: 'line_', path: tmpDir, offset: 5, head_limit: 3 },
      ctx({ cwd: tmpDir }),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('showing'));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});
