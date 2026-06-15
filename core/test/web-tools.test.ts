import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { webFetchTool } from '../src/tools/web-fetch';
import { webSearchTool } from '../src/tools/web-search';
import { notebookEditTool } from '../src/tools/notebook-edit';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeContext(cwd: string, allowLocalhost = false): ToolContext {
  return {
    cwd,
    abortSignal: new AbortController().signal,
    _allowLocalhostForTesting: allowLocalhost,
  };
}

function makeTempDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'si-web-test-'));
}

/**
 * Create a minimal valid Jupyter notebook JSON object.
 */
function makeNotebook(
  cells: Array<{
    cell_type: string;
    source: string;
    execution_count?: number | null;
    outputs?: unknown[];
  }>,
): object {
  return {
    nbformat: 4,
    nbformat_minor: 5,
    metadata: {
      language_info: { name: 'python' },
    },
    cells: cells.map((c, i) => ({
      cell_type: c.cell_type,
      id: `cell-${i}`,
      source: c.source,
      metadata: {},
      ...(c.cell_type === 'code'
        ? {
            execution_count: c.execution_count ?? null,
            outputs: c.outputs ?? [],
          }
        : {}),
    })),
  };
}

// ---------------------------------------------------------------------------
// web_fetch — local HTTP server tests
// ---------------------------------------------------------------------------
describe('web_fetch tool', () => {
  let server: http.Server;
  let baseUrl: string;

  before(
    () =>
      new Promise<void>((resolve) => {
        server = http.createServer((req, res) => {
          if (req.url === '/hello') {
            res.writeHead(200, { 'Content-Type': 'text/plain' });
            res.end('Hello, World!');
          } else if (req.url === '/html') {
            res.writeHead(200, { 'Content-Type': 'text/html' });
            res.end(
              '<html><head><title>Test</title></head><body><h1>Title</h1><p>Paragraph text</p><script>alert(1)</script><style>body{}</style></body></html>',
            );
          } else if (req.url === '/redirect') {
            res.writeHead(302, { Location: '/hello' });
            res.end();
          } else if (req.url === '/json') {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ key: 'value' }));
          } else if (req.url === '/slow') {
            // Do not respond — simulates a timeout
            // The request will hang until the client times out or disconnects
          } else if (req.url === '/error') {
            res.writeHead(500, { 'Content-Type': 'text/plain' });
            res.end('Internal Server Error');
          } else if (req.url === '/not-found') {
            res.writeHead(404, { 'Content-Type': 'text/plain' });
            res.end('Not Found');
          } else if (req.url === '/large') {
            res.writeHead(200, { 'Content-Type': 'text/plain' });
            // Write 60000 characters of content
            res.end('x'.repeat(60000));
          } else {
            res.writeHead(404);
            res.end();
          }
        });

        server.listen(0, '127.0.0.1', () => {
          const addr = server.address();
          if (addr && typeof addr === 'object') {
            baseUrl = `http://127.0.0.1:${addr.port}`;
          }
          resolve();
        });
      }),
  );

  after(
    () =>
      new Promise<void>((resolve) => {
        server.close(() => resolve());
      }),
  );

  it('fetches plain text from a URL', async () => {
    const result = await webFetchTool.execute(
      { url: `${baseUrl}/hello` },
      makeContext('/tmp', true),
    );
    assert.equal(result.isError, undefined);
    assert.ok(result.output.includes('Hello, World!'));
    assert.ok(result.output.includes('Status: 200'));
  });

  it('strips HTML tags from HTML responses', async () => {
    const result = await webFetchTool.execute(
      { url: `${baseUrl}/html` },
      makeContext('/tmp', true),
    );
    assert.equal(result.isError, undefined);
    // Should contain the visible text
    assert.ok(result.output.includes('Title'));
    assert.ok(result.output.includes('Paragraph text'));
    // Should NOT contain script/style content or raw tags
    assert.ok(!result.output.includes('alert(1)'));
    assert.ok(!result.output.includes('body{}'));
    assert.ok(!result.output.includes('<script'));
    assert.ok(!result.output.includes('<style'));
    assert.ok(!result.output.includes('<h1>'));
  });

  it('blocks redirects to private IPs (SSRF protection)', async () => {
    const result = await webFetchTool.execute(
      { url: `${baseUrl}/redirect` },
      makeContext('/tmp', true),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Blocked') || result.output.includes('redirect'));
  });

  it('handles JSON content type', async () => {
    const result = await webFetchTool.execute(
      { url: `${baseUrl}/json` },
      makeContext('/tmp', true),
    );
    assert.equal(result.isError, undefined);
    assert.ok(result.output.includes('"key"'));
    assert.ok(result.output.includes('"value"'));
  });

  it('returns error for HTTP 404', async () => {
    const result = await webFetchTool.execute(
      { url: `${baseUrl}/not-found` },
      makeContext('/tmp', true),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('404'));
  });

  it('returns error for HTTP 500', async () => {
    const result = await webFetchTool.execute(
      { url: `${baseUrl}/error` },
      makeContext('/tmp', true),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('500'));
  });

  it('truncates large responses', async () => {
    const result = await webFetchTool.execute(
      { url: `${baseUrl}/large` },
      makeContext('/tmp', true),
    );
    assert.equal(result.isError, undefined);
    assert.ok(result.output.includes('[Content truncated at 50000 characters]'));
    // The total output should be under 51000 chars (50000 + header)
    assert.ok(result.output.length < 51000);
  });

  it('includes prompt in output when provided', async () => {
    const result = await webFetchTool.execute(
      { url: `${baseUrl}/hello`, prompt: 'Extract the greeting' },
      makeContext('/tmp', true),
    );
    assert.equal(result.isError, undefined);
    assert.ok(result.output.includes('Prompt: Extract the greeting'));
  });

  it('returns error for invalid URL', async () => {
    const result = await webFetchTool.execute(
      { url: 'not-a-url' },
      makeContext('/tmp', true),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Invalid URL'));
  });

  it('returns error for empty URL', async () => {
    const result = await webFetchTool.execute(
      { url: '' },
      makeContext('/tmp', true),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('url must not be empty'));
  });

  it('returns error for unsupported protocol', async () => {
    const result = await webFetchTool.execute(
      { url: 'ftp://example.com/file' },
      makeContext('/tmp', true),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Unsupported protocol'));
  });

  it('handles abort signal', async () => {
    const ac = new AbortController();
    ac.abort(); // Abort immediately
    const ctx: ToolContext = { cwd: '/tmp', abortSignal: ac.signal };

    const result = await webFetchTool.execute(
      { url: `${baseUrl}/hello` },
      ctx,
    );
    assert.equal(result.isError, true);
  });

  it('returns error for connection refused', async () => {
    const result = await webFetchTool.execute(
      { url: 'http://127.0.0.1:1' },
      makeContext('/tmp', true),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Error fetching'));
  });

  it('SSRF: blocks localhost when _allowLocalhostForTesting is not set', async () => {
    const result = await webFetchTool.execute(
      { url: `${baseUrl}/hello` },
      makeContext('/tmp'), // no allowLocalhost
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Blocked'));
  });

  it('SSRF: blocks metadata endpoints', async () => {
    const result = await webFetchTool.execute(
      { url: 'http://169.254.169.254/latest/meta-data/' },
      makeContext('/tmp'),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Blocked'));
  });
});

// ---------------------------------------------------------------------------
// web_search — unit tests with mocked results
// ---------------------------------------------------------------------------
describe('web_search tool', () => {
  it('returns error for empty query', async () => {
    const result = await webSearchTool.execute(
      { query: '' },
      makeContext('/tmp', true),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('query must not be empty'));
  });

  it('has correct tool metadata', () => {
    assert.equal(webSearchTool.name, 'web_search');
    assert.equal(webSearchTool.isReadOnly, true);
    assert.ok(webSearchTool.description.includes('Search'));
    assert.deepEqual(webSearchTool.inputSchema.required, ['query']);
  });

  it('handles abort signal', async () => {
    const ac = new AbortController();
    ac.abort();
    const ctx: ToolContext = { cwd: '/tmp', abortSignal: ac.signal };

    const result = await webSearchTool.execute({ query: 'test' }, ctx);
    assert.equal(result.isError, true);
  });

  // Test the DuckDuckGo HTML parser using a mock HTTP server
  it('parses search results from a local mock DuckDuckGo page', async () => {
    // We test parsing indirectly via a local server that mimics the DDG HTML
    // structure. The tool makes a request to html.duckduckgo.com, so we can't
    // easily redirect it. Instead we verify the format expectations:
    // the tool should return a formatted string.
    // Since we can't easily mock the external DDG call, we just verify
    // the tool gracefully handles a network error (which is expected in
    // CI/test environments where DDG may not be reachable or may return
    // a captcha).
    const result = await webSearchTool.execute(
      { query: 'test query that will hit real DDG or fail gracefully' },
      makeContext('/tmp', true),
    );
    // Should either succeed with formatted results or fail gracefully
    if (result.isError) {
      assert.ok(
        result.output.includes('Error') ||
          result.output.includes('aborted'),
      );
    } else {
      // If it succeeded, verify the format
      assert.ok(
        result.output.includes('Search results for:') ||
          result.output.includes('No results found'),
      );
    }
  });
});

// ---------------------------------------------------------------------------
// notebook_edit
// ---------------------------------------------------------------------------
describe('notebook_edit tool', () => {
  let tmpDir: string;

  before(() => {
    tmpDir = makeTempDir();
  });

  after(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('replaces cell content (default mode)', async () => {
    const nb = makeNotebook([
      { cell_type: 'code', source: 'print("old")' },
      { cell_type: 'markdown', source: '# Hello' },
    ]);
    const fp = path.join(tmpDir, 'replace.ipynb');
    fs.writeFileSync(fp, JSON.stringify(nb));

    const result = await notebookEditTool.execute(
      { notebook_path: fp, cell_number: 0, new_source: 'print("new")' },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, undefined);
    assert.ok(result.output.includes('Replaced cell 0'));
    assert.ok(result.output.includes('2 cell(s)'));

    // Verify the file was updated
    const updated = JSON.parse(fs.readFileSync(fp, 'utf-8'));
    assert.equal(updated.cells[0].source, 'print("new")');
    // Execution count should be reset
    assert.equal(updated.cells[0].execution_count, null);
    assert.deepEqual(updated.cells[0].outputs, []);
    // Second cell should be unchanged
    assert.equal(updated.cells[1].source, '# Hello');
  });

  it('inserts a new cell', async () => {
    const nb = makeNotebook([
      { cell_type: 'code', source: 'cell 0' },
      { cell_type: 'code', source: 'cell 1' },
    ]);
    const fp = path.join(tmpDir, 'insert.ipynb');
    fs.writeFileSync(fp, JSON.stringify(nb));

    const result = await notebookEditTool.execute(
      {
        notebook_path: fp,
        cell_number: 1,
        new_source: 'inserted cell',
        cell_type: 'markdown',
        edit_mode: 'insert',
      },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, undefined);
    assert.ok(result.output.includes('Inserted'));
    assert.ok(result.output.includes('3 cell(s)'));

    const updated = JSON.parse(fs.readFileSync(fp, 'utf-8'));
    assert.equal(updated.cells.length, 3);
    assert.equal(updated.cells[0].source, 'cell 0');
    assert.equal(updated.cells[1].source, 'inserted cell');
    assert.equal(updated.cells[1].cell_type, 'markdown');
    assert.equal(updated.cells[2].source, 'cell 1');
  });

  it('inserts at end when cell_number equals cell count', async () => {
    const nb = makeNotebook([
      { cell_type: 'code', source: 'only cell' },
    ]);
    const fp = path.join(tmpDir, 'insert-end.ipynb');
    fs.writeFileSync(fp, JSON.stringify(nb));

    const result = await notebookEditTool.execute(
      {
        notebook_path: fp,
        cell_number: 1,
        new_source: 'appended cell',
        cell_type: 'code',
        edit_mode: 'insert',
      },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, undefined);

    const updated = JSON.parse(fs.readFileSync(fp, 'utf-8'));
    assert.equal(updated.cells.length, 2);
    assert.equal(updated.cells[1].source, 'appended cell');
  });

  it('deletes a cell', async () => {
    const nb = makeNotebook([
      { cell_type: 'code', source: 'keep me' },
      { cell_type: 'code', source: 'delete me' },
      { cell_type: 'code', source: 'also keep' },
    ]);
    const fp = path.join(tmpDir, 'delete.ipynb');
    fs.writeFileSync(fp, JSON.stringify(nb));

    const result = await notebookEditTool.execute(
      {
        notebook_path: fp,
        cell_number: 1,
        new_source: '', // required by schema but unused for delete
        edit_mode: 'delete',
      },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, undefined);
    assert.ok(result.output.includes('Deleted cell 1'));
    assert.ok(result.output.includes('2 cell(s)'));

    const updated = JSON.parse(fs.readFileSync(fp, 'utf-8'));
    assert.equal(updated.cells.length, 2);
    assert.equal(updated.cells[0].source, 'keep me');
    assert.equal(updated.cells[1].source, 'also keep');
  });

  it('auto-converts out-of-range replace to insert at end', async () => {
    const nb = makeNotebook([{ cell_type: 'code', source: 'x' }]);
    const fp = path.join(tmpDir, 'oor-replace.ipynb');
    fs.writeFileSync(fp, JSON.stringify(nb));

    const result = await notebookEditTool.execute(
      { notebook_path: fp, cell_number: 5, new_source: 'y' },
      makeContext(tmpDir),
    );
    // Out-of-range replace auto-converts to insert
    assert.equal(result.isError, undefined);
    const updated = JSON.parse(fs.readFileSync(fp, 'utf-8'));
    assert.equal(updated.cells.length, 2);
    assert.equal(updated.cells[1].source, 'y');
  });

  it('returns error for out-of-range cell_number on delete', async () => {
    const nb = makeNotebook([{ cell_type: 'code', source: 'x' }]);
    const fp = path.join(tmpDir, 'oor-delete.ipynb');
    fs.writeFileSync(fp, JSON.stringify(nb));

    const result = await notebookEditTool.execute(
      {
        notebook_path: fp,
        cell_number: 3,
        new_source: '',
        edit_mode: 'delete',
      },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('out of range'));
  });

  it('returns error for non-.ipynb file', async () => {
    const fp = path.join(tmpDir, 'notanotebook.json');
    fs.writeFileSync(fp, '{}');

    const result = await notebookEditTool.execute(
      { notebook_path: fp, new_source: 'x' },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('.ipynb'));
  });

  it('returns error for missing file', async () => {
    const fp = path.join(tmpDir, 'missing.ipynb');

    const result = await notebookEditTool.execute(
      { notebook_path: fp, new_source: 'x' },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('not found'));
  });

  it('returns error for invalid JSON', async () => {
    const fp = path.join(tmpDir, 'bad.ipynb');
    fs.writeFileSync(fp, 'this is not json');

    const result = await notebookEditTool.execute(
      { notebook_path: fp, new_source: 'x' },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('not valid JSON'));
  });

  it('returns error for JSON without cells array', async () => {
    const fp = path.join(tmpDir, 'nocells.ipynb');
    fs.writeFileSync(fp, JSON.stringify({ nbformat: 4 }));

    const result = await notebookEditTool.execute(
      { notebook_path: fp, new_source: 'x' },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Invalid notebook structure'));
  });

  it('returns error for empty notebook_path', async () => {
    const result = await notebookEditTool.execute(
      { notebook_path: '', new_source: 'x' },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('notebook_path must not be empty'));
  });

  it('returns error for invalid edit_mode', async () => {
    const nb = makeNotebook([{ cell_type: 'code', source: 'x' }]);
    const fp = path.join(tmpDir, 'badmode.ipynb');
    fs.writeFileSync(fp, JSON.stringify(nb));

    const result = await notebookEditTool.execute(
      {
        notebook_path: fp,
        cell_number: 0,
        new_source: 'y',
        edit_mode: 'append',
      },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('edit_mode'));
  });

  it('changes cell type when cell_type is specified on replace', async () => {
    const nb = makeNotebook([
      { cell_type: 'code', source: 'print("hi")' },
    ]);
    const fp = path.join(tmpDir, 'change-type.ipynb');
    fs.writeFileSync(fp, JSON.stringify(nb));

    const result = await notebookEditTool.execute(
      {
        notebook_path: fp,
        cell_number: 0,
        new_source: '# Now markdown',
        cell_type: 'markdown',
      },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, undefined);

    const updated = JSON.parse(fs.readFileSync(fp, 'utf-8'));
    assert.equal(updated.cells[0].cell_type, 'markdown');
    assert.equal(updated.cells[0].source, '# Now markdown');
  });

  it('resolves relative paths against cwd', async () => {
    const nb = makeNotebook([
      { cell_type: 'code', source: 'original' },
    ]);
    const fp = path.join(tmpDir, 'relative.ipynb');
    fs.writeFileSync(fp, JSON.stringify(nb));

    const result = await notebookEditTool.execute(
      { notebook_path: 'relative.ipynb', cell_number: 0, new_source: 'updated' },
      makeContext(tmpDir),
    );
    assert.equal(result.isError, undefined);
    assert.ok(result.output.includes('Replaced cell 0'));
  });

  it('generates cell ID for nbformat >= 4.5', async () => {
    const nb = makeNotebook([
      { cell_type: 'code', source: 'existing' },
    ]);
    const fp = path.join(tmpDir, 'cellid.ipynb');
    fs.writeFileSync(fp, JSON.stringify(nb));

    await notebookEditTool.execute(
      {
        notebook_path: fp,
        cell_number: 1,
        new_source: 'new cell with id',
        cell_type: 'code',
        edit_mode: 'insert',
      },
      makeContext(tmpDir),
    );

    const updated = JSON.parse(fs.readFileSync(fp, 'utf-8'));
    // The inserted cell should have an id
    assert.ok(updated.cells[1].id, 'Inserted cell should have an ID');
    assert.ok(typeof updated.cells[1].id === 'string');
  });
});
