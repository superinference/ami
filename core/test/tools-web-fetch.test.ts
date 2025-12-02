import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { webFetchTool } from '../src/tools/web-fetch';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Tool definition
// ---------------------------------------------------------------------------

describe('webFetchTool – definition', () => {
  it('has the correct name', () => {
    assert.equal(webFetchTool.name, 'web_fetch');
  });

  it('is read-only and concurrency-safe', () => {
    assert.equal(webFetchTool.isReadOnly, true);
    assert.equal(webFetchTool.isConcurrencySafe, true);
  });

  it('schema requires url', () => {
    assert.ok(webFetchTool.inputSchema.required?.includes('url'));
  });

  it('has url and prompt properties', () => {
    assert.ok('url' in webFetchTool.inputSchema.properties);
    assert.ok('prompt' in webFetchTool.inputSchema.properties);
  });
});

// ---------------------------------------------------------------------------
// URL validation
// ---------------------------------------------------------------------------

describe('webFetchTool – URL validation', () => {
  it('rejects empty url', async () => {
    const result = await webFetchTool.execute({ url: '' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('must not be empty'));
  });

  it('rejects invalid URL', async () => {
    const result = await webFetchTool.execute({ url: 'not a url' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Invalid URL'));
  });

  it('rejects non-http protocols (ftp)', async () => {
    const result = await webFetchTool.execute({ url: 'ftp://example.com/file' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Unsupported protocol'));
  });

  it('rejects file:// protocol', async () => {
    const result = await webFetchTool.execute({ url: 'file:///etc/passwd' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Unsupported protocol'));
  });
});

// ---------------------------------------------------------------------------
// SSRF protection
// ---------------------------------------------------------------------------

describe('webFetchTool – SSRF protection', () => {
  it('blocks localhost', async () => {
    const result = await webFetchTool.execute(
      { url: 'http://localhost:8080/secret' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Blocked'));
  });

  it('blocks 127.0.0.1', async () => {
    const result = await webFetchTool.execute(
      { url: 'http://127.0.0.1/secret' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Blocked'));
  });

  it('blocks metadata.google.internal', async () => {
    const result = await webFetchTool.execute(
      { url: 'http://metadata.google.internal/computeMetadata/v1/' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Blocked'));
  });

  it('allows localhost when _allowLocalhostForTesting is set', async () => {
    // This tests the flag — actual fetch will fail or timeout, but it should
    // NOT be blocked by SSRF check
    const ac = new AbortController();
    setTimeout(() => ac.abort(), 500);
    const result = await webFetchTool.execute(
      { url: 'http://localhost:19999/test' },
      ctx({ _allowLocalhostForTesting: true, abortSignal: ac.signal }),
    );
    // Should fail with connection error, not SSRF block
    if (result.isError) {
      assert.ok(!result.output.includes('Blocked'),
        `Should not be blocked by SSRF, got: ${result.output}`);
    }
  });
});

// ---------------------------------------------------------------------------
// Credential stripping
// ---------------------------------------------------------------------------

describe('webFetchTool – credential stripping', () => {
  it('strips user:pass from URL in error output', async () => {
    const result = await webFetchTool.execute(
      { url: 'http://user:pass@nonexistent.invalid/path' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(!result.output.includes('user:pass'),
      'Credentials should be stripped from output');
  });
});

// ---------------------------------------------------------------------------
// Abort handling
// ---------------------------------------------------------------------------

describe('webFetchTool – abort', () => {
  it('returns abort message when signal fires', async () => {
    const ac = new AbortController();
    ac.abort();
    const result = await webFetchTool.execute(
      { url: 'https://example.com' },
      ctx({ abortSignal: ac.signal }),
    );
    assert.equal(result.isError, true);
    assert.ok(
      result.output.includes('abort') || result.output.includes('Abort') || result.output.includes('Blocked'),
      `Expected abort-related output, got: ${result.output}`,
    );
  });
});

// ---------------------------------------------------------------------------
// HTTP fetch with response processing (lines 92-127)
// ---------------------------------------------------------------------------
import { before, after } from 'node:test';
import * as http from 'http';

describe('webFetchTool – HTTP server integration', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    const s = http.createServer((req, res) => {
      if (req.url === '/html-page') {
        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end('<html><body><p>Hello World</p></body></html>');
      } else if (req.url === '/plain-text') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('Plain text content');
      } else if (req.url === '/big-page') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('x'.repeat(60000));
      } else if (req.url === '/error-page') {
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        res.end('Internal Server Error');
      } else if (req.url === '/pdf-page') {
        res.writeHead(200, { 'Content-Type': 'application/pdf' });
        res.end('not-a-real-pdf');
      } else {
        res.writeHead(404);
        res.end('Not Found');
      }
    });

    await new Promise<void>(resolve => {
      s.listen(0, '127.0.0.1', () => resolve());
    });
    server = s;
    port = (s.address() as { port: number }).port;
  });

  after(() => { server.close(); });

  it('fetches HTML and strips tags', async () => {
    const result = await webFetchTool.execute(
      { url: `http://127.0.0.1:${port}/html-page` },
      ctx({ _allowLocalhostForTesting: true }),
    );
    assert.ok(!result.isError, `Should succeed, got: ${result.output}`);
    assert.ok(result.output.includes('Hello World'));
    assert.ok(result.output.includes('Status: 200'));
    assert.ok(result.output.includes('Content-Type: text/html'));
  });

  it('fetches plain text without modification', async () => {
    const result = await webFetchTool.execute(
      { url: `http://127.0.0.1:${port}/plain-text` },
      ctx({ _allowLocalhostForTesting: true }),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('Plain text content'));
  });

  it('truncates large responses', async () => {
    const result = await webFetchTool.execute(
      { url: `http://127.0.0.1:${port}/big-page` },
      ctx({ _allowLocalhostForTesting: true }),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('truncated'));
  });

  it('returns error for HTTP 500', async () => {
    const result = await webFetchTool.execute(
      { url: `http://127.0.0.1:${port}/error-page` },
      ctx({ _allowLocalhostForTesting: true }),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('500'));
  });

  it('includes prompt in output when provided', async () => {
    const result = await webFetchTool.execute(
      { url: `http://127.0.0.1:${port}/plain-text`, prompt: 'Extract keywords' },
      ctx({ _allowLocalhostForTesting: true }),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('Prompt: Extract keywords'));
  });

  it('handles PDF content type gracefully', async () => {
    const result = await webFetchTool.execute(
      { url: `http://127.0.0.1:${port}/pdf-page` },
      ctx({ _allowLocalhostForTesting: true }),
    );
    // pdf-parse may not be available, so it should fall back to error
    assert.ok(result.isError || result.output.includes('PDF'));
  });
});
