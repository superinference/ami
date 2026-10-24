import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';
import * as fs from 'fs';
import * as os from 'os';
import * as nodePath from 'path';
import { webFetchTool, summaryWindow } from '../src/tools/web-fetch';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _allowLocalhostForTesting: true,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// summaryWindow — exported helper, both branches
// ---------------------------------------------------------------------------

describe('summaryWindow', () => {
  it('returns text unchanged when within the limit', () => {
    const text = 'short content';
    assert.equal(summaryWindow(text, 1000), text);
  });

  it('returns text unchanged when exactly at the limit', () => {
    const text = 'a'.repeat(100);
    assert.equal(summaryWindow(text, 100), text);
  });

  it('keeps head and tail with a middle-omitted marker when over the limit', () => {
    const text = 'HEAD' + 'm'.repeat(1000) + 'TAIL';
    const out = summaryWindow(text, 300);
    assert.ok(out.length < text.length, 'result must be shorter than input');
    assert.ok(out.includes('middle omitted'), 'must include the omission marker');
    assert.ok(out.startsWith('HEAD'), 'must keep the head of the page');
    assert.ok(out.endsWith('TAIL'), 'must keep the tail of the page');
  });
});

// ---------------------------------------------------------------------------
// URL length guard (pre-network)
// ---------------------------------------------------------------------------

describe('webFetchTool – URL length guard', () => {
  it('rejects URLs longer than the max length', async () => {
    const url = 'https://example.com/' + 'a'.repeat(2500);
    const res = await webFetchTool.execute({ url }, ctx());
    assert.equal(res.isError, true);
    assert.match(res.output, /exceeds maximum length/);
  });
});

// ---------------------------------------------------------------------------
// http -> https auto-upgrade (observable via sanitized error output)
// ---------------------------------------------------------------------------

describe('webFetchTool – http to https upgrade', () => {
  it('upgrades non-localhost http URLs to https (and strips credentials)', async () => {
    // .invalid never resolves, so the fetch fails fast; the error output
    // reflects the upgraded, credential-stripped URL.
    const res = await webFetchTool.execute(
      { url: 'http://user:secret@does-not-exist.invalid/path' },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('https://'), `expected https upgrade, got: ${res.output}`);
    assert.ok(!res.output.includes('http://'), `must not retain the http scheme: ${res.output}`);
    assert.ok(!res.output.includes('secret'), 'credentials must be stripped');
  });
});

// ---------------------------------------------------------------------------
// Domain filtering (pre-network branches)
// ---------------------------------------------------------------------------

describe('webFetchTool – domain filtering', () => {
  it('blocks a host not in allowed_domains', async () => {
    const res = await webFetchTool.execute(
      { url: 'http://127.0.0.1:9/x', allowed_domains: ['example.com'] },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.match(res.output, /not in the allowed domains/);
  });

  it('blocks a host present in blocked_domains', async () => {
    const res = await webFetchTool.execute(
      { url: 'http://127.0.0.1:9/x', blocked_domains: ['127.0.0.1'] },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.match(res.output, /is blocked/);
  });
});

// ---------------------------------------------------------------------------
// Integration against a local mock server
// ---------------------------------------------------------------------------

describe('webFetchTool – server integration branches', () => {
  let server: http.Server;
  let port: number;
  let base: string;
  let hitCounts: Record<string, number>;

  before(async () => {
    hitCounts = {};
    const s = http.createServer((req, res) => {
      const u = req.url ?? '';
      hitCounts[u] = (hitCounts[u] ?? 0) + 1;
      if (u === '/cache-me') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('cacheable body content');
      } else if (u === '/allowed') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('allowed content');
      } else if (u === '/truncate-me') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('a'.repeat(1_200_000)); // > MAX_HTTP_BODY_BYTES (1 MiB)
      } else if (u === '/pdf/doc') {
        res.writeHead(200, { 'Content-Type': 'application/pdf' });
        res.end('this-is-not-a-real-pdf-body');
      } else if (u === '/html/doc') {
        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end('<html><body><p>' + 'The HTML rendition of the paper. '.repeat(20) + '</p></body></html>');
      } else if (u === '/summarize-me') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('raw page body for summarization');
      } else {
        res.writeHead(404);
        res.end('nope');
      }
    });
    await new Promise<void>(resolve => s.listen(0, '127.0.0.1', () => resolve()));
    server = s;
    port = (s.address() as { port: number }).port;
    base = `http://127.0.0.1:${port}`;
  });

  after(() => { server.close(); });

  it('allows a host present in allowed_domains and fetches it', async () => {
    const res = await webFetchTool.execute(
      { url: `${base}/allowed`, allowed_domains: ['127.0.0.1'] },
      ctx(),
    );
    assert.ok(!res.isError, res.output);
    assert.ok(res.output.includes('allowed content'));
  });

  it('serves a repeated request from cache without a second network hit', async () => {
    const first = await webFetchTool.execute({ url: `${base}/cache-me` }, ctx());
    assert.ok(!first.isError, first.output);
    const hitsAfterFirst = hitCounts['/cache-me'];
    const second = await webFetchTool.execute({ url: `${base}/cache-me` }, ctx());
    assert.equal(second.output, first.output, 'cached output must be identical');
    assert.equal(hitCounts['/cache-me'], hitsAfterFirst, 'second call must not hit the server');
  });

  it('reports the download-stopped marker when the body exceeds the byte cap', async () => {
    const res = await webFetchTool.execute({ url: `${base}/truncate-me` }, ctx());
    assert.ok(!res.isError, res.output);
    assert.ok(res.output.includes('Download stopped at'), `expected truncation marker, got tail: ${res.output.slice(-200)}`);
  });

  it('falls back to the HTML rendition when a PDF cannot be extracted', async () => {
    const tmp = fs.mkdtempSync(nodePath.join(os.tmpdir(), 'wf-pdf-'));
    try {
      const res = await webFetchTool.execute(
        { url: `${base}/pdf/doc` },
        ctx({ cwd: tmp }),
      );
      assert.ok(!res.isError, res.output);
      assert.ok(res.output.includes('HTML version of PDF'), `expected HTML fallback, got: ${res.output.slice(0, 200)}`);
      assert.ok(res.output.includes('HTML rendition of the paper'));
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('replaces the raw output with the LLM summary when an engine factory is present', async () => {
    const res = await webFetchTool.execute(
      { url: `${base}/summarize-me`, prompt: 'summarize-success' },
      ctx({
        _providerConfig: {} as any,
        _engineFactory: () => ({
          // eslint-disable-next-line require-yield
          async *submit() {
            yield { type: 'text_delta', text: 'SUMMARY: ' } as any;
            yield { type: 'text_delta', text: 'the gist' } as any;
            yield { type: 'other' } as any;
          },
          shutdown() {},
        }),
      }),
    );
    assert.ok(!res.isError, res.output);
    assert.equal(res.output, 'SUMMARY: the gist', 'output must be the trimmed summary');
    assert.ok(!res.output.includes('Status:'), 'summary must replace the raw formatted output');
  });

  it('keeps the raw output when the summary engine yields nothing', async () => {
    const res = await webFetchTool.execute(
      { url: `${base}/summarize-me`, prompt: 'summarize-empty' },
      ctx({
        _providerConfig: {} as any,
        _engineFactory: () => ({
          // eslint-disable-next-line require-yield
          async *submit() { /* yields no text */ },
        }),
      }),
    );
    assert.ok(!res.isError, res.output);
    assert.ok(res.output.includes('Status: 200'), 'raw output must survive an empty summary');
  });

  it('keeps the raw output when the summary engine throws', async () => {
    const res = await webFetchTool.execute(
      { url: `${base}/summarize-me`, prompt: 'summarize-throws' },
      ctx({
        _providerConfig: {} as any,
        _engineFactory: () => { throw new Error('engine boom'); },
      }),
    );
    assert.ok(!res.isError, res.output);
    assert.ok(res.output.includes('Status: 200'), 'raw output must survive an engine failure');
  });
});
