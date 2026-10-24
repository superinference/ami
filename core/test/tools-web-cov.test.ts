// Coverage-focused, behavior-driven tests for the "tools-web" group:
//   src/tools/web-fetch.ts, web-search.ts, web-utils.ts
//
// ESM-safe: no source text is read; no imported namespace binding is
// monkeypatched. Mocking is done via node:test mock.method on a mockable
// object (Date), real temp dirs, a real mock HTTP server, and the
// behavior-preserving dependency seams exposed by the sources
// (_webSearchHooks, _webFetchHttpGet, validateUrlSafety's lookup param).

import { describe, it, before, after, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { webFetchTool, getCached, setCache } from '../src/tools/web-fetch';
import { webSearchTool } from '../src/tools/web-search';
import type { WebSearchHooks } from '../src/tools/web-search';
import { httpGet, validateUrlSafety } from '../src/tools/web-utils';
import type { HostLookupAll } from '../src/tools/web-utils';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

const uniq = () => `k-${Math.random().toString(36).slice(2)}-${Date.now()}`;

// Canned-response HTTP getter matching the shape of web-utils.httpGet. Used as a
// _webSearchHooks.httpGet so parse/filter/formatting logic runs for real without
// a network. `routes` maps an exact URL (or '*') to a response.
type Resp = { body: string; statusCode: number; contentType?: string };
function mockHttpGet(routes: Record<string, Resp | (() => never)>): typeof httpGet {
  return (async (url: string) => {
    const r = routes[url] ?? routes['*'];
    if (!r) throw new Error(`no mock route for ${url}`);
    if (typeof r === 'function') r();
    const rr = r as Resp;
    return {
      body: rr.body,
      statusCode: rr.statusCode,
      contentType: rr.contentType ?? 'text/html',
      finalUrl: url,
    };
  }) as unknown as typeof httpGet;
}

const DDG_HTML = (port: number) => `
  <div class="result results_links web-result">
    <a class="result__a" href="http://127.0.0.1:${port}/page1">First Title</a>
    <a class="result__snippet">First snippet text.</a>
  </div>
  <div class="result results_links web-result">
    <a class="result__a" href="?uddg=http%3A%2F%2F127.0.0.1%3A${port}%2Fpage2">Second Title</a>
    <span class="result__snippet">Second snippet.</span>
  </div>
`;

// ===========================================================================
// web-fetch: byte-limited LRU cache (TTL expiry / re-set / eviction)
// These branches are unreachable through execute() because it always consults
// getCached (which deletes expired/absent keys) before setCache, so the cache
// helpers are exercised directly through their test seam.
// ===========================================================================

describe('web-fetch cache helpers', () => {
  it('getCached returns null for an absent key', () => {
    assert.equal(getCached(uniq()), null);
  });

  it('round-trips a stored value', () => {
    const k = uniq();
    setCache(k, 'hello world');
    assert.equal(getCached(k), 'hello world');
  });

  it('re-setting an existing key replaces the entry (existing-branch)', () => {
    const k = uniq();
    setCache(k, 'first-value');
    setCache(k, 'second-value');
    assert.equal(getCached(k), 'second-value');
  });

  it('expires an entry once the TTL elapses, then deletes it', () => {
    const k = uniq();
    const t0 = Date.now();
    setCache(k, 'fresh');
    assert.equal(getCached(k), 'fresh');

    const nowMock = mock.method(Date, 'now', () => t0 + 16 * 60 * 1000);
    try {
      // >15min since the timestamp => expiry branch returns null and deletes.
      assert.equal(getCached(k), null);
    } finally {
      nowMock.mock.restore();
    }
    // The entry was deleted on expiry; a real-time lookup is still a miss.
    assert.equal(getCached(k), null);
  });

  it('evicts the oldest entry when the byte budget is exceeded', () => {
    const bigKey = uniq();
    const smallKey = uniq();
    // One entry larger than the 50MB cap forces the eviction loop to run on the
    // next insert (it must drop bigKey to get back under the ceiling).
    const big = 'x'.repeat(51 * 1024 * 1024);
    setCache(bigKey, big);
    assert.equal(getCached(bigKey), big);
    setCache(smallKey, 'tiny');
    assert.equal(getCached(bigKey), null, 'oversized entry should have been evicted');
    assert.equal(getCached(smallKey), 'tiny');
  });
});

// ===========================================================================
// web-fetch: execute() branches reachable only with the httpGet seam / abort
// ===========================================================================

describe('webFetchTool.execute — redirect + abort branches', () => {
  it('reports a cross-host redirect without treating it as an error', async () => {
    const seam: typeof httpGet = (async () => ({
      body: 'ignored',
      statusCode: 200,
      contentType: 'text/html',
      finalUrl: 'https://elsewhere.example/landing',
    })) as unknown as typeof httpGet;

    const result = await webFetchTool.execute(
      { url: 'https://origin.example/start' },
      ctx({ _allowLocalhostForTesting: true, _webFetchHttpGet: seam }),
    );
    assert.equal(result.isError, undefined);
    assert.match(result.output, /Redirect detected/);
    assert.match(result.output, /elsewhere\.example/);
  });

  it('falls back to the deadline-only signal when no abortSignal is provided', async () => {
    const seam: typeof httpGet = (async () => ({
      body: 'plain body text',
      statusCode: 200,
      contentType: 'text/plain',
      finalUrl: 'https://plain.example/doc.txt',
    })) as unknown as typeof httpGet;
    const result = await webFetchTool.execute(
      { url: 'https://plain.example/doc.txt' },
      // abortSignal omitted on purpose -> exercises the `: deadlineSignal` arm.
      ctx({ cwd: fs.mkdtempSync(path.join(os.tmpdir(), 'wf-')), _allowLocalhostForTesting: true, _webFetchHttpGet: seam, abortSignal: undefined as unknown as AbortSignal }),
    );
    assert.equal(result.isError, undefined);
    assert.match(result.output, /plain body text/);
  });

  it('stringifies a non-Error thrown by the fetcher', async () => {
    const seam: typeof httpGet = (async () => { throw 'raw-string-failure'; }) as unknown as typeof httpGet;
    const result = await webFetchTool.execute(
      { url: 'https://boom.example/x' },
      ctx({ _allowLocalhostForTesting: true, _webFetchHttpGet: seam }),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /Error fetching .*raw-string-failure/);
  });

  it('blocks a domain matched via a parent-domain suffix (blocked_domains endsWith)', async () => {
    const result = await webFetchTool.execute(
      { url: 'https://api.sub.example.com/data', blocked_domains: ['example.com'] },
      ctx({ _allowLocalhostForTesting: true }),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /is blocked/);
  });

  it('returns "Fetch aborted." when the request signal is already aborted', async () => {
    // Uses the real httpGet (default seam branch): a pre-aborted signal makes
    // httpGet reject with "Aborted" while the overall deadline has NOT fired.
    const result = await webFetchTool.execute(
      { url: 'https://example.com/anything' },
      ctx({ _allowLocalhostForTesting: true, abortSignal: AbortSignal.abort() }),
    );
    assert.equal(result.isError, true);
    assert.equal(result.output, 'Fetch aborted.');
  });
});

// ===========================================================================
// web-search: full success path driven by the _webSearchHooks seam
// ===========================================================================

describe('webSearchTool.execute — success path (real mock HTTP server)', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    server = http.createServer((req, res) => {
      if (req.url?.startsWith('/search')) {
        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end(DDG_HTML(port));
      } else if (req.url === '/page1' || req.url === '/page2') {
        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end(`<html><body><p>Full article body for ${req.url}.</p></body></html>`);
      } else {
        res.writeHead(404);
        res.end('nope');
      }
    });
    await new Promise<void>(resolve => server.listen(0, '127.0.0.1', () => resolve()));
    port = (server.address() as { port: number }).port;
  });

  after(() => server.close());

  function liveHooks(): WebSearchHooks {
    return {
      httpGet,
      // Bypass SSRF for the 127.0.0.1 result URLs so fetchResultContent runs.
      validateUrlSafety: (async () => ({ resolvedIP: '127.0.0.1' })) as typeof validateUrlSafety,
      searchUrl: (q: string) => `http://127.0.0.1:${port}/search?q=${encodeURIComponent(q)}`,
    };
  }

  it('parses results, fetches top-result content, and appends the reminder', async () => {
    const result = await webSearchTool.execute(
      { query: 'hello world' },
      ctx({ _webSearchHooks: liveHooks() }),
    );
    assert.equal(result.isError, undefined);
    assert.match(result.output, /Search results for: "hello world"/);
    assert.match(result.output, /First Title/);
    assert.match(result.output, /Second Title/);
    assert.match(result.output, /Content from http:\/\/127\.0\.0\.1/);
    assert.match(result.output, /Full article body/);
    assert.match(result.output, /REMINDER: Include sources/);
  });
});

// ===========================================================================
// web-search: edge paths driven by an injected mock httpGet (no network)
// ===========================================================================

describe('webSearchTool.execute — hook-driven edge paths', () => {
  const SEARCH = 'mock://search';
  const P1 = 'http://127.0.0.1:1/page1';
  const P2 = 'http://127.0.0.1:1/page2';
  const okValidate = (async () => ({ resolvedIP: '127.0.0.1' })) as typeof validateUrlSafety;
  const ddg = `
    <div class="result web-result">
      <a class="result__a" href="${P1}">Alpha</a>
      <a class="result__snippet">Alpha snippet.</a>
    </div>
    <div class="result web-result">
      <a class="result__a" href="not a url">Bad</a>
      <span class="result__snippet">Bad snippet.</span>
    </div>`;

  function hooks(partial: Partial<WebSearchHooks>): WebSearchHooks {
    return { httpGet, validateUrlSafety: okValidate, searchUrl: () => SEARCH, ...partial };
  }

  it('returns an error when the search endpoint responds >= 400', async () => {
    const h = hooks({ httpGet: mockHttpGet({ [SEARCH]: { body: '', statusCode: 503 } }) });
    const result = await webSearchTool.execute({ query: 'q' }, ctx({ _webSearchHooks: h }));
    assert.equal(result.isError, true);
    assert.match(result.output, /DuckDuckGo returned HTTP 503/);
  });

  it('reports "No results found" when the page has no result blocks', async () => {
    const h = hooks({ httpGet: mockHttpGet({ [SEARCH]: { body: '<html>empty</html>', statusCode: 200 } }) });
    const result = await webSearchTool.execute({ query: 'ghost' }, ctx({ _webSearchHooks: h }));
    assert.equal(result.isError, undefined);
    assert.match(result.output, /No results found for "ghost"/);
  });

  it('allowed_domains keeps matching hosts and drops invalid/other URLs', async () => {
    const h = hooks({
      httpGet: mockHttpGet({ [SEARCH]: { body: ddg, statusCode: 200 }, '*': { body: '<p>c</p>', statusCode: 200 } }),
    });
    const result = await webSearchTool.execute(
      { query: 'q', allowed_domains: ['127.0.0.1'] },
      ctx({ _webSearchHooks: h }),
    );
    assert.match(result.output, /Alpha/);
    assert.doesNotMatch(result.output, /\] \(not a url\)|Bad/);
  });

  it('blocked_domains drops matching hosts and keeps the unparseable URL', async () => {
    const h = hooks({
      httpGet: mockHttpGet({ [SEARCH]: { body: ddg, statusCode: 200 }, '*': { body: '<p>c</p>', statusCode: 200 } }),
    });
    const result = await webSearchTool.execute(
      { query: 'q', blocked_domains: ['127.0.0.1'] },
      ctx({ _webSearchHooks: h }),
    );
    assert.doesNotMatch(result.output, /Alpha/);
    assert.match(result.output, /Bad/);
  });

  const ddgNamed = `
    <div class="result web-result">
      <a class="result__a" href="https://docs.example.com/guide">Docs</a>
      <a class="result__snippet">Docs snippet.</a>
    </div>`;

  it('allowed_domains matches a subdomain via the parent suffix (endsWith arm)', async () => {
    const h = hooks({
      validateUrlSafety: (async () => ({ error: 'skip-content' })) as typeof validateUrlSafety,
      httpGet: mockHttpGet({ [SEARCH]: { body: ddgNamed, statusCode: 200 } }),
    });
    const result = await webSearchTool.execute(
      { query: 'q', allowed_domains: ['example.com'] },
      ctx({ _webSearchHooks: h }),
    );
    assert.match(result.output, /Docs/);
  });

  it('blocked_domains matches a subdomain via the parent suffix (endsWith arm)', async () => {
    const h = hooks({
      httpGet: mockHttpGet({ [SEARCH]: { body: ddgNamed, statusCode: 200 } }),
    });
    const result = await webSearchTool.execute(
      { query: 'q', blocked_domains: ['example.com'] },
      ctx({ _webSearchHooks: h }),
    );
    assert.match(result.output, /No results found/);
  });

  it('fetchResultContent swallows an SSRF validation error (no content shown)', async () => {
    const h = hooks({
      validateUrlSafety: (async () => ({ error: 'blocked' })) as typeof validateUrlSafety,
      httpGet: mockHttpGet({ [SEARCH]: { body: ddg, statusCode: 200 } }),
    });
    const result = await webSearchTool.execute({ query: 'q' }, ctx({ _webSearchHooks: h }));
    assert.match(result.output, /Alpha/);
    assert.doesNotMatch(result.output, /--- Content from/);
  });

  it('fetchResultContent drops >= 400 page responses', async () => {
    const h = hooks({
      httpGet: mockHttpGet({ [SEARCH]: { body: ddg, statusCode: 200 }, [P1]: { body: 'x', statusCode: 404 }, '*': { body: 'x', statusCode: 404 } }),
    });
    const result = await webSearchTool.execute({ query: 'q' }, ctx({ _webSearchHooks: h }));
    assert.match(result.output, /Alpha/);
    assert.doesNotMatch(result.output, /--- Content from/);
  });

  it('fetchResultContent swallows a thrown fetch error', async () => {
    const h = hooks({
      httpGet: mockHttpGet({
        [SEARCH]: { body: ddg, statusCode: 200 },
        [P1]: () => { throw new Error('boom'); },
        '*': () => { throw new Error('boom'); },
      }),
    });
    const result = await webSearchTool.execute({ query: 'q' }, ctx({ _webSearchHooks: h }));
    assert.match(result.output, /Alpha/);
    assert.doesNotMatch(result.output, /--- Content from/);
  });

  it('returns "Search aborted." when httpGet rejects with Aborted', async () => {
    const h = hooks({ httpGet: (async () => { throw new Error('Aborted'); }) as unknown as typeof httpGet });
    const result = await webSearchTool.execute({ query: 'q' }, ctx({ _webSearchHooks: h }));
    assert.equal(result.isError, true);
    assert.equal(result.output, 'Search aborted.');
  });

  it('surfaces a generic fetch error', async () => {
    const h = hooks({ httpGet: (async () => { throw new Error('kaboom'); }) as unknown as typeof httpGet });
    const result = await webSearchTool.execute({ query: 'q' }, ctx({ _webSearchHooks: h }));
    assert.equal(result.isError, true);
    assert.match(result.output, /Error searching for "q": kaboom/);
  });
  void P2;
});

// ===========================================================================
// web-search: session budget with token-bucket refill
// ===========================================================================

describe('webSearchTool.execute — budget', () => {
  const okValidate = (async () => ({ resolvedIP: '127.0.0.1' })) as typeof validateUrlSafety;
  const h = (): WebSearchHooks => ({
    httpGet: mockHttpGet({ '*': { body: '<html>empty</html>', statusCode: 200 } }),
    validateUrlSafety: okValidate,
    searchUrl: () => 'mock://s',
  });

  it('refills the token bucket before counting the request', async () => {
    const budget = { count: 5, max: 10, lastRefillTime: Date.now() - 2 * 3_600_000, refillRate: 3 };
    const result = await webSearchTool.execute(
      { query: 'q' },
      ctx({ _webSearchHooks: h(), _webSearchBudget: budget }),
    );
    // 2h * 3/h = 6 refilled => count 5->0, then +1 for this request.
    assert.equal(budget.count, 1);
    assert.equal(result.isError, undefined);
  });

  it('blocks once the per-session cap is exceeded', async () => {
    const budget = { count: 10, max: 10 };
    const result = await webSearchTool.execute(
      { query: 'q' },
      ctx({ _webSearchHooks: h(), _webSearchBudget: budget }),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /web search limit reached \(10 per session\)/);
  });
});

// ===========================================================================
// web-utils: validateUrlSafety DNS-empty guard (seam) + res 'error' handler
// ===========================================================================

describe('web-utils validateUrlSafety — DNS lookup seam', () => {
  const lookupReturning = (addrs: Array<{ address: string; family: number }>): HostLookupAll =>
    ((_h, _o, cb) => cb(null, addrs as never));

  it('blocks a host whose DNS yields no usable (valid) addresses', async () => {
    const res = await validateUrlSafety('https://no-usable.example/', lookupReturning([
      { address: '', family: 4 },
      { address: 'not-an-ip', family: 4 },
    ]));
    assert.ok('error' in res);
    assert.match(res.error, /DNS resolution failed/);
  });

  it('allows a host that resolves to a valid public IP (seam bypass variant)', async () => {
    const res = await validateUrlSafety('https://ok.example/', lookupReturning([
      { address: '93.184.216.34', family: 4 },
    ]));
    assert.ok('resolvedIP' in res);
    assert.equal(res.resolvedIP, '93.184.216.34');
  });

  it('still blocks when the resolved IP is private', async () => {
    const res = await validateUrlSafety('https://sneaky.example/', lookupReturning([
      { address: '10.0.0.5', family: 4 },
    ]));
    assert.ok('error' in res);
    assert.match(res.error, /private IP 10\.0\.0\.5/);
  });
});

describe('web-utils httpGet — response stream error', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    server = http.createServer((req, res) => {
      // Announce more bytes than we send, then abruptly destroy the socket so
      // the client's response stream emits 'error' (premature close).
      res.writeHead(200, { 'Content-Type': 'text/plain', 'Content-Length': '1000' });
      res.write('partial');
      setImmediate(() => res.socket?.destroy());
    });
    await new Promise<void>(resolve => server.listen(0, '127.0.0.1', () => resolve()));
    port = (server.address() as { port: number }).port;
  });

  after(() => server.close());

  it('rejects when the response is cut off mid-body', async () => {
    await assert.rejects(
      httpGet(`http://127.0.0.1:${port}/`, new AbortController().signal),
    );
  });
});

describe('web-utils httpGet — body cap truncation', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    server = http.createServer((_req, res) => {
      res.writeHead(200, { 'Content-Type': 'text/plain' });
      // Stream well past the 1MB cap in many chunks so the client hits the cap
      // and stops reading mid-stream (and may receive already-buffered chunks
      // after it has settled — the settled-guard arm in the data handler).
      const chunk = Buffer.alloc(64 * 1024, 0x61);
      for (let i = 0; i < 48; i++) res.write(chunk);
      res.end();
    });
    await new Promise<void>(resolve => server.listen(0, '127.0.0.1', () => resolve()));
    port = (server.address() as { port: number }).port;
  });

  after(() => server.close());

  it('caps the body at MAX_HTTP_BODY_BYTES and flags truncation', async () => {
    const { body, truncated } = await httpGet(`http://127.0.0.1:${port}/`, new AbortController().signal);
    assert.equal(truncated, true);
    assert.ok(body.length <= 1_048_576);
  });
});

// ===========================================================================
// web-fetch: binary spill to a temp cwd (sanity — exercises the raw-bytes path)
// ===========================================================================

describe('webFetchTool.execute — binary content spill', () => {
  let dir: string;
  before(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'webfetch-')); });
  after(() => { fs.rmSync(dir, { recursive: true, force: true }); });

  it('writes binary payloads to disk instead of the model context', async () => {
    const seam: typeof httpGet = (async () => ({
      body: 'binarydata',
      bodyBuffer: Buffer.from([0, 1, 2, 3, 255]),
      statusCode: 200,
      contentType: 'application/octet-stream',
      finalUrl: 'https://cdn.example/file.bin',
    })) as unknown as typeof httpGet;

    const result = await webFetchTool.execute(
      { url: 'https://cdn.example/file.bin' },
      ctx({ cwd: dir, _allowLocalhostForTesting: true, _webFetchHttpGet: seam }),
    );
    assert.match(result.output, /Binary content/);
  });
});
