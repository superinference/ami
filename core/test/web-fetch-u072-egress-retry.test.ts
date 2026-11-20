/**
 * U072 — web_fetch egress-proxy structured error + transient-error retry.
 *
 * Covers two confirmed parity gaps:
 *  - Machine-readable structured error for enterprise egress/proxy blocks
 *    (403 + X-Proxy-Error / X-Squid-Error) — detectEgressBlock + the web_fetch
 *    execute() branch that returns it.
 *  - Transient-error classification + exponential-backoff retry for generic
 *    HTTP GETs — isTransientHttpError + httpGetWithRetry.
 */
import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'node:http';
import { webFetchTool, detectEgressBlock } from '../src/tools/web-fetch';
import {
  httpGetWithRetry,
  isTransientHttpError,
  DEFAULT_HTTP_RETRY_DELAYS_MS,
} from '../src/tools/web-utils';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _allowLocalhostForTesting: true,
    ...overrides,
  };
}

// A deterministic httpGet seam yielding a fixed status/headers response.
function mockGet(resp: {
  statusCode: number;
  headers?: http.IncomingHttpHeaders;
  body?: string;
  contentType?: string;
}) {
  return (async (url: string) => ({
    body: resp.body ?? 'page body',
    statusCode: resp.statusCode,
    contentType: resp.contentType ?? 'text/plain',
    finalUrl: url,
    headers: resp.headers,
  })) as unknown;
}

// ---------------------------------------------------------------------------
// detectEgressBlock — structured error classification
// ---------------------------------------------------------------------------
describe('detectEgressBlock', () => {
  it('returns a structured JSON error for 403 + X-Proxy-Error', () => {
    const out = detectEgressBlock(403, { 'x-proxy-error': 'blocked-by-allowlist' }, 'blocked.example');
    assert.ok(out);
    const parsed = JSON.parse(out!);
    assert.equal(parsed.error_type, 'EGRESS_BLOCKED');
    assert.equal(parsed.domain, 'blocked.example');
    assert.match(parsed.message, /network egress proxy/);
  });

  it('also recognizes a Squid proxy error header', () => {
    const out = detectEgressBlock(403, { 'x-squid-error': 'ERR_ACCESS_DENIED 0' }, 'squid.example');
    assert.ok(out);
    assert.equal(JSON.parse(out!).error_type, 'EGRESS_BLOCKED');
  });

  it('returns null for an ordinary 403 with no proxy header', () => {
    assert.equal(detectEgressBlock(403, { 'content-type': 'text/html' }, 'site.example'), null);
  });

  it('returns null for a non-403 status even with a proxy header', () => {
    assert.equal(detectEgressBlock(200, { 'x-proxy-error': 'x' }, 'site.example'), null);
  });

  it('returns null when headers are absent', () => {
    assert.equal(detectEgressBlock(403, undefined, 'site.example'), null);
  });
});

// ---------------------------------------------------------------------------
// web_fetch execute() — egress block surfaced as a structured tool error
// ---------------------------------------------------------------------------
describe('web_fetch egress-proxy block', () => {
  it('returns the structured EGRESS_BLOCKED error when the proxy blocks the fetch', async () => {
    const res = await webFetchTool.execute(
      { url: 'https://blocked.example/a', prompt: 'read it' },
      ctx({ _webFetchHttpGet: mockGet({ statusCode: 403, headers: { 'x-proxy-error': 'blocked-by-allowlist' } }) }),
    );
    assert.equal(res.isError, true);
    const parsed = JSON.parse(res.output);
    assert.equal(parsed.error_type, 'EGRESS_BLOCKED');
    assert.equal(parsed.domain, 'blocked.example');
  });

  it('treats an ordinary 403 (no proxy header) as normal content, not an egress block', async () => {
    const res = await webFetchTool.execute(
      { url: 'https://site.example/a', prompt: 'read it' },
      ctx({ _webFetchHttpGet: mockGet({ statusCode: 403, body: 'Forbidden page body' }) }),
    );
    assert.equal(res.isError, undefined);
    assert.doesNotMatch(res.output, /EGRESS_BLOCKED/);
    assert.match(res.output, /Status: 403 Forbidden/);
  });
});

// ---------------------------------------------------------------------------
// isTransientHttpError — retry classification
// ---------------------------------------------------------------------------
describe('isTransientHttpError', () => {
  it('classifies connection/network errors as transient', () => {
    assert.equal(isTransientHttpError(new Error('connect ECONNREFUSED 127.0.0.1:1')), true);
    assert.equal(isTransientHttpError(new Error('Request timed out after 30000ms')), true);
  });
  it('never retries aborts, SSRF blocks, redirect loops, or bad URLs', () => {
    assert.equal(isTransientHttpError(new Error('Aborted')), false);
    assert.equal(isTransientHttpError(new Error('Blocked: "x" is a private/metadata hostname')), false);
    assert.equal(isTransientHttpError(new Error('Too many redirects')), false);
    assert.equal(isTransientHttpError(new Error('Invalid URL: zzz')), false);
  });
  it('returns false for a non-Error value', () => {
    assert.equal(isTransientHttpError('nope'), false);
  });
});

// ---------------------------------------------------------------------------
// httpGetWithRetry — exponential-backoff retry against a real server
// ---------------------------------------------------------------------------
describe('httpGetWithRetry', () => {
  let server: http.Server;
  let base: string;
  let requestCount = 0;
  let flakyUntil = 0; // return 503 until this many requests have been served
  const pin = { resolvedIP: '127.0.0.1' };
  // Immediate backoff seam so the test never actually sleeps.
  const noSleep = (): Promise<void> => Promise.resolve();

  before(async () => {
    server = http.createServer((req, res) => {
      requestCount++;
      if (req.url === '/notfound') {
        res.writeHead(404);
        res.end('nope');
        return;
      }
      if (requestCount <= flakyUntil) {
        res.writeHead(503);
        res.end('try later');
        return;
      }
      res.writeHead(200, { 'content-type': 'text/plain' });
      res.end('ok');
    });
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
    const addr = server.address() as { port: number };
    base = `http://127.0.0.1:${addr.port}`;
  });

  after(() => server.close());

  it('exposes the reference-parity 4-step backoff schedule', () => {
    assert.deepEqual([...DEFAULT_HTTP_RETRY_DELAYS_MS], [2000, 4000, 8000, 16000]);
  });

  it('retries a transient 5xx then succeeds', async () => {
    requestCount = 0;
    flakyUntil = 2; // first two requests 503, third 200
    const res = await httpGetWithRetry(`${base}/x`, new AbortController().signal, {
      ...pin,
      retryDelaysMs: [1, 1, 1, 1],
      _delay: noSleep,
    });
    assert.equal(res.statusCode, 200);
    assert.equal(res.body, 'ok');
    assert.equal(requestCount, 3);
  });

  it('does NOT retry a non-transient 4xx', async () => {
    requestCount = 0;
    flakyUntil = 0;
    const res = await httpGetWithRetry(`${base}/notfound`, new AbortController().signal, {
      ...pin,
      retryDelaysMs: [1, 1],
      _delay: noSleep,
    });
    assert.equal(res.statusCode, 404);
    assert.equal(requestCount, 1);
  });

  it('gives up after exhausting retries on a persistent 5xx', async () => {
    requestCount = 0;
    flakyUntil = 99; // always 503
    const res = await httpGetWithRetry(`${base}/x`, new AbortController().signal, {
      ...pin,
      retryDelaysMs: [1, 1],
      _delay: noSleep,
    });
    assert.equal(res.statusCode, 503);
    assert.equal(requestCount, 3); // 1 initial + 2 retries
  });

  it('retries a transient transport error then rethrows it when exhausted', async () => {
    // A dead port => ECONNREFUSED on every attempt (transient => retried).
    let delays = 0;
    await assert.rejects(
      httpGetWithRetry('http://127.0.0.1:1/x', new AbortController().signal, {
        resolvedIP: '127.0.0.1',
        retryDelaysMs: [1, 1],
        _delay: () => {
          delays++;
          return Promise.resolve();
        },
      }),
      (err: Error) => /ECONNREFUSED/.test(err.message),
    );
    assert.equal(delays, 2); // backed off twice before the final throw
  });

  it('stops immediately when the signal is already aborted', async () => {
    const ac = new AbortController();
    ac.abort();
    await assert.rejects(
      httpGetWithRetry(`${base}/x`, ac.signal, { ...pin, _delay: noSleep }),
      /Aborted/,
    );
  });

  it('behaves like a single httpGet on first-attempt success', async () => {
    requestCount = 0;
    flakyUntil = 0;
    const res = await httpGetWithRetry(`${base}/x`, new AbortController().signal, pin);
    assert.equal(res.statusCode, 200);
    assert.equal(requestCount, 1);
  });
});
