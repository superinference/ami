/**
 * U072 (wave) — additive web_fetch parity improvements, all in web-fetch.ts /
 * web-utils.ts / web-preapproved.ts:
 *
 *  1. Redirect message reports the HTTP redirect status code + reason phrase
 *     (parity with Claude's status-code-to-text redirect message), threaded from
 *     the real httpGet redirect loop via a new `redirectStatusCode`.
 *  2. Time-bounded PDF text extraction (parsePdfWithTimeout) so a hostile/hung
 *     PDF cannot block the agent; the parser is always destroyed.
 *  3. HTML-version-of-PDF fallback uses the Markdown converter so source links
 *     survive as [text](url) instead of being stripped to plain text.
 *  4. Optional caller-supplied regex `filter` to cut a verbose page to the lines
 *     that match (filterTextByPattern).
 *  5. Path-scoped allowed_domains / blocked_domains entries (`host/path`) with
 *     segment-boundary enforcement (matchDomainRule).
 */

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import type * as http from 'node:http';
import type * as https from 'node:https';
import {
  webFetchTool,
  parsePdfWithTimeout,
  filterTextByPattern,
  PDF_PARSE_TIMEOUT_MS,
} from '../src/tools/web-fetch';
import { matchDomainRule } from '../src/tools/web-preapproved';
import { httpGet, _webHooks } from '../src/tools/web-utils';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _allowLocalhostForTesting: true,
    ...overrides,
  };
}

/** httpGet seam returning a single successful response (optionally cross-host). */
function mockGet(resp: Record<string, unknown>, byUrl?: (url: string) => Record<string, unknown>) {
  return (async (url: string) => (byUrl ? byUrl(url) : { finalUrl: url, ...resp })) as unknown;
}

// ---------------------------------------------------------------------------
// 1. parsePdfWithTimeout — bounded extraction + guaranteed destroy
// ---------------------------------------------------------------------------

describe('parsePdfWithTimeout', () => {
  it('returns the extracted text and destroys the parser on success', async () => {
    let destroyed = false;
    const parser = {
      getText: async () => ({ text: 'hello', total: 1 }),
      destroy: () => { destroyed = true; },
    };
    const out = await parsePdfWithTimeout(parser, 1000);
    assert.deepEqual(out, { text: 'hello', total: 1 });
    assert.equal(destroyed, true, 'parser must be destroyed on success');
  });

  it('rejects with a deadline error and still destroys the parser on a hang', async () => {
    let destroyed = false;
    const parser = {
      // Never resolves → the timeout must win.
      getText: () => new Promise<{ text: string }>(() => {}),
      destroy: () => { destroyed = true; },
    };
    await assert.rejects(parsePdfWithTimeout(parser, 10), /exceeded 10ms deadline/);
    assert.equal(destroyed, true, 'parser must be destroyed even on timeout');
  });

  it('swallows a destroy() failure (best-effort release) and still returns', async () => {
    const parser = {
      getText: async () => ({ text: 'ok' }),
      destroy: () => { throw new Error('destroy boom'); },
    };
    const out = await parsePdfWithTimeout(parser, 1000);
    assert.deepEqual(out, { text: 'ok' });
  });

  it('exposes a sane default deadline constant', () => {
    assert.equal(PDF_PARSE_TIMEOUT_MS, 30_000);
  });
});

// ---------------------------------------------------------------------------
// 2. filterTextByPattern — regex line filter
// ---------------------------------------------------------------------------

describe('filterTextByPattern', () => {
  it('keeps only matching lines', () => {
    const r = filterTextByPattern('alpha\nERROR x\nbeta\nERROR y', 'ERROR');
    assert.deepEqual(r, { text: 'ERROR x\nERROR y' });
  });

  it('returns an error for an invalid regex', () => {
    const r = filterTextByPattern('a\nb', '(');
    assert.ok('error' in r);
    assert.match((r as { error: string }).error, /invalid filter regex/);
  });
});

// ---------------------------------------------------------------------------
// 3. matchDomainRule — host-only + path-scoped with segment boundary
// ---------------------------------------------------------------------------

describe('matchDomainRule', () => {
  it('matches a bare host exactly and via subdomain', () => {
    assert.ok(matchDomainRule('example.com', '/x', 'example.com'));
    assert.ok(matchDomainRule('api.example.com', '/x', 'example.com'));
    assert.ok(!matchDomainRule('evil.com', '/x', 'example.com'));
  });

  it('matches a path-scoped entry only on a segment boundary', () => {
    assert.ok(matchDomainRule('github.com', '/anthropics', 'github.com/anthropics'));
    assert.ok(matchDomainRule('github.com', '/anthropics/claude-code', 'github.com/anthropics'));
    // prefix-spoofing guard: a sibling path that merely shares the prefix is NOT a match.
    assert.ok(!matchDomainRule('github.com', '/anthropics-evil/malware', 'github.com/anthropics'));
  });

  it('path-scoped entry requires the host to match too', () => {
    assert.ok(!matchDomainRule('gitlab.com', '/anthropics', 'github.com/anthropics'));
  });
});

// ---------------------------------------------------------------------------
// 4. web_fetch — path-scoped allowed_domains through execute()
// ---------------------------------------------------------------------------

describe('web_fetch path-scoped allowed_domains', () => {
  it('allows a URL under the approved path prefix', async () => {
    const res = await webFetchTool.execute(
      {
        url: 'https://github.com/anthropics/claude-code',
        prompt: 'summarize',
        allowed_domains: ['github.com/anthropics'],
      },
      ctx({ _webFetchHttpGet: mockGet({ body: 'docs', statusCode: 200, contentType: 'text/plain' }) }),
    );
    assert.notEqual(res.isError, true);
    assert.match(res.output, /docs/);
  });

  it('blocks a sibling path that only shares the prefix', async () => {
    const res = await webFetchTool.execute(
      {
        url: 'https://github.com/anthropics-evil/malware',
        prompt: 'summarize',
        allowed_domains: ['github.com/anthropics'],
      },
      ctx({ _webFetchHttpGet: mockGet({ body: 'docs', statusCode: 200, contentType: 'text/plain' }) }),
    );
    assert.equal(res.isError, true);
    assert.match(res.output, /not in the allowed domains list/);
  });
});

// ---------------------------------------------------------------------------
// 5. web_fetch — redirect message carries the redirect status + reason phrase
// ---------------------------------------------------------------------------

describe('web_fetch redirect status message', () => {
  it('reports the HTTP redirect code + reason phrase on a cross-host redirect', async () => {
    const res = await webFetchTool.execute(
      { url: 'https://orig.example/', prompt: 'summarize' },
      ctx({
        _webFetchHttpGet: mockGet({
          body: 'x',
          statusCode: 200,
          contentType: 'text/plain',
          finalUrl: 'https://other.example/',
          redirectStatusCode: 301,
        }),
      }),
    );
    assert.match(res.output, /Redirect detected: https:\/\/orig\.example\/ → https:\/\/other\.example\//);
    assert.match(res.output, /\(HTTP 301 Moved Permanently\)/);
    // The prompt is carried forward for the one-step re-fetch.
    assert.match(res.output, /prompt="summarize"/);
  });
});

// ---------------------------------------------------------------------------
// 6. web_fetch — regex filter through execute()
// ---------------------------------------------------------------------------

describe('web_fetch filter', () => {
  it('keeps only lines matching the filter', async () => {
    const res = await webFetchTool.execute(
      {
        url: 'https://logs.example/log.txt',
        prompt: 'find errors',
        filter: 'ERROR',
      },
      ctx({
        _webFetchHttpGet: mockGet({
          body: 'INFO start\nERROR boom\nDEBUG noise\nERROR again',
          statusCode: 200,
          contentType: 'text/plain',
        }),
      }),
    );
    assert.notEqual(res.isError, true);
    assert.match(res.output, /ERROR boom/);
    assert.match(res.output, /ERROR again/);
    assert.doesNotMatch(res.output, /DEBUG noise/);
    assert.doesNotMatch(res.output, /INFO start/);
  });

  it('surfaces an invalid filter regex as a tool error', async () => {
    const res = await webFetchTool.execute(
      {
        url: 'https://logs.example/log.txt',
        prompt: 'x',
        filter: '(',
        offset: 0,
      },
      ctx({
        _webFetchHttpGet: mockGet({ body: 'a\nb', statusCode: 200, contentType: 'text/plain' }),
      }),
    );
    assert.equal(res.isError, true);
    assert.match(res.output, /invalid filter regex/);
  });

  it('rejects a non-string filter', async () => {
    const res = await webFetchTool.execute(
      { url: 'https://logs.example/log.txt', prompt: 'x', filter: 123 },
      ctx({ _webFetchHttpGet: mockGet({ body: 'a', statusCode: 200, contentType: 'text/plain' }) }),
    );
    assert.equal(res.isError, true);
    assert.match(res.output, /filter must be a regular-expression string/);
  });
});

// ---------------------------------------------------------------------------
// 7. web_fetch — HTML-version-of-PDF fallback preserves links (markdown)
// ---------------------------------------------------------------------------

describe('web_fetch PDF→HTML fallback markdown', () => {
  it('keeps link targets from the HTML fallback as [text](url)', async () => {
    const pdfUrl = 'https://arxiv.org/pdf/2301.00001';
    const htmlBody =
      '<html><body><p>See the <a href="https://example.com/docs">docs</a> for more details. ' +
      'This paragraph is intentionally long enough to clear the 200-character minimum that the ' +
      'HTML-version-of-PDF fallback requires before it returns the converted content to the caller.</p></body></html>';
    const res = await webFetchTool.execute(
      { url: pdfUrl, prompt: 'summarize' },
      ctx({
        _webFetchHttpGet: mockGet({}, (url) => {
          if (url.includes('/pdf/')) {
            return {
              body: '%PDF-1.4\nnot a real pdf',
              bodyBuffer: Buffer.from('%PDF-1.4\nnot a real pdf'),
              statusCode: 200,
              contentType: 'application/pdf',
              finalUrl: url,
            };
          }
          return { body: htmlBody, statusCode: 200, contentType: 'text/html', finalUrl: url };
        }),
      }),
    );
    assert.notEqual(res.isError, true);
    assert.match(res.output, /HTML version of PDF/);
    assert.match(res.output, /\[docs\]\(https:\/\/example\.com\/docs\)/);
  });
});

// ---------------------------------------------------------------------------
// 8. E2E — redirectStatusCode threaded through the real httpGet redirect loop
// ---------------------------------------------------------------------------

type HopMap = Record<string, { status: number; location?: string; body?: string }>;

function seamFor(hops: HopMap): typeof _webHooks.rawGet {
  return ((
    _requester: typeof http | typeof https,
    url: string,
    _options: http.RequestOptions,
    callback: (res: http.IncomingMessage) => void,
  ) => {
    const hop = hops[url];
    const res = new EventEmitter() as unknown as http.IncomingMessage & { resume: () => void };
    res.resume = () => {};
    if (hop && hop.location) {
      res.statusCode = hop.status;
      res.headers = { location: hop.location };
      process.nextTick(() => callback(res));
    } else {
      res.statusCode = hop ? hop.status : 404;
      res.headers = { 'content-type': 'text/plain' };
      process.nextTick(() => {
        callback(res);
        process.nextTick(() => {
          res.emit('data', Buffer.from(hop?.body ?? ''));
          res.emit('end');
        });
      });
    }
    const req = new EventEmitter() as unknown as http.ClientRequest;
    (req as unknown as { destroy: () => void }).destroy = () => {};
    return req;
  }) as typeof _webHooks.rawGet;
}

describe('httpGet — surfaces the first redirect hop status (e2e)', () => {
  const origRawGet = _webHooks.rawGet;
  const origValidate = _webHooks.validateRedirectSafety;
  before(() => {
    _webHooks.validateRedirectSafety = async () => ({ resolvedIP: '93.184.216.34' });
  });
  after(() => {
    _webHooks.rawGet = origRawGet;
    _webHooks.validateRedirectSafety = origValidate;
  });

  it('returns redirectStatusCode for a followed cross-host redirect', async () => {
    _webHooks.rawGet = seamFor({
      'https://a.example/': { status: 302, location: 'https://b.example/final' },
      'https://b.example/final': { status: 200, body: 'FOLLOWED' },
    });
    const r = await httpGet('https://a.example/', new AbortController().signal, {
      maxRedirects: 3,
      resolvedIP: '93.184.216.34',
    });
    assert.equal(r.statusCode, 200);
    assert.equal(r.redirectStatusCode, 302);
  });

  it('leaves redirectStatusCode undefined when no redirect was followed', async () => {
    _webHooks.rawGet = seamFor({
      'https://direct.example/': { status: 200, body: 'OK' },
    });
    const r = await httpGet('https://direct.example/', new AbortController().signal, {
      resolvedIP: '93.184.216.34',
    });
    assert.equal(r.redirectStatusCode, undefined);
  });
});
