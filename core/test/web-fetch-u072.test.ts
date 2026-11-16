import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  webFetchTool,
  makeSecondaryModelPrompt,
  httpStatusText,
  clearWebFetchCache,
  getCached,
  setCache,
  MAX_MARKDOWN_LENGTH,
} from '../src/tools/web-fetch';
import { isPreapprovedHost, PREAPPROVED_HOSTS } from '../src/tools/web-preapproved';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _allowLocalhostForTesting: true,
    ...overrides,
  };
}

// A deterministic httpGet seam returning a successful text/plain response.
function mockGet(body = 'untrusted page body', finalUrl?: string) {
  return (async (url: string) => ({
    body,
    statusCode: 200,
    contentType: 'text/plain',
    finalUrl: finalUrl ?? url,
  })) as unknown;
}

// ---------------------------------------------------------------------------
// U072 — makeSecondaryModelPrompt: strict vs permissive guideline branch
// ---------------------------------------------------------------------------

describe('makeSecondaryModelPrompt', () => {
  it('applies strict copyright/quote guardrails for an untrusted domain', () => {
    const p = makeSecondaryModelPrompt('PAGE', 'What is X?', false);
    assert.match(p, /125-character maximum/);
    assert.match(p, /Never produce or reproduce exact song lyrics/);
    assert.match(p, /never comment on the legality/);
    assert.match(p, /What is X\?/);
    assert.match(p, /PAGE/);
  });

  it('applies permissive excerpt guidelines for a preapproved domain', () => {
    const p = makeSecondaryModelPrompt('DOCS', 'Explain the API', true);
    assert.match(p, /code examples, and documentation excerpts/);
    assert.doesNotMatch(p, /125-character maximum/);
    assert.doesNotMatch(p, /song lyrics/);
  });
});

// ---------------------------------------------------------------------------
// U072 — summarizer prompt is wired to the strict/permissive branch via
// allowed_domains (trusted domain => permissive)
// ---------------------------------------------------------------------------

describe('web_fetch summarizer guideline wiring', () => {
  it('feeds STRICT guidelines to the summarizer for a non-preapproved domain', async () => {
    let seenPrompt = '';
    const res = await webFetchTool.execute(
      { url: 'https://untrusted.example/a', prompt: 'summarize' },
      ctx({
        _providerConfig: {} as never,
        _webFetchHttpGet: mockGet(),
        _engineFactory: ((_cfg: unknown) => ({
          async *submit(p: string) {
            seenPrompt = p;
            yield { type: 'text_delta', text: 'ok' } as never;
          },
          shutdown() {},
        })) as never,
      }),
    );
    assert.equal(res.isError, undefined);
    assert.match(seenPrompt, /125-character maximum/, 'untrusted domain must get strict guidelines');
  });

  it('feeds PERMISSIVE guidelines when the host is explicitly allow-listed', async () => {
    let seenPrompt = '';
    const res = await webFetchTool.execute(
      { url: 'https://docs.trusted.example/a', prompt: 'summarize', allowed_domains: ['trusted.example'] },
      ctx({
        _providerConfig: {} as never,
        _webFetchHttpGet: mockGet(),
        _engineFactory: ((_cfg: unknown) => ({
          async *submit(p: string) {
            seenPrompt = p;
            yield { type: 'text_delta', text: 'ok' } as never;
          },
          shutdown() {},
        })) as never,
      }),
    );
    assert.equal(res.isError, undefined);
    assert.match(seenPrompt, /documentation excerpts/, 'allow-listed domain must get permissive guidelines');
    assert.doesNotMatch(seenPrompt, /125-character maximum/);
  });
});

// ---------------------------------------------------------------------------
// U072 — cancellation/deadline honored and surfaced during summarization
// ---------------------------------------------------------------------------

describe('web_fetch summarization cancellation', () => {
  it('surfaces an abort error when cancellation arrives during summarization', async () => {
    const ac = new AbortController();
    const res = await webFetchTool.execute(
      { url: 'https://ex.ample/page', prompt: 'summarize' },
      ctx({
        abortSignal: ac.signal,
        _providerConfig: {} as never,
        _webFetchHttpGet: mockGet(),
        _engineFactory: ((_cfg: unknown) => ({
          async *submit() {
            // Cancellation arrives mid-summarization (after the page was fetched).
            ac.abort();
            yield { type: 'text_delta', text: 'partial summary that must be discarded' } as never;
          },
          shutdown() {},
        })) as never,
      }),
    );
    assert.equal(res.isError, true, 'cancellation during summarization must be an error');
    assert.match(res.output, /abort/i);
    assert.doesNotMatch(res.output, /partial summary/, 'partial summary must not be returned');
  });
});

// ---------------------------------------------------------------------------
// U072 — single-label hostname rejected pre-DNS
// ---------------------------------------------------------------------------

describe('web_fetch single-label hostname guard', () => {
  it('rejects a hostname with no dot', async () => {
    const res = await webFetchTool.execute(
      { url: 'https://intranet/secret', prompt: 'read it' },
      ctx({ _allowLocalhostForTesting: false }),
    );
    assert.equal(res.isError, true);
    assert.match(res.output, /publicly-resolvable domain/);
  });

  it('allows a normal dotted hostname past the guard', async () => {
    // With the testing seam the dotted host reaches the httpGet mock, proving the
    // single-label guard did not reject it.
    const res = await webFetchTool.execute(
      { url: 'https://example.com/a', prompt: 'read it' },
      ctx({ _webFetchHttpGet: mockGet('hello world') }),
    );
    assert.equal(res.isError, undefined);
  });
});

// ---------------------------------------------------------------------------
// U072 — description affordances + schema parity
// ---------------------------------------------------------------------------

describe('web_fetch description and schema parity', () => {
  it('discloses MCP preference, cache, summarization, gh CLI and the auth warning', () => {
    const d = webFetchTool.description;
    assert.match(d, /MCP-provided web-fetch tool/);
    assert.match(d, /15-minute cache/);
    assert.match(d, /summarized/);
    assert.match(d, /gh CLI/);
    assert.match(d, /WILL FAIL for authenticated or private URLs/);
    assert.match(d, /redirects to a different host, protocol, or port/);
  });

  it('marks prompt as required in the input schema', () => {
    assert.ok(webFetchTool.inputSchema.required?.includes('url'));
    assert.ok(webFetchTool.inputSchema.required?.includes('prompt'));
  });
});

// ---------------------------------------------------------------------------
// U072 — httpStatusText: numeric code -> human reason phrase
// ---------------------------------------------------------------------------

describe('httpStatusText', () => {
  it('maps common codes to their reason phrase', () => {
    assert.equal(httpStatusText(200), 'OK');
    assert.equal(httpStatusText(404), 'Not Found');
    assert.equal(httpStatusText(500), 'Internal Server Error');
  });
  it('returns an empty string for an unknown code', () => {
    assert.equal(httpStatusText(799), '');
  });
});

// ---------------------------------------------------------------------------
// U072 — fetch result surfaces received size + human HTTP status
// ---------------------------------------------------------------------------

describe('web_fetch result formatting', () => {
  it('reports the human status phrase and the received byte size', async () => {
    const res = await webFetchTool.execute(
      { url: 'https://example.com/a', prompt: 'read it' },
      ctx({ _webFetchHttpGet: mockGet('hello world') }),
    );
    assert.equal(res.isError, undefined);
    assert.match(res.output, /Status: 200 OK/);
    assert.match(res.output, /Received: \d/);
  });
});

// ---------------------------------------------------------------------------
// U072 — built-in preapproved trusted-docs host list
// ---------------------------------------------------------------------------

describe('isPreapprovedHost (PREAPPROVED_HOSTS config knob)', () => {
  it('matches a curated hostname-only doc host', () => {
    assert.ok(PREAPPROVED_HOSTS.has('docs.python.org'));
    assert.ok(isPreapprovedHost('docs.python.org', '/3/library/os.html'));
    assert.ok(isPreapprovedHost('DOCS.PYTHON.ORG', '/'));
  });
  it('rejects an unlisted host', () => {
    assert.equal(isPreapprovedHost('evil.example', '/'), false);
  });
  it('enforces path-segment boundaries for path-scoped entries', () => {
    assert.ok(isPreapprovedHost('github.com', '/anthropics'));
    assert.ok(isPreapprovedHost('github.com', '/anthropics/claude-code'));
    assert.equal(isPreapprovedHost('github.com', '/anthropics-evil/malware'), false);
    assert.equal(isPreapprovedHost('github.com', '/other'), false);
  });
});

describe('web_fetch preapproved raw-markdown passthrough', () => {
  it('returns verbatim content WITHOUT summarizing for a preapproved docs host', async () => {
    let summarizerCalled = false;
    const res = await webFetchTool.execute(
      { url: 'https://docs.python.org/3/library/os.html', prompt: 'summarize' },
      ctx({
        _providerConfig: {} as never,
        _webFetchHttpGet: mockGet('VERBATIM DOCS CONTENT'),
        _engineFactory: ((_cfg: unknown) => ({
          async *submit(_p: string) {
            summarizerCalled = true;
            yield { type: 'text_delta', text: 'LOSSY SUMMARY' } as never;
          },
          shutdown() {},
        })) as never,
      }),
    );
    assert.equal(res.isError, undefined);
    assert.equal(summarizerCalled, false, 'preapproved host must not run the summarizer');
    assert.match(res.output, /VERBATIM DOCS CONTENT/);
    assert.doesNotMatch(res.output, /LOSSY SUMMARY/);
  });

  it('still summarizes an unlisted host (regression lock on default behavior)', async () => {
    let summarizerCalled = false;
    const res = await webFetchTool.execute(
      { url: 'https://untrusted-u072-regression.example/a', prompt: 'summarize' },
      ctx({
        _providerConfig: {} as never,
        _webFetchHttpGet: mockGet('raw page'),
        _engineFactory: ((_cfg: unknown) => ({
          async *submit(_p: string) {
            summarizerCalled = true;
            yield { type: 'text_delta', text: 'SUMMARY OUT' } as never;
          },
          shutdown() {},
        })) as never,
      }),
    );
    assert.equal(res.isError, undefined);
    assert.equal(summarizerCalled, true, 'unlisted host must still summarize');
    assert.equal(res.output, 'SUMMARY OUT');
  });

  it('feeds PERMISSIVE guidelines for a preapproved host when content exceeds the raw ceiling', async () => {
    let seenPrompt = '';
    const big = 'x'.repeat(MAX_MARKDOWN_LENGTH + 10);
    const res = await webFetchTool.execute(
      { url: 'https://docs.python.org/huge', prompt: 'summarize' },
      ctx({
        _providerConfig: {} as never,
        _webFetchHttpGet: mockGet(big),
        _engineFactory: ((_cfg: unknown) => ({
          async *submit(p: string) {
            seenPrompt = p;
            yield { type: 'text_delta', text: 'ok' } as never;
          },
          shutdown() {},
        })) as never,
      }),
    );
    assert.equal(res.isError, undefined);
    assert.match(seenPrompt, /documentation excerpts/, 'preapproved host must get permissive guidelines');
    assert.doesNotMatch(seenPrompt, /125-character maximum/);
  });
});

// ---------------------------------------------------------------------------
// U072 — clearWebFetchCache evicts cached fetched pages (/clear parity)
// ---------------------------------------------------------------------------

describe('clearWebFetchCache', () => {
  it('evicts all cached entries', () => {
    const key = 'https://cache.example/x\u0000prompt\u00000';
    setCache(key, 'cached-body');
    assert.equal(getCached(key), 'cached-body');
    clearWebFetchCache();
    assert.equal(getCached(key), null);
  });
});
