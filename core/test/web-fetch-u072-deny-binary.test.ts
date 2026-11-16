/**
 * U072 — web_fetch deny-by-default binary content-type detection + searchHint.
 *
 * Parity gaps implemented here (additive):
 *  1. Deny-by-default binary detection (isBinaryContentType): content is treated
 *     as BINARY and saved to disk unless it is provably text (text/*, +json,
 *     +xml, application/javascript, form-urlencoded). The old allow-list only
 *     caught image/*, zip, octet-stream, so an unknown binary type (font/woff2,
 *     video/mp4, application/wasm, …) LEAKED raw bytes into the model context.
 *     Reference: claude-code-main/src/utils/mcpOutputStorage.ts:L120-L136.
 *  2. searchHint tool-def field so ToolSearch can find web_fetch by intent
 *     synonyms that are not in its name/description.
 *     Reference: claude-code-main/src/tools/WebFetchTool/WebFetchTool.ts:L68.
 *
 * Everything drives the REAL tools via seams; no socket is opened.
 */
import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as nodePath from 'path';
import { webFetchTool, isBinaryContentType } from '../src/tools/web-fetch';
import { toolSearchTool, setSearchableTools } from '../src/tools/tool-search';
import type { ToolContext } from '../src/types';

type HttpGetResult = {
  body: string;
  bodyBuffer?: Buffer;
  statusCode: number;
  contentType: string;
  finalUrl: string;
  truncated?: boolean;
};

function mockGet(resp: Partial<HttpGetResult>) {
  return async (url: string): Promise<HttpGetResult> => ({
    body: '',
    statusCode: 200,
    contentType: 'text/plain',
    finalUrl: url,
    ...resp,
  });
}

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

let saved: Array<[string, string | undefined]>;
before(() => {
  saved = [
    ['CLAUDE_CODE_DISABLE_WEB_FETCH', process.env.CLAUDE_CODE_DISABLE_WEB_FETCH],
    ['SUPERINFERENCE_DISABLE_WEB_FETCH', process.env.SUPERINFERENCE_DISABLE_WEB_FETCH],
    ['SUPERINFERENCE_DATA_DIR', process.env.SUPERINFERENCE_DATA_DIR],
  ];
  delete process.env.CLAUDE_CODE_DISABLE_WEB_FETCH;
  delete process.env.SUPERINFERENCE_DISABLE_WEB_FETCH;
});
after(() => {
  for (const [k, v] of saved) {
    if (v === undefined) delete process.env[k]; else process.env[k] = v;
  }
});

// ---------------------------------------------------------------------------
// isBinaryContentType — deny-by-default
// ---------------------------------------------------------------------------
describe('isBinaryContentType — deny-by-default', () => {
  it('treats provably-text types as NON-binary', () => {
    assert.equal(isBinaryContentType('text/plain'), false);
    assert.equal(isBinaryContentType('text/html; charset=utf-8'), false);
    assert.equal(isBinaryContentType('text/css'), false);
    assert.equal(isBinaryContentType('application/json'), false);
    assert.equal(isBinaryContentType('application/ld+json'), false);
    assert.equal(isBinaryContentType('application/xml'), false);
    assert.equal(isBinaryContentType('image/svg+xml'), false);
    assert.equal(isBinaryContentType('application/javascript; charset=utf-8'), false);
    assert.equal(isBinaryContentType('application/x-www-form-urlencoded'), false);
  });

  it('treats everything else (including UNKNOWN types) as binary', () => {
    assert.equal(isBinaryContentType('font/woff2'), true);
    assert.equal(isBinaryContentType('video/mp4'), true);
    assert.equal(isBinaryContentType('audio/mpeg'), true);
    assert.equal(isBinaryContentType('application/wasm'), true);
    assert.equal(isBinaryContentType('application/x-tar'), true);
    assert.equal(isBinaryContentType('application/octet-stream'), true);
    assert.equal(isBinaryContentType('image/png'), true);
    assert.equal(isBinaryContentType('application/zip'), true);
  });

  it('treats a missing/empty content-type as NON-binary (text fallback)', () => {
    assert.equal(isBinaryContentType(''), false);
    assert.equal(isBinaryContentType(undefined), false);
  });
});

// ---------------------------------------------------------------------------
// E2E: an unknown binary type is saved to disk, not leaked into context.
// RED on revert (old image/zip/octet allow-list): font/woff2 falls through to
// the text path and the raw bytes are returned as the output body.
// ---------------------------------------------------------------------------
describe('web_fetch — unknown binary type is persisted, not leaked', () => {
  it('saves a font/woff2 response to disk instead of returning its bytes', async () => {
    const tmp = fs.mkdtempSync(nodePath.join(os.tmpdir(), 'u072-deny-'));
    process.env.SUPERINFERENCE_DATA_DIR = tmp;
    try {
      const marker = 'SECRET_BINARY_PAYLOAD_SHOULD_NOT_LEAK';
      const bytes = Buffer.from(marker + '\u0000ÿ', 'binary');
      const r = await webFetchTool.execute(
        { url: 'https://cdn.example/font.woff2', prompt: 'read it' },
        ctx({
          cwd: tmp,
          _allowLocalhostForTesting: true,
          _webFetchHttpGet: mockGet({
            body: bytes.toString('binary'),
            bodyBuffer: bytes,
            contentType: 'font/woff2',
          }),
        }),
      );
      assert.equal(r.isError, undefined);
      assert.match(r.output, /Binary content \(font\/woff2, .*saved to /);
      assert.ok(!r.output.includes(marker), 'raw binary bytes must NOT leak into the model context');
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('still returns an unknown TEXT type (application/xml) inline', async () => {
    const r = await webFetchTool.execute(
      { url: 'https://example.com/feed.xml', prompt: 'read it' },
      ctx({
        _allowLocalhostForTesting: true,
        _webFetchHttpGet: mockGet({ body: '<rss>hello</rss>', contentType: 'application/xml' }),
      }),
    );
    assert.equal(r.isError, undefined);
    assert.match(r.output, /<rss>hello<\/rss>/);
  });
});

// ---------------------------------------------------------------------------
// searchHint — web_fetch is findable by an intent synonym not in its text.
// RED on revert (no searchHint): "scrape" matches nothing → not-found message.
// ---------------------------------------------------------------------------
describe('web_fetch — searchHint enables intent discovery', () => {
  it('declares a non-empty searchHint with synonyms absent from name/description', () => {
    const hint = webFetchTool.searchHint ?? '';
    assert.ok(hint.length > 0, 'web_fetch must declare a searchHint');
    const haystack = `${webFetchTool.name} ${webFetchTool.description}`.toLowerCase();
    assert.ok(!haystack.includes('scrape'), 'test synonym must be unique to searchHint');
    assert.match(hint.toLowerCase(), /scrape/);
  });

  it('tool_search finds web_fetch via a searchHint-only synonym', async () => {
    setSearchableTools([webFetchTool]);
    const res = await toolSearchTool.execute({ query: 'scrape' }, ctx());
    assert.equal(res.isError, false);
    assert.match(res.output, /web_fetch/);
    assert.doesNotMatch(res.output, /No tools or skills matched/);
  });
});
