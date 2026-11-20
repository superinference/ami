import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';

import {
  createIssueFallbackTitle,
  normalizeIssueTitle,
  generateIssueTitle,
  ISSUE_TITLE_MAX_LEN,
  ISSUE_TITLE_GENERATION_PROMPT,
  type AuxQueryResult,
} from '../src/title-generator';
import { startsWithApiErrorPrefix, API_ERROR_MESSAGE_PREFIX } from '../src/error-classifier';
import type { ProviderConfig } from '../src/types';

// ---------------------------------------------------------------------------
// SSE mock server (OpenAI-compatible streaming) — same shape as the sibling
// title-generator tests so generateIssueTitle is exercised end-to-end.
// ---------------------------------------------------------------------------

type SSEHandler = (body: Record<string, unknown>) => string[];

function createSSEServer(handler: SSEHandler): Promise<{ server: http.Server; port: number }> {
  return new Promise((resolve) => {
    const server = http.createServer(async (req, res) => {
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(chunk as Buffer);
      const body = JSON.parse(Buffer.concat(chunks).toString());
      const sseLines = handler(body);
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
      });
      for (const line of sseLines) res.write(line + '\n\n');
      res.end();
    });
    server.listen(0, '127.0.0.1', () => {
      const addr = server.address() as { port: number };
      resolve({ server, port: addr.port });
    });
  });
}

function textResponseSSE(text: string): string[] {
  const lines: string[] = [
    'data: {"id":"c","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant"},"index":0}]}',
    `data: {"id":"c","object":"chat.completion.chunk","choices":[{"delta":{"content":${JSON.stringify(text)}},"index":0}]}`,
    'data: {"id":"c","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}]}',
    'data: [DONE]',
  ];
  return lines;
}

function makeProvider(port: number, model = 'test-model'): ProviderConfig {
  return { baseUrl: `http://127.0.0.1:${port}/v1`, apiKey: 'test-key', model, maxTokens: 50 };
}

// ---------------------------------------------------------------------------
// createIssueFallbackTitle — VERBATIM parity with the reference's createFallbackTitle
// (components/Feedback.tsx:L475-499)
// ---------------------------------------------------------------------------
describe('createIssueFallbackTitle', () => {
  it('uses a short first line (6-60 chars) verbatim', () => {
    assert.equal(createIssueFallbackTitle('App crashes on startup'), 'App crashes on startup');
  });

  it('takes only the FIRST line of a multi-line description', () => {
    assert.equal(createIssueFallbackTitle('Crash on open\nmore detail here'), 'Crash on open');
  });

  it('truncates a long first line at a word boundary with a trailing ellipsis', () => {
    const desc = 'The application unexpectedly terminates whenever the user attempts to open a large project file';
    const t = createIssueFallbackTitle(desc);
    assert.ok(t.length <= 63, `expected <=63 got ${t.length}`);
    assert.ok(t.endsWith('...'), 'truncated title must end with ...');
    assert.ok(!t.slice(0, -3).endsWith(' '), 'no trailing space before ellipsis');
    // Broke on a word boundary — the last whole word is preserved intact.
    assert.ok(desc.startsWith(t.slice(0, -3)));
  });

  it('hard-cuts (no word break) when the only boundary is before 30 chars', () => {
    // A 61-char first "word" with a single space at index 5 → lastSpace(5) not > 30.
    const desc = 'short ' + 'x'.repeat(70);
    const t = createIssueFallbackTitle(desc);
    assert.ok(t.endsWith('...'));
    assert.equal(t.slice(0, -3).length, 60);
  });

  it('collapses a too-short result to the literal "Bug Report"', () => {
    assert.equal(createIssueFallbackTitle('boom'), 'Bug Report');
    assert.equal(createIssueFallbackTitle('hi'), 'Bug Report');
  });

  it('returns "Bug Report" for an empty description', () => {
    assert.equal(createIssueFallbackTitle(''), 'Bug Report');
  });

  it('uses a boundary first line of exactly 60 chars verbatim', () => {
    const line = 'a'.repeat(60);
    assert.equal(createIssueFallbackTitle(line), line);
  });
});

// ---------------------------------------------------------------------------
// startsWithApiErrorPrefix — parity with the reference's errors.ts:56-60
// ---------------------------------------------------------------------------
describe('startsWithApiErrorPrefix', () => {
  it('matches the bare API Error prefix', () => {
    assert.equal(startsWithApiErrorPrefix(`${API_ERROR_MESSAGE_PREFIX}: overloaded`), true);
  });
  it('does not match a real title', () => {
    assert.equal(startsWithApiErrorPrefix('[Bug] App crashes on startup'), false);
  });
});

// ---------------------------------------------------------------------------
// normalizeIssueTitle — accept a real title, reject noise/API-error
// ---------------------------------------------------------------------------
describe('normalizeIssueTitle', () => {
  it('accepts a clean type-prefixed title', () => {
    assert.equal(normalizeIssueTitle('[Bug] Auto-compact triggers too soon'), '[Bug] Auto-compact triggers too soon');
  });
  it('strips wrapping quotes and <think> reasoning', () => {
    assert.equal(
      normalizeIssueTitle('<think>let me decide</think>"[Bug] Missing tool result block"'),
      '[Bug] Missing tool result block',
    );
  });
  it('rejects an API-error message (falls back via empty string)', () => {
    assert.equal(normalizeIssueTitle(`${API_ERROR_MESSAGE_PREFIX}: 529 overloaded`), '');
  });
  it('rejects empty / pure-punctuation noise', () => {
    assert.equal(normalizeIssueTitle(''), '');
    assert.equal(normalizeIssueTitle('   '), '');
    assert.equal(normalizeIssueTitle('***'), '');
  });
  it('caps an over-long title at ISSUE_TITLE_MAX_LEN', () => {
    const long = '[Bug] ' + 'word '.repeat(40);
    const t = normalizeIssueTitle(long);
    assert.ok(t.length <= ISSUE_TITLE_MAX_LEN, `expected <=${ISSUE_TITLE_MAX_LEN} got ${t.length}`);
  });
  it('prompt requires the [Bug]/[Feature Request] prefix', () => {
    assert.ok(ISSUE_TITLE_GENERATION_PROMPT.includes('[Bug]'));
    assert.ok(ISSUE_TITLE_GENERATION_PROMPT.includes('[Feature Request]'));
  });
});

// ---------------------------------------------------------------------------
// generateIssueTitle — end-to-end through a real stream + fallback paths
// ---------------------------------------------------------------------------
describe('generateIssueTitle — model success', () => {
  let server: http.Server;
  let port: number;
  before(async () => {
    const mock = await createSSEServer(() => textResponseSSE('[Bug] App crashes opening large file'));
    server = mock.server; port = mock.port;
  });
  after(() => { server.close(); });

  it('returns the model-generated type-prefixed title', async () => {
    const t = await generateIssueTitle('The app crashes when I open a large file', makeProvider(port));
    assert.equal(t, '[Bug] App crashes opening large file');
  });

  it('reports a structured success record tagged issue_title', async () => {
    const records: AuxQueryResult[] = [];
    await generateIssueTitle('app crashes', makeProvider(port), undefined, undefined, undefined, (r) => records.push(r));
    assert.equal(records.length, 1);
    assert.equal(records[0]!.source, 'issue_title');
    assert.equal(records[0]!.type, 'success');
  });
});

describe('generateIssueTitle — API-error output falls back deterministically', () => {
  let server: http.Server;
  let port: number;
  before(async () => {
    const mock = await createSSEServer(() => textResponseSSE(`${API_ERROR_MESSAGE_PREFIX}: 529 overloaded`));
    server = mock.server; port = mock.port;
  });
  after(() => { server.close(); });

  it('falls back to createIssueFallbackTitle when the model emits an API error', async () => {
    const desc = 'App crashes on startup every time';
    const t = await generateIssueTitle(desc, makeProvider(port));
    assert.equal(t, createIssueFallbackTitle(desc));
  });

  it('reports an unparseable error record for the API-error output', async () => {
    const records: AuxQueryResult[] = [];
    await generateIssueTitle('App crashes on startup every time', makeProvider(port), undefined, undefined, undefined, (r) => records.push(r));
    assert.equal(records[0]!.type, 'error');
    if (records[0]!.type === 'error') assert.equal(records[0]!.reason, 'unparseable');
  });
});

describe('generateIssueTitle — short-circuits + transport failure', () => {
  it('returns the deterministic fallback for an empty description with NO model call', async () => {
    let called = false;
    const fakeStream = (() => { called = true; throw new Error('should not be called'); }) as never;
    const t = await generateIssueTitle('   ', makeProvider(1), undefined, fakeStream);
    assert.equal(t, 'Bug Report');
    assert.equal(called, false);
  });

  it('falls back on a transport/connection failure (unreachable provider)', async () => {
    const desc = 'Something went wrong while syncing the workspace index';
    // Port 1 is not listening → connection refused → outer failure → fallback.
    const t = await generateIssueTitle(desc, makeProvider(1));
    assert.equal(t, createIssueFallbackTitle(desc));
  });

  it('short-circuits to the fallback when the caller signal is already aborted', async () => {
    const desc = 'App crashes on startup every single time it boots';
    const ac = new AbortController();
    ac.abort();
    const t = await generateIssueTitle(desc, makeProvider(1), undefined, undefined, ac.signal);
    assert.equal(t, createIssueFallbackTitle(desc));
  });
});
