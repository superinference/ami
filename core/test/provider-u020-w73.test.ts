import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  streamChatCompletionWithRetry,
  streamChatCompletion,
  shouldRetry529,
  FOREGROUND_529_RETRY_SOURCES,
  identifyGateway,
  type QuerySource,
} from '../src/provider';
import { SessionMemoryExtractor } from '../src/session-memory';
import type { ProviderConfig, Message, StreamChunk } from '../src/types';

// ---------------------------------------------------------------------------
// U020 / w73 — query-source tagging + foreground/background 529 overload retry
// gating, and gateway/proxy fingerprint identification (core/core/src/provider.ts).
//
// Covered parity gaps:
//  - querysource parameter tagging the origin of a model request
//  - querysource purpose tag per query options / per llm query
//  - foreground vs background 529 retry suppression / gating by query source
//  - querysource-gated 529 retry (foreground sources set)
//  - querysource tagging of background model calls (e.g. memory extraction)
//  - known gateway fingerprint identification (which proxy is in front)
// ---------------------------------------------------------------------------

const config: ProviderConfig = {
  baseUrl: 'http://127.0.0.1:1',
  apiKey: 'test-key',
  model: 'test-model',
};

const messages: Message[] = [{ role: 'user', content: 'hi' }];

async function collect(gen: AsyncGenerator<StreamChunk>): Promise<StreamChunk[]> {
  const out: StreamChunk[] = [];
  for await (const c of gen) out.push(c);
  return out;
}

/** A single-attempt stream that always yields the same error chunk. */
function erroringStream(error: string, responseHeaders?: Record<string, string>) {
  let attempts = 0;
  const fn = (async function* () {
    attempts++;
    yield { type: 'error', error, responseHeaders } as StreamChunk;
  }) as typeof streamChatCompletion;
  return { fn, getAttempts: () => attempts };
}

describe('shouldRetry529 (query-source overload gating)', () => {
  it('retries for an untagged (undefined) source — conservative default', () => {
    assert.equal(shouldRetry529(undefined), true);
  });

  it('retries for every foreground source', () => {
    for (const src of FOREGROUND_529_RETRY_SOURCES) {
      assert.equal(shouldRetry529(src), true, `foreground ${src} must retry 529`);
    }
  });

  it('suppresses the retry for known background sources', () => {
    for (const src of ['session_memory', 'title', 'insights', 'critic', 'skill_judge'] as QuerySource[]) {
      assert.equal(shouldRetry529(src), false, `background ${src} must bail on 529`);
      assert.equal(FOREGROUND_529_RETRY_SOURCES.has(src), false);
    }
  });

  it('includes the expected foreground sources', () => {
    for (const src of ['repl_main_thread', 'detached', 'sdk', 'agent', 'compact', 'side_question', 'auto_mode'] as QuerySource[]) {
      assert.ok(FOREGROUND_529_RETRY_SOURCES.has(src), `${src} should be foreground`);
    }
  });
});

describe('identifyGateway (proxy/gateway fingerprint)', () => {
  it('returns undefined for missing headers', () => {
    assert.equal(identifyGateway(undefined), undefined);
    assert.equal(identifyGateway({}), undefined);
  });

  it('identifies Cloudflare by cf-ray or server', () => {
    assert.equal(identifyGateway({ 'cf-ray': 'abc-DFW' }), 'cloudflare');
    assert.equal(identifyGateway({ Server: 'cloudflare' }), 'cloudflare');
  });

  it('identifies AWS by amazon headers', () => {
    assert.equal(identifyGateway({ 'x-amz-cf-id': 'z' }), 'aws');
    assert.equal(identifyGateway({ 'x-amzn-requestid': 'z' }), 'aws');
    assert.equal(identifyGateway({ server: 'awselb/2.0' }), 'aws');
  });

  it('identifies Envoy / nginx / gunicorn', () => {
    assert.equal(identifyGateway({ 'x-envoy-upstream-service-time': '12' }), 'envoy');
    assert.equal(identifyGateway({ server: 'nginx/1.25' }), 'nginx');
    assert.equal(identifyGateway({ server: 'gunicorn/21' }), 'gunicorn');
  });

  it('identifies a Google frontend and a generic Via proxy', () => {
    assert.equal(identifyGateway({ via: '1.1 google' }), 'google-frontend');
    assert.equal(identifyGateway({ Via: '1.1 squid-cache, 1.0 other' }), 'via:1.1');
  });

  it('is case-insensitive on header names', () => {
    assert.equal(identifyGateway({ 'CF-RAY': 'x' }), 'cloudflare');
  });
});

describe('streamChatCompletionWithRetry — 529 foreground/background gating', () => {
  it('BACKGROUND source bails immediately on a 529 overload (no retry)', async () => {
    const s = erroringStream('HTTP 529: overloaded', { 'cf-ray': 'r1' });
    let retried = false;
    const chunks = await collect(
      streamChatCompletionWithRetry(config, messages, [], new AbortController().signal, {
        maxRetries: 5, baseDelayMs: 1, maxDelayMs: 5,
        querySource: 'session_memory',
        streamImpl: s.fn,
        onRetry: () => { retried = true; },
      }),
    );
    // GREEN: background 529 is suppressed → exactly one attempt, no retry.
    // RED (revert the overloadSuppressed gate): 529 ∈ RETRYABLE_STATUS_CODES so it
    // retries 5× → attempts === 6, retried === true.
    assert.equal(s.getAttempts(), 1, `expected 1 attempt, got ${s.getAttempts()}`);
    assert.equal(retried, false);
    assert.equal(chunks[chunks.length - 1].type, 'error');
  });

  it('BACKGROUND source bails on a body-detected overloaded_error (status-less 529)', async () => {
    const s = erroringStream('{"type":"overloaded_error","message":"Overloaded"}');
    let retried = false;
    await collect(
      streamChatCompletionWithRetry(config, messages, [], new AbortController().signal, {
        maxRetries: 4, baseDelayMs: 1, maxDelayMs: 5,
        querySource: 'critic',
        streamImpl: s.fn,
        onRetry: () => { retried = true; },
      }),
    );
    assert.equal(s.getAttempts(), 1);
    assert.equal(retried, false);
  });

  it('FOREGROUND source still retries a 529 overload', async () => {
    const s = erroringStream('HTTP 529: overloaded');
    let retries = 0;
    await collect(
      streamChatCompletionWithRetry(config, messages, [], new AbortController().signal, {
        maxRetries: 3, baseDelayMs: 1, maxDelayMs: 3,
        querySource: 'repl_main_thread',
        streamImpl: s.fn,
        onRetry: () => { retries++; },
      }),
    );
    assert.equal(s.getAttempts(), 4, 'foreground should retry maxRetries times');
    assert.equal(retries, 3);
  });

  it('UNTAGGED source retries a 529 overload (behavior preserved for existing callers)', async () => {
    const s = erroringStream('HTTP 529: overloaded');
    let retries = 0;
    await collect(
      streamChatCompletionWithRetry(config, messages, [], new AbortController().signal, {
        maxRetries: 3, baseDelayMs: 1, maxDelayMs: 3,
        streamImpl: s.fn,
        onRetry: () => { retries++; },
      }),
    );
    assert.equal(s.getAttempts(), 4);
    assert.equal(retries, 3);
  });

  it('BACKGROUND source STILL retries a 429 (only 529 is gated)', async () => {
    const s = erroringStream('HTTP 429: rate limited');
    let retries = 0;
    await collect(
      streamChatCompletionWithRetry(config, messages, [], new AbortController().signal, {
        maxRetries: 2, baseDelayMs: 1, maxDelayMs: 3,
        querySource: 'session_memory',
        streamImpl: s.fn,
        onRetry: () => { retries++; },
      }),
    );
    assert.equal(s.getAttempts(), 3, '429 must retry regardless of query source');
    assert.equal(retries, 2);
  });

  it('x-should-retry:true overrides background 529 suppression', async () => {
    const s = erroringStream('HTTP 529: overloaded', { 'x-should-retry': 'true' });
    let retries = 0;
    await collect(
      streamChatCompletionWithRetry(config, messages, [], new AbortController().signal, {
        maxRetries: 2, baseDelayMs: 1, maxDelayMs: 3,
        querySource: 'session_memory',
        streamImpl: s.fn,
        onRetry: () => { retries++; },
      }),
    );
    assert.equal(s.getAttempts(), 3, 'explicit x-should-retry:true wins over suppression');
    assert.equal(retries, 2);
  });
});

describe('SessionMemoryExtractor tags background extraction with a query source', () => {
  it('passes querySource=session_memory to the stream function', async () => {
    const extractor = new SessionMemoryExtractor(config);
    let capturedOptions: { querySource?: QuerySource } | undefined;
    extractor._streamFn = (async function* (
      _c: ProviderConfig, _m: Message[], _t: unknown, _s: AbortSignal,
      opts?: { querySource?: QuerySource },
    ) {
      capturedOptions = opts;
      yield { type: 'content_delta', text: '' } as StreamChunk;
    }) as typeof streamChatCompletion;

    await extractor.extractFacts(
      [{ toolName: 'bash', output: 'ok', isError: false }],
      [{ role: 'user', content: 'remember x' }],
    );

    assert.equal(capturedOptions?.querySource, 'session_memory');
  });

  it('end-to-end: a 529 during extraction bails without retrying (returns no facts)', async () => {
    const extractor = new SessionMemoryExtractor(config);
    // Real retry wrapper, but the underlying single-attempt stream always 529s.
    const s = erroringStream('HTTP 529: overloaded');
    extractor._streamFn = ((c: ProviderConfig, m: Message[], t: never, sig: AbortSignal, opts?: object) =>
      streamChatCompletionWithRetry(c, m, t, sig, { ...opts, baseDelayMs: 1, maxDelayMs: 2, maxRetries: 5, streamImpl: s.fn })
    ) as unknown as typeof streamChatCompletion;

    const facts = await extractor.extractFacts(
      [{ toolName: 'bash', output: 'ok', isError: false }],
      [{ role: 'user', content: 'remember x' }],
    );

    assert.deepEqual(facts, []);
    // The background tag suppressed the 529 retry → a single underlying attempt.
    assert.equal(s.getAttempts(), 1);
  });
});
