// ===========================================================================
// provider-cov.test.ts
//
// Behavioral branch/line/function coverage for the "provider" group:
//   - src/provider.ts            (retry/catch/sleep-abort/stream-catch paths)
//   - src/provider-coordinator.ts (health, credential pool, compaction fallback)
//   - src/cost-tracker.ts        (live-pricing fetch branches, cache diagnostics)
//   - src/rate-limiter.ts        (parseDuration units + getStatus branches)
//   - src/model-registry.ts      (diagnostics, listGemini, httpGetJSON errors)
//
// ESM-coverage-safe (runs under `node --import tsx` AND scripts/coverage-esm.mjs):
//   - No source-text assertions (no readFileSync of any .ts).
//   - The only imported-namespace mock is globalThis.fetch via node:test mock,
//     always restored. All other seams are real: http servers on port 0, temp
//     env vars, and the behavior-preserving streamImpl / env-timeout seams in
//     source (default behavior unchanged).
//   - Fixtures/servers created in-process.
// ===========================================================================
import { describe, it, before, after, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';
import type { AddressInfo } from 'net';

import {
  streamChatCompletion,
  streamChatCompletionWithRetry,
  resolveModel,
  resolveAvailableModel,
  inferProviderFromBaseUrl,
} from '../src/provider';
import { ProviderCoordinator } from '../src/provider-coordinator';
import type { ProviderConfig, ProviderSubsystem, Message, StreamChunk } from '../src/types';
import {
  CostTracker,
  categorizeTool,
  CacheMissDiagnostics,
  _resetPricingCache,
  _fetchPricing,
} from '../src/cost-tracker';
import { RateLimitTracker } from '../src/rate-limiter';
import {
  detectProvider,
  listProviders,
  listModels,
  listModelsDiagnostic,
  diagnoseModelDiscovery,
  validateModel,
  formatModelList,
} from '../src/model-registry';

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------
function portOf(server: http.Server): number {
  return (server.address() as AddressInfo).port;
}
function listen(server: http.Server): Promise<void> {
  return new Promise((r) => server.listen(0, '127.0.0.1', () => r()));
}
async function collect(gen: AsyncGenerator<StreamChunk>): Promise<StreamChunk[]> {
  const out: StreamChunk[] = [];
  for await (const c of gen) out.push(c);
  return out;
}

// An injected single-attempt stream (the streamImpl seam). Yields the given
// chunks, then optionally throws the given error.
function fakeStream(opts: {
  chunks?: StreamChunk[];
  throwErr?: unknown;
  beforeThrowMs?: number;
}): typeof streamChatCompletion {
  const impl = (async function* () {
    for (const c of opts.chunks ?? []) yield c;
    if (opts.throwErr !== undefined) {
      if (opts.beforeThrowMs) {
        await new Promise((r) => setTimeout(r, opts.beforeThrowMs));
      }
      throw opts.throwErr;
    }
  }) as unknown as typeof streamChatCompletion;
  return impl;
}

const NO_MSG: Message[] = [{ role: 'user', content: 'hi' }];
const OPENAI_CFG: ProviderConfig = {
  provider: 'openai',
  apiKey: 'sk-test1234567890',
  baseUrl: 'http://127.0.0.1:1/v1',
  model: 'gpt-4o',
};

// ===========================================================================
// provider.ts — streamChatCompletionWithRetry catch + retry branches (seam)
// ===========================================================================
describe('streamChatCompletionWithRetry — catch/retry branches (streamImpl seam)', () => {
  it('SSL error thrown by the stream is non-transient and rejects with an actionable hint', async () => {
    const sslErr = Object.assign(new Error('bad cert'), { code: 'DEPTH_ZERO_SELF_SIGNED_CERT' });
    await assert.rejects(
      () => collect(streamChatCompletionWithRetry(OPENAI_CFG, NO_MSG, [], new AbortController().signal, {
        maxRetries: 2, baseDelayMs: 1, maxDelayMs: 2,
        streamImpl: fakeStream({ throwErr: sslErr }),
      })),
      /SSL\/TLS certificate error: bad cert/,
    );
  });

  it('bypass: a plain error that is NOT ssl/network/abort is re-thrown unchanged', async () => {
    await assert.rejects(
      () => collect(streamChatCompletionWithRetry(OPENAI_CFG, NO_MSG, [], new AbortController().signal, {
        maxRetries: 2, baseDelayMs: 1, maxDelayMs: 2,
        streamImpl: fakeStream({ throwErr: new Error('totally unrelated failure') }),
      })),
      /totally unrelated failure/,
    );
  });

  it('network error with no content retries, then succeeds on a later attempt', async () => {
    let attempt = 0;
    const impl = (async function* () {
      if (attempt++ === 0) {
        throw Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' });
      }
      yield { type: 'content_delta', text: 'recovered' } as StreamChunk;
      yield { type: 'done', finishReason: 'stop' } as StreamChunk;
    }) as unknown as typeof streamChatCompletion;

    const retries: number[] = [];
    const chunks = await collect(streamChatCompletionWithRetry(OPENAI_CFG, NO_MSG, [], new AbortController().signal, {
      maxRetries: 3, baseDelayMs: 1, maxDelayMs: 2, onRetry: (a) => retries.push(a), streamImpl: impl,
    }));
    const text = chunks.filter((c) => c.type === 'content_delta').map((c) => c.text).join('');
    assert.equal(text, 'recovered');
    assert.ok(retries.length >= 1, 'onRetry fires for the network retry');
    assert.equal(attempt, 2);
  });

  it('network error exhausts the maxTotalWait budget and reports it', async () => {
    const netErr = Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' });
    const chunks = await collect(streamChatCompletionWithRetry(OPENAI_CFG, NO_MSG, [], new AbortController().signal, {
      maxRetries: 5, baseDelayMs: 1, maxDelayMs: 2, maxTotalWaitMs: 1,
      streamImpl: fakeStream({ throwErr: netErr }),
    }));
    const err = chunks.find((c) => c.type === 'error');
    assert.ok(err && /retry time limit exceeded/i.test(err.error!), `got ${err?.error}`);
  });

  it('a non-Error thrown value (string) is stringified and re-thrown', async () => {
    await assert.rejects(
      () => collect(streamChatCompletionWithRetry(OPENAI_CFG, NO_MSG, [], new AbortController().signal, {
        maxRetries: 1, baseDelayMs: 1, maxDelayMs: 2,
        streamImpl: fakeStream({ throwErr: 'a bare string failure' }),
      })),
    );
  });

  it('a non-Error SSL-coded object still triggers the SSL guard (stringified message)', async () => {
    await assert.rejects(
      () => collect(streamChatCompletionWithRetry(OPENAI_CFG, NO_MSG, [], new AbortController().signal, {
        maxRetries: 1, baseDelayMs: 1, maxDelayMs: 2,
        streamImpl: fakeStream({ throwErr: { code: 'CERT_HAS_EXPIRED' } }),
      })),
      /SSL\/TLS certificate error/,
    );
  });

  it('an error chunk with no error string is handled without a status code', async () => {
    const chunks = await collect(streamChatCompletionWithRetry(OPENAI_CFG, NO_MSG, [], new AbortController().signal, {
      maxRetries: 1, baseDelayMs: 1, maxDelayMs: 2,
      streamImpl: fakeStream({ chunks: [{ type: 'error' } as StreamChunk] }),
    }));
    assert.equal(chunks.length, 1);
    assert.equal(chunks[0].type, 'error');
  });

  it('abort-flavoured throw with no content is treated as a timeout and retried to exhaustion', async () => {
    const abortErr = new Error('The operation was aborted');
    const chunks = await collect(streamChatCompletionWithRetry(OPENAI_CFG, NO_MSG, [], new AbortController().signal, {
      maxRetries: 1, baseDelayMs: 1, maxDelayMs: 2,
      streamImpl: fakeStream({ throwErr: abortErr }),
    }));
    const err = chunks.find((c) => c.type === 'error');
    assert.ok(err && /timed out/i.test(err.error!), `got ${err?.error}`);
  });

  it('abort-flavoured throw AFTER content yields a partial-content timeout error', async () => {
    const abortErr = new Error('aborted mid-flight');
    const chunks = await collect(streamChatCompletionWithRetry(OPENAI_CFG, NO_MSG, [], new AbortController().signal, {
      maxRetries: 2, baseDelayMs: 1, maxDelayMs: 2,
      streamImpl: fakeStream({ chunks: [{ type: 'content_delta', text: 'partial' }], throwErr: abortErr }),
    }));
    const err = chunks.find((c) => c.type === 'error');
    assert.ok(err && /partial content received/i.test(err.error!), `got ${err?.error}`);
  });

  it('requestAbort firing (idle) while the stream then throws a non-abort error is a timeout', async () => {
    // idle timer aborts requestAbort at ~10ms; the fake stream throws a plain
    // (non-abort) error at ~40ms -> requestAbort.signal.aborted branch.
    const chunks = await collect(streamChatCompletionWithRetry(OPENAI_CFG, NO_MSG, [], new AbortController().signal, {
      idleTimeoutMs: 10, maxRetries: 0, baseDelayMs: 1, maxDelayMs: 2,
      streamImpl: fakeStream({ throwErr: new Error('late boom'), beforeThrowMs: 40 }),
    }));
    const err = chunks.find((c) => c.type === 'error');
    assert.ok(err && /timed out/i.test(err.error!), `got ${err?.error}`);
  });

  it('timeout with no retries left while maxTotalWait is already blown reports the retry limit', async () => {
    // First attempt: abort-flavoured throw -> timedOut, attempt<maxRetries, but
    // maxTotalWait already exceeded -> "Retry time limit exceeded".
    const chunks = await collect(streamChatCompletionWithRetry(OPENAI_CFG, NO_MSG, [], new AbortController().signal, {
      maxRetries: 3, baseDelayMs: 1, maxDelayMs: 2, maxTotalWaitMs: 1,
      streamImpl: fakeStream({ throwErr: new Error('operation was aborted') }),
    }));
    const err = chunks.find((c) => c.type === 'error');
    assert.ok(err && /retry time limit exceeded/i.test(err.error!), `got ${err?.error}`);
  });
});

// ===========================================================================
// provider.ts — sleep()'s abort path during a retry backoff
// ===========================================================================
describe('streamChatCompletionWithRetry — abort during backoff sleep', () => {
  it('aborting the caller signal mid-backoff rejects the sleep and yields Request aborted', async () => {
    const ac = new AbortController();
    // Stream yields a retryable 429 error (no content) -> enters sleep(baseDelay).
    const impl = fakeStream({ chunks: [{ type: 'error', error: 'HTTP 429: slow down' }] });
    // Abort the caller during the (long) backoff.
    setTimeout(() => ac.abort(), 20);
    const chunks = await collect(streamChatCompletionWithRetry(OPENAI_CFG, NO_MSG, [], ac.signal, {
      maxRetries: 3, baseDelayMs: 500, maxDelayMs: 1000, streamImpl: impl,
    }));
    const err = chunks.find((c) => c.type === 'error');
    assert.ok(err && /aborted/i.test(err.error!), `got ${err?.error}`);
  });

  it('a signal already aborted when the backoff sleep begins rejects immediately', async () => {
    // streamImpl aborts the caller signal (after the loop-top guard) then yields
    // a retryable 429 -> sleep() is entered with signal.aborted already true,
    // taking its early-abort reject path.
    const ac = new AbortController();
    const impl = (async function* () {
      ac.abort();
      yield { type: 'error', error: 'HTTP 429: slow down' } as StreamChunk;
    }) as unknown as typeof streamChatCompletion;
    const chunks = await collect(streamChatCompletionWithRetry(OPENAI_CFG, NO_MSG, [], ac.signal, {
      maxRetries: 3, baseDelayMs: 50, maxDelayMs: 100, streamImpl: impl,
    }));
    const err = chunks.find((c) => c.type === 'error');
    assert.ok(err && /aborted/i.test(err.error!), `got ${err?.error}`);
  });

  it('a stream that simply ends after content (no done) returns once content was committed', async () => {
    const chunks = await collect(streamChatCompletionWithRetry(OPENAI_CFG, NO_MSG, [], new AbortController().signal, {
      maxRetries: 2, baseDelayMs: 1, maxDelayMs: 2,
      streamImpl: fakeStream({ chunks: [{ type: 'content_delta', text: 'done-ish' }] }),
    }));
    assert.deepEqual(chunks, [{ type: 'content_delta', text: 'done-ish' }]);
  });

  it('idle abort while the stream ends gracefully (no throw) is detected as a timeout', async () => {
    // idle fires requestAbort at ~10ms; the stream returns normally at ~40ms
    // with no throw and no done chunk -> the post-finally requestAbort.aborted
    // check flags timedOut.
    const impl = (async function* () {
      await new Promise((r) => setTimeout(r, 40));
    }) as unknown as typeof streamChatCompletion;
    const chunks = await collect(streamChatCompletionWithRetry(OPENAI_CFG, NO_MSG, [], new AbortController().signal, {
      idleTimeoutMs: 10, maxRetries: 0, baseDelayMs: 1, maxDelayMs: 2, streamImpl: impl,
    }));
    const err = chunks.find((c) => c.type === 'error');
    assert.ok(err && /timed out/i.test(err.error!), `got ${err?.error}`);
  });

  it('aborting the caller during the network-retry backoff yields Request aborted', async () => {
    const ac = new AbortController();
    const netErr = Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' });
    setTimeout(() => ac.abort(), 20);
    const chunks = await collect(streamChatCompletionWithRetry(OPENAI_CFG, NO_MSG, [], ac.signal, {
      maxRetries: 3, baseDelayMs: 500, maxDelayMs: 1000,
      streamImpl: fakeStream({ throwErr: netErr }),
    }));
    const err = chunks.find((c) => c.type === 'error');
    assert.ok(err && /aborted/i.test(err.error!), `got ${err?.error}`);
  });

  it('a graceful done (no finishReason) after an idle abort is treated as a timeout', async () => {
    const impl = (async function* () {
      await new Promise((r) => setTimeout(r, 40));
      yield { type: 'done' } as StreamChunk; // no finishReason
    }) as unknown as typeof streamChatCompletion;
    const chunks = await collect(streamChatCompletionWithRetry(OPENAI_CFG, NO_MSG, [], new AbortController().signal, {
      idleTimeoutMs: 10, maxRetries: 0, baseDelayMs: 1, maxDelayMs: 2, streamImpl: impl,
    }));
    const err = chunks.find((c) => c.type === 'error');
    assert.ok(err && /timed out/i.test(err.error!), `got ${err?.error}`);
  });

  it('an error chunk that arrives after content commits the stream and returns', async () => {
    const chunks = await collect(streamChatCompletionWithRetry(OPENAI_CFG, NO_MSG, [], new AbortController().signal, {
      maxRetries: 3, baseDelayMs: 1, maxDelayMs: 2,
      streamImpl: fakeStream({ chunks: [{ type: 'content_delta', text: 'hi' }, { type: 'error', error: 'mid-stream glitch' }] }),
    }));
    assert.equal(chunks.length, 2);
    assert.equal(chunks[0].type, 'content_delta');
    assert.equal(chunks[1].type, 'error');
  });

  it('aborting the caller during the timeout backoff yields Request aborted', async () => {
    const ac = new AbortController();
    setTimeout(() => ac.abort(), 20);
    const chunks = await collect(streamChatCompletionWithRetry(OPENAI_CFG, NO_MSG, [], ac.signal, {
      maxRetries: 3, baseDelayMs: 500, maxDelayMs: 1000,
      streamImpl: fakeStream({ throwErr: new Error('operation was aborted') }),
    }));
    const err = chunks.find((c) => c.type === 'error');
    assert.ok(err && /aborted/i.test(err.error!), `got ${err?.error}`);
  });

  it('a pre-aborted caller signal short-circuits immediately', async () => {
    const ac = new AbortController();
    ac.abort();
    const chunks = await collect(streamChatCompletionWithRetry(OPENAI_CFG, NO_MSG, [], ac.signal, {
      maxRetries: 1, baseDelayMs: 1,
      streamImpl: fakeStream({ chunks: [{ type: 'content_delta', text: 'never' }] }),
    }));
    assert.equal(chunks.length, 1);
    assert.equal(chunks[0].type, 'error');
    assert.match(chunks[0].error!, /aborted/i);
  });
});

// ===========================================================================
// provider.ts — streamChatCompletion's own try/catch (real transport throws)
// ===========================================================================
describe('streamChatCompletion — generator catch path (real transport)', () => {
  it('a refused connection surfaces as an error chunk from the catch (no status code)', async () => {
    // Bind then immediately close a server to get a definitely-closed port.
    const tmp = http.createServer();
    await listen(tmp);
    const port = portOf(tmp);
    await new Promise<void>((r) => tmp.close(() => r()));
    const config: ProviderConfig = { ...OPENAI_CFG, baseUrl: `http://127.0.0.1:${port}/v1` };
    const chunks = await collect(streamChatCompletion(config, NO_MSG, [], new AbortController().signal));
    const err = chunks.find((c) => c.type === 'error');
    assert.ok(err, 'expected an error chunk');
    assert.ok(typeof err!.error === 'string' && err!.error!.length > 0);
  });

  it('anthropic cache breakpoints skip empty-string and text-less messages', async () => {
    // Vertex env would reroute claude models through the vertex SDK; strip it.
    const VE = ['CLAUDE_CODE_USE_VERTEX', 'ANTHROPIC_VERTEX_PROJECT_ID', 'GOOGLE_VERTEX_PROJECT', 'CLOUD_ML_REGION', 'GOOGLE_VERTEX_LOCATION'];
    const saved: Record<string, string | undefined> = {};
    for (const k of VE) { saved[k] = process.env[k]; delete process.env[k]; }
    const tmp = http.createServer();
    await listen(tmp);
    const port = portOf(tmp);
    await new Promise<void>((r) => tmp.close(() => r()));
    try {
      const config: ProviderConfig = { provider: 'anthropic', apiKey: 'sk-ant-xxxxxxxxxx', baseUrl: `http://127.0.0.1:${port}`, model: 'claude-sonnet-4-6' };
      // last two non-tool messages become cache breakpoints (index 0 also via
      // the system prompt): index 0 = empty-string user, index 1 = image-only
      // user (no text part). Both must hit the `continue` arms of the loop.
      const messages: Message[] = [
        { role: 'system', content: 'You are a test.' },
        { role: 'user', content: '' },
        { role: 'user', content: [{ type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'AAAA' } }] },
      ];
      const chunks = await collect(streamChatCompletion(config, messages, [], new AbortController().signal));
      // The cache-breakpoint loop runs before the (failing) network call; we only
      // require that the generator completed and surfaced an error chunk.
      assert.ok(chunks.some((c) => c.type === 'error'));
    } finally {
      for (const [k, v] of Object.entries(saved)) { if (v === undefined) delete process.env[k]; else process.env[k] = v; }
    }
  });

  it('a socket reset mid-successful-stream is caught and reported with its status code', async () => {
    const server = http.createServer((req, res) => {
      res.writeHead(200, { 'Content-Type': 'text/event-stream' });
      res.write('data: {"id":"c","object":"chat.completion.chunk","choices":[{"delta":{"content":"hi"},"index":0}]}\n\n');
      setTimeout(() => res.socket?.destroy(), 25);
    });
    await listen(server);
    const config: ProviderConfig = { ...OPENAI_CFG, baseUrl: `http://127.0.0.1:${portOf(server)}/v1` };
    const chunks = await collect(streamChatCompletion(config, NO_MSG, [], new AbortController().signal));
    await new Promise<void>((r) => server.close(() => r()));
    // Content started, then the SDK threw while finishing -> caught at the
    // generator's try/catch and emitted as an error chunk.
    assert.ok(chunks.some((c) => c.type === 'content_delta'));
    assert.ok(chunks.some((c) => c.type === 'error'));
  });
});

// ===========================================================================
// provider.ts — stream-part switch + generator catch (streamTextImpl seam)
// ===========================================================================
describe('streamChatCompletion — stream parts and catch (streamTextImpl seam)', () => {
  const CFG: ProviderConfig = { provider: 'openai', apiKey: 'sk-x', baseUrl: 'https://api.openai.com/v1', model: 'gpt-4o' };

  // Build a fake streamText: yields the given parts, optionally invokes onError
  // callbacks, and optionally throws after the parts are exhausted.
  interface FakeOpts {
    parts?: Array<Record<string, unknown>>;
    onErrors?: unknown[];
    throwErr?: unknown;
  }
  function fakeStreamText(opts: FakeOpts): unknown {
    return (cfg: { onError?: (e: { error: unknown }) => void }) => {
      for (const e of opts.onErrors ?? []) cfg.onError?.({ error: e });
      return {
        stream: (async function* () {
          for (const p of opts.parts ?? []) yield p;
          if (opts.throwErr !== undefined) throw opts.throwErr;
        })(),
      };
    };
  }
  const run = (opts: FakeOpts, signal?: AbortSignal) =>
    collect(streamChatCompletion(CFG, NO_MSG, [], signal ?? new AbortController().signal, {
      streamTextImpl: fakeStreamText(opts) as never,
    }));

  it('maps text, reasoning (with and without text), tool-call, unknown and finish-step parts', async () => {
    const chunks = await run({ parts: [
      { type: 'text-delta', text: 'hello ' },
      { type: 'reasoning-delta', text: 'pondering' },
      { type: 'reasoning-delta' }, // no text -> ?? ''
      { type: 'tool-call', toolCallId: 'c1', toolName: 'bash', input: { cmd: 'ls' } },
      { type: 'source', url: 'ignored' }, // default -> ignored
      { type: 'finish-step', finishReason: 'stop', usage: { inputTokens: 3, outputTokens: 5, totalTokens: 8, cachedInputTokens: 2 }, response: { headers: { 'x-req': '1' } } },
    ] });
    const types = chunks.map((c) => c.type);
    assert.deepEqual(types, ['content_delta', 'thinking_delta', 'thinking_delta', 'tool_call_delta', 'done']);
    const done = chunks.find((c) => c.type === 'done')!;
    assert.equal(done.finishReason, 'stop');
    assert.equal(done.usage!.promptTokens, 3);
    assert.equal(done.usage!.cachedPromptTokens, 2);
  });

  it('computes totalTokens when the usage omits it', async () => {
    const chunks = await run({ parts: [
      { type: 'finish-step', finishReason: 'stop', usage: { inputTokens: 4, outputTokens: 6 } },
    ] });
    assert.equal(chunks.find((c) => c.type === 'done')!.usage!.totalTokens, 10);
  });

  it('surfaces an error part with status code and Retry-After', async () => {
    const chunks = await run({ parts: [
      { type: 'error', error: { statusCode: 429, message: 'rate limited', responseHeaders: { 'retry-after': '7' } } },
    ] });
    const err = chunks.find((c) => c.type === 'error')!;
    assert.match(err.error!, /HTTP 429: rate limited/);
    assert.equal(err.retryAfter, 7);
  });

  it('surfaces an error part given as a bare string', async () => {
    const chunks = await run({ parts: [{ type: 'error', error: 'bare failure text' }] });
    assert.match(chunks.find((c) => c.type === 'error')!.error!, /bare failure text/);
  });

  it('surfaces an error part object without a status code', async () => {
    const chunks = await run({ parts: [{ type: 'error', error: { message: 'just a message' } }] });
    const err = chunks.find((c) => c.type === 'error')!;
    assert.match(err.error!, /just a message/);
    assert.equal(err.retryAfter, undefined);
  });

  it('emits a terminal done when the stream ends with no finish-step', async () => {
    const chunks = await run({ parts: [{ type: 'text-delta', text: 'x' }] });
    assert.equal(chunks.at(-1)!.type, 'done');
  });

  it('onError swallows MissingToolResults and stringifies non-Error errors', async () => {
    // Neither onError call throws; the stream then finishes normally.
    const chunks = await run({
      onErrors: [new Error('MissingToolResults: tool x'), 'a non-error onError value'],
      parts: [{ type: 'finish-step', finishReason: 'stop', usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } }],
    });
    assert.equal(chunks.find((c) => c.type === 'done')!.finishReason, 'stop');
  });

  it('catch: a caller-aborted stream throw yields "Request aborted"', async () => {
    const ac = new AbortController();
    ac.abort();
    const chunks = await run({ throwErr: new Error('stream blew up') }, ac.signal);
    assert.equal(chunks.length, 1);
    assert.match(chunks[0].error!, /Request aborted/);
  });

  it('catch: a MissingToolResults throw is treated as a graceful tool-calls finish', async () => {
    const chunks = await run({ throwErr: new Error('AI_MissingToolResults: incomplete') });
    const done = chunks.find((c) => c.type === 'done')!;
    assert.equal(done.finishReason, 'tool-calls');
  });

  it('catch: an API error with status + headers is formatted and carries Retry-After', async () => {
    const apiErr = Object.assign(new Error('upstream exploded'), { statusCode: 500, responseHeaders: { 'retry-after': '3' } });
    const chunks = await run({ throwErr: apiErr });
    const err = chunks.find((c) => c.type === 'error')!;
    assert.match(err.error!, /HTTP 500: upstream exploded/);
    assert.equal(err.retryAfter, 3);
    assert.ok(err.errorObject instanceof Error);
  });

  it('catch: a non-Error throw is stringified with no status code or errorObject', async () => {
    const chunks = await run({ throwErr: { weird: 'object' } });
    const err = chunks.find((c) => c.type === 'error')!;
    assert.ok(err.error && err.error.length > 0);
    assert.doesNotMatch(err.error!, /^HTTP /);
    assert.equal(err.errorObject, undefined);
  });
});

// ===========================================================================
// provider.ts — resolveModel / resolveAvailableModel / inferProviderFromBaseUrl
// ===========================================================================
describe('provider.ts — model resolution fallbacks', () => {
  const VE = ['CLAUDE_CODE_USE_VERTEX', 'ANTHROPIC_VERTEX_PROJECT_ID', 'GOOGLE_VERTEX_PROJECT', 'CLOUD_ML_REGION', 'GOOGLE_VERTEX_LOCATION', 'GROQ_API_KEY', 'MISTRAL_API_KEY', 'XAI_API_KEY', 'DEEPSEEK_API_KEY', 'TOGETHER_AI_API_KEY', 'COHERE_API_KEY', 'FIREWORKS_API_KEY', 'PERPLEXITY_API_KEY', 'DEEPINFRA_API_KEY', 'CEREBRAS_API_KEY', 'ALIBABA_API_KEY', 'DASHSCOPE_API_KEY', 'LUMA_API_KEY', 'HF_TOKEN', 'AZURE_OPENAI_API_KEY', 'AWS_ACCESS_KEY_ID', 'GOOGLE_APPLICATION_CREDENTIALS'];
  function withCleanEnv<T>(fn: () => T): T {
    const saved: Record<string, string | undefined> = {};
    for (const k of VE) { saved[k] = process.env[k]; delete process.env[k]; }
    try { return fn(); } finally { for (const [k, v] of Object.entries(saved)) { if (v === undefined) delete process.env[k]; else process.env[k] = v; } }
  }

  afterEach(() => mock.restoreAll());

  it('resolveModel falls back to the OpenAI base URL when nothing is inferable', () => {
    withCleanEnv(() => {
      const model = resolveModel({ apiKey: 'nondescript-opaque-key', model: 'custom-local-model' });
      assert.ok(model, 'a model object is returned'); // no network; just the SDK model
    });
  });

  it('inferProviderFromBaseUrl maps an AI-gateway host (no catalog) to the openai model list', () => {
    const r = inferProviderFromBaseUrl('https://gateway.ai.cloudflare.com/v1/acct/gw/openai');
    assert.ok(r);
    assert.equal(r.provider, 'ai-gateway');
    assert.ok(r.defaultModel.length > 0, 'falls back to the openai preference list');
  });

  it('resolveAvailableModel returns null when the endpoint reports no models (every provider shape)', async () => {
    mock.method(globalThis, 'fetch', async () => ({ ok: true, json: async () => ({}) }));
    assert.equal(await resolveAvailableModel('google', 'k'), null);     // (data.models || [])
    assert.equal(await resolveAvailableModel('anthropic', 'k'), null);  // (data.data || [])
    assert.equal(await resolveAvailableModel('openai', 'k'), null);     // (data.data || [])
  });

  it('resolveAvailableModel prefers a listed preference, else the first model', async () => {
    mock.method(globalThis, 'fetch', async () => ({ ok: true, json: async () => ({ data: [{ id: 'z-model' }, { id: 'o4-mini' }] }) }));
    assert.equal(await resolveAvailableModel('openai', 'k'), 'o4-mini'); // preference hit
    mock.restoreAll();
    mock.method(globalThis, 'fetch', async () => ({ ok: true, json: async () => ({ data: [{ id: 'only-this' }] }) }));
    assert.equal(await resolveAvailableModel('openai', 'k'), 'only-this'); // first fallback
  });

  it('resolveAvailableModel returns null on a non-ok response or a fetch rejection', async () => {
    mock.method(globalThis, 'fetch', async () => ({ ok: false, json: async () => ({}) }));
    assert.equal(await resolveAvailableModel('openai', 'k'), null);
    mock.restoreAll();
    mock.method(globalThis, 'fetch', async () => { throw new Error('down'); });
    assert.equal(await resolveAvailableModel('openai', 'k'), null);
  });
});

// ===========================================================================
// provider.ts — streamChatCompletion setup branches (tools, multi-system, repair)
// ===========================================================================
describe('provider.ts — streamChatCompletion setup branches', () => {
  const VE = ['CLAUDE_CODE_USE_VERTEX', 'ANTHROPIC_VERTEX_PROJECT_ID', 'GOOGLE_VERTEX_PROJECT', 'CLOUD_ML_REGION', 'GOOGLE_VERTEX_LOCATION'];

  it('threads tools, concatenated system prompts and a repair callback, then fails on the dead host', async () => {
    const saved: Record<string, string | undefined> = {};
    for (const k of VE) { saved[k] = process.env[k]; delete process.env[k]; }
    try {
      // No model / no inferable provider / unmatched host -> modelId gpt-4o fallback.
      const config: ProviderConfig = { apiKey: 'nondescript-opaque-key', baseUrl: 'http://does-not-resolve.invalid/v1' };
      const tools: ToolDefinition[] = [{ name: 'bash', description: 'run', inputSchema: { type: 'object', properties: {} } }];
      const messages: Message[] = [
        { role: 'system', content: 'first system' },
        { role: 'system', content: 'second system' },
        { role: 'user', content: 'hello' },
      ];
      const chunks = await collect(streamChatCompletion(config, messages, tools, new AbortController().signal, {
        repairToolCall: async () => null,
      }));
      assert.ok(chunks.some((c) => c.type === 'error'));
    } finally {
      for (const [k, v] of Object.entries(saved)) { if (v === undefined) delete process.env[k]; else process.env[k] = v; }
    }
  });
});

// ===========================================================================
// provider-coordinator.ts
// ===========================================================================
describe('ProviderCoordinator — health, compaction, credential pool branches', () => {
  function sub(overrides?: Partial<ProviderSubsystem>): ProviderSubsystem {
    return {
      primary: { baseUrl: 'https://api.openai.com/v1', apiKey: 'sk-test-key', model: 'gpt-4o' },
      ...overrides,
    };
  }

  it('records request latency and surfaces health stats', () => {
    const c = new ProviderCoordinator(sub());
    c.recordRequestLatency(120);
    c.recordRequestLatency(80);
    const stats = c.getHealthStats();
    assert.ok(stats, 'health stats present');
    assert.equal(typeof stats.requestCount, 'number');
    assert.ok(stats.requestCount >= 2);
  });

  it('getCompactionConfig picks a distinct fallback from the chain', () => {
    const c = new ProviderCoordinator(sub({ compactionModel: 'gpt-4o-mini', fallbackModels: ['gpt-4o-mini', 'o4-mini'] }));
    const cfg = c.getCompactionConfig();
    assert.equal(cfg.model, 'gpt-4o-mini');
    // chain.find skips the compaction model itself and picks o4-mini.
    assert.equal(cfg.fallbackModel, 'o4-mini');
  });

  it('getCompactionConfig leaves fallbackModel unset when the only chain entry equals the model', () => {
    const c = new ProviderCoordinator(sub({ compactionModel: 'gpt-4o-mini', fallbackModel: 'gpt-4o-mini' }));
    const cfg = c.getCompactionConfig();
    assert.equal(cfg.fallbackModel, undefined);
  });

  it('getCompactionConfig with no compaction model and no chain is just the primary', () => {
    const c = new ProviderCoordinator(sub());
    const cfg = c.getCompactionConfig();
    assert.equal(cfg.model, 'gpt-4o');
    assert.equal(cfg.fallbackModel, undefined);
  });

  it('getArchitectConfig returns the architect model when set, else primary', () => {
    assert.equal(new ProviderCoordinator(sub({ architectModel: 'o4-mini' })).getArchitectConfig().model, 'o4-mini');
    assert.equal(new ProviderCoordinator(sub()).getArchitectConfig().model, 'gpt-4o');
  });

  it('isAnthropicProvider is true via model, baseUrl, or provider, and false otherwise', () => {
    assert.equal(new ProviderCoordinator(sub({ primary: { apiKey: 'k', model: 'claude-sonnet-4-6' } })).isAnthropicProvider(), true);
    assert.equal(new ProviderCoordinator(sub({ primary: { apiKey: 'k', model: 'x', baseUrl: 'https://api.anthropic.com' } })).isAnthropicProvider(), true);
    assert.equal(new ProviderCoordinator(sub({ primary: { apiKey: 'k', model: 'x', provider: 'anthropic' } })).isAnthropicProvider(), true);
    assert.equal(new ProviderCoordinator(sub({ primary: { apiKey: 'k', model: 'gpt-4o', baseUrl: 'https://api.openai.com/v1', provider: 'openai' } })).isAnthropicProvider(), false);
  });

  it('hasFallback reflects the presence of a fallback chain', () => {
    assert.equal(new ProviderCoordinator(sub()).hasFallback(), false);
    assert.equal(new ProviderCoordinator(sub({ fallbackModel: 'x' })).hasFallback(), true);
  });

  it('availableCredentials defaults to 1 and markCredentialExhausted is a no-op without a pool', () => {
    const c = new ProviderCoordinator(sub());
    assert.equal(c.availableCredentials, 1);
    c.markCredentialExhausted(); // no pool -> no throw
    assert.equal(c.availableCredentials, 1);
  });

  it('with additional keys, getConfig rotates through the pool and exhaustion reduces availability', () => {
    const c = new ProviderCoordinator(sub(), ['sk-key-two', 'sk-key-three']);
    assert.equal(c.availableCredentials, 3);
    const first = c.getConfig();
    assert.ok(first.apiKey);
    c.markCredentialExhausted(100_000);
    assert.equal(c.availableCredentials, 2);
  });

  it('getConfig falls back to the base config when the pool has no available credential', () => {
    const c = new ProviderCoordinator(sub(), ['sk-key-two']);
    // Exhaust every credential in the pool for a long cooldown.
    for (let i = 0; i < 5; i++) {
      c.getConfig();
      c.markCredentialExhausted(10_000_000);
    }
    const cfg = c.getConfig();
    // No credential available -> the primary apiKey is used unchanged.
    assert.equal(cfg.apiKey, 'sk-test-key');
    assert.equal(c.availableCredentials, 0);
  });

  it('getConfig returns the active fallback model after stepping down the chain', () => {
    const c = new ProviderCoordinator(sub({ fallbackModels: ['m2', 'm3'] }));
    c.recordError();
    assert.equal(c.getConfig().model, 'm2');
  });

  it('recordSuccess on primary (index 0) is a no-op', () => {
    const c = new ProviderCoordinator(sub({ fallbackModel: 'm2' }));
    c.recordSuccess();
    assert.equal(c.getState().usedFallback, false);
  });

  it('sticky fallback keeps the fallback model despite repeated successes', () => {
    const c = new ProviderCoordinator(sub({ fallbackModel: 'm2' }));
    c.recordError();
    c.setStickyFallback(true);
    for (let i = 0; i < 10; i++) c.recordSuccess();
    assert.equal(c.getConfig().model, 'm2');
  });

  it('non-sticky fallback recovers to primary after the cooldown of successes', () => {
    const c = new ProviderCoordinator(sub({ fallbackModel: 'm2' }));
    c.recordError();
    c.recordSuccess();
    c.recordSuccess();
    c.recordSuccess();
    assert.equal(c.getConfig().model, 'gpt-4o');
  });

  it('recordError returns false when the chain is exhausted', () => {
    const c = new ProviderCoordinator(sub({ fallbackModel: 'm2' }));
    assert.equal(c.recordError(), true);
    assert.equal(c.recordError(), false);
  });

  it('infers provider/model from an env var when no key/url hints exist', () => {
    const saved = process.env.GROQ_API_KEY;
    process.env.GROQ_API_KEY = 'gsk_env';
    try {
      const c = new ProviderCoordinator({ primary: { apiKey: '', model: '' } });
      // Env-inferred provider populates currentProvider/currentModel.
      assert.ok(c.getState().currentProvider.length > 0);
    } finally {
      if (saved === undefined) delete process.env.GROQ_API_KEY; else process.env.GROQ_API_KEY = saved;
    }
  });

  it('falls back to "unknown" model/provider when nothing can be inferred', () => {
    const ENV = ['GROQ_API_KEY', 'MISTRAL_API_KEY', 'XAI_API_KEY', 'DEEPSEEK_API_KEY', 'TOGETHER_AI_API_KEY', 'COHERE_API_KEY', 'FIREWORKS_API_KEY', 'PERPLEXITY_API_KEY', 'DEEPINFRA_API_KEY', 'CEREBRAS_API_KEY', 'ALIBABA_API_KEY', 'DASHSCOPE_API_KEY', 'LUMA_API_KEY', 'HF_TOKEN', 'AZURE_OPENAI_API_KEY', 'AWS_ACCESS_KEY_ID', 'ANTHROPIC_VERTEX_PROJECT_ID', 'GOOGLE_APPLICATION_CREDENTIALS'];
    const saved: Record<string, string | undefined> = {};
    for (const k of ENV) { saved[k] = process.env[k]; delete process.env[k]; }
    try {
      const c = new ProviderCoordinator({ primary: { apiKey: '', model: '' } });
      assert.equal(c.getState().currentModel, 'unknown');
      assert.equal(c.getState().currentProvider, 'unknown');
    } finally {
      for (const [k, v] of Object.entries(saved)) { if (v === undefined) delete process.env[k]; else process.env[k] = v; }
    }
  });

  it('rate-limit status flows through updateRateLimits', () => {
    const c = new ProviderCoordinator(sub());
    c.updateRateLimits({ 'x-ratelimit-limit-requests': '100', 'x-ratelimit-remaining-requests': '10' });
    assert.ok(c.getRateLimitStatus().requests);
  });
});

// ===========================================================================
// cost-tracker.ts — live pricing fetch branches (globalThis.fetch mock)
// ===========================================================================
describe('cost-tracker — fetchLivePricing branch coverage', () => {
  afterEach(() => {
    mock.timers.reset();
    mock.restoreAll();
    _resetPricingCache();
  });

  function mockFetch(handler: () => Promise<unknown> | unknown): void {
    mock.method(globalThis, 'fetch', async () => handler());
  }
  const okJson = (data: unknown) => ({ ok: true, json: async () => data });

  it('populates live pricing from gateway data and uses full id + short name + cache price', async () => {
    _resetPricingCache();
    mockFetch(() => okJson({
      data: [
        { id: 'openai/gpt-cov-test', pricing: { input: '0.000002', output: '0.000008', input_cache_read: '0.0000002' } },
        { id: 'noprefix-model', pricing: { input: '0.000001', output: '0.000003' } },
        { id: 'openai/gpt-cov-test', pricing: { input: '0.00009', output: '0.00009' } }, // duplicate short name -> not overwritten
        { pricing: { input: '1', output: '1' } },           // no id -> skipped
        { id: 'x', pricing: { input: '1' } },               // no output -> skipped
        { id: 'y', pricing: { output: '1' } },              // no input -> skipped
        { id: 'z', pricing: { input: 'abc', output: '1' } },// NaN -> skipped
      ],
    }));
    await _fetchPricing();

    const t = new CostTracker('gpt-cov-test'); // resolved via the short-name map
    t.trackUsage({ promptTokens: 1_000_000, completionTokens: 1_000_000, cachedPromptTokens: 1_000_000 });
    const stats = t.getStats();
    // input 2 + output 8 per-million; cache read uses the explicit cached price.
    assert.ok(stats.totalCost > 0);
    assert.ok(stats.cachedCostSavings > 0, 'explicit cache price yields savings');
  });

  it('ignores a non-ok gateway response', async () => {
    _resetPricingCache();
    mockFetch(() => ({ ok: false, json: async () => ({}) }));
    await _fetchPricing();
    // livePricing stays null -> fallback table used; unknown model -> default price.
    const t = new CostTracker('totally-unknown-model');
    t.trackUsage({ promptTokens: 1_000_000, completionTokens: 0 });
    assert.ok(t.getStats().totalCost > 0);
  });

  it('ignores a non-array data payload', async () => {
    _resetPricingCache();
    mockFetch(() => okJson({ data: { not: 'an array' } }));
    await _fetchPricing();
    const t = new CostTracker('gpt-4o');
    t.trackUsage({ promptTokens: 1_000_000, completionTokens: 0 });
    assert.ok(t.getStats().totalCost > 0);
  });

  it('ignores an all-invalid payload (empty map, no live pricing set)', async () => {
    _resetPricingCache();
    mockFetch(() => okJson({ data: [{ id: 'q', pricing: { input: 'x', output: 'y' } }] }));
    await _fetchPricing();
    const t = new CostTracker('gpt-4o');
    t.trackUsage({ promptTokens: 1_000_000, completionTokens: 0 });
    assert.ok(t.getStats().totalCost > 0);
  });

  it('swallows a fetch rejection (network error) and keeps the fallback table', async () => {
    _resetPricingCache();
    mockFetch(() => { throw new Error('network down'); });
    await _fetchPricing();
    const t = new CostTracker('gpt-4o');
    t.trackUsage({ promptTokens: 1_000_000, completionTokens: 0 });
    assert.ok(t.getStats().totalCost > 0);
  });

  it('aborts the gateway fetch when the 5s timeout fires', async () => {
    _resetPricingCache();
    mock.timers.enable({ apis: ['setTimeout'] });
    // fetch stays pending until its abort signal fires -> rejects with abort.
    mock.method(globalThis, 'fetch', (_url: unknown, opts: { signal: AbortSignal }) =>
      new Promise((_res, rej) => {
        opts.signal.addEventListener('abort', () => rej(new Error('aborted by timeout')));
      }),
    );
    const p = _fetchPricing();
    mock.timers.tick(5000); // fires setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS)
    await p; // the abort propagates to fetch, whose rejection is swallowed
    mock.timers.reset();
    // Fallback table still works.
    const t = new CostTracker('gpt-4o');
    t.trackUsage({ promptTokens: 1_000_000, completionTokens: 0 });
    assert.ok(t.getStats().totalCost > 0);
  });
});

describe('cost-tracker — matchPricing, categorizeTool, cached savings fallback', () => {
  afterEach(() => _resetPricingCache());

  it('categorizeTool classifies every branch', () => {
    assert.equal(categorizeTool('mcp__srv__do'), 'mcp');
    assert.equal(categorizeTool('ns:tool'), 'mcp');
    assert.equal(categorizeTool('task'), 'subagent');
    assert.equal(categorizeTool('task_run'), 'subagent');
    assert.equal(categorizeTool('workflow'), 'subagent');
    assert.equal(categorizeTool('skill'), 'skill');
    assert.equal(categorizeTool('SkillFoo'), 'skill');
    assert.equal(categorizeTool('bash'), 'builtin');
  });

  it('matchPricing uses exact, longest-substring, and default fallback', () => {
    _resetPricingCache();
    // Exact fallback-table hit.
    const exact = new CostTracker('gpt-4o');
    exact.trackUsage({ promptTokens: 1_000_000, completionTokens: 0 });
    assert.ok(Math.abs(exact.getStats().totalCost - 2.5) < 1e-6);
    // Substring hit (model string contains a known key).
    const sub = new CostTracker('my-gpt-4o-deployment');
    sub.trackUsage({ promptTokens: 1_000_000, completionTokens: 0 });
    assert.ok(sub.getStats().totalCost > 0);
    // Default pricing for a wholly unknown model.
    const def = new CostTracker('zzz-unknown-xyz');
    def.trackUsage({ promptTokens: 1_000_000, completionTokens: 0 });
    assert.ok(Math.abs(def.getStats().totalCost - 1) < 1e-6);
  });

  it('cached savings use the implicit ratio when no explicit cache price exists', () => {
    _resetPricingCache();
    const t = new CostTracker('gpt-4o'); // fallback entry has null cached price
    t.trackUsage({ promptTokens: 1_000_000, completionTokens: 0, cachedPromptTokens: 1_000_000 });
    const stats = t.getStats();
    assert.ok(stats.cachedCostSavings > 0);
    assert.equal(stats.cachedPromptTokens, 1_000_000);
    assert.ok(stats.cacheHitRate > 0.99);
  });

  it('getStats cacheHitRate is 0 with no prompt tokens; trackTurn/trackToolCall/reset work', () => {
    _resetPricingCache();
    const t = new CostTracker('gpt-4o');
    assert.equal(t.getStats().cacheHitRate, 0);
    t.trackTurn();
    t.trackToolCall('bash');
    t.trackToolCall(); // no name -> no category bump
    assert.equal(t.getStats().turnCount, 1);
    assert.equal(t.getStats().toolCallCount, 2);
    t.reset();
    assert.equal(t.getStats().toolCallCount, 0);
    assert.deepEqual(t.getStats().byCategory, {});
  });
});

// ===========================================================================
// cost-tracker.ts — CacheMissDiagnostics (all causes + priority ordering)
// ===========================================================================
describe('cost-tracker — CacheMissDiagnostics', () => {
  const req = (over: Partial<{ toolDefHash: string; systemPromptHash: string; thinkingMode: string; model: string }> = {}) => ({
    toolDefHash: 'T', systemPromptHash: 'S', thinkingMode: 'off', model: 'gpt-4o', ...over,
  });

  it('first response miss is attributed to first_request; a hit records nothing', () => {
    const d = new CacheMissDiagnostics();
    d.recordRequest(req());
    d.recordResponse(0, 0); // miss, first request
    assert.equal(d.getLastMissCause(), 'first_request');
    d.recordRequest(req());
    d.recordResponse(10, 0); // hit -> no new cause
    assert.equal(d.getTotalMisses(), 1);
  });

  it('recordResponse with no pending request is ignored', () => {
    const d = new CacheMissDiagnostics();
    d.recordResponse(0, 0);
    assert.equal(d.getLastMissCause(), null);
  });

  it('attributes a model switch, then an idle-past-ttl, then tool/system/thinking/unknown', () => {
    const d = new CacheMissDiagnostics(1000);
    d.recordRequest(req());
    d.recordResponse(5, 0); // hit, establishes prev
    // model switch
    d.recordRequest(req({ model: 'gpt-4o-mini' }));
    d.recordResponse(0, 0);
    assert.equal(d.getLastMissCause(), 'model_switched');
    // idle past ttl
    d.recordRequest(req({ model: 'gpt-4o-mini' }));
    d._setLastRequestTime(Date.now() - 10_000);
    d.recordResponse(0, 0);
    assert.equal(d.getLastMissCause(), 'idle_past_ttl');
    // tool defs changed
    d.recordRequest(req({ model: 'gpt-4o-mini', toolDefHash: 'T2' }));
    d.recordResponse(0, 0);
    assert.equal(d.getLastMissCause(), 'tool_definitions_changed');
    // system prompt changed
    d.recordRequest(req({ model: 'gpt-4o-mini', toolDefHash: 'T2', systemPromptHash: 'S2' }));
    d.recordResponse(0, 0);
    assert.equal(d.getLastMissCause(), 'system_prompt_changed');
    // thinking mode changed
    d.recordRequest(req({ model: 'gpt-4o-mini', toolDefHash: 'T2', systemPromptHash: 'S2', thinkingMode: 'high' }));
    d.recordResponse(0, 0);
    assert.equal(d.getLastMissCause(), 'thinking_mode_changed');
    // unknown (everything identical to prev)
    d.recordRequest(req({ model: 'gpt-4o-mini', toolDefHash: 'T2', systemPromptHash: 'S2', thinkingMode: 'high' }));
    d.recordResponse(0, 0);
    assert.equal(d.getLastMissCause(), 'unknown');

    assert.ok(d.getCacheMissCauses().get('model_switched')! >= 1);
    assert.ok(d.getTotalMisses() >= 6);
    d.reset();
    assert.equal(d.getLastMissCause(), null);
    assert.equal(d.getTotalMisses(), 0);
  });
});

// ===========================================================================
// rate-limiter.ts
// ===========================================================================
describe('RateLimitTracker — parseDuration units and getStatus branches', () => {
  it('decodes compound h/m durations when the reset header is not a plain seconds value', () => {
    const t = new RateLimitTracker();
    t.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '40',
      'x-ratelimit-reset-requests': '1h30m', // parser can't decode -> parseDuration h+m
    });
    const req = t.getStatus().requests!;
    assert.ok(req.resetAt > Date.now() + 80 * 60_000, 'about 90 minutes out');
  });

  it('decodes ms and a bare-number (default) segment', () => {
    const t = new RateLimitTracker();
    t.update({
      'x-ratelimit-limit-tokens': '1000',
      'x-ratelimit-remaining-tokens': '250',
      'x-ratelimit-reset-tokens': '500ms20', // ms + bare-number default branch
    });
    const tok = t.getStatus().tokens!;
    assert.ok(tok.resetAt >= Date.now());
  });

  it('decodes a compound value containing seconds (the s unit)', () => {
    const t = new RateLimitTracker();
    t.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '40',
      'x-ratelimit-reset-requests': '2m30s', // parser rejects -> parseDuration m + s
    });
    const req = t.getStatus().requests!;
    assert.ok(req.resetAt > Date.now() + 140_000, 'about 2m30s out');
  });

  it('falls back to 60s when the reset header contains no digits at all', () => {
    const t = new RateLimitTracker();
    const before = Date.now();
    t.update({
      'x-ratelimit-limit-requests': '100',
      'x-ratelimit-remaining-requests': '1',
      'x-ratelimit-reset-requests': 'soon', // no digits, invalid date -> found=false -> 60s
    });
    const req = t.getStatus().requests!;
    assert.ok(req.resetAt >= before + 59_000 && req.resetAt <= Date.now() + 61_000);
  });

  it('resetAt is 0 when there is no reset header at all', () => {
    const t = new RateLimitTracker();
    t.update({ 'x-ratelimit-limit-requests': '100', 'x-ratelimit-remaining-requests': '1' });
    assert.equal(t.getStatus().requests!.resetAt, 0);
  });

  it('getStatus treats a zero limit as fully consumed (100%) for requests and tokens', () => {
    const t = new RateLimitTracker();
    t.update({ 'x-ratelimit-limit-requests': '0', 'x-ratelimit-remaining-requests': '0' });
    t.update({ 'x-ratelimit-limit-tokens': '0', 'x-ratelimit-remaining-tokens': '0' });
    const s = t.getStatus();
    assert.equal(s.requestsPercent, 100);
    assert.equal(s.tokensPercent, 100);
    assert.equal(s.shouldWarn, true);
  });

  it('uses the parser-decoded reset time for both requests and tokens when available', () => {
    const t = new RateLimitTracker();
    t.update({
      'x-ratelimit-limit-requests': '100', 'x-ratelimit-remaining-requests': '50', 'x-ratelimit-reset-requests': '30s',
      'x-ratelimit-limit-tokens': '1000', 'x-ratelimit-remaining-tokens': '500', 'x-ratelimit-reset-tokens': '45s',
    });
    const s = t.getStatus();
    assert.ok(s.requests!.resetAt > Date.now() + 20_000);
    assert.ok(s.tokens!.resetAt > Date.now() + 35_000);
  });

  it('getStatus is all-zero and not warning when no buckets are present', () => {
    const s = new RateLimitTracker().getStatus();
    assert.equal(s.requests, null);
    assert.equal(s.tokens, null);
    assert.equal(s.requestsPercent, 0);
    assert.equal(s.tokensPercent, 0);
    assert.equal(s.shouldWarn, false);
  });

  it('warns when token usage crosses the threshold', () => {
    const t = new RateLimitTracker();
    t.update({ 'x-ratelimit-limit-tokens': '100', 'x-ratelimit-remaining-tokens': '5' });
    const s = t.getStatus();
    assert.equal(s.tokensPercent, 95);
    assert.equal(s.shouldWarn, true);
  });
});

// ===========================================================================
// model-registry.ts
// ===========================================================================
describe('model-registry — listProviders / formatModelList / validateModel', () => {
  it('detectProvider honors an explicit provider and every key/url/env signal', () => {
    // Explicit provider wins (checked before the vertex-env short-circuit).
    assert.equal(detectProvider({ provider: 'cohere', apiKey: 'x' }), 'cohere');
    // The remaining signals are only consulted when the vertex env is absent.
    const keys = ['ANTHROPIC_VERTEX_PROJECT_ID', 'CLAUDE_CODE_USE_VERTEX'];
    const saved: Record<string, string | undefined> = {};
    for (const k of keys) { saved[k] = process.env[k]; delete process.env[k]; }
    try {
      // Key-prefix detection.
      assert.equal(detectProvider({ apiKey: 'sk-ant-abc' }), 'anthropic');
      assert.equal(detectProvider({ apiKey: 'AIzaXYZ' }), 'google');
      assert.equal(detectProvider({ apiKey: 'sk-or-abc' }), 'openrouter');
      assert.equal(detectProvider({ apiKey: 'gsk_abc' }), 'groq');
      assert.equal(detectProvider({ apiKey: 'xai-abc' }), 'xai');
      assert.equal(detectProvider({ apiKey: 'pplx-abc' }), 'perplexity');
      // URL detection (no key).
      assert.equal(detectProvider({ apiKey: '', baseUrl: 'https://generativelanguage.googleapis.com/v1' }), 'google');
      assert.equal(detectProvider({ apiKey: '', baseUrl: 'https://aiplatform.googleapis.com/x' }), 'google-vertex');
      assert.equal(detectProvider({ apiKey: '', baseUrl: 'https://api.anthropic.com' }), 'anthropic');
      assert.equal(detectProvider({ apiKey: '', baseUrl: 'https://openrouter.ai/api/v1' }), 'openrouter');
      assert.equal(detectProvider({ apiKey: '', baseUrl: 'https://api.groq.com/openai/v1' }), 'groq');
      assert.equal(detectProvider({ apiKey: '', baseUrl: 'https://api.mistral.ai/v1' }), 'mistral');
      assert.equal(detectProvider({ apiKey: '', baseUrl: 'https://api.x.ai/v1' }), 'xai');
      assert.equal(detectProvider({ apiKey: '', baseUrl: 'https://api.deepseek.com' }), 'deepseek');
      assert.equal(detectProvider({ apiKey: '', baseUrl: 'https://api.together.xyz/v1' }), 'togetherai');
      assert.equal(detectProvider({ apiKey: '', baseUrl: 'http://localhost:11434/v1' }), 'ollama');
      assert.equal(detectProvider({ apiKey: '', baseUrl: 'http://127.0.0.1:11434/v1' }), 'ollama');
      assert.equal(detectProvider({ apiKey: '', baseUrl: 'https://api.openai.com/v1' }), 'openai');
      // Generic sk- key -> openai; nothing at all -> openai default.
      assert.equal(detectProvider({ apiKey: 'sk-plain-key' }), 'openai');
    } finally {
      for (const [k, v] of Object.entries(saved)) { if (v === undefined) delete process.env[k]; else process.env[k] = v; }
    }
  });

  it('detectProvider detects vertex from the environment when no other signal exists', () => {
    const saved = process.env.ANTHROPIC_VERTEX_PROJECT_ID;
    const savedUse = process.env.CLAUDE_CODE_USE_VERTEX;
    process.env.ANTHROPIC_VERTEX_PROJECT_ID = 'proj-123';
    delete process.env.CLAUDE_CODE_USE_VERTEX;
    try {
      assert.equal(detectProvider({ apiKey: 'nondescript-key' }), 'anthropic-vertex');
    } finally {
      if (saved === undefined) delete process.env.ANTHROPIC_VERTEX_PROJECT_ID; else process.env.ANTHROPIC_VERTEX_PROJECT_ID = saved;
      if (savedUse === undefined) delete process.env.CLAUDE_CODE_USE_VERTEX; else process.env.CLAUDE_CODE_USE_VERTEX = savedUse;
    }
  });

  it('detectProvider falls back to openai with no signals and the env cleared', () => {
    const keys = ['ANTHROPIC_VERTEX_PROJECT_ID', 'CLAUDE_CODE_USE_VERTEX'];
    const saved: Record<string, string | undefined> = {};
    for (const k of keys) { saved[k] = process.env[k]; delete process.env[k]; }
    try {
      assert.equal(detectProvider({ apiKey: 'nondescript-key', baseUrl: 'https://unknown.example/v1' }), 'openai');
    } finally {
      for (const [k, v] of Object.entries(saved)) { if (v === undefined) delete process.env[k]; else process.env[k] = v; }
    }
  });

  it('listProviders returns provider ids with their preferred models', () => {
    const list = listProviders();
    assert.ok(list.length > 0);
    const openai = list.find((p) => p.id === 'openai');
    assert.ok(openai && openai.models.length > 0);
  });

  it('formatModelList renders a header + indented models, or an empty-state line', () => {
    assert.match(formatModelList(['a', 'b'], 'openai'), /Available models \(openai, 2\):/);
    assert.match(formatModelList([], 'openai'), /Could not retrieve/);
  });

  it('validateModel short-circuits as valid when the provider returns no list', async () => {
    // ollama has no static catalog; with no baseUrl the live listing throws and
    // listModels returns [] -> validateModel cannot validate -> valid:true.
    const res = await validateModel({ provider: 'ollama', apiKey: '', baseUrl: '' }, 'anything');
    assert.equal(res.valid, true);
    assert.deepEqual(res.available, []);
  });

  it('validateModel reports exact and partial matches from a live list', async () => {
    const server = http.createServer((req, res) => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ data: [{ id: 'gpt-foo-123' }, { id: 'gpt-bar-456' }] }));
    });
    await listen(server);
    const cfg: ProviderConfig = { provider: 'openai', apiKey: 'sk-x', baseUrl: `http://127.0.0.1:${portOf(server)}/v1` };
    const exact = await validateModel(cfg, 'gpt-foo-123');
    const partial = await validateModel(cfg, 'gpt-foo'); // substring -> suggestion
    const miss = await validateModel(cfg, 'nothing-like-this');
    await new Promise<void>((r) => server.close(() => r()));
    assert.equal(exact.valid, true);
    assert.equal(partial.valid, false);
    assert.equal(partial.suggestion, 'gpt-foo-123');
    assert.equal(miss.valid, false);
    assert.equal(miss.suggestion, undefined);
  });
});

describe('model-registry — diagnoseModelDiscovery (credential-safe diagnostics)', () => {
  it('401/403 name the provider credential hint for openrouter/ollama/default providers', () => {
    assert.match(diagnoseModelDiscovery(Object.assign(new Error('x'), { statusCode: 401 }), 'openrouter'), /AI_API_KEY \(OpenRouter\)/);
    assert.match(diagnoseModelDiscovery(Object.assign(new Error('x'), { statusCode: 403 }), 'ollama'), /Ollama needs no API key/);
    assert.match(diagnoseModelDiscovery(Object.assign(new Error('x'), { statusCode: 401 }), 'groq'), /the groq API key/);
    assert.match(diagnoseModelDiscovery(Object.assign(new Error('x'), { statusCode: 401 }), 'openai'), /OPENAI_API_KEY/);
    assert.match(diagnoseModelDiscovery(Object.assign(new Error('x'), { statusCode: 403 }), 'google'), /GOOGLE_API_KEY/);
  });

  it('classifies 404, other status codes, and network/timeout/json/baseUrl failures', () => {
    assert.match(diagnoseModelDiscovery(Object.assign(new Error('x'), { statusCode: 404 }), 'openai'), /not found \(HTTP 404\)/);
    assert.match(diagnoseModelDiscovery(Object.assign(new Error('x'), { statusCode: 500 }), 'openai'), /returned HTTP 500/);
    assert.match(diagnoseModelDiscovery(new Error('baseUrl is required'), 'openai'), /No base URL configured/);
    assert.match(diagnoseModelDiscovery(new Error('Timeout'), 'openai'), /Timed out/);
    assert.match(diagnoseModelDiscovery(Object.assign(new Error('x'), { code: 'ECONNREFUSED' }), 'openai'), /Could not reach/);
    assert.match(diagnoseModelDiscovery(Object.assign(new Error('x'), { code: 'ENOTFOUND' }), 'openai'), /ENOTFOUND/);
    assert.match(diagnoseModelDiscovery(new Error('Invalid JSON response'), 'openai'), /non-JSON response/);
  });

  it('redacts secrets from an otherwise-unclassified error message', () => {
    const msg = diagnoseModelDiscovery(new Error('boom Bearer sk-secret1234567890abcdef'), 'openai');
    assert.doesNotMatch(msg, /sk-secret1234567890abcdef/);
    assert.match(msg, /Could not list models/);
  });

  it('handles a non-Error, nullish failure value', () => {
    assert.match(diagnoseModelDiscovery(null, 'openai'), /Could not list models|unknown error/);
  });
});

describe('model-registry — listModels / listModelsDiagnostic over real transports', () => {
  it('returns the static list for a provider with a built-in catalog (anthropic)', async () => {
    const models = await listModels({ provider: 'anthropic', apiKey: 'sk-ant-xxx' });
    assert.ok(models.length > 0);
    assert.ok(models.every((m) => m.id && m.name));
    const diag = await listModelsDiagnostic({ provider: 'anthropic', apiKey: 'sk-ant-xxx' });
    assert.equal(diag.diagnostic, null);
    assert.ok(diag.models.length > 0);
  });

  it('lists models from an OpenAI-compatible endpoint and feeds capability discovery', async () => {
    const server = http.createServer((req, res) => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ data: [{ id: 'local-model-a', owned_by: 'me' }, { id: 'local-model-b' }] }));
    });
    await listen(server);
    const cfg: ProviderConfig = { provider: 'openai', apiKey: 'sk-x', baseUrl: `http://127.0.0.1:${portOf(server)}/v1` };
    const models = await listModels(cfg);
    const diag = await listModelsDiagnostic(cfg);
    await new Promise<void>((r) => server.close(() => r()));
    assert.deepEqual(models.map((m) => m.id).sort(), ['local-model-a', 'local-model-b']);
    assert.equal(diag.diagnostic, null);
  });

  it('lists models without an Authorization header when no api key is set, tolerating a missing data field', async () => {
    const seen: Array<string | undefined> = [];
    const server = http.createServer((req, res) => {
      seen.push(req.headers.authorization);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({})); // no `data` field -> data.data || []
    });
    await listen(server);
    const cfg: ProviderConfig = { provider: 'openai', apiKey: '', baseUrl: `http://127.0.0.1:${portOf(server)}/v1` };
    const models = await listModels(cfg);
    await new Promise<void>((r) => server.close(() => r()));
    assert.deepEqual(models, []);
    assert.equal(seen[0], undefined, 'no Authorization header sent without an api key');
  });

  it('reports an empty-list diagnostic when the endpoint returns no models', async () => {
    const server = http.createServer((req, res) => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ data: [] }));
    });
    await listen(server);
    const cfg: ProviderConfig = { provider: 'openai', apiKey: 'sk-x', baseUrl: `http://127.0.0.1:${portOf(server)}/v1` };
    const diag = await listModelsDiagnostic(cfg);
    await new Promise<void>((r) => server.close(() => r()));
    assert.ok(diag.diagnostic && /empty model list/i.test(diag.diagnostic));
  });

  it('diagnoses an HTTP error status from the endpoint', async () => {
    const server = http.createServer((req, res) => { res.writeHead(500); res.end('nope'); });
    await listen(server);
    const cfg: ProviderConfig = { provider: 'openai', apiKey: 'sk-x', baseUrl: `http://127.0.0.1:${portOf(server)}/v1` };
    const diag = await listModelsDiagnostic(cfg);
    const models = await listModels(cfg); // falls back to static (openai prefs)
    await new Promise<void>((r) => server.close(() => r()));
    assert.ok(diag.diagnostic && /HTTP 500/.test(diag.diagnostic));
    assert.ok(models.length > 0, 'listModels falls back to the static openai list');
  });

  it('diagnoses invalid JSON from the endpoint', async () => {
    const server = http.createServer((req, res) => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end('not json at all');
    });
    await listen(server);
    const cfg: ProviderConfig = { provider: 'openai', apiKey: 'sk-x', baseUrl: `http://127.0.0.1:${portOf(server)}/v1` };
    const diag = await listModelsDiagnostic(cfg);
    await new Promise<void>((r) => server.close(() => r()));
    assert.ok(diag.diagnostic && /non-JSON response/i.test(diag.diagnostic));
  });

  it('reports "No base URL configured" when an OpenAI-compatible provider has no baseUrl', async () => {
    const diag = await listModelsDiagnostic({ provider: 'openai', apiKey: 'sk-x', baseUrl: '' });
    assert.ok(diag.diagnostic && /No base URL configured/.test(diag.diagnostic));
  });

  it('diagnoses a refused connection as unreachable', async () => {
    const tmp = http.createServer();
    await listen(tmp);
    const port = portOf(tmp);
    await new Promise<void>((r) => tmp.close(() => r()));
    const cfg: ProviderConfig = { provider: 'openai', apiKey: 'sk-x', baseUrl: `http://127.0.0.1:${port}/v1` };
    const diag = await listModelsDiagnostic(cfg);
    assert.ok(diag.diagnostic && diag.diagnostic.length > 0);
  });

  it('lists Gemini models from the (env-overridable) Gemini endpoint', async () => {
    const server = http.createServer((req, res) => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ models: [{ name: 'models/gemini-test-pro', displayName: 'Gemini Test Pro' }] }));
    });
    await listen(server);
    const saved = process.env.SI_GEMINI_MODELS_URL;
    process.env.SI_GEMINI_MODELS_URL = `http://127.0.0.1:${portOf(server)}/v1beta/models`;
    try {
      const models = await listModels({ provider: 'google', apiKey: 'AIzaKey' });
      assert.deepEqual(models, [{ id: 'gemini-test-pro', name: 'Gemini Test Pro' }]);
    } finally {
      if (saved === undefined) delete process.env.SI_GEMINI_MODELS_URL; else process.env.SI_GEMINI_MODELS_URL = saved;
      await new Promise<void>((r) => server.close(() => r()));
    }
  });

  it('tolerates a Gemini response with no models field', async () => {
    const server = http.createServer((req, res) => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({})); // no `models` -> data.models || []
    });
    await listen(server);
    const saved = process.env.SI_GEMINI_MODELS_URL;
    process.env.SI_GEMINI_MODELS_URL = `http://127.0.0.1:${portOf(server)}/v1beta/models`;
    try {
      const diag = await listModelsDiagnostic({ provider: 'google', apiKey: 'AIzaKey' });
      assert.deepEqual(diag.models, []);
      assert.ok(diag.diagnostic && /empty model list/i.test(diag.diagnostic));
    } finally {
      if (saved === undefined) delete process.env.SI_GEMINI_MODELS_URL; else process.env.SI_GEMINI_MODELS_URL = saved;
      await new Promise<void>((r) => server.close(() => r()));
    }
  });

  it('times out a model-list request when the endpoint never responds', async () => {
    const server = http.createServer(() => { /* accept the connection, never reply */ });
    await listen(server);
    const saved = process.env.SI_MODEL_LIST_TIMEOUT_MS;
    process.env.SI_MODEL_LIST_TIMEOUT_MS = '60';
    const cfg: ProviderConfig = { provider: 'openai', apiKey: 'sk-x', baseUrl: `http://127.0.0.1:${portOf(server)}/v1` };
    try {
      const diag = await listModelsDiagnostic(cfg);
      assert.ok(diag.diagnostic && /Timed out/i.test(diag.diagnostic), `got ${diag.diagnostic}`);
    } finally {
      if (saved === undefined) delete process.env.SI_MODEL_LIST_TIMEOUT_MS; else process.env.SI_MODEL_LIST_TIMEOUT_MS = saved;
      server.close();
    }
  });

  it('exercises the Gemini listing path (returns the static fallback on failure)', async () => {
    // google is a non-static provider -> listGeminiModels runs against the real
    // Google endpoint. Without valid creds it fails fast and listModels returns
    // the static google catalog; the function body is executed either way.
    const models = await listModels({ provider: 'google', apiKey: 'AIzaInvalidKeyForTest' });
    assert.ok(Array.isArray(models));
  });
});

// Keep a stable top-level hook so the suite has deterministic setup/teardown.
describe('provider-cov suite guards', () => {
  before(() => { /* no-op: ensures describe import is used */ });
  after(() => { _resetPricingCache(); });
  it('is present', () => assert.ok(true));
});
