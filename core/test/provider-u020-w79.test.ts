import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { streamChatCompletionWithRetry, streamChatCompletion } from '../src/provider';
import type { ProviderConfig, Message, StreamChunk } from '../src/types';

// ---------------------------------------------------------------------------
// U020 / w79 — retry status distinguishes a connection-level failure (no HTTP
// response) from a real HTTP status.
//
// Parity gap (references/claude/.../entrypoints/sdk/coreSchemas.ts:L1572-1588):
// Claude's SDKAPIRetryMessageSchema types `error_status: z.number().nullable()`
// and documents it is "null for connection errors (e.g. timeouts) that had no
// HTTP response." AMI previously collapsed that case to the sentinel `0`, which
// is indistinguishable from a (fabricated) HTTP status. These tests assert
// `onRetry` now receives `null` for timeout / network drops, and a real number
// for an HTTP error status.
//
// RED on revert: restoring the `0` sentinels at the timeout / network / no-parsed
// -status onRetry call sites makes the `null` assertions fail.
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

describe('streamChatCompletionWithRetry — connection-failure retry status is null', () => {
  it('reports null (no HTTP response) when a network drop is retried', async () => {
    let attempts = 0;
    const seen: Array<number | null> = [];

    const fakeStream = (async function* () {
      attempts++;
      if (attempts === 1) {
        // A connection-level network failure: the SDK throws (no error chunk,
        // no HTTP response). ETIMEDOUT is a transient network code, not a stale
        // keep-alive reset, so it takes the plain network-retry branch.
        throw Object.assign(new Error('fetch failed: connect ETIMEDOUT'), { code: 'ETIMEDOUT' });
      }
      yield { type: 'done', finishReason: 'stop' } as StreamChunk;
    }) as typeof streamChatCompletion;

    const ac = new AbortController();
    await collect(
      streamChatCompletionWithRetry(config, messages, [], ac.signal, {
        maxRetries: 3,
        baseDelayMs: 1,
        maxDelayMs: 5,
        streamImpl: fakeStream,
        onRetry: (_a, _m, _d, statusCode) => { seen.push(statusCode); },
      }),
    );

    assert.equal(attempts, 2, 'expected one retry after the network drop');
    assert.ok(seen.length >= 1, 'onRetry must fire for the network retry');
    assert.ok(seen.every((s) => s === null), `network retry status must be null, got ${JSON.stringify(seen)}`);
    assert.ok(!seen.includes(0), 'network retry must NOT use the 0 sentinel');
  });

  it('reports null (no HTTP response) when a request timeout is retried', async () => {
    let attempts = 0;
    const seen: Array<number | null> = [];

    const fakeStream = (async function* () {
      attempts++;
      if (attempts === 1) {
        // A timeout surfaces as an AbortError while the CALLER signal is not
        // aborted — streamChatCompletionWithRetry treats that as a timeout.
        throw Object.assign(new Error('The operation was aborted'), { name: 'AbortError' });
      }
      yield { type: 'done', finishReason: 'stop' } as StreamChunk;
    }) as typeof streamChatCompletion;

    const ac = new AbortController();
    await collect(
      streamChatCompletionWithRetry(config, messages, [], ac.signal, {
        maxRetries: 3,
        baseDelayMs: 1,
        maxDelayMs: 5,
        streamImpl: fakeStream,
        onRetry: (_a, _m, _d, statusCode) => { seen.push(statusCode); },
      }),
    );

    assert.equal(attempts, 2, 'expected one retry after the timeout');
    assert.ok(seen.length >= 1, 'onRetry must fire for the timeout retry');
    assert.ok(seen.every((s) => s === null), `timeout retry status must be null, got ${JSON.stringify(seen)}`);
    assert.ok(!seen.includes(0), 'timeout retry must NOT use the 0 sentinel');
  });

  it('still reports the real numeric HTTP status for a server-error retry', async () => {
    let attempts = 0;
    const seen: Array<number | null> = [];

    const fakeStream = (async function* () {
      attempts++;
      if (attempts === 1) {
        yield { type: 'error', error: 'HTTP 503: service temporarily unavailable' } as StreamChunk;
        return;
      }
      yield { type: 'done', finishReason: 'stop' } as StreamChunk;
    }) as typeof streamChatCompletion;

    const ac = new AbortController();
    await collect(
      streamChatCompletionWithRetry(config, messages, [], ac.signal, {
        maxRetries: 3,
        baseDelayMs: 1,
        maxDelayMs: 5,
        streamImpl: fakeStream,
        onRetry: (_a, _m, _d, statusCode) => { seen.push(statusCode); },
      }),
    );

    assert.equal(attempts, 2, 'expected one retry after the 503');
    assert.ok(seen.includes(503), `HTTP retry must carry the numeric status, got ${JSON.stringify(seen)}`);
    assert.ok(!seen.includes(null), 'a real HTTP status must NOT be reported as null');
  });
});
