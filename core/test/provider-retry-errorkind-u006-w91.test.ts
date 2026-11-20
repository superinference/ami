import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { streamChatCompletionWithRetry, streamChatCompletion } from '../src/provider';
import type { ProviderConfig, Message, StreamChunk, RetryErrorKind } from '../src/types';

// ---------------------------------------------------------------------------
// U006 / w91 — the `onRetry` callback now carries an `errorKind` discriminator
// (parity: the reference's withRetry cause classification) so a connection reset, a
// request timeout, an idle-stream abort, and an HTTP-status retry are each
// representable distinctly even when no HTTP status is present.
//
// RED on revert: removing the 5th `errorKind` argument from the onRetry call
// sites (or the `idleFired` flag) makes these assertions fail.
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

describe('streamChatCompletionWithRetry — onRetry errorKind (U006/w91)', () => {
  it('classifies a network drop as connection_reset', async () => {
    let attempts = 0;
    const kinds: RetryErrorKind[] = [];
    const fakeStream = (async function* () {
      attempts++;
      if (attempts === 1) {
        throw Object.assign(new Error('fetch failed: connect ETIMEDOUT'), { code: 'ETIMEDOUT' });
      }
      yield { type: 'done', finishReason: 'stop' } as StreamChunk;
    }) as typeof streamChatCompletion;

    await collect(streamChatCompletionWithRetry(config, messages, [], new AbortController().signal, {
      maxRetries: 3, baseDelayMs: 1, maxDelayMs: 5, streamImpl: fakeStream,
      onRetry: (_a, _m, _d, _s, errorKind) => { kinds.push(errorKind); },
    }));

    assert.equal(attempts, 2);
    assert.ok(kinds.length >= 1, 'onRetry must fire');
    assert.ok(kinds.every((k) => k === 'connection_reset'), `got ${JSON.stringify(kinds)}`);
  });

  it('classifies a request timeout as connection_timeout', async () => {
    let attempts = 0;
    const kinds: RetryErrorKind[] = [];
    const fakeStream = (async function* () {
      attempts++;
      if (attempts === 1) {
        throw Object.assign(new Error('The operation was aborted'), { name: 'AbortError' });
      }
      yield { type: 'done', finishReason: 'stop' } as StreamChunk;
    }) as typeof streamChatCompletion;

    await collect(streamChatCompletionWithRetry(config, messages, [], new AbortController().signal, {
      maxRetries: 3, baseDelayMs: 1, maxDelayMs: 5, streamImpl: fakeStream,
      onRetry: (_a, _m, _d, _s, errorKind) => { kinds.push(errorKind); },
    }));

    assert.equal(attempts, 2);
    assert.ok(kinds.every((k) => k === 'connection_timeout'), `got ${JSON.stringify(kinds)}`);
  });

  it('classifies an idle-stream watchdog abort as stream_idle', async () => {
    let attempts = 0;
    const kinds: RetryErrorKind[] = [];
    // First attempt never yields a chunk; it blocks until the idle watchdog aborts
    // the per-request signal, which surfaces as an AbortError — distinct from a hard
    // request timeout because the idle timer fired.
    const fakeStream = (async function* (_c: ProviderConfig, _m: Message[], _t: unknown, sig: AbortSignal) {
      attempts++;
      if (attempts === 1) {
        await new Promise<void>((_resolve, reject) => {
          sig.addEventListener('abort', () => reject(Object.assign(new Error('aborted'), { name: 'AbortError' })), { once: true });
        });
      }
      yield { type: 'done', finishReason: 'stop' } as StreamChunk;
    }) as unknown as typeof streamChatCompletion;

    await collect(streamChatCompletionWithRetry(config, messages, [], new AbortController().signal, {
      maxRetries: 3, baseDelayMs: 1, maxDelayMs: 5, idleTimeoutMs: 10, streamImpl: fakeStream,
      onRetry: (_a, _m, _d, _s, errorKind) => { kinds.push(errorKind); },
    }));

    assert.equal(attempts, 2, 'expected one retry after the idle abort');
    assert.ok(kinds.includes('stream_idle'), `expected stream_idle, got ${JSON.stringify(kinds)}`);
    assert.ok(!kinds.includes('connection_timeout'), 'idle abort must not be a plain connection_timeout');
  });

  it('classifies a real HTTP server-error retry as http (with the numeric status)', async () => {
    let attempts = 0;
    const seen: Array<{ status: number | null; kind: RetryErrorKind }> = [];
    const fakeStream = (async function* () {
      attempts++;
      if (attempts === 1) {
        yield { type: 'error', error: 'HTTP 503: service temporarily unavailable' } as StreamChunk;
        return;
      }
      yield { type: 'done', finishReason: 'stop' } as StreamChunk;
    }) as typeof streamChatCompletion;

    await collect(streamChatCompletionWithRetry(config, messages, [], new AbortController().signal, {
      maxRetries: 3, baseDelayMs: 1, maxDelayMs: 5, streamImpl: fakeStream,
      onRetry: (_a, _m, _d, status, kind) => { seen.push({ status, kind }); },
    }));

    assert.equal(attempts, 2);
    assert.ok(seen.some((s) => s.status === 503 && s.kind === 'http'), `got ${JSON.stringify(seen)}`);
  });
});
