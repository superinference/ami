import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { streamChatCompletionWithRetry, streamChatCompletion } from '../src/provider';
import type { ProviderConfig, Message, StreamChunk } from '../src/types';

// ---------------------------------------------------------------------------
// Red-green reproducer for the `nonretryable-retry` bug:
// streamChatCompletionWithRetry retried requests the API had explicitly marked
// as non-retryable (x-should-retry: false) because the retry gate decided
// purely on the status code. For a 503 carrying x-should-retry: false it would
// retry up to maxRetries, holding the turn for minutes. The fix honors the
// x-should-retry header already captured on the error chunk.
//
// These tests use the `streamImpl` test seam so the bug is observed through the
// real streamChatCompletionWithRetry control flow without any network I/O.
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

describe('streamChatCompletionWithRetry - honors x-should-retry header', () => {
  it('does NOT retry a retryable status code when x-should-retry is false', async () => {
    let attempts = 0;
    let retried = false;

    const fakeStream = (async function* () {
      attempts++;
      yield {
        type: 'error',
        error: 'HTTP 503: service temporarily unavailable',
        responseHeaders: { 'x-should-retry': 'false' },
      } as StreamChunk;
    }) as typeof streamChatCompletion;

    const ac = new AbortController();
    const chunks = await collect(
      streamChatCompletionWithRetry(config, messages, [], ac.signal, {
        maxRetries: 5,
        baseDelayMs: 1,
        maxDelayMs: 10,
        streamImpl: fakeStream,
        onRetry: () => { retried = true; },
      }),
    );

    // RED (current source): 503 is in RETRYABLE_STATUS_CODES, the header is
    // ignored, so the wrapper retries 5 times -> attempts === 6, retried === true.
    // GREEN (after fix): x-should-retry:false blocks the retry on the first
    // error -> attempts === 1, onRetry never fires.
    assert.equal(attempts, 1, `expected exactly one attempt, got ${attempts}`);
    assert.equal(retried, false, 'onRetry must not fire when x-should-retry is false');
    assert.equal(chunks.filter((c) => c.type === 'error').length, 1);
    assert.equal(chunks[chunks.length - 1].type, 'error');
  });

  it('still retries a retryable status code when no x-should-retry header is present', async () => {
    let attempts = 0;

    const fakeStream = (async function* () {
      attempts++;
      if (attempts === 1) {
        yield { type: 'error', error: 'HTTP 503: service temporarily unavailable' } as StreamChunk;
        return;
      }
      yield { type: 'content_delta', text: 'ok' } as StreamChunk;
      yield { type: 'done', finishReason: 'stop' } as StreamChunk;
    }) as typeof streamChatCompletion;

    const ac = new AbortController();
    const chunks = await collect(
      streamChatCompletionWithRetry(config, messages, [], ac.signal, {
        maxRetries: 3,
        baseDelayMs: 1,
        maxDelayMs: 10,
        streamImpl: fakeStream,
      }),
    );

    // Guards against over-blocking: a normal retryable 503 with no header must
    // still retry and succeed. Covers the `shouldRetryHdr !== 'false'` true arm.
    assert.ok(attempts >= 2, `expected a retry, got ${attempts} attempt(s)`);
    assert.ok(chunks.some((c) => c.type === 'content_delta'));
  });

  it('retries a normally non-retryable status when x-should-retry is true', async () => {
    let attempts = 0;

    const fakeStream = (async function* () {
      attempts++;
      if (attempts === 1) {
        // 408 is NOT in RETRYABLE_STATUS_CODES, but the API says to retry.
        yield { type: 'error', error: 'HTTP 408: request timeout', responseHeaders: { 'x-should-retry': 'true' } } as StreamChunk;
        return;
      }
      yield { type: 'content_delta', text: 'ok' } as StreamChunk;
      yield { type: 'done', finishReason: 'stop' } as StreamChunk;
    }) as typeof streamChatCompletion;

    const ac = new AbortController();
    const chunks = await collect(
      streamChatCompletionWithRetry(config, messages, [], ac.signal, {
        maxRetries: 3, baseDelayMs: 1, maxDelayMs: 10, streamImpl: fakeStream,
      }),
    );

    // GREEN: x-should-retry:true forces a retry on a status the set would reject,
    // matching the SDK semantics the disabled SDK layer would otherwise provide.
    // RED (header-only gate removed): 408 is non-retryable -> no retry -> attempts===1.
    assert.ok(attempts >= 2, `x-should-retry:true must force a retry, got ${attempts}`);
    assert.ok(chunks.some((c) => c.type === 'content_delta'));
  });
});
