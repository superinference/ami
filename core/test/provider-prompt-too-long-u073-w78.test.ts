import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { streamChatCompletionWithRetry, streamChatCompletion } from '../src/provider';
import type { ProviderConfig, Message, StreamChunk } from '../src/types';

// ---------------------------------------------------------------------------
// U073 / w78 — deterministic 400 "prompt is too long" do-not-retry special-casing.
//
// A 400 whose body is "prompt is too long: N tokens > M maximum" is DETERMINISTIC:
// the input itself exceeds the context window, so re-sending the SAME request
// loops. The provider must stop immediately with a single error chunk and never
// retry — even when a gateway sends x-should-retry:true (which would otherwise
// force a pointless retry). Mirrors the reference's detectPromptTooLong path.
//
// RED (revert the provider guard): x-should-retry:true forces a retry, so
// attempts >= 2 and onRetry fires. GREEN (with the guard): attempts === 1 and
// onRetry never fires.
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

describe('streamChatCompletionWithRetry — deterministic prompt-too-long 400', () => {
  it('does NOT retry a prompt-too-long 400 even with x-should-retry:true', async () => {
    let attempts = 0;
    let retried = false;

    const fakeStream = (async function* () {
      attempts++;
      yield {
        type: 'error',
        error: 'HTTP 400: prompt is too long: 215000 tokens > 200000 maximum',
        responseHeaders: { 'x-should-retry': 'true' },
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

    assert.equal(attempts, 1, `expected exactly one attempt, got ${attempts}`);
    assert.equal(retried, false, 'onRetry must not fire for a deterministic prompt-too-long 400');
    assert.equal(chunks.filter((c) => c.type === 'error').length, 1);
    assert.equal(chunks[chunks.length - 1].type, 'error');
  });

  it('still surfaces a prompt-too-long 400 as a single terminal error chunk (no header)', async () => {
    let attempts = 0;
    const fakeStream = (async function* () {
      attempts++;
      yield {
        type: 'error',
        error: 'prompt is too long: 137500 tokens > 135000 maximum',
      } as StreamChunk;
    }) as typeof streamChatCompletion;

    const ac = new AbortController();
    const chunks = await collect(
      streamChatCompletionWithRetry(config, messages, [], ac.signal, {
        maxRetries: 3, baseDelayMs: 1, maxDelayMs: 10, streamImpl: fakeStream,
      }),
    );

    assert.equal(attempts, 1, `expected one attempt, got ${attempts}`);
    assert.equal(chunks.filter((c) => c.type === 'error').length, 1);
  });

  it('still retries a transient 429 (guard does not over-block other 4xx)', async () => {
    let attempts = 0;
    const fakeStream = (async function* () {
      attempts++;
      if (attempts === 1) {
        yield { type: 'error', error: 'HTTP 429: too many requests' } as StreamChunk;
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

    assert.ok(attempts >= 2, `expected a retry on 429, got ${attempts}`);
    assert.ok(chunks.some((c) => c.type === 'content_delta'));
  });
});
