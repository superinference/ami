import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  streamChatCompletion,
  streamChatCompletionWithRetry,
  isEmptyStreamError,
  EMPTY_STREAM_ERROR,
} from '../src/provider';
import type { ProviderConfig, Message, StreamChunk } from '../src/types';

// ---------------------------------------------------------------------------
// Empty / degenerate response-stream detection (parity with Claude Code's
// claude.ts "Stream ended without receiving any events" non-streaming fallback).
//
// Two proxy failure modes must be treated as a retryable failure rather than a
// silent, empty `done` turn:
//   1. No events at all (proxy returned HTTP 200 with a non-SSE body).
//   2. A message_start arrived but zero content blocks completed AND no
//      stop_reason — a truncated SSE body.
// Both surface as a stream that ends with no content / tool-call / finish-step.
// A legitimate structured-output "end_turn with no content" response carries a
// stop_reason (a finish-step) and so must NOT be flagged.
// ---------------------------------------------------------------------------

const CFG: ProviderConfig = {
  baseUrl: 'http://127.0.0.1:1',
  apiKey: 'test-key',
  model: 'test-model',
};
const MSG: Message[] = [{ role: 'user', content: 'hi' }];

async function collect(gen: AsyncGenerator<StreamChunk>): Promise<StreamChunk[]> {
  const out: StreamChunk[] = [];
  for await (const c of gen) out.push(c);
  return out;
}

// Fake AI-SDK streamText that yields the given parts (and never a finish-step
// unless one is listed).
function fakeStreamText(parts: Array<Record<string, unknown>>): unknown {
  return () => ({
    stream: (async function* () {
      for (const p of parts) yield p;
    })(),
  });
}

describe('isEmptyStreamError', () => {
  it('matches the sentinel text and nothing else', () => {
    assert.equal(isEmptyStreamError(EMPTY_STREAM_ERROR), true);
    assert.equal(isEmptyStreamError('HTTP 500: boom'), false);
    assert.equal(isEmptyStreamError(undefined), false);
    assert.equal(isEmptyStreamError(''), false);
  });
});

describe('streamChatCompletion - empty/degenerate stream', () => {
  it('yields a retryable EMPTY_STREAM_ERROR when the stream produces no parts at all', async () => {
    const chunks = await collect(
      streamChatCompletion(CFG, MSG, [], new AbortController().signal, {
        streamTextImpl: fakeStreamText([]) as never,
      }),
    );
    // RED (pre-fix): the loop fell through to a silent `yield { type: 'done' }`.
    // GREEN (post-fix): a single error chunk carrying the empty-stream sentinel.
    assert.equal(chunks.length, 1);
    assert.equal(chunks[0].type, 'error');
    assert.equal(isEmptyStreamError(chunks[0].error), true);
    assert.equal(chunks.some((c) => c.type === 'done'), false);
  });

  it('still emits a terminal done when content was produced but no finish-step arrived', async () => {
    // Regression lock: a truncated stream that DID produce content keeps the
    // prior behavior (content already committed) and must NOT be flagged empty.
    const chunks = await collect(
      streamChatCompletion(CFG, MSG, [], new AbortController().signal, {
        streamTextImpl: fakeStreamText([{ type: 'text-delta', text: 'x' }]) as never,
      }),
    );
    assert.equal(chunks.at(-1)!.type, 'done');
    assert.equal(chunks.some((c) => isEmptyStreamError(c.error)), false);
  });

  it('does NOT flag a legitimate empty response that carries a finish-step (stop_reason)', async () => {
    // Structured-output end_turn-with-no-content: zero content blocks but a real
    // finish-step. Must surface as a normal done, never the empty-stream error.
    const chunks = await collect(
      streamChatCompletion(CFG, MSG, [], new AbortController().signal, {
        streamTextImpl: fakeStreamText([
          { type: 'finish-step', finishReason: 'stop', usage: { inputTokens: 1, outputTokens: 0, totalTokens: 1 } },
        ]) as never,
      }),
    );
    const done = chunks.find((c) => c.type === 'done')!;
    assert.equal(done.finishReason, 'stop');
    assert.equal(chunks.some((c) => isEmptyStreamError(c.error)), false);
  });
});

describe('streamChatCompletionWithRetry - empty-stream retry (E2E control flow)', () => {
  it('retries an empty stream and succeeds once real content arrives', async () => {
    let attempts = 0;
    const retries: number[] = [];
    const fakeStream = (async function* () {
      attempts++;
      if (attempts < 3) {
        yield { type: 'error', error: EMPTY_STREAM_ERROR } as StreamChunk;
        return;
      }
      yield { type: 'content_delta', text: 'ok' } as StreamChunk;
      yield { type: 'done', finishReason: 'stop' } as StreamChunk;
    }) as typeof streamChatCompletion;

    const chunks = await collect(
      streamChatCompletionWithRetry(CFG, MSG, [], new AbortController().signal, {
        maxRetries: 5,
        baseDelayMs: 1,
        maxDelayMs: 5,
        streamImpl: fakeStream,
        onRetry: (attempt) => retries.push(attempt),
      }),
    );

    // RED (pre-fix): the empty-stream error is not retryable, so the wrapper
    // surfaces it on attempt 1 (attempts === 1) and never reaches the content.
    // GREEN (post-fix): the first two empty streams are retried, the third
    // delivers content + done.
    assert.equal(attempts, 3, `expected 3 attempts, got ${attempts}`);
    assert.deepEqual(retries, [1, 2]);
    assert.equal(chunks.some((c) => c.type === 'content_delta'), true);
    assert.equal(chunks.at(-1)!.type, 'done');
    assert.equal(chunks.some((c) => isEmptyStreamError(c.error)), false);
  });

  it('surfaces the empty-stream error after exhausting retries', async () => {
    let attempts = 0;
    const fakeStream = (async function* () {
      attempts++;
      yield { type: 'error', error: EMPTY_STREAM_ERROR } as StreamChunk;
    }) as typeof streamChatCompletion;

    const chunks = await collect(
      streamChatCompletionWithRetry(CFG, MSG, [], new AbortController().signal, {
        maxRetries: 2,
        baseDelayMs: 1,
        maxDelayMs: 5,
        streamImpl: fakeStream,
      }),
    );

    assert.equal(attempts, 3, `expected 3 attempts (1 + 2 retries), got ${attempts}`);
    const last = chunks.at(-1)!;
    assert.equal(last.type, 'error');
    assert.equal(isEmptyStreamError(last.error), true);
  });
});
