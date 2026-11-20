import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  streamChatCompletionWithRetry,
  runNonStreamingFallback,
  generateTextToStreamAdapter,
  adjustParamsForNonStreaming,
  nonStreamingFallbackEnabled,
  nonstreamingFallbackTimeoutMs,
  MAX_NON_STREAMING_TOKENS,
  isEmptyStreamError,
  EMPTY_STREAM_ERROR,
  streamChatCompletion,
} from '../src/provider';
import type { ProviderConfig, Message, StreamChunk } from '../src/types';

// ---------------------------------------------------------------------------
// U020 (w89) — streaming -> NON-STREAMING request fallback.
//
// Parity with the reference's executeNonStreamingRequest: a gateway/proxy that
// breaks SSE streaming (200 + non-SSE body => EMPTY_STREAM_ERROR, or a 404 on the
// streaming endpoint) but serves a plain request fine is recovered by ONE bounded,
// token-capped non-streaming request (AI SDK generateText) whose result is replayed
// as the normal stream. Off by default; opt in via config.nonStreamingFallback or
// SUPERINFERENCE_NONSTREAMING_FALLBACK / CLAUDE_CODE_NONSTREAMING_FALLBACK.
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

// A fake AI-SDK generateText returning a completed (non-streamed) result.
type GenResult = Record<string, unknown>;
function fakeGenerateText(result: GenResult, onCall?: (params: Record<string, unknown>) => void) {
  return (async (params: Record<string, unknown>) => {
    onCall?.(params);
    return result as never;
  }) as never;
}

const ENV_KEYS = [
  'SUPERINFERENCE_NONSTREAMING_FALLBACK',
  'CLAUDE_CODE_NONSTREAMING_FALLBACK',
  'SUPERINFERENCE_NONSTREAMING_FALLBACK_TIMEOUT_MS',
];
afterEach(() => {
  for (const k of ENV_KEYS) delete process.env[k];
});

describe('nonStreamingFallbackEnabled', () => {
  it('off by default (no config, no env)', () => {
    assert.equal(nonStreamingFallbackEnabled(CFG, {}), false);
  });

  it('an explicit config boolean WINS over the env knob', () => {
    assert.equal(nonStreamingFallbackEnabled({ ...CFG, nonStreamingFallback: false }, { SUPERINFERENCE_NONSTREAMING_FALLBACK: '1' }), false);
    assert.equal(nonStreamingFallbackEnabled({ ...CFG, nonStreamingFallback: true }, {}), true);
  });

  it('honors either env knob when config is unset', () => {
    assert.equal(nonStreamingFallbackEnabled(CFG, { SUPERINFERENCE_NONSTREAMING_FALLBACK: 'yes' }), true);
    assert.equal(nonStreamingFallbackEnabled(CFG, { CLAUDE_CODE_NONSTREAMING_FALLBACK: 'on' }), true);
    assert.equal(nonStreamingFallbackEnabled(CFG, { SUPERINFERENCE_NONSTREAMING_FALLBACK: 'off' }), false);
  });
});

describe('adjustParamsForNonStreaming', () => {
  it('caps a too-large budget to MAX_NON_STREAMING_TOKENS', () => {
    const out = adjustParamsForNonStreaming({ ...CFG, maxTokens: 999_999 });
    assert.equal(out.maxTokens, MAX_NON_STREAMING_TOKENS);
  });
  it('defaults an absent budget to the cap', () => {
    const out = adjustParamsForNonStreaming({ ...CFG });
    assert.equal(out.maxTokens, MAX_NON_STREAMING_TOKENS);
  });
  it('leaves a smaller budget untouched and does not mutate the input', () => {
    const input = { ...CFG, maxTokens: 10 };
    const out = adjustParamsForNonStreaming(input);
    assert.equal(out.maxTokens, 10);
    assert.notEqual(out, input);
    assert.equal(input.maxTokens, 10);
  });
});

describe('nonstreamingFallbackTimeoutMs', () => {
  it('120s for a remote endpoint', () => {
    assert.equal(nonstreamingFallbackTimeoutMs({ ...CFG, baseUrl: 'https://api.anthropic.com' }, {}), 120_000);
  });
  it('300s for a localhost endpoint', () => {
    assert.equal(nonstreamingFallbackTimeoutMs({ ...CFG, baseUrl: 'http://localhost:11434/v1' }, {}), 300_000);
    assert.equal(nonstreamingFallbackTimeoutMs({ ...CFG, baseUrl: 'http://127.0.0.1:8080' }, {}), 300_000);
  });
  it('honors a positive env override', () => {
    assert.equal(
      nonstreamingFallbackTimeoutMs({ ...CFG, baseUrl: 'https://api.anthropic.com' }, { SUPERINFERENCE_NONSTREAMING_FALLBACK_TIMEOUT_MS: '5000' }),
      5000,
    );
  });
  it('ignores a non-positive/invalid override', () => {
    assert.equal(
      nonstreamingFallbackTimeoutMs({ ...CFG, baseUrl: 'https://api.anthropic.com' }, { SUPERINFERENCE_NONSTREAMING_FALLBACK_TIMEOUT_MS: '-1' }),
      120_000,
    );
    assert.equal(
      nonstreamingFallbackTimeoutMs({ ...CFG, baseUrl: 'https://api.anthropic.com' }, { SUPERINFERENCE_NONSTREAMING_FALLBACK_TIMEOUT_MS: 'nope' }),
      120_000,
    );
  });
});

describe('generateTextToStreamAdapter', () => {
  it('replays a completed result as reasoning -> text -> tool-calls -> finish-step and strips onError', async () => {
    let received: Record<string, unknown> | undefined;
    const adapter = generateTextToStreamAdapter(
      fakeGenerateText(
        {
          reasoningText: 'because',
          text: 'hello world',
          toolCalls: [{ toolCallId: 'tc1', toolName: 'bash', input: { command: 'ls' } }],
          finishReason: 'tool-calls',
          totalUsage: { inputTokens: 5, outputTokens: 3, totalTokens: 8 },
        },
        (p) => { received = p; },
      ),
    );
    const fake = adapter({ messages: [], onError: () => {}, maxOutputTokens: 7 } as never);
    const parts: Array<Record<string, unknown>> = [];
    for await (const p of (fake as unknown as { stream: AsyncIterable<Record<string, unknown>> }).stream) parts.push(p);

    assert.deepEqual(parts.map((p) => p.type), ['reasoning-delta', 'text-delta', 'tool-call', 'finish-step']);
    assert.equal(parts[1].text, 'hello world');
    assert.equal((parts[2] as { toolName: string }).toolName, 'bash');
    assert.equal((parts[3] as { finishReason: string }).finishReason, 'tool-calls');
    // onError (a streamText-only callback) is NOT forwarded to generateText.
    assert.ok(received && !('onError' in received), 'onError must be stripped');
    assert.equal((received as { maxOutputTokens?: number }).maxOutputTokens, 7);
  });

  it('propagates a thrown generateText error out of the replay', async () => {
    const adapter = generateTextToStreamAdapter(
      (async () => { throw new Error('boom-404'); }) as never,
    );
    const fake = adapter({ messages: [] } as never);
    await assert.rejects(async () => {
      for await (const _ of (fake as unknown as { stream: AsyncIterable<unknown> }).stream) { void _; }
    }, /boom-404/);
  });
});

describe('runNonStreamingFallback', () => {
  it('yields the non-streaming result as normal stream chunks via streamChatCompletion', async () => {
    let seenMaxOutput: number | undefined;
    const chunks = await collect(
      runNonStreamingFallback({ ...CFG, maxTokens: 999_999 }, MSG, [], new AbortController().signal, {
        generateTextImpl: fakeGenerateText(
          { text: 'recovered', toolCalls: [], finishReason: 'stop', usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } },
          (p) => { seenMaxOutput = (p as { maxOutputTokens?: number }).maxOutputTokens; },
        ),
      }),
    );
    assert.equal(chunks.some((c) => c.type === 'content_delta' && c.text === 'recovered'), true);
    assert.equal(chunks.at(-1)!.type, 'done');
    // The token budget reached generateText is bounded by the non-streaming cap.
    assert.ok(seenMaxOutput !== undefined && seenMaxOutput <= MAX_NON_STREAMING_TOKENS, `budget ${seenMaxOutput} must be <= cap`);
  });
});

describe('streamChatCompletionWithRetry - non-streaming fallback (E2E)', () => {
  // A streaming impl that always fails with the given error chunk.
  const failing = (error: string): typeof streamChatCompletion =>
    (async function* () { yield { type: 'error', error } as StreamChunk; }) as typeof streamChatCompletion;

  it('recovers an EMPTY_STREAM failure via the non-streaming fallback when enabled', async () => {
    let attempts = 0;
    const fakeStream = (async function* () { attempts++; yield { type: 'error', error: EMPTY_STREAM_ERROR } as StreamChunk; }) as typeof streamChatCompletion;
    const chunks = await collect(
      streamChatCompletionWithRetry({ ...CFG, nonStreamingFallback: true }, MSG, [], new AbortController().signal, {
        maxRetries: 2,
        baseDelayMs: 1,
        maxDelayMs: 2,
        streamImpl: fakeStream,
        generateTextImpl: fakeGenerateText({ text: 'via-nonstream', toolCalls: [], finishReason: 'stop', usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } }),
      }),
    );
    // Streaming is retried to exhaustion (1 + 2) before the single fallback runs.
    assert.equal(attempts, 3);
    assert.equal(chunks.some((c) => c.type === 'content_delta' && c.text === 'via-nonstream'), true);
    assert.equal(chunks.at(-1)!.type, 'done');
    assert.equal(chunks.some((c) => isEmptyStreamError(c.error)), false);
  });

  it('recovers a 404-on-stream failure immediately (non-retryable) via the fallback', async () => {
    let attempts = 0;
    const fakeStream = (async function* () { attempts++; yield { type: 'error', error: 'HTTP 404: no streaming here' } as StreamChunk; }) as typeof streamChatCompletion;
    const chunks = await collect(
      streamChatCompletionWithRetry({ ...CFG, nonStreamingFallback: true }, MSG, [], new AbortController().signal, {
        maxRetries: 3,
        baseDelayMs: 1,
        streamImpl: fakeStream,
        generateTextImpl: fakeGenerateText({ text: 'ns-ok', toolCalls: [], finishReason: 'stop', usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } }),
      }),
    );
    // 404 is not retryable, so no streaming retries — the fallback runs at once.
    assert.equal(attempts, 1);
    assert.equal(chunks.some((c) => c.type === 'content_delta' && c.text === 'ns-ok'), true);
  });

  it('is OFF by default — surfaces the EMPTY_STREAM error without a fallback (regression lock + RED discriminator)', async () => {
    const chunks = await collect(
      streamChatCompletionWithRetry(CFG, MSG, [], new AbortController().signal, {
        maxRetries: 1,
        baseDelayMs: 1,
        streamImpl: failing(EMPTY_STREAM_ERROR),
        // generateTextImpl present but MUST NOT run because the fallback is disabled.
        generateTextImpl: fakeGenerateText({ text: 'should-not-appear', toolCalls: [], finishReason: 'stop', usage: {} }),
      }),
    );
    const last = chunks.at(-1)!;
    assert.equal(last.type, 'error');
    assert.ok((last.error ?? '').includes(EMPTY_STREAM_ERROR));
    assert.equal(chunks.some((c) => c.type === 'content_delta'), false);
  });

  it('surfaces the ORIGINAL error when the fallback itself fails', async () => {
    const chunks = await collect(
      streamChatCompletionWithRetry({ ...CFG, nonStreamingFallback: true }, MSG, [], new AbortController().signal, {
        maxRetries: 1,
        baseDelayMs: 1,
        streamImpl: failing(EMPTY_STREAM_ERROR),
        generateTextImpl: (async () => { throw new Error('also-broken'); }) as never,
      }),
    );
    const last = chunks.at(-1)!;
    assert.equal(last.type, 'error');
    // The fallback's own failure does not mask the original empty-stream error.
    assert.ok((last.error ?? '').includes(EMPTY_STREAM_ERROR), `expected original error, got: ${last.error}`);
    assert.equal(chunks.some((c) => c.type === 'content_delta'), false);
  });
});
