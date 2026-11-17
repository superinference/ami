// ===========================================================================
// provider-fault-injection.test.ts — E2E for the runtime fault-injection seam
//
// Drives the REAL streamChatCompletion / streamChatCompletionWithRetry (no mock
// of the provider internals — only a fake AI-SDK streamText) with
// AMI_FAULT_INJECTION set, proving the injected fault is thrown on the live path
// and the REAL recovery machinery reacts:
//   • transient → the provider retry loop retries and then succeeds
//   • fatal     → a non-retryable terminal error is surfaced (no retry)
//
// RED/GREEN: if the `maybeThrowFault('provider')` hook is removed from
// provider.ts, the fake streamText runs on the FIRST attempt, no fault is
// thrown, no retry fires — the transient assertions (attempt count / onRetry)
// fail. That discriminates the fix from the old behavior.
// ===========================================================================
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';

import { streamChatCompletion, streamChatCompletionWithRetry } from '../src/provider';
import { FAULT_INJECTION_ENV, resetFaultInjection } from '../src/fault-injection';
import type { ProviderConfig, Message, StreamChunk } from '../src/types';

const CFG: ProviderConfig = {
  provider: 'openai',
  apiKey: 'sk-test1234567890',
  baseUrl: 'http://127.0.0.1:1/v1',
  model: 'gpt-4o',
};
const MSG: Message[] = [{ role: 'user', content: 'hi' }];

async function collect(gen: AsyncGenerator<StreamChunk>): Promise<StreamChunk[]> {
  const out: StreamChunk[] = [];
  for await (const c of gen) out.push(c);
  return out;
}

// Fake AI-SDK streamText that records how many times it is invoked and yields a
// single successful content + finish-step turn.
function countingStreamText(counter: { calls: number }): unknown {
  return () => {
    counter.calls++;
    return {
      stream: (async function* () {
        yield { type: 'text-delta', text: 'ok' };
        yield {
          type: 'finish-step',
          finishReason: 'stop',
          usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
        };
      })(),
    };
  };
}

afterEach(() => {
  delete process.env[FAULT_INJECTION_ENV];
  resetFaultInjection();
});

describe('provider fault injection — single-attempt streamChatCompletion', () => {
  it('a transient fault is thrown BEFORE the request and yielded as a retryable HTTP 503 error chunk', async () => {
    process.env[FAULT_INJECTION_ENV] = 'provider:transient';
    resetFaultInjection();
    const counter = { calls: 0 };
    const chunks = await collect(
      streamChatCompletion(CFG, MSG, [], new AbortController().signal, {
        streamTextImpl: countingStreamText(counter) as never,
      }),
    );
    const err = chunks.find((c) => c.type === 'error');
    assert.ok(err, 'expected an error chunk from the injected fault');
    assert.match(err!.error!, /HTTP 503/);
    // The fault fired before the real request — the fake streamText never ran.
    assert.equal(counter.calls, 0, 'streamText must not be called when the fault fires first');
  });

  it('a fatal fault surfaces as an HTTP 403 (non-retryable) error chunk', async () => {
    process.env[FAULT_INJECTION_ENV] = 'provider:fatal';
    resetFaultInjection();
    const counter = { calls: 0 };
    const chunks = await collect(
      streamChatCompletion(CFG, MSG, [], new AbortController().signal, {
        streamTextImpl: countingStreamText(counter) as never,
      }),
    );
    const err = chunks.find((c) => c.type === 'error');
    assert.ok(err && /HTTP 403/.test(err.error!), `got ${err?.error}`);
    assert.equal(counter.calls, 0);
  });

  it('no fault configured → the request runs normally (hot path unchanged)', async () => {
    const counter = { calls: 0 };
    const chunks = await collect(
      streamChatCompletion(CFG, MSG, [], new AbortController().signal, {
        streamTextImpl: countingStreamText(counter) as never,
      }),
    );
    assert.ok(chunks.some((c) => c.type === 'content_delta'), 'expected normal content');
    assert.equal(counter.calls, 1, 'streamText runs exactly once with no fault');
  });
});

describe('provider fault injection — live recovery via streamChatCompletionWithRetry', () => {
  it('transient fault → the retry loop backs off, retries, and succeeds', async () => {
    process.env[FAULT_INJECTION_ENV] = 'provider:transient:503:1';
    resetFaultInjection();
    const counter = { calls: 0 };
    const retries: number[] = [];
    // Real single-attempt generator, with a fake streamText threaded through so
    // no real network is touched. The fault-injection hook lives inside it.
    const realImpl = ((c, m, t, s, o) =>
      streamChatCompletion(c, m, t, s, { ...o, streamTextImpl: countingStreamText(counter) as never })) as typeof streamChatCompletion;

    const chunks = await collect(
      streamChatCompletionWithRetry(CFG, MSG, [], new AbortController().signal, {
        maxRetries: 3,
        baseDelayMs: 1,
        maxDelayMs: 2,
        streamImpl: realImpl,
        onRetry: (_a, _m, _d, status) => { if (status !== null) retries.push(status); },
      }),
    );

    assert.ok(chunks.some((c) => c.type === 'content_delta'), 'expected content after the retry recovered');
    assert.equal(counter.calls, 1, 'streamText runs once — only on the retry after the fault drained');
    assert.ok(retries.includes(503), `expected a 503 retry, got ${JSON.stringify(retries)}`);
  });

  it('fatal fault → terminal error surfaced, NO retry', async () => {
    process.env[FAULT_INJECTION_ENV] = 'provider:fatal:403:1';
    resetFaultInjection();
    const counter = { calls: 0 };
    let retried = false;
    const realImpl = ((c, m, t, s, o) =>
      streamChatCompletion(c, m, t, s, { ...o, streamTextImpl: countingStreamText(counter) as never })) as typeof streamChatCompletion;

    const chunks = await collect(
      streamChatCompletionWithRetry(CFG, MSG, [], new AbortController().signal, {
        maxRetries: 3,
        baseDelayMs: 1,
        maxDelayMs: 2,
        streamImpl: realImpl,
        onRetry: () => { retried = true; },
      }),
    );

    const err = chunks.find((c) => c.type === 'error');
    assert.ok(err && /HTTP 403/.test(err.error!), `expected a terminal 403, got ${err?.error}`);
    assert.equal(retried, false, 'a fatal fault must not be retried');
    assert.equal(counter.calls, 0, 'the real request is never reached on a fatal fault');
  });
});
