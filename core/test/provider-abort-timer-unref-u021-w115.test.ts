import { describe, it, mock, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { armUnrefTimer } from '../src/utils/abort';
import {
  runNonStreamingFallback,
  streamChatCompletionWithRetry,
  streamChatCompletion,
} from '../src/provider';
import type { ProviderConfig, Message, StreamChunk } from '../src/types';

// ---------------------------------------------------------------------------
// U021 (w115) — abort/deadline timers must be `unref`'d so a pending timeout
// never keeps the Node event loop (or the process) alive on a path where its
// `clearTimeout` is skipped. Parity with the reference's `timer.unref?.()` on
// every abort-timeout timer (utils/combinedAbortSignal.ts L35).
//
// RED on revert:
//   - revert `timer.unref?.()` in armUnrefTimer  -> the helper test fails.
//   - revert any provider `armUnrefTimer(...)` back to `setTimeout(...)`
//     -> the corresponding "captured timers are unref'd" assertion fails
//      (the plain timer reports hasRef() === true).
// ---------------------------------------------------------------------------

type TimerLike = { hasRef?: () => boolean };

/** Record every timer `setTimeout` returns during `fn`, delegating to the real impl. */
async function captureTimers(fn: () => Promise<void>): Promise<TimerLike[]> {
  const captured: TimerLike[] = [];
  const realSetTimeout = globalThis.setTimeout;
  mock.method(globalThis, 'setTimeout', ((handler: (...a: unknown[]) => void, ms?: number, ...rest: unknown[]) => {
    const t = (realSetTimeout as (...a: unknown[]) => unknown)(handler, ms, ...rest) as TimerLike;
    captured.push(t);
    return t;
  }) as typeof setTimeout);
  try {
    await fn();
  } finally {
    mock.restoreAll();
  }
  return captured.filter((t) => typeof t === 'object' && t !== null && typeof t.hasRef === 'function');
}

const CFG: ProviderConfig = { baseUrl: 'http://127.0.0.1:1', apiKey: 'test-key', model: 'test-model' };
const MSG: Message[] = [{ role: 'user', content: 'hi' }];

function fakeGenerateText(result: Record<string, unknown>) {
  return (async () => result as never) as never;
}

async function drain(gen: AsyncGenerator<StreamChunk>): Promise<void> {
  for await (const _ of gen) void _;
}

afterEach(() => mock.restoreAll());

describe('armUnrefTimer', () => {
  it('returns a timer that does NOT keep the event loop alive (unref applied)', () => {
    let fired = false;
    const t = armUnrefTimer(() => { fired = true; }, 60_000) as unknown as TimerLike & { [Symbol.dispose]?: () => void };
    try {
      assert.equal(typeof t.hasRef, 'function');
      // The whole point: a pending timer must not be ref'd (would delay exit).
      assert.equal(t.hasRef!(), false);
      assert.equal(fired, false);
    } finally {
      clearTimeout(t as unknown as ReturnType<typeof setTimeout>);
    }
  });

  it('still fires its callback when it is allowed to elapse', async () => {
    const fired = await new Promise<boolean>((resolve) => {
      armUnrefTimer(() => resolve(true), 1);
    });
    assert.equal(fired, true);
  });

  it('is cleared cleanly with clearTimeout before firing', async () => {
    let fired = false;
    const t = armUnrefTimer(() => { fired = true; }, 5);
    clearTimeout(t);
    await new Promise((r) => setTimeout(r, 20));
    assert.equal(fired, false);
  });
});

describe('provider abort timers are unref (wired through armUnrefTimer)', () => {
  it('runNonStreamingFallback arms its per-attempt deadline timer unref', async () => {
    const timers = await captureTimers(async () => {
      await drain(runNonStreamingFallback(CFG, MSG, [], new AbortController().signal, {
        generateTextImpl: fakeGenerateText({ text: 'ok', toolCalls: [], finishReason: 'stop', usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } }),
      }));
    });
    assert.ok(timers.length >= 1, 'the non-streaming fallback must arm at least one deadline timer');
    for (const t of timers) {
      assert.equal(t.hasRef!(), false, 'every fallback timer must be unref (not keep the loop alive)');
    }
  });

  it('streamChatCompletionWithRetry arms request + idle watchdog timers unref', async () => {
    const okStream = (async function* () {
      yield { type: 'content_delta', text: 'hello' } as StreamChunk;
      yield { type: 'done' } as StreamChunk;
    }) as typeof streamChatCompletion;
    const timers = await captureTimers(async () => {
      await drain(streamChatCompletionWithRetry(CFG, MSG, [], new AbortController().signal, {
        maxRetries: 0,
        requestTimeoutMs: 60_000,
        idleTimeoutMs: 50_000,
        streamImpl: okStream,
      }));
    });
    assert.ok(timers.length >= 1, 'the retry wrapper must arm the request/idle timers');
    for (const t of timers) {
      assert.equal(t.hasRef!(), false, 'every request/idle timer must be unref');
    }
  });
});
