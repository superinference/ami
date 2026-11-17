import { test } from 'node:test';
import assert from 'node:assert/strict';
import { getMaxListeners } from 'node:events';
import { createCombinedAbortSignal } from '../src/utils/combined-abort-signal';

const tick = (ms = 0): Promise<void> => new Promise(r => setTimeout(r, ms));

test('combines the primary signal: aborts (with its reason) when it aborts', () => {
  const a = new AbortController();
  const { signal, cleanup } = createCombinedAbortSignal(a.signal);
  assert.equal(signal.aborted, false);
  a.abort(new Error('boom'));
  assert.equal(signal.aborted, true);
  assert.equal((signal.reason as Error).message, 'boom');
  cleanup();
});

test('combines signalB: aborts when the second source aborts', () => {
  const a = new AbortController();
  const b = new AbortController();
  const { signal, cleanup } = createCombinedAbortSignal(a.signal, { signalB: b.signal });
  assert.equal(signal.aborted, false);
  b.abort(new Error('second'));
  assert.equal(signal.aborted, true);
  assert.equal((signal.reason as Error).message, 'second');
  cleanup();
});

test('timeoutMs aborts the combined signal after the delay', async () => {
  const { signal, cleanup } = createCombinedAbortSignal(undefined, { timeoutMs: 10 });
  assert.equal(signal.aborted, false);
  await tick(30);
  assert.equal(signal.aborted, true);
  assert.equal((signal.reason as Error).message, 'timed out');
  cleanup();
});

test('cleanup() clears the timer so the signal never aborts afterward', async () => {
  const { signal, cleanup } = createCombinedAbortSignal(undefined, { timeoutMs: 10 });
  cleanup();
  await tick(30);
  assert.equal(signal.aborted, false, 'a cleaned-up timeout must not fire');
});

test('cleanup() detaches source listeners (later source abort is a no-op)', async () => {
  const a = new AbortController();
  const b = new AbortController();
  const { signal, cleanup } = createCombinedAbortSignal(a.signal, { signalB: b.signal });
  cleanup();
  a.abort(new Error('late'));
  b.abort(new Error('late-b'));
  await tick(0);
  assert.equal(signal.aborted, false, 'a cleaned-up combined signal must ignore source aborts');
});

test('cleanup() is idempotent (double-call does not throw)', () => {
  const { cleanup } = createCombinedAbortSignal(undefined, { timeoutMs: 1000 });
  assert.doesNotThrow(() => {
    cleanup();
    cleanup();
  });
});

test('already-aborted primary signal short-circuits with its reason + no-op cleanup', () => {
  const a = new AbortController();
  a.abort(new Error('pre'));
  const { signal, cleanup } = createCombinedAbortSignal(a.signal, { timeoutMs: 10 });
  assert.equal(signal.aborted, true);
  assert.equal((signal.reason as Error).message, 'pre');
  // cleanup is the no-op variant; must not throw.
  assert.doesNotThrow(cleanup);
});

test('already-aborted signalB short-circuits with its reason', () => {
  const a = new AbortController();
  const b = new AbortController();
  b.abort(new Error('pre-b'));
  const { signal } = createCombinedAbortSignal(a.signal, { signalB: b.signal });
  assert.equal(signal.aborted, true);
  assert.equal((signal.reason as Error).message, 'pre-b');
});

test('undefined primary signal with no options yields a never-aborting signal', () => {
  const { signal, cleanup } = createCombinedAbortSignal(undefined);
  assert.equal(signal.aborted, false);
  cleanup();
  assert.equal(signal.aborted, false);
});

test('raises the combined signal listener cap above Node default (10)', () => {
  const { signal, cleanup } = createCombinedAbortSignal(undefined, { timeoutMs: 1000 });
  assert.equal(getMaxListeners(signal), 50);
  cleanup();
});

test('maxListeners option is forwarded to the combined + source signals', () => {
  const a = new AbortController();
  const { signal, cleanup } = createCombinedAbortSignal(a.signal, { maxListeners: 7 });
  assert.equal(getMaxListeners(signal), 7);
  assert.equal(getMaxListeners(a.signal), 7);
  cleanup();
});

test('the timeout timer is unref\'d (does not keep the event loop alive)', () => {
  // A smoke test: creating a long combined-timeout and leaving it must not throw
  // and the returned cleanup must be callable — unref prevents a hung process.
  const { cleanup } = createCombinedAbortSignal(undefined, { timeoutMs: 60_000 });
  assert.doesNotThrow(cleanup);
});

test('aborts a pending race the way a fetch deadline would', async () => {
  // Integration-style: a hanging promise is cut off by the combined timeout,
  // exactly the leak-free deadline behavior web-fetch/provider rely on.
  const { signal, cleanup } = createCombinedAbortSignal(undefined, { timeoutMs: 10 });
  const hang = new Promise<string>(() => {});
  const aborted = new Promise<string>((resolve) => {
    signal.addEventListener('abort', () => resolve('aborted'), { once: true });
  });
  const result = await Promise.race([hang, aborted]);
  assert.equal(result, 'aborted');
  cleanup();
});
