// ===========================================================================
// provider-u020-w82.test.ts
//
// Parity unit U020 (wave 82) — selectable abort semantics for the shared backoff
// `sleep` (parity: the reference's utils/sleep.ts). By DEFAULT an abort SILENTLY
// RESOLVES (caller checks signal.aborted after the await); `throwOnAbort` rejects
// so a surrounding retry loop is cancelled; `abortError` rejects with a specific
// error class (implies throwOnAbort). The default rejection is a DOMException
// 'AbortError', which isAbort() recognizes.
//
// RED on revert: the OLD sleep ALWAYS rejected on abort, so the
// "silent-resolve default" cases below would reject (throw) and fail; and the
// sleepWithRetryHeartbeat regression proves the heartbeat still rejects (it now
// opts into throwOnAbort).
// ===========================================================================
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { sleep, sleepWithRetryHeartbeat } from '../src/provider';
import { isAbort } from '../src/error-classifier';

const abortedSignal = (): AbortSignal => {
  const ac = new AbortController();
  ac.abort();
  return ac.signal;
};

describe('U020/w82 — sleep selectable abort semantics', () => {
  it('default: an ALREADY-aborted signal resolves silently (no throw)', async () => {
    await assert.doesNotReject(sleep(10_000, abortedSignal()));
  });

  it('default: a mid-sleep abort resolves silently (no throw)', async () => {
    const ac = new AbortController();
    const p = sleep(10_000, ac.signal);
    setTimeout(() => ac.abort(), 5);
    await assert.doesNotReject(p);
  });

  it('throwOnAbort: an already-aborted signal rejects with a recognized AbortError', async () => {
    await assert.rejects(
      sleep(10_000, abortedSignal(), { throwOnAbort: true }),
      (err: unknown) => {
        assert.equal(isAbort(err), true, 'default rejection is a recognized abort error');
        return true;
      },
    );
  });

  it('throwOnAbort: a mid-sleep abort rejects', async () => {
    const ac = new AbortController();
    const p = sleep(10_000, ac.signal, { throwOnAbort: true });
    setTimeout(() => ac.abort(), 5);
    await assert.rejects(p, (err: unknown) => isAbort(err));
  });

  it('abortError: rejects with the supplied error class (implies throwOnAbort) on an already-aborted signal', async () => {
    class MyAbort extends Error {}
    await assert.rejects(
      sleep(10_000, abortedSignal(), { abortError: () => new MyAbort('boom') }),
      (err: unknown) => err instanceof MyAbort && (err as Error).message === 'boom',
    );
  });

  it('abortError: rejects with the supplied error class on a mid-sleep abort', async () => {
    class MyAbort extends Error {}
    const ac = new AbortController();
    const p = sleep(10_000, ac.signal, { abortError: () => new MyAbort('late') });
    setTimeout(() => ac.abort(), 5);
    await assert.rejects(p, (err: unknown) => err instanceof MyAbort);
  });

  it('completes normally (resolves) when the timer elapses without an abort', async () => {
    await assert.doesNotReject(sleep(5));
  });

  it('unref: option is honored without affecting resolution', async () => {
    await assert.doesNotReject(sleep(5, undefined, { unref: true }));
  });

  // Regression / discriminator: the retry-backoff heartbeat MUST still reject on
  // abort (it opts into throwOnAbort). This locks the one real caller's contract
  // while the base sleep default flips to silent-resolve.
  it('sleepWithRetryHeartbeat still rejects on abort (opts into throwOnAbort)', async () => {
    const ac = new AbortController();
    const p = sleepWithRetryHeartbeat(10_000, ac.signal, () => {}, 10);
    setTimeout(() => ac.abort(), 5);
    await assert.rejects(p, (err: unknown) => isAbort(err));
  });
});
