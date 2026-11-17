// ===========================================================================
// Unit U297 — canonical core/core/src/utils/sleep.ts
//
// Consolidates AMI's previously-duplicated timing primitives into ONE core-layer
// home (parity with Claude Code's utils/sleep.ts):
//   - sleep(ms, signal?, opts?)  — abort-responsive delay, selectable abort
//   - withTimeout(promise, ms, message) — race-against-deadline guard
//
// provider.ts (sleep), common/src/abort-sleep.ts (sleep), and
// common/src/provision/clock.ts (withTimeout) all now re-export from here, so the
// existing provider/abort-sleep/provision integration tests exercise THIS impl.
// This file locks the canonical module's own contract + every branch.
// ===========================================================================
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { getEventListeners } from 'node:events';

import { sleep, withTimeout } from '../src/utils/sleep';
import type { SleepOptions } from '../src/utils/sleep';

describe('utils/sleep — sleep()', () => {
  it('resolves after the delay when no signal is given', async () => {
    const start = Date.now();
    await sleep(20);
    assert.ok(Date.now() - start >= 18);
  });

  it('wakes EARLY (silently resolves) when the signal aborts mid-sleep', async () => {
    const ac = new AbortController();
    const start = Date.now();
    setTimeout(() => ac.abort(), 10);
    await assert.doesNotReject(sleep(10_000, ac.signal));
    assert.ok(Date.now() - start < 500);
  });

  it('resolves immediately (silently) when the signal is already aborted', async () => {
    const ac = new AbortController();
    ac.abort();
    const start = Date.now();
    await assert.doesNotReject(sleep(10_000, ac.signal));
    assert.ok(Date.now() - start < 100);
  });

  it('throwOnAbort: rejects with a recognized DOMException AbortError (mid-sleep)', async () => {
    const ac = new AbortController();
    setTimeout(() => ac.abort(), 5);
    await assert.rejects(
      sleep(10_000, ac.signal, { throwOnAbort: true }),
      (err: unknown) =>
        err instanceof DOMException && err.name === 'AbortError' && /aborted/i.test(err.message),
    );
  });

  it('throwOnAbort: rejects immediately when already aborted', async () => {
    const ac = new AbortController();
    ac.abort();
    await assert.rejects(
      sleep(10_000, ac.signal, { throwOnAbort: true }),
      (err: unknown) => err instanceof DOMException && err.name === 'AbortError',
    );
  });

  it('abortError: rejects with the supplied error class (implies throwOnAbort)', async () => {
    const ac = new AbortController();
    setTimeout(() => ac.abort(), 5);
    class MyAbort extends Error {}
    await assert.rejects(
      sleep(10_000, ac.signal, { abortError: () => new MyAbort('custom') }),
      (e: unknown) => e instanceof MyAbort && (e as Error).message === 'custom',
    );
  });

  it('abortError: rejects immediately with the supplied class when already aborted', async () => {
    const ac = new AbortController();
    ac.abort();
    class MyAbort extends Error {}
    await assert.rejects(
      sleep(10_000, ac.signal, { abortError: () => new MyAbort('eager') }),
      (e: unknown) => e instanceof MyAbort,
    );
  });

  it('detaches the abort listener after the timer fires (no leak)', async () => {
    const ac = new AbortController();
    await sleep(5, ac.signal);
    assert.equal(getEventListeners(ac.signal, 'abort').length, 0);
  });

  it('clears the listener on the abort path too', async () => {
    const ac = new AbortController();
    setTimeout(() => ac.abort(), 5);
    await sleep(50, ac.signal);
    assert.equal(getEventListeners(ac.signal, 'abort').length, 0);
  });

  it('unref: resolves without keeping a reference to the process (no throw)', async () => {
    const opts: SleepOptions = { unref: true };
    await assert.doesNotReject(sleep(5, undefined, opts));
  });
});

describe('utils/sleep — withTimeout()', () => {
  it('resolves with the promise value when it settles in time', async () => {
    assert.equal(await withTimeout(Promise.resolve(42), 1000, 'nope'), 42);
  });

  it('propagates the inner rejection (not the timeout) when the promise rejects first', async () => {
    await assert.rejects(
      () => withTimeout(Promise.reject(new Error('inner boom')), 1000, 'timeout msg'),
      /inner boom/,
    );
  });

  it('rejects with Error(message) when the promise does not settle in time', async () => {
    await assert.rejects(
      () => withTimeout(new Promise<never>(() => {}), 10, 'bounded wait expired'),
      /bounded wait expired/,
    );
  });

  it('treats a non-positive / non-finite ms as unbounded (returns the promise as-is)', () => {
    const p = Promise.resolve('x');
    assert.equal(withTimeout(p, 0, 'm'), p);
    assert.equal(withTimeout(p, -5, 'm'), p);
    assert.equal(withTimeout(p, Number.POSITIVE_INFINITY, 'm'), p);
  });

  it('clears the timeout timer once the promise settles (no dangling timer)', async () => {
    // If the timer were not cleared+unref'd, the long timeout would keep the
    // event loop alive; resolving fast and returning proves it is cleared.
    const start = Date.now();
    await withTimeout(sleep(5), 60_000, 'never');
    assert.ok(Date.now() - start < 1000);
  });
});
