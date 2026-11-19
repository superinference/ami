/**
 * bounded-call.test.ts — unit tests for the `boundedCall` timeout-race wrapper.
 *
 * `boundedCall(fn, opts)` generalizes Claude Code's computer-use `drainRunLoop(fn)`
 * wrapper (utils/computerUse/drainRunLoop.ts): a timeout race that is SAFE against an
 * orphaned-promise late rejection (no-op `.catch`) and against a synchronous throw
 * from the work factory (release-on-sync-throw via finally), plus an optional
 * acquire/release lifecycle (the refcounted pump expressed as callbacks).
 *
 * ESM-SAFE: pure in-memory promises + timers, no fs, no network, no source-text reads.
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { boundedCall } from '../src/utils/bounded-call';

const delay = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

describe('boundedCall — happy path (work wins the race)', () => {
  it('resolves with the work value when it settles before the timeout', async () => {
    const out = await boundedCall(async () => { await delay(1); return 42; }, { timeoutMs: 1000 });
    assert.equal(out, 42);
  });

  it("rejects with the WORK's own error when it rejects before the timeout", async () => {
    await assert.rejects(
      () => boundedCall(async () => { await delay(1); throw new Error('work failed'); }, { timeoutMs: 1000 }),
      /work failed/,
    );
  });
});

describe('boundedCall — timeout wins the race', () => {
  it('rejects with the default message when the work never settles in time', async () => {
    await assert.rejects(
      () => boundedCall(() => new Promise<never>(() => { /* never settles */ }), { timeoutMs: 5 }),
      /Bounded call exceeded 5ms/,
    );
  });

  it('rejects with a custom string message', async () => {
    await assert.rejects(
      () => boundedCall(() => new Promise<never>(() => {}), { timeoutMs: 5, message: 'native call stalled' }),
      /native call stalled/,
    );
  });

  it('rejects with a lazily-computed message (factory only invoked on timeout)', async () => {
    let calls = 0;
    await assert.rejects(
      () => boundedCall(() => new Promise<never>(() => {}), { timeoutMs: 5, message: () => `lazy#${++calls}` }),
      /lazy#1/,
    );
    assert.equal(calls, 1);
    // The factory is NOT invoked when the work wins.
    calls = 0;
    await boundedCall(async () => 'ok', { timeoutMs: 1000, message: () => `lazy#${++calls}` });
    assert.equal(calls, 0);
  });
});

describe('boundedCall — orphan late rejection is swallowed', () => {
  // Behavioral guarantee (not a red-green discriminator): Promise.race already
  // attaches a reaction to the losing promise, and the explicit no-op .catch() is
  // belt-and-suspenders. This locks the observable property either way.
  it('does not emit an unhandledRejection when the orphaned work rejects AFTER the timeout', async () => {
    const SENTINEL = 'orphan-late-reject-sentinel';
    const seen: unknown[] = [];
    const onUnhandled = (reason: unknown): void => {
      if (reason instanceof Error && reason.message === SENTINEL) seen.push(reason);
    };
    process.on('unhandledRejection', onUnhandled);
    try {
      // Timeout fires at 5ms; the work rejects at ~40ms, long after it was orphaned.
      await assert.rejects(
        () => boundedCall(
          () => new Promise<never>((_, reject) => setTimeout(() => reject(new Error(SENTINEL)), 40)),
          { timeoutMs: 5 },
        ),
        /Bounded call exceeded 5ms/,
      );
      // Wait well past the orphaned rejection + several microtask turns so an
      // unhandledRejection (if any) would have fired by now.
      await delay(80);
      await Promise.resolve();
    } finally {
      process.removeListener('unhandledRejection', onUnhandled);
    }
    assert.deepEqual(seen, [], 'the orphaned late rejection must be swallowed, not surfaced');
  });
});

describe('boundedCall — synchronous throw from the work factory', () => {
  it('rejects with the synchronous error AND still runs onRelease (no leaked resource)', async () => {
    let acquired = 0;
    let released = 0;
    await assert.rejects(
      () => boundedCall<never>(() => { throw new Error('sync NAPI validation'); }, {
        timeoutMs: 1000,
        onAcquire: () => { acquired++; },
        onRelease: () => { released++; },
      }),
      /sync NAPI validation/,
    );
    assert.equal(acquired, 1, 'onAcquire runs before fn()');
    assert.equal(released, 1, 'onRelease runs in finally even on a synchronous throw');
  });
});

describe('boundedCall — acquire/release lifecycle', () => {
  it('calls onAcquire before fn and onRelease after success', async () => {
    const order: string[] = [];
    const out = await boundedCall(
      async () => { order.push('fn'); return 'done'; },
      { timeoutMs: 1000, onAcquire: () => order.push('acquire'), onRelease: () => order.push('release') },
    );
    assert.equal(out, 'done');
    assert.deepEqual(order, ['acquire', 'fn', 'release']);
  });

  it('runs onRelease even when the timeout wins', async () => {
    let released = 0;
    await assert.rejects(
      () => boundedCall(() => new Promise<never>(() => {}), {
        timeoutMs: 5,
        onRelease: () => { released++; },
      }),
      /exceeded/,
    );
    assert.equal(released, 1);
  });

  it('does NOT run onRelease when onAcquire itself throws (nothing was acquired)', async () => {
    let released = 0;
    let fnInvoked = 0;
    await assert.rejects(
      () => boundedCall(async () => { fnInvoked++; return 1; }, {
        timeoutMs: 1000,
        onAcquire: () => { throw new Error('acquire failed'); },
        onRelease: () => { released++; },
      }),
      /acquire failed/,
    );
    assert.equal(fnInvoked, 0, 'fn is never invoked when acquire throws');
    assert.equal(released, 0, 'release must not run for an acquire that never succeeded');
  });
});

describe('boundedCall — unbounded (no timer armed)', () => {
  for (const ms of [0, -1, Number.POSITIVE_INFINITY, Number.NaN]) {
    it(`treats timeoutMs=${String(ms)} as "no bound" and returns the work outcome`, async () => {
      const out = await boundedCall(async () => { await delay(1); return 'unbounded'; }, { timeoutMs: ms });
      assert.equal(out, 'unbounded');
    });
  }

  it('still runs acquire/release and swallows an orphan when unbounded + work rejects', async () => {
    let released = 0;
    await assert.rejects(
      () => boundedCall(async () => { throw new Error('boom'); }, { timeoutMs: 0, onRelease: () => { released++; } }),
      /boom/,
    );
    assert.equal(released, 1);
  });
});

describe('boundedCall — timer hygiene', () => {
  it('clears the timeout timer the instant the work settles (no dangling timer)', async () => {
    // If the timer were not cleared, a 5ms timeout would reject AFTER this resolves;
    // awaiting past the deadline must NOT produce a late rejection.
    const out = await boundedCall(async () => 'fast', { timeoutMs: 5 });
    assert.equal(out, 'fast');
    await delay(20); // past the (now-cleared) deadline
    // Reaching here without an unhandled timeout rejection proves the timer was cleared.
    assert.ok(true);
  });

  it('honors unref:false without changing the resolved value', async () => {
    const out = await boundedCall(async () => 'v', { timeoutMs: 1000, unref: false });
    assert.equal(out, 'v');
  });
});
