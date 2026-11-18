// ===========================================================================
// Unit U283 — `unref` option on the shared sleep + its best-effort consumers.
//
// U297 added `sleep(ms, signal?, { unref })` to core/core/src/utils/sleep.ts
// (parity with Claude Code's utils/sleep.ts L50-L52). This unit (a) locks that
// `{ unref: true }` ACTUALLY calls `timer.unref()` (not just that it doesn't
// throw), and (b) WIRES that option into the real best-effort consumers that must
// never hold the process open at shutdown: the analytics graceful-close drain
// (`raceWithTimeout` / `closeWithGrace`) and the inter-batch OTLP delay — the
// AMI analog of Claude's `sleep(500, undefined, { unref: true })` grace race in
// bridgeMain.ts L2064-2068.
//
// RED/GREEN: these assert `timer.unref()` is invoked. Revert the analytics
// default back to the raw `new Promise(r => setTimeout(r, m))` and the analytics
// cases fail (no unref call); revert sleep.ts's `if (opts?.unref) timer.unref()`
// and the helper case fails.
// ===========================================================================
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import { sleep } from '../src/utils/sleep';
import { raceWithTimeout, AnalyticsTracker } from '../src/analytics';

// Spy on the global timer so we can observe whether the sleep's backing timer is
// unref'd. We wrap the REAL setTimeout (so timers still fire) and record every ms
// for which `.unref()` is called on the returned handle.
type TimerHandle = ReturnType<typeof setTimeout>;
const origSetTimeout = global.setTimeout;
let unrefMs: number[] = [];

function installTimerSpy(): void {
  unrefMs = [];
  (global as { setTimeout: typeof setTimeout }).setTimeout = ((
    fn: (...a: unknown[]) => void,
    ms?: number,
    ...rest: unknown[]
  ): TimerHandle => {
    const t = origSetTimeout(fn, ms, ...rest) as TimerHandle & { unref: () => TimerHandle };
    const realUnref = t.unref.bind(t);
    t.unref = (): TimerHandle => {
      unrefMs.push(ms ?? 0);
      return realUnref();
    };
    return t;
  }) as unknown as typeof setTimeout;
}

function restoreTimerSpy(): void {
  (global as { setTimeout: typeof setTimeout }).setTimeout = origSetTimeout;
}

describe('U283 — sleep({ unref: true }) calls timer.unref()', () => {
  beforeEach(installTimerSpy);
  afterEach(restoreTimerSpy);

  it('unrefs the backing timer when opts.unref is set', async () => {
    await sleep(5, undefined, { unref: true });
    assert.ok(unrefMs.includes(5), 'expected the 5ms sleep timer to be unref\'d');
  });

  it('does NOT unref when opts.unref is absent', async () => {
    await sleep(6);
    assert.equal(unrefMs.includes(6), false, 'a plain sleep must not unref its timer');
  });
});

describe('U283 — analytics best-effort drains route through an unref\'d sleep', () => {
  beforeEach(installTimerSpy);
  afterEach(restoreTimerSpy);

  it('raceWithTimeout default sleep unrefs its grace timer (no dangling ref on fast flush)', async () => {
    // Fast flush wins the race; the losing grace timer must NOT hold the loop
    // open — the default sleepImpl must unref it.
    await raceWithTimeout(Promise.resolve('fast'), 45_000);
    assert.ok(unrefMs.includes(45_000), 'grace timer must be unref\'d via the shared sleep');
  });

  it('closeWithGrace default sleep unrefs the drain timer', async () => {
    const t = new AnalyticsTracker(undefined, {
      env: {} as NodeJS.ProcessEnv,
      // No sleepImpl injected → exercises the production default (_sleep).
    });
    t.log('e', {});
    await t.closeWithGrace(47_000);
    assert.equal(t.isShutdown(), true);
    assert.ok(unrefMs.includes(47_000), 'closeWithGrace drain timer must be unref\'d');
  });
});
