import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { runSearch, grepHooks } from '../src/tools/grep';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// runSearch — SIGKILL-then-grace settle path (killThenSettle).
//
// When a search process wedges and never emits 'close' (seen on Windows and for
// uninterruptible I/O), the tool must still resolve: on timeout OR abort it
// SIGKILLs the child and then settles after `grepHooks.killGraceMs` even though
// 'close' never fires. These tests drive that path deterministically via the
// documented DI seam (grepHooks.spawn / killGraceMs) with a fake child that
// never closes, so the grace timer is the only thing that can resolve.
// ---------------------------------------------------------------------------

/**
 * A fake spawned child that mimics the surface runSearch touches: stdout/stderr
 * data streams and a kill() method. It deliberately NEVER emits 'close' or
 * 'error', so only the grace timer can resolve the search.
 */
function makeWedgedChild(opts: { killThrows?: boolean } = {}) {
  const child: any = new EventEmitter();
  child.stdout = new EventEmitter();
  child.stderr = new EventEmitter();
  child.killCalls = [];
  child.kill = (signal?: string) => {
    child.killCalls.push(signal);
    if (opts.killThrows) throw new Error('ESRCH: no such process');
    return true;
  };
  return child;
}

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

const savedTimeoutMs = grepHooks.timeoutMs;
const savedKillGraceMs = grepHooks.killGraceMs;

afterEach(() => {
  mock.restoreAll();
  grepHooks.timeoutMs = savedTimeoutMs;
  grepHooks.killGraceMs = savedKillGraceMs;
});

describe('runSearch – timeout kills the wedged child then settles after the grace period', () => {
  it('SIGKILLs the child and resolves timedOut once the grace timer fires', async () => {
    const child = makeWedgedChild();
    mock.method(grepHooks, 'spawn', () => child as any);
    grepHooks.killGraceMs = 10;

    const start = Date.now();
    // timeout = 10ms triggers the timer -> killThenSettle; child never 'close's,
    // so the grace timer (10ms) is the ONLY thing that can resolve the promise.
    const outcome = await runSearch(['pattern', '/tmp'], 'rg', ctx(), 10);

    // stdout stayed empty -> output coalesces to null; timedOut flag is set.
    assert.equal(outcome.output, null);
    assert.equal(outcome.timedOut, true);
    // The child was SIGKILLed exactly once by killThenSettle.
    assert.deepEqual(child.killCalls, ['SIGKILL']);
    // It genuinely waited for timeout + grace (not an instant 'close' resolve).
    assert.ok(Date.now() - start >= 18, 'should wait for timeout + grace period');
  });

  it('preserves partial stdout collected before the timeout', async () => {
    const child = makeWedgedChild();
    mock.method(grepHooks, 'spawn', () => child as any);
    grepHooks.killGraceMs = 10;

    const p = runSearch(['pattern', '/tmp'], 'rg', ctx(), 20);
    // Feed some output before the timeout fires.
    child.stdout.emit('data', Buffer.from('file.ts:1:partial\n'));
    const outcome = await p;

    assert.equal(outcome.output, 'file.ts:1:partial\n');
    assert.equal(outcome.timedOut, true);
    assert.deepEqual(child.killCalls, ['SIGKILL']);
  });
});

describe('runSearch – abort kills the wedged child then settles via the grace timer', () => {
  it('resolves with empty output when aborted mid-search', async () => {
    const child = makeWedgedChild();
    mock.method(grepHooks, 'spawn', () => child as any);
    grepHooks.killGraceMs = 10;

    const ac = new AbortController();
    // No timeout (0) so ONLY the abort path can reach killThenSettle.
    const p = runSearch(['pattern', '/tmp'], 'rg', ctx({ abortSignal: ac.signal }), 0);
    // Abort after the listener is registered (executor ran synchronously).
    setImmediate(() => ac.abort());
    const outcome = await p;

    assert.deepEqual(outcome, { output: '' });
    assert.deepEqual(child.killCalls, ['SIGKILL']);
  });
});

describe('runSearch – killThenSettle swallows a kill() that throws', () => {
  it('still settles after the grace period when child.kill throws', async () => {
    const child = makeWedgedChild({ killThrows: true });
    mock.method(grepHooks, 'spawn', () => child as any);
    grepHooks.killGraceMs = 10;

    // kill() throwing (already-gone process) must be swallowed; the grace timer
    // still resolves the timed-out search instead of rejecting/hanging.
    const outcome = await runSearch(['pattern', '/tmp'], 'rg', ctx(), 10);

    assert.equal(outcome.timedOut, true);
    assert.equal(outcome.output, null);
    assert.deepEqual(child.killCalls, ['SIGKILL']);
  });
});
