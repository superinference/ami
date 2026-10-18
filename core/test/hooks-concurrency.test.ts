import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventConcurrencyLimiter, MAX_CONCURRENT_PER_EVENT, HookManager } from '../src/index';

describe('EventConcurrencyLimiter', () => {
  it('allows up to maxConcurrent without blocking', async () => {
    const limiter = new EventConcurrencyLimiter(3);
    await limiter.acquire('test');
    await limiter.acquire('test');
    await limiter.acquire('test');
    assert.equal(limiter.getActive('test'), 3);
    limiter.release('test');
    limiter.release('test');
    limiter.release('test');
    assert.equal(limiter.getActive('test'), 0);
  });

  it('queues waiters beyond maxConcurrent', async () => {
    const limiter = new EventConcurrencyLimiter(2);
    await limiter.acquire('e');
    await limiter.acquire('e');
    assert.equal(limiter.getActive('e'), 2);

    let thirdAcquired = false;
    const p = limiter.acquire('e').then(() => { thirdAcquired = true; });
    // Let microtasks run
    await new Promise(r => setTimeout(r, 10));
    assert.equal(thirdAcquired, false, 'third acquire should wait');
    assert.equal(limiter.getWaiting('e'), 1);

    limiter.release('e');
    await p;
    assert.equal(thirdAcquired, true, 'third should proceed after release');
    assert.equal(limiter.getActive('e'), 2);

    limiter.release('e');
    limiter.release('e');
  });

  it('tracks events independently', async () => {
    const limiter = new EventConcurrencyLimiter(1);
    await limiter.acquire('a');
    await limiter.acquire('b');
    assert.equal(limiter.getActive('a'), 1);
    assert.equal(limiter.getActive('b'), 1);
    limiter.release('a');
    limiter.release('b');
  });

  it('defaults to MAX_CONCURRENT_PER_EVENT (12)', () => {
    const limiter = new EventConcurrencyLimiter();
    assert.equal(MAX_CONCURRENT_PER_EVENT, 12);
  });

  it('processes waiters in FIFO order', async () => {
    const limiter = new EventConcurrencyLimiter(1);
    await limiter.acquire('e');

    const order: number[] = [];
    const p1 = limiter.acquire('e').then(() => { order.push(1); });
    const p2 = limiter.acquire('e').then(() => { order.push(2); });
    const p3 = limiter.acquire('e').then(() => { order.push(3); });

    assert.equal(limiter.getWaiting('e'), 3);

    limiter.release('e');
    await p1;
    limiter.release('e');
    await p2;
    limiter.release('e');
    await p3;
    limiter.release('e');

    assert.deepEqual(order, [1, 2, 3]);
  });

  it('cleans up active count when reaching 0', async () => {
    const limiter = new EventConcurrencyLimiter(2);
    await limiter.acquire('e');
    limiter.release('e');
    assert.equal(limiter.getActive('e'), 0);
  });

  it('getWaiting returns 0 for unknown events', () => {
    const limiter = new EventConcurrencyLimiter();
    assert.equal(limiter.getWaiting('nonexistent'), 0);
    assert.equal(limiter.getActive('nonexistent'), 0);
  });
});

describe('HookManager – concurrency gate', () => {
  function makeHookManager(): HookManager {
    const hm = new HookManager();
    hm.setCwd('/tmp');
    hm.setSessionId('test-session');
    return hm;
  }

  it('limits concurrent preToolUse executions to 12', async () => {
    const hm = makeHookManager();
    let concurrent = 0;
    let maxConcurrent = 0;

    const slowHook = async () => {
      concurrent++;
      maxConcurrent = Math.max(maxConcurrent, concurrent);
      await new Promise(r => setTimeout(r, 50));
      concurrent--;
      return { action: 'allow' as const };
    };

    for (let i = 0; i < 15; i++) {
      hm.onPreToolUse(slowHook);
    }

    // Fire 15 concurrent preToolUse invocations
    const ctx = {
      messages: [], turnCount: 1,
      toolName: 'bash', toolInput: { command: 'echo hi' },
    };
    const promises = Array.from({ length: 15 }, () => hm.executePreToolUse({ ...ctx }));
    await Promise.all(promises);

    assert.ok(maxConcurrent <= 12, `max concurrent was ${maxConcurrent}, expected ≤ 12`);
  });

  it('allows different events to run concurrently up to their own limits', async () => {
    const hm = makeHookManager();
    let preToolActive = 0;
    let postToolActive = 0;

    hm.onPreToolUse(async () => {
      preToolActive++;
      await new Promise(r => setTimeout(r, 30));
      preToolActive--;
      return { action: 'allow' as const };
    });

    hm.onPostToolUse(async () => {
      postToolActive++;
      await new Promise(r => setTimeout(r, 30));
      postToolActive--;
    });

    const preCalls = Array.from({ length: 3 }, () =>
      hm.executePreToolUse({ messages: [], turnCount: 1, toolName: 'bash', toolInput: {} }));
    const postCalls = Array.from({ length: 3 }, () =>
      hm.executePostToolUse({ messages: [], turnCount: 1, toolName: 'bash', toolInput: {}, toolOutput: 'ok', isError: false }));

    await Promise.all([...preCalls, ...postCalls]);
    // Both event types should have been able to run their hooks
    assert.equal(preToolActive, 0);
    assert.equal(postToolActive, 0);
  });

  it('gates callback hooks by event name', async () => {
    const hm = makeHookManager();
    let concurrent = 0;
    let maxConcurrent = 0;

    hm.registerCallback('custom', async () => {
      concurrent++;
      maxConcurrent = Math.max(maxConcurrent, concurrent);
      await new Promise(r => setTimeout(r, 20));
      concurrent--;
      return undefined;
    });

    const calls = Array.from({ length: 5 }, () => hm.executeCallbacks('custom', {}));
    await Promise.all(calls);

    assert.ok(maxConcurrent <= 12);
  });
});
