import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { HookManager, EventConcurrencyLimiter, MAX_CONCURRENT_PER_EVENT, type HookContext } from '../src/hooks';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function makeContext(overrides: Partial<HookContext> = {}): HookContext {
  return {
    messages: [{ role: 'user', content: 'hello' }],
    turnCount: 1,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// onPostSampling / executePostSampling
// ---------------------------------------------------------------------------
describe('HookManager – postSampling', () => {
  it('onPostSampling registers a hook', () => {
    const hm = new HookManager();
    let called = false;
    hm.onPostSampling(async () => { called = true; });
    // Not called until executed
    assert.equal(called, false);
  });

  it('executePostSampling calls registered hooks with context', async () => {
    const hm = new HookManager();
    let receivedCtx: HookContext | undefined;
    hm.onPostSampling(async (ctx) => { receivedCtx = ctx; });

    const ctx = makeContext({ turnCount: 7 });
    await hm.executePostSampling(ctx);

    assert.ok(receivedCtx);
    assert.equal(receivedCtx!.turnCount, 7);
    assert.deepEqual(receivedCtx!.messages, ctx.messages);
  });
});

// ---------------------------------------------------------------------------
// onStop / executeStop
// ---------------------------------------------------------------------------
describe('HookManager – stop', () => {
  it('onStop and executeStop work', async () => {
    const hm = new HookManager();
    let called = false;
    hm.onStop(async () => { called = true; });
    await hm.executeStop(makeContext());
    assert.ok(called);
  });
});

// ---------------------------------------------------------------------------
// onError / executeError
// ---------------------------------------------------------------------------
describe('HookManager – error', () => {
  it('onError and executeError work', async () => {
    const hm = new HookManager();
    let called = false;
    hm.onError(async () => { called = true; });
    await hm.executeError(makeContext());
    assert.ok(called);
  });
});

// ---------------------------------------------------------------------------
// Multiple hooks execute in order
// ---------------------------------------------------------------------------
describe('HookManager – ordering', () => {
  it('multiple hooks execute in registration order', async () => {
    const hm = new HookManager();
    const order: number[] = [];
    hm.onPostSampling(async () => { order.push(1); });
    hm.onPostSampling(async () => { order.push(2); });
    hm.onPostSampling(async () => { order.push(3); });

    await hm.executePostSampling(makeContext());
    assert.deepEqual(order, [1, 2, 3]);
  });
});

// ---------------------------------------------------------------------------
// HookContext population
// ---------------------------------------------------------------------------
describe('HookManager – context', () => {
  it('hooks receive correct HookContext fields', async () => {
    const hm = new HookManager();
    let receivedCtx: HookContext | undefined;
    hm.onPostSampling(async (ctx) => { receivedCtx = ctx; });

    const ctx: HookContext = {
      messages: [
        { role: 'user', content: 'hi' },
        { role: 'assistant', content: 'hello' },
      ],
      lastAssistantMessage: { role: 'assistant', content: 'hello' },
      toolResults: [
        { toolName: 'bash', output: 'ok', isError: false },
      ],
      turnCount: 3,
    };

    await hm.executePostSampling(ctx);

    assert.ok(receivedCtx);
    assert.equal(receivedCtx!.messages.length, 2);
    assert.deepEqual(receivedCtx!.lastAssistantMessage, { role: 'assistant', content: 'hello' });
    assert.equal(receivedCtx!.toolResults!.length, 1);
    assert.equal(receivedCtx!.toolResults![0].toolName, 'bash');
    assert.equal(receivedCtx!.turnCount, 3);
  });
});

// ---------------------------------------------------------------------------
// Error handling – hooks are non-blocking
// ---------------------------------------------------------------------------
describe('HookManager – error handling', () => {
  it('hook errors are caught and swallowed (non-blocking)', async () => {
    const hm = new HookManager();
    const order: string[] = [];

    hm.onPostSampling(async () => { order.push('before'); });
    hm.onPostSampling(async () => { throw new Error('boom'); });
    hm.onPostSampling(async () => { order.push('after'); });

    // Should not throw
    await hm.executePostSampling(makeContext());
    assert.deepEqual(order, ['before', 'after']);
  });

  it('error hooks swallow errors too', async () => {
    const hm = new HookManager();
    hm.onError(async () => { throw new Error('double fault'); });
    // Should not throw
    await hm.executeError(makeContext());
  });

  it('stop hooks swallow errors too', async () => {
    const hm = new HookManager();
    hm.onStop(async () => { throw new Error('stop failure'); });
    // Should not throw
    await hm.executeStop(makeContext());
  });
});

// ---------------------------------------------------------------------------
// Async hooks
// ---------------------------------------------------------------------------
describe('HookManager – async hooks', () => {
  it('async hooks work and are awaited', async () => {
    const hm = new HookManager();
    let value = 0;
    hm.onPostSampling(async () => {
      await new Promise<void>((resolve) => setTimeout(resolve, 10));
      value = 42;
    });
    await hm.executePostSampling(makeContext());
    assert.equal(value, 42);
  });

  it('async hooks execute sequentially', async () => {
    const hm = new HookManager();
    const order: number[] = [];
    hm.onPostSampling(async () => {
      await new Promise<void>((resolve) => setTimeout(resolve, 20));
      order.push(1);
    });
    hm.onPostSampling(async () => {
      order.push(2);
    });
    await hm.executePostSampling(makeContext());
    assert.deepEqual(order, [1, 2]);
  });
});

// ---------------------------------------------------------------------------
// Empty hook manager
// ---------------------------------------------------------------------------
describe('HookManager – empty', () => {
  it('empty hook manager executes without error', async () => {
    const hm = new HookManager();
    await hm.executePostSampling(makeContext());
    await hm.executeStop(makeContext());
    await hm.executeError(makeContext());
  });
});

// ---------------------------------------------------------------------------
// EventConcurrencyLimiter
// ---------------------------------------------------------------------------
describe('EventConcurrencyLimiter', () => {
  it('exports MAX_CONCURRENT_PER_EVENT = 12', () => {
    assert.equal(MAX_CONCURRENT_PER_EVENT, 12);
  });

  it('allows up to max concurrent acquires', async () => {
    const limiter = new EventConcurrencyLimiter(3);
    await limiter.acquire('test');
    await limiter.acquire('test');
    await limiter.acquire('test');
    assert.equal(limiter.getActive('test'), 3);
    limiter.release('test');
    assert.equal(limiter.getActive('test'), 2);
    limiter.release('test');
    limiter.release('test');
    assert.equal(limiter.getActive('test'), 0);
  });

  it('queues acquires beyond the limit', async () => {
    const limiter = new EventConcurrencyLimiter(2);
    await limiter.acquire('ev');
    await limiter.acquire('ev');
    assert.equal(limiter.getActive('ev'), 2);

    let thirdResolved = false;
    const thirdPromise = limiter.acquire('ev').then(() => { thirdResolved = true; });
    assert.equal(limiter.getWaiting('ev'), 1);
    assert.equal(thirdResolved, false);

    limiter.release('ev');
    await thirdPromise;
    assert.equal(thirdResolved, true);
    assert.equal(limiter.getActive('ev'), 2);
    assert.equal(limiter.getWaiting('ev'), 0);

    limiter.release('ev');
    limiter.release('ev');
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

  it('hooks execute through concurrency gate', async () => {
    const hm = new HookManager();
    const log: string[] = [];
    hm.onPostSampling(async () => { log.push('hook1'); });
    hm.onPostSampling(async () => { log.push('hook2'); });
    await hm.executePostSampling(makeContext());
    assert.deepEqual(log, ['hook1', 'hook2']);
  });
});
