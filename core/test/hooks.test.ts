import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { HookManager, type HookContext } from '../src/hooks';

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
