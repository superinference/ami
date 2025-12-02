import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  HookManager,
  type HookContext,
  type PreToolUseContext,
  type PostToolUseContext,
  type PreSamplingContext,
  type HookDecision,
} from '../src/hooks';

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

function makePreToolUseContext(overrides: Partial<PreToolUseContext> = {}): PreToolUseContext {
  return {
    messages: [{ role: 'user', content: 'hello' }],
    turnCount: 1,
    toolName: 'bash',
    toolInput: { command: 'echo hello' },
    ...overrides,
  };
}

function makePostToolUseContext(overrides: Partial<PostToolUseContext> = {}): PostToolUseContext {
  return {
    messages: [{ role: 'user', content: 'hello' }],
    turnCount: 1,
    toolName: 'bash',
    toolInput: { command: 'echo hello' },
    toolOutput: 'hello',
    isError: false,
    ...overrides,
  };
}

function makePreSamplingContext(overrides: Partial<PreSamplingContext> = {}): PreSamplingContext {
  return {
    messages: [{ role: 'user', content: 'hello' }],
    turnCount: 1,
    apiMessages: [{ role: 'system', content: 'prompt' }, { role: 'user', content: 'hello' }],
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// executePreToolUse — decision hooks (lines 124-126, 153-178)
// ---------------------------------------------------------------------------
describe('HookManager — executePreToolUse', () => {
  it('returns allow when no hooks registered', async () => {
    const hm = new HookManager();
    const decision = await hm.executePreToolUse(makePreToolUseContext());
    assert.equal(decision.action, 'allow');
  });

  it('returns deny from first denying hook', async () => {
    const hm = new HookManager();
    hm.onPreToolUse(async () => ({ action: 'deny', reason: 'blocked' }));
    hm.onPreToolUse(async () => ({ action: 'allow' })); // Should not be reached

    const decision = await hm.executePreToolUse(makePreToolUseContext());
    assert.equal(decision.action, 'deny');
    if (decision.action === 'deny') {
      assert.equal(decision.reason, 'blocked');
    }
  });

  it('returns allow when all hooks allow', async () => {
    const hm = new HookManager();
    hm.onPreToolUse(async () => ({ action: 'allow' }));
    hm.onPreToolUse(async () => ({ action: 'allow' }));

    const decision = await hm.executePreToolUse(makePreToolUseContext());
    assert.equal(decision.action, 'allow');
  });

  it('accumulates modify decisions', async () => {
    const hm = new HookManager();
    hm.onPreToolUse(async () => ({
      action: 'modify',
      updatedInput: { command: 'echo modified' },
    }));
    hm.onPreToolUse(async () => ({
      action: 'modify',
      updatedMessages: [{ role: 'user' as const, content: 'updated' }],
    }));

    const decision = await hm.executePreToolUse(makePreToolUseContext());
    assert.equal(decision.action, 'modify');
    if (decision.action === 'modify') {
      assert.deepEqual(decision.updatedInput, { command: 'echo modified' });
      assert.ok(decision.updatedMessages);
      assert.equal(decision.updatedMessages![0].content, 'updated');
    }
  });

  it('later modify hook overrides earlier modify updatedInput', async () => {
    const hm = new HookManager();
    hm.onPreToolUse(async () => ({
      action: 'modify',
      updatedInput: { command: 'first' },
    }));
    hm.onPreToolUse(async () => ({
      action: 'modify',
      updatedInput: { command: 'second' },
    }));

    const decision = await hm.executePreToolUse(makePreToolUseContext());
    assert.equal(decision.action, 'modify');
    if (decision.action === 'modify') {
      assert.deepEqual(decision.updatedInput, { command: 'second' });
    }
  });

  it('deny after modify wins', async () => {
    const hm = new HookManager();
    hm.onPreToolUse(async () => ({
      action: 'modify',
      updatedInput: { command: 'modified' },
    }));
    hm.onPreToolUse(async () => ({
      action: 'deny',
      reason: 'blocked after modify',
    }));

    const decision = await hm.executePreToolUse(makePreToolUseContext());
    assert.equal(decision.action, 'deny');
  });

  it('swallows hook errors and continues', async () => {
    const hm = new HookManager();
    hm.onPreToolUse(async () => { throw new Error('hook crashed'); });
    hm.onPreToolUse(async () => ({ action: 'deny', reason: 'after error' }));

    const decision = await hm.executePreToolUse(makePreToolUseContext());
    assert.equal(decision.action, 'deny');
  });

  it('modify without updatedInput preserves previous updatedInput', async () => {
    const hm = new HookManager();
    hm.onPreToolUse(async () => ({
      action: 'modify',
      updatedInput: { command: 'preserved' },
    }));
    hm.onPreToolUse(async () => ({
      action: 'modify',
      // No updatedInput — should preserve from previous
    }));

    const decision = await hm.executePreToolUse(makePreToolUseContext());
    assert.equal(decision.action, 'modify');
    if (decision.action === 'modify') {
      assert.deepEqual(decision.updatedInput, { command: 'preserved' });
    }
  });
});

// ---------------------------------------------------------------------------
// executePostToolUse (lines 131-139)
// ---------------------------------------------------------------------------
describe('HookManager — executePostToolUse', () => {
  it('executes post-tool-use hooks', async () => {
    const hm = new HookManager();
    let called = false;
    hm.onPostToolUse(async () => { called = true; });

    await hm.executePostToolUse(makePostToolUseContext());
    assert.ok(called);
  });

  it('receives correct context', async () => {
    const hm = new HookManager();
    let receivedCtx: PostToolUseContext | undefined;
    hm.onPostToolUse(async (ctx) => { receivedCtx = ctx; });

    const ctx = makePostToolUseContext({
      toolName: 'file_read',
      toolOutput: 'file contents',
      isError: false,
    });
    await hm.executePostToolUse(ctx);

    assert.ok(receivedCtx);
    assert.equal(receivedCtx!.toolName, 'file_read');
    assert.equal(receivedCtx!.toolOutput, 'file contents');
    assert.equal(receivedCtx!.isError, false);
  });

  it('swallows errors in post-tool-use hooks', async () => {
    const hm = new HookManager();
    let secondCalled = false;
    hm.onPostToolUse(async () => { throw new Error('crash'); });
    hm.onPostToolUse(async () => { secondCalled = true; });

    await hm.executePostToolUse(makePostToolUseContext());
    assert.ok(secondCalled);
  });
});

// ---------------------------------------------------------------------------
// executePreSampling (lines 145-147)
// ---------------------------------------------------------------------------
describe('HookManager — executePreSampling', () => {
  it('returns allow when no hooks', async () => {
    const hm = new HookManager();
    const decision = await hm.executePreSampling(makePreSamplingContext());
    assert.equal(decision.action, 'allow');
  });

  it('deny hook stops execution', async () => {
    const hm = new HookManager();
    hm.onPreSampling(async () => ({ action: 'deny', reason: 'stop' }));

    const decision = await hm.executePreSampling(makePreSamplingContext());
    assert.equal(decision.action, 'deny');
  });

  it('modify hook updates messages', async () => {
    const hm = new HookManager();
    hm.onPreSampling(async () => ({
      action: 'modify',
      updatedMessages: [{ role: 'user' as const, content: 'modified' }],
    }));

    const decision = await hm.executePreSampling(makePreSamplingContext());
    assert.equal(decision.action, 'modify');
    if (decision.action === 'modify') {
      assert.ok(decision.updatedMessages);
    }
  });

  it('swallows errors and continues', async () => {
    const hm = new HookManager();
    hm.onPreSampling(async () => { throw new Error('crash'); });
    hm.onPreSampling(async () => ({ action: 'deny', reason: 'after error' }));

    const decision = await hm.executePreSampling(makePreSamplingContext());
    assert.equal(decision.action, 'deny');
  });
});

// ---------------------------------------------------------------------------
// Multiple hook types on the same manager
// ---------------------------------------------------------------------------
describe('HookManager — multiple hook types', () => {
  it('manages all hook types independently', async () => {
    const hm = new HookManager();
    const called: string[] = [];

    hm.onPostSampling(async () => { called.push('postSampling'); });
    hm.onStop(async () => { called.push('stop'); });
    hm.onError(async () => { called.push('error'); });
    hm.onPreToolUse(async () => { called.push('preToolUse'); return { action: 'allow' }; });
    hm.onPostToolUse(async () => { called.push('postToolUse'); });
    hm.onPreSampling(async () => { called.push('preSampling'); return { action: 'allow' }; });

    await hm.executePostSampling(makeContext());
    await hm.executeStop(makeContext());
    await hm.executeError(makeContext());
    await hm.executePreToolUse(makePreToolUseContext());
    await hm.executePostToolUse(makePostToolUseContext());
    await hm.executePreSampling(makePreSamplingContext());

    assert.deepEqual(called, [
      'postSampling', 'stop', 'error', 'preToolUse', 'postToolUse', 'preSampling',
    ]);
  });
});
