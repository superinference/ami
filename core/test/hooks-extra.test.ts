import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  HookManager,
  type HookContext,
  type PreToolUseContext,
  type PostToolUseContext,
  type PreSamplingContext,
  type PermissionContext,
  type TaskEventContext,
  type UserPromptContext,
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

// ---------------------------------------------------------------------------
// Permission hooks (permissionRequest, permissionDenied)
// ---------------------------------------------------------------------------
describe('HookManager — permissionRequest / permissionDenied', () => {
  it('executePermissionRequest calls registered hooks', async () => {
    const hm = new HookManager();
    let received: PermissionContext | undefined;
    hm.onPermissionRequest(async (ctx) => { received = ctx; });

    await hm.executePermissionRequest({ toolName: 'bash', toolInput: { command: 'ls' } });
    assert.ok(received);
    assert.equal(received!.toolName, 'bash');
  });

  it('executePermissionDenied calls registered hooks', async () => {
    const hm = new HookManager();
    let received: PermissionContext | undefined;
    hm.onPermissionDenied(async (ctx) => { received = ctx; });

    await hm.executePermissionDenied({ toolName: 'bash', toolInput: { command: 'rm -rf /' }, command: 'rm -rf /' });
    assert.ok(received);
    assert.equal(received!.toolName, 'bash');
    assert.equal(received!.command, 'rm -rf /');
  });

  it('permission hooks swallow errors', async () => {
    const hm = new HookManager();
    let called = false;
    hm.onPermissionRequest(async () => { throw new Error('boom'); });
    hm.onPermissionRequest(async () => { called = true; });

    await hm.executePermissionRequest({ toolName: 'bash', toolInput: {} });
    assert.ok(called);
  });
});

// ---------------------------------------------------------------------------
// UserPromptSubmit hook
// ---------------------------------------------------------------------------
describe('HookManager — userPromptSubmit', () => {
  it('executeUserPromptSubmit calls registered hooks', async () => {
    const hm = new HookManager();
    let received: UserPromptContext | undefined;
    hm.onUserPromptSubmit(async (ctx) => { received = ctx; });

    await hm.executeUserPromptSubmit({ prompt: 'fix the bug', turnCount: 3 });
    assert.ok(received);
    assert.equal(received!.prompt, 'fix the bug');
    assert.equal(received!.turnCount, 3);
  });
});

// ---------------------------------------------------------------------------
// Task lifecycle hooks (taskCreated, taskCompleted)
// ---------------------------------------------------------------------------
describe('HookManager — taskCreated / taskCompleted', () => {
  it('executeTaskCreated calls registered hooks', async () => {
    const hm = new HookManager();
    let received: TaskEventContext | undefined;
    hm.onTaskCreated(async (ctx) => { received = ctx; });

    await hm.executeTaskCreated({ taskId: '42', subject: 'Fix login', status: 'pending' });
    assert.ok(received);
    assert.equal(received!.taskId, '42');
    assert.equal(received!.subject, 'Fix login');
    assert.equal(received!.status, 'pending');
  });

  it('executeTaskCompleted calls registered hooks', async () => {
    const hm = new HookManager();
    let received: TaskEventContext | undefined;
    hm.onTaskCompleted(async (ctx) => { received = ctx; });

    await hm.executeTaskCompleted({ taskId: '42', subject: 'Fix login', status: 'completed' });
    assert.ok(received);
    assert.equal(received!.status, 'completed');
  });

  it('task hooks swallow errors', async () => {
    const hm = new HookManager();
    let secondCalled = false;
    hm.onTaskCreated(async () => { throw new Error('crash'); });
    hm.onTaskCreated(async () => { secondCalled = true; });

    await hm.executeTaskCreated({ taskId: '1', subject: 'test', status: 'pending' });
    assert.ok(secondCalled);
  });
});

// ---------------------------------------------------------------------------
// All 17 hook events execute independently
// ---------------------------------------------------------------------------
describe('HookManager — all 17 events independent', () => {
  it('registers and fires all event types without interference', async () => {
    const hm = new HookManager();
    const fired: string[] = [];

    hm.onPostSampling(async () => { fired.push('postSampling'); });
    hm.onStop(async () => { fired.push('stop'); });
    hm.onError(async () => { fired.push('error'); });
    hm.onPreToolUse(async () => { fired.push('preToolUse'); return { action: 'allow' }; });
    hm.onPostToolUse(async () => { fired.push('postToolUse'); });
    hm.onPreSampling(async () => { fired.push('preSampling'); return { action: 'allow' }; });
    hm.onSessionStart(async () => { fired.push('sessionStart'); });
    hm.onSessionEnd(async () => { fired.push('sessionEnd'); });
    hm.onSubagentStart(async () => { fired.push('subagentStart'); });
    hm.onSubagentStop(async () => { fired.push('subagentStop'); });
    hm.onPreCompact(async () => { fired.push('preCompact'); });
    hm.onPostCompact(async () => { fired.push('postCompact'); });
    hm.onPermissionRequest(async () => { fired.push('permissionRequest'); });
    hm.onPermissionDenied(async () => { fired.push('permissionDenied'); });
    hm.onUserPromptSubmit(async () => { fired.push('userPromptSubmit'); });
    hm.onTaskCreated(async () => { fired.push('taskCreated'); });
    hm.onTaskCompleted(async () => { fired.push('taskCompleted'); });

    await hm.executePostSampling(makeContext());
    await hm.executeStop(makeContext());
    await hm.executeError(makeContext());
    await hm.executePreToolUse(makePreToolUseContext());
    await hm.executePostToolUse(makePostToolUseContext());
    await hm.executePreSampling(makePreSamplingContext());
    await hm.executeSessionStart({ sessionId: 'test', cwd: '/tmp' });
    await hm.executeSessionEnd({ sessionId: 'test', cwd: '/tmp' });
    await hm.executeSubagentStart({ parentSessionId: 'p', subagentSessionId: 's', prompt: 'go' });
    await hm.executeSubagentStop({ parentSessionId: 'p', subagentSessionId: 's', prompt: 'go' });
    await hm.executePreCompact({ messageCount: 10, tokenEstimate: 5000 });
    await hm.executePostCompact({ messageCount: 5, tokenEstimate: 2500 });
    await hm.executePermissionRequest({ toolName: 'bash', toolInput: {} });
    await hm.executePermissionDenied({ toolName: 'bash', toolInput: {} });
    await hm.executeUserPromptSubmit({ prompt: 'test', turnCount: 1 });
    await hm.executeTaskCreated({ taskId: '1', subject: 'test', status: 'pending' });
    await hm.executeTaskCompleted({ taskId: '1', subject: 'test', status: 'completed' });

    assert.equal(fired.length, 17);
  });
});
