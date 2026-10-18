import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { HookManager } from '../src/hooks';
import type { PreToolUseContext, HookDecision } from '../src/hooks';

function makeCtx(toolName = 'bash'): PreToolUseContext {
  return {
    messages: [],
    turnCount: 1,
    toolName,
    toolInput: { command: 'echo hello' },
  };
}

describe('hooks-failclosed: Gap 2 — executeDecisionHooks fail-closed on error', () => {
  it('returns deny when a hook throws', async () => {
    const mgr = new HookManager();
    mgr.onPreToolUse(async (_ctx: PreToolUseContext): Promise<HookDecision> => {
      throw new Error('kaboom');
    });

    const result = await mgr.executePreToolUse(makeCtx());
    assert.equal(result.action, 'deny');
    assert.ok(
      (result as any).reason?.includes('fail-closed'),
      `Expected reason to contain "fail-closed", got: ${(result as any).reason}`,
    );
    assert.ok(
      (result as any).reason?.includes('kaboom'),
      `Expected reason to contain the error message "kaboom", got: ${(result as any).reason}`,
    );
  });

  it('returns allow when hooks do NOT throw', async () => {
    const mgr = new HookManager();
    mgr.onPreToolUse(async (_ctx: PreToolUseContext): Promise<HookDecision> => {
      return { action: 'allow' };
    });

    const result = await mgr.executePreToolUse(makeCtx());
    assert.equal(result.action, 'allow');
  });

  it('does not execute subsequent hooks after the first one throws', async () => {
    const mgr = new HookManager();
    let secondHookCalled = false;

    mgr.onPreToolUse(async (_ctx: PreToolUseContext): Promise<HookDecision> => {
      throw new Error('first hook fails');
    });

    mgr.onPreToolUse(async (_ctx: PreToolUseContext): Promise<HookDecision> => {
      secondHookCalled = true;
      return { action: 'allow' };
    });

    const result = await mgr.executePreToolUse(makeCtx());
    assert.equal(result.action, 'deny');
    assert.equal(secondHookCalled, false, 'Second hook should not have been called');
  });

  it('returns deny with non-Error thrown values', async () => {
    const mgr = new HookManager();
    mgr.onPreToolUse(async (_ctx: PreToolUseContext): Promise<HookDecision> => {
      throw 'string-error';
    });

    const result = await mgr.executePreToolUse(makeCtx());
    assert.equal(result.action, 'deny');
    assert.ok(
      (result as any).reason?.includes('string-error'),
      `Expected reason to contain "string-error", got: ${(result as any).reason}`,
    );
  });

  it('returns deny from first hook that throws even when earlier hooks succeed', async () => {
    const mgr = new HookManager();

    mgr.onPreToolUse(async (_ctx: PreToolUseContext): Promise<HookDecision> => {
      return { action: 'allow' };
    });

    mgr.onPreToolUse(async (_ctx: PreToolUseContext): Promise<HookDecision> => {
      throw new Error('second hook fails');
    });

    const result = await mgr.executePreToolUse(makeCtx());
    assert.equal(result.action, 'deny');
    assert.ok((result as any).reason?.includes('fail-closed'));
  });
});
