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

describe('hooks-tier-holdover: Gap 1 — tier-aware verdict hold-over', () => {
  it('holds builtin deny over user allow when denyRulesHold is true', async () => {
    const mgr = new HookManager();
    mgr.setDenyRulesHold(true);

    // Builtin hook that returns deny
    const builtinHook = async (_ctx: PreToolUseContext): Promise<HookDecision> => {
      return { action: 'deny', reason: 'org says no' };
    };
    mgr.onPreToolUse(builtinHook, 'builtin');

    const result = await mgr.executePreToolUse(makeCtx());
    // A deny from builtin should pass through immediately
    assert.equal(result.action, 'deny');
    assert.ok((result as any).reason?.includes('org says no'));
  });

  it('holds builtin modify over user allow when denyRulesHold is true', async () => {
    const mgr = new HookManager();
    mgr.setDenyRulesHold(true);

    // Builtin hook that returns modify (strictness 2)
    const builtinHook = async (_ctx: PreToolUseContext): Promise<HookDecision> => {
      return { action: 'modify', updatedInput: { command: 'safe' } };
    };
    mgr.onPreToolUse(builtinHook, 'builtin');

    // User hook that tries to loosen to allow (strictness 1)
    const userHook = async (_ctx: PreToolUseContext): Promise<HookDecision> => {
      return { action: 'allow' };
    };
    mgr.onPreToolUse(userHook, 'user');

    const result = await mgr.executePreToolUse(makeCtx());
    // The user loosened from modify→allow, but hold-over should deny
    assert.equal(result.action, 'deny');
    assert.ok(
      (result as any).reason?.includes('denyRulesHold'),
      `Expected reason to mention denyRulesHold, got: ${(result as any).reason}`,
    );
  });

  it('allows user loosening when denyRulesHold is false', async () => {
    const mgr = new HookManager();
    mgr.setDenyRulesHold(false); // default

    // Builtin hook that returns modify
    const builtinHook = async (_ctx: PreToolUseContext): Promise<HookDecision> => {
      return { action: 'modify', updatedInput: { command: 'safe' } };
    };
    mgr.onPreToolUse(builtinHook, 'builtin');

    // User hook that returns allow
    const userHook = async (_ctx: PreToolUseContext): Promise<HookDecision> => {
      return { action: 'allow' };
    };
    mgr.onPreToolUse(userHook, 'user');

    const result = await mgr.executePreToolUse(makeCtx());
    // Without hold, the user can loosen — the accumulated verdict is allow
    // (modify gets overridden by allow additionalContext logic)
    assert.notEqual(result.action, 'deny');
  });

  it('user deny still works even with denyRulesHold (deny is not loosening)', async () => {
    const mgr = new HookManager();
    mgr.setDenyRulesHold(true);

    // Builtin hook that returns allow
    const builtinHook = async (_ctx: PreToolUseContext): Promise<HookDecision> => {
      return { action: 'allow' };
    };
    mgr.onPreToolUse(builtinHook, 'builtin');

    // User hook that returns deny (stricter, not loosening)
    const userHook = async (_ctx: PreToolUseContext): Promise<HookDecision> => {
      return { action: 'deny', reason: 'user says no' };
    };
    mgr.onPreToolUse(userHook, 'user');

    const result = await mgr.executePreToolUse(makeCtx());
    assert.equal(result.action, 'deny');
    assert.ok((result as any).reason?.includes('user says no'));
  });

  it('org-tier hooks also get privileged treatment', async () => {
    const mgr = new HookManager();
    mgr.setDenyRulesHold(true);

    // Org hook that returns modify
    const orgHook = async (_ctx: PreToolUseContext): Promise<HookDecision> => {
      return { action: 'modify', updatedInput: { command: 'restricted' } };
    };
    mgr.onPreToolUse(orgHook, 'org');

    // User hook that returns allow (loosening)
    const userHook = async (_ctx: PreToolUseContext): Promise<HookDecision> => {
      return { action: 'allow' };
    };
    mgr.onPreToolUse(userHook, 'user');

    const result = await mgr.executePreToolUse(makeCtx());
    assert.equal(result.action, 'deny');
    assert.ok((result as any).reason?.includes('denyRulesHold'));
  });

  it('does not hold when user verdict is at same or greater strictness', async () => {
    const mgr = new HookManager();
    mgr.setDenyRulesHold(true);

    // Builtin hook that returns allow
    const builtinHook = async (_ctx: PreToolUseContext): Promise<HookDecision> => {
      return { action: 'allow' };
    };
    mgr.onPreToolUse(builtinHook, 'builtin');

    // User hook also returns allow (same level, not loosening)
    const userHook = async (_ctx: PreToolUseContext): Promise<HookDecision> => {
      return { action: 'allow' };
    };
    mgr.onPreToolUse(userHook, 'user');

    const result = await mgr.executePreToolUse(makeCtx());
    assert.equal(result.action, 'allow');
  });
});
