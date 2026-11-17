/**
 * U006 — generic per-tool destructive-operation guard.
 *
 * Parity: Claude's generic `Tool.isDestructive(input)`. The permission layer must
 * downgrade an over-broad `allow` rule to an explicit prompt (`ask`) in `ask` mode
 * for ANY tool whose injected destructive resolver flags the call irreversible —
 * not just bash. It must also attribute that prompt to the `destructive` safetyCheck
 * reason, and `exitWorktreeTool.isDestructive` must classify only `action: 'remove'`
 * as destructive (keep/merge are not).
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { PermissionManager } from '../src/permissions';
import { exitWorktreeTool } from '../src/tools/exit-worktree';

describe('U006 — generic Tool.isDestructive(input) permission guard', () => {
  it('downgrades a wildcard allow rule to ask for a destructive non-bash tool (ask mode)', async () => {
    const pm = new PermissionManager('ask', [{ tool: 'exit_worktree', action: 'allow' }]);
    pm.setToolDestructiveResolver((toolName, input) =>
      toolName === 'exit_worktree' && input.action === 'remove');

    // remove is destructive -> allow rule downgraded to ask
    assert.equal(await pm.check('exit_worktree', { action: 'remove' }), 'ask');
    // keep is NOT destructive -> allow rule honored
    assert.equal(await pm.check('exit_worktree', { action: 'keep' }), 'allow');
  });

  it('without a resolver, the allow rule is honored even for action=remove (feature off by default)', async () => {
    const pm = new PermissionManager('ask', [{ tool: 'exit_worktree', action: 'allow' }]);
    assert.equal(await pm.check('exit_worktree', { action: 'remove' }), 'allow');
  });

  it('resolver returning undefined/false is treated as not-destructive', async () => {
    const pm = new PermissionManager('ask', [{ tool: 'exit_worktree', action: 'allow' }]);
    pm.setToolDestructiveResolver(() => false);
    assert.equal(await pm.check('exit_worktree', { action: 'remove' }), 'allow');
    // clearing the resolver restores default behavior
    pm.setToolDestructiveResolver(undefined);
    assert.equal(await pm.check('exit_worktree', { action: 'remove' }), 'allow');
  });

  it('the downgrade fires only in ask mode, not acceptEdits (bash parity)', async () => {
    const pm = new PermissionManager('acceptEdits', [{ tool: 'exit_worktree', action: 'allow' }]);
    pm.setToolDestructiveResolver(() => true);
    assert.equal(await pm.check('exit_worktree', { action: 'remove' }), 'allow');
  });

  it('a destructive tool call still denies under an explicit deny rule (deny wins)', async () => {
    const pm = new PermissionManager('ask', [
      { tool: 'exit_worktree', action: 'deny' },
      { tool: 'exit_worktree', action: 'allow' },
    ]);
    pm.setToolDestructiveResolver(() => true);
    assert.equal(await pm.check('exit_worktree', { action: 'remove' }), 'deny');
  });

  it('attributes the ask to the destructive safetyCheck reason', async () => {
    const pm = new PermissionManager('ask', [{ tool: 'exit_worktree', action: 'allow' }]);
    pm.setToolDestructiveResolver(() => true);
    const verdict = await pm.checkWithReason('exit_worktree', { action: 'remove' });
    assert.equal(verdict.decision, 'ask');
    assert.ok(verdict.reason);
    assert.equal(verdict.reason!.type, 'safetyCheck');
    assert.equal((verdict.reason as { check: string }).check, 'destructive');
  });

  it('does not misattribute a non-destructive ask to the destructive reason', async () => {
    // No matching allow rule: plain ask-mode prompt attributed to the mode, not destructive.
    const pm = new PermissionManager('ask');
    pm.setToolDestructiveResolver(() => false);
    const verdict = await pm.checkWithReason('exit_worktree', { action: 'keep' });
    assert.equal(verdict.decision, 'ask');
    assert.equal(verdict.reason!.type, 'mode');
  });

  it('bash destructiveness is unaffected by (and independent of) the resolver', async () => {
    const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: 'rm:*', action: 'allow' }]);
    // No resolver set: a destructive bash command still downgrades to ask.
    assert.equal(await pm.check('bash', { command: 'rm -rf build' }), 'ask');
  });
});

describe('U006 — exitWorktreeTool.isDestructive(input)', () => {
  it('classifies only action=remove as destructive', () => {
    assert.ok(exitWorktreeTool.isDestructive);
    assert.equal(exitWorktreeTool.isDestructive!({ action: 'remove' }), true);
    assert.equal(exitWorktreeTool.isDestructive!({ action: 'keep' }), false);
    assert.equal(exitWorktreeTool.isDestructive!({ action: 'merge' }), false);
    assert.equal(exitWorktreeTool.isDestructive!({}), false);
  });
});
