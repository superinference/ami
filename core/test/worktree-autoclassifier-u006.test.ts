import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { enterWorktreeTool } from '../src/tools/enter-worktree';
import { exitWorktreeTool } from '../src/tools/exit-worktree';

// ---------------------------------------------------------------------------
// U006 — per-tool `toAutoClassifierInput` projection for the worktree tools.
// Parity: Claude Code's EnterWorktreeTool (`input.name ?? ''`) and
// ExitWorktreeTool (`input.action`) feed a compact, decision-relevant value to
// the auto-mode security classifier so an auto-approval keys on the actual
// parameter (the worktree name / the keep-vs-remove action) rather than the full
// argument blob. A non-string / omitted field projects to '' (classifier skips).
// ---------------------------------------------------------------------------

describe('enter_worktree toAutoClassifierInput projects the requested name', () => {
  it('returns the worktree name when provided', () => {
    assert.equal(
      enterWorktreeTool.toAutoClassifierInput?.({ name: 'graceful-unicorn' }),
      'graceful-unicorn',
    );
  });

  it('returns empty string when name is omitted', () => {
    assert.equal(enterWorktreeTool.toAutoClassifierInput?.({ base_ref: 'main' }), '');
  });

  it('returns empty string for a non-string name', () => {
    assert.equal(enterWorktreeTool.toAutoClassifierInput?.({ name: 42 }), '');
  });
});

describe('exit_worktree toAutoClassifierInput projects the action', () => {
  it('returns the action for each keep/merge/remove/stash value', () => {
    for (const action of ['keep', 'merge', 'remove', 'stash']) {
      assert.equal(exitWorktreeTool.toAutoClassifierInput?.({ action }), action);
    }
  });

  it('returns empty string when action is omitted or non-string', () => {
    assert.equal(exitWorktreeTool.toAutoClassifierInput?.({}), '');
    assert.equal(exitWorktreeTool.toAutoClassifierInput?.({ action: true }), '');
  });
});
