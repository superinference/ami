/**
 * U064 — exit_worktree user-facing display-name affordance (parity gap).
 *
 * Gap (Claude ExitWorktreeTool UI.tsx L7-9 / ExitWorktreeTool.ts L164-166):
 * Claude's ExitWorktree exposes `userFacingName() => 'Exiting worktree'` so a
 * renderer / discovery surface shows the readable activity phrase instead of the
 * raw snake_case tool id. AMI's `ToolDefinition` already carries the
 * `userFacingName` hook (types.ts) and the `ami --introspect` manifest already
 * projects it (cli/src/index.ts — `t.userFacingName ? { userFacingName: t.userFacingName({}) } : {}`),
 * but `exit_worktree` did not SET it, so it surfaced only the bare `exit_worktree`
 * id with no display name. These tests lock the affordance in and prove it flows
 * through the exact introspect projection rule end-to-end.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { exitWorktreeTool } from '../src/tools/exit-worktree';
import { enterWorktreeTool } from '../src/tools/enter-worktree';
import type { ToolDefinition } from '../src/types';

// Mirror of the compact affordance projection in cli/src/index.ts's --introspect
// tool manifest builder: emit `userFacingName` ONLY when the tool defines the hook
// (omitted, never emitted empty, for tools that don't — the isOpenWorld-style
// compact rule). Replicated here so the end-to-end surfacing contract is asserted
// in a deterministic unit test without spawning the CLI.
function projectToolAffordance(t: ToolDefinition): { name: string; userFacingName?: string } {
  return {
    name: t.name,
    ...(t.userFacingName ? { userFacingName: t.userFacingName({}) } : {}),
  };
}

describe('U064 exit_worktree userFacingName affordance', () => {
  it('exposes a static readable display name (parity: Claude "Exiting worktree")', () => {
    assert.equal(typeof exitWorktreeTool.userFacingName, 'function', 'exit_worktree must define userFacingName');
    assert.equal(exitWorktreeTool.userFacingName!({}), 'Exiting worktree');
  });

  it('is input-independent (parity: Claude userFacingName ignores input)', () => {
    // Every action projects to the same phrase — the label describes the turn's
    // activity, not the specific keep/merge/remove/stash choice.
    for (const action of ['keep', 'merge', 'remove', 'stash']) {
      assert.equal(exitWorktreeTool.userFacingName!({ action }), 'Exiting worktree');
    }
  });

  it('flows through the --introspect compact projection rule', () => {
    const projected = projectToolAffordance(exitWorktreeTool);
    assert.equal(projected.name, 'exit_worktree');
    assert.equal(projected.userFacingName, 'Exiting worktree', 'the manifest surfaces exit_worktree display name');
  });

  it('a tool without the hook omits the field (compact-manifest rule)', () => {
    // enter_worktree does not define userFacingName, so the projection must OMIT
    // the key entirely (not emit it empty) — the same compact rule as isOpenWorld.
    assert.equal(enterWorktreeTool.userFacingName, undefined, 'enter_worktree defines no userFacingName');
    const projected = projectToolAffordance(enterWorktreeTool);
    assert.ok(!('userFacingName' in projected), 'omit userFacingName when the tool defines none');
  });
});
