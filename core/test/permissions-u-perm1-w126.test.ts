import { test } from 'node:test';
import assert from 'node:assert/strict';
import { outOfWorkspacePathDisposition } from '../src/permissions';
import type { OutOfWorkspacePathDisposition } from '../src/permissions';

// U-perm1 / w126 — acceptEdits CWD scoping for out-of-workspace paths.
//
// Claude Code's acceptEdits fast path is CWD-scoped: edits are auto-accepted WITHIN
// the launch cwd, but a path OUTSIDE the cwd is CLASSIFIED (subjected to consent),
// not blanket-granted ("allowed in CWD, classified outside CWD" —
// classifierDecision.ts safe-set doc comment + modeValidation CWD fast-path).
//
// outOfWorkspacePathDisposition() is the single source of truth for how each mode
// disposes of an out-of-workspace path. The parity-critical assertion is that
// 'acceptEdits' returns 'classify' (NOT 'grant'), so the executor's path-boundary
// guard prompts for an out-of-cwd edit instead of silently granting it.

test('auto-allow + bypassPermissions blanket-GRANT out-of-workspace paths', () => {
  assert.equal(outOfWorkspacePathDisposition('auto-allow'), 'grant');
  assert.equal(outOfWorkspacePathDisposition('bypassPermissions'), 'grant');
});

test('deny-all + plan hard-DENY out-of-workspace paths', () => {
  assert.equal(outOfWorkspacePathDisposition('deny-all'), 'deny');
  assert.equal(outOfWorkspacePathDisposition('plan'), 'deny');
});

test('ask + auto CLASSIFY out-of-workspace paths (require consent)', () => {
  assert.equal(outOfWorkspacePathDisposition('ask'), 'classify');
  assert.equal(outOfWorkspacePathDisposition('auto'), 'classify');
});

test('PARITY: acceptEdits CLASSIFIES out-of-workspace paths (not grant)', () => {
  // The whole point of the gap: acceptEdits must NOT silently grant an out-of-cwd
  // path. It auto-accepts edits only WITHIN the cwd; outside the cwd it is classified.
  assert.equal(outOfWorkspacePathDisposition('acceptEdits'), 'classify');
  assert.notEqual(outOfWorkspacePathDisposition('acceptEdits'), 'grant');
});

test('an unset/undefined mode falls back to classify (fail-safe default)', () => {
  assert.equal(outOfWorkspacePathDisposition(undefined), 'classify');
});

test('the disposition is always one of the three declared outcomes', () => {
  const modes = ['ask', 'auto', 'auto-allow', 'deny-all', 'acceptEdits', 'bypassPermissions', 'plan'] as const;
  const valid: OutOfWorkspacePathDisposition[] = ['grant', 'deny', 'classify'];
  for (const m of modes) {
    assert.ok(valid.includes(outOfWorkspacePathDisposition(m)), `mode ${m} returned an undeclared disposition`);
  }
});
