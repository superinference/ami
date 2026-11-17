import { test } from 'node:test';
import assert from 'node:assert/strict';
import { buildPermissionSuggestions, PermissionManager } from '../src/permissions';

// ---------------------------------------------------------------------------
// U003 / w96 — G9 (missing affordance): a blocked file EDIT/WRITE now offers a
// one-tap "switch to acceptEdits" (setMode) suggestion alongside the existing
// path-scoped allow rule, mirroring Claude's pathValidation write affordance
// `{ type:'setMode', mode:'acceptEdits' }` (tools/.../pathValidation.ts
// L1792-1798). AMI already offered addDirectories/addRules affordances; the
// setMode affordance was absent. Offered ONLY for edit-capable tools (the ones
// acceptEdits actually auto-allows), never for reads or bash.
// ---------------------------------------------------------------------------

test('G9: a file_edit write/create carries a setMode acceptEdits suggestion', () => {
  const suggestions = buildPermissionSuggestions('file_edit', { file_path: '/tmp/does-not-exist-xyz.txt' });
  const setMode = suggestions.find(s => s.type === 'setMode');
  assert.ok(setMode, 'a blocked edit offers switching to acceptEdits');
  assert.equal(setMode!.type === 'setMode' && setMode.mode, 'acceptEdits');
  // The existing path-scoped allow affordance is preserved (additive, not a replace).
  assert.ok(suggestions.some(s => s.type === 'addRules'), 'the path allow rule affordance is still offered');
});

test('G9: a file READ offers directory affordances but NOT setMode acceptEdits', () => {
  const suggestions = buildPermissionSuggestions('file_read', { file_path: '/etc/hosts' });
  assert.ok(suggestions.some(s => s.type === 'addDirectories'), 'a read widens the read boundary');
  assert.ok(!suggestions.some(s => s.type === 'setMode'), 'acceptEdits does not help a read, so it is not offered');
});

test('G9: a bash command carries no setMode acceptEdits suggestion', () => {
  const suggestions = buildPermissionSuggestions('bash', { command: 'rm foo' });
  assert.ok(!suggestions.some(s => s.type === 'setMode'));
});

test('G9: the setMode suggestion is a valid applicable PermissionUpdate (wired end-to-end)', () => {
  const suggestions = buildPermissionSuggestions('file_write', { file_path: '/tmp/does-not-exist-abc.txt' });
  const setMode = suggestions.find(s => s.type === 'setMode');
  assert.ok(setMode);
  const pm = new PermissionManager('ask');
  const { applied } = pm.applyUpdate(setMode!);
  assert.equal(applied, 1, 'the suggestion applies cleanly through applyUpdate');
  assert.equal(pm.getMode(), 'acceptEdits');
});
