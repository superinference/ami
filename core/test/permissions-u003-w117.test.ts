import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  MAX_SUGGESTED_RULES_FOR_COMPOUND,
  buildPermissionSuggestions,
  getCompoundCommandPrefixes,
  isNormalizedCdCommand,
  isNormalizedGitCommand,
  countDirectoryChanges,
  commandHasCdWriteDesync,
  isGitReadOnlyCommandSafe,
  PermissionManager,
  type PermissionUpdate,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U003 / w117 — permissions.ts parity with the reference's bash permission helpers.
//
// Two additive capabilities implemented together:
//   1. MAX_SUGGESTED_RULES_FOR_COMPOUND cap (default 5) on the per-subcommand
//      "allow always" rule dump a compound command's prompt may offer — mirrors
//      Claude's `MAX_SUGGESTED_RULES_FOR_COMPOUND` (bashPermissions.ts).
//   2. Canonical, reusable `isNormalizedCdCommand` / `isNormalizedGitCommand`
//      command-identity predicates (Claude's `CommandIdentityCheckers`) that the
//      compound-command guards now share, so directory-change / git recognition
//      is consistent across every guard instead of three drifted inline sets.
// ---------------------------------------------------------------------------

// --- Feature 1: MAX_SUGGESTED_RULES_FOR_COMPOUND cap -----------------------

const WIDE_COMPOUND =
  'echo a && ls b && cat c && head d && tail e && wc f && sort g && uniq h';

test('w117: the compound-suggestion cap is 5 (Claude parity)', () => {
  assert.equal(MAX_SUGGESTED_RULES_FOR_COMPOUND, 5);
});

test('w117: a wide compound command yields MORE than the cap of raw prefixes', () => {
  // Guards the test's own premise: without a cap the prompt would dump >5 rules.
  assert.ok(
    getCompoundCommandPrefixes(WIDE_COMPOUND).length > MAX_SUGGESTED_RULES_FOR_COMPOUND,
    'the probe command must produce more prefixes than the cap for the cap to matter',
  );
});

test('w117: buildPermissionSuggestions caps the per-subcommand rule dump at 5', () => {
  const suggestions = buildPermissionSuggestions('bash', { command: WIDE_COMPOUND });
  const addRules = suggestions.find(s => s.type === 'addRules');
  assert.ok(addRules && addRules.type === 'addRules', 'a wide compound offers allow-always rules');
  assert.equal(
    addRules.rules.length,
    MAX_SUGGESTED_RULES_FOR_COMPOUND,
    'the suggested-rule dump is capped at 5 — not the full 7+ prefix list',
  );
});

test('w117: a narrow compound command (<= cap) suggests all its prefixes uncapped', () => {
  const suggestions = buildPermissionSuggestions('bash', { command: 'ls a && cat b' });
  const addRules = suggestions.find(s => s.type === 'addRules');
  assert.ok(addRules && addRules.type === 'addRules');
  assert.equal(addRules.rules.length, 2, 'below the cap nothing is dropped');
});

test('w117 (e2e): checkWithReason surfaces at most 5 suggestions for a wide compound', async () => {
  // Drives the real decision path: an `ask`-mode manager evaluating a wide
  // compound bash command attaches the (capped) suggestion list to its verdict.
  const pm = new PermissionManager('ask');
  const result = await pm.checkWithReason('bash', { command: WIDE_COMPOUND });
  assert.equal(result.decision, 'ask');
  const addRules = (result.suggestions ?? []).find((s: PermissionUpdate) => s.type === 'addRules');
  assert.ok(addRules && addRules.type === 'addRules', 'the ask verdict carries allow-always suggestions');
  assert.ok(
    addRules.rules.length <= MAX_SUGGESTED_RULES_FOR_COMPOUND,
    `suggestions must respect the cap, got ${addRules.rules.length}`,
  );
});

// --- Feature 2: normalized cd / git command-identity predicates ------------

test('w117: isNormalizedCdCommand recognizes every directory-change verb', () => {
  for (const verb of ['cd', 'chdir', 'pushd', 'popd']) {
    assert.equal(isNormalizedCdCommand(verb), true, `${verb} is a directory change`);
  }
  // PowerShell Set-Location is matched case-insensitively (as PowerShell resolves it).
  assert.equal(isNormalizedCdCommand('Set-Location'), true);
  assert.equal(isNormalizedCdCommand('set-location'), true);
  assert.equal(isNormalizedCdCommand('SET-LOCATION'), true);
});

test('w117: isNormalizedCdCommand rejects non-cd tokens and undefined', () => {
  assert.equal(isNormalizedCdCommand('ls'), false);
  assert.equal(isNormalizedCdCommand('cdx'), false);
  assert.equal(isNormalizedCdCommand(''), false);
  assert.equal(isNormalizedCdCommand(undefined), false);
});

test('w117: isNormalizedGitCommand matches only the bare git base token', () => {
  assert.equal(isNormalizedGitCommand('git'), true);
  assert.equal(isNormalizedGitCommand('github'), false);
  assert.equal(isNormalizedGitCommand('gitk'), false);
  assert.equal(isNormalizedGitCommand(undefined), false);
});

test('w117: the directory-change guards now recognize popd consistently', () => {
  // Before consolidation countDirectoryChanges / commandHasCdWriteDesync omitted
  // `popd`, so these commands slipped past the multiple-cd and cd+write desync
  // guards. The shared predicate closes that drift.
  assert.equal(countDirectoryChanges('cd a && popd'), 2);
  assert.equal(commandHasCdWriteDesync('popd && echo payload > config'), true);
});

test('w117: the git read-only guard recognizes a Set-Location + git desync', () => {
  // Before consolidation the git read-only guard omitted `set-location`, so a
  // PowerShell-style directory change before git was not flagged. It is now.
  assert.equal(isGitReadOnlyCommandSafe('set-location /tmp; git status'), false);
  // A plain read-only git with no directory change is still safe (no regression).
  assert.equal(isGitReadOnlyCommandSafe('git status'), true);
});
