import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  wordAlignedCommonPrefix,
  collapseCompoundPrefixes,
  isBareSubcommandAwareRoot,
} from '../src/tools/command-prefix';
import { getCompoundCommandPrefixes, SUBCOMMAND_AWARE_ROOTS } from '../src/permissions';

// ---------------------------------------------------------------------------
// UNS006 — Per-subcommand prefix suggestions for compound commands, collapsed
// via CASE-INSENSITIVE word-aligned longest-common-prefix.
// Parity: references/claude/claude-code-main/src/utils/powershell/
//   staticPrefix.ts L188-316 — getCompoundCommandPrefixesStatic groups by root
//   (lowercased key), collapses each group via wordAlignedLCP (case-insensitive
//   word comparison, first-seen casing emitted), and drops a group whose LCP is
//   a bare subcommand-aware root.
//
// Before this unit the LCP collapse + root grouping was case-SENSITIVE, so a
// PowerShell-style mixed-case chain (`Git add . && git commit`) landed in two
// separate groups, neither collapsed to the bare `git` root, and a too-broad
// pair of `Git add` / `git commit` suggestions leaked out instead of being
// dropped. These tests pin the case-insensitive behavior.
// ---------------------------------------------------------------------------

const isAware = (root: string) => SUBCOMMAND_AWARE_ROOTS.has(root);

test('wordAlignedCommonPrefix compares case-insensitively and emits first casing', () => {
  assert.equal(wordAlignedCommonPrefix(['Git status', 'git log']), 'Git');
  assert.equal(wordAlignedCommonPrefix(['npm run test', 'npm run lint']), 'npm run');
  assert.equal(wordAlignedCommonPrefix(['npm Run test', 'npm run lint']), 'npm Run');
  assert.equal(wordAlignedCommonPrefix(['Get-Process']), 'Get-Process');
  assert.equal(wordAlignedCommonPrefix([]), '');
});

test('collapseCompoundPrefixes groups case-insensitively, keeps first casing', () => {
  // Same root, different casing — one group, two-word LCP kept.
  assert.deepEqual(collapseCompoundPrefixes(['npm Run a', 'npm run b'], isAware), ['npm Run']);
  // Same root, different casing — collapses to bare aware root → dropped.
  assert.deepEqual(collapseCompoundPrefixes(['Git add', 'git commit'], isAware), []);
  // Mixed roots: git group drops, npm keeps.
  assert.deepEqual(
    collapseCompoundPrefixes(['Git add', 'git commit', 'npm run a', 'npm run b'], isAware),
    ['npm run'],
  );
});

test('isBareSubcommandAwareRoot matches the injected oracle case-insensitively', () => {
  assert.equal(isBareSubcommandAwareRoot('Git', isAware), true);
  assert.equal(isBareSubcommandAwareRoot('DOCKER', isAware), true);
  assert.equal(isBareSubcommandAwareRoot('GCloud', isAware), true);
  // Lowercase still works, and multi-word is still kept.
  assert.equal(isBareSubcommandAwareRoot('git', isAware), true);
  assert.equal(isBareSubcommandAwareRoot('Git commit', isAware), false);
});

test('getCompoundCommandPrefixes drops a mixed-case subcommand-aware chain end-to-end', () => {
  // `Git add . && git commit` → one `git` group, bare-root LCP → dropped.
  assert.deepEqual(
    getCompoundCommandPrefixes('Git add . && git commit -m "x"'),
    [],
  );
  // A benign shared-root chain still collapses to the safe two-word `npm run`.
  assert.deepEqual(
    getCompoundCommandPrefixes('npm run lint && npm run test'),
    ['npm run'],
  );
});
