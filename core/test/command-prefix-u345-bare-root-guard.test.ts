import { test } from 'node:test';
import assert from 'node:assert/strict';
import { isBareSubcommandAwareRoot } from '../src/tools/command-prefix';
import { getCompoundCommandPrefixes, SUBCOMMAND_AWARE_ROOTS } from '../src/permissions';

// ---------------------------------------------------------------------------
// U345 (w112) — compound-prefix bare-root drop guard
// (parity: references/claude/claude-code-main/src/utils/powershell/
//  staticPrefix.ts L271-280 — getCompoundCommandPrefixesStatic collapse guard)
// ---------------------------------------------------------------------------

const isAware = (root: string) => SUBCOMMAND_AWARE_ROOTS.has(root);

test('isBareSubcommandAwareRoot: bare subcommand-aware root is flagged to drop', () => {
  assert.equal(isBareSubcommandAwareRoot('git', isAware), true);
  assert.equal(isBareSubcommandAwareRoot('docker', isAware), true);
  assert.equal(isBareSubcommandAwareRoot('kubectl', isAware), true);
  assert.equal(isBareSubcommandAwareRoot('gcloud', isAware), true);
});

test('isBareSubcommandAwareRoot: multi-word LCP is kept (scoped, safe)', () => {
  assert.equal(isBareSubcommandAwareRoot('npm run', isAware), false);
  assert.equal(isBareSubcommandAwareRoot('git commit', isAware), false);
  assert.equal(isBareSubcommandAwareRoot('gcloud compute instances', isAware), false);
});

test('isBareSubcommandAwareRoot: bare NON-subcommand-aware root is kept', () => {
  // `ls`, `echo`, `cat` are not subcommand-aware — a bare rule is fine.
  assert.equal(isBareSubcommandAwareRoot('ls', isAware), false);
  assert.equal(isBareSubcommandAwareRoot('echo', isAware), false);
});

test('isBareSubcommandAwareRoot: empty / whitespace-only prefix is never a bare root', () => {
  assert.equal(isBareSubcommandAwareRoot('', isAware), false);
  assert.equal(isBareSubcommandAwareRoot('   ', isAware), false);
});

test('isBareSubcommandAwareRoot: trims surrounding whitespace before deciding', () => {
  assert.equal(isBareSubcommandAwareRoot('  git  ', isAware), true);
  assert.equal(isBareSubcommandAwareRoot(' npm run ', isAware), false);
});

test('isBareSubcommandAwareRoot: injected oracle controls the decision', () => {
  // The predicate is injected, so a caller can supply any oracle.
  assert.equal(isBareSubcommandAwareRoot('frobnicate', () => true), true);
  assert.equal(isBareSubcommandAwareRoot('git', () => false), false);
});

// End-to-end through the real permission-suggestion path: a compound `git`
// chain whose LCP collapses to bare `git` must yield NO allow-always rule,
// while a chain collapsing to a two-word `npm run` still does.
test('getCompoundCommandPrefixes drops bare git LCP end-to-end', () => {
  assert.deepEqual(
    getCompoundCommandPrefixes('git fetch && git worktree add wt && git stash'),
    [],
  );
  assert.deepEqual(
    getCompoundCommandPrefixes('npm run a && npm run b'),
    ['npm run'],
  );
});
