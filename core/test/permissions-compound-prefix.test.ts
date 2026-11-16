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
    getCompoundCommandPrefixes('git fetch origin && git worktree add wt'),
    ['git'],
  );
  assert.deepEqual(
    getCompoundCommandPrefixes('npm run lint && npm run test'),
    ['npm run'],
  );
});

test('compound command where every subcommand is excluded yields no rules', () => {
  assert.deepEqual(
    getCompoundCommandPrefixes('cd a && cd b', () => true),
    [],
  );
});
