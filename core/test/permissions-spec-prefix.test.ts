import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  getSimpleCommandPrefix,
  suggestAllowPrefix,
  getCompoundCommandPrefixes,
  wildcardMatch,
  BASH_COMMAND_SPECS,
  PermissionManager,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// Spec-driven command-prefix extraction: flag-value skipping
// (parity: references/claude/claude-code-main/src/utils/shell/specPrefix.ts
//  + utils/powershell/staticPrefix.ts command-spec option schema)
// ---------------------------------------------------------------------------

test('getSimpleCommandPrefix skips leading global flags + their values to find the subcommand', () => {
  // `-C` consumes `/repo` as its value → the real subcommand is `status`.
  assert.equal(getSimpleCommandPrefix('git -C /repo status --short'), 'git status');
  // `-c key=val` global config flag consumes its value.
  assert.equal(getSimpleCommandPrefix('git -c core.pager=cat log --oneline'), 'git log');
  // inline `--flag=value` carries its own value — do not swallow the next token.
  assert.equal(getSimpleCommandPrefix('git --git-dir=/x status'), 'git status');
  // boolean global flag (not in valueFlags) is skipped without eating a token.
  assert.equal(getSimpleCommandPrefix('git --no-pager log'), 'git log');
});

test('getSimpleCommandPrefix preserves the plain two-word contract', () => {
  assert.equal(getSimpleCommandPrefix('git commit -m "fix typo"'), 'git commit');
  assert.equal(getSimpleCommandPrefix('npm run build'), 'npm run');
  assert.equal(getSimpleCommandPrefix('NODE_ENV=prod npm run build'), 'npm run');
  // no subcommand-shaped token → null (flag / path / number / filename)
  assert.equal(getSimpleCommandPrefix('ls -la'), null);
  assert.equal(getSimpleCommandPrefix('git /tmp'), null);
  assert.equal(getSimpleCommandPrefix('git -C /repo'), null);
  assert.equal(getSimpleCommandPrefix('MY_SECRET=x npm run build'), null);
  assert.equal(getSimpleCommandPrefix('   '), null);
});

test('spec depth lets deep CLIs extend past two words (DEPTH_RULES parity)', () => {
  // kubectl depth 3, with a namespace flag consuming its value first.
  assert.equal(getSimpleCommandPrefix('kubectl -n kube-system get pods'), 'kubectl get pods');
  assert.equal(getSimpleCommandPrefix('docker compose up -d'), 'docker compose up');
  // aws depth 4: command + up to 3 subcommand-shaped words (parity with Claude's
  // spec-less DEPTH_RULES[aws]=4 — narrower rules are safe).
  assert.equal(getSimpleCommandPrefix('aws s3api get-object --bucket b'), 'aws s3api get-object');
  // stop at a flag once collection has begun (does not run past `-f`).
  assert.equal(getSimpleCommandPrefix('docker compose -f x.yml up'), 'docker compose');
});

test('BASH_COMMAND_SPECS is a readable command-spec option model', () => {
  assert.ok(BASH_COMMAND_SPECS.git!.valueFlags.has('-C'));
  assert.ok(BASH_COMMAND_SPECS.kubectl!.valueFlags.has('--namespace'));
  assert.equal(BASH_COMMAND_SPECS.docker!.depth, 3);
  assert.equal(BASH_COMMAND_SPECS.aws!.depth, 4);
});

test('suggestAllowPrefix + compound derivation benefit from flag-value skipping end-to-end', () => {
  // Previously `git -C /repo status` yielded NO scoped rule (bare-root refused);
  // now it derives a correctly scoped `git status`.
  assert.equal(suggestAllowPrefix('git -C /repo status'), 'git status');
  // Compound command: each subcommand scoped through the same spec-driven path.
  assert.deepEqual(
    getCompoundCommandPrefixes('git -C /repo fetch && git -C /repo pull'),
    ['git'],
  );
});

// ---------------------------------------------------------------------------
// Trailing ` *` prefix semantics (parity: shellRuleMatching.ts L136-145)
// ---------------------------------------------------------------------------

test('a sole trailing ` *` matches the bare command AND an argument form', () => {
  // The additive behavior: `git *` now matches bare `git`...
  assert.equal(wildcardMatch('git *', 'git'), true);
  // ...and still matches `git add` (prefix semantics, like legacy `git:*`).
  assert.equal(wildcardMatch('git *', 'git add'), true);
  assert.equal(wildcardMatch('git *', 'git log --oneline'), true);
  // but not a different command sharing the stem.
  assert.equal(wildcardMatch('git *', 'gitfoo'), false);
  assert.equal(wildcardMatch('git *', 'gh pr list'), false);
});

test('multi-wildcard patterns keep strict meaning (not rewritten)', () => {
  // `* run *` must NOT match bare `npm run` (needs a trailing arg): only one
  // unescaped wildcard qualifies for the trailing-optional rewrite.
  assert.equal(wildcardMatch('* run *', 'npm run'), false);
  assert.equal(wildcardMatch('* run *', 'npm run build'), true);
  // interior wildcard untouched.
  assert.equal(wildcardMatch('a*c', 'ac'), true);
});

test('trailing ` *` rule matches the bare command through the PermissionManager', async () => {
  const pm = new PermissionManager('ask', [
    { tool: 'bash', action: 'allow', pattern: 'git *' },
  ]);
  assert.equal(await pm.check('bash', { command: 'git' }), 'allow');
  assert.equal(await pm.check('bash', { command: 'git status' }), 'allow');
});
