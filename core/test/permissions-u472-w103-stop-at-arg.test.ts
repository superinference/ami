import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  getSimpleCommandPrefix,
  suggestAllowPrefix,
  getCompoundCommandPrefixes,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U472 — grant-scope guard parity: the derived "allow always" prefix must
// STOP at the first transient argument (a file path, a token with a file
// extension, or an http/https/ftp URL) and must cap the depth of a leaf
// subcommand so a transient SHA/ref/tag never gets baked into a persisted
// bash allow rule.
//
// AMI already enforces this in core/core/src/permissions.ts through
// getSimpleCommandPrefix (SUBCOMMAND_SHAPE_RE rejects any token containing
// `/`, `.`, `:` so the collection loop breaks at it) plus the per-command
// spec `depth` (git stays at the two-word default so the 3rd, transient
// token is never reached). These tests lock that behavior as an explicit
// parity regression surface for Claude's shouldStopAtArg + calculateDepth
// leaf-subcommand cap
// (references/claude/claude-code-main/src/utils/shell/specPrefix.ts:L140-241):
// a widening of SUBCOMMAND_SHAPE_RE or a depth bump that let a path / URL /
// ref leak into a stored grant would fail here.
// ---------------------------------------------------------------------------

test('derived prefix stops at a slash-containing path argument', () => {
  // A path at the subcommand position yields no scoped two-word prefix.
  assert.equal(getSimpleCommandPrefix('git /tmp/foo'), null);
  // A path reached after a real subcommand is dropped, not embedded.
  assert.equal(getSimpleCommandPrefix('docker run ./local/dir'), 'docker run');
  assert.equal(getSimpleCommandPrefix('docker run /abs/path'), 'docker run');
});

test('derived prefix stops at a file-extension argument', () => {
  // Extension token at the subcommand position → no two-word prefix.
  assert.equal(getSimpleCommandPrefix('git file.txt'), null);
  // Extension token after a real subcommand is dropped (not `docker run image.tar`).
  assert.equal(getSimpleCommandPrefix('docker run image.tar'), 'docker run');
  // A version/tag-shaped token (dots) is transient → never embedded.
  assert.equal(getSimpleCommandPrefix('git v1.2.3'), null);
});

test('derived prefix stops at an http/https/ftp URL argument', () => {
  // URL at the subcommand position → no two-word prefix; first-word fallback
  // keeps only the (safe) root command, never the URL.
  assert.equal(getSimpleCommandPrefix('git https://evil.test/x'), null);
  assert.equal(suggestAllowPrefix('curl https://evil.test/x'), 'curl');
  assert.equal(suggestAllowPrefix('wget http://evil.test/x'), 'wget');
  assert.equal(suggestAllowPrefix('aria2c ftp://evil.test/x'), 'aria2c');
  // URL reached after a real subcommand in a deep-depth spec is dropped.
  assert.equal(getSimpleCommandPrefix('aws s3 cp https://evil.test/x s3://b'), 'aws s3 cp');
});

test('leaf subcommand depth cap keeps a transient SHA/ref/tag out of the grant', () => {
  // git stays at the two-word default depth, so the 3rd (transient) token —
  // a commit SHA, branch/ref name, or tag — is never part of the stored rule.
  assert.equal(getSimpleCommandPrefix('git show 81210f8'), 'git show');
  assert.equal(getSimpleCommandPrefix('git log abc1234'), 'git log');
  assert.equal(getSimpleCommandPrefix('git checkout featurebranch'), 'git checkout');
  assert.equal(getSimpleCommandPrefix('git tag releasecandidate'), 'git tag');
  // ...and the full suggestAllowPrefix pipeline agrees.
  assert.equal(suggestAllowPrefix('git show 81210f8'), 'git show');
});

test('transient tokens are excluded end-to-end from a compound allow grant', () => {
  // The exact function tool-executor calls to seed "allow always" rules: a
  // compound chain of git commands with transient ref/SHA args collapses to a
  // single root-scoped rule with NO transient token embedded.
  assert.deepEqual(
    getCompoundCommandPrefixes('git show 81210f8 && git checkout mybranch'),
    ['git'],
  );
  // A URL-bearing download subcommand contributes only its safe root, never
  // the URL, alongside a sibling subcommand's scoped prefix.
  assert.deepEqual(
    getCompoundCommandPrefixes('curl https://evil.test/x && npm run build'),
    ['curl', 'npm run'],
  );
});
