import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  stripLeadingNoopCd,
  countDirectoryChanges,
  commandHasCdWriteDesync,
  stripSafeEnvVars,
  SAFE_ENV_VARS,
  PermissionManager,
} from '../src/permissions.ts';

// ===========================================================================
// UNIT U003 (w91) — two additive, static command-string parity improvements:
//
//  1. Filtering of a model-prepended no-op `cd <cwd>` subcommand before the
//     cwd-change counting / cd+write-desync guards run. Models routinely emit
//     `cd $PWD && <real-cmd>`; that identity cd is a no-op and must not count
//     toward the multiple-cd guard or the cd+redirect desync guard (it would
//     otherwise spuriously escalate a benign command to an explicit prompt).
//     Parity: Claude's BashTool bashPermissions strips a leading `cd <cwd>`
//     subcommand before counting cd commands / per-subcommand evaluation.
//
//  2. SAFE_ENV_VARS allowlist coverage: pytest config, extended locale,
//     color/display tooling, and Go build flags that Claude treats as safe
//     environment-assignment prefixes for permission stripping.
// ===========================================================================

// ---------------------------------------------------------------------------
// 1a. stripLeadingNoopCd — removes an identity cd only
// ---------------------------------------------------------------------------
test('stripLeadingNoopCd removes a leading identity cd ($PWD / . / ${PWD})', () => {
  assert.equal(stripLeadingNoopCd('cd $PWD && ls -la'), 'ls -la');
  assert.equal(stripLeadingNoopCd('cd . ; echo hi'), 'echo hi');
  assert.equal(stripLeadingNoopCd('cd "${PWD}" && git status'), 'git status');
  assert.equal(stripLeadingNoopCd("cd '$PWD' && make test"), 'make test');
  assert.equal(stripLeadingNoopCd('chdir $PWD && cat a'), 'cat a');
  assert.equal(stripLeadingNoopCd('cd $(pwd) && ls'), 'ls');
});

test('stripLeadingNoopCd leaves real directory changes and non-cd commands untouched', () => {
  // A cd into a real directory shifts the resolution base — never strip it.
  assert.equal(stripLeadingNoopCd('cd src && ls'), 'cd src && ls');
  assert.equal(stripLeadingNoopCd('cd /tmp && cat a'), 'cd /tmp && cat a');
  assert.equal(stripLeadingNoopCd('cd ../.. && cat secret'), 'cd ../.. && cat secret');
  // Not a cd at all.
  assert.equal(stripLeadingNoopCd('ls -la'), 'ls -la');
  assert.equal(stripLeadingNoopCd('echo cd $PWD'), 'echo cd $PWD');
  // A bare identity cd with no following command is left as-is (nothing to peel).
  assert.equal(stripLeadingNoopCd('cd $PWD'), 'cd $PWD');
  // Only the SINGLE leading identity cd is peeled, never a later segment.
  assert.equal(stripLeadingNoopCd('ls && cd $PWD'), 'ls && cd $PWD');
});

// ---------------------------------------------------------------------------
// 1b. countDirectoryChanges ignores the no-op leading cd
// ---------------------------------------------------------------------------
test('countDirectoryChanges does not count a model-prepended identity cd', () => {
  // RED without the fix: the leading `cd $PWD` counts → 2 (trips the >1 guard).
  assert.equal(countDirectoryChanges('cd $PWD && cd sub && ls'), 1);
  assert.equal(countDirectoryChanges('cd . && cd a'), 1);
  // Two REAL directory changes still count as two (no over-stripping).
  assert.equal(countDirectoryChanges('cd a && cd b && ls'), 2);
  assert.equal(countDirectoryChanges('cd a && ls'), 1);
  assert.equal(countDirectoryChanges('ls -la'), 0);
});

// ---------------------------------------------------------------------------
// 1c. commandHasCdWriteDesync ignores the no-op leading cd
// ---------------------------------------------------------------------------
test('commandHasCdWriteDesync ignores a model-prepended identity cd before a redirect', () => {
  // RED without the fix: `cd $PWD` is treated as a real cd, so cd+redirect → true.
  assert.equal(commandHasCdWriteDesync('cd $PWD && echo x > f'), false);
  // A REAL cd before a redirect is still a desync (unchanged behavior).
  assert.equal(commandHasCdWriteDesync('cd /tmp && echo x > f'), true);
  assert.equal(commandHasCdWriteDesync('cd sub && echo x >> f'), true);
});

// ---------------------------------------------------------------------------
// 1d. End-to-end: the identity-cd prefix no longer forces an explicit prompt
// ---------------------------------------------------------------------------
test('acceptEdits auto-allows a safe command behind a model-prepended identity cd', async () => {
  const pm = new PermissionManager('acceptEdits');
  // RED without the fix: `cd . && cd sub && ls` counts 2 cds → 'unsafe' → not
  // auto-allowed → 'ask'. GREEN: the no-op `cd .` is filtered → single cd →
  // 'safe' → auto-allowed. (The `$PWD` spelling independently trips AMI's
  // separate `$`-expansion guard, so the identity form used here is `cd .`.)
  assert.equal(await pm.check('bash', { command: 'cd . && cd sub && ls' }), 'allow');
  // A genuine double directory change still requires approval (no over-reach).
  assert.equal(await pm.check('bash', { command: 'cd a && cd b && ls' }), 'ask');
});

// ---------------------------------------------------------------------------
// 2. SAFE_ENV_VARS coverage
// ---------------------------------------------------------------------------
test('SAFE_ENV_VARS covers pytest / extended-locale / color / Go-build flags', () => {
  for (const v of [
    'PYTEST_DISABLE_PLUGIN_AUTOLOAD', 'PYTEST_DEBUG',
    'LANGUAGE', 'LC_TIME', 'CHARSET',
    'LS_COLORS', 'LSCOLORS', 'GREP_COLORS', 'GCC_COLORS',
    'TIME_STYLE', 'BLOCK_SIZE', 'BLOCKSIZE',
    'GO111MODULE', 'CGO_ENABLED', 'GOEXPERIMENT',
  ]) {
    assert.ok(SAFE_ENV_VARS.has(v), `SAFE_ENV_VARS should include ${v}`);
  }
});

test('stripSafeEnvVars drops the newly-covered safe prefixes but keeps unknown ones', () => {
  assert.equal(stripSafeEnvVars('GOEXPERIMENT=synctest go test'), 'go test');
  assert.equal(stripSafeEnvVars('CGO_ENABLED=0 go build'), 'go build');
  assert.equal(stripSafeEnvVars('PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest'), 'pytest');
  // An unknown (potentially secret) assignment is preserved for scrutiny.
  assert.equal(stripSafeEnvVars('SECRET_TOKEN=abc go build'), 'SECRET_TOKEN=abc go build');
});
