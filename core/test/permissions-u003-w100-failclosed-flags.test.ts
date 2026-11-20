import { test } from 'node:test';
import assert from 'node:assert/strict';
import { validateCommandFlags, PermissionManager } from '../src/permissions.ts';

/**
 * U003 (w100) — fail-closed per-command flag model.
 *
 * Mirrors the reference's CommandConfig fail-closed semantics
 * (readOnlyValidation.ts L39-56): a SAFE command must DECLARE its flag policy,
 * and a command with no declaration is positional-only. This replaces AMI's old
 * fail-OPEN default where `validateCommandFlags` returned `true` for any command
 * without a `SAFE_COMMAND_FLAGS` record, silently auto-approving every flag.
 *
 * RED/GREEN discriminators (fail on revert of the U003 change):
 *  - the fail-closed default: an undeclared command rejects flag-shaped tokens
 *    (was `true`, now `false`);
 *  - `hostname -b` / `-F` (SET the system hostname) are no longer auto-safe
 *    (was `safe`, now `unsafe`).
 */

test('U003 fail-closed default: an undeclared command is positional-only', () => {
  // A command with NO SAFE_COMMAND_FLAGS entry rejects any flag-shaped token.
  // (Old fail-OPEN behavior returned `true` here.)
  assert.equal(validateCommandFlags('madeupcmd', ['-x']), false);
  assert.equal(validateCommandFlags('madeupcmd', ['--delete-everything']), false);
  // …but a purely-positional invocation is still accepted.
  assert.equal(validateCommandFlags('madeupcmd', ['file.txt']), true);
  assert.equal(validateCommandFlags('madeupcmd', []), true);
});

test('U003 allowAllFlags opt-in: declared read-only utilities accept any non-blocked flag', () => {
  // The all-flags-safe read utilities are explicitly declared (allowAllFlags),
  // so the inverted default never regresses them.
  assert.equal(validateCommandFlags('cat', ['-n', '--show-ends']), true);
  assert.equal(validateCommandFlags('ls', ['-la', '--color=always']), true);
  assert.equal(validateCommandFlags('df', ['-h', '--total']), true);
  assert.equal(validateCommandFlags('diff', ['-u', '--ignore-all-space']), true);
  assert.equal(validateCommandFlags('dotnet', ['--version']), true);
});

test('U003 hostname: read flags stay safe, SET flags are rejected (fail-closed hardening)', () => {
  // Identification flags are read-only.
  assert.equal(validateCommandFlags('hostname', ['-f']), true);
  assert.equal(validateCommandFlags('hostname', ['-i']), true);
  assert.equal(validateCommandFlags('hostname', ['--all-ip-addresses']), true);
  // `-b`/`--boot` and `-F`/`--file` SET the hostname — must fall through to a prompt.
  // (Old fail-OPEN behavior auto-approved these.)
  assert.equal(validateCommandFlags('hostname', ['-b']), false);
  assert.equal(validateCommandFlags('hostname', ['--boot']), false);
  assert.equal(validateCommandFlags('hostname', ['-F']), false);
  assert.equal(validateCommandFlags('hostname', ['--file']), false);
});

test('U003 end-to-end: classifier no longer auto-approves a hostname write', () => {
  const pm = new PermissionManager();
  // RED on revert: `hostname -b` (set from boot config, no positional) classified
  // `safe` under the old fail-OPEN flag default. It must now be `unsafe`.
  assert.equal(pm.classifyBashCommand('hostname -b'), 'unsafe');
  assert.equal(pm.classifyBashCommand('hostname -F /etc/myname'), 'unsafe');
  // Regression locks: reads and the pre-existing positional-set guard are unchanged.
  assert.equal(pm.classifyBashCommand('hostname'), 'safe');
  assert.equal(pm.classifyBashCommand('hostname -f'), 'safe');
  assert.equal(pm.classifyBashCommand('hostname newname'), 'unsafe');
});

test('U003 end-to-end: declared read utilities still auto-approve as before', () => {
  const pm = new PermissionManager();
  // Regression locks: the fail-closed inversion must not regress the common
  // read-only commands (each declared allowAllFlags).
  assert.equal(pm.classifyBashCommand('ls -la'), 'safe');
  assert.equal(pm.classifyBashCommand('cat -n README.md'), 'safe');
  assert.equal(pm.classifyBashCommand('df -h'), 'safe');
  assert.equal(pm.classifyBashCommand('du -sh .'), 'safe');
  assert.equal(pm.classifyBashCommand('wc -l file'), 'safe');
  assert.equal(pm.classifyBashCommand('dotnet --version'), 'safe');
  // dotnet subcommand still prompts (isDotnetSafe runs after flag validation).
  assert.equal(pm.classifyBashCommand('dotnet build'), 'unsafe');
});
