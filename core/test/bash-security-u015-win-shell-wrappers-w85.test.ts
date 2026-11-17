import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  PermissionManager,
  commandInvokesGit,
  BARE_SHELL_PREFIXES,
  isBareShellPrefix,
  getFirstWordPrefix,
} from '../src/permissions';

/**
 * U015 / wave w85 — DANGEROUS_SHELL_PREFIXES parity for Windows shells
 * (core/core/src/permissions.ts SHELL_WRAPPERS, which feeds the bash-security
 * command decomposition `extractBaseCommands`).
 *
 * Claude Code maintains DANGEROUS_SHELL_PREFIXES = {sh, bash, zsh, fish, csh,
 * tcsh, ksh, dash, cmd, cmd.exe, powershell, powershell.exe, pwsh, pwsh.exe,
 * bash.exe} and refuses to treat any of them as a bare permission-granting
 * prefix — allowing e.g. `bash` or `powershell` as a prefix would let any
 * command through and defeat the permission system (utils/shell/prefix.ts).
 *
 * AMI already denies these as grantable *prefixes* via BARE_SHELL_PREFIXES /
 * isBareShellPrefix. The genuinely-missing half was command DECOMPOSITION: the
 * wrapper-unwrap set `SHELL_WRAPPERS` (used by extractBaseCommands) covered only
 * the Unix shells and OMITTED every Windows shell + `.exe` variant, so a denied
 * inner command could hide behind `powershell -Command "<denied>"`, `pwsh -c
 * ...`, `cmd /c ...`, or `bash.exe -c ...`. The inner command was never surfaced
 * for deny/allow rule matching — a permission bypass equivalent to the one
 * DANGEROUS_SHELL_PREFIXES exists to prevent. This wave unwraps Windows shells
 * (incl. their `-Command` / `/c` / `/k` command-payload flags) too.
 */

// ---------------------------------------------------------------------------
// Regression lock: the existing denylist already covers Windows shells.
// ---------------------------------------------------------------------------

test('BARE_SHELL_PREFIXES denies Windows shells + .exe variants as grantable prefixes', () => {
  for (const name of ['cmd', 'cmd.exe', 'powershell', 'powershell.exe', 'pwsh', 'pwsh.exe', 'bash.exe']) {
    assert.ok(BARE_SHELL_PREFIXES.has(name), `${name} should be a bare shell prefix`);
    assert.equal(isBareShellPrefix(name.toUpperCase()), true, `isBareShellPrefix(${name}) case-insensitive`);
  }
  // A shell is never suggested as an allow-rule first-word prefix.
  assert.equal(getFirstWordPrefix('powershell -Command "git push"'), null);
  assert.equal(getFirstWordPrefix('cmd /c git status'), null);
});

// ---------------------------------------------------------------------------
// Base-command decomposition now unwraps Windows shells.
// ---------------------------------------------------------------------------

test('commandInvokesGit sees through Windows-shell -Command / -c / /c payloads', () => {
  // Unix shells were already unwrapped (regression lock).
  assert.equal(commandInvokesGit('bash -c "git push origin main"'), true);
  assert.equal(commandInvokesGit('zsh -c "git push origin main"'), true);
  // Windows shells + .exe variants must now also be unwrapped.
  assert.equal(commandInvokesGit('powershell -Command "git push origin main"'), true);
  assert.equal(commandInvokesGit('powershell.exe -Command "git push"'), true);
  assert.equal(commandInvokesGit('pwsh -c "git push origin main"'), true);
  assert.equal(commandInvokesGit('cmd /c "git push origin main"'), true);
  assert.equal(commandInvokesGit('cmd.exe /c git status'), true);
  assert.equal(commandInvokesGit('bash.exe -c "git push"'), true);
});

test('unwrapping a Windows shell does not misread a path operand as a switch', () => {
  // `/usr/bin` must NOT be consumed as a `/x` cmd switch — the inner `git` is
  // still reached (regression guard against an over-broad `/`-switch matcher).
  assert.equal(commandInvokesGit('cmd /c "cd /usr/src && git status"'), true);
});

// ---------------------------------------------------------------------------
// End-to-end: a deny rule on the inner command blocks the wrapped form.
// ---------------------------------------------------------------------------

test('E2E: deny rule on inner command blocks Windows-shell-wrapped invocation', async () => {
  const rules = [{ tool: 'bash' as const, pattern: 'git push*', action: 'deny' as const }];
  const pm = new PermissionManager('auto-allow', rules);

  // Direct + Unix-shell forms were already denied (regression lock).
  assert.equal(await pm.check('bash', { command: 'git push origin main' }), 'deny');
  assert.equal(await pm.check('bash', { command: 'bash -c "git push origin main"' }), 'deny');

  // The bypass: these were ALLOWED before the fix because the Windows-shell
  // wrapper was not unwrapped, so the inner `git push` never matched the rule.
  assert.equal(await pm.check('bash', { command: 'powershell -Command "git push origin main"' }), 'deny');
  assert.equal(await pm.check('bash', { command: 'pwsh -c "git push origin main"' }), 'deny');
  assert.equal(await pm.check('bash', { command: 'cmd /c "git push origin main"' }), 'deny');
});

test('E2E: a non-matching inner command is still auto-allowed through a Windows shell', async () => {
  const rules = [{ tool: 'bash' as const, pattern: 'git push*', action: 'deny' as const }];
  const pm = new PermissionManager('auto-allow', rules);
  // `git status` does not match the `git push*` deny rule — unwrapping must not
  // over-block; it still auto-allows.
  assert.equal(await pm.check('bash', { command: 'pwsh -c "git status"' }), 'allow');
});
