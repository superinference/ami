import { test } from 'node:test';
import assert from 'node:assert/strict';
import { PermissionManager } from '../src/permissions';
import { canonicalizePowerShellCommandName } from '../src/tools/powershell-security';

// ---------------------------------------------------------------------------
// U003 / w105 — Canonical PowerShell alias cross-resolution in rule matching
// with the deny-over-match / allow-no-over-match asymmetry.
//
// the reference parity: PowerShellTool/powershellPermissions.ts
// filterRulesByContentsMatchingInput — a deny rule for `Remove-Item` also blocks
// `rm`/`del`/`ri`, a rule for `rm` blocks `Remove-Item`, matching is
// case-insensitive, and a module prefix is stripped for deny/ask (fail-safe
// over-match) but KEPT for allow (allow must never over-match a different
// module's cmdlet).
// ---------------------------------------------------------------------------

// --- pure helper: canonicalization + separator normalization + asymmetry -----

test('canonicalizePowerShellCommandName resolves an alias to its cmdlet', () => {
  assert.equal(canonicalizePowerShellCommandName('rm -rf x', true), 'remove-item -rf x');
  assert.equal(canonicalizePowerShellCommandName('gci somedir', true), 'get-childitem somedir');
  assert.equal(canonicalizePowerShellCommandName('del a b', true), 'remove-item a b');
});

test('canonicalizePowerShellCommandName normalizes a tab/multi-space separator to one space', () => {
  assert.equal(canonicalizePowerShellCommandName('rm\t./secret.txt', true), 'remove-item ./secret.txt');
  assert.equal(canonicalizePowerShellCommandName('rm   foo', true), 'remove-item foo');
});

test('canonicalizePowerShellCommandName strips a module prefix only when asked', () => {
  // deny/ask path (stripModule=true): module-qualified name reduces to the cmdlet
  assert.equal(
    canonicalizePowerShellCommandName('Microsoft.PowerShell.Management\\Remove-Item x', true),
    'remove-item x',
  );
  // allow path (stripModule=false): the module prefix is preserved (lowercased)
  assert.equal(
    canonicalizePowerShellCommandName('ModuleA\\Get-Thing x', false),
    'modulea\\get-thing x',
  );
});

test('canonicalizePowerShellCommandName leaves a non-alias / empty input shape intact', () => {
  assert.equal(canonicalizePowerShellCommandName('git status', true), 'git status');
  assert.equal(canonicalizePowerShellCommandName('   ', true), '   ');
  assert.equal(canonicalizePowerShellCommandName('', true), '');
});

// --- end-to-end through PermissionManager, with Windows platform mocked -------

async function withWin32<T>(fn: () => Promise<T>): Promise<T> {
  const original = process.platform;
  Object.defineProperty(process, 'platform', { value: 'win32', configurable: true });
  try {
    return await fn();
  } finally {
    Object.defineProperty(process, 'platform', { value: original, configurable: true });
  }
}

test('win32: a deny rule for the cmdlet also blocks the alias form (cross-alias deny)', async () => {
  await withWin32(async () => {
    const pm = new PermissionManager('auto-allow', [
      { tool: 'bash', pattern: 'Get-ChildItem *', action: 'deny', reason: 'user' },
    ]);
    // `gci` is the alias for Get-ChildItem — the deny rule must catch it.
    const r = await pm.checkWithReason('bash', { command: 'gci somedir' });
    assert.equal(r.decision, 'deny');
    assert.equal(r.reason?.type, 'rule');
  });
});

test('win32: deny strips a module prefix so a module-qualified deny rule still blocks the alias', async () => {
  await withWin32(async () => {
    const pm = new PermissionManager('auto-allow', [
      { tool: 'bash', pattern: 'App.Mgmt\\Get-ChildItem *', action: 'deny', reason: 'user' },
    ]);
    const r = await pm.checkWithReason('bash', { command: 'gci somedir' });
    assert.equal(r.decision, 'deny');
  });
});

test('win32: an allow rule written for the cmdlet auto-allows the alias form (cross-alias allow)', async () => {
  await withWin32(async () => {
    const pm = new PermissionManager('ask', [
      { tool: 'bash', pattern: 'Rename-Item *', action: 'allow', reason: 'user' },
    ]);
    // `ren` is the alias for Rename-Item — the allow rule must cover it.
    const r = await pm.checkWithReason('bash', { command: 'ren a b' });
    assert.equal(r.decision, 'allow');
  });
});

test('win32: allow does NOT strip a module prefix, so a differently-scoped allow rule never over-matches', async () => {
  await withWin32(async () => {
    const pm = new PermissionManager('ask', [
      { tool: 'bash', pattern: 'OtherModule\\Rename-Item *', action: 'allow', reason: 'user' },
    ]);
    const r = await pm.checkWithReason('bash', { command: 'ren a b' });
    // The module-qualified allow rule must NOT auto-allow the bare alias.
    assert.notEqual(r.decision, 'allow');
  });
});

test('posix: alias cross-resolution does NOT fire (a Linux `rm`/`echo` is never a PowerShell cmdlet)', async () => {
  // On a non-win32 platform (the CI host), an allow rule for a PowerShell cmdlet
  // must NOT auto-allow a POSIX alias — that would fail OPEN.
  assert.notEqual(process.platform, 'win32');
  const pm = new PermissionManager('ask', [
    { tool: 'bash', pattern: 'Rename-Item *', action: 'allow', reason: 'user' },
  ]);
  // `ren` → Rename-Item only under the Windows gate; here it must not match.
  const r = await pm.checkWithReason('bash', { command: 'ren a b' });
  assert.notEqual(r.decision, 'allow');
});
