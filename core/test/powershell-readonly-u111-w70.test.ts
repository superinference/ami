import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  POWERSHELL_READONLY_CMDLETS,
  isReadOnlyPowerShellCommand,
} from '../src/tools/powershell-security';
import { PermissionManager } from '../src/permissions';

// ---------------------------------------------------------------------------
// U111-w70 — PowerShell read-only cmdlet allowlist (CMDLET_ALLOWLIST /
// isReadOnlyCommand parity). The Windows counterpart of the POSIX read-only
// SAFE_COMMANDS auto-allow: a command composed solely of allowlisted read-only
// cmdlets with no dangerous construct is auto-allowed in `ask` mode.
// ---------------------------------------------------------------------------

describe('isReadOnlyPowerShellCommand — allowlisted read-only cmdlets', () => {
  it('allows a single read-only cmdlet', () => {
    assert.equal(isReadOnlyPowerShellCommand('Get-Content foo.txt'), true);
    assert.equal(isReadOnlyPowerShellCommand('Get-ChildItem -Recurse -Path src'), true);
    assert.equal(isReadOnlyPowerShellCommand('Test-Path C:\\temp'), true);
    assert.equal(isReadOnlyPowerShellCommand('Select-String -Pattern foo file.txt'), true);
    assert.equal(isReadOnlyPowerShellCommand('Get-Process'), true);
  });

  it('is case-insensitive and strips a module qualifier / .exe', () => {
    assert.equal(isReadOnlyPowerShellCommand('get-content foo.txt'), true);
    assert.equal(
      isReadOnlyPowerShellCommand('Microsoft.PowerShell.Management\\Get-ChildItem'),
      true,
    );
    assert.equal(isReadOnlyPowerShellCommand('Get-Process.exe'), true);
  });

  it('allows a pipeline of only read-only cmdlets', () => {
    assert.equal(
      isReadOnlyPowerShellCommand('Get-Process | Select-Object Name | Sort-Object'),
      true,
    );
    assert.equal(
      isReadOnlyPowerShellCommand('Get-ChildItem | Where-Object Name -like README'),
      true,
    );
  });
});

describe('isReadOnlyPowerShellCommand — fail-closed rejections', () => {
  it('rejects an empty / blank command', () => {
    assert.equal(isReadOnlyPowerShellCommand(''), false);
    assert.equal(isReadOnlyPowerShellCommand('   '), false);
  });

  it('rejects a write / execution cmdlet not on the allowlist', () => {
    assert.equal(isReadOnlyPowerShellCommand('Remove-Item foo'), false);
    assert.equal(isReadOnlyPowerShellCommand('Set-Content a b'), false);
    assert.equal(isReadOnlyPowerShellCommand('Out-File -FilePath x'), false);
    assert.equal(isReadOnlyPowerShellCommand('Invoke-Expression foo'), false);
  });

  it('rejects any variable / subexpression / env reference ($)', () => {
    assert.equal(isReadOnlyPowerShellCommand('Get-Content $env:SECRET'), false);
    assert.equal(isReadOnlyPowerShellCommand('Get-ChildItem $(Get-Location)'), false);
  });

  it('rejects script blocks, grouping, splatting, call-operator, backtick', () => {
    assert.equal(isReadOnlyPowerShellCommand('Get-Process | Where-Object { $_.CPU }'), false);
    assert.equal(isReadOnlyPowerShellCommand('Get-Date (Get-Date)'), false);
    assert.equal(isReadOnlyPowerShellCommand('Get-ChildItem @splat'), false);
    assert.equal(isReadOnlyPowerShellCommand('& Get-Content foo'), false);
    assert.equal(isReadOnlyPowerShellCommand('Get-Content `nfoo'), false);
  });

  it('rejects output redirection (a file write)', () => {
    assert.equal(isReadOnlyPowerShellCommand('Get-ChildItem > out.txt'), false);
    assert.equal(isReadOnlyPowerShellCommand('Get-Content a.txt >> log.txt'), false);
  });

  it('rejects multi-statement commands (;)', () => {
    assert.equal(isReadOnlyPowerShellCommand('Get-Process; Remove-Item x'), false);
  });

  it('rejects a pipeline whose later segment is not read-only', () => {
    assert.equal(isReadOnlyPowerShellCommand('Get-Content x.ps1 | Invoke-Expression'), false);
  });

  it('rejects a leading flag / empty pipeline segment', () => {
    assert.equal(isReadOnlyPowerShellCommand('-Path foo'), false);
    assert.equal(isReadOnlyPowerShellCommand('Get-Process |'), false);
  });

  it('defense-in-depth: a dangerous construct inside an allowlisted cmdlet still rejects', () => {
    // New-Object is not on the allowlist, but even an allowlisted cmdlet carrying
    // an unsafe type-cast must not auto-allow (detectUnsafeTypeCast backstop).
    assert.equal(isReadOnlyPowerShellCommand('New-Object System.Net.WebClient'), false);
  });

  it('allowlist holds only hyphenated cmdlet names (no bare bash-colliding aliases)', () => {
    for (const name of POWERSHELL_READONLY_CMDLETS) {
      assert.ok(name.includes('-'), `${name} must be a hyphenated Verb-Noun cmdlet`);
      assert.equal(name, name.toLowerCase(), `${name} must be stored lowercase`);
    }
  });
});

describe('PermissionManager.classifyBashCommand — PowerShell read-only auto-allow (integration)', () => {
  it('auto-allows a plain read-only PowerShell command as safe', () => {
    const pm = new PermissionManager();
    assert.equal(pm.classifyBashCommand('Get-Content README.md'), 'safe');
    assert.equal(pm.classifyBashCommand('Get-ChildItem -Recurse'), 'safe');
    assert.equal(
      pm.classifyBashCommand('Get-Process | Select-Object Name'),
      'safe',
    );
  });

  it('does not downgrade a destructive PowerShell command', () => {
    const pm = new PermissionManager();
    assert.equal(pm.classifyBashCommand('Remove-Item -Recurse foo'), 'destructive');
  });

  it('a read of a credential path is still elevated to unsafe', () => {
    const pm = new PermissionManager();
    assert.equal(pm.classifyBashCommand('Get-Content ~/.ssh/id_rsa'), 'unsafe');
  });

  it('a non-allowlisted PowerShell cmdlet is not auto-allowed', () => {
    const pm = new PermissionManager();
    assert.equal(pm.classifyBashCommand('Set-Content a.txt b'), 'unsafe');
  });
});
