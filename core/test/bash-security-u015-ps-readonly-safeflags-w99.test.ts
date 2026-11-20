import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  POWERSHELL_READONLY_CMDLETS,
  POWERSHELL_READONLY_CONFIG,
  COMMON_PARAMETERS,
  isReadOnlyPowerShellCommand,
} from '../src/tools/powershell-security';
import { PermissionManager } from '../src/permissions';

// ---------------------------------------------------------------------------
// U015-w99 — Per-cmdlet safe-flag gate for the PowerShell read-only auto-allow
// classifier (Claude readOnlyValidation.ts CMDLET_ALLOWLIST safeFlags /
// allowAllFlags + COMMON_PARAMETERS merge).
//
// Before this change `isReadOnlyPowerShellCommand` auto-allowed ANY flag on an
// allowlisted read cmdlet, so a remote/side-effecting flag on an otherwise
// read-only cmdlet (`Get-Process -ComputerName remote`, `Get-Service
// -ComputerName remote`, `Get-DnsClientCache -CimSession s`) was silently
// classified as safe. Parity with the reference: an allowlisted read cmdlet carrying a
// flag outside its read-only surface falls through to a prompt.
// ---------------------------------------------------------------------------

describe('isReadOnlyPowerShellCommand — per-cmdlet safe-flag gate', () => {
  it('rejects a read cmdlet carrying a flag outside its safeFlags set', () => {
    // -ComputerName connects to a remote host (network request) and is NOT in
    // Get-Process / Get-Service safeFlags — must not auto-allow.
    assert.equal(isReadOnlyPowerShellCommand('Get-Process -ComputerName remote'), false);
    assert.equal(isReadOnlyPowerShellCommand('Get-Service -ComputerName remote'), false);
    // -CimSession connects to a remote host — excluded from Get-DnsClientCache.
    assert.equal(isReadOnlyPowerShellCommand('Get-DnsClientCache -CimSession s'), false);
    // An entirely unknown flag on a read cmdlet also fails closed.
    assert.equal(isReadOnlyPowerShellCommand('Get-ChildItem -Bogus x'), false);
  });

  it('allows flags that are in the cmdlet safeFlags set', () => {
    assert.equal(isReadOnlyPowerShellCommand('Get-Content -Raw foo.txt'), true);
    assert.equal(isReadOnlyPowerShellCommand('Get-Process -Name pwsh'), true);
    assert.equal(isReadOnlyPowerShellCommand('Get-ChildItem -Recurse -Depth 2 -Path src'), true);
  });

  it('allows any flag on an allowAllFlags cmdlet (display-only surface)', () => {
    assert.equal(isReadOnlyPowerShellCommand('Format-Table -AutoSize'), true);
    assert.equal(isReadOnlyPowerShellCommand('Get-ComputerInfo -Property OsName'), true);
    assert.equal(
      isReadOnlyPowerShellCommand('Get-Process | Select-Object -First 5 -Property Name'),
      true,
    );
  });

  it('merges PowerShell COMMON_PARAMETERS into every cmdlet safe-flag set', () => {
    // -ErrorAction / -ErrorVariable etc. are available on every advanced cmdlet
    // and must be accepted even though no per-cmdlet safeFlags list them.
    assert.equal(isReadOnlyPowerShellCommand('Get-Content -ErrorAction Stop foo.txt'), true);
    assert.equal(isReadOnlyPowerShellCommand('Get-Process -ErrorVariable e'), true);
    assert.equal(isReadOnlyPowerShellCommand('Get-Service -Verbose'), true);
    // Sanity: the flag gate really is consulting COMMON_PARAMETERS, not just
    // allowing everything — a non-common unknown flag still rejects.
    assert.equal(isReadOnlyPowerShellCommand('Get-Service -NotACommonParam'), false);
  });

  it('still rejects the XXE-capable Get-WinEvent flags via safeFlags', () => {
    assert.equal(isReadOnlyPowerShellCommand('Get-WinEvent -FilterXml evil.xml'), false);
    assert.equal(isReadOnlyPowerShellCommand('Get-WinEvent -FilterHashtable x'), false);
    assert.equal(isReadOnlyPowerShellCommand('Get-WinEvent -LogName System'), true);
    assert.equal(isReadOnlyPowerShellCommand('Get-WinEvent -FilterXPath "*"'), true);
    // en-dash prefix is normalized to '-' before the flag check.
    assert.equal(isReadOnlyPowerShellCommand('Get-WinEvent –FilterXml a.xml'), false);
  });

  it('config and membership set are derived from one source (no drift)', () => {
    const configKeys = new Set(Object.keys(POWERSHELL_READONLY_CONFIG));
    assert.equal(configKeys.size, POWERSHELL_READONLY_CMDLETS.size);
    for (const name of POWERSHELL_READONLY_CMDLETS) {
      assert.ok(configKeys.has(name), `${name} missing a read-only config entry`);
    }
    // Every config entry declares exactly one of safeFlags / allowAllFlags, with
    // lowercase, leading-dash flag names (runtime comparison form).
    for (const [name, cfg] of Object.entries(POWERSHELL_READONLY_CONFIG)) {
      const hasFlags = Array.isArray(cfg.safeFlags);
      const allowAll = cfg.allowAllFlags === true;
      assert.ok(hasFlags || allowAll, `${name} must set safeFlags or allowAllFlags`);
      for (const flag of cfg.safeFlags ?? []) {
        assert.equal(flag, flag.toLowerCase(), `${name}: ${flag} must be lowercase`);
        assert.ok(flag.startsWith('-'), `${name}: ${flag} must lead with '-'`);
        // A per-cmdlet flag must not redundantly restate a COMMON_PARAMETER.
        assert.ok(
          !COMMON_PARAMETERS.has(flag),
          `${name}: ${flag} is a COMMON_PARAMETER (merged automatically)`,
        );
      }
    }
  });
});

describe('PermissionManager.classifyBashCommand — PowerShell safe-flag gate (e2e)', () => {
  it('does not auto-allow a remote-flag read cmdlet as safe', () => {
    const pm = new PermissionManager();
    // Benign read form is safe...
    assert.equal(pm.classifyBashCommand('Get-Process -Name pwsh'), 'safe');
    // ...but the remote (-ComputerName) form must not be auto-allowed.
    assert.notEqual(pm.classifyBashCommand('Get-Process -ComputerName remote'), 'safe');
    assert.notEqual(pm.classifyBashCommand('Get-Service -ComputerName remote'), 'safe');
  });
});
