import { test } from 'node:test';
import assert from 'node:assert/strict';

import { isReadOnlyPowerShellCommand } from '../src/tools/powershell-security';
import { PermissionManager } from '../src/permissions';

// ---------------------------------------------------------------------------
// U015-w86 — SSRF/XXE defense in the PowerShell read-only auto-allow classifier.
//
// Get-WinEvent is a read cmdlet, but its -FilterXml / -FilterHashtable flags
// accept XML whose DOCTYPE SYSTEM external entity triggers an outbound HTTP GET
// (XXE → SSRF). Before this change `Get-WinEvent -FilterXml evil.xml` was
// auto-allowed (silently exfiltrates / hits an attacker URL). Parity with
// Claude's readOnlyValidation.ts removal of -FilterXml/-FilterHashtable from
// Get-WinEvent's safeFlags; -FilterXPath (plain string pattern) stays allowed.
// ---------------------------------------------------------------------------

test('Get-WinEvent -FilterXml is NOT auto-allowed as read-only (XXE)', () => {
  assert.equal(isReadOnlyPowerShellCommand('Get-WinEvent -FilterXml evil.xml'), false);
});

test('Get-WinEvent -FilterHashtable is NOT auto-allowed as read-only', () => {
  assert.equal(isReadOnlyPowerShellCommand('Get-WinEvent -FilterHashtable x'), false);
});

test('Get-WinEvent flag match is case- and alt-dash-insensitive', () => {
  assert.equal(isReadOnlyPowerShellCommand('get-winevent -filterxml a.xml'), false);
  // En-dash prefix (PowerShell treats it as `-`).
  assert.equal(isReadOnlyPowerShellCommand('Get-WinEvent –FilterXml a.xml'), false);
});

test('benign Get-WinEvent forms stay auto-allowed', () => {
  assert.equal(isReadOnlyPowerShellCommand('Get-WinEvent -LogName System'), true);
  // -FilterXPath is a plain pattern (no external entities) and stays allowed.
  assert.equal(isReadOnlyPowerShellCommand('Get-WinEvent -FilterXPath "*"'), true);
});

test('the dangerous flag only disqualifies the owning cmdlet, not look-alikes', () => {
  // A different read cmdlet that happens to carry a -Filter* argument is unaffected.
  assert.equal(isReadOnlyPowerShellCommand('Get-ChildItem -Filter *.txt'), true);
});

test('Select-Xml / Test-Json / Get-WmiObject are not auto-allowed (not allowlisted)', () => {
  assert.equal(isReadOnlyPowerShellCommand('Select-Xml -Path a.xml -XPath /'), false);
  assert.equal(isReadOnlyPowerShellCommand('Test-Json -Json "{}"'), false);
  assert.equal(isReadOnlyPowerShellCommand('Get-WmiObject -Class Win32_PingStatus'), false);
});

test('end-to-end: PermissionManager does not classify Get-WinEvent -FilterXml as safe', () => {
  const pm = new PermissionManager();
  // The benign form auto-classifies as safe...
  assert.equal(pm.classifyBashCommand('Get-WinEvent -LogName System'), 'safe');
  // ...but the XXE-capable -FilterXml form must not be auto-allowed.
  assert.notEqual(pm.classifyBashCommand('Get-WinEvent -FilterXml evil.xml'), 'safe');
});
