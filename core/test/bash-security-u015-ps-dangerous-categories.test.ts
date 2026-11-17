import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  detectPersistenceCommands,
  detectPowerShellExecution,
  validateBashSecurity,
} from '../src/tools/bash-security';

// ---------------------------------------------------------------------------
// U015 — PowerShell / cross-platform dangerous-command security categories.
//
// Parity with Claude Code's yoloClassifier POWERSHELL_DENY_GUIDANCE
// (references/claude/claude-code-main/src/utils/permissions/yoloClassifier.ts
// L1402-L1411): AMI already blocked the "Code from External" download-and-execute
// cradles, but NOT the Persistence (profile / service / Run-key / WMI event) and
// Security-Weaken / Elevation (execution-policy bypass, Defender tamper, AMSI
// bypass) categories. These are now detected by the existing deny-layer
// (detectPersistenceCommands + detectPowerShellExecution), wired through
// validateBashSecurity → bash.ts.
//
// FUNCTIONAL CHANGE (labeled): commands in these categories that were previously
// auto-approvable are now hard-denied, consistent with AMI's existing
// cron/schtasks/systemd persistence and sudo/setuid privilege-escalation blocks.
// ---------------------------------------------------------------------------

describe('U015 PS persistence — profile / service / Run-key / WMI event', () => {
  const blocked: Array<[string, string]> = [
    ['New-Service -Name evil -BinaryPathName C:\\e.exe', 'New-Service (Windows service)'],
    ['New-Service evil C:\\e.exe', 'New-Service positional'],
    ['sc.exe create evil binPath= c:\\e.exe', 'sc.exe create'],
    ['sc create evil binPath= c:\\e.exe', 'sc create'],
    [
      "Set-ItemProperty 'HKCU:\\Software\\Microsoft\\Windows\\CurrentVersion\\Run' x c:\\e.exe",
      'Set-ItemProperty Run key',
    ],
    [
      'New-ItemProperty HKLM:\\Software\\Microsoft\\Windows\\CurrentVersion\\RunOnce -Name x -Value c:\\e.exe',
      'New-ItemProperty RunOnce key',
    ],
    [
      'reg add HKCU\\Software\\Microsoft\\Windows\\CurrentVersion\\Run /v x /d c:\\e.exe',
      'reg add Run key',
    ],
    ['reg.exe add "HKLM\\...\\CurrentVersion\\Run" /v x /d c:\\e.exe', 'reg.exe add Run key'],
    ['Add-Content $PROFILE "calc"', 'Add-Content $PROFILE'],
    ['Set-Content $PROFILE -Value "calc"', 'Set-Content $PROFILE'],
    ['Out-File -FilePath $PROFILE -InputObject "x"', 'Out-File $PROFILE'],
    ["Register-WmiEvent -Query 'q' -Action {calc}", 'Register-WmiEvent'],
    ["Register-CimIndicationEvent -Query 'q' -Action {calc}", 'Register-CimIndicationEvent'],
    ['Set-WmiInstance -Class __EventFilter -Arguments @{}', 'Set-WmiInstance __EventFilter'],
    [
      'Set-WmiInstance -Class CommandLineEventConsumer -Arguments @{}',
      'Set-WmiInstance CommandLineEventConsumer',
    ],
  ];
  for (const [cmd, label] of blocked) {
    it(`blocks ${label}`, () => {
      assert.equal(detectPersistenceCommands(cmd).blocked, true, cmd);
      // End-to-end through the real security entry point used by bash.ts.
      assert.equal(validateBashSecurity(cmd).safe, false, cmd);
    });
  }

  const allowed = [
    'Get-Service',
    'sc query evil',
    'reg query HKCU\\Software\\Microsoft\\Windows\\CurrentVersion\\Run',
    'Get-ItemProperty HKCU:\\Software\\Microsoft\\Windows\\CurrentVersion\\Run',
    'Set-Content notes.txt "hello"',
    'Add-Content log.txt "line"',
    'Set-WmiInstance -Class Win32_Environment -Arguments @{}',
    // write-cmdlet present but NOT a Run-key target (exercises the AND false arm)
    'Set-ItemProperty config.json name value',
    // $PROFILE referenced but no write cmdlet (reading the profile path is benign)
    'Write-Output $PROFILE',
  ];
  for (const cmd of allowed) {
    it(`allows benign: ${cmd}`, () => {
      assert.equal(detectPersistenceCommands(cmd).blocked, false, cmd);
    });
  }
});

describe('U015 PS elevation / security-weaken — exec-policy / Defender / AMSI', () => {
  const blocked: Array<[string, string]> = [
    ['powershell -ExecutionPolicy Bypass -File x.ps1', '-ExecutionPolicy Bypass'],
    ['pwsh -ep Unrestricted -c hi', '-ep Unrestricted'],
    ['powershell -exec bypass -c hi', '-exec bypass (abbrev)'],
    ['powershell -ExecutionPolicy:Bypass', 'colon-bound'],
    ['powershell –ExecutionPolicy Bypass -File x.ps1', 'en-dash prefix'],
    ['Set-ExecutionPolicy Bypass -Scope CurrentUser', 'Set-ExecutionPolicy cmdlet'],
    ['Set-MpPreference -DisableRealtimeMonitoring $true', 'Defender disable realtime'],
    ['Set-MpPreference -DisableBehaviorMonitoring 1', 'Defender disable behavior'],
    ['Add-MpPreference -ExclusionPath C:\\temp', 'Defender exclusion path'],
    ['Set-MpPreference -MAPSReporting Disabled', 'Defender MAPS disabled'],
    [
      "[Ref].Assembly.GetType('System.Management.Automation.AmsiUtils')",
      'AMSI bypass (AmsiUtils)',
    ],
    ['$x = amsiInitFailed', 'AMSI bypass (amsiInitFailed)'],
  ];
  for (const [cmd, label] of blocked) {
    it(`blocks ${label}`, () => {
      assert.equal(detectPowerShellExecution(cmd).blocked, true, cmd);
      assert.equal(validateBashSecurity(cmd).safe, false, cmd);
    });
  }

  const allowed = [
    'powershell -ExecutionPolicy RemoteSigned -File x.ps1',
    'pwsh -NoProfile -c "Get-Date"',
    'Get-MpPreference',
    // Set-MpPreference present but a benign (non-disable/exclusion) option —
    // exercises the AND false arm of the Defender-tamper check.
    'Set-MpPreference -ScanScheduleDay Everyday',
    'powershell -File build.ps1',
    'pwsh -Command Get-Date',
  ];
  for (const cmd of allowed) {
    it(`allows benign: ${cmd}`, () => {
      assert.equal(detectPowerShellExecution(cmd).blocked, false, cmd);
    });
  }
});

describe('U015 PS categories — compound-command coverage', () => {
  it('blocks a persistence command hidden after a benign prefix', () => {
    assert.equal(
      validateBashSecurity('echo hi && New-Service -Name evil -BinaryPathName c:\\e.exe').safe,
      false,
    );
  });
  it('blocks an exec-policy bypass hidden after a pipe', () => {
    assert.equal(validateBashSecurity('Get-Date; Set-ExecutionPolicy Bypass').safe, false);
  });
});
