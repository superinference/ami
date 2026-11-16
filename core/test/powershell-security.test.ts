import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  CLM_ALLOWED_TYPES,
  normalizeTypeName,
  isClmAllowedType,
  detectUnsafeTypeCast,
  CODE_EXEC_CMDLETS,
  MODULE_LOADING_CMDLETS,
  WMI_CIM_CMDLETS,
  ALIAS_HIJACK_CMDLETS,
  ENV_WRITE_CMDLETS,
  detectUnsafeNewObject,
  detectStartProcessElevation,
  detectRuntimeStateHijack,
  detectEnvVarManipulation,
  detectDangerousPsCmdlets,
  detectStopParsingToken,
  detectDangerousPowerShellCommand,
  NETWORK_CMDLETS,
  FILEPATH_EXECUTION_CMDLETS,
  DANGEROUS_SCRIPT_BLOCK_CMDLETS,
  ARG_GATED_CMDLETS,
  POWERSHELL_NEVER_SUGGEST,
  isPowerShellNeverSuggest,
} from '../src/tools/powershell-security';
import { validateBashSecurity, detectPowerShellExecution } from '../src/tools/bash-security';

// ---------------------------------------------------------------------------
// CLM_ALLOWED_TYPES — safe-type allowlist (config/default) + hardened removals
// ---------------------------------------------------------------------------
describe('CLM_ALLOWED_TYPES allowlist', () => {
  it('contains common safe accelerators and their System.* FQ equivalents', () => {
    for (const t of ['string', 'int', 'bool', 'hashtable', 'regex', 'pscustomobject']) {
      assert.equal(CLM_ALLOWED_TYPES.has(t), true, `${t} should be allowed`);
    }
    assert.equal(CLM_ALLOWED_TYPES.has('system.string'), true);
    assert.equal(CLM_ALLOWED_TYPES.has('system.int32'), true);
  });

  it('stores every entry lowercase (normalization invariant)', () => {
    for (const t of CLM_ALLOWED_TYPES) {
      assert.equal(t, t.toLowerCase(), `${t} must be stored lowercase`);
    }
  });

  it('SECURITY: omits network-binding accelerators (adsi/wmi/cimsession family)', () => {
    // Short-name accelerators that perform LDAP / remote-WMI / CIM network binds.
    for (const t of [
      'adsi',
      'adsisearcher',
      'wmi',
      'wmiclass',
      'wmisearcher',
      'cimsession',
    ]) {
      assert.equal(CLM_ALLOWED_TYPES.has(t), false, `${t} must be removed`);
    }
    // FQ forms with the same network-bind hazard.
    for (const t of [
      'system.directoryservices.directoryentry',
      'system.directoryservices.directorysearcher',
      'system.management.managementobject',
      'system.management.managementclass',
      'system.management.managementobjectsearcher',
      'microsoft.management.infrastructure.cimsession',
    ]) {
      assert.equal(CLM_ALLOWED_TYPES.has(t), false, `${t} FQ must be removed`);
    }
  });
});

// ---------------------------------------------------------------------------
// normalizeTypeName — options (lowercase, array-suffix, generic-bracket strip)
// ---------------------------------------------------------------------------
describe('normalizeTypeName', () => {
  it('lowercases the name', () => {
    assert.equal(normalizeTypeName('String'), 'string');
    assert.equal(normalizeTypeName('System.Int32'), 'system.int32');
  });

  it('strips an array suffix []', () => {
    assert.equal(normalizeTypeName('String[]'), 'string');
    assert.equal(normalizeTypeName('int[]'), 'int');
  });

  it('strips generic brackets (checks the outer type conservatively)', () => {
    assert.equal(normalizeTypeName('List[int]'), 'list');
    assert.equal(normalizeTypeName('Dictionary[string,int]'), 'dictionary');
  });

  it('trims surrounding whitespace', () => {
    assert.equal(normalizeTypeName('  string  '), 'string');
  });
});

// ---------------------------------------------------------------------------
// isClmAllowedType — edge cases feeding the allowlist lookup
// ---------------------------------------------------------------------------
describe('isClmAllowedType', () => {
  it('allows safe types regardless of case', () => {
    assert.equal(isClmAllowedType('string'), true);
    assert.equal(isClmAllowedType('STRING'), true);
    assert.equal(isClmAllowedType('String'), true);
  });

  it('allows arrays of allowed types (String[])', () => {
    assert.equal(isClmAllowedType('String[]'), true);
    assert.equal(isClmAllowedType('int[]'), true);
  });

  it('allows System.* FQ aliases of accelerators', () => {
    assert.equal(isClmAllowedType('System.String'), true);
    assert.equal(isClmAllowedType('system.int32'), true);
  });

  it('conservatively flags generic wrappers whose outer type is not allowed', () => {
    assert.equal(isClmAllowedType('List[int]'), false);
  });

  it('flags removed network-binding types as unsafe', () => {
    assert.equal(isClmAllowedType('adsi'), false);
    assert.equal(isClmAllowedType('wmi'), false);
    assert.equal(isClmAllowedType('cimsession'), false);
  });

  it('flags arbitrary dangerous .NET types as unsafe', () => {
    assert.equal(isClmAllowedType('System.Reflection.Assembly'), false);
    assert.equal(isClmAllowedType('System.Diagnostics.Process'), false);
    assert.equal(isClmAllowedType('System.Net.Sockets.TcpClient'), false);
  });
});

// ---------------------------------------------------------------------------
// detectUnsafeTypeCast — the CLM type-cast boundary scanner
// ---------------------------------------------------------------------------
describe('detectUnsafeTypeCast', () => {
  it('blocks a cast to a removed network-binding type', () => {
    const r = detectUnsafeTypeCast("[adsi]'LDAP://evil.com/dc=x'");
    assert.equal(r.blocked, true);
    assert.equal(r.type, 'adsi');
    assert.match(r.reason!, /non-allowlisted .NET type \[adsi\]/);
  });

  it('blocks a static-member access on a non-allowlisted type (::)', () => {
    const r = detectUnsafeTypeCast('[System.Reflection.Assembly]::Load($bytes)');
    assert.equal(r.blocked, true);
    assert.equal(r.type, 'System.Reflection.Assembly');
  });

  it('blocks a cast to a reflection/process type with a value', () => {
    assert.equal(detectUnsafeTypeCast('[System.Net.Sockets.TcpClient]$c').blocked, true);
    assert.equal(detectUnsafeTypeCast("[wmi]'\\\\host\\root\\cimv2:Win32_Process'").blocked, true);
  });

  it('allows a cast to an allowlisted safe type', () => {
    assert.equal(detectUnsafeTypeCast("[string]'hello'").blocked, false);
    assert.equal(detectUnsafeTypeCast('[int]$x').blocked, false);
    assert.equal(detectUnsafeTypeCast('[string[]]$arr').blocked, false);
    assert.equal(detectUnsafeTypeCast('[pscustomobject]@{ a = 1 }').blocked, false);
  });

  it('returns the FIRST non-allowlisted type when several casts appear', () => {
    const r = detectUnsafeTypeCast("[string]'ok'; [wmi]'bad'");
    assert.equal(r.blocked, true);
    assert.equal(r.type, 'wmi');
  });

  it('does NOT false-positive on bash array / glob / test-bracket syntax', () => {
    assert.equal(detectUnsafeTypeCast('arr[0]=1').blocked, false);
    assert.equal(detectUnsafeTypeCast('ls [a-z]*.txt').blocked, false);
    assert.equal(detectUnsafeTypeCast('echo "${arr[i]}"').blocked, false);
    assert.equal(detectUnsafeTypeCast('[ -f foo ] && echo yes').blocked, false);
    assert.equal(detectUnsafeTypeCast('[[ $x == y ]]').blocked, false);
  });
});

// ---------------------------------------------------------------------------
// Integration: wired into detectPowerShellExecution + validateBashSecurity so
// the CLM boundary reaches the real permission/ask path (fail-safe: not auto-
// approved -> ask). Red-green: reverting the detectUnsafeTypeCast call in
// detectPowerShellExecution flips these `safe: false` expectations back to true.
// ---------------------------------------------------------------------------
describe('CLM type-cast boundary wired into the permission path', () => {
  it('detectPowerShellExecution flags a non-allowlisted cast', () => {
    const r = detectPowerShellExecution("powershell -c \"[adsi]'LDAP://host'\"");
    assert.equal(r.blocked, true);
    assert.match(r.reason!, /Constrained Language Mode/);
  });

  it('validateBashSecurity treats a dangerous cast as not auto-approvable (ask)', () => {
    assert.equal(validateBashSecurity("[adsi]'LDAP://evil.com'").safe, false);
    assert.equal(
      validateBashSecurity('[System.Reflection.Assembly]::Load($b)').safe,
      false,
    );
  });

  it('validateBashSecurity keeps allowlisted casts and ordinary bash safe', () => {
    assert.equal(validateBashSecurity("powershell -c \"[string]'hi'\"").safe, true);
    assert.equal(validateBashSecurity('echo hello').safe, true);
    assert.equal(validateBashSecurity('ls [a-z]*.txt').safe, true);
  });
});

// ---------------------------------------------------------------------------
// Dangerous-cmdlet config lists (mirrors Claude's dangerousCmdlets.ts). The
// detectors below consume these, so they are the single source of truth.
// ---------------------------------------------------------------------------
describe('PowerShell dangerous-cmdlet config lists', () => {
  it('expose the canonical hazard-class sets, lowercase', () => {
    assert.equal(CODE_EXEC_CMDLETS.has('add-type'), true);
    assert.equal(MODULE_LOADING_CMDLETS.has('import-module'), true);
    assert.equal(MODULE_LOADING_CMDLETS.has('ipmo'), true);
    assert.equal(WMI_CIM_CMDLETS.has('invoke-wmimethod'), true);
    assert.equal(WMI_CIM_CMDLETS.has('invoke-cimmethod'), true);
    assert.equal(ALIAS_HIJACK_CMDLETS.has('set-alias'), true);
    assert.equal(ALIAS_HIJACK_CMDLETS.has('set-variable'), true);
    assert.equal(ENV_WRITE_CMDLETS.has('set-item'), true);
    for (const set of [
      CODE_EXEC_CMDLETS,
      MODULE_LOADING_CMDLETS,
      WMI_CIM_CMDLETS,
      ALIAS_HIJACK_CMDLETS,
      ENV_WRITE_CMDLETS,
    ]) {
      for (const n of set) assert.equal(n, n.toLowerCase());
    }
  });

  it('omit bash-colliding aliases to avoid POSIX-shell false positives', () => {
    for (const n of ['rm', 'del', 'rmdir', 'erase']) {
      assert.equal(ENV_WRITE_CMDLETS.has(n), false, `${n} must not be in ENV_WRITE_CMDLETS`);
    }
    // sv (runit) / nv collide with real POSIX commands and are dropped from the
    // unconditionally-blocking alias set.
    assert.equal(ALIAS_HIJACK_CMDLETS.has('sv'), false);
    assert.equal(ALIAS_HIJACK_CMDLETS.has('nv'), false);
  });
});

// ---------------------------------------------------------------------------
// New-Object: -ComObject + string -TypeName CLM allowlist check.
// ---------------------------------------------------------------------------
describe('detectUnsafeNewObject', () => {
  it('flags -ComObject (and abbreviations)', () => {
    assert.equal(detectUnsafeNewObject("New-Object -ComObject WScript.Shell").blocked, true);
    assert.equal(detectUnsafeNewObject("new-object -com Shell.Application").blocked, true);
  });

  it('flags a string -TypeName outside the CLM allowlist (positional + named)', () => {
    assert.equal(detectUnsafeNewObject('New-Object System.Net.WebClient').blocked, true);
    assert.equal(detectUnsafeNewObject('New-Object -TypeName Net.WebClient').blocked, true);
    assert.equal(detectUnsafeNewObject('New-Object -TypeName:System.Net.Sockets.TcpClient').blocked, true);
    // Named value-params before the positional TypeName are skipped.
    assert.equal(
      detectUnsafeNewObject('New-Object -ArgumentList 1,2 System.Net.WebClient').blocked,
      true,
    );
  });

  it('allows an allowlisted -TypeName and ignores new-object as a bare argument', () => {
    assert.equal(detectUnsafeNewObject('New-Object System.Collections.Hashtable').blocked, false);
    assert.equal(detectUnsafeNewObject('New-Object PSObject').blocked, false);
    assert.equal(detectUnsafeNewObject('Write-Output new-object').blocked, false);
  });
});

// ---------------------------------------------------------------------------
// Start-Process: elevation + nested-shell launch.
// ---------------------------------------------------------------------------
describe('detectStartProcessElevation', () => {
  it('flags -Verb RunAs (space and colon / quoted forms)', () => {
    assert.equal(detectStartProcessElevation('Start-Process notepad -Verb RunAs').blocked, true);
    assert.equal(detectStartProcessElevation("Start-Process cmd -Verb:'RunAs'").blocked, true);
    assert.equal(detectStartProcessElevation('saps app -v runas').blocked, true);
  });

  it('flags launching a nested PowerShell', () => {
    assert.equal(detectStartProcessElevation('Start-Process powershell -ArgumentList "-c",$x').blocked, true);
    assert.equal(detectStartProcessElevation('Start-Process -FilePath pwsh').blocked, true);
  });

  it('allows an ordinary Start-Process', () => {
    assert.equal(detectStartProcessElevation('Start-Process notepad.exe').blocked, false);
    assert.equal(detectStartProcessElevation('echo start-process').blocked, false);
  });
});

// ---------------------------------------------------------------------------
// Runtime-state hijack: alias / variable mutation.
// ---------------------------------------------------------------------------
describe('detectRuntimeStateHijack', () => {
  it('flags alias/variable mutation cmdlets, including module-qualified', () => {
    assert.equal(detectRuntimeStateHijack('Set-Alias Get-Content Invoke-Expression').blocked, true);
    assert.equal(detectRuntimeStateHijack('New-Variable x 1').blocked, true);
    assert.equal(
      detectRuntimeStateHijack('Microsoft.PowerShell.Utility\\Set-Alias ls rm').blocked,
      true,
    );
  });

  it('does not flag the cmdlet name as a plain argument', () => {
    assert.equal(detectRuntimeStateHijack('Write-Output set-alias').blocked, false);
    assert.equal(detectRuntimeStateHijack('echo hello').blocked, false);
  });
});

// ---------------------------------------------------------------------------
// Env-var manipulation: only when an env: reference is present.
// ---------------------------------------------------------------------------
describe('detectEnvVarManipulation', () => {
  it('flags $env: assignment and env: write cmdlets', () => {
    assert.equal(detectEnvVarManipulation('$env:Path = "C:\\evil"').blocked, true);
    assert.equal(detectEnvVarManipulation('Set-Item Env:Path "x"').blocked, true);
  });

  it('leaves [Environment]::SetEnvironmentVariable to the CLM cast check (no dup)', () => {
    // Not matched here (no env: scope token) but caught by detectUnsafeTypeCast.
    assert.equal(
      detectEnvVarManipulation("[Environment]::SetEnvironmentVariable('Path','x','User')").blocked,
      false,
    );
    assert.equal(
      detectUnsafeTypeCast("[Environment]::SetEnvironmentVariable('Path','x','User')").blocked,
      true,
    );
  });

  it('does not fire without an env: reference (reading / POSIX-shell safe)', () => {
    assert.equal(detectEnvVarManipulation('$x = $env:Path').blocked, false);
    assert.equal(detectEnvVarManipulation('echo $env:USERNAME').blocked, false);
    assert.equal(detectEnvVarManipulation('set-content file.txt "hi"').blocked, false);
  });
});

// ---------------------------------------------------------------------------
// Code-exec / module-load / WMI-spawn cmdlets.
// ---------------------------------------------------------------------------
describe('detectDangerousPsCmdlets', () => {
  it('flags Add-Type, module loads, and WMI/CIM spawns', () => {
    assert.equal(detectDangerousPsCmdlets('Add-Type -TypeDefinition $code').blocked, true);
    assert.equal(detectDangerousPsCmdlets('Import-Module .\\evil.psm1').blocked, true);
    assert.equal(
      detectDangerousPsCmdlets('Invoke-WmiMethod -Class Win32_Process -Name Create -ArgumentList calc').blocked,
      true,
    );
    assert.equal(detectDangerousPsCmdlets('Invoke-CimMethod -ClassName Win32_Process').blocked, true);
  });

  it('allows unrelated commands', () => {
    assert.equal(detectDangerousPsCmdlets('Get-ChildItem').blocked, false);
    assert.equal(detectDangerousPsCmdlets('echo import-module').blocked, false);
  });
});

// ---------------------------------------------------------------------------
// detectStopParsingToken — the `--%` stop-parsing / analysis-bypass boundary.
// Mirrors Claude Code's parser token check (MinusMinus / dash-normalized '--%').
// ---------------------------------------------------------------------------
describe('detectStopParsingToken', () => {
  it('flags a standalone --% stop-parsing token', () => {
    assert.equal(detectStopParsingToken('cmd --% /c whoami').blocked, true);
    assert.equal(detectStopParsingToken('pwsh --%').blocked, true);
    assert.match(detectStopParsingToken('foo --% bar').reason!, /stop-parsing/);
  });

  it('flags alt-dash variants (en-dash / em-dash / horizontal-bar)', () => {
    // PowerShell's tokenizer treats these as dash-equivalents.
    assert.equal(detectStopParsingToken('cmd ––% args').blocked, true);
    assert.equal(detectStopParsingToken('cmd ——% args').blocked, true);
    assert.equal(detectStopParsingToken('cmd ――% args').blocked, true);
  });

  it('flags the token after a statement/pipeline separator', () => {
    assert.equal(detectStopParsingToken('echo ok; cmd --% rest').blocked, true);
    assert.equal(detectStopParsingToken('a | cmd --% rest').blocked, true);
  });

  it('does not match --% as a substring of another token', () => {
    assert.equal(detectStopParsingToken('git log --%H').blocked, false);
    assert.equal(detectStopParsingToken("sed 's/--%/x/' file").blocked, false);
    assert.equal(detectStopParsingToken('printf -- %s done').blocked, false);
    assert.equal(detectStopParsingToken('Get-ChildItem').blocked, false);
  });
});

// ---------------------------------------------------------------------------
// Aggregate + end-to-end wiring into detectPowerShellExecution /
// validateBashSecurity. Red-green: reverting the detectDangerousPowerShellCommand
// call in detectPowerShellExecution flips every `safe:false` here back to true.
// ---------------------------------------------------------------------------
describe('detectDangerousPowerShellCommand aggregate + permission-path wiring', () => {
  it('returns the first matching detector reason', () => {
    assert.match(
      detectDangerousPowerShellCommand('New-Object System.Net.WebClient').reason!,
      /New-Object/,
    );
    assert.match(
      detectDangerousPowerShellCommand('Set-Alias a b').reason!,
      /alias or variable/,
    );
    assert.equal(detectDangerousPowerShellCommand('Get-ChildItem').blocked, false);
  });

  it('detectPowerShellExecution blocks the new dangerous patterns', () => {
    assert.equal(detectPowerShellExecution('New-Object System.Net.WebClient').blocked, true);
    assert.equal(detectPowerShellExecution('Start-Process notepad -Verb RunAs').blocked, true);
    assert.equal(detectPowerShellExecution('Add-Type -TypeDefinition $c').blocked, true);
    assert.equal(detectPowerShellExecution('$env:Path = "x"').blocked, true);
    // Alt-dash (en-dash) is normalized before the checks run.
    assert.equal(detectPowerShellExecution('Start-Process cmd \u2013Verb RunAs').blocked, true);
    // Stop-parsing token (--%) \u2014 remainder is opaque to analysis.
    assert.equal(detectPowerShellExecution('icacls C:\\ --% /grant everyone:F').blocked, true);
  });

  it('validateBashSecurity treats them as not auto-approvable, bash stays safe', () => {
    assert.equal(validateBashSecurity('New-Object System.Net.WebClient').safe, false);
    assert.equal(validateBashSecurity('Invoke-WmiMethod -Class Win32_Process -Name Create').safe, false);
    assert.equal(validateBashSecurity('reg query HKLM --% /v Foo').safe, false);
    assert.equal(validateBashSecurity('echo hello').safe, true);
    assert.equal(validateBashSecurity('rm file.txt').safe, true);
    assert.equal(validateBashSecurity('set-content out.txt "data"').safe, true);
    assert.equal(validateBashSecurity('git log --%H').safe, true);
  });
});

// ---------------------------------------------------------------------------
// Categorized dangerous-cmdlet config lists (network / filepath-exec /
// scriptblock) — mirrors Claude's dangerousCmdlets.ts data structures.
// ---------------------------------------------------------------------------
describe('categorized dangerous-cmdlet config lists', () => {
  it('NETWORK_CMDLETS covers the web-request/exfil cmdlets', () => {
    assert.equal(NETWORK_CMDLETS.has('invoke-webrequest'), true);
    assert.equal(NETWORK_CMDLETS.has('invoke-restmethod'), true);
  });

  it('FILEPATH_EXECUTION_CMDLETS covers -FilePath script runners', () => {
    for (const c of ['invoke-command', 'start-job', 'start-threadjob', 'register-scheduledjob']) {
      assert.equal(FILEPATH_EXECUTION_CMDLETS.has(c), true, c);
    }
  });

  it('DANGEROUS_SCRIPT_BLOCK_CMDLETS covers code-exec scriptblock sinks', () => {
    for (const c of [
      'invoke-command',
      'invoke-expression',
      'start-job',
      'new-pssession',
      'enter-pssession',
      'register-engineevent',
    ]) {
      assert.equal(DANGEROUS_SCRIPT_BLOCK_CMDLETS.has(c), true, c);
    }
  });

  it('ARG_GATED_CMDLETS covers allowlist callback-gated cmdlets + native exes', () => {
    for (const c of [
      'select-object',
      'where-object',
      'write-output',
      'start-sleep',
      'ipconfig',
      'hostname',
      'route',
    ]) {
      assert.equal(ARG_GATED_CMDLETS.has(c), true, c);
    }
    // foreach-object is handled separately in NEVER_SUGGEST (its -MemberName
    // positional arg has no allowlist entry to sync with), not in ARG_GATED.
    assert.equal(ARG_GATED_CMDLETS.has('foreach-object'), false);
  });

  it('all config-list entries are stored lowercase (no sync drift)', () => {
    for (const set of [
      NETWORK_CMDLETS,
      FILEPATH_EXECUTION_CMDLETS,
      DANGEROUS_SCRIPT_BLOCK_CMDLETS,
      ARG_GATED_CMDLETS,
    ]) {
      for (const entry of set) {
        assert.equal(entry, entry.toLowerCase(), `${entry} must be lowercase`);
      }
    }
  });
});

// ---------------------------------------------------------------------------
// NEVER_SUGGEST — commands never offered as a wildcard-prefix permission rule.
// ---------------------------------------------------------------------------
describe('POWERSHELL_NEVER_SUGGEST wildcard-prefix suggestion gate', () => {
  it('includes every dangerous-cmdlet category', () => {
    const canonical = [
      // process spawners / code loaders
      'start-process',
      'add-type',
      'new-object',
      // network
      'invoke-webrequest',
      'invoke-restmethod',
      // filepath / scriptblock execution
      'invoke-command',
      'start-job',
      'invoke-expression',
      'new-pssession',
      // module loading
      'import-module',
      'install-module',
      // wmi/cim
      'invoke-wmimethod',
      'invoke-cimmethod',
      // alias/variable hijack
      'set-alias',
      'new-alias',
      // arg-gated
      'foreach-object',
      'select-object',
    ];
    for (const c of canonical) {
      assert.equal(POWERSHELL_NEVER_SUGGEST.has(c), true, `${c} should be NEVER_SUGGEST`);
    }
  });

  it('includes alias forms derived from the shared COMMON_ALIASES map', () => {
    // iex→Invoke-Expression, iwr→Invoke-WebRequest, icm→Invoke-Command,
    // ipmo→Import-Module, iwmi→Invoke-WmiMethod, %→ForEach-Object.
    for (const a of ['iex', 'iwr', 'irm', 'icm', 'ipmo', 'iwmi', '%']) {
      assert.equal(POWERSHELL_NEVER_SUGGEST.has(a), true, `${a} alias should be NEVER_SUGGEST`);
    }
  });

  it('isPowerShellNeverSuggest is case-insensitive and matches canonical + alias', () => {
    assert.equal(isPowerShellNeverSuggest('Invoke-WebRequest'), true);
    assert.equal(isPowerShellNeverSuggest('INVOKE-EXPRESSION'), true);
    assert.equal(isPowerShellNeverSuggest('iwr'), true);
    assert.equal(isPowerShellNeverSuggest('ForEach-Object'), true);
  });

  it('does not block benign commands', () => {
    for (const c of ['git', 'npm', 'ls', 'cargo', 'get-childitem', 'get-content']) {
      assert.equal(isPowerShellNeverSuggest(c), false, c);
    }
  });
});
