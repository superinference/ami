import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  CLM_ALLOWED_TYPES,
  normalizeTypeName,
  isClmAllowedType,
  detectUnsafeTypeCast,
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
