import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  containsVulnerableUncPath,
  validateBashSecurity,
  validateSedDangerous,
  CHECK_IDS,
} from '../src/tools/bash-security.ts';

// ===========================================================================
// UNIT U015 — wave 67 parity additions to core/core/src/tools/bash-security.ts
//
//   Gap 9 (readOnlyValidation.ts containsVulnerableUncPath L1688-1692):
//     Windows UNC path (\\server\share, //server/share, WebDAV, IP/IPv6) makes
//     Windows resolve the host over SMB/WebDAV, triggering NTLM/Kerberos auth
//     (credential-hash theft / exfil) even for a "read-only" command. AMI had no
//     UNC detection. Added containsVulnerableUncPath + a validateBashSecurity
//     gate, platform-gated (UNC has no network side effect on POSIX).
//
//   Gap 8 (sedValidation.ts fail-closed on malformed quoting L247-260):
//     Claude tokenizes sed with a real shell parser and fails closed (deny) when
//     the parse throws. AMI extracts sed expressions with quote-delimited
//     regexes, so an unbalanced quote made that extraction unreliable (a w FILE /
//     e command could hide in the mis-tokenized tail). Now a sed segment whose
//     quotes do not balance is rejected rather than auto-approved.
// ===========================================================================

describe('U015-w67: containsVulnerableUncPath (gap 9)', () => {
  const win = 'win32' as NodeJS.Platform;
  const lin = 'linux' as NodeJS.Platform;

  it('detects backslash UNC paths on Windows', () => {
    assert.equal(containsVulnerableUncPath('cat \\\\server\\share\\file', win), true);
    assert.equal(containsVulnerableUncPath('\\\\foo.com\\x', win), true);
  });

  it('detects forward-slash UNC paths on Windows', () => {
    assert.equal(containsVulnerableUncPath('cat //server/share', win), true);
  });

  it('detects WebDAV SSL/port and DavWWWRoot markers on Windows', () => {
    assert.equal(containsVulnerableUncPath('cat \\\\host@SSL@8443\\p', win), true);
    assert.equal(containsVulnerableUncPath('cat \\\\host@8443@SSL\\p', win), true);
    assert.equal(containsVulnerableUncPath('cat \\\\host\\DavWWWRoot\\p', win), true);
  });

  it('detects IPv4 and bracketed IPv6 UNC hosts on Windows', () => {
    assert.equal(containsVulnerableUncPath('\\\\192.168.1.1\\share', win), true);
    assert.equal(containsVulnerableUncPath('//10.0.0.1/share', win), true);
    assert.equal(containsVulnerableUncPath('\\\\[2001:db8::1]\\share', win), true);
    assert.equal(containsVulnerableUncPath('//[::1]/p', win), true);
  });

  it('detects mixed-separator UNC paths produced by bash escape processing', () => {
    assert.equal(containsVulnerableUncPath('cat /\\\\server', win), true);
    assert.equal(containsVulnerableUncPath('cat \\\\/server', win), true);
  });

  it('does NOT misclassify URL schemes as UNC', () => {
    assert.equal(containsVulnerableUncPath('curl https://example.com/x', win), false);
    assert.equal(containsVulnerableUncPath('wget http://example.com', win), false);
  });

  it('returns false for a plain command with no UNC reference', () => {
    assert.equal(containsVulnerableUncPath('cat ./local/file', win), false);
  });

  it('is a no-op on non-Windows platforms (POSIX literal filename, no network)', () => {
    assert.equal(containsVulnerableUncPath('cat \\\\server\\share', lin), false);
    assert.equal(containsVulnerableUncPath('cat //server/share', lin), false);
  });
});

describe('U015-w67: validateBashSecurity UNC gate (gap 9, end-to-end)', () => {
  it('blocks a UNC path through the real security entry on Windows', () => {
    const r = validateBashSecurity('cat \\\\attacker.com\\share\\x', { platform: 'win32' as NodeJS.Platform });
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.UNC_PATH);
  });

  it('does not block the same command on POSIX (literal path, no SMB)', () => {
    const r = validateBashSecurity('cat //server/share', { platform: 'linux' as NodeJS.Platform });
    // No UNC failure on linux; command is otherwise benign.
    assert.notEqual(r.checkId, CHECK_IDS.UNC_PATH);
    assert.equal(r.safe, true);
  });

  it('UNC check runs before read-only-style auto-allow for a benign reader', () => {
    // `cat` of a UNC share would otherwise look read-only; it must still be blocked.
    const r = validateBashSecurity('cat \\\\10.0.0.9\\c$\\windows', { platform: 'win32' as NodeJS.Platform });
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.UNC_PATH);
  });
});

describe('U015-w67: sed fail-closed on malformed quoting (gap 8)', () => {
  it('rejects a sed command with an unbalanced single quote', () => {
    const r = validateSedDangerous("sed 's/a/b/ file");
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.SED_DANGEROUS);
  });

  it('rejects a sed command with an unbalanced double quote', () => {
    const r = validateSedDangerous('sed "s/a/b/ file');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.SED_DANGEROUS);
  });

  it('still allows a well-formed balanced sed substitution', () => {
    assert.equal(validateSedDangerous("sed 's/a/b/g' file").safe, true);
    assert.equal(validateSedDangerous('sed "s/a/b/" file').safe, true);
    assert.equal(validateSedDangerous("sed -e 's/x/y/' -e 's/p/q/' file").safe, true);
  });

  it('blocks malformed sed through the full validateBashSecurity entry', () => {
    // No command separator, so the malformed-token-injection check (which also
    // keys off unbalanced quotes but requires a separator) does not pre-empt it;
    // the sed fail-closed guard is what blocks here.
    const r = validateBashSecurity("sed 's/a/b/ /tmp/x");
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.SED_DANGEROUS);
  });
});
