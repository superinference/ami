import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  validateSedDangerous,
  validateBashSecurity,
  CHECK_IDS,
} from '../src/tools/bash-security.ts';

// ---------------------------------------------------------------------------
// Parity hardening (U015): close two sed denylist evasions that AMI's
// address/delimiter-aware detectors leaked but Claude Code's
// containsDangerousOperations rejects (sedValidation.ts L488-L552):
//   A. backslash-delimited substitution carrying a write (`w`) / execute (`e`)
//      operation — AMI's `s(...)` detectors require a non-backslash delimiter
//      (`[^\\\n]`), so `s\pat\rep\w FILE` previously sailed through.
//   B. GNU relative/offset range addresses (`addr,+N`) before a `w`/`W`/`R`/`e`
//      command — AMI's SED_ADDR did not model the `,+N` second-operand form, so
//      `/pat/,+2 w FILE` evaded the address-prefixed write/exec detectors.
// ---------------------------------------------------------------------------

describe('validateSedDangerous — backslash-delimiter substitution evasion (A)', () => {
  it('blocks a backslash-delimited substitution with a write (w) flag', () => {
    const r = validateSedDangerous("sed 's\\a\\b\\w /tmp/evil' f");
    assert.ok(!r.safe, 'backslash-delim s///w must be blocked');
    assert.equal(r.checkId, CHECK_IDS.SED_DANGEROUS);
  });

  it('blocks a backslash-delimited substitution with an execute (e) flag', () => {
    const r = validateSedDangerous("sed 's\\a\\b\\e' f");
    assert.ok(!r.safe, 'backslash-delim s///e must be blocked');
    assert.equal(r.checkId, CHECK_IDS.SED_DANGEROUS);
  });

  it('blocks a backslash-delimited substitution with a trailing w command', () => {
    const r = validateSedDangerous("sed 's\\a\\b\\ w /tmp/evil' f");
    assert.ok(!r.safe, 'backslash-delim s with trailing w command must be blocked');
  });

  it('blocks an address-prefixed backslash-delimited substitution with w', () => {
    const r = validateSedDangerous("sed '/foo/s\\a\\b\\w /tmp/evil' f");
    assert.ok(!r.safe, 'address-prefixed backslash-delim s///w must be blocked');
  });

  it('does NOT over-block a legit substitution whose replacement contains s\\', () => {
    // The literal `s\` here is inside the replacement text, not a delimiter.
    const r = validateSedDangerous("sed 's/a/pas\\s/' f");
    assert.ok(r.safe, 'replacement text containing s-backslash must stay allowed');
  });

  it('does NOT over-block a harmless backslash-delimited substitution (no w/e)', () => {
    const r = validateSedDangerous("sed 's\\a\\b\\g' f");
    assert.ok(r.safe, 'backslash-delim substitution without a dangerous flag stays allowed');
  });
});

describe('validateSedDangerous — GNU offset range address evasion (B)', () => {
  it('blocks a GNU offset range (,+N) before a w command', () => {
    const r = validateSedDangerous("sed '/pat/,+2 w /tmp/evil' f");
    assert.ok(!r.safe, 'offset-range w command must be blocked');
    assert.equal(r.checkId, CHECK_IDS.SED_DANGEROUS);
  });

  it('blocks a GNU offset range (,+N) before an e command', () => {
    const r = validateSedDangerous("sed '/pat/,+2 e id' f");
    assert.ok(!r.safe, 'offset-range e command must be blocked');
  });

  it('blocks a numeric offset range (N,+M) before a W command', () => {
    const r = validateSedDangerous("sed '1,+3 W /tmp/evil' f");
    assert.ok(!r.safe, 'numeric offset-range W command must be blocked');
  });

  it('does NOT over-block a legit GNU offset delete (1,+2d)', () => {
    const r = validateSedDangerous("sed '1,+2d' f");
    assert.ok(r.safe, 'offset-range delete must stay allowed');
  });
});

describe('validateSedDangerous — regression: legit sed still allowed', () => {
  for (const cmd of [
    "sed 's/foo/bar/g' file.txt",
    "sed -n '5p' file.txt",
    "sed '/^#/d' file.txt",
    "sed -i 's/old/new/' src/main.ts",
    "sed 's|a|b|g' file.txt",
  ]) {
    it(`allows ${cmd}`, () => {
      assert.ok(validateSedDangerous(cmd).safe);
    });
  }
});

describe('validateBashSecurity — sed evasions blocked end-to-end', () => {
  it('blocks the backslash-delim write evasion through the top-level validator', () => {
    const r = validateBashSecurity("sed 's\\a\\b\\w /tmp/evil' f");
    assert.ok(!r.safe);
    assert.equal(r.checkId, CHECK_IDS.SED_DANGEROUS);
  });

  it('blocks the offset-range write evasion through the top-level validator', () => {
    const r = validateBashSecurity("sed '/pat/,+2 w /tmp/evil' f");
    assert.ok(!r.safe);
    assert.equal(r.checkId, CHECK_IDS.SED_DANGEROUS);
  });
});
