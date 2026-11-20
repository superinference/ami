import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  validateMalformedTokenInjection,
  validateSedDangerous,
  validateBashSecurity,
  CHECK_IDS,
} from '../src/tools/bash-security.ts';

// ---------------------------------------------------------------------------
// U015 parity hardening for core/core/src/tools/bash-security.ts
//
// Gap A (bashSecurity.ts: malformed-token injection via real parse) —
//   AMI's validateMalformedTokenInjection used a naive `.split(quote).length`
//   quote counter that (1) miscounts quotes nested inside the other quote type
//   and (2) only fired when a quote was lexically adjacent to a separator. Claude
//   Code's hasMalformedTokens walks the raw command with BASH quoting semantics
//   (backslash escapes the next char outside single quotes; no escapes inside
//   single quotes) and flags an ODD count of unescaped `"` or `'` whenever a
//   command separator is present — independent of adjacency. This both FIXES a
//   false positive (`echo "it's fine" && ls` is well-formed) and strengthens
//   detection of unterminated quotes paired with a separator.
//
// Gap B (sedValidation.ts: conservative "when in doubt, unsafe") —
//   Claude's containsDangerousOperations conservatively rejects a sed script that
//   contains `{`/`}` blocks (too complex to reason about) or an embedded newline
//   (multi-line scripts are too complex). AMI auto-approved both. We port the
//   brace + embedded-newline rejection. (The crude `#`-comment rejection is
//   intentionally NOT ported: AMI deliberately allows `/^#/d` — a /regex/ address
//   matching a literal `#` — which is more precise than the reference's blanket block and
//   is locked by bash-security.test.ts; keeping it is an AMI superset, not a gap.)
// ---------------------------------------------------------------------------

describe('U015 Gap A — validateMalformedTokenInjection bash-aware quote balance', () => {
  it('passes a single quote NESTED inside double quotes with a separator (no false positive)', () => {
    // RED on the old naive counter: split("'").length is even -> flagged.
    const r = validateMalformedTokenInjection('echo "it\'s fine" && ls');
    assert.equal(r.safe, true, 'balanced quotes (apostrophe inside "") must not be flagged');
  });

  it('passes a double quote nested inside single quotes with a separator', () => {
    const r = validateMalformedTokenInjection("echo 'a \"b\" c' ; ls");
    assert.equal(r.safe, true);
  });

  it('blocks an unterminated single quote with a separator even when not adjacent', () => {
    // RED on the old regex: the `'` is not lexically adjacent to the `;`.
    const r = validateMalformedTokenInjection("echo 'unterminated ; rm -rf /");
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.MALFORMED_TOKEN);
  });

  it('blocks an unterminated double quote with a separator', () => {
    const r = validateMalformedTokenInjection('echo "oops && rm -rf /');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.MALFORMED_TOKEN);
  });

  it('does not flag a well-formed command that has no separator', () => {
    const r = validateMalformedTokenInjection("echo 'lone");
    assert.equal(r.safe, true, 'no separator -> no malformed-token concern');
  });

  it('treats a backslash-escaped quote outside single quotes as not opening a quote', () => {
    // \" is escaped, so the sole real `"` leaves an odd count with a separator.
    const r = validateMalformedTokenInjection('echo \\" "open ; ls');
    assert.equal(r.safe, false);
  });

  it('end-to-end through validateBashSecurity: well-formed nested quote is allowed', () => {
    const r = validateBashSecurity('echo "it\'s fine" && echo ok');
    assert.equal(r.safe, true);
  });
});

describe('U015 Gap B — sed conservative rejection of {} blocks and embedded newlines', () => {
  it('blocks a sed script containing a {} block (grouping is too complex to reason about)', () => {
    const r = validateSedDangerous("sed '1,5{/x/d}' file");
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.SED_DANGEROUS);
  });

  it('blocks a sed script with a bare brace block after an address', () => {
    const r = validateSedDangerous("sed '/foo/{p}' file");
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.SED_DANGEROUS);
  });

  it('blocks a sed expression with an embedded newline (multi-line script)', () => {
    const r = validateSedDangerous("sed '1a\\\ninjected' file");
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.SED_DANGEROUS);
  });

  it('end-to-end through validateBashSecurity: brace-block sed is not auto-approved', () => {
    const r = validateBashSecurity("sed '/pat/{s/a/b/}' file");
    assert.equal(r.safe, false);
  });

  // Regression lock: AMI's deliberate superset precision stays intact.
  it('still ALLOWS /^#/d (a /regex/ address matching a literal #) — AMI superset', () => {
    const r = validateSedDangerous("sed '/^#/d' file.txt");
    assert.equal(r.safe, true);
  });

  it('still allows a normal sed substitution', () => {
    assert.equal(validateSedDangerous("sed 's/a/b/g' file").safe, true);
  });
});
