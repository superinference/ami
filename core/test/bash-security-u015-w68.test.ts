import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  validateMalformedTokenInjection,
  validateBashSecurity,
  CHECK_IDS,
} from '../src/tools/bash-security.ts';

// ---------------------------------------------------------------------------
// U015 (wave 68) parity hardening for core/core/src/tools/bash-security.ts
//
// Gap — balanced-delimiter arm of Claude Code's hasMalformedTokens
// (shellQuote.ts L145-161): a token with an unbalanced {}, (), or [] is a
// malformed token that can hide an injected command when re-tokenized. AMI's
// validateBraceExpansion already blocked the EXCESS-CLOSER direction
// (close > open) for `{}` only; the unclosed-OPENER direction for `{}`/`()`/`[]`
// was unguarded, so `echo {a; rm -rf /tmp/x` (unclosed `{` → the `rm` runs after
// the `;`) slipped through validateMalformedTokenInjection. This closes that half
// while preserving the existing quote-parity detection and avoiding false
// positives on `case … )`, `[ -f x ]`, subshells, and `$( … )`.
//
// A dangerous rm target is deliberately avoided in the literals below; the point
// is the structural-token classification, not the payload.
// ---------------------------------------------------------------------------

const SEP = '; echo pwned';

describe('U015 w68 — unclosed structural delimiter in malformed-token guard', () => {
  it('blocks an unclosed brace followed by a separator', () => {
    const r = validateMalformedTokenInjection(`echo {a${SEP}`);
    assert.equal(r.safe, false, 'unclosed { with a separator must be blocked');
    assert.equal(r.checkId, CHECK_IDS.MALFORMED_TOKEN);
  });

  it('blocks an unclosed parenthesis followed by a separator', () => {
    const r = validateMalformedTokenInjection(`echo (a && echo b`);
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.MALFORMED_TOKEN);
  });

  it('blocks an unclosed bracket followed by a separator', () => {
    const r = validateMalformedTokenInjection(`ls [a${SEP}`);
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.MALFORMED_TOKEN);
  });

  it('does not flag an unclosed opener WITHOUT a separator', () => {
    // No separator -> a malformed single command the shell just rejects; not an
    // injection vector, so no block (matches the quote-parity gating).
    const r = validateMalformedTokenInjection('echo {a');
    assert.equal(r.safe, true);
  });

  // --- No false positives on legitimate balanced constructs with separators ---

  it('allows balanced test brackets chained with &&', () => {
    const r = validateMalformedTokenInjection('[ -f a ] && [ -f b ] && ls');
    assert.equal(r.safe, true);
  });

  it('allows a case statement (excess closers, not opener-excess)', () => {
    const r = validateMalformedTokenInjection('case $x in a) ls;; b) pwd;; esac');
    assert.equal(r.safe, true);
  });

  it('allows a balanced subshell group with a separator', () => {
    const r = validateMalformedTokenInjection('(cd /tmp && ls); echo done');
    assert.equal(r.safe, true);
  });

  it('allows command substitution and arithmetic with separators', () => {
    assert.equal(validateMalformedTokenInjection('echo $(date); ls').safe, true);
    assert.equal(validateMalformedTokenInjection('echo $((1 + 2)); ls').safe, true);
  });

  it('allows for ((;;)) C-style loops', () => {
    const r = validateMalformedTokenInjection('for ((i=0; i<3; i++)); do echo $i; done');
    assert.equal(r.safe, true);
  });

  it('does not count delimiters inside quotes (awk/grep programs)', () => {
    assert.equal(validateMalformedTokenInjection("awk '{print $1}' f; ls").safe, true);
    assert.equal(validateMalformedTokenInjection("grep '[a-z]*' f && ls").safe, true);
  });

  it('treats a backslash-escaped opener as a literal (not an unclosed delimiter)', () => {
    const r = validateMalformedTokenInjection('echo \\{ ; ls');
    assert.equal(r.safe, true);
  });

  it('end-to-end through validateBashSecurity: unclosed brace + separator is blocked', () => {
    const r = validateBashSecurity(`echo {a${SEP}`);
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.MALFORMED_TOKEN);
  });

  it('end-to-end: a legitimate test-and-run compound is still allowed', () => {
    const r = validateBashSecurity('[ -f package.json ] && echo ok');
    assert.equal(r.safe, true);
  });
});
