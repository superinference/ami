/**
 * U015 / w90 — tokenization-based read-only `sed` classification.
 *
 * Replaces the raw-string `-i` regex in the bash permission classifier with a
 * per-segment, quote-aware tokenized check (`isReadOnlySedCommand`). Mirrors
 * Claude Code's shell-quote-tokenized isReadOnlySed (BashTool/sedValidation.ts):
 * a `sed` invocation is read-only (auto-approvable) only when it has NO in-place
 * write flag, detected by tokenizing each sed segment rather than substring-
 * matching any `-...i` anywhere on the line.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { isReadOnlySedCommand } from '../src/tools/bash-security';
import { PermissionManager } from '../src/permissions';

describe('isReadOnlySedCommand — tokenized read-only sed classification', () => {
  it('treats a stdout-only substitution as read-only', () => {
    assert.equal(isReadOnlySedCommand("sed 's/a/b/' file"), true);
    assert.equal(isReadOnlySedCommand("sed -E 's/a+/b/g' file"), true);
    assert.equal(isReadOnlySedCommand("cat f | sed -e 's/a/b/g'"), true);
  });

  it('rejects the -i in-place flag in all its spellings', () => {
    assert.equal(isReadOnlySedCommand("sed -i 's/a/b/' file"), false);
    assert.equal(isReadOnlySedCommand("sed --in-place 's/a/b/' file"), false);
    assert.equal(isReadOnlySedCommand("sed --in-place=.bak 's/a/b/' file"), false);
    assert.equal(isReadOnlySedCommand("sed -i.bak 's/a/b/' file"), false);
  });

  it('detects -i fused into a short-flag cluster', () => {
    assert.equal(isReadOnlySedCommand("sed -ni 's/a/b/p' file"), false);
    assert.equal(isReadOnlySedCommand("sed -nri 's/a/b/' file"), false);
  });

  it('does not treat a quoted "-i" inside the script as an in-place flag', () => {
    assert.equal(isReadOnlySedCommand("sed 's/-i/X/' f"), true);
  });

  it('honours -- end-of-options so a file literally named -i.txt is not an in-place flag', () => {
    assert.equal(isReadOnlySedCommand("sed 's/x/y/' -- -i.txt"), true);
  });

  it('is precise about which command owns the -i flag in a compound (regex false positive fix)', () => {
    // The old substring regex matched `ls -li` and refused the whole line; the
    // tokenized check scopes the in-place flag to the sed segment only.
    assert.equal(isReadOnlySedCommand("ls -li && sed 's/a/b/' f"), true);
  });

  it('returns false when no sed invocation is present', () => {
    assert.equal(isReadOnlySedCommand('grep foo bar'), false);
    assert.equal(isReadOnlySedCommand('cat file.txt'), false);
  });

  it('fails closed on an unbalanced-quote sed segment (cannot be tokenized safely)', () => {
    assert.equal(isReadOnlySedCommand("sed 's/a/b/ file"), false);
  });

  it('wires into the bash permission classifier end-to-end', () => {
    const pm = new PermissionManager();
    // read-only sed → auto-safe
    assert.equal(pm.classifyBashCommand("sed 's/a/b/' file"), 'safe');
    // in-place sed → still requires a prompt (existing invariant preserved)
    assert.equal(pm.classifyBashCommand('sed -i "s/a/b/" file.txt'), 'unsafe');
    // FAIL-CLOSED: an unbalanced-quote sed segment can no longer be tokenized, so
    // it is no longer auto-approved as read-only (the old substring regex, finding
    // no `-...i`, waved it through as 'safe'). This is the red-green discriminator:
    // reverting isReadOnlySedCommand back to the regex flips this to 'safe'.
    assert.equal(pm.classifyBashCommand("sed 's/a/b/ file"), 'unsafe');
  });
});
