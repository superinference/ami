/**
 * Red-green reproducer for the nuclear/hardline blocklist obfuscation bypass.
 *
 * BUG: `deobfuscateEnvExpansion` existed but was never wired into the blocklist,
 * so a destructive command word split by empty quotes / empty expansions /
 * backslash escapes (r""m, r''m, r$()m, r${}m, r\m) executed as `rm` yet evaded
 * both detectNuclearCommand and detectHardlineCommand.
 *
 * RED: revert the deobfuscation wiring in bash-security.ts / permissions.ts (or
 * the strengthened deobfuscateEnvExpansion) and the "blocks obfuscated" cases
 * below fail (blocked === false).
 * GREEN: with the fix, every obfuscated form of `rm -rf /` is blocked, while
 * legitimate commands are not over-blocked.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { detectNuclearCommand } from '../src/tools/bash-security';
import { detectHardlineCommand } from '../src/permissions';
import { deobfuscateEnvExpansion } from '../src/tools/command-normalize';

const OBFUSCATED_RM_ROOT = [
  'r""m -rf /',
  "r''m -rf /",
  'r\\m -rf /',
  'r$()m -rf /',
  'r${}m -rf /',
  'r``m -rf /',
  'r""""m -rf /',        // stacked empty quotes collapse to a fixpoint
  'echo hi; r""m -rf /', // obfuscation inside a later segment
];

describe('nuclear blocklist resists command-word obfuscation', () => {
  for (const cmd of OBFUSCATED_RM_ROOT) {
    it(`detectNuclearCommand blocks ${JSON.stringify(cmd)}`, () => {
      assert.equal(detectNuclearCommand(cmd).blocked, true);
    });
  }

  it('plain rm -rf / stays blocked (control)', () => {
    assert.equal(detectNuclearCommand('rm -rf /').blocked, true);
  });
});

describe('hardline blocklist resists command-word obfuscation', () => {
  for (const cmd of ['r""m -rf /', "r''m -rf /", 'r\\m -rf /', 'r$()m -rf /']) {
    it(`detectHardlineCommand blocks ${JSON.stringify(cmd)}`, () => {
      assert.equal(detectHardlineCommand(cmd).blocked, true);
    });
  }
});

describe('obfuscation fix does not over-block legitimate commands', () => {
  for (const cmd of [
    'echo "hello world"',
    'ls -la',
    'grep -r "pattern" .',
    'rm -rf ./build',
    "awk 'BEGIN{print \"\"}'",
    'git commit -m "fix: normalize"',
  ]) {
    it(`allows ${JSON.stringify(cmd)}`, () => {
      assert.equal(detectNuclearCommand(cmd).blocked, false);
      assert.equal(detectHardlineCommand(cmd).blocked, false);
    });
  }
});

describe('deobfuscateEnvExpansion collapses obfuscation to a fixpoint', () => {
  it('collapses empty quotes', () => {
    assert.equal(deobfuscateEnvExpansion('r""m -rf /'), 'rm -rf /');
    assert.equal(deobfuscateEnvExpansion("r''m -rf /"), 'rm -rf /');
  });
  it('collapses stacked empty quotes', () => {
    assert.equal(deobfuscateEnvExpansion('r""""m'), 'rm');
  });
  it('collapses empty command substitution and parameter expansion', () => {
    assert.equal(deobfuscateEnvExpansion('r$()m'), 'rm');
    assert.equal(deobfuscateEnvExpansion('r${}m'), 'rm');
    assert.equal(deobfuscateEnvExpansion('r``m'), 'rm');
  });
  it('collapses backslash-escaped letters', () => {
    assert.equal(deobfuscateEnvExpansion('r\\m'), 'rm');
    assert.equal(deobfuscateEnvExpansion('ec\\ho'), 'echo');
  });
  it('leaves quoted content untouched', () => {
    assert.equal(deobfuscateEnvExpansion('echo "hello world"'), 'echo "hello world"');
  });
});
