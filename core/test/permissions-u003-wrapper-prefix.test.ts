import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  unwrapWrapperChain,
  WRAPPER_PREFIX_COMMANDS,
} from '../src/tools/command-prefix';
import { suggestAllowPrefix } from '../src/permissions';

// ---------------------------------------------------------------------------
// U003 — wrapper-aware permission-rule prefix derivation
// (parity: references/claude/claude-code-main/src/utils/bash/prefix.ts
//  getCommandPrefixStatic / handleWrapper — bounded recursion + composed prefix)
// ---------------------------------------------------------------------------

test('WRAPPER_PREFIX_COMMANDS covers safe wrappers + prefix-only wrappers', () => {
  for (const w of ['timeout', 'nice', 'env', 'stdbuf', 'nohup', 'sudo', 'doas', 'xargs', 'watch']) {
    assert.ok(WRAPPER_PREFIX_COMMANDS.has(w), `${w} should be a wrapper`);
  }
  // A real command is NOT a wrapper.
  assert.ok(!WRAPPER_PREFIX_COMMANDS.has('git'));
});

test('unwrapWrapperChain returns null for a non-wrapped command', () => {
  assert.equal(unwrapWrapperChain('git commit -m x'), null);
  assert.equal(unwrapWrapperChain('npm run build'), null);
  assert.equal(unwrapWrapperChain(''), null);
});

test('unwrapWrapperChain strips a single wrapper and its operands', () => {
  assert.deepEqual(unwrapWrapperChain('nice -n5 git log'), { wrappers: ['nice'], inner: 'git log' });
  assert.deepEqual(unwrapWrapperChain('nice -n 5 git log'), { wrappers: ['nice'], inner: 'git log' });
  assert.deepEqual(unwrapWrapperChain('nice 10 git log'), { wrappers: ['nice'], inner: 'git log' });
  assert.deepEqual(unwrapWrapperChain('timeout 5 npm test'), { wrappers: ['timeout'], inner: 'npm test' });
  assert.deepEqual(unwrapWrapperChain('timeout -s KILL 30s git fetch'), { wrappers: ['timeout'], inner: 'git fetch' });
  assert.deepEqual(unwrapWrapperChain('sudo apt-get install foo'), { wrappers: ['sudo'], inner: 'apt-get install foo' });
  assert.deepEqual(unwrapWrapperChain('sudo -u root git status'), { wrappers: ['sudo'], inner: 'git status' });
  // absolute wrapper path is normalized to its bare name
  assert.deepEqual(unwrapWrapperChain('/usr/bin/sudo git status'), { wrappers: ['sudo'], inner: 'git status' });
});

test('unwrapWrapperChain composes a bounded nested chain', () => {
  assert.deepEqual(unwrapWrapperChain('nice sudo git commit'), { wrappers: ['nice', 'sudo'], inner: 'git commit' });
});

test('unwrapWrapperChain fails closed past the wrapper-count bound', () => {
  // 3 wrappers > MAX_WRAPPER_COUNT (2) → null (anti-obfuscation).
  assert.equal(unwrapWrapperChain('nice sudo env git commit'), null);
});

test('unwrapWrapperChain fails closed on a bare/empty wrapper run', () => {
  assert.equal(unwrapWrapperChain('sudo'), null);
  assert.equal(unwrapWrapperChain('nice sudo'), null);
});

test('unwrapWrapperChain fails closed on an unrecognised env flag (argv splitter)', () => {
  // `env -S` can split/relocate the command — do not trust a derived prefix.
  assert.equal(unwrapWrapperChain("env -S 'git status' foo"), null);
});

test('unwrapWrapperChain skips env assignments and safe env flags', () => {
  assert.deepEqual(unwrapWrapperChain('env FOO=bar git log'), { wrappers: ['env'], inner: 'git log' });
  assert.deepEqual(unwrapWrapperChain('env -u SECRET git log'), { wrappers: ['env'], inner: 'git log' });
});

// --- composed suggestion through the real suggestAllowPrefix hook --------------

test('suggestAllowPrefix composes wrapper + inner subcommand prefix', () => {
  assert.equal(suggestAllowPrefix('nice -n5 git log --oneline'), 'nice git log');
  assert.equal(suggestAllowPrefix('timeout 5 npm run build'), 'timeout npm run');
  assert.equal(suggestAllowPrefix('sudo apt-get install foo'), 'sudo apt-get install');
  assert.equal(suggestAllowPrefix('nice sudo git commit -m x'), 'nice sudo git commit');
});

test('suggestAllowPrefix refuses a wrapped shell/interpreter inner command', () => {
  // The inner command is a bare shell — must never anchor an auto-allow rule even
  // when wrapped, so no over-broad `sudo bash` rule is offered.
  assert.equal(suggestAllowPrefix("sudo bash -c 'rm -rf /'"), null);
  assert.equal(suggestAllowPrefix('nice node -e "process.exit(1)"'), null);
});

test('suggestAllowPrefix is unchanged for non-wrapped commands', () => {
  // regression lock: the wrapper hook must not perturb the existing contract.
  assert.equal(suggestAllowPrefix('git commit -m "fix"'), 'git commit');
  assert.equal(suggestAllowPrefix('npm run build'), 'npm run');
  assert.equal(suggestAllowPrefix('ls -la'), 'ls');
});
