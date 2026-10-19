import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { normalizeCommand, deobfuscateEnvExpansion, collapseWhitespace } from '../src/tools/command-normalize';

describe('collapseWhitespace', () => {
  it('collapses multiple spaces', () => {
    assert.equal(collapseWhitespace('ls   -la    /tmp'), 'ls -la /tmp');
  });

  it('trims leading/trailing whitespace', () => {
    assert.equal(collapseWhitespace('  ls  '), 'ls');
  });

  it('collapses tabs to spaces', () => {
    assert.equal(collapseWhitespace('ls\t-la'), 'ls -la');
  });
});

describe('deobfuscateEnvExpansion', () => {
  it('expands simple concatenation attempts', () => {
    const result = deobfuscateEnvExpansion('r""m -rf /');
    assert.ok(result.includes('rm'));
  });

  it('detects empty string concatenation', () => {
    const result = deobfuscateEnvExpansion("r''m -rf /");
    assert.ok(result.includes('rm'));
  });

  it('leaves normal strings unchanged', () => {
    assert.equal(deobfuscateEnvExpansion('echo "hello world"'), 'echo "hello world"');
  });
});

describe('normalizeCommand', () => {
  it('collapses whitespace', () => {
    assert.equal(normalizeCommand('ls   -la'), 'ls -la');
  });

  it('strips ANSI escape sequences', () => {
    const result = normalizeCommand('\x1b[31mrm -rf /\x1b[0m');
    assert.equal(result, 'rm -rf /');
  });

  it('strips inline comments', () => {
    assert.equal(normalizeCommand('rm -rf / # cleanup'), 'rm -rf /');
  });

  it('preserves hash in quotes', () => {
    const result = normalizeCommand('echo "# not a comment"');
    assert.ok(result.includes('# not a comment'));
  });

  it('normalizes backslash continuations', () => {
    assert.equal(normalizeCommand('rm \\\n-rf /'), 'rm -rf /');
  });

  it('handles empty string', () => {
    assert.equal(normalizeCommand(''), '');
  });

  it('preserves hash inside single quotes', () => {
    const result = normalizeCommand("echo '# comment in single quotes'");
    assert.ok(result.includes('# comment'));
  });

  it('strips comment at start of line', () => {
    assert.equal(normalizeCommand('# this is a comment'), '');
  });

  it('preserves hash that is not preceded by whitespace', () => {
    const result = normalizeCommand('echo foo#bar');
    assert.ok(result.includes('foo#bar'));
  });

  it('preserves meaningful command structure', () => {
    const cmd = 'git diff --stat HEAD~3..HEAD';
    assert.equal(normalizeCommand(cmd), cmd);
  });
});
