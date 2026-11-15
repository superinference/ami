import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  normalizeCommand,
  deobfuscateEnvExpansion,
  collapseWhitespace,
  DEFAULT_MAX_DEOBFUSCATE_ITERATIONS,
} from '../src/tools/command-normalize';

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

  // Backslash-escape-aware quote tracking (parity: bashSecurity quote scanner).
  // An escaped double quote inside a double-quoted string must NOT close the
  // quote; otherwise a `#` genuinely inside the quote is mistaken for a comment
  // and a trailing `; rm -rf ~` is silently dropped from the text that security
  // checks inspect — an under-normalization bypass.
  it('does not drop a dangerous suffix after an escaped quote and in-quote hash', () => {
    // Shell: echo "x \" # " ; rm -rf ~
    const result = normalizeCommand('echo "x \\" # " ; rm -rf ~');
    assert.ok(
      result.includes('rm -rf ~'),
      `dangerous suffix must survive normalization, got: ${result}`,
    );
    assert.ok(result.includes(';'), `command separator must survive, got: ${result}`);
  });

  it('preserves a hash inside a double-quoted arg that contains an escaped quote', () => {
    // Shell: echo "a \" # b"  (a single double-quoted argument containing '#')
    const result = normalizeCommand('echo "a \\" # b"');
    assert.ok(result.includes('# b'), `in-quote hash must be preserved, got: ${result}`);
  });

  it('still strips a genuine inline comment that follows an escaped quote', () => {
    // Shell: echo "a\"b" # trailing comment
    // Quotes balance after the escaped quote, so the comment IS outside quotes.
    const result = normalizeCommand('echo "a\\"b" # trailing comment');
    assert.equal(result, 'echo "a\\"b"');
  });

  it('treats a backslash-escaped hash as literal, not a comment', () => {
    // Shell: echo foo \# bar  — the escaped '#' is not a comment delimiter.
    const result = normalizeCommand('echo foo \\# bar');
    assert.ok(result.includes('bar'), `escaped-hash content must survive, got: ${result}`);
  });

  it('does not treat backslash as an escape inside single quotes', () => {
    // In single quotes bash keeps the backslash literal; the closing quote still
    // closes, so a following ' # c' IS a comment.
    const result = normalizeCommand("echo 'a\\' # c");
    assert.equal(result, "echo 'a\\'");
  });
});

describe('deobfuscateEnvExpansion adversarial-input guard', () => {
  it('exposes a generous default iteration cap that never affects real input', () => {
    assert.ok(DEFAULT_MAX_DEOBFUSCATE_ITERATIONS >= 100);
  });

  it('fully converges a multi-pass collapse by default', () => {
    // '\\a' (backslash backslash a) needs two passes: \\a -> \a -> a.
    assert.equal(deobfuscateEnvExpansion('\\\\a'), 'a');
  });

  it('honors a maxIterations cap, returning the partially collapsed string', () => {
    // With a single pass, '\\a' collapses only once: \\a -> \a (not yet 'a').
    assert.equal(deobfuscateEnvExpansion('\\\\a', { maxIterations: 1 }), '\\a');
  });

  it('ignores a non-positive maxIterations and uses the safe default', () => {
    assert.equal(deobfuscateEnvExpansion('\\\\a', { maxIterations: 0 }), 'a');
    assert.equal(deobfuscateEnvExpansion('\\\\a', { maxIterations: -5 }), 'a');
  });

  it('terminates on pathological input within the cap', () => {
    const pathological = '\\'.repeat(5000) + 'a';
    const result = deobfuscateEnvExpansion(pathological, { maxIterations: 3 });
    // 3 passes strip 3 backslashes off the escaped run; the rest remain.
    assert.ok(result.endsWith('a'));
    assert.ok(result.length < pathological.length);
  });
});
