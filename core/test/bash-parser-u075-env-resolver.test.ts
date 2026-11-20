import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  parseBashAST,
  extractCommands,
  extractCommandsFromString,
  PARSE_ABORTED,
  CommandNode,
  ParseEnv,
} from '../src/tools/bash-parser';

/**
 * U075 — caller-supplied environment for `$VAR` command-name resolution.
 *
 * Parity with the reference's `tryParseShellCommand(cmd, env?)`
 * (utils/bash/shellQuote.ts L24-L45): the parse/extraction entrypoints accept an
 * optional `env` that is EITHER a `Record<string,string|undefined>` OR a resolver
 * function `(key)=>string|undefined`, letting the caller control `$VAR` expansion
 * during command extraction. AMI resolves this where it matters for security: a
 * `$VAR` used as a COMMAND NAME that is not assigned inside the command itself is
 * resolved against the supplied env, so env-pinned command indirection
 * (`$EDITOR file` with `EDITOR=nc`) surfaces the real command for the AST
 * blocklist. Without env (default) behavior is unchanged: `$EDITOR` stays literal.
 */
describe('bash-parser U075: caller-supplied env for $VAR command-name resolution', () => {
  const ast = (c: string): CommandNode => {
    const a = parseBashAST(c, { timeoutMs: Infinity });
    assert.ok(a && a !== PARSE_ABORTED, `expected AST for ${c}`);
    return a as CommandNode;
  };

  it('default (no env) leaves an unassigned $VAR command name literal', () => {
    assert.deepEqual(extractCommands(ast('$EDITOR file')), ['$EDITOR']);
    assert.deepEqual(extractCommandsFromString('$EDITOR file'), ['$EDITOR']);
  });

  it('resolves a $VAR command name against an env Record', () => {
    const env: ParseEnv = { EDITOR: 'nc' };
    assert.deepEqual(extractCommands(ast('$EDITOR -e 10.0.0.1 4444'), env), ['nc']);
  });

  it('resolves a ${VAR} brace command name against an env Record', () => {
    assert.ok(extractCommands(ast('${TOOL} TCP:host:4444'), { TOOL: 'socat' }).includes('socat'));
  });

  it('resolves via a resolver FUNCTION env', () => {
    const resolver: ParseEnv = (k) => (k === 'RUN' ? 'insmod' : undefined);
    assert.deepEqual(extractCommands(ast('$RUN evil.ko'), resolver), ['insmod']);
  });

  it('threads env through extractCommandsFromString (3rd arg) with a budget', () => {
    assert.deepEqual(
      extractCommandsFromString('$EDITOR file', { timeoutMs: Infinity }, { EDITOR: 'vim' }),
      ['vim'],
    );
  });

  it('threads env through extractCommandsFromString with no explicit budget', () => {
    assert.deepEqual(
      extractCommandsFromString('$EDITOR file', undefined, { EDITOR: 'vim' }),
      ['vim'],
    );
  });

  it('IN-COMMAND assignment WINS over env (static scope has priority)', () => {
    // `A=ls` assigned inside the command shadows an env `A=nc`: `$A` → `ls`.
    const cmds = extractCommands(ast('A=ls; $A -la'), { A: 'nc' });
    assert.ok(cmds.includes('ls'));
    assert.ok(!cmds.includes('nc'));
  });

  it('a key absent from the env Record leaves the $VAR literal', () => {
    assert.deepEqual(extractCommands(ast('$MISSING x'), { OTHER: 'nc' }), ['$MISSING']);
  });

  it('a resolver returning undefined leaves the $VAR literal', () => {
    assert.deepEqual(extractCommands(ast('$MISSING x'), () => undefined), ['$MISSING']);
  });

  it('env does not affect a plain (non-$VAR) command name', () => {
    assert.deepEqual(extractCommands(ast('echo hi'), { echo: 'rm' }), ['echo']);
  });

  it('env resolves a $VAR command name used inside a subshell', () => {
    assert.ok(extractCommands(ast('($RUN evil.ko)'), { RUN: 'insmod' }).includes('insmod'));
  });
});
