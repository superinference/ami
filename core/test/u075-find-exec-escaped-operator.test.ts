/**
 * U075 — AST operator-node detection: distinguish an escaped `\;` word-argument
 * from a real `;` operator so the POSIX `find … -exec cmd {} \;` idiom is not
 * false-blocked (parity: the reference `hasActualOperatorNodes`,
 * utils/bash/treeSitterAnalysis.ts).
 *
 * RED-GREEN: reverting ANY of the three source fixes (the new
 * `hasActualOperatorNodes` gate in `validateBackslashEscapedOperators`, the
 * escaped-trailing-operator exception in `validateDanglingOperator`, or the
 * escape-stripping in `validateShellMetacharsInCommands`) turns one of the
 * `validateBashSecurity` integration cases below RED.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { parseBashAST, hasActualOperatorNodes, PARSE_ABORTED } from '../src/tools/bash-parser';
import {
  validateBashSecurity,
  validateDanglingOperator,
  validateShellMetacharsInCommands,
  validateBackslashEscapedOperators,
  CHECK_IDS,
} from '../src/tools/bash-security';

function hasOp(command: string): boolean {
  const ast = parseBashAST(command, { timeoutMs: Infinity });
  assert.ok(ast && ast !== PARSE_ABORTED, `expected a real AST for ${JSON.stringify(command)}`);
  return hasActualOperatorNodes(ast as Exclude<typeof ast, null | typeof PARSE_ABORTED>);
}

describe('hasActualOperatorNodes (bash-parser)', () => {
  it('is false for a simple command whose only "operator" is an escaped \\;', () => {
    assert.equal(hasOp('find . -type f -exec rm {} \\;'), false);
    assert.equal(hasOp('ls \\; rm'), false);
  });

  it('is false for a dangling trailing operator (no second operand → no list node)', () => {
    assert.equal(hasOp('cmd &&'), false);
    assert.equal(hasOp('cmd ;'), false);
  });

  it('is true when a real list/pipeline operator is present', () => {
    assert.equal(hasOp('a && b'), true);
    assert.equal(hasOp('a || b'), true);
    assert.equal(hasOp('a ; b'), true);
    assert.equal(hasOp('a | b'), true);
  });

  it('recurses into subshell / command-substitution children', () => {
    assert.equal(hasOp('(a && b)'), true);
    assert.equal(hasOp('$(a; b)'), true);
  });
});

describe('validateDanglingOperator — escaped trailing operator', () => {
  it('allows the POSIX find -exec \\; terminator (escaped ; at end)', () => {
    assert.equal(validateDanglingOperator('find . -type f -exec rm {} \\;').safe, true);
  });

  it('allows an escaped trailing | (\\|)', () => {
    assert.equal(validateDanglingOperator('foo \\|').safe, true);
  });

  it('still blocks a genuine dangling ; / && / || / |', () => {
    assert.equal(validateDanglingOperator('echo hi ;').safe, false);
    assert.equal(validateDanglingOperator('echo hi &&').safe, false);
    assert.equal(validateDanglingOperator('echo hi ||').safe, false);
    assert.equal(validateDanglingOperator('echo hi |').safe, false);
  });

  it('still blocks when the backslash itself is escaped (even count → operator dangles)', () => {
    // `\\;` = a literal backslash followed by a REAL dangling `;`.
    assert.equal(validateDanglingOperator('echo hi \\\\;').safe, false);
  });
});

describe('validateShellMetacharsInCommands — escaped metachars', () => {
  it('allows find/grep/xargs with an escaped \\; terminator', () => {
    assert.equal(validateShellMetacharsInCommands('find . -type f -exec grep foo {} \\;').safe, true);
    assert.equal(validateShellMetacharsInCommands('find . -name x -exec chmod 755 {} \\;').safe, true);
  });

  it('still blocks genuine unescaped metacharacter injection near find', () => {
    assert.equal(validateShellMetacharsInCommands('find . ; rm -rf /').safe, false);
    assert.equal(validateShellMetacharsInCommands('find . -name x `id`').safe, false);
  });
});

describe('validateBackslashEscapedOperators — AST gate', () => {
  it('allows an escaped operator in a simple command (no real operators)', () => {
    assert.equal(validateBackslashEscapedOperators('find . -exec rm {} \\;').safe, true);
    assert.equal(validateBackslashEscapedOperators('cmd \\; evil').safe, true);
  });

  it('still blocks an escaped operator when a real operator co-occurs', () => {
    const r = validateBackslashEscapedOperators('echo a && cmd \\; evil');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.BACKSLASH_ESCAPED_OPERATORS);
  });
});

describe('validateBashSecurity — find -exec \\; false-positive eliminated (E2E)', () => {
  const nowSafe = [
    'find . -type f -exec grep foo {} \\;',
    'find . -name x -exec chmod 755 {} \\;',
    'find . -type f -exec rm {} \\;',
    'find . -type f -exec wc -l {} \\;',
    'ls \\; rm',
  ];
  for (const cmd of nowSafe) {
    it(`allows ${JSON.stringify(cmd)}`, () => {
      // RED on revert of any of the three fixes.
      assert.equal(validateBashSecurity(cmd).safe, true, `${JSON.stringify(cmd)} should be safe`);
    });
  }

  const stillBlocked: Array<[string, number]> = [
    ['find . ; rm -rf /', CHECK_IDS.SHELL_METACHARS_IN_COMMANDS],
    ['find . -name x `id`', CHECK_IDS.SHELL_METACHARS_IN_COMMANDS],
    ['a=1 ; cmd2\\;cmd3', CHECK_IDS.BACKSLASH_ESCAPED_OPERATORS],
    ['ls -la &&', CHECK_IDS.DANGLING_OPERATOR],
    ['echo hi ;', CHECK_IDS.DANGLING_OPERATOR],
  ];
  for (const [cmd, id] of stillBlocked) {
    it(`still blocks ${JSON.stringify(cmd)} (#${id})`, () => {
      const r = validateBashSecurity(cmd);
      assert.equal(r.safe, false, `${JSON.stringify(cmd)} should be blocked`);
      assert.equal(r.checkId, id);
    });
  }
});
