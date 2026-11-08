import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  parseBashAST,
  extractCommands,
  extractCommandsFromString,
  type CommandNode,
} from '../src/tools/bash-parser';

/**
 * cov100 behaviour tests for bash-parser.ts.
 *
 * Targets the residual uncovered branch arms around the lexer's end-of-input
 * guards and the `$`-prefixed-token discrimination that the existing suites
 * (bash-parser.test.ts, bash-parser-wave1-cov.test.ts) do not exercise:
 *
 *   - parseSimpleCommand line `p === '$' && peekTwo() === '$('` — the FALSE arm
 *     where a token starts with `$` but is NOT a `$(` command substitution.
 *   - the `peekTwo()` null-at-EOF boundary (a lone trailing `$`).
 *   - the `else if (p === '&')` background-operator branch in parseList,
 *     including a trailing `&` whose rhs pipeline is null.
 *   - readWord escape handling: an escaped separator folded into one word and a
 *     dangling trailing backslash at EOF.
 *   - the skipWhitespace / isAtEnd guards that bound every peek() call, so
 *     parsing terminates cleanly at end-of-input instead of looping.
 *
 * Every test asserts concrete parser output, not mere execution.
 */

describe('bash-parser: `$`-prefixed token that is NOT a command substitution', () => {
  it('a leading $VAR token parses as a simple command, not command_substitution', () => {
    // p === '$' is true, but peekTwo() === '$(' is FALSE (it is "$E"),
    // so the substitution branch is skipped and $EDITOR is read as a plain word.
    const ast = parseBashAST('$EDITOR notes.txt');
    assert.ok(ast);
    assert.equal(ast.type, 'simple');
    assert.equal(ast.command, '$EDITOR');
    assert.deepEqual(ast.args, ['notes.txt']);
    // it must NOT be mistaken for a $() substitution node
    assert.notEqual(ast.type, 'command_substitution');
  });

  it('a `$` immediately followed by a space is a bare `$` command with args', () => {
    // peekTwo() returns "$ " (not "$("), so again the substitution arm is false.
    const ast = parseBashAST('$ foo');
    assert.ok(ast);
    assert.equal(ast.type, 'simple');
    assert.equal(ast.command, '$');
    assert.deepEqual(ast.args, ['foo']);
  });

  it('a lone trailing `$` at end-of-input is tolerated (peekTwo null boundary)', () => {
    // Here pos+1 >= length, so peekTwo() returns null; `null === '$('` is false,
    // and the single `$` becomes the command. No throw, no substitution node.
    const ast = parseBashAST('$');
    assert.ok(ast);
    assert.equal(ast.type, 'simple');
    assert.equal(ast.command, '$');
    assert.deepEqual(ast.args, []);
  });

  it('$()-substitution discrimination still works alongside the $VAR case', () => {
    // Sanity: the TRUE arm is unaffected — a real $() still becomes a substitution.
    const sub = parseBashAST('$(whoami)');
    assert.equal(sub!.type, 'command_substitution');
    const notSub = parseBashAST('$HOME');
    assert.equal(notSub!.type, 'simple');
    assert.equal(notSub!.command, '$HOME');
  });
});

describe('bash-parser: background `&` operator branch (parseList `p === \'&\'`)', () => {
  it('chained background jobs build a list with one `&` operator per separator', () => {
    const ast = parseBashAST('a & b & c');
    assert.ok(ast);
    assert.equal(ast.type, 'list');
    assert.equal(ast.children!.length, 3);
    // two `&` separators between the three jobs
    assert.equal(ast.operator, '&,&');
    assert.deepEqual(extractCommands(ast), ['a', 'b', 'c']);
  });

  it('a trailing `&` with no following command does not append a null list item', () => {
    // enters the `p === '&'` branch, advances, parsePipeline returns null at EOF,
    // so `if (next) listItems.push(next)` takes its FALSE arm -> stays a single node.
    const ast = parseBashAST('server &');
    assert.ok(ast);
    assert.equal(ast.type, 'simple');
    assert.equal(ast.command, 'server');
    assert.deepEqual(extractCommands(ast), ['server']);
  });

  it('`&` mixed with `&&` is distinguished (background vs logical-and)', () => {
    // peekTwo() === '&&' is checked before p === '&', so the two must not collide.
    const ast = parseBashAST('build && deploy & notify');
    assert.ok(ast);
    assert.equal(ast.type, 'list');
    assert.deepEqual(extractCommands(ast), ['build', 'deploy', 'notify']);
    // operators recorded in order: && then &
    assert.equal(ast.operator, '&&,&');
  });
});

describe('bash-parser: readWord escape handling', () => {
  it('an escaped separator is folded into a single word', () => {
    // `a\ b` -> the backslash escapes the space so it stays one argument.
    const ast = parseBashAST('echo a\\ b');
    assert.ok(ast);
    assert.equal(ast.type, 'simple');
    assert.equal(ast.command, 'echo');
    assert.deepEqual(ast.args, ['a\\ b']);
    // crucially NOT split into two args ['a\\', 'b']
    assert.equal(ast.args!.length, 1);
  });

  it('a dangling trailing backslash at end-of-input is preserved, not dropped', () => {
    // readWord sets escaped=true on the final `\`; the loop then ends at EOF.
    const ast = parseBashAST('echo foo\\');
    assert.ok(ast);
    assert.equal(ast.command, 'echo');
    assert.deepEqual(ast.args, ['foo\\']);
  });
});

describe('bash-parser: end-of-input guards keep parsing bounded', () => {
  // Every lexer.peek() call is guarded by isAtEnd(); these inputs drive the
  // parser straight to EOF through skipWhitespace so it terminates cleanly
  // (no hang, no throw) and returns exactly the leading command.
  const atEndCases: Array<[string, string]> = [
    ['ls   ', 'ls'],       // trailing spaces
    ['   ls', 'ls'],       // leading spaces
    ['ls\t', 'ls'],        // trailing tab
    ['ls\n\n', 'ls'],      // trailing newlines
    ['ls\n   \n', 'ls'],   // whitespace-then-newline then EOF
  ];
  for (const [input, cmd] of atEndCases) {
    it(`terminates at EOF for ${JSON.stringify(input)} -> ${cmd}`, () => {
      const ast = parseBashAST(input);
      assert.ok(ast);
      assert.equal(ast.type, 'simple');
      assert.equal(ast.command, cmd);
      assert.deepEqual(ast.args, []);
    });
  }

  it('whitespace-only input returns null (trim guard in parseBashAST)', () => {
    assert.equal(parseBashAST('   \t  '), null);
    assert.equal(parseBashAST('\n\n'), null);
  });
});

describe('bash-parser: `2>&1`-style fd redirection is NOT understood (documented limitation)', () => {
  it('splits `echo hi 2>&1` on the bare `&` instead of treating it as fd-dup', () => {
    // The parser has no notion of `>&` fd duplication: the `2` leaks as an arg,
    // the `&` is read as a background separator, and `1` becomes a second command.
    // This documents current behaviour so a future fix is a deliberate change.
    const ast = parseBashAST('echo hi 2>&1');
    assert.ok(ast);
    assert.equal(ast.type, 'list');
    assert.deepEqual(extractCommands(ast), ['echo', '1']);
    const first = ast.children![0] as CommandNode;
    assert.equal(first.command, 'echo');
    assert.deepEqual(first.args, ['hi', '2']);
    assert.equal(ast.operator, '&');
  });
});

describe('bash-parser: extractCommandsFromString over the $VAR / background forms', () => {
  it('reaches commands across a background list', () => {
    assert.deepEqual(extractCommandsFromString('a & b & c'), ['a', 'b', 'c']);
  });

  it('keeps a $VAR invocation as its command name', () => {
    assert.deepEqual(extractCommandsFromString('$EDITOR file'), ['$EDITOR']);
  });
});
