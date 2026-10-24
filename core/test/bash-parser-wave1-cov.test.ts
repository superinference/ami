import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  parseBashAST,
  extractCommands,
  extractCommandsFromString,
} from '../src/tools/bash-parser';

/**
 * Wave-1 coverage tests for bash-parser.ts.
 * Targets uncovered branches: trailing operators (next === null), redirect-only
 * commands, command-substitution at token start, quote/escape handling inside
 * subshell/substitution scanning, and the raw-fallback ternary.
 * These pin real parser behaviour, not just line execution.
 */

describe('bash-parser trailing-operator branches (next === null)', () => {
  it('trailing pipe does not append a null pipeline child', () => {
    // parsePipeline: `if (next) pipelineCommands.push(next)` false-branch
    const ast = parseBashAST('cat file |');
    assert.ok(ast);
    // only the single `cat` survives; no second child was pushed for the empty rhs
    assert.equal(ast.type, 'simple');
    assert.equal(ast.command, 'cat');
    assert.deepEqual(extractCommands(ast), ['cat']);
  });

  it('trailing pipe keeps a real left-hand pipeline intact', () => {
    const ast = parseBashAST('a | b |');
    assert.ok(ast);
    assert.equal(ast.type, 'pipeline');
    // exactly two real commands, not three (trailing | added nothing)
    assert.equal(ast.children!.length, 2);
    assert.deepEqual(extractCommands(ast), ['a', 'b']);
  });

  it('trailing && does not append a null list item', () => {
    // parseList &&/|| branch: `if (next) listItems.push(next)` false-branch
    const ast = parseBashAST('make &&');
    assert.ok(ast);
    assert.equal(ast.type, 'simple');
    assert.equal(ast.command, 'make');
    assert.deepEqual(extractCommands(ast), ['make']);
  });

  it('trailing || does not append a null list item', () => {
    const ast = parseBashAST('test -f x ||');
    assert.ok(ast);
    assert.equal(ast.type, 'simple');
    assert.equal(ast.command, 'test');
  });

  it('trailing semicolon does not append a null list item', () => {
    // parseList `;` branch: `if (next) listItems.push(next)` false-branch
    const ast = parseBashAST('ls;');
    assert.ok(ast);
    assert.equal(ast.type, 'simple');
    assert.equal(ast.command, 'ls');
    assert.deepEqual(extractCommands(ast), ['ls']);
  });

  it('trailing background operator does not append a null list item', () => {
    // parseList `&` branch: `if (next) listItems.push(next)` false-branch
    const ast = parseBashAST('sleep 10 &');
    assert.ok(ast);
    assert.equal(ast.type, 'simple');
    assert.equal(ast.command, 'sleep');
    assert.deepEqual(extractCommands(ast), ['sleep']);
  });

  it('background operator with following command still builds a two-item list', () => {
    const ast = parseBashAST('sleep 1 & wait');
    assert.ok(ast);
    assert.equal(ast.type, 'list');
    assert.equal(ast.children!.length, 2);
    assert.ok(ast.operator!.includes('&'));
  });
});

describe('bash-parser newline separator branch', () => {
  it('splits a list on a literal newline (p === \'\\n\' branch)', () => {
    // parseList: `p === ';' || p === '\n'` — exercises the \n operand
    const ast = parseBashAST('cd /tmp\nls\npwd');
    assert.ok(ast);
    assert.equal(ast.type, 'list');
    assert.equal(ast.children!.length, 3);
    assert.deepEqual(extractCommands(ast), ['cd', 'ls', 'pwd']);
  });

  it('trailing newline does not append a null list item', () => {
    const ast = parseBashAST('ls\n');
    assert.ok(ast);
    assert.equal(ast.type, 'simple');
    assert.equal(ast.command, 'ls');
  });

  it('newline acts like a separator even mixed with semicolons', () => {
    const ast = parseBashAST('a\nb; c');
    assert.ok(ast);
    assert.equal(ast.type, 'list');
    assert.deepEqual(extractCommands(ast), ['a', 'b', 'c']);
  });
});

describe('bash-parser list terminator (else-break) branch', () => {
  it('trailing non-operator content after a subshell breaks the list loop', () => {
    // parseList: enters the while loop (not at end) but p is neither
    // &&/||, ; , \n, nor & -> the final `else break` with `p === '&'` false
    const ast = parseBashAST('(ls) foo');
    assert.ok(ast);
    // the subshell is returned; the stray `foo` does not become a list item
    assert.equal(ast.type, 'subshell');
    assert.deepEqual(extractCommands(ast), ['ls']);
  });

  it('backslash outside quotes inside parens does not trigger the inDouble escape path', () => {
    // readUntilCloseParen: `c === '\\' && inDouble` with inDouble === false
    const ast = parseBashAST('(echo a\\b)');
    assert.ok(ast);
    assert.equal(ast.type, 'subshell');
    assert.equal(ast.children![0].command, 'echo');
  });
});

describe('bash-parser redirect-only / empty-word branches', () => {
  it('a redirect-only command yields null (words.length === 0)', () => {
    // parseSimpleCommand: `if (words.length === 0) return null`
    const ast = parseBashAST('> out.txt');
    assert.equal(ast, null);
  });

  it('input redirect-only command yields null', () => {
    const ast = parseBashAST('< in.txt');
    assert.equal(ast, null);
  });

  it('append redirect (>>) is consumed and does not become a command', () => {
    // exercises the `peek() === '>'` second-char advance in the redirect branch
    const ast = parseBashAST('echo hi >> log.txt');
    assert.ok(ast);
    assert.equal(ast.type, 'simple');
    assert.equal(ast.command, 'echo');
    assert.deepEqual(ast.args, ['hi']);
    // the redirect target must NOT leak into args
    assert.ok(!ast.args!.includes('log.txt'));
  });

  it('redirect with no target at end is tolerated', () => {
    const ast = parseBashAST('echo hi >');
    assert.ok(ast);
    assert.equal(ast.command, 'echo');
  });
});

describe('bash-parser command-substitution at token start', () => {
  it('$(...) as the whole command parses as command_substitution node', () => {
    // parseSimpleCommand: the `$` + peekTwo === '$(' branch
    const ast = parseBashAST('$(whoami)');
    assert.ok(ast);
    assert.equal(ast.type, 'command_substitution');
    assert.ok(ast.children);
    assert.equal(ast.children!.length, 1);
    assert.equal(ast.children![0].command, 'whoami');
    assert.ok(ast.raw.startsWith('$('));
  });

  it('empty command substitution $() yields an empty children list', () => {
    // inner parse returns null → `children ? [children] : []` false-branch
    const ast = parseBashAST('$()');
    assert.ok(ast);
    assert.equal(ast.type, 'command_substitution');
    assert.deepEqual(ast.children, []);
  });

  it('empty subshell () yields an empty children list', () => {
    const ast = parseBashAST('()');
    assert.ok(ast);
    assert.equal(ast.type, 'subshell');
    assert.deepEqual(ast.children, []);
  });

  it('extractCommandsFromString reaches commands nested in a top-level $()', () => {
    const cmds = extractCommandsFromString('$(rm -rf /tmp/x)');
    assert.ok(cmds.includes('rm'));
  });
});

describe('bash-parser quote/escape handling inside paren scanning', () => {
  it('single quotes protect a close-paren inside a subshell', () => {
    // readUntilCloseParen: `c === "'" && !inDouble` toggle; `)` inside quotes ignored
    const ast = parseBashAST("(echo 'a)b')");
    assert.ok(ast);
    assert.equal(ast.type, 'subshell');
    const inner = ast.children![0];
    // the inner command is the full echo, the quoted ) did not terminate the subshell early
    assert.equal(inner.command, 'echo');
    assert.ok(ast.raw.includes(')b'));
  });

  it('double quotes protect a close-paren inside a subshell', () => {
    // readUntilCloseParen: `c === '"' && !inSingle` toggle
    const ast = parseBashAST('(echo "a)b")');
    assert.ok(ast);
    assert.equal(ast.type, 'subshell');
    assert.ok(ast.raw.includes(')b'));
  });

  it('escaped quote inside double quotes is preserved in command substitution', () => {
    // readUntilCloseParen: `c === '\\' && inDouble && pos+1 < length` branch
    const ast = parseBashAST('$(echo "a\\"b")');
    assert.ok(ast);
    assert.equal(ast.type, 'command_substitution');
    // the backslash-escaped quote did not end the double-quote region prematurely,
    // so the trailing real close-paren terminated the substitution correctly
    assert.ok(ast.raw.includes('\\"'));
    assert.equal(ast.children![0].command, 'echo');
  });

  it('nested parens increase depth and do not terminate the outer subshell early', () => {
    // readUntilCloseParen: `c === '(' => depth++`
    const ast = parseBashAST('(a && (b || c))');
    assert.ok(ast);
    assert.equal(ast.type, 'subshell');
    const cmds = extractCommands(ast);
    assert.ok(cmds.includes('a'));
    assert.ok(cmds.includes('b'));
    assert.ok(cmds.includes('c'));
  });

  it("single quote inside double quotes does not toggle single-quote state", () => {
    // readUntilCloseParen: `c === "'" && !inDouble` with inDouble === true (false arm)
    const ast = parseBashAST('(echo "a\'b")');
    assert.ok(ast);
    assert.equal(ast.type, 'subshell');
    assert.equal(ast.children![0].command, 'echo');
    assert.ok(ast.raw.includes("a'b"));
  });

  it('trailing backslash in an unterminated double quote is tolerated (pos+1 < length false)', () => {
    // readUntilCloseParen escape branch: `this.pos + 1 < this.input.length` false arm —
    // backslash is the final character, inside an unclosed double quote / subshell
    const ast = parseBashAST('( "x\\');
    assert.ok(ast);
    assert.equal(ast.type, 'subshell');
    // scanner consumed to EOF without crashing; the backslash is preserved in raw
    assert.ok(ast.raw.includes('\\'));
  });

  it('single quote inside paren scan suppresses a double-quote toggle', () => {
    // exercises the !inDouble guard on the single-quote branch with a " inside '...'
    const ast = parseBashAST('(echo \'a"b\')');
    assert.ok(ast);
    assert.equal(ast.type, 'subshell');
    assert.equal(ast.children![0].command, 'echo');
  });
});

describe('bash-parser raw fallback and env-var-only commands', () => {
  it('a later command in a list keeps its command as raw (remaining < startPos)', () => {
    // parseSimpleCommand raw ternary true-branch: getRemaining().length < startPos
    const ast = parseBashAST('echo first; secondcmd');
    assert.ok(ast);
    assert.equal(ast.type, 'list');
    const second = ast.children![1];
    assert.equal(second.command, 'secondcmd');
    assert.equal(second.raw, 'secondcmd');
  });

  it('env-var-only assignment uses the assignment as the command', () => {
    // parseSimpleCommand: `command || (envVars.length > 0 ? envVars[0] : '')`
    const ast = parseBashAST('FOO=bar');
    assert.ok(ast);
    assert.equal(ast.type, 'simple');
    assert.equal(ast.command, 'FOO=bar');
  });

  it('multiple env assignments with no command fall back to the first assignment', () => {
    const ast = parseBashAST('A=1 B=2');
    assert.ok(ast);
    assert.equal(ast.command, 'A=1');
  });
});

describe('bash-parser extractCommands on a childless simple node', () => {
  it('a simple node without children is handled (no children walk)', () => {
    const node = { type: 'simple' as const, command: 'ls', args: [], raw: 'ls' };
    assert.deepEqual(extractCommands(node), ['ls']);
  });

  it('a simple node with empty command is skipped', () => {
    const node = { type: 'simple' as const, command: '', args: [], raw: '' };
    assert.deepEqual(extractCommands(node), []);
  });
});
