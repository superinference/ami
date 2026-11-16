/**
 * U075 — DECLARATION_COMMANDS allow-set + declaration-command variable
 * extraction (parity: Claude Code's utils/bash/parser.ts DECLARATION_COMMANDS,
 * L20-28, and extractCommandArguments L189-196).
 *
 * bash/tree-sitter models `export`/`declare`/`typeset`/`readonly`/`local`/
 * `unset`/`unsetenv` as declaration commands whose operands are variable
 * declarations rather than ordinary arguments. AMI's recursive-descent parser
 * emits them as `simple` nodes, so {@link extractDeclaredAssignments} recovers
 * the touched variable names from the AST.
 *
 * Security impact (RED→GREEN): `detectCodeLoadingEnvSet`'s textual regex only
 * covered `export`, `declare -x`, and `unset`, so a pinned code-loading variable
 * assigned via `readonly`/`local`/`typeset`/`declare` (without `-x`) slipped
 * through unblocked. The AST declaration pass closes that gap.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  parseBashAST,
  extractDeclaredAssignments,
  DECLARATION_COMMANDS,
  PARSE_ABORTED,
  type CommandNode,
} from '../src/tools/bash-parser';
import { detectCodeLoadingEnvSet } from '../src/tools/bash-security';
import { bashTool } from '../src/tools/bash';
import type { ToolContext } from '../src/types';

function ast(cmd: string): CommandNode {
  const node = parseBashAST(cmd);
  assert.ok(node && node !== PARSE_ABORTED, `expected a parsed AST for ${cmd}`);
  return node as CommandNode;
}

describe('DECLARATION_COMMANDS set', () => {
  it('contains the bash declaration keywords (Claude parity)', () => {
    for (const kw of ['export', 'declare', 'typeset', 'readonly', 'local', 'unset', 'unsetenv']) {
      assert.ok(DECLARATION_COMMANDS.has(kw), `missing ${kw}`);
    }
    assert.equal(DECLARATION_COMMANDS.size, 7);
  });
});

describe('extractDeclaredAssignments', () => {
  it('extracts the variable from `export FOO=bar`', () => {
    assert.deepEqual(extractDeclaredAssignments(ast('export FOO=bar')), [
      { command: 'export', variable: 'FOO' },
    ]);
  });

  it('extracts from readonly / local / typeset / declare', () => {
    assert.deepEqual(extractDeclaredAssignments(ast('readonly LD_PRELOAD=/x')), [
      { command: 'readonly', variable: 'LD_PRELOAD' },
    ]);
    assert.deepEqual(extractDeclaredAssignments(ast('typeset LD_LIBRARY_PATH=/x')), [
      { command: 'typeset', variable: 'LD_LIBRARY_PATH' },
    ]);
    assert.deepEqual(extractDeclaredAssignments(ast('local PATH=/tmp/x')), [
      { command: 'local', variable: 'PATH' },
    ]);
    assert.deepEqual(extractDeclaredAssignments(ast('declare FOO=1')), [
      { command: 'declare', variable: 'FOO' },
    ]);
  });

  it('handles bare (value-less) operands like `unset FOO`', () => {
    assert.deepEqual(extractDeclaredAssignments(ast('unset FOO')), [
      { command: 'unset', variable: 'FOO' },
    ]);
  });

  it('skips option flags and keeps real operands', () => {
    assert.deepEqual(extractDeclaredAssignments(ast('declare -x -g FOO=bar')), [
      { command: 'declare', variable: 'FOO' },
    ]);
  });

  it('returns nothing for non-declaration commands', () => {
    assert.deepEqual(extractDeclaredAssignments(ast('echo FOO=bar')), []);
  });

  it('walks into command substitutions / subshells', () => {
    const found = extractDeclaredAssignments(ast('(export LD_AUDIT=/x)'));
    assert.deepEqual(found, [{ command: 'export', variable: 'LD_AUDIT' }]);
  });
});

describe('detectCodeLoadingEnvSet — declaration-keyword coverage', () => {
  // RED before the AST pass: these declaration forms were NOT blocked.
  it('blocks `readonly LD_PRELOAD=...`', () => {
    const r = detectCodeLoadingEnvSet('readonly LD_PRELOAD=/evil.so');
    assert.equal(r.blocked, true);
    assert.equal(r.variable, 'LD_PRELOAD');
  });

  it('blocks `typeset LD_LIBRARY_PATH=...`', () => {
    assert.equal(detectCodeLoadingEnvSet('typeset LD_LIBRARY_PATH=/x').blocked, true);
  });

  it('blocks `local PATH=...`', () => {
    assert.equal(detectCodeLoadingEnvSet('local PATH=/tmp/x').blocked, true);
  });

  it('blocks `declare LD_PRELOAD=...` (no -x)', () => {
    assert.equal(detectCodeLoadingEnvSet('declare LD_PRELOAD=/evil.so').blocked, true);
  });

  // Regression: the original regex-covered forms must still block.
  it('still blocks `export LD_PRELOAD=...` (regression)', () => {
    assert.equal(detectCodeLoadingEnvSet('export LD_PRELOAD=/evil.so').blocked, true);
  });

  it('still blocks `declare -x LD_PRELOAD=...` (regression)', () => {
    assert.equal(detectCodeLoadingEnvSet('declare -x LD_PRELOAD=/evil.so').blocked, true);
  });

  // Negative: a non-pinned variable via a declaration keyword is allowed.
  it('allows a non-pinned variable (`readonly MY_VAR=1`)', () => {
    assert.equal(detectCodeLoadingEnvSet('readonly MY_VAR=1').blocked, false);
  });
});

describe('E2E — bash tool blocks declaration-form code-loading assignment', () => {
  const ctx = (): ToolContext => ({ cwd: process.cwd(), abortSignal: new AbortController().signal });

  it('rejects `readonly LD_PRELOAD=/evil.so` before execution', async () => {
    const result = await bashTool.execute({ command: 'readonly LD_PRELOAD=/evil.so' }, ctx());
    assert.equal(result.isError, true);
    assert.match(result.output, /LD_PRELOAD is blocked/);
  });
});
