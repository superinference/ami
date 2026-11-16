/**
 * Integration (end-to-end tool path) coverage for two bash-security parity
 * guards added in unit U015:
 *   - MAX_SUBCOMMANDS_FOR_SECURITY_CHECK — subcommand fanout cap (ReDoS/CPU DoS)
 *   - detectPs4TraceRce — PS4 trace-time RCE via assignment
 *
 * These drive the REAL bash tool (`bashTool.execute`), proving the guards are
 * wired into the execution gate (bash.ts → validateBashSecurity), not just unit-
 * reachable. Each malicious case MUST surface as `isError`; a benign lookalike
 * MUST still run — so reverting either guard turns its case red.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { bashTool } from '../src/tools/bash';
import { MAX_SUBCOMMANDS_FOR_SECURITY_CHECK } from '../src/tools/bash-security';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

describe('bashTool integration — subcommand fanout cap', () => {
  it('blocks an over-cap compound command end-to-end', async () => {
    const wide = Array.from(
      { length: MAX_SUBCOMMANDS_FOR_SECURITY_CHECK + 5 },
      () => 'echo hi',
    ).join(' && ');
    const result = await bashTool.execute({ command: wide }, ctx());
    assert.ok(result.isError, `expected block, got: ${result.output}`);
    assert.match(result.output, /subcommands/i);
  });

  it('runs a modest compound command (below the cap)', async () => {
    const result = await bashTool.execute(
      { command: 'echo a && echo b && echo c' },
      ctx(),
    );
    assert.ok(!result.isError, `expected success, got: ${result.output}`);
    assert.ok(result.output.includes('a'));
  });
});

describe('bashTool integration — PS4 trace-time RCE', () => {
  it('blocks a PS4 command-substitution assignment end-to-end', async () => {
    const result = await bashTool.execute(
      { command: "PS4='$(id)' && set -x && :" },
      ctx(),
    );
    assert.ok(result.isError, `expected block, got: ${result.output}`);
    assert.match(result.output, /PS4/);
  });

  it('allows a literal PS4 value with no expansion', async () => {
    const result = await bashTool.execute(
      { command: "PS4='+ ' echo traced" },
      ctx(),
    );
    assert.ok(!result.isError, `expected success, got: ${result.output}`);
    assert.ok(result.output.includes('traced'));
  });
});
