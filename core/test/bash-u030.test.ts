/**
 * U030 — bash.ts parity improvements
 *
 * 1. isSilentBashCommand: compound/pipeline-aware no-output detection with a
 *    broad silent-command set and `||` fallback handling (parity: the reference
 *    BashTool isSilentBashCommand / BASH_SILENT_COMMANDS). Replaces the old
 *    leading-regex that only looked at the FIRST command and a narrow set, so a
 *    compound command whose later segment is noisy no longer gets a misleading
 *    "no output expected" affordance, and a broader set of silent commands
 *    (rmdir/chgrp/ln/cd/export/unset/wait) now gets it.
 * 2. formatDuration: human-readable command-timeout message (parity: the reference
 *    ShellCommand `Command timed out after ${formatDuration(timeout)}`), as a
 *    superset that keeps sub-second precision in ms.
 * 3. findstr.exe exit-code semantics (1 = no match, non-error) via
 *    interpretExitCode.
 *
 * CORE ISOLATION: imports only from ../src/tools/bash. No common/cli/vscode.
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  isSilentBashCommand,
  formatDuration,
  formatTimeoutBackgroundOutput,
  interpretExitCode,
  bashTool,
} from '../src/tools/bash';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

describe('isSilentBashCommand — compound / pipeline-aware', () => {
  it('is true for a single silent command', () => {
    assert.equal(isSilentBashCommand('mkdir foo'), true);
    assert.equal(isSilentBashCommand('rmdir foo'), true);
    assert.equal(isSilentBashCommand('chgrp staff a'), true);
    assert.equal(isSilentBashCommand('ln -s a b'), true);
  });

  it('is true when EVERY chained segment is silent', () => {
    assert.equal(isSilentBashCommand('cp a b && mv c d'), true);
    assert.equal(isSilentBashCommand('touch a; chmod 644 a'), true);
  });

  it('is false when a later chained segment is NOT silent (old leading-regex bug)', () => {
    // The old `/^\s*(mkdir|...)\b/` matched only the FIRST command, so this
    // wrongly got the affordance. The compound-aware check returns false.
    assert.equal(isSilentBashCommand('cp a b && ls'), false);
    assert.equal(isSilentBashCommand('mkdir foo && echo done'), false);
  });

  it('treats a neutral command after `||` as a fallback and skips it', () => {
    assert.equal(isSilentBashCommand('rm x || echo fail'), true);
    assert.equal(isSilentBashCommand('mkdir d || true'), true);
  });

  it('is false for read/list commands and empty input', () => {
    assert.equal(isSilentBashCommand('ls'), false);
    assert.equal(isSilentBashCommand('cat x'), false);
    assert.equal(isSilentBashCommand(''), false);
    assert.equal(isSilentBashCommand('   '), false);
  });

  it('is false when a silent command is piped into a non-silent one', () => {
    assert.equal(isSilentBashCommand('rm x | tee log'), false);
  });
});

describe('formatDuration — human-readable timeout', () => {
  it('keeps sub-second precision in ms (superset over Claude)', () => {
    assert.equal(formatDuration(400), '400ms');
    assert.equal(formatDuration(0), '0ms');
  });
  it('renders whole seconds / minutes / hours / days', () => {
    assert.equal(formatDuration(4200), '4s');
    assert.equal(formatDuration(120000), '2m');
    assert.equal(formatDuration(90000), '1m 30s');
    assert.equal(formatDuration(3661000), '1h 1m 1s');
    assert.equal(formatDuration(90061000), '1d 1h 1m 1s');
  });
  it('is defensive against non-finite / negative input', () => {
    assert.equal(formatDuration(Number.NaN), 'NaNms');
    assert.equal(formatDuration(-5), '-5ms');
  });
});

describe('formatTimeoutBackgroundOutput — uses formatDuration', () => {
  it('renders the timeout human-readably', () => {
    const out = formatTimeoutBackgroundOutput('', '', 120000, 'task-x');
    assert.match(out, /Command timed out after 2m\./);
  });
});

describe('interpretExitCode — findstr', () => {
  it('treats findstr exit 1 (no match) as a non-error', () => {
    const info = interpretExitCode('findstr foo bar.txt', 1);
    assert.ok(info);
    assert.equal(info!.isNonError, true);
    assert.match(info!.message, /no matches found/i);
  });
  it('treats findstr exit 2 as a real error', () => {
    const info = interpretExitCode('findstr foo bar.txt', 2);
    assert.ok(info);
    assert.ok(!info!.isNonError);
  });
});

describe('bashTool.execute — silent-command affordance (end-to-end)', () => {
  it('does NOT show the no-output affordance when a chained segment is noisy', async () => {
    const res = await bashTool.execute({ command: 'true && echo hi' }, ctx());
    assert.equal(res.isError, false);
    assert.ok(!res.output.includes('no output expected'), res.output);
    assert.match(res.output, /hi/);
  });

  it('shows the no-output affordance for a broad silent command the old regex missed', async () => {
    const res = await bashTool.execute(
      { command: 'mkdir -p __u030_tmp && rmdir __u030_tmp' },
      ctx(),
    );
    assert.equal(res.isError, false);
    assert.ok(res.output.includes('no output expected'), res.output);
  });
});
