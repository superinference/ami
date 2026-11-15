/**
 * Bash-tool functional parity: BASH_MAX_OUTPUT_LENGTH is honored.
 *
 * GAP: Claude Code's Bash tool caps command output at a configurable number of
 * characters via the BASH_MAX_OUTPUT_LENGTH env var. In AMI that var was already
 * registered as a SAFE_ENV_VAR tuning knob (env-protection.ts) and merged by the
 * config layer, but the bash tool never READ it — the cap was the hardcoded
 * MAX_OUTPUT_LENGTH (200k), so setting BASH_MAX_OUTPUT_LENGTH had no effect at
 * all. This suite adds + validates `resolveBashMaxOutput()` and its wiring into
 * the two output-cap sites (spill fallback + formatOutput truncation).
 *
 * All command execution is driven deterministically through the `_bashHooks`
 * execCommand DI seam (node:test `mock.method`) — no real command runs, so the
 * large-output cases are safe-by-construction.
 *
 * RED-GREEN (reverting the wiring back to MAX_OUTPUT_LENGTH):
 *   - "spill fallback honors BASH_MAX_OUTPUT_LENGTH"  → FAILS on revert
 *   - "formatOutput truncates stderr at BASH_MAX_OUTPUT_LENGTH" → FAILS on revert
 * (the pure resolveBashMaxOutput unit cases error out if the symbol is removed.)
 */
import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { bashTool, _bashHooks, resolveBashMaxOutput } from '../src/tools/bash';
import type { ToolContext } from '../src/types';

const ENV_KEY = 'BASH_MAX_OUTPUT_LENGTH';

function withEnv(value: string | undefined, fn: () => void): void {
  const prev = process.env[ENV_KEY];
  if (value === undefined) delete process.env[ENV_KEY];
  else process.env[ENV_KEY] = value;
  try {
    fn();
  } finally {
    if (prev === undefined) delete process.env[ENV_KEY];
    else process.env[ENV_KEY] = prev;
  }
}

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

// ---------------------------------------------------------------------------
// Pure resolver — every branch
// ---------------------------------------------------------------------------
describe('resolveBashMaxOutput — honors BASH_MAX_OUTPUT_LENGTH', () => {
  it('falls back to the 200k default when unset', () => {
    withEnv(undefined, () => assert.equal(resolveBashMaxOutput(), 200_000));
  });

  it('falls back to default for a non-numeric value', () => {
    withEnv('not-a-number', () => assert.equal(resolveBashMaxOutput(), 200_000));
  });

  it('falls back to default for a zero / negative value', () => {
    withEnv('0', () => assert.equal(resolveBashMaxOutput(), 200_000));
    withEnv('-500', () => assert.equal(resolveBashMaxOutput(), 200_000));
  });

  it('uses a positive value above the floor verbatim', () => {
    withEnv('5000', () => assert.equal(resolveBashMaxOutput(), 5000));
  });

  it('clamps an absurdly small positive value up to the 1000-char floor', () => {
    withEnv('50', () => assert.equal(resolveBashMaxOutput(), 1000));
  });
});

// ---------------------------------------------------------------------------
// Execution wiring — red-green discriminators
// ---------------------------------------------------------------------------
describe('bashTool — BASH_MAX_OUTPUT_LENGTH caps real command output', () => {
  afterEach(() => mock.restoreAll());

  it('spill fallback honors BASH_MAX_OUTPUT_LENGTH (stdout)', async () => {
    // 5000 chars of stdout; cap set to 1500 → must be truncated/spilled.
    mock.method(_bashHooks, 'execCommand', async () => ({
      stdout: 'A'.repeat(5000),
      stderr: '',
      exitCode: 0,
    }));

    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-bash-cap-'));
    try {
      await withEnvAsync('1500', async () => {
        const res = await bashTool.execute({ command: 'run-big-output' }, ctx({ cwd: tmp }));
        // DISCRIMINATOR: with the cap honored the 5000-char stdout is truncated
        // (spill banner contains "truncated") and the returned output is far
        // below the raw 5000 chars. Pre-fix (cap = 200k) the full output passes
        // through verbatim with no truncation marker.
        assert.ok(res.output.includes('truncated'), `expected truncation marker, got len=${res.output.length}`);
        assert.ok(res.output.length < 3000, `expected capped output, got len=${res.output.length}`);
      });
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('formatOutput truncates stderr at BASH_MAX_OUTPUT_LENGTH', async () => {
    // Large stderr, exit 0 — stderr is not spilled, so this exercises the
    // formatOutput truncateBashText path specifically.
    mock.method(_bashHooks, 'execCommand', async () => ({
      stdout: '',
      stderr: 'B'.repeat(5000),
      exitCode: 0,
    }));

    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-bash-cap-'));
    try {
      await withEnvAsync('1500', async () => {
        const res = await bashTool.execute({ command: 'run-big-stderr' }, ctx({ cwd: tmp }));
        // DISCRIMINATOR: the stderr is truncated with the "[truncated N characters]"
        // marker. Pre-fix (cap = 200k) the full 5000-char stderr passes through.
        assert.match(res.output, /\[truncated \d+ characters\]/);
        assert.ok(res.output.length < 3000, `expected capped output, got len=${res.output.length}`);
      });
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('leaves small output unchanged regardless of the cap', async () => {
    mock.method(_bashHooks, 'execCommand', async () => ({
      stdout: 'hello world',
      stderr: '',
      exitCode: 0,
    }));
    await withEnvAsync('1500', async () => {
      const res = await bashTool.execute({ command: 'run-small' }, ctx());
      assert.ok(res.output.includes('hello world'));
      assert.ok(!res.output.includes('truncated'));
    });
  });
});

async function withEnvAsync(value: string | undefined, fn: () => Promise<void>): Promise<void> {
  const prev = process.env[ENV_KEY];
  if (value === undefined) delete process.env[ENV_KEY];
  else process.env[ENV_KEY] = value;
  try {
    await fn();
  } finally {
    if (prev === undefined) delete process.env[ENV_KEY];
    else process.env[ENV_KEY] = prev;
  }
}
