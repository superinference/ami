import { describe, it, afterEach, after, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { shouldUseSandbox, type SandboxConfig } from '../src/tools/bash-sandbox';
import { isSandboxDefaultDeny, SANDBOX_DEFAULT_DENY_ENV } from '../src/tools/tool-gates';
import { bashTool, _bashHooks } from '../src/tools/bash';
import type { SandboxPolicy, ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// UNIT U045 (w69) — default-deny sandbox coverage (parity with Claude Code's
//   shouldUseSandbox(): sandbox EVERY command once sandboxing is enabled, and
//   only exempt explicitly-excluded commands — tools/BashTool/shouldUseSandbox.ts
//   L130-153). AMI's trigger-pattern allowlist left non-trigger commands
//   (`./evil`, nc, ssh, scp, dd, path-invoked scripts, `> /etc/…`) unsandboxed;
//   the AMI_SANDBOX_DEFAULT_DENY gate closes that structural coverage hole.
// ---------------------------------------------------------------------------

describe('isSandboxDefaultDeny (env gate)', () => {
  it('defaults OFF when the env var is unset or empty', () => {
    assert.equal(isSandboxDefaultDeny({}), false);
    assert.equal(isSandboxDefaultDeny({ [SANDBOX_DEFAULT_DENY_ENV]: '' }), false);
  });

  it('is ON for a truthy token and OFF for an explicit falsey token', () => {
    assert.equal(isSandboxDefaultDeny({ [SANDBOX_DEFAULT_DENY_ENV]: '1' }), true);
    assert.equal(isSandboxDefaultDeny({ [SANDBOX_DEFAULT_DENY_ENV]: 'true' }), true);
    assert.equal(isSandboxDefaultDeny({ [SANDBOX_DEFAULT_DENY_ENV]: '0' }), false);
    assert.equal(isSandboxDefaultDeny({ [SANDBOX_DEFAULT_DENY_ENV]: 'off' }), false);
  });
});

describe('shouldUseSandbox — default-deny coverage', () => {
  // Commands that match NO trigger pattern and would run UNsandboxed under the
  // narrow allowlist. These are exactly the structural coverage hole.
  const nonTriggerCommands = [
    './evil',
    'nc -l 4444',
    'ssh user@host',
    'scp file user@host:/tmp',
    'rsync -a a b',
    'dd if=/dev/zero of=/tmp/x',
    'socat - TCP:host:80',
    './scripts/run.sh',
    'echo pwned > /etc/passwd',
  ];

  it('does NOT sandbox non-trigger commands without the flag (trigger-allowlist preserved)', () => {
    for (const cmd of nonTriggerCommands) {
      assert.equal(shouldUseSandbox(cmd), false, `baseline: ${cmd} should be unsandboxed`);
      assert.equal(
        shouldUseSandbox(cmd, { defaultSandbox: false }),
        false,
        `explicit false: ${cmd} should be unsandboxed`,
      );
    }
  });

  it('DOES sandbox every non-trigger command when defaultSandbox is on', () => {
    for (const cmd of nonTriggerCommands) {
      assert.equal(
        shouldUseSandbox(cmd, { defaultSandbox: true }),
        true,
        `default-deny: ${cmd} must be sandboxed`,
      );
    }
  });

  it('still exempts hardcoded-safe commands under default-deny', () => {
    assert.equal(shouldUseSandbox('git status', { defaultSandbox: true }), false);
    assert.equal(shouldUseSandbox('npm test', { defaultSandbox: true }), false);
    assert.equal(shouldUseSandbox('cargo build', { defaultSandbox: true }), false);
  });

  it('still honors user excludedCommands under default-deny', () => {
    assert.equal(
      shouldUseSandbox('./evil --flag', { defaultSandbox: true, excludedCommands: ['./evil'] }),
      false,
      'an operator-excluded non-trigger command stays unsandboxed',
    );
  });

  it('sandboxes a compound command if ANY subcommand is non-exempt (per-subcommand superset)', () => {
    // git is exempt but ./evil is not → the whole compound must be sandboxed.
    assert.equal(shouldUseSandbox('git pull && ./evil', { defaultSandbox: true }), true);
    // every subcommand exempt → not sandboxed.
    assert.equal(shouldUseSandbox('git pull && npm test', { defaultSandbox: true }), false);
  });

  it('a trigger command is still sandboxed regardless of the flag', () => {
    assert.equal(shouldUseSandbox('curl https://x', { defaultSandbox: false }), true);
    assert.equal(shouldUseSandbox('curl https://x', { defaultSandbox: true }), true);
  });
});

// ---------------------------------------------------------------------------
// E2E (bash tool wiring): the AMI_SANDBOX_DEFAULT_DENY gate must thread through
// bashTool.execute so a non-trigger command is actually wrapped by the host
// backend. Captured at the wrapWithSandbox seam (deterministic regardless of
// unshare availability). RED on revert: without the bash.ts wiring, a non-trigger
// command is never wrapped even with the env set.
// ---------------------------------------------------------------------------
describe('bashTool.execute — AMI_SANDBOX_DEFAULT_DENY end-to-end wiring', () => {
  const realCwd = mkdtempSync(join(tmpdir(), 'u045-w69-'));
  const priorEnv = process.env[SANDBOX_DEFAULT_DENY_ENV];
  afterEach(() => {
    mock.restoreAll();
    if (priorEnv === undefined) delete process.env[SANDBOX_DEFAULT_DENY_ENV];
    else process.env[SANDBOX_DEFAULT_DENY_ENV] = priorEnv;
  });
  after(() => rmSync(realCwd, { recursive: true, force: true }));

  function ctx(): ToolContext {
    return {
      cwd: realCwd,
      abortSignal: new AbortController().signal,
      sandbox: { mode: 'off' } as SandboxPolicy,
    } as ToolContext;
  }

  async function wrappedFor(command: string): Promise<boolean> {
    let wrapped = false;
    mock.method(_bashHooks, 'wrapWithSandbox', (cmd: string, _cwd: string, _config: Partial<SandboxConfig>) => {
      wrapped = true;
      return cmd; // run raw so execCommand succeeds without a real namespace
    });
    mock.method(_bashHooks, 'execCommand', async () => ({ stdout: '', stderr: '', exitCode: 0 }));
    const result = await bashTool.execute({ command }, ctx());
    assert.notEqual(result.isError, true, `execute should succeed: ${JSON.stringify(result)}`);
    return wrapped;
  }

  it('wraps a non-trigger command when the gate is on', async () => {
    process.env[SANDBOX_DEFAULT_DENY_ENV] = '1';
    // Unique command string per test so the read-only bash cache never collides.
    assert.equal(await wrappedFor('echo gate-on-w69'), true, 'default-deny must sandbox a non-trigger command');
  });

  it('does NOT wrap a non-trigger command when the gate is off', async () => {
    delete process.env[SANDBOX_DEFAULT_DENY_ENV];
    assert.equal(await wrappedFor('echo gate-off-w69'), false, 'without the gate the trigger allowlist applies');
  });
});
