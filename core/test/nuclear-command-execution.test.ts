/**
 * Tests that nuclear commands are blocked at the bash tool execute() level —
 * proving the command never reaches child_process.spawn / execCommand.
 *
 * SAFETY INVARIANT (why this file can never harm the real machine):
 *   These tests feed real destructive command STRINGS (rm -rf /, rm -rf $HOME,
 *   dd, mkfs, fork bomb, …) to the real bashTool.execute(). To make it
 *   PHYSICALLY impossible for any of them to run for real — even if
 *   detectNuclearCommand() is reverted, regressed, or deleted — the test
 *   environment is neutralised before any test runs:
 *
 *     1. _bashHooks.execCommand is replaced with a FAKE that records the call
 *        and returns a canned result. No real process is ever spawned by the
 *        foreground path. A blocked command must reach it ZERO times; a safe
 *        command reaches it exactly once.
 *     2. shellHooks.spawn (the real child_process.spawn underneath) is replaced
 *        with a TRIPWIRE that throws. If anything ever tries to start a real
 *        process, the test errors loudly instead of executing it.
 *     3. $HOME is pointed at a sacrificial temp dir and PATH is prefixed with a
 *        shim dir whose rm/dd/mkfs/find/… are no-ops. Even an escape resolves to
 *        nothing real.
 *
 * Red-green contract (now safe):
 *   - Revert detectNuclearCommand() → "blocked" tests FAIL because the executor
 *     fake is now called (callCount becomes 1, asserted to be 0) — but STILL no
 *     real command runs. Red without risk.
 *   - Re-apply → executor callCount is 0 for every nuclear command → green.
 */
import { describe, it, before, after, beforeEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { bashTool, _bashHooks } from '../src/tools/bash';
import { shellHooks, type ExecCommandResult } from '../src/utils/shell';
import type { ToolContext, ToolResult } from '../src/types';

let tmpDir: string;
let sacrificialHome: string | undefined;
let shimBin: string | undefined;
let origHome: string | undefined;
let origPath: string | undefined;

// The fake executor: records every (would-be) execution and returns a canned
// success. It NEVER spawns a real process.
const execCalls: string[] = [];
async function fakeExecCommand(command: string): Promise<ExecCommandResult> {
  execCalls.push(command);
  return { stdout: '[mocked: no real process was spawned]', stderr: '', exitCode: 0 };
}

function makeContext(overrides: Partial<ToolContext> = {}): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    readFile: async (p: string) => fs.readFileSync(p, 'utf-8'),
    detachedMode: false,
    ...overrides,
  } as unknown as ToolContext;
}

before(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-nuclear-exec-'));
  fs.writeFileSync(path.join(tmpDir, 'canary.txt'), 'I must survive');

  // Defense-in-depth: sacrificial HOME + PATH shim so ~/$HOME and the real
  // rm/dd/mkfs/find binaries resolve to nothing destructive even on an escape.
  sacrificialHome = fs.mkdtempSync(path.join(os.tmpdir(), 'si-sacrificial-home-'));
  shimBin = fs.mkdtempSync(path.join(os.tmpdir(), 'si-shim-bin-'));
  for (const bin of ['rm', 'rmdir', 'dd', 'mkfs', 'mkfs.ext4', 'wipefs', 'shred', 'find', 'chmod', 'chown', 'truncate', 'sudo']) {
    fs.writeFileSync(path.join(shimBin, bin), '#!/bin/sh\nexit 0\n', { mode: 0o755 });
  }
  origHome = process.env.HOME;
  origPath = process.env.PATH;
  process.env.HOME = sacrificialHome;
  process.env.PATH = `${shimBin}:${origPath ?? ''}`;

  // Primary guarantee: replace the executor with a fake, and make any real
  // spawn a hard error.
  mock.method(_bashHooks, 'execCommand', fakeExecCommand);
  mock.method(shellHooks, 'spawn', () => {
    throw new Error('SAFETY TRIPWIRE: a real child_process.spawn was attempted inside a nuclear-command safety test');
  });
});

beforeEach(() => {
  execCalls.length = 0;
});

after(() => {
  mock.restoreAll();
  if (origHome === undefined) delete process.env.HOME; else process.env.HOME = origHome;
  if (origPath === undefined) delete process.env.PATH; else process.env.PATH = origPath;
  fs.rmSync(tmpDir, { recursive: true, force: true });
  if (sacrificialHome) fs.rmSync(sacrificialHome, { recursive: true, force: true });
  if (shimBin) fs.rmSync(shimBin, { recursive: true, force: true });
});

describe('nuclear commands blocked at bash execute() — command never runs', () => {

  // -----------------------------------------------------------------------
  // Core assertion: blocked + executor never invoked + output has BLOCKED
  // -----------------------------------------------------------------------

  async function assertNuclearBlocked(command: string, label: string): Promise<void> {
    const ctx = makeContext();
    const result: ToolResult = await bashTool.execute({ command }, ctx);

    // 1. Must return isError: true
    assert.equal(result.isError, true, `${label}: must return isError: true`);

    // 2. Output must contain BLOCKED
    const output = typeof result.output === 'string' ? result.output : JSON.stringify(result.output);
    assert.ok(output.includes('BLOCKED'), `${label}: output must contain BLOCKED, got: ${output.slice(0, 200)}`);

    // 3. The executor must NEVER have been invoked — proves the command was
    //    stopped before execution. (If the guard is reverted, this flips to 1
    //    and the test fails RED, while STILL spawning nothing for real.)
    assert.equal(execCalls.length, 0, `${label}: executor must NOT be called — command must be blocked before execution`);

    // 4. Canary intact (nothing ran against the real FS regardless).
    assert.equal(fs.readFileSync(path.join(tmpDir, 'canary.txt'), 'utf-8'), 'I must survive', `${label}: canary must be intact`);
  }

  // -----------------------------------------------------------------------
  // rm -rf / — the exact incident
  // -----------------------------------------------------------------------

  it('rm -rf / is blocked, never executes', async () => {
    await assertNuclearBlocked('rm -rf /', 'rm -rf /');
  });

  it('rm -rf /* is blocked, never executes', async () => {
    await assertNuclearBlocked('rm -rf /*', 'rm -rf /*');
  });

  it('rm -rf /home is blocked, never executes', async () => {
    await assertNuclearBlocked('rm -rf /home', 'rm -rf /home');
  });

  it('rm -rf /etc is blocked, never executes', async () => {
    await assertNuclearBlocked('rm -rf /etc', 'rm -rf /etc');
  });

  it('rm -rf ~ is blocked, never executes', async () => {
    await assertNuclearBlocked('rm -rf ~', 'rm -rf ~');
  });

  it('rm -rf $HOME is blocked, never executes', async () => {
    await assertNuclearBlocked('rm -rf $HOME', 'rm -rf $HOME');
  });

  // -----------------------------------------------------------------------
  // Evasion: prefixes that strip to rm -rf /
  // -----------------------------------------------------------------------

  it('sudo rm -rf / is blocked, never executes', async () => {
    await assertNuclearBlocked('sudo rm -rf /', 'sudo rm -rf /');
  });

  it('/usr/bin/rm -rf / is blocked, never executes', async () => {
    await assertNuclearBlocked('/usr/bin/rm -rf /', '/usr/bin/rm -rf /');
  });

  it('env rm -rf / is blocked, never executes', async () => {
    await assertNuclearBlocked('env rm -rf /', 'env rm -rf /');
  });

  it('command rm -rf / is blocked, never executes', async () => {
    await assertNuclearBlocked('command rm -rf /', 'command rm -rf /');
  });

  // -----------------------------------------------------------------------
  // Compound: nuclear hidden after safe command
  // -----------------------------------------------------------------------

  it('echo hi; rm -rf / — nuclear after safe is still blocked', async () => {
    await assertNuclearBlocked('echo hi; rm -rf /', 'compound ;');
  });

  it('true && rm -rf / — nuclear after && is still blocked', async () => {
    await assertNuclearBlocked('true && rm -rf /', 'compound &&');
  });

  it('false || rm -rf / — nuclear after || is still blocked', async () => {
    await assertNuclearBlocked('false || rm -rf /', 'compound ||');
  });

  // -----------------------------------------------------------------------
  // Disk wipe
  // -----------------------------------------------------------------------

  it('mkfs.ext4 /dev/sda1 is blocked, never executes', async () => {
    await assertNuclearBlocked('mkfs.ext4 /dev/sda1', 'mkfs');
  });

  it('dd if=/dev/zero of=/dev/sda is blocked, never executes', async () => {
    await assertNuclearBlocked('dd if=/dev/zero of=/dev/sda', 'dd wipe');
  });

  it('wipefs -a /dev/sda is blocked, never executes', async () => {
    await assertNuclearBlocked('wipefs -a /dev/sda', 'wipefs');
  });

  it('shred /dev/sda is blocked, never executes', async () => {
    await assertNuclearBlocked('shred /dev/sda', 'shred');
  });

  // -----------------------------------------------------------------------
  // Fork bomb
  // -----------------------------------------------------------------------

  it(':(){ :|:& };: is blocked, never executes', async () => {
    await assertNuclearBlocked(':(){ :|:& };:', 'fork bomb');
  });

  // -----------------------------------------------------------------------
  // Separate flags
  // -----------------------------------------------------------------------

  it('rm -r -f / (separate flags) is blocked', async () => {
    await assertNuclearBlocked('rm -r -f /', 'rm -r -f /');
  });

  it('rm --recursive --force / (long flags) is blocked', async () => {
    await assertNuclearBlocked('rm --recursive --force /', 'rm --recursive --force /');
  });

  // -----------------------------------------------------------------------
  // Quoted targets
  // -----------------------------------------------------------------------

  it('rm -rf "/" (double-quoted) is blocked', async () => {
    await assertNuclearBlocked('rm -rf "/"', 'rm -rf "/"');
  });

  it("rm -rf '/' (single-quoted) is blocked", async () => {
    await assertNuclearBlocked("rm -rf '/'", "rm -rf '/'");
  });

  // -----------------------------------------------------------------------
  // Background mode: nuclear check runs BEFORE processManager.spawn()
  // -----------------------------------------------------------------------

  it('rm -rf / with run_in_background=true is still blocked', async () => {
    const ctx = makeContext();
    const result: ToolResult = await bashTool.execute(
      { command: 'rm -rf /', run_in_background: true },
      ctx,
    );
    assert.equal(result.isError, true, 'background nuclear must be blocked');
    const output = typeof result.output === 'string' ? result.output : '';
    assert.ok(output.includes('BLOCKED'), 'background output must contain BLOCKED');
    assert.equal(execCalls.length, 0, 'background nuclear must not reach the executor');
  });

  // -----------------------------------------------------------------------
  // Safe commands: prove they AREN'T blocked (they reach the executor)
  // -----------------------------------------------------------------------

  it('echo hello is permitted (reaches the executor, not blocked)', async () => {
    const ctx = makeContext();
    const result: ToolResult = await bashTool.execute({ command: 'echo hello' }, ctx);
    assert.equal(result.isError, false, 'safe command must not be blocked');
    assert.equal(execCalls.length, 1, 'safe command must be routed to the executor');
    assert.ok(execCalls[0].includes('echo hello'), 'executor must receive the safe command');
  });

  it('rm -rf inside a safe subdirectory is permitted (not blocked)', async () => {
    const subdir = path.join(tmpDir, 'deleteme');
    const ctx = makeContext();
    const result: ToolResult = await bashTool.execute({ command: `rm -rf ${subdir}` }, ctx);
    assert.equal(result.isError, false, 'rm -rf on a specific subdir must be allowed');
    assert.equal(execCalls.length, 1, 'safe rm must be routed to the executor');
    assert.ok(execCalls[0].includes(subdir), 'executor must receive the targeted rm');
    // Canary intact (the fake executor performed no real deletion).
    assert.ok(fs.existsSync(path.join(tmpDir, 'canary.txt')), 'canary must survive');
  });
});
