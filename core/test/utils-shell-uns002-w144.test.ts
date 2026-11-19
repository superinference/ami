/**
 * UNS002 (w144) — Windows executable-discovery diagnostics + distinct configured-path error.
 *
 * Additive parity for core/core/src/utils/shell.ts against Claude Code's
 * src/utils/windowsPaths.ts:
 *  - findExecutable emits a debug/introspection trace through the opt-in
 *    {@link shellHooks.onShellDebug} surface each time a `where.exe` candidate is
 *    rejected because it resolves inside the current working directory (a planted
 *    git.exe/.bat), so a user debugging why their git/shell was skipped can see
 *    the security filter firing — parity with Claude's
 *    `logForDebugging('Skipping potentially malicious executable …')`
 *    (windowsPaths.ts:L64-68).
 *  - findGitBashPath distinguishes "you set AMI_GIT_BASH_PATH/CLAUDE_CODE_GIT_BASH_PATH
 *    but that path does not exist" from "git-bash not found at all", echoing the
 *    bad value back so a typo is obvious — parity with windowsPaths.ts:L104-106
 *    vs L120-122.
 *  - describeGitBashResolutionError builds the distinct user-facing exit-126
 *    launch-failure message execCommand surfaces (echoes the bad configured value).
 *  - GIT_DEFAULT_LOCATIONS probe order lock: 64-bit before 32-bit (regression).
 *
 * Reference: Claude Code src/utils/windowsPaths.ts.
 */
import { describe, it, mock, afterEach, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import * as path from 'path';
import { EventEmitter } from 'events';
import { PassThrough } from 'stream';

import {
  execCommand,
  shellHooks,
  findExecutable,
  findGitBashPath,
  describeGitBashResolutionError,
  resetGitBashCache,
  SPAWN_FAILURE_EXIT_CODE,
} from '../src/utils/shell';

class FakeProc extends EventEmitter {
  stdout = new PassThrough();
  stderr = new PassThrough();
  stdin = new PassThrough();
  pid: number | undefined = 7601;
  kill(): boolean { return true; }
}

const GB_ENV_KEYS = ['AMI_GIT_BASH_PATH', 'CLAUDE_CODE_GIT_BASH_PATH'];

function captureDebug(): string[] {
  const msgs: string[] = [];
  mock.method(shellHooks, 'onShellDebug', ((m: string) => { msgs.push(m); }) as typeof shellHooks.onShellDebug);
  return msgs;
}

beforeEach(() => {
  for (const k of GB_ENV_KEYS) delete process.env[k];
  resetGitBashCache();
});

afterEach(() => {
  mock.restoreAll();
  for (const k of GB_ENV_KEYS) delete process.env[k];
  resetGitBashCache();
});

// ---------------------------------------------------------------------------
// Gap 3 — findExecutable diagnostic for cwd-shadowed (potentially malicious) hits
// ---------------------------------------------------------------------------
describe('UNS002 findExecutable: cwd-shadow skip diagnostic', () => {
  it('logs a "potentially malicious" debug line for each cwd-resident candidate skipped', () => {
    const planted = path.join(process.cwd(), 'git'); // resolves inside cwd → shadowed
    const real = '/usr/bin/git';
    mock.method(shellHooks, 'existsSync', (() => false) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'execFileSync', (() => `${planted}\r\n${real}\r\n`) as typeof shellHooks.execFileSync);
    const debug = captureDebug();

    assert.equal(findExecutable('git'), real, 'the trusted (non-cwd) candidate is returned');
    assert.ok(
      debug.some(m => m === `Skipping potentially malicious executable in current directory: ${planted}`),
      `expected a skip diagnostic echoing the planted path; got: ${JSON.stringify(debug)}`,
    );
  });

  it('emits NO skip diagnostic when the first candidate is already trusted', () => {
    mock.method(shellHooks, 'existsSync', (() => false) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'execFileSync', (() => '/usr/bin/node\n') as typeof shellHooks.execFileSync);
    const debug = captureDebug();

    assert.equal(findExecutable('node'), '/usr/bin/node');
    assert.ok(!debug.some(m => m.startsWith('Skipping potentially malicious')), 'no skip trace for a clean path');
  });
});

// ---------------------------------------------------------------------------
// Gap 2 — distinct configured-path-missing vs not-found diagnostics
// ---------------------------------------------------------------------------
describe('UNS002 findGitBashPath: distinct configured-path diagnostic', () => {
  it('echoes the bad configured value when the override path does not exist', () => {
    process.env.AMI_GIT_BASH_PATH = 'C:\\typo\\bash.exe';
    mock.method(shellHooks, 'existsSync', (() => false) as typeof shellHooks.existsSync);
    const debug = captureDebug();

    assert.equal(findGitBashPath(), null);
    assert.ok(
      debug.some(m => m.includes('C:\\typo\\bash.exe') && /configured/i.test(m)),
      `expected a configured-path-missing trace echoing the value; got: ${JSON.stringify(debug)}`,
    );
    assert.ok(!debug.some(m => m.includes('git-bash not found')), 'must NOT also emit the not-found-at-all trace');
  });

  it('emits the distinct "not found at all" trace (no echoed value) when no override is set', () => {
    mock.method(shellHooks, 'existsSync', (() => false) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'execFileSync', (() => { throw new Error('nope'); }) as typeof shellHooks.execFileSync);
    const debug = captureDebug();

    assert.equal(findGitBashPath(), null);
    assert.ok(debug.some(m => m.includes('git-bash not found')), `expected a not-found trace; got: ${JSON.stringify(debug)}`);
  });
});

describe('UNS002 describeGitBashResolutionError: user-facing distinct message', () => {
  it('echoes the bad configured value supplied by the user', () => {
    process.env.AMI_GIT_BASH_PATH = 'C:\\wrong\\place\\bash.exe';
    const msg = describeGitBashResolutionError();
    assert.match(msg, /C:\\wrong\\place\\bash\.exe/);
    assert.match(msg, /does not exist/);
  });

  it('accepts CLAUDE_CODE_GIT_BASH_PATH as the configured source too', () => {
    process.env.CLAUDE_CODE_GIT_BASH_PATH = 'C:\\cc\\bash.exe';
    assert.match(describeGitBashResolutionError(), /C:\\cc\\bash\.exe/);
  });

  it('falls back to the generic "No git-bash found" message when nothing is configured', () => {
    const msg = describeGitBashResolutionError();
    assert.match(msg, /No git-bash found/);
    assert.match(msg, /AMI_GIT_BASH_PATH/);
    assert.doesNotMatch(msg, /does not exist/);
  });
});

// ---------------------------------------------------------------------------
// Integration — execCommand surfaces the distinct error on win32
// ---------------------------------------------------------------------------
describe('UNS002 execCommand: distinct configured-path launch failure (win32)', () => {
  it('echoes the bad AMI_GIT_BASH_PATH in the exit-126 stderr instead of the generic message', async () => {
    mock.method(shellHooks, 'platform', () => 'win32');
    process.env.AMI_GIT_BASH_PATH = 'C:\\does\\not\\exist\\bash.exe';
    mock.method(shellHooks, 'existsSync', ((p: string) => p === os.tmpdir()) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'execFileSync', (() => { throw new Error('not found'); }) as typeof shellHooks.execFileSync);
    const spawnSpy = mock.method(shellHooks, 'spawn', (() => { throw new Error('should not spawn'); }) as typeof shellHooks.spawn);

    const res = await execCommand('echo hi', { cwd: os.tmpdir(), stallTimeoutMs: 0 });
    assert.equal(res.exitCode, SPAWN_FAILURE_EXIT_CODE);
    assert.match(res.stderr, /C:\\does\\not\\exist\\bash\.exe/, 'bad configured value is echoed back');
    assert.match(res.stderr, /does not exist/);
    assert.equal(spawnSpy.mock.callCount(), 0);
  });

  it('keeps the generic "No git-bash found" message when nothing is configured (regression lock)', async () => {
    mock.method(shellHooks, 'platform', () => 'win32');
    mock.method(shellHooks, 'existsSync', ((p: string) => p === os.tmpdir()) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'execFileSync', (() => { throw new Error('not found'); }) as typeof shellHooks.execFileSync);
    const spawnSpy = mock.method(shellHooks, 'spawn', (() => { throw new Error('should not spawn'); }) as typeof shellHooks.spawn);

    const res = await execCommand('echo hi', { cwd: os.tmpdir(), stallTimeoutMs: 0 });
    assert.equal(res.exitCode, SPAWN_FAILURE_EXIT_CODE);
    assert.match(res.stderr, /No git-bash found/);
    assert.equal(spawnSpy.mock.callCount(), 0);
  });
});

// ---------------------------------------------------------------------------
// Gap 1 — GIT_DEFAULT_LOCATIONS probe order lock (already implemented)
// ---------------------------------------------------------------------------
describe('UNS002 findExecutable: git default-location probe order (64-bit before 32-bit)', () => {
  it('probes the 64-bit install location before the 32-bit (x86) fallback', () => {
    const probed: string[] = [];
    mock.method(shellHooks, 'existsSync', ((p: string) => { probed.push(p); return false; }) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'execFileSync', (() => { throw new Error('no where.exe'); }) as typeof shellHooks.execFileSync);

    assert.equal(findExecutable('git'), null);
    const i64 = probed.indexOf('C:\\Program Files\\Git\\cmd\\git.exe');
    const i32 = probed.indexOf('C:\\Program Files (x86)\\Git\\cmd\\git.exe');
    assert.ok(i64 >= 0 && i32 >= 0, 'both default git locations are probed');
    assert.ok(i64 < i32, '64-bit must be probed before 32-bit (x86)');
  });
});
