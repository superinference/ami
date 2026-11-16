/**
 * U049 (w67) — Windows git-bash resolution + cwd-shadow executable security guard.
 *
 * Covers the parity gaps for core/core/src/utils/shell.ts:
 *  - findExecutable: trusted executable discovery on Windows (git default-location
 *    probe + where.exe) with a cwd-shadow security filter that rejects a
 *    git.exe/git.bat planted in (or under) the current working directory.
 *  - findGitBashPath: AMI_GIT_BASH_PATH / CLAUDE_CODE_GIT_BASH_PATH override,
 *    git-derived bash.exe fallback, null-on-missing (library-safe, no process.exit).
 *  - getCachedGitBashPath / resetGitBashCache: memoized discovery.
 *  - execCommand: on win32 the command runs through git-bash's bash.exe (`-c`,
 *    with the `>nul` → `/dev/null` POSIX repair), NOT `cmd`; a missing git-bash
 *    surfaces an actionable exit-126 launch failure.
 *
 * Reference: Claude Code src/utils/windowsPaths.ts (findExecutable L29-80,
 * findGitBashPath L98-125, cwd-shadow filter L65).
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
  getCachedGitBashPath,
  resetGitBashCache,
  SPAWN_FAILURE_EXIT_CODE,
} from '../src/utils/shell';

// Deterministic fake child process (drives stdout/stderr, emits 'close').
class FakeProc extends EventEmitter {
  stdout = new PassThrough();
  stderr = new PassThrough();
  stdin = new PassThrough();
  pid: number | undefined = 7767;
  kill(): boolean { return true; }
}

const GB_ENV_KEYS = ['AMI_GIT_BASH_PATH', 'CLAUDE_CODE_GIT_BASH_PATH'];

beforeEach(() => {
  for (const k of GB_ENV_KEYS) delete process.env[k];
  resetGitBashCache();
});

afterEach(() => {
  mock.restoreAll();
  for (const k of GB_ENV_KEYS) delete process.env[k];
  resetGitBashCache();
});

function spawnCapture(fake: FakeProc): { shell?: string; args?: string[]; opts?: Record<string, unknown> } {
  const captured: { shell?: string; args?: string[]; opts?: Record<string, unknown> } = {};
  mock.method(shellHooks, 'spawn', ((cmd: string, args: string[], opts: Record<string, unknown>) => {
    captured.shell = cmd;
    captured.args = args;
    captured.opts = opts;
    return fake as unknown as ReturnType<typeof shellHooks.spawn>;
  }) as typeof shellHooks.spawn);
  return captured;
}

// ---------------------------------------------------------------------------
// findExecutable — trusted discovery + cwd-shadow security filter
// ---------------------------------------------------------------------------
describe('U049 findExecutable: trusted Windows executable discovery', () => {
  it('probes git default install locations (64-bit before 32-bit) first', () => {
    const win64 = 'C:\\Program Files\\Git\\cmd\\git.exe';
    mock.method(shellHooks, 'existsSync', ((p: string) => p === win64) as typeof shellHooks.existsSync);
    const spy = mock.method(shellHooks, 'execFileSync', (() => { throw new Error('should not probe PATH'); }) as typeof shellHooks.execFileSync);
    assert.equal(findExecutable('git'), win64);
    assert.equal(spy.mock.callCount(), 0, 'default-location hit must skip where.exe');
  });

  it('falls back to where.exe when default git locations are absent', () => {
    mock.method(shellHooks, 'existsSync', (() => false) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'execFileSync', (() => '/opt/git/cmd/git.exe\n') as typeof shellHooks.execFileSync);
    assert.equal(findExecutable('git'), '/opt/git/cmd/git.exe');
  });

  it('SECURITY: rejects a candidate planted directly in the cwd, returns the next trusted one', () => {
    mock.method(shellHooks, 'existsSync', (() => false) as typeof shellHooks.existsSync);
    const planted = path.join(process.cwd(), 'git.exe');
    mock.method(shellHooks, 'execFileSync', (() => `${planted}\r\n/opt/git/cmd/git.exe\r\n`) as typeof shellHooks.execFileSync);
    assert.equal(findExecutable('git'), '/opt/git/cmd/git.exe');
  });

  it('SECURITY: rejects a candidate planted in a cwd SUBDIRECTORY (subtree guard)', () => {
    mock.method(shellHooks, 'existsSync', (() => false) as typeof shellHooks.existsSync);
    const plantedSub = path.join(process.cwd(), 'node_modules', '.bin', 'git.exe');
    mock.method(shellHooks, 'execFileSync', (() => `${plantedSub}\n/usr/bin/git\n`) as typeof shellHooks.execFileSync);
    assert.equal(findExecutable('git'), '/usr/bin/git');
  });

  it('returns null when every candidate is cwd-shadowed', () => {
    mock.method(shellHooks, 'existsSync', (() => false) as typeof shellHooks.existsSync);
    const planted = path.join(process.cwd(), 'git.exe');
    mock.method(shellHooks, 'execFileSync', (() => `${planted}\n`) as typeof shellHooks.execFileSync);
    assert.equal(findExecutable('git'), null);
  });

  it('returns null when where.exe throws (executable not on PATH)', () => {
    mock.method(shellHooks, 'existsSync', (() => false) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'execFileSync', (() => { throw new Error('not found'); }) as typeof shellHooks.execFileSync);
    assert.equal(findExecutable('git'), null);
  });

  it('skips the git default-location probe for a non-git executable', () => {
    const existsSpy = mock.method(shellHooks, 'existsSync', (() => false) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'execFileSync', (() => '/usr/bin/node\n') as typeof shellHooks.execFileSync);
    assert.equal(findExecutable('node'), '/usr/bin/node');
    assert.equal(existsSpy.mock.callCount(), 0, 'non-git must not probe git install dirs');
  });
});

// ---------------------------------------------------------------------------
// findGitBashPath — override, git-derived fallback, null-on-missing
// ---------------------------------------------------------------------------
describe('U049 findGitBashPath: git-bash bash.exe resolution', () => {
  it('honors AMI_GIT_BASH_PATH when it names an existing file', () => {
    process.env.AMI_GIT_BASH_PATH = 'D:\\tools\\git\\bin\\bash.exe';
    mock.method(shellHooks, 'existsSync', ((p: string) => p === 'D:\\tools\\git\\bin\\bash.exe') as typeof shellHooks.existsSync);
    const spy = mock.method(shellHooks, 'execFileSync', (() => { throw new Error('should not probe'); }) as typeof shellHooks.execFileSync);
    assert.equal(findGitBashPath(), 'D:\\tools\\git\\bin\\bash.exe');
    assert.equal(spy.mock.callCount(), 0);
  });

  it('accepts CLAUDE_CODE_GIT_BASH_PATH as a parity fallback', () => {
    process.env.CLAUDE_CODE_GIT_BASH_PATH = 'C:\\g\\bash.exe';
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    assert.equal(findGitBashPath(), 'C:\\g\\bash.exe');
  });

  it('returns null when the override path does not exist (no silent fallback)', () => {
    process.env.AMI_GIT_BASH_PATH = 'C:\\nope\\bash.exe';
    mock.method(shellHooks, 'existsSync', (() => false) as typeof shellHooks.existsSync);
    assert.equal(findGitBashPath(), null);
  });

  it('derives bash.exe from the resolved git.exe (<git>/cmd/git.exe → <git>/bin/bash.exe)', () => {
    const gitExe = 'C:\\Program Files\\Git\\cmd\\git.exe';
    const expectedBash = path.win32.join(gitExe, '..', '..', 'bin', 'bash.exe');
    mock.method(shellHooks, 'existsSync', ((p: string) => p === gitExe || p === expectedBash) as typeof shellHooks.existsSync);
    assert.equal(findGitBashPath(), expectedBash);
    assert.ok(expectedBash.endsWith(path.win32.join('bin', 'bash.exe')));
  });

  it('returns null when git cannot be located', () => {
    mock.method(shellHooks, 'existsSync', (() => false) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'execFileSync', (() => { throw new Error('not found'); }) as typeof shellHooks.execFileSync);
    assert.equal(findGitBashPath(), null);
  });

  it('returns null when git is found but the sibling bash.exe is missing', () => {
    const gitExe = 'C:\\Program Files\\Git\\cmd\\git.exe';
    mock.method(shellHooks, 'existsSync', ((p: string) => p === gitExe) as typeof shellHooks.existsSync);
    assert.equal(findGitBashPath(), null);
  });
});

// ---------------------------------------------------------------------------
// getCachedGitBashPath / resetGitBashCache — memoization
// ---------------------------------------------------------------------------
describe('U049 getCachedGitBashPath: memoized discovery', () => {
  it('resolves once while cached and re-resolves after resetGitBashCache', () => {
    process.env.AMI_GIT_BASH_PATH = 'C:\\g\\bash.exe';
    let calls = 0;
    mock.method(shellHooks, 'existsSync', ((p: string) => { if (p === 'C:\\g\\bash.exe') { calls++; return true; } return false; }) as typeof shellHooks.existsSync);
    assert.equal(getCachedGitBashPath(), 'C:\\g\\bash.exe');
    assert.equal(getCachedGitBashPath(), 'C:\\g\\bash.exe');
    assert.equal(calls, 1, 'discovery should run once while cached');
    resetGitBashCache();
    assert.equal(getCachedGitBashPath(), 'C:\\g\\bash.exe');
    assert.equal(calls, 2, 'reset should force re-discovery');
  });
});

// ---------------------------------------------------------------------------
// execCommand wiring — Windows runs through git-bash, not cmd (integration)
// ---------------------------------------------------------------------------
describe('U049 execCommand: Windows git-bash wiring', () => {
  it('spawns git-bash bash.exe with `-c` (not cmd /c) and rewrites >nul on win32', async () => {
    mock.method(shellHooks, 'platform', () => 'win32');
    process.env.AMI_GIT_BASH_PATH = 'C:\\Git\\bin\\bash.exe';
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    const fake = new FakeProc();
    const cap = spawnCapture(fake);
    const p = execCommand('ls 2>nul', { cwd: os.tmpdir(), stallTimeoutMs: 0 });
    fake.emit('close', 0, null);
    await p;
    assert.equal(cap.shell, 'C:\\Git\\bin\\bash.exe', 'win32 must run through git-bash, not cmd');
    assert.equal(cap.args?.[0], '-c');
    assert.equal(cap.args?.[1], 'ls 2>/dev/null', '>nul must be repaired to /dev/null under bash');
  });

  it('prefers an explicit shellPath over discovery on win32', async () => {
    mock.method(shellHooks, 'platform', () => 'win32');
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    const fake = new FakeProc();
    const cap = spawnCapture(fake);
    const p = execCommand('echo hi', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellPath: 'E:\\custom\\bash.exe' });
    fake.emit('close', 0, null);
    await p;
    assert.equal(cap.shell, 'E:\\custom\\bash.exe');
    assert.deepEqual(cap.args, ['-c', 'echo hi']);
  });

  it('surfaces an actionable exit-126 launch failure when no git-bash is found on win32', async () => {
    mock.method(shellHooks, 'platform', () => 'win32');
    // cwd exists, but no git default locations and where.exe fails → no bash.
    mock.method(shellHooks, 'existsSync', ((p: string) => p === os.tmpdir()) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'execFileSync', (() => { throw new Error('not found'); }) as typeof shellHooks.execFileSync);
    const spawnSpy = mock.method(shellHooks, 'spawn', (() => { throw new Error('should not spawn'); }) as typeof shellHooks.spawn);
    const res = await execCommand('echo hi', { cwd: os.tmpdir(), stallTimeoutMs: 0 });
    assert.equal(res.exitCode, SPAWN_FAILURE_EXIT_CODE);
    assert.match(res.stderr, /No git-bash found/);
    assert.match(res.stderr, /AMI_GIT_BASH_PATH/);
    assert.equal(spawnSpy.mock.callCount(), 0);
  });
});

// ---------------------------------------------------------------------------
// E2E — the unified bash branch still runs real commands on POSIX
// ---------------------------------------------------------------------------
describe('U049 execCommand: real POSIX bash path unchanged (e2e)', () => {
  it('runs a real command through the resolved POSIX shell and repairs >nul', async () => {
    const res = await execCommand('printf "hi\\n" 1>nul; echo kept', { cwd: os.tmpdir(), stallTimeoutMs: 0 });
    // `1>nul` is rewritten to `/dev/null`, so only "kept" reaches stdout, and no
    // literal `nul` file is created. A real bash/zsh executes this end to end.
    assert.equal(res.exitCode, 0);
    assert.match(res.stdout, /kept/);
    assert.ok(!res.stdout.includes('hi'), 'output redirected to /dev/null must not appear');
  });
});
