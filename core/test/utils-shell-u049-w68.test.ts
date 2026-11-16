/**
 * U049 (w68) — Shell-environment snapshot sourcing (user aliases / functions /
 * options / PATH) for core/core/src/utils/shell.ts.
 *
 * Parity gaps covered (Claude Code ShellSnapshot.ts + shell/bashProvider.ts):
 *  - Shell snapshot sourcing: capture the user's interactive aliases/functions/
 *    options/PATH once per session and source them before each command so a
 *    model-run `bash -c` behaves like the user's interactive shell
 *    (shellProvider.ts:L10-21, ShellSnapshot.ts).
 *  - skipSnapshot option: a per-call opt-out that runs a plain non-login shell
 *    even when snapshots are enabled (bashProvider.ts createBashShellProvider).
 *  - Login-shell (-l) graceful fallback when the snapshot is missing / failed to
 *    create (bashProvider.ts getSpawnArgs skipLoginShell / L63-103).
 *  - Debug/introspection logging of the command-assembly decisions
 *    (bashProvider.ts logForDebugging L96-103).
 *
 * Aliases are re-applied via `eval` (a second parse pass) because a shell parses
 * a whole command line before executing, so a just-sourced alias would not
 * expand without it — the exact reason Claude wraps the user command in `eval`.
 */
import { describe, it, mock, afterEach, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs';
import { EventEmitter } from 'events';
import { PassThrough } from 'stream';

import {
  execCommand,
  shellHooks,
  createShellSnapshot,
  getCachedShellSnapshot,
  resetShellSnapshotCache,
  findSuitableShell,
  SHELL_SNAPSHOT_ENV,
  SHELL_DEBUG_ENV,
} from '../src/utils/shell';

// Deterministic fake child process (drives stdout/stderr, emits 'close').
class FakeProc extends EventEmitter {
  stdout = new PassThrough();
  stderr = new PassThrough();
  stdin = new PassThrough();
  pid: number | undefined = 8168;
  kill(): boolean { return true; }
}

const SNAP_ENV_KEYS = [SHELL_SNAPSHOT_ENV, SHELL_DEBUG_ENV, 'AMI_LOGIN_SHELL'];
const savedEnv: Record<string, string | undefined> = {};

beforeEach(() => {
  for (const k of [...SNAP_ENV_KEYS, 'HOME']) savedEnv[k] = process.env[k];
  for (const k of SNAP_ENV_KEYS) delete process.env[k];
  resetShellSnapshotCache();
});

afterEach(() => {
  mock.restoreAll();
  for (const k of [...SNAP_ENV_KEYS, 'HOME']) {
    if (savedEnv[k] === undefined) delete process.env[k];
    else process.env[k] = savedEnv[k];
  }
  resetShellSnapshotCache();
});

function spawnCapture(fake: FakeProc): { shell?: string; args?: string[] } {
  const captured: { shell?: string; args?: string[] } = {};
  mock.method(shellHooks, 'spawn', ((cmd: string, args: string[]) => {
    captured.shell = cmd;
    captured.args = args;
    return fake as unknown as ReturnType<typeof shellHooks.spawn>;
  }) as typeof shellHooks.spawn);
  return captured;
}

// ---------------------------------------------------------------------------
// createShellSnapshot / getCachedShellSnapshot — creation + memoization
// ---------------------------------------------------------------------------
describe('U049 createShellSnapshot: creation and failure handling', () => {
  it('returns the snapshot path and verifies it with existsSync (mocked shell)', () => {
    let scriptShell = '';
    let scriptArgs: string[] = [];
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'mkdirSync', (() => undefined) as typeof shellHooks.mkdirSync);
    mock.method(shellHooks, 'execFileSync', ((file: string, args: string[]) => {
      scriptShell = file; scriptArgs = args; return '';
    }) as typeof shellHooks.execFileSync);
    const p = createShellSnapshot('/bin/bash');
    assert.ok(p && p.includes('ami-shell-snapshots'), 'snapshot path under ami-shell-snapshots');
    assert.ok(p!.includes('snapshot-bash-'), 'path encodes the bash shell family');
    assert.equal(scriptShell, '/bin/bash');
    assert.deepEqual(scriptArgs.slice(0, 2), ['-c', '-l'], 'capture runs a login shell');
    // The capture script sources the user rc and writes functions/options/aliases/PATH.
    assert.match(scriptArgs[2]!, /declare -F/, 'bash function capture');
    assert.match(scriptArgs[2]!, /shopt -s expand_aliases/, 'force alias expansion');
    assert.match(scriptArgs[2]!, /export PATH=/, 'PATH captured');
  });

  it('builds the zsh capture variant for a zsh shell path', () => {
    let scriptArgs: string[] = [];
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'mkdirSync', (() => undefined) as typeof shellHooks.mkdirSync);
    mock.method(shellHooks, 'execFileSync', ((_f: string, args: string[]) => { scriptArgs = args; return ''; }) as typeof shellHooks.execFileSync);
    const p = createShellSnapshot('/usr/bin/zsh');
    assert.ok(p!.includes('snapshot-zsh-'));
    assert.match(scriptArgs[2]!, /typeset \+f/, 'zsh function capture');
    assert.match(scriptArgs[2]!, /setopt/, 'zsh options capture');
  });

  it('returns null and logs when the capture shell throws', () => {
    const dbg = mock.method(shellHooks, 'onShellDebug', () => undefined);
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'mkdirSync', (() => undefined) as typeof shellHooks.mkdirSync);
    mock.method(shellHooks, 'execFileSync', (() => { throw new Error('boom'); }) as typeof shellHooks.execFileSync);
    assert.equal(createShellSnapshot('/bin/bash'), null);
    assert.ok(dbg.mock.calls.some(c => /Failed to create shell snapshot: boom/.test(String(c.arguments[0]))));
  });

  it('returns null and logs when the snapshot file is not created', () => {
    const dbg = mock.method(shellHooks, 'onShellDebug', () => undefined);
    // config exists (first existsSync true) but the snapshot file never appears.
    let calls = 0;
    mock.method(shellHooks, 'existsSync', (() => { calls += 1; return calls === 1; }) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'mkdirSync', (() => undefined) as typeof shellHooks.mkdirSync);
    mock.method(shellHooks, 'execFileSync', (() => '') as typeof shellHooks.execFileSync);
    assert.equal(createShellSnapshot('/bin/bash'), null);
    assert.ok(dbg.mock.calls.some(c => /Snapshot file was not created/.test(String(c.arguments[0]))));
  });

  it('uses the no-config script branch when the rc file is absent', () => {
    let scriptArgs: string[] = [];
    // existsSync: configFile=false, snapshot file=true
    let calls = 0;
    mock.method(shellHooks, 'existsSync', (() => { calls += 1; return calls !== 1; }) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'mkdirSync', (() => undefined) as typeof shellHooks.mkdirSync);
    mock.method(shellHooks, 'execFileSync', ((_f: string, args: string[]) => { scriptArgs = args; return ''; }) as typeof shellHooks.execFileSync);
    const p = createShellSnapshot('/bin/bash');
    assert.ok(p);
    assert.doesNotMatch(scriptArgs[2]!, /declare -F/, 'no user config → no function capture');
    assert.match(scriptArgs[2]!, /shopt -s expand_aliases/, 'bash default still enables alias expansion');
  });

  it('memoizes per shell path (getCachedShellSnapshot runs createShellSnapshot once)', () => {
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'mkdirSync', (() => undefined) as typeof shellHooks.mkdirSync);
    const spy = mock.method(shellHooks, 'execFileSync', (() => '') as typeof shellHooks.execFileSync);
    const a = getCachedShellSnapshot('/bin/bash');
    const b = getCachedShellSnapshot('/bin/bash');
    assert.equal(a, b);
    assert.equal(spy.mock.callCount(), 1, 'capture runs at most once per session per shell');
    getCachedShellSnapshot('/usr/bin/zsh');
    assert.equal(spy.mock.callCount(), 2, 'a different shell path captures again');
  });
});

// ---------------------------------------------------------------------------
// execCommand — snapshot wiring (opt-in, eval second-pass, -l fallback, skip)
// ---------------------------------------------------------------------------
describe('U049 execCommand: snapshot sourcing wiring', () => {
  it('does NOT source a snapshot by default (env unset) — behavior unchanged', async () => {
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    const snapSpy = mock.method(shellHooks, 'execFileSync', (() => '') as typeof shellHooks.execFileSync);
    const fake = new FakeProc();
    const cap = spawnCapture(fake);
    const p = execCommand('echo hi', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellPath: '/bin/bash' });
    fake.emit('close', 0, null);
    await p;
    assert.deepEqual(cap.args, ['-c', 'echo hi']);
    assert.equal(snapSpy.mock.callCount(), 0, 'no snapshot capture when disabled');
  });

  it('sources the snapshot and eval-wraps the command (no -l) when enabled + available', async () => {
    process.env[SHELL_SNAPSHOT_ENV] = '1';
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'mkdirSync', (() => undefined) as typeof shellHooks.mkdirSync);
    mock.method(shellHooks, 'execFileSync', (() => '') as typeof shellHooks.execFileSync);
    const fake = new FakeProc();
    const cap = spawnCapture(fake);
    const p = execCommand('echo hi', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellPath: '/bin/bash' });
    fake.emit('close', 0, null);
    await p;
    assert.equal(cap.args?.[0], '-c', 'no -l when a snapshot was sourced');
    assert.equal(cap.args?.length, 2);
    assert.match(cap.args![1]!, /^source '.*ami-shell-snapshots.*' 2>\/dev\/null \|\| true && eval 'echo hi'$/);
  });

  it('falls back to a login shell (-l) when snapshot creation fails', async () => {
    process.env[SHELL_SNAPSHOT_ENV] = '1';
    const dbg = mock.method(shellHooks, 'onShellDebug', () => undefined);
    // cwd exists; snapshot capture throws → createShellSnapshot returns null.
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'mkdirSync', (() => undefined) as typeof shellHooks.mkdirSync);
    mock.method(shellHooks, 'execFileSync', (() => { throw new Error('nope'); }) as typeof shellHooks.execFileSync);
    const fake = new FakeProc();
    const cap = spawnCapture(fake);
    const p = execCommand('echo hi', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellPath: '/bin/bash' });
    fake.emit('close', 0, null);
    await p;
    assert.deepEqual(cap.args, ['-l', '-c', 'echo hi'], 'login-shell fallback when no snapshot');
    assert.ok(dbg.mock.calls.some(c => /unavailable; spawning login shell/.test(String(c.arguments[0]))));
  });

  it('falls back to -l and clears a snapshot that vanished mid-session', async () => {
    process.env[SHELL_SNAPSHOT_ENV] = '1';
    const dbg = mock.method(shellHooks, 'onShellDebug', () => undefined);
    // Prime the cache with a path, then make existsSync report it gone.
    resetShellSnapshotCache();
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'mkdirSync', (() => undefined) as typeof shellHooks.mkdirSync);
    mock.method(shellHooks, 'execFileSync', (() => '') as typeof shellHooks.execFileSync);
    getCachedShellSnapshot('/bin/bash'); // cached non-null
    mock.restoreAll();
    const dbg2 = mock.method(shellHooks, 'onShellDebug', () => undefined);
    // Now the snapshot file is reported missing but cwd exists.
    mock.method(shellHooks, 'existsSync', ((pp: string) => !pp.includes('ami-shell-snapshots')) as typeof shellHooks.existsSync);
    const fake = new FakeProc();
    const cap = spawnCapture(fake);
    const p = execCommand('echo hi', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellPath: '/bin/bash' });
    fake.emit('close', 0, null);
    await p;
    assert.deepEqual(cap.args, ['-l', '-c', 'echo hi']);
    assert.ok(dbg2.mock.calls.some(c => /Snapshot file missing, falling back/.test(String(c.arguments[0]))));
    void dbg;
  });

  it('skipSnapshot:true bypasses the snapshot even when enabled', async () => {
    process.env[SHELL_SNAPSHOT_ENV] = '1';
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    const snapSpy = mock.method(shellHooks, 'execFileSync', (() => '') as typeof shellHooks.execFileSync);
    const fake = new FakeProc();
    const cap = spawnCapture(fake);
    const p = execCommand('echo hi', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellPath: '/bin/bash', skipSnapshot: true });
    fake.emit('close', 0, null);
    await p;
    assert.deepEqual(cap.args, ['-c', 'echo hi']);
    assert.equal(snapSpy.mock.callCount(), 0, 'skipSnapshot must not capture');
  });
});

// ---------------------------------------------------------------------------
// onShellDebug default sink — gated by SHELL_DEBUG_ENV
// ---------------------------------------------------------------------------
describe('U049 onShellDebug default sink', () => {
  it('writes to stderr only when SHELL_DEBUG_ENV is set', () => {
    const writes: string[] = [];
    mock.method(process.stderr, 'write', ((s: string) => { writes.push(s); return true; }) as typeof process.stderr.write);
    delete process.env[SHELL_DEBUG_ENV];
    shellHooks.onShellDebug('quiet');
    assert.equal(writes.length, 0, 'no output when debug env unset');
    process.env[SHELL_DEBUG_ENV] = '1';
    shellHooks.onShellDebug('loud');
    assert.equal(writes.length, 1);
    assert.match(writes[0]!, /\[shell-debug\] loud/);
  });
});

// ---------------------------------------------------------------------------
// E2E — real bash: a snapshot actually re-applies user aliases + functions
// ---------------------------------------------------------------------------
describe('U049 execCommand: real snapshot sourcing (e2e)', () => {
  it('re-applies the user alias and function through a real bash snapshot', async () => {
    const shell = findSuitableShell();
    if (!shell || !shell.includes('bash')) return; // requires a POSIX bash
    const home = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-snap-home-'));
    fs.writeFileSync(path.join(home, '.bashrc'), "alias greet='echo hello-alias'\nmyfn() { echo \"fn-ran:$1\"; }\n");
    process.env.HOME = home;
    process.env[SHELL_SNAPSHOT_ENV] = '1';
    resetShellSnapshotCache();
    try {
      const res = await execCommand('greet && myfn ARG', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellPath: shell });
      assert.equal(res.exitCode, 0, res.stderr);
      assert.match(res.stdout, /hello-alias/, 'user alias re-applied via snapshot + eval');
      assert.match(res.stdout, /fn-ran:ARG/, 'user function re-applied via snapshot');
    } finally {
      fs.rmSync(home, { recursive: true, force: true });
    }
  });

  it('does NOT see the user alias when skipSnapshot:true (real bash)', async () => {
    const shell = findSuitableShell();
    if (!shell || !shell.includes('bash')) return;
    const home = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-snap-home-'));
    fs.writeFileSync(path.join(home, '.bashrc'), "alias greet='echo hello-alias'\n");
    process.env.HOME = home;
    process.env[SHELL_SNAPSHOT_ENV] = '1';
    resetShellSnapshotCache();
    try {
      const res = await execCommand('greet', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellPath: shell, skipSnapshot: true });
      assert.notEqual(res.exitCode, 0, 'unknown command "greet" fails without the snapshot');
      assert.doesNotMatch(res.stdout, /hello-alias/);
    } finally {
      fs.rmSync(home, { recursive: true, force: true });
    }
  });

  it('createShellSnapshot captures the real user aliases/functions into the file', () => {
    const shell = findSuitableShell();
    if (!shell || !shell.includes('bash')) return;
    const home = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-snap-home-'));
    fs.writeFileSync(path.join(home, '.bashrc'), "alias gg='echo gg'\nhelper() { echo helper; }\n");
    process.env.HOME = home;
    resetShellSnapshotCache();
    try {
      const file = createShellSnapshot(shell);
      assert.ok(file, 'snapshot created');
      const content = fs.readFileSync(file!, 'utf8');
      assert.match(content, /alias -- gg=/, 'alias captured');
      assert.match(content, /helper \(\)/, 'function captured');
      assert.match(content, /export PATH=/, 'PATH captured');
      fs.rmSync(file!, { force: true });
    } finally {
      fs.rmSync(home, { recursive: true, force: true });
    }
  });
});
