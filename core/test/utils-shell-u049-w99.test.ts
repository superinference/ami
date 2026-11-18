/**
 * U049 (w99) — async per-shell environment overrides seam for
 * core/core/src/utils/shell.ts.
 *
 * Parity gap covered (verified=confirmed, ami-missing):
 *  - Async per-shell environment overrides (getEnvironmentOverrides), the hook
 *    Claude Code's ShellProvider exposes to compute extra env per shell family
 *    just before spawn, MAY performing async initialization — the reference cites
 *    tmux socket setup for bash (shellProvider.ts:L28-32 / bashProvider.ts:L208).
 *    AMI previously passed env verbatim via options.env with no per-shell async
 *    override seam.
 *
 * AMI adaptation (documented in shell.ts): the seam is OPT-IN via
 * AMI_TMUX_ISOLATION and returns a plain record SYNCHRONOUSLY for the common
 * case (so the executor's deterministic same-tick spawn is preserved) and a
 * Promise only when a shell genuinely needs init (bash tmux-socket isolation,
 * which async-creates the socket dir and points TMUX_TMPDIR at it). execCommand
 * merges the overrides on top of the caller's env.
 *
 * Includes a real-bash E2E proving a tmux-using command sees the isolated
 * TMUX_TMPDIR in its environment.
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
  getEnvironmentOverrides,
} from '../src/utils/shell';
import {
  commandUsesTmux,
  TMUX_ISOLATION_ENV,
  tmuxSocketHooks,
  __resetTmuxSocketStateForTest,
} from '../src/utils/tmux-socket';

class FakeProc extends EventEmitter {
  stdout = new PassThrough();
  stderr = new PassThrough();
  stdin = new PassThrough();
  pid: number | undefined = 4242;
  kill(): boolean { return true; }
}

function spawnCapture(fake: FakeProc): { opts?: Record<string, unknown> } {
  const captured: { opts?: Record<string, unknown> } = {};
  mock.method(shellHooks, 'spawn', ((_cmd: string, _args: string[], opts: Record<string, unknown>) => {
    captured.opts = opts;
    return fake as unknown as ReturnType<typeof shellHooks.spawn>;
  }) as typeof shellHooks.spawn);
  return captured;
}

function isThenable(v: unknown): boolean {
  return !!v && typeof (v as { then?: unknown }).then === 'function';
}

const ENV_KEYS = [TMUX_ISOLATION_ENV, 'AMI_TMPDIR', 'CLAUDE_CODE_TMPDIR', 'AMI_SHELL_DEBUG', 'TMUX_TMPDIR'];
const savedEnv: Record<string, string | undefined> = {};
beforeEach(() => {
  for (const k of ENV_KEYS) { savedEnv[k] = process.env[k]; delete process.env[k]; }
  // Reset the tmux-socket singleton state and simulate tmux being installed so
  // the isolation path exercises its wiring on hosts without a real tmux binary.
  // The on-exit teardown is stubbed so tests don't leak `process.exit` listeners.
  __resetTmuxSocketStateForTest();
  mock.method(tmuxSocketHooks, 'probeTmuxAvailable', () => true);
  mock.method(tmuxSocketHooks, 'registerTeardown', () => {});
});
afterEach(() => {
  mock.restoreAll();
  for (const k of ENV_KEYS) {
    if (savedEnv[k] === undefined) delete process.env[k]; else process.env[k] = savedEnv[k];
  }
});

// ---------------------------------------------------------------------------
// commandUsesTmux — word-boundary tmux detection
// ---------------------------------------------------------------------------
describe('U049 w99 — commandUsesTmux', () => {
  it('matches a bare tmux word and tmux subcommands', () => {
    assert.equal(commandUsesTmux('tmux'), true);
    assert.equal(commandUsesTmux('tmux new -s work'), true);
    assert.equal(commandUsesTmux('echo hi; tmux ls'), true);
    assert.equal(commandUsesTmux('TMUX=/x tmux attach'), true);
  });

  it('does not match non-tmux commands or tmux as a substring', () => {
    assert.equal(commandUsesTmux('echo hi'), false);
    assert.equal(commandUsesTmux('ls -la'), false);
    assert.equal(commandUsesTmux('mytmuxwrapper'), false);
    assert.equal(commandUsesTmux('notmux'), false);
  });
});

// ---------------------------------------------------------------------------
// getEnvironmentOverrides — sync fast path vs async tmux init
// ---------------------------------------------------------------------------
describe('U049 w99 — getEnvironmentOverrides', () => {
  it('returns a SYNCHRONOUS empty record for powershell (no tmux seam)', () => {
    process.env[TMUX_ISOLATION_ENV] = '1';
    const res = getEnvironmentOverrides('powershell', 'tmux new');
    assert.equal(isThenable(res), false);
    assert.deepEqual(res, {});
  });

  it('returns a SYNCHRONOUS empty record for bash when isolation is disabled', () => {
    const res = getEnvironmentOverrides('bash', 'tmux new');
    assert.equal(isThenable(res), false);
    assert.deepEqual(res, {});
  });

  it('returns a SYNCHRONOUS empty record for bash when isolation is on but the command has no tmux', () => {
    process.env[TMUX_ISOLATION_ENV] = '1';
    const res = getEnvironmentOverrides('bash', 'echo hi');
    assert.equal(isThenable(res), false);
    assert.deepEqual(res, {});
  });

  it('treats blank / unknown AMI_TMUX_ISOLATION values as disabled', () => {
    for (const bad of ['', '  ', '0', 'false', 'off', 'no', 'nope']) {
      process.env[TMUX_ISOLATION_ENV] = bad;
      assert.deepEqual(getEnvironmentOverrides('bash', 'tmux ls'), {}, `bad=${JSON.stringify(bad)}`);
    }
  });

  it('accepts 1/true/on/yes as enabled and returns an async TMUX_TMPDIR override', async () => {
    process.env.AMI_TMPDIR = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-tmuxbase-'));
    const calls: Array<{ dir: unknown; opts: unknown }> = [];
    mock.method(tmuxSocketHooks, 'mkdirAsync', (async (dir: unknown, opts: unknown) => {
      calls.push({ dir, opts });
      return undefined;
    }) as typeof tmuxSocketHooks.mkdirAsync);
    try {
      for (const good of ['1', 'true', 'on', 'yes', 'TRUE', 'On']) {
        calls.length = 0;
        process.env[TMUX_ISOLATION_ENV] = good;
        const res = getEnvironmentOverrides('bash', 'tmux ls');
        assert.equal(isThenable(res), true, `good=${good} should be async`);
        const resolved = await res;
        const expectedDir = path.join(process.env.AMI_TMPDIR!, 'ami-tmux');
        assert.deepEqual(resolved, { TMUX_TMPDIR: expectedDir });
        assert.equal(calls.length, 1);
        assert.equal(calls[0]!.dir, expectedDir);
        assert.deepEqual(calls[0]!.opts, { recursive: true });
      }
    } finally {
      fs.rmSync(process.env.AMI_TMPDIR!, { recursive: true, force: true });
    }
  });

  it('is best-effort: a failed socket-dir setup resolves to {} without throwing', async () => {
    process.env[TMUX_ISOLATION_ENV] = '1';
    process.env.AMI_SHELL_DEBUG = '1';
    const debug: string[] = [];
    mock.method(tmuxSocketHooks, 'onDebug', ((m: string) => { debug.push(m); }) as typeof tmuxSocketHooks.onDebug);
    mock.method(tmuxSocketHooks, 'mkdirAsync', (async () => { throw new Error('EACCES denied'); }) as typeof tmuxSocketHooks.mkdirAsync);
    const res = getEnvironmentOverrides('bash', 'tmux ls');
    assert.equal(isThenable(res), true);
    assert.deepEqual(await res, {});
    assert.ok(debug.some(m => /socket dir setup failed/.test(m) && /EACCES denied/.test(m)), debug.join(' | '));
  });
});

// ---------------------------------------------------------------------------
// execCommand wiring — overrides merged into the spawn env
// ---------------------------------------------------------------------------
describe('U049 w99 — execCommand merges per-shell env overrides', () => {
  it('fast path: no overrides + no caller env leaves the spawn env unset (no regression, same-tick spawn)', async () => {
    const fake = new FakeProc();
    const captured = spawnCapture(fake);
    mock.method(shellHooks, 'getEnvironmentOverrides', (() => ({})) as typeof shellHooks.getEnvironmentOverrides);
    const p = execCommand('echo hi', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellPath: '/bin/bash' });
    // The override is synchronous, so spawn must have happened on THIS tick —
    // a synchronous close must be observed (proves no async regression).
    assert.ok(captured.opts, 'spawn was called synchronously on the fast path');
    assert.equal(captured.opts!.env, undefined);
    fake.emit('close', 0, null);
    await p;
  });

  it('sync overrides are merged on top of the inherited process env', async () => {
    const fake = new FakeProc();
    const captured = spawnCapture(fake);
    mock.method(shellHooks, 'getEnvironmentOverrides', (() => ({ TMUX_TMPDIR: '/iso/x' })) as typeof shellHooks.getEnvironmentOverrides);
    const p = execCommand('echo hi', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellPath: '/bin/bash' });
    const env = captured.opts!.env as NodeJS.ProcessEnv;
    assert.equal(env.TMUX_TMPDIR, '/iso/x');
    assert.equal(env.PATH, process.env.PATH, 'inherited process env is preserved');
    fake.emit('close', 0, null);
    await p;
  });

  it('sync overrides are merged on top of a caller-supplied env (overrides win)', async () => {
    const fake = new FakeProc();
    const captured = spawnCapture(fake);
    mock.method(shellHooks, 'getEnvironmentOverrides', (() => ({ TMUX_TMPDIR: '/iso/y', FOO: 'override' })) as typeof shellHooks.getEnvironmentOverrides);
    const p = execCommand('echo hi', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellPath: '/bin/bash', env: { FOO: 'base', BAR: 'keep' } as NodeJS.ProcessEnv });
    const env = captured.opts!.env as NodeJS.ProcessEnv;
    assert.equal(env.FOO, 'override');
    assert.equal(env.BAR, 'keep');
    assert.equal(env.TMUX_TMPDIR, '/iso/y');
    fake.emit('close', 0, null);
    await p;
  });

  it('async overrides: the command launches only after the promise settles, with the merged env', async () => {
    const fake = new FakeProc();
    const captured = spawnCapture(fake);
    mock.method(shellHooks, 'getEnvironmentOverrides', (() => Promise.resolve({ TMUX_TMPDIR: '/iso/async' })) as typeof shellHooks.getEnvironmentOverrides);
    const p = execCommand('echo hi', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellPath: '/bin/bash' });
    // Deferred: spawn has NOT happened yet on this tick.
    assert.equal(captured.opts, undefined, 'spawn is deferred until the async override settles');
    await new Promise((r) => setImmediate(r));
    assert.ok(captured.opts, 'spawn ran after the override promise settled');
    assert.equal((captured.opts!.env as NodeJS.ProcessEnv).TMUX_TMPDIR, '/iso/async');
    fake.emit('close', 0, null);
    await p;
  });

  it('async override rejection is best-effort: the command still launches with no overrides', async () => {
    const fake = new FakeProc();
    const captured = spawnCapture(fake);
    mock.method(shellHooks, 'getEnvironmentOverrides', (() => Promise.reject(new Error('boom'))) as typeof shellHooks.getEnvironmentOverrides);
    const p = execCommand('echo hi', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellPath: '/bin/bash' });
    await new Promise((r) => setImmediate(r));
    assert.ok(captured.opts, 'spawn ran after the rejected override');
    assert.equal(captured.opts!.env, undefined, 'no overrides applied on rejection');
    fake.emit('close', 0, null);
    await p;
  });
});

// ---------------------------------------------------------------------------
// E2E (real bash) — a tmux-using command sees the isolated TMUX_TMPDIR
// ---------------------------------------------------------------------------
describe('U049 w99 — tmux isolation E2E (real bash)', () => {
  const hasBash = fs.existsSync('/bin/bash');
  it('points TMUX_TMPDIR at the AMI-managed socket dir for a tmux-using command', { skip: !hasBash }, async () => {
    const base = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-tmux-e2e-'));
    process.env.AMI_TMPDIR = base;
    process.env[TMUX_ISOLATION_ENV] = '1';
    try {
      // The command references `tmux` (triggering isolation) but does not need it
      // installed — it just prints the injected env var the seam set.
      const result = await execCommand('echo tmux-marker; printf "TMPDIR=%s\\n" "$TMUX_TMPDIR"', {
        cwd: base,
        stallTimeoutMs: 0,
        shellPath: '/bin/bash',
      });
      const expectedDir = path.join(base, 'ami-tmux');
      assert.match(result.stdout, new RegExp(`TMPDIR=${expectedDir.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}`));
      assert.ok(fs.existsSync(expectedDir), 'the socket dir was created');
    } finally {
      fs.rmSync(base, { recursive: true, force: true });
    }
  });

  it('does NOT set TMUX_TMPDIR for a non-tmux command (opt-in, scoped)', { skip: !hasBash }, async () => {
    const base = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-tmux-e2e2-'));
    process.env.AMI_TMPDIR = base;
    process.env[TMUX_ISOLATION_ENV] = '1';
    try {
      const result = await execCommand('printf "TMPDIR=[%s]\\n" "$TMUX_TMPDIR"', {
        cwd: base,
        stallTimeoutMs: 0,
        shellPath: '/bin/bash',
      });
      assert.match(result.stdout, /TMPDIR=\[\]/);
    } finally {
      fs.rmSync(base, { recursive: true, force: true });
    }
  });
});
