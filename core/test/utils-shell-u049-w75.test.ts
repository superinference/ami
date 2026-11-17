/**
 * U049 (w75) — additive execCommand / shell-util capabilities for
 * core/core/src/utils/shell.ts.
 *
 * Parity gaps covered (all verified=confirmed):
 *  - Configurable default command timeout via AMI_DEFAULT_TIMEOUT_MS /
 *    CLAUDE_CODE_SHELL_TIMEOUT (resolveDefaultTimeoutMs), so long builds/test
 *    suites can match Claude Code's 30-minute default (Shell.ts:L44) instead of
 *    AMI's conservative 2-minute baseline.
 *  - Interactive-shell introspection getShellType() → 'zsh'|'bash'|'fish'|
 *    'unknown' for shell-tailored setup hints (localInstaller.ts:L153-162), also
 *    consolidated as the single source of resolveShell's bash-vs-zsh preference.
 *  - Persist the COMPLETE stdout to disk (capped at 64MB) for model retrieval on
 *    large output (persistFullOutput → fullOutputPath/fullOutputSize), so the
 *    middle AMI's in-memory head+tail window drops is never lost
 *    (BashTool.tsx:L725-753 / PowerShellTool.tsx:L587-617).
 */
import { describe, it, mock, afterEach, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import * as fs from 'fs';
import { EventEmitter } from 'events';
import { PassThrough } from 'stream';

import {
  execCommand,
  shellHooks,
  getShellType,
  resolveDefaultTimeoutMs,
  DEFAULT_TIMEOUT_MS,
  MAX_PERSISTED_OUTPUT_BYTES,
} from '../src/utils/shell';

class FakeProc extends EventEmitter {
  stdout = new PassThrough();
  stderr = new PassThrough();
  stdin = new PassThrough();
  pid: number | undefined = 9192;
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

const ENV_KEYS = ['AMI_DEFAULT_TIMEOUT_MS', 'CLAUDE_CODE_SHELL_TIMEOUT', 'SHELL'];
const savedEnv: Record<string, string | undefined> = {};
beforeEach(() => {
  for (const k of ENV_KEYS) { savedEnv[k] = process.env[k]; delete process.env[k]; }
});
afterEach(() => {
  mock.restoreAll();
  for (const k of ENV_KEYS) {
    if (savedEnv[k] === undefined) delete process.env[k]; else process.env[k] = savedEnv[k];
  }
});

// ---------------------------------------------------------------------------
// getShellType — interactive login-shell classifier (parity: getShellType)
// ---------------------------------------------------------------------------
describe('U049 getShellType', () => {
  it('classifies zsh, bash, fish and unknown from an explicit path', () => {
    assert.equal(getShellType('/usr/bin/zsh'), 'zsh');
    assert.equal(getShellType('/bin/bash'), 'bash');
    assert.equal(getShellType('/usr/local/bin/fish'), 'fish');
    assert.equal(getShellType('/usr/bin/dash'), 'unknown');
    assert.equal(getShellType(''), 'unknown');
  });

  it('prefers zsh over bash when both substrings appear (Claude precedence)', () => {
    // A path mentioning both resolves to zsh first — matches localInstaller order.
    assert.equal(getShellType('/opt/zsh/bash-compat'), 'zsh');
  });

  it('defaults to process.env.SHELL when no argument is given', () => {
    process.env.SHELL = '/usr/bin/fish';
    assert.equal(getShellType(), 'fish');
    delete process.env.SHELL;
    assert.equal(getShellType(), 'unknown');
  });
});

// ---------------------------------------------------------------------------
// resolveDefaultTimeoutMs — env-overridable default command timeout
// ---------------------------------------------------------------------------
describe('U049 resolveDefaultTimeoutMs', () => {
  it('returns the 120s baseline when unset', () => {
    assert.equal(resolveDefaultTimeoutMs(), DEFAULT_TIMEOUT_MS);
    assert.equal(DEFAULT_TIMEOUT_MS, 120_000);
  });

  it('honors AMI_DEFAULT_TIMEOUT_MS (e.g. Claude Code 30-minute default)', () => {
    process.env.AMI_DEFAULT_TIMEOUT_MS = String(30 * 60 * 1000);
    assert.equal(resolveDefaultTimeoutMs(), 1_800_000);
  });

  it('accepts CLAUDE_CODE_SHELL_TIMEOUT as a parity fallback', () => {
    process.env.CLAUDE_CODE_SHELL_TIMEOUT = '600000';
    assert.equal(resolveDefaultTimeoutMs(), 600_000);
  });

  it('prefers AMI_DEFAULT_TIMEOUT_MS over CLAUDE_CODE_SHELL_TIMEOUT', () => {
    process.env.AMI_DEFAULT_TIMEOUT_MS = '111';
    process.env.CLAUDE_CODE_SHELL_TIMEOUT = '222';
    assert.equal(resolveDefaultTimeoutMs(), 111);
  });

  it('falls back to the baseline for blank / NaN / non-positive values', () => {
    for (const bad of ['', '   ', 'abc', '0', '-5', 'NaN']) {
      process.env.AMI_DEFAULT_TIMEOUT_MS = bad;
      assert.equal(resolveDefaultTimeoutMs(), DEFAULT_TIMEOUT_MS, `bad=${JSON.stringify(bad)}`);
    }
  });

  it('is used as execCommand\'s default timeout when no timeout is passed', async () => {
    process.env.AMI_DEFAULT_TIMEOUT_MS = '5000';
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    const fake = new FakeProc();
    spawnCapture(fake);
    // Spy on setTimeout to observe the timeout the exec arms.
    const realSetTimeout = global.setTimeout;
    const armed: number[] = [];
    (global as unknown as { setTimeout: typeof setTimeout }).setTimeout = ((fn: () => void, ms?: number, ...rest: unknown[]) => {
      if (typeof ms === 'number') armed.push(ms);
      return realSetTimeout(fn, ms, ...(rest as []));
    }) as typeof setTimeout;
    try {
      const p = execCommand('echo hi', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellPath: '/bin/bash' });
      fake.stdout.write('hi\n');
      fake.emit('close', 0, null);
      await p;
    } finally {
      (global as unknown as { setTimeout: typeof setTimeout }).setTimeout = realSetTimeout;
    }
    assert.ok(armed.includes(5000), `expected a 5000ms timeout to be armed, saw ${armed.join(',')}`);
  });
});

// ---------------------------------------------------------------------------
// persistFullOutput — complete stdout to disk, 64MB cap, truncation-gated
// ---------------------------------------------------------------------------
describe('U049 execCommand persistFullOutput', () => {
  it('exposes a 64MB persisted-output ceiling constant', () => {
    assert.equal(MAX_PERSISTED_OUTPUT_BYTES, 64 * 1024 * 1024);
  });

  it('persists the COMPLETE stdout to a file when output overflows the in-memory window', async () => {
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    const fake = new FakeProc();
    spawnCapture(fake);
    // 1,000,100 chars > HEAD(670k)+TAIL(330k): the in-memory buffer drops 100 chars
    // from the middle, so the on-disk copy must be strictly more complete.
    const total = 1_000_100;
    const big = 'x'.repeat(total);
    const p = execCommand('bigcmd', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellPath: '/bin/bash', persistFullOutput: true });
    fake.stdout.write(big);
    fake.emit('close', 0, null);
    const result = await p;
    assert.ok(result.fullOutputPath, 'fullOutputPath is set when output was truncated');
    assert.equal(result.fullOutputSize, total, 'fullOutputSize reflects the complete byte count');
    // The in-memory stdout shows a truncation banner; the FILE has every byte.
    assert.match(result.stdout, /truncated/);
    const onDisk = fs.readFileSync(result.fullOutputPath!, 'utf8');
    assert.equal(onDisk.length, total, 'the persisted file holds the full, untruncated output');
    assert.equal(onDisk, big);
    fs.unlinkSync(result.fullOutputPath!);
  });

  it('does NOT persist (and leaves no file) when output fits the in-memory window', async () => {
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    const fake = new FakeProc();
    spawnCapture(fake);
    let openedPath: string | null = null;
    const realOpen = shellHooks.openSync;
    mock.method(shellHooks, 'openSync', ((pth: string, flags: string) => {
      openedPath = pth;
      return realOpen(pth, flags);
    }) as typeof shellHooks.openSync);
    const p = execCommand('smallcmd', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellPath: '/bin/bash', persistFullOutput: true });
    fake.stdout.write('small output\n');
    fake.emit('close', 0, null);
    const result = await p;
    assert.equal(result.fullOutputPath, undefined, 'no path for small, complete output');
    assert.equal(result.fullOutputSize, undefined);
    // A file may have been opened lazily, but must be removed on finish.
    if (openedPath) assert.equal(fs.existsSync(openedPath), false, 'the temp file is discarded');
  });

  it('is off by default: no persistence without the opt-in', async () => {
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    const openSpy = mock.method(shellHooks, 'openSync', shellHooks.openSync);
    const fake = new FakeProc();
    spawnCapture(fake);
    const p = execCommand('bigcmd', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellPath: '/bin/bash' });
    fake.stdout.write('x'.repeat(1_000_100));
    fake.emit('close', 0, null);
    const result = await p;
    assert.equal(result.fullOutputPath, undefined);
    assert.equal(openSpy.mock.callCount(), 0, 'no temp file opened when persistFullOutput is off');
  });

  it('caps the persisted file at MAX_PERSISTED_OUTPUT_BYTES and never fails the command on a write error', async () => {
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    const fake = new FakeProc();
    spawnCapture(fake);
    // Force the write to throw — persistence is best-effort and must not reject.
    mock.method(shellHooks, 'writeSync', (() => { throw new Error('ENOSPC'); }) as typeof shellHooks.writeSync);
    const p = execCommand('bigcmd', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellPath: '/bin/bash', persistFullOutput: true });
    fake.stdout.write('x'.repeat(1_000_100));
    fake.emit('close', 0, null);
    const result = await p;
    assert.equal(result.exitCode, 0, 'command still resolves normally despite the persist failure');
    assert.equal(result.fullOutputPath, undefined, 'no path surfaced when persistence failed');
  });
});
