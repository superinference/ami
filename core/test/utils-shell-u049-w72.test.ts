/**
 * U049 (w72) — additive execCommand capabilities for core/core/src/utils/shell.ts.
 *
 * Parity gaps covered (all verified=confirmed):
 *  - Structured `onProgress` callback with positional args
 *    (lastLines, allLines, totalLines, totalBytes, isIncomplete), distinct from
 *    the raw-chunk `onData` pipe (Shell.ts:L161-169).
 *  - Total-line-count extrapolation from a bounded tail sample with a monotone
 *    counter, exact when the whole output fits the tail (TaskOutput.ts:L126-149).
 *  - Progress heartbeat tick fires EVEN with an empty tail so the loop wakes on a
 *    silent long-running command (TaskOutput.ts:L119-125).
 *  - `preventCwdChanges` opt-in: capture the command's final `pwd -P` into
 *    result.cwd so a `cd` can persist across invocations (Shell.ts:L170,L385-414;
 *    shellProvider.ts per-command pwd tracking).
 *  - Interrupt-aware abort: a SOFT `'interrupt'` abort does NOT kill the process
 *    (result.backgrounded=true, partial output preserved); any other abort reason
 *    is a hard kill (ShellCommand.ts:L186-193).
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
  computeProgressSlices,
  findSuitableShell,
  SHELL_SNAPSHOT_ENV,
} from '../src/utils/shell';

// Deterministic fake child process (drives stdout/stderr, emits 'close'/'exit').
class FakeProc extends EventEmitter {
  stdout = new PassThrough();
  stderr = new PassThrough();
  stdin = new PassThrough();
  pid: number | undefined = 8168;
  kill(): boolean { return true; }
}

const savedEnv: Record<string, string | undefined> = {};
beforeEach(() => {
  for (const k of [SHELL_SNAPSHOT_ENV, 'AMI_LOGIN_SHELL']) { savedEnv[k] = process.env[k]; delete process.env[k]; }
});
afterEach(() => {
  mock.restoreAll();
  for (const k of [SHELL_SNAPSHOT_ENV, 'AMI_LOGIN_SHELL']) {
    if (savedEnv[k] === undefined) delete process.env[k]; else process.env[k] = savedEnv[k];
  }
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
// computeProgressSlices — pure newline-walk (matches the reference's TaskOutput loop)
// ---------------------------------------------------------------------------
describe('U049 computeProgressSlices', () => {
  it('returns empty slices and zero lines for empty input', () => {
    assert.deepEqual(computeProgressSlices(''), { last5: '', last100: '', totalLines: 0 });
  });

  it('counts the final no-newline line and returns the whole text as last5 for <5 lines', () => {
    // "a\nb\nc\n": 3 "x\n" plus a trailing empty line → 4 counted lines; never hits
    // 5, so last5/last100 are the whole string (n5/n100 stay 0).
    const r = computeProgressSlices('a\nb\nc\n');
    assert.equal(r.totalLines, 4);
    assert.equal(r.last5, 'a\nb\nc\n');
    assert.equal(r.last100, 'a\nb\nc\n');
  });

  it('slices exactly the last 5 lines once more than 5 exist', () => {
    const text = Array.from({ length: 8 }, (_, i) => `line${i}`).join('\n'); // 8 lines, no trailing \n
    const r = computeProgressSlices(text);
    assert.equal(r.totalLines, 8);
    // last 5 lines are line3..line7
    assert.equal(r.last5, 'line3\nline4\nline5\nline6\nline7');
  });

  it('slices the last 100 lines and counts all for >100 lines', () => {
    const text = Array.from({ length: 150 }, (_, i) => `L${i}`).join('\n'); // 150 lines
    const r = computeProgressSlices(text);
    assert.equal(r.totalLines, 150);
    assert.equal(r.last100.split('\n').length, 100, 'last100 holds exactly 100 lines');
    assert.equal(r.last100.split('\n')[0], 'L50', 'last100 starts at line 50');
  });
});

// ---------------------------------------------------------------------------
// execCommand onProgress — structured args + extrapolation + silent heartbeat
// ---------------------------------------------------------------------------
describe('U049 execCommand onProgress', () => {
  it('delivers the last-5/last-100 tail, exact total lines, total bytes, isIncomplete=false', async () => {
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    const fake = new FakeProc();
    spawnCapture(fake);
    const calls: Array<[string, string, number, number, boolean]> = [];
    let resolveData!: () => void;
    const gotData = new Promise<void>((r) => { resolveData = r; });
    const onProgress = (l5: string, l100: string, tl: number, tb: number, inc: boolean): void => {
      calls.push([l5, l100, tl, tb, inc]);
      if (l5) resolveData();
    };
    const p = execCommand('x', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellPath: '/bin/bash', onProgress, progressIntervalMs: 10 });
    fake.stdout.write('a\nb\nc\n'); // 6 bytes, 4 counted lines
    await gotData;
    fake.emit('close', 0, null);
    await p;
    const dataCall = calls.find(c => c[0] !== '');
    assert.ok(dataCall, 'at least one progress tick carried output');
    assert.equal(dataCall![0], 'a\nb\nc\n', 'lastLines = last-5 slice');
    assert.equal(dataCall![2], 4, 'exact total line count');
    assert.equal(dataCall![3], 6, 'total bytes');
    assert.equal(dataCall![4], false, 'isIncomplete=false when the whole output fits the tail');
  });

  it('extrapolates the total line count from the tail and sets isIncomplete when output exceeds the retained tail', async () => {
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    const fake = new FakeProc();
    spawnCapture(fake);
    const calls: Array<[string, string, number, number, boolean]> = [];
    let resolveData!: () => void;
    const gotData = new Promise<void>((r) => { resolveData = r; });
    const onProgress = (l5: string, l100: string, tl: number, tb: number, inc: boolean): void => {
      calls.push([l5, l100, tl, tb, inc]);
      if (l5) resolveData();
    };
    const big = 'x\n'.repeat(80_000); // 160000 bytes, 80000 "x\n" lines — exceeds the 64KB tail
    const p = execCommand('x', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellPath: '/bin/bash', onProgress, progressIntervalMs: 10 });
    fake.stdout.write(big);
    await gotData;
    fake.emit('close', 0, null);
    await p;
    const dataCall = calls.find(c => c[0] !== '')!;
    assert.equal(dataCall[3], 160_000, 'total bytes are exact (full output)');
    assert.equal(dataCall[4], true, 'isIncomplete=true when the tail omits earlier output');
    assert.ok(dataCall[2] > 32_769, `extrapolated total (${dataCall[2]}) exceeds the tail sample count`);
  });

  it('fires a heartbeat tick with empty slices on a silent command so the loop wakes', async () => {
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    const fake = new FakeProc();
    spawnCapture(fake);
    let resolveTick!: () => void;
    const gotTick = new Promise<void>((r) => { resolveTick = r; });
    const ticks: Array<[string, string, number, number, boolean]> = [];
    const onProgress = (l5: string, l100: string, tl: number, tb: number, inc: boolean): void => {
      ticks.push([l5, l100, tl, tb, inc]);
      resolveTick();
    };
    const p = execCommand('sleep', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellPath: '/bin/bash', onProgress, progressIntervalMs: 10 });
    // No output emitted at all.
    await gotTick;
    assert.deepEqual(ticks[0], ['', '', 0, 0, false], 'silent tick fires with empty tail');
    fake.emit('close', 0, null);
    await p;
  });

  it('does not arm a heartbeat when progressIntervalMs <= 0', async () => {
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    const fake = new FakeProc();
    spawnCapture(fake);
    let fired = false;
    const p = execCommand('x', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellPath: '/bin/bash', onProgress: () => { fired = true; }, progressIntervalMs: 0 });
    fake.stdout.write('data\n');
    fake.emit('close', 0, null);
    await p;
    assert.equal(fired, false, 'no progress callbacks when the heartbeat is disabled');
  });
});

// ---------------------------------------------------------------------------
// execCommand preventCwdChanges — cwd capture wiring + default unchanged
// ---------------------------------------------------------------------------
describe('U049 execCommand preventCwdChanges (cwd tracking)', () => {
  it('does NOT append pwd tracking by default (behavior unchanged)', async () => {
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    const fake = new FakeProc();
    const cap = spawnCapture(fake);
    const p = execCommand('echo hi', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellPath: '/bin/bash' });
    fake.emit('close', 0, null);
    await p;
    assert.deepEqual(cap.args, ['-c', 'echo hi'], 'no cwd-capture wrapper without opt-in');
  });

  it('appends an exit-code-preserving pwd -P capture when preventCwdChanges:false', async () => {
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'readFileSync', (() => '/captured/dir\n') as typeof shellHooks.readFileSync);
    const unlinkSpy = mock.method(shellHooks, 'unlinkSync', (() => undefined) as typeof shellHooks.unlinkSync);
    const fake = new FakeProc();
    const cap = spawnCapture(fake);
    const p = execCommand('echo hi', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellPath: '/bin/bash', preventCwdChanges: false });
    fake.emit('close', 0, null);
    const res = await p;
    const cmdArg = cap.args![cap.args!.length - 1]!;
    assert.match(cmdArg, /^echo hi\n__ami_ec=\$\?\ncommand pwd -P > '.*ami-cwd.*' 2>\/dev\/null\nexit \$__ami_ec$/);
    assert.equal(res.cwd, '/captured/dir', 'final cwd read back from the temp file');
    assert.equal(unlinkSpy.mock.callCount(), 1, 'temp cwd file is cleaned up');
  });

  it('leaves cwd unset when the capture file was never written', async () => {
    // cwd (effectiveCwd) exists, but the random ami-cwd temp file does not.
    mock.method(shellHooks, 'existsSync', ((pp: string) => !String(pp).includes('ami-cwd')) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'unlinkSync', (() => undefined) as typeof shellHooks.unlinkSync);
    const fake = new FakeProc();
    spawnCapture(fake);
    const p = execCommand('echo hi', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellPath: '/bin/bash', preventCwdChanges: false });
    fake.emit('close', 0, null);
    const res = await p;
    assert.equal(res.cwd, undefined);
  });

  it('e2e: real bash persists the final cwd of a `cd` and preserves the exit code', async () => {
    const shell = findSuitableShell();
    if (!shell || !shell.includes('bash')) return;
    const base = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-cwd-e2e-'));
    const sub = path.join(base, 'nested');
    fs.mkdirSync(sub);
    try {
      const ok = await execCommand(`cd ${sub} && echo moved`, { cwd: base, stallTimeoutMs: 0, shellPath: shell, preventCwdChanges: false });
      assert.equal(ok.exitCode, 0, ok.stderr);
      assert.match(ok.stdout, /moved/);
      assert.equal(fs.realpathSync(ok.cwd!), fs.realpathSync(sub), 'result.cwd is the final working directory');

      // Default (no opt-in) does not capture cwd.
      const noTrack = await execCommand(`cd ${sub} && echo moved`, { cwd: base, stallTimeoutMs: 0, shellPath: shell });
      assert.equal(noTrack.cwd, undefined);

      // The command's own non-zero exit code survives the pwd-capture wrapper.
      const failed = await execCommand('false', { cwd: base, stallTimeoutMs: 0, shellPath: shell, preventCwdChanges: false });
      assert.equal(failed.exitCode, 1, 'exit code preserved through the cwd-capture wrapper');
      assert.equal(fs.realpathSync(failed.cwd!), fs.realpathSync(base));
    } finally {
      fs.rmSync(base, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// execCommand interrupt-aware abort — soft interrupt backgrounds, hard kills
// ---------------------------------------------------------------------------
describe('U049 execCommand interrupt-aware abort', () => {
  it('backgrounds (does NOT kill) on a soft "interrupt" abort, preserving partial output', async () => {
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    const killSpy = mock.method(shellHooks, 'kill', (() => undefined) as typeof shellHooks.kill);
    const fake = new FakeProc();
    spawnCapture(fake);
    const ac = new AbortController();
    const p = execCommand('long-running', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellPath: '/bin/bash', abortSignal: ac.signal });
    fake.stdout.write('partial work\n');
    await new Promise((r) => setImmediate(r)); // let the data chunk land
    ac.abort('interrupt');
    const res = await p;
    assert.equal(res.backgrounded, true, 'soft interrupt backgrounds the process');
    assert.equal(res.exitCode, null);
    assert.match(res.stdout, /partial work/, 'partial output is preserved');
    assert.equal(killSpy.mock.callCount(), 0, 'the process is NOT killed on interrupt');
  });

  it('hard-kills the process tree on any non-interrupt abort', async () => {
    mock.method(shellHooks, 'existsSync', (() => true) as typeof shellHooks.existsSync);
    mock.method(shellHooks, 'platform', (() => 'linux') as typeof shellHooks.platform);
    const killSpy = mock.method(shellHooks, 'kill', (() => undefined) as typeof shellHooks.kill);
    const fake = new FakeProc();
    spawnCapture(fake);
    const ac = new AbortController();
    const p = execCommand('long-running', { cwd: os.tmpdir(), stallTimeoutMs: 0, shellPath: '/bin/bash', abortSignal: ac.signal });
    ac.abort(); // default reason (AbortError) → hard abort
    const res = await p;
    assert.equal(res.backgrounded, undefined, 'hard abort does not background');
    assert.equal(res.exitCode, null);
    assert.ok(killSpy.mock.callCount() > 0, 'the process tree is killed on a hard abort');
  });
});
