import { describe, it, mock, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import { EventEmitter } from 'events';
import { PassThrough } from 'stream';

import {
  execCommand,
  shellHooks,
  resolveTmpBase,
  SLOW_EXEC_THRESHOLD_ENV,
} from '../src/utils/shell';

// Deterministic fake child process: the test drives its stdout/stderr and emits
// 'close' manually to settle execCommand without spawning a real subprocess.
class FakeProc extends EventEmitter {
  stdout = new PassThrough();
  stderr = new PassThrough();
  stdin = new PassThrough();
  pid: number | undefined = 5151;
  kill(): boolean { return true; }
}

afterEach(() => {
  mock.restoreAll();
  delete process.env.AMI_SLOW_EXEC_MS;
  delete process.env.AMI_TMPDIR;
  delete process.env.CLAUDE_CODE_TMPDIR;
});

// ---------------------------------------------------------------------------
// resolveTmpBase — AMI_TMPDIR / CLAUDE_CODE_TMPDIR override
// ---------------------------------------------------------------------------

describe('resolveTmpBase – temp-dir override knob', () => {
  it('falls back to os.tmpdir() when no override is set', () => {
    delete process.env.AMI_TMPDIR;
    delete process.env.CLAUDE_CODE_TMPDIR;
    assert.equal(resolveTmpBase(), os.tmpdir());
  });

  it('honors AMI_TMPDIR', () => {
    process.env.AMI_TMPDIR = '/custom/scratch';
    assert.equal(resolveTmpBase(), '/custom/scratch');
  });

  it('accepts CLAUDE_CODE_TMPDIR as a fallback for parity', () => {
    delete process.env.AMI_TMPDIR;
    process.env.CLAUDE_CODE_TMPDIR = '/cc/tmp';
    assert.equal(resolveTmpBase(), '/cc/tmp');
  });

  it('prefers AMI_TMPDIR over CLAUDE_CODE_TMPDIR', () => {
    process.env.AMI_TMPDIR = '/ami/tmp';
    process.env.CLAUDE_CODE_TMPDIR = '/cc/tmp';
    assert.equal(resolveTmpBase(), '/ami/tmp');
  });

  it('treats a blank/whitespace override as unset', () => {
    process.env.AMI_TMPDIR = '   ';
    assert.equal(resolveTmpBase(), os.tmpdir());
  });
});

// ---------------------------------------------------------------------------
// Slow-command diagnostics — AMI_SLOW_EXEC_MS
// ---------------------------------------------------------------------------

describe('execCommand – slow-command diagnostics', () => {
  it('env constant names the AMI knob', () => {
    assert.equal(SLOW_EXEC_THRESHOLD_ENV, 'AMI_SLOW_EXEC_MS');
  });

  it('reports a slow command through onSlowCommand when the threshold is met', async () => {
    process.env.AMI_SLOW_EXEC_MS = '0'; // threshold 0 → every command qualifies
    const calls: { command: string; durationMs: number; thresholdMs: number }[] = [];
    mock.method(shellHooks, 'onSlowCommand', (info: typeof calls[number]) => { calls.push(info); });
    const result = await execCommand('echo slow', { cwd: process.cwd(), stallTimeoutMs: 0 });
    assert.equal(result.exitCode, 0);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].command, 'echo slow');
    assert.equal(calls[0].thresholdMs, 0);
    assert.ok(calls[0].durationMs >= 0);
  });

  it('does NOT report when the knob is unset (zero default noise)', async () => {
    delete process.env.AMI_SLOW_EXEC_MS;
    let fired = false;
    mock.method(shellHooks, 'onSlowCommand', () => { fired = true; });
    await execCommand('echo quiet', { cwd: process.cwd(), stallTimeoutMs: 0 });
    assert.equal(fired, false);
  });

  it('ignores a non-numeric or negative threshold', async () => {
    for (const bad of ['abc', '-5', '  ']) {
      process.env.AMI_SLOW_EXEC_MS = bad;
      let fired = false;
      mock.method(shellHooks, 'onSlowCommand', () => { fired = true; });
      await execCommand('echo x', { cwd: process.cwd(), stallTimeoutMs: 0 });
      assert.equal(fired, false, `threshold "${bad}" should disable diagnostics`);
      mock.restoreAll();
    }
  });

  it('does NOT report a command that finishes under the threshold', async () => {
    process.env.AMI_SLOW_EXEC_MS = '600000'; // 10 min — a quick echo never reaches it
    let fired = false;
    mock.method(shellHooks, 'onSlowCommand', () => { fired = true; });
    await execCommand('echo fast', { cwd: process.cwd(), stallTimeoutMs: 0 });
    assert.equal(fired, false);
  });
});

// ---------------------------------------------------------------------------
// stdin mode option
// ---------------------------------------------------------------------------

describe('execCommand – stdin mode option', () => {
  function captureStdio(): { getStdio: () => unknown; fake: FakeProc } {
    const fake = new FakeProc();
    let captured: unknown;
    mock.method(shellHooks, 'spawn', (_cmd: string, _args: string[], opts: { stdio: unknown }) => {
      captured = opts.stdio;
      return fake as unknown as ReturnType<typeof shellHooks.spawn>;
    });
    return { getStdio: () => captured, fake };
  }

  it("defaults stdin to 'ignore'", async () => {
    const { getStdio, fake } = captureStdio();
    const p = execCommand('echo hi', { cwd: os.tmpdir(), stallTimeoutMs: 0 });
    fake.emit('close', 0, null);
    await p;
    assert.deepEqual(getStdio(), ['ignore', 'pipe', 'pipe']);
  });

  it("honors stdin: 'inherit'", async () => {
    const { getStdio, fake } = captureStdio();
    const p = execCommand('echo hi', { cwd: os.tmpdir(), stallTimeoutMs: 0, stdin: 'inherit' });
    fake.emit('close', 0, null);
    await p;
    assert.deepEqual(getStdio(), ['inherit', 'pipe', 'pipe']);
  });

  it('input payload still forces a piped stdin regardless of stdin mode', async () => {
    const { getStdio, fake } = captureStdio();
    const p = execCommand('cat', { cwd: os.tmpdir(), stallTimeoutMs: 0, stdin: 'inherit', input: 'payload' });
    fake.emit('close', 0, null);
    await p;
    assert.deepEqual(getStdio(), ['pipe', 'pipe', 'pipe']);
  });
});

// ---------------------------------------------------------------------------
// preserveOutputOnError option
// ---------------------------------------------------------------------------

describe('execCommand – preserveOutputOnError', () => {
  it('discards output on a non-zero exit when false', async () => {
    const result = await execCommand('echo out; echo err >&2; exit 3', {
      cwd: process.cwd(),
      stallTimeoutMs: 0,
      preserveOutputOnError: false,
    });
    assert.equal(result.exitCode, 3);
    assert.equal(result.stdout, '');
    assert.equal(result.stderr, '');
  });

  it('keeps output on a non-zero exit by default', async () => {
    const result = await execCommand('echo out; exit 3', { cwd: process.cwd(), stallTimeoutMs: 0 });
    assert.equal(result.exitCode, 3);
    assert.ok(result.stdout.includes('out'));
  });

  it('keeps output on a zero exit even when preserveOutputOnError is false', async () => {
    const result = await execCommand('echo kept', {
      cwd: process.cwd(),
      stallTimeoutMs: 0,
      preserveOutputOnError: false,
    });
    assert.equal(result.exitCode, 0);
    assert.ok(result.stdout.includes('kept'));
  });

  it('keeps output on a timeout (exitCode null) even when false', async () => {
    const result = await execCommand('echo partial; sleep 60', {
      cwd: process.cwd(),
      timeout: 250,
      stallTimeoutMs: 0,
      preserveOutputOnError: false,
    });
    assert.equal(result.exitCode, null);
    assert.ok(result.stdout.includes('partial'));
  });
});
