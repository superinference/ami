import { describe, it, mock, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import { EventEmitter, once } from 'events';
import { PassThrough } from 'stream';

import { execCommand, shellHooks } from '../src/utils/shell';

// Deterministic fake child process: its stdout/stderr are PassThroughs the test
// drives, and `close` is emitted manually to settle execCommand.
class FakeProc extends EventEmitter {
  stdout = new PassThrough();
  stderr = new PassThrough();
  stdin = new PassThrough();
  pid: number | undefined = 4242;
  kill(): boolean { return true; }
}

afterEach(() => {
  mock.restoreAll();
});

// ---------------------------------------------------------------------------
// windowsHide spawn flag (U049 — parity with the reference Shell.ts:336)
// ---------------------------------------------------------------------------

describe('execCommand – windowsHide spawn flag', () => {
  it('passes windowsHide: true to spawn so Windows never flashes a console window', async () => {
    const fake = new FakeProc();
    let capturedOpts: Record<string, unknown> | undefined;
    mock.method(shellHooks, 'spawn', (_cmd: string, _args: string[], opts: Record<string, unknown>) => {
      capturedOpts = opts;
      return fake as unknown as ReturnType<typeof shellHooks.spawn>;
    });
    const p = execCommand('echo hi', { cwd: os.tmpdir(), stallTimeoutMs: 0 });
    fake.emit('close', 0, null);
    await p;
    assert.ok(capturedOpts, 'spawn should have been called');
    assert.equal(capturedOpts!.windowsHide, true);
  });
});

// ---------------------------------------------------------------------------
// Enriched truncation message reporting total size + kept bounds (U049)
// ---------------------------------------------------------------------------

describe('execCommand – truncation message reports total size', () => {
  it('reports kept head+tail and the true total char count when output is truncated', async () => {
    const fake = new FakeProc();
    mock.method(shellHooks, 'spawn', () => fake as unknown as ReturnType<typeof shellHooks.spawn>);
    const p = execCommand('cat bigfile', { cwd: os.tmpdir(), stallTimeoutMs: 0 });
    // Emit more than HEAD_CHARS (670k) + TAIL_CHARS (330k): 100 chars must drop.
    const big = 'x'.repeat(670_000 + 330_000 + 100);
    fake.stdout.end(big);
    await once(fake.stdout, 'end'); // all chunks consumed by execCommand's reader
    fake.emit('close', 0, null);
    const r = await p;
    assert.match(r.stdout, /kept first 670000 \+ last 330000 of 1000100 chars; 100 truncated/);
  });

  it('does not add a truncation note when output fits within the bound', async () => {
    const fake = new FakeProc();
    mock.method(shellHooks, 'spawn', () => fake as unknown as ReturnType<typeof shellHooks.spawn>);
    const p = execCommand('echo small', { cwd: os.tmpdir(), stallTimeoutMs: 0 });
    fake.stdout.end('small-output');
    await once(fake.stdout, 'end');
    fake.emit('close', 0, null);
    const r = await p;
    assert.equal(r.stdout, 'small-output');
    assert.ok(!r.stdout.includes('truncated'));
  });
});

const tick = (): Promise<void> => new Promise((res) => setImmediate(res));

// ---------------------------------------------------------------------------
// Resolve on the shell's own 'exit', not only 'close' (U049 — parity with
// Claude ShellCommand.ts L269-272). A detached grandchild that inherits the
// stdio pipes keeps 'close' from firing; AMI must still return when the shell
// itself exits instead of blocking for the full timeout.
// ---------------------------------------------------------------------------

describe('execCommand – resolves on shell exit when close is blocked', () => {
  it('returns the real exit code when only "exit" fires (grandchild holds the pipes open)', async () => {
    const fake = new FakeProc();
    mock.method(shellHooks, 'spawn', () => fake as unknown as ReturnType<typeof shellHooks.spawn>);
    // Generous timeout so a pre-fix 'close'-only wait would (if at all) resolve
    // via timeout with exitCode null — discriminating the fix by VALUE (0 vs
    // null), not merely by a test-harness hang.
    const p = execCommand('echo done; sleep 300 &', { cwd: os.tmpdir(), timeout: 5000, stallTimeoutMs: 0 });
    fake.stdout.write('done\n');
    await tick(); // let execCommand's reader consume the chunk
    fake.emit('exit', 0, null); // shell exits; NO 'close' — grandchild holds the fds
    const r = await p;
    assert.equal(r.exitCode, 0, 'completes with the shell exit code, not a timeout null');
    assert.ok(r.stdout.includes('done'), r.stdout);
  });

  it('surfaces a signal note when the shell exits via signal with close blocked', async () => {
    const fake = new FakeProc();
    mock.method(shellHooks, 'spawn', () => fake as unknown as ReturnType<typeof shellHooks.spawn>);
    const p = execCommand('x', { cwd: os.tmpdir(), timeout: 5000, stallTimeoutMs: 0 });
    fake.emit('exit', null, 'SIGSEGV');
    const r = await p;
    assert.equal(r.signal, 'SIGSEGV');
    assert.ok(r.stderr.includes('terminated by signal SIGSEGV'), r.stderr);
    assert.ok(r.stderr.includes('segmentation fault'), r.stderr);
  });

  it('prefers close output: close wins the race so exit never truncates', async () => {
    const fake = new FakeProc();
    mock.method(shellHooks, 'spawn', () => fake as unknown as ReturnType<typeof shellHooks.spawn>);
    const p = execCommand('echo hi', { cwd: os.tmpdir(), stallTimeoutMs: 0 });
    fake.stdout.write('hi\n');
    await tick();
    fake.emit('exit', 0, null);
    fake.emit('close', 0, null); // close arrives right after exit (normal case)
    const r = await p;
    assert.equal(r.exitCode, 0);
    assert.ok(r.stdout.includes('hi'), r.stdout);
  });
});

// ---------------------------------------------------------------------------
// Full POSIX process-tree kill (U049 — parity with the reference's treeKill,
// ShellCommand.ts #doKill). Kill every descendant pid, not just the process
// group, so a child that escaped the group via setsid() is still terminated —
// while never touching an unrelated pid.
// ---------------------------------------------------------------------------

describe('killTree – POSIX full process-tree kill on abort', () => {
  it('kills every descendant pid in the tree (not just the group), sparing unrelated pids', async () => {
    const fake = new FakeProc();
    fake.pid = 1000;
    mock.method(shellHooks, 'platform', () => 'linux' as NodeJS.Platform);
    mock.method(shellHooks, 'spawn', () => fake as unknown as ReturnType<typeof shellHooks.spawn>);
    // Tree: 1000 -> 1001 -> {1002, 1003}; plus an unrelated 9999 under init.
    mock.method(
      shellHooks,
      'execSync',
      (() => '  1000     1\n  1001  1000\n  1002  1001\n  1003  1001\n  9999     1\n') as typeof shellHooks.execSync,
    );
    const killed: number[] = [];
    mock.method(shellHooks, 'kill', ((pid: number) => { killed.push(pid); }) as typeof shellHooks.kill);

    const ac = new AbortController();
    const p = execCommand('run-tree', { cwd: os.tmpdir(), stallTimeoutMs: 0, abortSignal: ac.signal });
    ac.abort();
    await p;

    assert.ok(killed.includes(1000), 'root killed');
    assert.ok(killed.includes(1001), 'child killed');
    assert.ok(killed.includes(1002), 'grandchild 1002 killed');
    assert.ok(killed.includes(1003), 'grandchild 1003 killed');
    assert.ok(killed.includes(-1000), 'process group swept');
    assert.ok(!killed.includes(9999), 'unrelated pid untouched');
  });

  it('falls back to the process group when the process table cannot be read', async () => {
    const fake = new FakeProc();
    fake.pid = 2000;
    mock.method(shellHooks, 'platform', () => 'linux' as NodeJS.Platform);
    mock.method(shellHooks, 'spawn', () => fake as unknown as ReturnType<typeof shellHooks.spawn>);
    mock.method(shellHooks, 'execSync', (() => { throw new Error('ps unavailable'); }) as typeof shellHooks.execSync);
    const killed: number[] = [];
    mock.method(shellHooks, 'kill', ((pid: number) => { killed.push(pid); }) as typeof shellHooks.kill);

    const ac = new AbortController();
    const p = execCommand('run', { cwd: os.tmpdir(), stallTimeoutMs: 0, abortSignal: ac.signal });
    ac.abort();
    await p;

    // Root (from the [rootPid] fallback) and the group sweep are both signalled.
    assert.ok(killed.includes(2000), 'root killed via fallback');
    assert.ok(killed.includes(-2000), 'process group swept');
  });
});
