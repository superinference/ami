import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  ProcessManager,
  osHooks,
  fsHooks,
  pmLogHooks,
  signalToExitCode,
  resolveExitCode,
  classifyExitStatus,
  OUTPUT_FLUSH_MS,
} from '../src/process-manager';

// ---------------------------------------------------------------------------
// Wave-3 parity for process-manager.ts:
//   - 'exit' (not 'close') so a backgrounded grandchild can't wedge a task
//   - signal-aware exit classification + 128+signo synthetic exit codes
//   - endTime frozen elapsed duration
//   - O_NOFOLLOW/O_EXCL symlink-safe task-output open
//   - retry-once + logging on transient output-write failures
// Driven through the real exported surface + the osHooks/fsHooks/pmLogHooks
// DI seams; temp dirs from fs.mkdtempSync; no source-text or namespace hacks.
// ---------------------------------------------------------------------------

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-secexit-'));
}

function waitForComplete(pm: ProcessManager, taskId: string, timeoutMs = 8000): Promise<any> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Timed out waiting for ${taskId}`)), timeoutMs);
    pm.on('complete', function handler(event: any) {
      if (event.taskId === taskId) {
        clearTimeout(timer);
        pm.off('complete', handler);
        resolve(event);
      }
    });
  });
}

async function cleanupPm(pm: ProcessManager | null): Promise<void> {
  if (!pm) return;
  pm.cleanup();
  await new Promise(r => setTimeout(r, 120));
}

// ===========================================================================
// Pure helpers: signal classification + synthetic exit codes
// ===========================================================================
describe('process-manager — signal-aware exit classification (pure helpers)', () => {
  it('signalToExitCode maps signals to 128 + signo, unknown -> 128', () => {
    assert.equal(signalToExitCode('SIGTERM'), 128 + (os.constants.signals.SIGTERM as number));
    assert.equal(signalToExitCode('SIGKILL'), 128 + (os.constants.signals.SIGKILL as number));
    assert.equal(signalToExitCode('SIGINT'), 128 + (os.constants.signals.SIGINT as number));
    // An unrecognised signal name has no number -> fall back to 128 (the base).
    assert.equal(signalToExitCode('NOPE' as any), 128);
  });

  it('resolveExitCode prefers a real code, else synthesises from the signal, else null', () => {
    assert.equal(resolveExitCode(0, null), 0);
    assert.equal(resolveExitCode(42, null), 42);
    assert.equal(resolveExitCode(42, 'SIGTERM'), 42); // real code still wins
    assert.equal(resolveExitCode(null, 'SIGTERM'), signalToExitCode('SIGTERM'));
    assert.equal(resolveExitCode(null, null), null);
  });

  it('classifyExitStatus distinguishes interruption from crash from success', () => {
    assert.equal(classifyExitStatus(0, null), 'completed');
    assert.equal(classifyExitStatus(1, null), 'failed');
    assert.equal(classifyExitStatus(null, null), 'failed');
    assert.equal(classifyExitStatus(null, 'SIGTERM'), 'killed');
    assert.equal(classifyExitStatus(null, 'SIGINT'), 'killed');
    // A non-fatal signal that isn't an interruption is still a crash.
    assert.equal(classifyExitStatus(null, 'SIGSEGV'), 'failed');
  });
});

// ===========================================================================
// Integration: external SIGTERM is recorded as 'killed' with a 143 synthetic
// code, NOT 'failed' with a null code (the old (code)-only handler behaviour).
// ===========================================================================
describe('process-manager — external signal termination', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('an EXTERNAL SIGTERM yields status=killed and exitCode=143', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('sleep 60', { cwd: tmpDir });
    await new Promise(r => setTimeout(r, 150));
    const pid = pm.get(id)!.pid;
    // Signal the shell directly WITHOUT going through pm.kill(), so status is not
    // pre-flagged 'killed' — the close/exit handler must classify it from signal.
    try { process.kill(pid, 'SIGTERM'); } catch { /* already gone */ }
    const event = await waitForComplete(pm, id);
    const task = pm.get(id)!;
    assert.equal(task.status, 'killed', 'external SIGTERM must read as killed, not failed');
    assert.equal(task.exitCode, 128 + (os.constants.signals.SIGTERM as number));
    assert.equal(event.exitCode, task.exitCode, 'completion event carries the synthetic code');
  });
});

// ===========================================================================
// 'exit' not 'close': a backgrounded grandchild holding the stdio fds must not
// keep a finished task 'running'. With 'close' this would block ~grandchild
// lifetime; with 'exit' + OUTPUT_FLUSH_MS it finalizes promptly.
// ===========================================================================
describe('process-manager — exit-not-close grandchild escape', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  let leakedPid: number | undefined;
  afterEach(async () => {
    await cleanupPm(pm);
    if (leakedPid !== undefined) { try { process.kill(-leakedPid, 'SIGKILL'); } catch { /* gone */ } }
    leakedPid = undefined;
    if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('finalizes promptly even when a grandchild inherits the stdio fds', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    // The shell backgrounds `sleep 8` (inherits stdout/stderr) then exits. Under
    // 'close' the task would stay running ~8s; under 'exit' it finalizes in ~ms.
    const started = Date.now();
    const id = pm.spawn('sleep 8 & echo parent-done', { cwd: tmpDir });
    leakedPid = pm.get(id)!.pid;
    const event = await waitForComplete(pm, id, 6000);
    const elapsed = Date.now() - started;
    assert.ok(elapsed < 3000, `task should finalize well before the grandchild exits (took ${elapsed}ms)`);
    assert.equal(event.exitCode, 0);
    assert.equal(pm.get(id)!.status, 'completed');
    // The parent's own output was still captured despite the early finalize.
    assert.ok((pm.getOutput(id)?.output ?? '').includes('parent-done'));
  });
});

// ===========================================================================
// endTime: a terminal task's elapsed duration freezes instead of growing.
// ===========================================================================
describe('process-manager — frozen elapsed (endTime)', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('records endTime and freezes elapsedMs after completion', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo hi', { cwd: tmpDir });
    await waitForComplete(pm, id);
    const task = pm.get(id)!;
    assert.equal(typeof task.endTime, 'number', 'endTime must be set on a terminal task');
    const first = pm.getOutput(id)!.elapsedMs;
    await new Promise(r => setTimeout(r, 80));
    const second = pm.getOutput(id)!.elapsedMs;
    assert.equal(first, second, 'elapsed must not grow after the task finished');
    assert.ok(task.endTime! >= task.startTime);
  });

  it('freezes elapsed for a killed in-process agent (updateEntry terminal path)', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    pm.registerAgent('agent-end', {
      command: '[agent] work', description: 'w', outputPath: path.join(tmpDir, 'a.out'), abortFn: () => {},
    });
    assert.equal(pm.get('agent-end')!.endTime, undefined);
    pm.updateEntry('agent-end', { status: 'completed' });
    const end1 = pm.get('agent-end')!.endTime;
    assert.equal(typeof end1, 'number');
    // A later non-terminal-affecting update must NOT move the frozen timestamp.
    await new Promise(r => setTimeout(r, 20));
    pm.updateEntry('agent-end', { tokenCount: 5 });
    assert.equal(pm.get('agent-end')!.endTime, end1);
  });
});

// ===========================================================================
// O_NOFOLLOW / O_EXCL: symlink-safe task-output open.
// ===========================================================================
describe('process-manager — symlink-safe task output', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('creates the output file as a regular file (O_EXCL create semantics)', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo x', { cwd: tmpDir });
    const outPath = pm.get(id)!.outputPath;
    assert.ok(fs.lstatSync(outPath).isFile());
    assert.ok(!fs.lstatSync(outPath).isSymbolicLink());
    // O_EXCL mechanism: a second exclusive create at an existing path must fail.
    assert.throws(() => fs.closeSync(fs.openSync(outPath, 'wx')), /EEXIST/);
    await waitForComplete(pm, id);
  });

  it('getOutput does NOT follow a symlink planted at the output path', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const secret = path.join(tmpDir, 'secret.txt');
    fs.writeFileSync(secret, 'TOP-SECRET-HOST-CONTENT');
    const id = pm.spawn('sleep 30', { cwd: tmpDir });
    await new Promise(r => setTimeout(r, 120));
    const outPath = pm.get(id)!.outputPath;
    // Attacker swaps the output file for a symlink to a host file.
    fs.rmSync(outPath, { force: true });
    fs.symlinkSync(secret, outPath);
    const result = pm.getOutput(id);
    assert.ok(result, 'getOutput still returns a record');
    assert.ok(!result!.output.includes('TOP-SECRET'), 'O_NOFOLLOW must refuse to read through the symlink');
  });

  it('getOutputTail does NOT follow a symlink planted at the output path', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const secret = path.join(tmpDir, 'secret2.txt');
    fs.writeFileSync(secret, 'TOP-SECRET-TAIL');
    const id = pm.spawn('sleep 30', { cwd: tmpDir });
    await new Promise(r => setTimeout(r, 120));
    const outPath = pm.get(id)!.outputPath;
    fs.rmSync(outPath, { force: true });
    fs.symlinkSync(secret, outPath);
    const tail = pm.getOutputTail(id, 0);
    assert.ok(tail, 'getOutputTail returns a record (error-tolerant)');
    assert.ok(!tail!.output.includes('TOP-SECRET'), 'O_NOFOLLOW must refuse the tail read through the symlink');
  });

  it('getOutput falls back to the "r" flag under a reported win32 platform', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo win-read', { cwd: tmpDir });
    await waitForComplete(pm, id);
    const real = osHooks.platform;
    try {
      osHooks.platform = () => 'win32';
      const out = pm.getOutput(id);
      assert.ok(out!.output.includes('win-read'));
    } finally {
      osHooks.platform = real;
    }
  });
});

// ===========================================================================
// Transient write-error retry-once + logging (fsHooks / pmLogHooks seams).
// ===========================================================================
describe('process-manager — transient output-write retry', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); });

  function withFd(dir: string, fn: (fd: number) => void): void {
    const fd = fs.openSync(path.join(dir, 'w.out'), 'w');
    try { fn(fd); } finally { try { fs.closeSync(fd); } catch { /* ignore */ } }
  }

  it('retries a transient EMFILE write once and logs it', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const realWrite = fsHooks.writeSync;
    const realLog = pmLogHooks.onWriteError;
    const logs: Array<{ code?: string; willRetry: boolean }> = [];
    let calls = 0;
    try {
      fsHooks.writeSync = (fd, buf) => {
        calls++;
        if (calls === 1) { const e: NodeJS.ErrnoException = new Error('EMFILE'); e.code = 'EMFILE'; throw e; }
        realWrite(fd, buf);
      };
      pmLogHooks.onWriteError = (e, willRetry) => logs.push({ code: e.code, willRetry });
      withFd(tmpDir, (fd) => {
        const ok = (pm as any).safeWrite(fd, Buffer.from('payload'));
        assert.equal(ok, true, 'the retry must succeed');
      });
      assert.equal(calls, 2, 'exactly one retry');
      assert.deepEqual(logs, [{ code: 'EMFILE', willRetry: true }]);
    } finally {
      fsHooks.writeSync = realWrite;
      pmLogHooks.onWriteError = realLog;
    }
  });

  it('gives up (and logs twice) when the retry also fails', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const realWrite = fsHooks.writeSync;
    const realLog = pmLogHooks.onWriteError;
    const logs: boolean[] = [];
    try {
      fsHooks.writeSync = () => { const e: NodeJS.ErrnoException = new Error('EPERM'); e.code = 'EPERM'; throw e; };
      pmLogHooks.onWriteError = (_e, willRetry) => logs.push(willRetry);
      withFd(tmpDir, (fd) => {
        const ok = (pm as any).safeWrite(fd, Buffer.from('x'));
        assert.equal(ok, false, 'a persistently failing write is dropped');
      });
      assert.deepEqual(logs, [true, false], 'logs the pre-retry then the give-up');
    } finally {
      fsHooks.writeSync = realWrite;
      pmLogHooks.onWriteError = realLog;
    }
  });

  it('treats a closed fd (EBADF) as fatal — no retry, no log', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const realWrite = fsHooks.writeSync;
    const realLog = pmLogHooks.onWriteError;
    let logged = 0;
    let calls = 0;
    try {
      fsHooks.writeSync = () => { calls++; const e: NodeJS.ErrnoException = new Error('EBADF'); e.code = 'EBADF'; throw e; };
      pmLogHooks.onWriteError = () => { logged++; };
      withFd(tmpDir, (fd) => {
        const ok = (pm as any).safeWrite(fd, Buffer.from('x'));
        assert.equal(ok, false);
      });
      assert.equal(calls, 1, 'no retry on a fatal closed fd');
      assert.equal(logged, 0, 'EBADF is not logged as a transient error');
    } finally {
      fsHooks.writeSync = realWrite;
      pmLogHooks.onWriteError = realLog;
    }
  });
});

describe('process-manager — constants', () => {
  it('exposes a positive output flush window', () => {
    assert.ok(OUTPUT_FLUSH_MS > 0);
  });
});
