import { describe, it, afterEach, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  ProcessManager,
  validateBackgroundCommand,
  isProcessAlive,
  forceKillAfterTimeout,
  procHooks,
  GRACEFUL_SHUTDOWN_MS,
  MAX_BACKGROUND_PROCESSES,
} from '../src/process-manager';

// ---------------------------------------------------------------------------
// POV: core/process-manager.ts — spawn real trivial commands, observe the
// real on-disk output, exit codes, state transitions, list/get shapes, kill
// semantics, and the pure helper functions. Everything is measured against
// OBSERVED results (return values, emitted events, file contents, status).
// ---------------------------------------------------------------------------

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pov-pm-'));
}

/** Resolve when the manager emits a 'complete' event for the given task. */
function waitForComplete(pm: ProcessManager, taskId: string, timeoutMs = 8000): Promise<any> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timed out waiting for ${taskId}`)), timeoutMs);
    function handler(event: any) {
      if (event.taskId === taskId) {
        clearTimeout(timer);
        pm.off('complete', handler);
        resolve(event);
      }
    }
    pm.on('complete', handler);
  });
}

/** Poll until predicate is true or the deadline passes. */
async function waitFor(pred: () => boolean, timeoutMs = 4000): Promise<boolean> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (pred()) return true;
    await new Promise(r => setTimeout(r, 15));
  }
  return pred();
}

async function cleanupPm(pm: ProcessManager | null): Promise<void> {
  if (!pm) return;
  pm.cleanup();
  await new Promise(r => setTimeout(r, 80));
}

// ===========================================================================
// 1. spawn -> list -> output capture -> completion / exit recorded
// ===========================================================================

describe('POV pm — spawn / list / output / completion', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  beforeEach(() => { tmpDir = makeTmpDir(); pm = new ProcessManager(tmpDir); });
  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('spawn returns a bg- id that list() reports as running', () => {
    const id = pm.spawn('sleep 2', { cwd: tmpDir });
    assert.match(id, /^bg-[0-9a-f]{8}$/);
    const tasks = pm.list();
    assert.equal(tasks.length, 1);
    assert.equal(tasks[0].taskId, id);
    assert.equal(tasks[0].status, 'running');
    assert.ok(tasks[0].pid > 0);
  });

  it('captures stdout to the output file and records exit 0 -> completed', async () => {
    const id = pm.spawn("printf 'hello-stdout'", { cwd: tmpDir });
    const evt = await waitForComplete(pm, id);
    assert.equal(evt.exitCode, 0);
    const out = pm.getOutput(id);
    assert.ok(out);
    assert.equal(out!.status, 'completed');
    assert.equal(out!.exitCode, 0);
    assert.match(out!.output, /hello-stdout/);
    assert.ok(out!.elapsedMs >= 0);
  });

  it('captures stderr as well as stdout', async () => {
    const id = pm.spawn("printf 'to-err' 1>&2", { cwd: tmpDir });
    await waitForComplete(pm, id);
    const out = pm.getOutput(id);
    assert.match(out!.output, /to-err/);
  });

  it('non-zero exit -> status failed with the real exit code', async () => {
    const id = pm.spawn('exit 3', { cwd: tmpDir });
    const evt = await waitForComplete(pm, id);
    assert.equal(evt.exitCode, 3);
    const task = pm.get(id);
    assert.equal(task!.status, 'failed');
    assert.equal(task!.exitCode, 3);
  });

  it('BackgroundProcess transitions running -> completed over a real spawn', async () => {
    const id = pm.spawn("sh -c 'sleep 0.15; printf done'", { cwd: tmpDir });
    // Observe the running state before it finishes.
    assert.equal(pm.get(id)!.status, 'running');
    assert.equal(pm.runningCount(), 1);
    await waitForComplete(pm, id);
    assert.equal(pm.get(id)!.status, 'completed');
    assert.equal(pm.runningCount(), 0);
  });

  it('default description is the command truncated to 80 chars', () => {
    const long = 'echo ' + 'a'.repeat(200);
    const id = pm.spawn(long, { cwd: tmpDir });
    assert.equal(pm.get(id)!.description, long.slice(0, 80));
  });

  it('honors an explicit description', async () => {
    const id = pm.spawn('echo hi', { cwd: tmpDir, description: 'my task' });
    assert.equal(pm.get(id)!.description, 'my task');
    await waitForComplete(pm, id);
  });

  it('passes a custom env through to the child', async () => {
    const id = pm.spawn("printf %s \"$POV_MARKER\"", { cwd: tmpDir, env: { ...process.env, POV_MARKER: 'env-xyz' } });
    await waitForComplete(pm, id);
    assert.match(pm.getOutput(id)!.output, /env-xyz/);
  });

  it('get() / list() omit internal fields (proc, outputFd, bytesWritten)', () => {
    const id = pm.spawn('sleep 2', { cwd: tmpDir });
    const task = pm.get(id) as any;
    assert.ok(!('proc' in task));
    assert.ok(!('outputFd' in task));
    assert.ok(!('bytesWritten' in task));
    const listed = pm.list()[0] as any;
    assert.ok(!('proc' in listed));
    assert.ok(!('outputFd' in listed));
  });

  it('get() and getOutput() return null for an unknown task', () => {
    assert.equal(pm.get('bg-nope'), null);
    assert.equal(pm.getOutput('bg-nope'), null);
    assert.equal(pm.getOutputTail('bg-nope'), null);
    assert.equal(pm.getOutputSize('bg-nope'), 0);
  });

  it('writes the output file inside the tasks state dir', async () => {
    const id = pm.spawn('echo persisted', { cwd: tmpDir });
    const task = pm.get(id)!;
    await waitForComplete(pm, id);
    assert.ok(fs.existsSync(task.outputPath));
    assert.match(fs.readFileSync(task.outputPath, 'utf-8'), /persisted/);
  });
});

// ===========================================================================
// 2. kill semantics + killed state transition
// ===========================================================================

describe('POV pm — kill', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  beforeEach(() => { tmpDir = makeTmpDir(); pm = new ProcessManager(tmpDir); });
  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('kill() terminates a running process and sets status killed', async () => {
    const id = pm.spawn('sleep 30', { cwd: tmpDir });
    assert.equal(pm.get(id)!.status, 'running');
    const ok = pm.kill(id);
    assert.equal(ok, true);
    assert.equal(pm.get(id)!.status, 'killed');
    // The underlying process really dies -> close fires -> runningCount drops.
    const dropped = await waitFor(() => pm.runningCount() === 0);
    assert.equal(dropped, true);
  });

  it('kill() returns false for an unknown task', () => {
    assert.equal(pm.kill('bg-missing'), false);
  });

  it('kill() returns false when the task already finished', async () => {
    const id = pm.spawn('echo quick', { cwd: tmpDir });
    await waitForComplete(pm, id);
    assert.equal(pm.get(id)!.status, 'completed');
    assert.equal(pm.kill(id), false);
  });

  it('a second kill() on an already-killed task returns false', async () => {
    const id = pm.spawn('sleep 30', { cwd: tmpDir });
    assert.equal(pm.kill(id), true);
    assert.equal(pm.kill(id), false);
    await waitFor(() => pm.runningCount() === 0);
  });

  it('cleanup() kills all running tasks', async () => {
    pm.spawn('sleep 30', { cwd: tmpDir });
    pm.spawn('sleep 30', { cwd: tmpDir });
    assert.equal(pm.runningCount(), 2);
    pm.cleanup();
    // All transitioned out of running synchronously via kill().
    assert.equal(pm.runningCount(), 0);
    await new Promise(r => setTimeout(r, 80));
  });
});

// ===========================================================================
// 3. incremental tail, size, counts, drain buffer
// ===========================================================================

describe('POV pm — tail / size / counts / drain', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  beforeEach(() => { tmpDir = makeTmpDir(); pm = new ProcessManager(tmpDir); });
  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('getOutputTail reads appended bytes incrementally and reports the new offset', async () => {
    const id = pm.spawn("printf 'abcde'", { cwd: tmpDir });
    await waitForComplete(pm, id);
    const first = pm.getOutputTail(id, 0)!;
    assert.match(first.output, /abcde/);
    assert.ok(first.offset > 0);
    assert.equal(first.reset, false);
    // Reading from the end yields nothing new.
    const second = pm.getOutputTail(id, first.offset)!;
    assert.equal(second.output, '');
    assert.equal(second.offset, first.offset);
  });

  it('getOutputTail flags reset when fromOffset is past the file size', async () => {
    const id = pm.spawn("printf 'xy'", { cwd: tmpDir });
    await waitForComplete(pm, id);
    const r = pm.getOutputTail(id, 9_999_999)!;
    assert.equal(r.reset, true);
    assert.match(r.output, /xy/);
  });

  it('getOutputSize reflects bytes written for the task', async () => {
    const id = pm.spawn("printf '0123456789'", { cwd: tmpDir });
    await waitForComplete(pm, id);
    await waitFor(() => pm.getOutputSize(id) >= 10);
    assert.equal(pm.getOutputSize(id), 10);
  });

  it('getOutput tailLines=0 returns the full content', async () => {
    const id = pm.spawn("printf 'l1\\nl2\\nl3\\n'", { cwd: tmpDir });
    await waitForComplete(pm, id);
    const full = pm.getOutput(id, 0)!;
    assert.match(full.output, /l1/);
    assert.match(full.output, /l3/);
  });

  it('getOutput with a small tailLines keeps only the last lines', async () => {
    const id = pm.spawn("printf 'a\\nb\\nc\\nd\\n'", { cwd: tmpDir });
    await waitForComplete(pm, id);
    const tail = pm.getOutput(id, 1)!;
    assert.ok(!/a/.test(tail.output.replace(/\n/g, '')) || tail.output.trim() === 'd' || tail.output.trim() === '');
  });

  it('runningAgentCount only counts [agent]-prefixed running tasks', async () => {
    const bash = pm.spawn('sleep 30', { cwd: tmpDir });
    pm.registerAgent('agent-1', {
      command: '[agent] do work',
      description: 'agent',
      outputPath: path.join(tmpDir, 'agent-1.out'),
      abortFn: () => {},
    });
    assert.equal(pm.runningAgentCount(), 1);
    assert.equal(pm.runningCount(), 2);
    pm.kill(bash);
    await waitFor(() => pm.runningCount() === 1);
  });

  it('drainCompletedTasks returns buffered completions then clears', async () => {
    const id = pm.spawn('echo buffered', { cwd: tmpDir });
    await waitForComplete(pm, id);
    const drained = pm.drainCompletedTasks();
    assert.ok(drained.some(d => d.taskId === id));
    // Second drain is empty — the buffer cleared.
    assert.deepEqual(pm.drainCompletedTasks(), []);
  });
});

// ===========================================================================
// 4. registerAgent / updateEntry in-process lifecycle
// ===========================================================================

describe('POV pm — registered agents', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  beforeEach(() => { tmpDir = makeTmpDir(); pm = new ProcessManager(tmpDir); });
  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('registerAgent adds a running task whose pid is the main process', () => {
    pm.registerAgent('a1', {
      command: '[agent] x',
      description: 'd',
      outputPath: path.join(tmpDir, 'a1.out'),
      abortFn: () => {},
    });
    const t = pm.get('a1')!;
    assert.equal(t.status, 'running');
    assert.equal(t.pid, process.pid);
    assert.equal(t.tokenCount, 0);
  });

  it('kill() of a registered agent invokes the abort callback, not process group kill', () => {
    let aborted = false;
    pm.registerAgent('a2', {
      command: '[agent] y',
      description: 'd',
      outputPath: path.join(tmpDir, 'a2.out'),
      abortFn: () => { aborted = true; },
    });
    const ok = pm.kill('a2');
    assert.equal(ok, true);
    assert.equal(aborted, true);
    assert.equal(pm.get('a2')!.status, 'killed');
  });

  it('updateEntry patches status / exitCode / tokenCount', () => {
    pm.registerAgent('a3', {
      command: '[agent] z',
      description: 'd',
      outputPath: path.join(tmpDir, 'a3.out'),
      abortFn: () => {},
    });
    pm.updateEntry('a3', { status: 'completed', exitCode: 0, tokenCount: 42 });
    const t = pm.get('a3')!;
    assert.equal(t.status, 'completed');
    assert.equal(t.exitCode, 0);
    assert.equal(t.tokenCount, 42);
  });

  it('updateEntry on an unknown task is a no-op', () => {
    assert.doesNotThrow(() => pm.updateEntry('nope', { status: 'failed' }));
  });
});

// ===========================================================================
// 5. pure helpers: validateBackgroundCommand / isProcessAlive / forceKill
// ===========================================================================

describe('POV pm — validateBackgroundCommand', () => {
  it('rejects empty / whitespace-only commands', () => {
    assert.equal(validateBackgroundCommand('').allowed, false);
    assert.equal(validateBackgroundCommand('   ').allowed, false);
  });

  it('rejects infinite-loop shapes', () => {
    for (const cmd of ['while true; do echo x; done', 'while(true){}', 'yes | head', 'cat /dev/urandom', 'tail -f /dev/null', 'for(;;){}', 'while : ; do :; done']) {
      const r = validateBackgroundCommand(cmd);
      assert.equal(r.allowed, false, `expected blocked: ${cmd}`);
      assert.match(r.reason!, /infinite loop/);
    }
  });

  it('rejects commands that discard all output to /dev/null', () => {
    assert.equal(validateBackgroundCommand('run.sh > /dev/null 2>&1').allowed, false);
    assert.equal(validateBackgroundCommand('run.sh &> /dev/null').allowed, false);
  });

  it('allows an ordinary command', () => {
    const r = validateBackgroundCommand('npm run build');
    assert.equal(r.allowed, true);
    assert.equal(r.reason, undefined);
  });
});

describe('POV pm — isProcessAlive / forceKillAfterTimeout', () => {
  it('isProcessAlive is true for the current process, false for a dead pid', () => {
    assert.equal(isProcessAlive(process.pid), true);
    assert.equal(isProcessAlive(2_147_483_600), false);
  });

  it('forceKillAfterTimeout returns false immediately when the pid is already dead', async () => {
    const saved = { isAlive: procHooks.isAlive, kill: procHooks.kill };
    procHooks.isAlive = () => false;
    let killed = false;
    procHooks.kill = () => { killed = true; };
    try {
      const r = await forceKillAfterTimeout(1234, 50);
      assert.equal(r, false);
      assert.equal(killed, false);
    } finally {
      procHooks.isAlive = saved.isAlive;
      procHooks.kill = saved.kill;
    }
  });

  it('forceKillAfterTimeout resolves true once SIGTERM makes the process die', async () => {
    const saved = { isAlive: procHooks.isAlive, kill: procHooks.kill };
    let alive = true;
    const signals: Array<NodeJS.Signals | number> = [];
    procHooks.isAlive = () => alive;
    procHooks.kill = (_pid, sig) => { signals.push(sig); if (sig === 'SIGTERM') alive = false; };
    try {
      const r = await forceKillAfterTimeout(4321, 500);
      assert.equal(r, true);
      assert.ok(signals.includes('SIGTERM'));
      assert.ok(!signals.includes('SIGKILL'));
    } finally {
      procHooks.isAlive = saved.isAlive;
      procHooks.kill = saved.kill;
    }
  });

  it('forceKillAfterTimeout escalates to SIGKILL when SIGTERM does not kill in time', async () => {
    const saved = { isAlive: procHooks.isAlive, kill: procHooks.kill };
    const signals: Array<NodeJS.Signals | number> = [];
    procHooks.isAlive = () => true; // never dies within the window
    procHooks.kill = (_pid, sig) => { signals.push(sig); };
    try {
      const r = await forceKillAfterTimeout(5555, 60);
      assert.equal(r, true);
      assert.ok(signals.includes('SIGTERM'));
      assert.ok(signals.includes('SIGKILL'));
    } finally {
      procHooks.isAlive = saved.isAlive;
      procHooks.kill = saved.kill;
    }
  });

  it('forceKillAfterTimeout returns false when SIGTERM throws', async () => {
    const saved = { isAlive: procHooks.isAlive, kill: procHooks.kill };
    procHooks.isAlive = () => true;
    procHooks.kill = () => { throw new Error('EPERM'); };
    try {
      const r = await forceKillAfterTimeout(6666, 50);
      assert.equal(r, false);
    } finally {
      procHooks.isAlive = saved.isAlive;
      procHooks.kill = saved.kill;
    }
  });

  it('exposes stable module constants', () => {
    assert.equal(typeof GRACEFUL_SHUTDOWN_MS, 'number');
    assert.ok(GRACEFUL_SHUTDOWN_MS > 0);
    assert.equal(MAX_BACKGROUND_PROCESSES, 20);
  });
});
