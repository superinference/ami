import { describe, it, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { ProcessManager } from '../src/process-manager';

// ---------------------------------------------------------------------------
// Parity tests for background-process management (additive superset vs the reference
// Code background shells): concurrency-limit enforcement, output-line filter,
// and terminal-task reaping/orphan cleanup. Each block is red-green validated:
// reverting the corresponding src change makes exactly that block fail.
// ---------------------------------------------------------------------------

const tmpDirs: string[] = [];
function makeTmpDir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-parity-'));
  tmpDirs.push(d);
  return d;
}

function waitForComplete(pm: ProcessManager, taskId: string, timeoutMs = 5000): Promise<any> {
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
  await new Promise(r => setTimeout(r, 100));
}

after(() => {
  for (const d of tmpDirs) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
});

// ---------------------------------------------------------------------------
// GAP 1: concurrent background-process limit is actually enforced.
// RED (revert the atCapacity()/throw guard in spawn): the second spawn below
// succeeds and returns a bg- id, so assert.throws fails.
// ---------------------------------------------------------------------------

describe('parity: concurrency-limit enforcement', () => {
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); });

  it('atCapacity() reflects the running set vs the configured ceiling', async () => {
    const dir = makeTmpDir();
    pm = new ProcessManager(dir);
    pm.maxBackgroundProcesses = 1;
    assert.equal(pm.atCapacity(), false);
    pm.spawn('sleep 30', { cwd: dir });
    assert.equal(pm.atCapacity(), true);
  });

  it('spawn() throws once the running set reaches maxBackgroundProcesses', async () => {
    const dir = makeTmpDir();
    pm = new ProcessManager(dir);
    pm.maxBackgroundProcesses = 1;
    const first = pm.spawn('sleep 30', { cwd: dir });
    assert.ok(first.startsWith('bg-'));
    assert.throws(
      () => pm.spawn('sleep 30', { cwd: dir }),
      /too many background processes/,
    );
    // Still exactly one tracked task — the rejected spawn created nothing.
    assert.equal(pm.list().length, 1);
    assert.equal(pm.runningCount(), 1);
  });

  it('spawn() is allowed again after a running task is killed (frees a slot)', async () => {
    const dir = makeTmpDir();
    pm = new ProcessManager(dir);
    pm.maxBackgroundProcesses = 1;
    const first = pm.spawn('sleep 30', { cwd: dir });
    assert.equal(pm.atCapacity(), true);
    pm.kill(first);
    assert.equal(pm.atCapacity(), false);
    const second = pm.spawn('echo ok', { cwd: dir });
    assert.ok(second.startsWith('bg-'));
    await waitForComplete(pm, second);
  });
});

// ---------------------------------------------------------------------------
// GAP 2: getOutput() honors an optional line filter (regex), like the reference's
// BashOutput `filter`. RED (revert the filter logic in getOutput): the full
// output is returned, so the "only matching lines" assertions fail.
// ---------------------------------------------------------------------------

describe('parity: output line filter', () => {
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); });

  it('returns only lines matching the filter, dropping non-matches', async () => {
    const dir = makeTmpDir();
    pm = new ProcessManager(dir);
    const id = pm.spawn('printf "alpha\\nBETA-match\\ngamma\\ndelta-match\\n"', { cwd: dir });
    await waitForComplete(pm, id);
    const res = pm.getOutput(id, 0, 'match');
    assert.ok(res);
    assert.ok(res!.output.includes('BETA-match'));
    assert.ok(res!.output.includes('delta-match'));
    assert.ok(!res!.output.includes('alpha'));
    assert.ok(!res!.output.includes('gamma'));
  });

  it('accepts a RegExp and no filter returns all lines', async () => {
    const dir = makeTmpDir();
    pm = new ProcessManager(dir);
    const id = pm.spawn('printf "keep-1\\nskip\\nkeep-2\\n"', { cwd: dir });
    await waitForComplete(pm, id);
    const filtered = pm.getOutput(id, 0, /keep-\d/);
    assert.ok(filtered!.output.includes('keep-1'));
    assert.ok(filtered!.output.includes('keep-2'));
    assert.ok(!filtered!.output.includes('skip'));
    const all = pm.getOutput(id, 0);
    assert.ok(all!.output.includes('skip'));
  });

  it('an invalid regex string yields empty (never-match) output rather than throwing', async () => {
    const dir = makeTmpDir();
    pm = new ProcessManager(dir);
    const id = pm.spawn('printf "line-a\\nline-b\\n"', { cwd: dir });
    await waitForComplete(pm, id);
    const res = pm.getOutput(id, 0, '([unterminated');
    assert.ok(res);
    assert.equal(res!.output.trim(), '');
  });
});

// ---------------------------------------------------------------------------
// GAP 3: terminal-task reaping / orphan cleanup. RED (revert removeTask):
// pm.removeTask is undefined → calling it throws TypeError → these fail.
// The behavioral discriminators (refuses running tasks, deletes the output
// file) further guard against a no-op reimplementation.
// ---------------------------------------------------------------------------

describe('parity: terminal-task reaping / orphan cleanup', () => {
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); });

  it('removeTask() reaps a finished task and deletes its output file', async () => {
    const dir = makeTmpDir();
    pm = new ProcessManager(dir);
    const id = pm.spawn('echo done', { cwd: dir });
    await waitForComplete(pm, id);
    const outputPath = pm.get(id)!.outputPath;
    assert.ok(fs.existsSync(outputPath));
    assert.equal(pm.removeTask(id), true);
    assert.equal(pm.get(id), null);
    assert.equal(pm.getStatus(id), undefined);
    assert.equal(fs.existsSync(outputPath), false);
  });

  it('removeTask() refuses a running task and an unknown id', async () => {
    const dir = makeTmpDir();
    pm = new ProcessManager(dir);
    const id = pm.spawn('sleep 30', { cwd: dir });
    assert.equal(pm.removeTask(id), false);       // still running → kept
    assert.ok(pm.get(id));
    assert.equal(pm.removeTask('bg-nonexistent'), false);
  });

  it('pruneTerminal() bulk-reaps only finished tasks, leaving running ones', async () => {
    const dir = makeTmpDir();
    pm = new ProcessManager(dir);
    const doneA = pm.spawn('echo a', { cwd: dir });
    const waitA = waitForComplete(pm, doneA);
    const doneB = pm.spawn('echo b', { cwd: dir });
    const waitB = waitForComplete(pm, doneB);
    const running = pm.spawn('sleep 30', { cwd: dir });
    await Promise.all([waitA, waitB]);
    const reaped = pm.pruneTerminal();
    assert.equal(reaped, 2);
    assert.equal(pm.get(doneA), null);
    assert.equal(pm.get(doneB), null);
    assert.ok(pm.get(running));
    assert.equal(pm.list().length, 1);
  });
});
