import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { ProcessManager, computeElapsedMs, STALL_THRESHOLD_MS } from '../src/process-manager';

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'pm-elapsed-'));
}

async function cleanupPm(pm: ProcessManager | undefined): Promise<void> {
  if (!pm) return;
  try { pm.cleanup(); } catch { /* ignore */ }
  await new Promise((r) => setTimeout(r, 30));
}

function waitFor(cond: () => boolean, timeoutMs = 4000): Promise<void> {
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const tick = () => {
      if (cond()) return resolve();
      if (Date.now() - start > timeoutMs) return reject(new Error('waitFor timed out'));
      setTimeout(tick, 20);
    };
    tick();
  });
}

// ---------------------------------------------------------------------------
// computeElapsedMs: Claude-parity formula — freeze at endTime, subtract paused,
// clamp negative skew to 0 (mirrors CoordinatorAgentStatus/useElapsedTime).
// ---------------------------------------------------------------------------
describe('computeElapsedMs', () => {
  it('uses now while running, freezes at endTime once terminal', () => {
    assert.equal(computeElapsedMs(1000, undefined, 0, 6000), 5000);
    assert.equal(computeElapsedMs(1000, 4000, 0, 999999), 3000);
  });

  it('subtracts total paused time from the wall-clock', () => {
    assert.equal(computeElapsedMs(1000, 6000, 1500, 999999), 3500);
    assert.equal(computeElapsedMs(1000, undefined, 2000, 6000), 3000);
  });

  it('clamps a negative duration (clock skew / paused > wall) to 0', () => {
    assert.equal(computeElapsedMs(5000, 1000, 0, 999999), 0); // endTime < startTime
    assert.equal(computeElapsedMs(1000, 6000, 999999, 999999), 0); // paused > wall
  });
});

// ---------------------------------------------------------------------------
// pauseTask / resumeTask accounting
// ---------------------------------------------------------------------------
describe('ProcessManager – pause/resume accounting', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('accumulates totalPausedMs across a pause/resume cycle and is idempotent', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    pm.registerAgent('agent-1', {
      command: '[agent] long task',
      description: 'long',
      outputPath: path.join(tmpDir, 'a1.out'),
      abortFn: () => {},
    });

    assert.equal(pm.pauseTask('agent-1'), true);
    assert.equal(pm.pauseTask('agent-1'), false, 'second pause without resume is a no-op');
    await new Promise((r) => setTimeout(r, 40));
    assert.equal(pm.resumeTask('agent-1'), true);
    assert.equal(pm.resumeTask('agent-1'), false, 'resume without an active pause is a no-op');

    const paused = pm.get('agent-1')!.totalPausedMs ?? 0;
    assert.ok(paused >= 30, `expected paused >= 30ms, got ${paused}`);
  });

  it('refuses to pause unknown or non-running tasks', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    assert.equal(pm.pauseTask('nope'), false);
    assert.equal(pm.resumeTask('nope'), false);
    pm.registerAgent('agent-done', {
      command: '[agent] done', description: 'd', outputPath: path.join(tmpDir, 'd.out'), abortFn: () => {},
    });
    pm.updateEntry('agent-done', { status: 'completed' });
    assert.equal(pm.pauseTask('agent-done'), false, 'cannot pause a terminal task');
  });

  it('settles an open pause when a task is killed so accounting is not lost', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    pm.registerAgent('agent-k', {
      command: '[agent] k', description: 'k', outputPath: path.join(tmpDir, 'k.out'), abortFn: () => {},
    });
    assert.equal(pm.pauseTask('agent-k'), true);
    assert.equal(pm.kill('agent-k'), true);
    const entry = pm.get('agent-k')!;
    assert.equal(entry.status, 'killed');
    assert.ok((entry.totalPausedMs ?? 0) >= 0);
    // The pause is settled: elapsed is frozen and non-negative.
    const elapsed = computeElapsedMs(entry.startTime, entry.endTime, entry.totalPausedMs ?? 0);
    assert.ok(elapsed >= 0);
  });
});

// ---------------------------------------------------------------------------
// getOutput elapsedMs is pause-aware + clamped end-to-end.
// ---------------------------------------------------------------------------
describe('ProcessManager – getOutput elapsed is pause-aware', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('subtracts paused time from the reported elapsedMs', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('sleep 30', { cwd: tmpDir });
    await new Promise((r) => setTimeout(r, 20));
    const before = pm.getOutput(id)!.elapsedMs;
    assert.equal(pm.pauseTask(id), true);
    await new Promise((r) => setTimeout(r, 60));
    assert.equal(pm.resumeTask(id), true);
    const after = pm.getOutput(id)!.elapsedMs;
    const paused = pm.get(id)!.totalPausedMs ?? 0;
    assert.ok(paused >= 50, `paused accounting should be recorded, got ${paused}`);
    // Despite ~80ms of wall-clock passing, elapsed grew by far less than that
    // because the paused window is discounted.
    assert.ok(after - before < paused + 40, 'paused window is discounted from elapsed');
    assert.ok(after >= 0);
  });
});

// ---------------------------------------------------------------------------
// Stall watchdog: actionable notification instead of silent kill.
// ---------------------------------------------------------------------------
describe('ProcessManager – stall watchdog notifies instead of killing', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('writes an actionable hint + pauses the clock + emits stalled, without killing', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    // A command that prints a prompt-like line then blocks, so output stops
    // growing with an interactive-prompt tail.
    const id = pm.spawn("printf 'Continue? (y/n) '; sleep 30", { cwd: tmpDir });
    await waitFor(() => pm.getOutputSize(id) > 0);

    const stalledEvents: any[] = [];
    pm.on('stalled', (e) => stalledEvents.push(e));

    // Drive the extracted stall-check tick directly with an injected clock (the
    // real poll cadence is STALL_CHECK_INTERVAL_MS; the no-growth threshold is
    // STALL_THRESHOLD_MS). The first tick records growth; we only act once the
    // injected clock has advanced past the threshold with no further growth.
    const t0 = Date.now();
    let last = 0;
    last = (pm as any).stallTick(id, last, t0); // first tick records the size + growth time
    const sizeBeforeHint = pm.getOutputSize(id);
    // Still within the no-growth threshold → must NOT notify yet.
    last = (pm as any).stallTick(id, last, t0 + STALL_THRESHOLD_MS - 1);
    assert.equal(stalledEvents.length, 0, 'no stall notification before the no-growth threshold elapses');
    // Past the threshold + prompt-like tail → notify.
    last = (pm as any).stallTick(id, last, t0 + STALL_THRESHOLD_MS + 1);

    // The task is NOT killed — parity with the reference's actionable notification.
    assert.equal(pm.getStatus(id), 'running', 'stalled task must stay running, not be killed');
    assert.equal(stalledEvents.length, 1, 'stalled event emitted exactly once');
    assert.equal(stalledEvents[0].taskId, id);

    // The actionable hint is written into the task output so the model sees it
    // via getOutput (the already-wired delivery surface).
    const out = pm.getOutput(id, 100)!.output;
    assert.match(out, /interactive input/i);
    assert.match(out, /piped input|non-interactive/i);

    // The elapsed clock was paused.
    assert.ok((pm.get(id)!.totalPausedMs ?? 0) >= 0);
    const entry: any = (pm as any).processes.get(id);
    assert.ok(entry._pausedAt !== undefined, 'clock is paused while stalled');

    // A further tick does not re-notify (one-shot) while still stalled.
    const beforeCount = stalledEvents.length;
    (pm as any).stallTick(id, pm.getOutputSize(id), t0 + STALL_THRESHOLD_MS + 2);
    assert.equal(stalledEvents.length, beforeCount, 'stall notification is one-shot');

    // Output hint only written once.
    assert.ok(sizeBeforeHint <= pm.getOutputSize(id));
    const hintCount = (pm.getOutput(id, 1000)!.output.match(/interactive input/gi) || []).length;
    assert.equal(hintCount, 1, 'actionable hint written exactly once');
  });

  it('resumes the clock when output grows again after a stall', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    pm.registerAgent('agent-s', {
      command: '[agent] s', description: 's', outputPath: path.join(tmpDir, 's.out'), abortFn: () => {},
    });
    pm.pauseTask('agent-s');
    const entryBefore: any = (pm as any).processes.get('agent-s');
    assert.ok(entryBefore._pausedAt !== undefined);
    // Simulate output growth: stallTick sees a changed size and resumes.
    (entryBefore as any).bytesWritten = 123;
    (pm as any).stallTick('agent-s', 0);
    const entryAfter: any = (pm as any).processes.get('agent-s');
    assert.equal(entryAfter._pausedAt, undefined, 'clock resumed after output grew');
  });
});
