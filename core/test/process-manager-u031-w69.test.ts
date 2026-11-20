import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  ProcessManager,
  formatUsageClause,
  formatBackgroundCompletionNote,
  computeElapsedMs,
  TERMINAL_RETENTION_MS,
  type CompletedTaskEvent,
} from '../src/process-manager';
import { taskListTool } from '../src/tools/task-list';
import type { ToolContext } from '../src/types';

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-u031-w69-'));
}

function waitFor(pred: () => boolean, timeoutMs = 5000): Promise<void> {
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const tick = () => {
      if (pred()) return resolve();
      if (Date.now() - start > timeoutMs) return reject(new Error('waitFor timed out'));
      setTimeout(tick, 15);
    };
    tick();
  });
}

async function cleanupPm(pm: ProcessManager | null): Promise<void> {
  if (!pm) return;
  pm.cleanup();
  await new Promise(r => setTimeout(r, 100));
}

// ---------------------------------------------------------------------------
// Feature A — usage (duration_ms) clause in the background completion note.
//   gap: LocalAgentTask "agent completion notification includes a usage section
//   with duration_ms". The drained bash-completion note now carries how long the
//   task ran, mirroring the reference's completion `duration_ms`.
// ---------------------------------------------------------------------------

const baseEvent: CompletedTaskEvent = {
  taskId: 'bg-1', exitCode: 0, command: 'build', description: 'build', outputPath: '/tmp/o',
};

describe('formatUsageClause', () => {
  it('renders a duration clause when durationMs is known', () => {
    assert.equal(formatUsageClause({ ...baseEvent, durationMs: 3210 }), ' Usage: ran 3.2s.');
  });

  it('renders 0.0s for a zero duration (still a known value)', () => {
    assert.equal(formatUsageClause({ ...baseEvent, durationMs: 0 }), ' Usage: ran 0.0s.');
  });

  it('returns empty string when durationMs is absent (legacy/unknown)', () => {
    assert.equal(formatUsageClause(baseEvent), '');
  });

  it('returns empty string for a negative durationMs (skewed clock)', () => {
    assert.equal(formatUsageClause({ ...baseEvent, durationMs: -5 }), '');
  });
});

describe('formatBackgroundCompletionNote – usage clause integration', () => {
  it('appends the usage clause after the summary, before Command', () => {
    const note = formatBackgroundCompletionNote({ ...baseEvent, status: 'completed', exitCode: 0, durationMs: 1500 });
    assert.match(note, /"bg-1" completed \(exit code: 0\)\. Usage: ran 1\.5s\. Command: build\./);
  });

  it('omits the usage clause entirely when no duration is attached (backward compatible)', () => {
    const note = formatBackgroundCompletionNote({ ...baseEvent, status: 'completed', exitCode: 0 });
    assert.doesNotMatch(note, /Usage:/);
    assert.match(note, /"bg-1" completed \(exit code: 0\)\. Command: build\./);
  });

  it('a killed note keeps "was stopped", never an exit code, even with a usage clause', () => {
    const note = formatBackgroundCompletionNote({ ...baseEvent, status: 'killed', exitCode: 137, durationMs: 42000 });
    assert.match(note, /"bg-1" was stopped\. Usage: ran 42\.0s\./);
    assert.doesNotMatch(note, /exit code/);
  });
});

describe('ProcessManager – emitComplete attaches durationMs (E2E through drain)', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('a real spawned command buffers a completion carrying a non-negative durationMs', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('true', { cwd: tmpDir });
    await waitFor(() => pm.getStatus(id) === 'completed');
    const ev = pm.drainCompletedTasks().find(e => e.taskId === id);
    assert.ok(ev, 'completion buffered');
    assert.equal(typeof ev!.durationMs, 'number');
    assert.ok(ev!.durationMs! >= 0, 'durationMs is non-negative');
    // The drained note the engine would push carries the usage clause.
    assert.match(formatBackgroundCompletionNote(ev!), /Usage: ran \d+\.\ds\./);
  });

  it('the attached durationMs matches the frozen computeElapsedMs value', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('true', { cwd: tmpDir });
    await waitFor(() => pm.getStatus(id) === 'completed');
    const snap = pm.get(id)!;
    const ev = pm.drainCompletedTasks().find(e => e.taskId === id)!;
    const expected = computeElapsedMs(snap.startTime, snap.endTime, snap.totalPausedMs ?? 0);
    // endTime is frozen, so both compute off the same inputs and must agree.
    assert.equal(ev.durationMs, expected);
  });
});

// ---------------------------------------------------------------------------
// Feature B — evictAfter deadline-based retention + opportunistic pruneExpired.
//   gaps: evictAfter / grace-period eviction / timed auto-eviction of terminal
//   rows (CoordinatorAgentStatus, teammateViewHelpers, framework). A finished
//   task is retained for TERMINAL_RETENTION_MS, then auto-reaped.
// ---------------------------------------------------------------------------

describe('ProcessManager – evictAfter retention field', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('sets evictAfter = endTime + TERMINAL_RETENTION_MS on a terminal transition', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('true', { cwd: tmpDir });
    await waitFor(() => pm.getStatus(id) === 'completed');
    const snap = pm.get(id)!;
    assert.equal(typeof snap.endTime, 'number');
    assert.equal(snap.evictAfter, snap.endTime! + TERMINAL_RETENTION_MS);
  });

  it('a running task has no evictAfter', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('sleep 30', { cwd: tmpDir });
    await waitFor(() => pm.getStatus(id) === 'running');
    assert.equal(pm.get(id)!.evictAfter, undefined);
  });

  it('a killed registered agent gets an evictAfter deadline', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    pm.registerAgent('agent-x', {
      command: '[agent] worker', description: 'worker', outputPath: '', abortFn: () => {},
    });
    pm.kill('agent-x');
    const snap = pm.get('agent-x')!;
    assert.equal(snap.status, 'killed');
    assert.equal(snap.evictAfter, snap.endTime! + TERMINAL_RETENTION_MS);
  });
});

describe('ProcessManager – pruneExpired honours the grace window', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('does NOT reap a freshly-finished task (still within the grace window)', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('true', { cwd: tmpDir });
    await waitFor(() => pm.getStatus(id) === 'completed');
    assert.equal(pm.pruneExpired(), 0, 'nothing expired yet');
    assert.ok(pm.get(id), 'task still tracked');
  });

  it('reaps a terminal task once its evictAfter deadline has passed', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('true', { cwd: tmpDir });
    await waitFor(() => pm.getStatus(id) === 'completed');
    const deadline = pm.get(id)!.evictAfter!;
    // Drive the clock past the deadline deterministically via the injectable now.
    const reaped = pm.pruneExpired(deadline + 1);
    assert.equal(reaped, 1);
    assert.equal(pm.get(id), null, 'task evicted from the table');
  });

  it('never reaps a running task even with a far-future now', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('sleep 30', { cwd: tmpDir });
    await waitFor(() => pm.getStatus(id) === 'running');
    assert.equal(pm.pruneExpired(Date.now() + TERMINAL_RETENTION_MS * 10), 0);
    assert.ok(pm.get(id), 'running task retained');
  });
});

// ---------------------------------------------------------------------------
// Feature B wiring — task_list opportunistically evicts expired terminal rows.
// ---------------------------------------------------------------------------

describe('task_list – opportunistic retention GC via pruneExpired', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); });

  function ctx(mgr: ProcessManager): ToolContext {
    return { cwd: '/tmp', abortSignal: new AbortController().signal, processManager: mgr } as ToolContext;
  }

  it('task_list invokes pruneExpired so expired terminal rows are reaped before listing', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const done = pm.spawn('true', { cwd: tmpDir });
    const live = pm.spawn('sleep 30', { cwd: tmpDir });
    await waitFor(() => pm.getStatus(done) === 'completed');

    // Spy on pruneExpired: the wiring must CALL it. To prove the reap actually
    // removes an expired row, the spy drives the clock past the finished task's
    // deadline (delegating to the real impl) — so without the task_list hook the
    // row would remain and this test goes RED.
    let pruneCalls = 0;
    const realPrune = pm.pruneExpired.bind(pm);
    const doneDeadline = pm.get(done)!.evictAfter!;
    pm.pruneExpired = (now?: number) => { pruneCalls++; return realPrune(now ?? doneDeadline + 1); };

    const r = await taskListTool.execute({}, ctx(pm));
    assert.equal(r.isError, false);
    assert.ok(pruneCalls >= 1, 'task_list called pruneExpired');
    assert.doesNotMatch(r.output, new RegExp(done), 'evicted task absent from listing');
    assert.match(r.output, new RegExp(live), 'running task still listed');
  });

  it('a minimal manager stub lacking pruneExpired is tolerated (optional-chained)', async () => {
    const stub = { list: () => [] } as any;
    const r = await taskListTool.execute({}, ctx(stub));
    assert.equal(r.isError, false);
    assert.match(r.output, /No background tasks\./);
  });
});
