import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  ProcessManager,
  formatBackgroundCompletionNote,
  STALL_CHECK_INTERVAL_MS,
  STALL_THRESHOLD_MS,
  STALL_TAIL_BYTES,
  type CompletedTaskEvent,
  type KilledTaskEvent,
} from '../src/process-manager';

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'pm-w66-'));
}

async function cleanupPm(pm: ProcessManager | undefined): Promise<void> {
  if (!pm) return;
  try { pm.cleanup(); } catch { /* ignore */ }
  await new Promise((r) => setTimeout(r, 30));
}

function waitFor(cond: () => boolean, timeoutMs = 5000): Promise<void> {
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
// GAP: status-aware completion summary (completed vs failed vs stopped) +
// distinct terminal-state labelling. formatBackgroundCompletionNote branches on
// the terminal status rather than always saying "completed".
// ---------------------------------------------------------------------------
describe('formatBackgroundCompletionNote – status-aware wording', () => {
  const base: CompletedTaskEvent = {
    taskId: 'bg-1', exitCode: 0, command: 'build', description: 'build', outputPath: '/tmp/o',
  };

  it('completed with a known exit code', () => {
    const note = formatBackgroundCompletionNote({ ...base, status: 'completed', exitCode: 0 });
    assert.match(note, /"bg-1" completed \(exit code: 0\)/);
    assert.match(note, /Output file: \/tmp\/o\./);
    assert.match(note, /task_output\(\{ task_id: "bg-1" \}\)/);
  });

  it('failed renders "failed with exit code N"', () => {
    const note = formatBackgroundCompletionNote({ ...base, status: 'failed', exitCode: 2 });
    assert.match(note, /"bg-1" failed with exit code 2\./);
    assert.doesNotMatch(note, /completed/);
  });

  it('killed renders "was stopped" and never an exit code', () => {
    const note = formatBackgroundCompletionNote({ ...base, status: 'killed', exitCode: 137 });
    assert.match(note, /"bg-1" was stopped\./);
    assert.doesNotMatch(note, /exit code/);
  });

  it('omits the exit code when it is unknown (null)', () => {
    const note = formatBackgroundCompletionNote({ ...base, status: 'failed', exitCode: null });
    assert.match(note, /"bg-1" failed\./);
    assert.doesNotMatch(note, /exit code/);
  });

  it('a legacy event without status falls back to completed wording', () => {
    const note = formatBackgroundCompletionNote({ ...base, exitCode: 0 });
    assert.match(note, /"bg-1" completed \(exit code: 0\)/);
  });

  it('omits the output-file clause when there is no output path', () => {
    const note = formatBackgroundCompletionNote({ ...base, status: 'completed', outputPath: '' });
    assert.doesNotMatch(note, /Output file:/);
  });
});

// ---------------------------------------------------------------------------
// E2E: a real spawned command that FAILS surfaces a status-aware note through
// the full spawn -> close -> durable buffer -> drain -> format path.
// ---------------------------------------------------------------------------
describe('ProcessManager – CompletedTaskEvent carries the terminal status (E2E)', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('a failing background command buffers status="failed" and formats as failed', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('exit 3', { cwd: tmpDir });
    await waitFor(() => pm.getStatus(id) === 'failed');
    const drained = pm.drainCompletedTasks();
    const ev = drained.find((e) => e.taskId === id);
    assert.ok(ev, 'completion was buffered');
    assert.equal(ev!.status, 'failed');
    assert.equal(ev!.exitCode, 3);
    assert.match(formatBackgroundCompletionNote(ev!), /failed with exit code 3/);
  });

  it('a successful background command buffers status="completed"', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('true', { cwd: tmpDir });
    await waitFor(() => pm.getStatus(id) === 'completed');
    const ev = pm.drainCompletedTasks().find((e) => e.taskId === id);
    assert.ok(ev);
    assert.equal(ev!.status, 'completed');
  });
});

// ---------------------------------------------------------------------------
// GAP: introspection/debug signal when a background task is killed.
// ---------------------------------------------------------------------------
describe('ProcessManager – kill() emits a "killed" introspection event', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('emits taskType="bash" and suppressed=false for a default kill', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const events: KilledTaskEvent[] = [];
    pm.on('killed', (e: KilledTaskEvent) => events.push(e));
    const id = pm.spawn('sleep 30', { cwd: tmpDir });
    assert.equal(pm.kill(id), true);
    assert.equal(events.length, 1);
    assert.equal(events[0].taskId, id);
    assert.equal(events[0].taskType, 'bash');
    assert.equal(events[0].suppressed, false);
  });

  it('emits suppressed=true when the explicit stop latches notified', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const events: KilledTaskEvent[] = [];
    pm.on('killed', (e: KilledTaskEvent) => events.push(e));
    const id = pm.spawn('sleep 30', { cwd: tmpDir });
    assert.equal(pm.kill(id, { suppressCompletion: true }), true);
    assert.equal(events.length, 1);
    assert.equal(events[0].suppressed, true);
  });

  it('emits taskType="agent" for an in-process agent', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const events: KilledTaskEvent[] = [];
    pm.on('killed', (e: KilledTaskEvent) => events.push(e));
    pm.registerAgent('agent-x', {
      command: '[agent] x', description: 'x', outputPath: path.join(tmpDir, 'x.out'), abortFn: () => {},
    });
    assert.equal(pm.kill('agent-x'), true);
    assert.equal(events[0].taskType, 'agent');
  });

  it('does not emit for an unknown or already-terminal task', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const events: KilledTaskEvent[] = [];
    pm.on('killed', (e: KilledTaskEvent) => events.push(e));
    assert.equal(pm.kill('nope'), false);
    assert.equal(events.length, 0);
  });
});

// ---------------------------------------------------------------------------
// GAP: per-agent ownership (agentId) + agent-scoped orphan reaping.
// ---------------------------------------------------------------------------
describe('ProcessManager – killTasksForAgent reaps a sub-agent\'s orphan shells', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('records agentId on spawn and exposes it via get()/list()', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('sleep 30', { cwd: tmpDir, agentId: 'agent-A' });
    assert.equal(pm.get(id)!.agentId, 'agent-A');
    assert.ok(pm.list().some((t) => t.taskId === id && t.agentId === 'agent-A'));
  });

  it('kills only the running tasks owned by the given agent', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const a1 = pm.spawn('sleep 30', { cwd: tmpDir, agentId: 'agent-A' });
    const a2 = pm.spawn('sleep 30', { cwd: tmpDir, agentId: 'agent-A' });
    const b1 = pm.spawn('sleep 30', { cwd: tmpDir, agentId: 'agent-B' });
    const main = pm.spawn('sleep 30', { cwd: tmpDir }); // no owner

    const killed = pm.killTasksForAgent('agent-A');
    assert.equal(killed, 2);
    assert.equal(pm.getStatus(a1), 'killed');
    assert.equal(pm.getStatus(a2), 'killed');
    assert.equal(pm.getStatus(b1), 'running');
    assert.equal(pm.getStatus(main), 'running');
  });

  it('returns 0 for an empty agentId or one that owns nothing', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    assert.equal(pm.killTasksForAgent(''), 0);
    assert.equal(pm.killTasksForAgent('ghost'), 0);
  });

  it('registerAgent records a parent agentId when supplied', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    pm.registerAgent('child', {
      command: '[agent] child', description: 'c', outputPath: path.join(tmpDir, 'c.out'),
      abortFn: () => {}, agentId: 'parent',
    });
    assert.equal(pm.get('child')!.agentId, 'parent');
    assert.equal(pm.killTasksForAgent('parent'), 1);
    assert.equal(pm.getStatus('child'), 'killed');
  });
});

// ---------------------------------------------------------------------------
// GAP: stall-watchdog tuning knobs (separate check interval, no-growth
// threshold, byte-bounded tail).
// ---------------------------------------------------------------------------
describe('ProcessManager – stall-watchdog tuning knobs', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('exposes distinct, the reference-parity knob constants', () => {
    assert.equal(STALL_CHECK_INTERVAL_MS, 5_000);
    assert.equal(STALL_THRESHOLD_MS, 45_000);
    assert.equal(STALL_TAIL_BYTES, 1024);
    assert.ok(STALL_CHECK_INTERVAL_MS < STALL_THRESHOLD_MS, 'poll cadence is finer than the threshold');
  });

  it('does not notify until output has not grown for the full threshold', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn("printf 'Continue? (y/n) '; sleep 30", { cwd: tmpDir });
    await waitFor(() => pm.getOutputSize(id) > 0);
    const events: any[] = [];
    pm.on('stalled', (e) => events.push(e));

    const t0 = Date.now();
    let last = 0;
    last = (pm as any).stallTick(id, last, t0);                         // records growth
    last = (pm as any).stallTick(id, last, t0 + 10_000);                // 10s no growth < 45s
    assert.equal(events.length, 0, 'within-threshold stall is not reported');
    last = (pm as any).stallTick(id, last, t0 + STALL_THRESHOLD_MS + 1); // past threshold
    assert.equal(events.length, 1, 'stall reported once the threshold elapses');
    assert.equal(pm.getStatus(id), 'running', 'stall notifies, never kills');
  });
});
