import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  ProcessManager,
  MAX_COMPLETED_BUFFER,
  type CompletedTaskEvent,
} from '../src/process-manager';
import { taskOutputTool } from '../src/tools/task-output';
import { taskKillTool } from '../src/tools/task-kill';
import type { ToolContext } from '../src/types';

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-u031w65-'));
}

function waitForComplete(pm: ProcessManager, taskId: string, timeoutMs = 5000): Promise<CompletedTaskEvent> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Timed out waiting for ${taskId}`)), timeoutMs);
    pm.on('complete', function handler(event: CompletedTaskEvent) {
      if (event.taskId === taskId) {
        clearTimeout(timer);
        pm.off('complete', handler);
        resolve(event);
      }
    });
  });
}

function waitForStatus(pm: ProcessManager, taskId: string, status: string, timeoutMs = 5000): Promise<void> {
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const tick = () => {
      if (pm.get(taskId)?.status === status) return resolve();
      if (Date.now() - start > timeoutMs) return reject(new Error(`Timed out waiting for ${taskId} -> ${status}`));
      setTimeout(tick, 20);
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
// GAP U031 w65 (gaps 13/19/20/23): atomic `notified` dedup — suppress the
// redundant exit-137 completion after an explicit bash kill (bash-vs-agent split).
// ---------------------------------------------------------------------------
describe('ProcessManager – notified dedup suppresses the post-kill bash completion', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('an EXPLICIT bash kill is NOT re-surfaced by drainCompletedTasks (no exit-137 noise)', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('sleep 60', { cwd: tmpDir });
    await new Promise(r => setTimeout(r, 100));

    assert.equal(pm.kill(id, { suppressCompletion: true }), true);
    // An explicit kill latches the bash task notified immediately.
    assert.equal(pm.get(id)!.notified, true);

    // Let the real child's close/exit handlers fire → finalize() → emitComplete().
    await new Promise(r => setTimeout(r, 300));

    // The suppressed completion must NOT appear in the durable buffer.
    const drained = pm.drainCompletedTasks();
    assert.ok(
      !drained.some(e => e.taskId === id),
      'a task the model explicitly killed must not re-surface a redundant completion',
    );
  });

  it('an INTERNAL kill (default, e.g. timeout/cleanup) STILL surfaces the completion', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('sleep 60', { cwd: tmpDir });
    await new Promise(r => setTimeout(r, 100));

    // No suppressCompletion option → the model did not ask to stop this task
    // (e.g. a timeout watchdog), so it MUST still be told.
    assert.equal(pm.kill(id), true);
    assert.notEqual(pm.get(id)!.notified, true);
    const ev = await waitForComplete(pm, id, 4000);
    assert.equal(ev.taskId, id);
  });

  it('E2E: task_kill tool on a running bash task suppresses the redundant completion', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('sleep 60', { cwd: tmpDir });
    await new Promise(r => setTimeout(r, 100));

    const ctx = { cwd: tmpDir, processManager: pm } as unknown as ToolContext;
    const res = await taskKillTool.execute!({ task_id: id }, ctx);
    assert.ok(!res.isError);
    assert.ok(res.output.includes('killed successfully'));
    assert.equal(pm.get(id)!.notified, true);

    await new Promise(r => setTimeout(r, 300));
    const drained = pm.drainCompletedTasks();
    assert.ok(
      !drained.some(e => e.taskId === id),
      'after task_kill the model must not also get a "[Background task completed (exit 137)]" note',
    );
  });

  it('a naturally-completed bash task IS still surfaced (dedup does not over-suppress)', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo done', { cwd: tmpDir });
    await waitForComplete(pm, id);
    assert.notEqual(pm.get(id)!.notified, true);
    const drained = pm.drainCompletedTasks();
    assert.ok(drained.some(e => e.taskId === id), 'a natural completion must still be delivered');
  });

  it('killing an in-process AGENT does NOT latch notified (abort completion still surfaces)', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    pm.registerAgent('agent-1', {
      command: '[agent] work',
      description: 'an agent',
      outputPath: path.join(tmpDir, 'agent-1.out'),
      abortFn: () => { /* abort */ },
    });
    // Even an EXPLICIT kill with suppressCompletion must NOT latch an agent — only
    // bash tasks are latched. The agent's abort completion carries the partial
    // result and must still be reported, mirroring the reference's stopTask bash-vs-agent split.
    assert.equal(pm.kill('agent-1', { suppressCompletion: true }), true);
    assert.equal(pm.get('agent-1')!.status, 'killed');
    assert.notEqual(pm.get('agent-1')!.notified, true);
  });
});

// ---------------------------------------------------------------------------
// GAP U031 w65 (gap 22): mark a task notified on retrieval via task_output, so a
// result the model already consumed is not re-announced.
// ---------------------------------------------------------------------------
describe('ProcessManager.markNotified + task_output retrieval dedup', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('markNotified is an idempotent check-and-set that drops a buffered completion', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo hi', { cwd: tmpDir });
    await waitForComplete(pm, id);
    // Completion is buffered (natural completion).
    assert.equal(pm.markNotified(id), true);
    // Now notified → the buffered completion was dropped.
    assert.deepEqual(pm.drainCompletedTasks().filter(e => e.taskId === id), []);
    // Idempotent: a second call is a no-op.
    assert.equal(pm.markNotified(id), false);
    // Unknown task → false.
    assert.equal(pm.markNotified('bg-nope'), false);
  });

  it('task_output on a TERMINAL task latches notified so the completion is not re-announced', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo terminal-output', { cwd: tmpDir });
    await waitForComplete(pm, id);

    const ctx = { cwd: tmpDir, processManager: pm } as unknown as ToolContext;
    const res = await taskOutputTool.execute!({ task_id: id }, ctx);
    assert.ok(!res.isError);
    assert.ok(res.output.includes('terminal-output'));
    // The successful terminal read latched the task.
    assert.equal(pm.get(id)!.notified, true);
    assert.deepEqual(pm.drainCompletedTasks().filter(e => e.taskId === id), []);
  });

  it('task_output on a RUNNING task leaves it un-notified', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('sleep 60', { cwd: tmpDir });
    await waitForStatus(pm, id, 'running');
    const ctx = { cwd: tmpDir, processManager: pm } as unknown as ToolContext;
    // wait:false — a one-shot non-blocking read of the running task (task_output
    // blocks by default now, which would wait out the 30s timeout on `sleep 60`).
    const res = await taskOutputTool.execute!({ task_id: id, wait: false }, ctx);
    assert.ok(!res.isError);
    assert.notEqual(pm.get(id)!.notified, true);
  });
});

// ---------------------------------------------------------------------------
// GAP U031 w65 (gaps 79/80): bounded completed-task buffer with drop-oldest.
// ---------------------------------------------------------------------------
describe('ProcessManager – bounded completed-task buffer (drop-oldest)', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('caps the durable buffer at MAX_COMPLETED_BUFFER, evicting the oldest first', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const emit = (payload: CompletedTaskEvent) => (pm as unknown as { emitComplete: (p: CompletedTaskEvent) => void }).emitComplete(payload);
    const total = MAX_COMPLETED_BUFFER + 5;
    for (let i = 0; i < total; i++) {
      emit({ taskId: `t-${i}`, exitCode: 0, command: `c${i}`, description: `d${i}`, outputPath: '' });
    }
    const drained = pm.drainCompletedTasks();
    // Never grows past the cap.
    assert.equal(drained.length, MAX_COMPLETED_BUFFER);
    // The OLDEST 5 were evicted; the newest survive in order.
    assert.equal(drained[0].taskId, 't-5');
    assert.equal(drained[drained.length - 1].taskId, `t-${total - 1}`);
  });
});
