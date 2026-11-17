import { describe, it, afterEach, before, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  ProcessManager,
  spawnHooks,
  SPAWN_FAILURE_EXIT_CODE,
  TASK_REGISTRY_FILE,
  type CompletedTaskEvent,
  type PersistedTaskRecord,
} from '../src/process-manager';
import { projectStateDir } from '../src/reference-journal';
import { taskOutputTool } from '../src/tools/task-output';
import type { ToolContext } from '../src/types';

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'pm-u031w78-'));
}

async function cleanupPm(p: ProcessManager | undefined): Promise<void> {
  if (!p) return;
  try { p.cleanup(); } catch { /* ignore */ }
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

function toolCtx(pm: ProcessManager, tmp: string): ToolContext {
  return {
    cwd: tmp,
    abortSignal: new AbortController().signal,
    processManager: pm,
  } as unknown as ToolContext;
}

let pm: ProcessManager | undefined;
let tmp: string;
const realSpawn = spawnHooks.spawn;
afterEach(async () => {
  spawnHooks.spawn = realSpawn;
  await cleanupPm(pm);
  pm = undefined;
  if (tmp) fs.rmSync(tmp, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Gap: per-session task-output directory (concurrent-session isolation)
// ---------------------------------------------------------------------------
describe('U031-w78 per-session tasks dir', () => {
  it('scopes the output dir to the session id so two sessions in one cwd are isolated', async () => {
    tmp = makeTmpDir();
    const a = new ProcessManager(tmp, 'session-AAA');
    const b = new ProcessManager(tmp, 'session-BBB');
    try {
      const ta = a.spawn('echo hi', { cwd: tmp });
      const tb = b.spawn('echo hi', { cwd: tmp });
      const pathA = a.get(ta)!.outputPath;
      const pathB = b.get(tb)!.outputPath;
      // RED (revert the sessionId keying): both paths collapse to the same
      // project-level tasks/ dir, so the parent dirs are equal and this fails.
      assert.notEqual(path.dirname(pathA), path.dirname(pathB));
      assert.ok(path.dirname(pathA).includes('session-AAA'));
      assert.ok(path.dirname(pathB).includes('session-BBB'));
    } finally {
      await cleanupPm(a);
      await cleanupPm(b);
    }
  });

  it('falls back to the project-level dir when no session id is given', async () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp);
    const t = pm.spawn('echo hi', { cwd: tmp });
    const dir = path.dirname(pm.get(t)!.outputPath);
    assert.equal(path.basename(dir), 'tasks');
  });

  it('rejects an unsafe session id (path traversal) and falls back', async () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp, '../../escape');
    const t = pm.spawn('echo hi', { cwd: tmp });
    const dir = path.dirname(pm.get(t)!.outputPath);
    // An unsafe segment is refused; the dir is the plain project-level tasks/ dir
    // and never contains the traversal payload.
    assert.equal(path.basename(dir), 'tasks');
    assert.ok(!dir.includes('escape'));
  });
});

// ---------------------------------------------------------------------------
// Gap: synchronous spawn-failure handling (fd cleanup + terminal stub, code 126)
// ---------------------------------------------------------------------------
describe('U031-w78 synchronous spawn failure', () => {
  it('records a terminal failed task (exit 126) instead of throwing when spawn throws', async () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp, 'session-spawnfail');
    const completions: CompletedTaskEvent[] = [];
    pm.on('complete', (e: CompletedTaskEvent) => completions.push(e));

    spawnHooks.spawn = () => { const e: NodeJS.ErrnoException = new Error('EMFILE: too many open files'); e.code = 'EMFILE'; throw e; };

    // RED (revert the try/catch → createTerminalStub change): spawn rethrows, so
    // this call throws and the assertions below never run.
    let taskId = '';
    assert.doesNotThrow(() => { taskId = pm!.spawn('echo hi', { cwd: tmp }); });

    const task = pm.get(taskId)!;
    assert.equal(task.status, 'failed');
    assert.equal(task.exitCode, SPAWN_FAILURE_EXIT_CODE);
    assert.equal(task.exitCode, 126);
    assert.ok(task.error && task.error.includes('EMFILE'));
    // The completion was buffered for durable drain and worded as a failure.
    const drained = pm.drainCompletedTasks();
    assert.equal(drained.length, 1);
    assert.equal(drained[0].status, 'failed');
    assert.ok(drained[0].summary && drained[0].summary.includes('failed'));
    // The diagnostic was written to the output file (fd was opened then closed —
    // no leak; the file is readable afterwards).
    const diag = fs.readFileSync(task.outputPath, 'utf-8');
    assert.ok(diag.includes('failed to start command'));
  });
});

// ---------------------------------------------------------------------------
// Gap: per-command configurable max output byte cap
// ---------------------------------------------------------------------------
describe('U031-w78 per-spawn maxOutputBytes override', () => {
  it('kills a task whose output exceeds the per-spawn cap, independent of the manager default', async () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp, 'session-cap');
    // Manager-level cap stays large; the per-spawn override pins THIS task to 20B.
    pm.maxOutputBytes = 5 * 1024 * 1024;
    const taskId = pm.spawn("printf 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'", { cwd: tmp, maxOutputBytes: 20 });
    // RED (revert the per-spawn cap): writeChunk uses the 5MB manager default, the
    // 36-byte output never trips the cap, the task completes rather than being
    // killed, and getStatus never becomes 'killed' → waitFor times out.
    await waitFor(() => pm!.getStatus(taskId) === 'killed');
    const out = fs.readFileSync(pm.get(taskId)!.outputPath, 'utf-8');
    assert.ok(out.includes('[output exceeded 20B cap — task killed]'));
  });

  it('a spawn WITHOUT an override still uses the manager default (completes normally)', async () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp, 'session-cap2');
    pm.maxOutputBytes = 5 * 1024 * 1024;
    const taskId = pm.spawn("printf 'hello world'", { cwd: tmp });
    await waitFor(() => pm!.getStatus(taskId) === 'completed');
    assert.equal(pm.get(taskId)!.status, 'completed');
  });
});

// ---------------------------------------------------------------------------
// Gap: outputFileRedundant optimization (delete on-disk copy read fully inline)
// ---------------------------------------------------------------------------
describe('U031-w78 outputFileRedundant', () => {
  it('deleteRedundantOutput deletes a terminal task output read fully inline', async () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp, 'session-redundant');
    const taskId = pm.spawn("printf 'small output'", { cwd: tmp });
    await waitFor(() => pm!.getStatus(taskId) === 'completed');
    const outputPath = pm.get(taskId)!.outputPath;
    assert.ok(fs.existsSync(outputPath));

    // Whole file fit inline (bytesOmitted 0) → the on-disk copy is redundant.
    const deleted = pm.deleteRedundantOutput(taskId, 0);
    assert.equal(deleted, true);
    // RED (revert deleteRedundantOutput): the file is never removed, so this fails.
    assert.ok(!fs.existsSync(outputPath));
    // The entry stays tracked (status still readable) but its path is blanked, so
    // a later read returns empty rather than a "could not be read" diagnostic.
    assert.equal(pm.get(taskId)!.status, 'completed');
    assert.equal(pm.get(taskId)!.outputPath, '');
    assert.equal(pm.getOutput(taskId)!.output, '');
  });

  it('refuses to delete a running task, an omitted-prefix read, or twice', async () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp, 'session-redundant2');
    const taskId = pm.spawn("printf 'x'", { cwd: tmp });
    // Still running (printf may already be done — spawn a sleeper instead).
    const runner = pm.spawn('sleep 2', { cwd: tmp });
    assert.equal(pm.deleteRedundantOutput(runner, 0), false); // running → refused
    await waitFor(() => pm!.getStatus(taskId) === 'completed');
    assert.equal(pm.deleteRedundantOutput(taskId, 5), false); // bytesOmitted>0 → refused
    assert.equal(pm.deleteRedundantOutput(taskId, 0), true);  // now deletes
    assert.equal(pm.deleteRedundantOutput(taskId, 0), false); // path blanked → idempotent false
    assert.equal(pm.deleteRedundantOutput('no-such-task', 0), false);
  });

  it('task_output tool deletes the redundant file after a terminal full read', async () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp, 'session-tooldel');
    const taskId = pm.spawn("printf 'done'", { cwd: tmp });
    await waitFor(() => pm!.getStatus(taskId) === 'completed');
    const outputPath = pm.get(taskId)!.outputPath;
    assert.ok(fs.existsSync(outputPath));

    const res = await taskOutputTool.execute({ task_id: taskId }, toolCtx(pm, tmp));
    assert.equal(res.isError, false);
    assert.ok(res.output.includes('done'));
    // RED (revert the task-output wiring): the file survives the read.
    assert.ok(!fs.existsSync(outputPath));
  });
});

// ---------------------------------------------------------------------------
// Gap: crash-safe resume / re-adoption of in-flight background tasks (sidecar)
// ---------------------------------------------------------------------------
describe('U031-w78 crash-safe resume (rescanTasks)', () => {
  let dataDir: string;
  let prevDataDir: string | undefined;
  before(() => {
    prevDataDir = process.env.SUPERINFERENCE_DATA_DIR;
    dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pm-u031w78-data-'));
    process.env.SUPERINFERENCE_DATA_DIR = dataDir;
  });
  after(() => {
    if (prevDataDir === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
    else process.env.SUPERINFERENCE_DATA_DIR = prevDataDir;
    try { fs.rmSync(dataDir, { recursive: true, force: true }); } catch { /* ignore */ }
  });

  function seedRegistry(cwd: string, sessionId: string, rec: PersistedTaskRecord, outputBody: string): void {
    const tasksDir = projectStateDir(cwd, path.join('tasks', sessionId));
    fs.mkdirSync(tasksDir, { recursive: true });
    fs.writeFileSync(rec.outputPath, outputBody, 'utf-8');
    fs.writeFileSync(path.join(tasksDir, TASK_REGISTRY_FILE), JSON.stringify([rec]), 'utf-8');
  }

  function outputPathFor(cwd: string, sessionId: string, taskId: string): string {
    return path.join(projectStateDir(cwd, path.join('tasks', sessionId)), `${taskId}.output`);
  }

  it('re-adopts a still-running detached task after a restart', () => {
    tmp = makeTmpDir();
    const sid = 'session-adopt';
    const outputPath = outputPathFor(tmp, sid, 'bg-alive');
    seedRegistry(tmp, sid, { taskId: 'bg-alive', pid: 424242, command: 'sleep 100', description: 'detached sleeper', outputPath, startTime: Date.now() - 2000 }, 'hello from detached\n');

    pm = new ProcessManager(tmp, sid);
    // RED (revert rescanTasks): the method does not exist / returns 0 and the task
    // is never re-tracked, so list() stays empty and these assertions fail.
    const res = pm.rescanTasks({ isAlive: () => true });
    assert.deepEqual(res, { readopted: 1, reaped: 0 });
    assert.equal(pm.getStatus('bg-alive'), 'running');
    assert.ok(pm.list().some((t) => t.taskId === 'bg-alive'));
    assert.ok(pm.getOutput('bg-alive')!.output.includes('hello from detached'));
  });

  it('reaps a task that finished while the CLI was down (surfaces a completion)', () => {
    tmp = makeTmpDir();
    const sid = 'session-reap';
    const outputPath = outputPathFor(tmp, sid, 'bg-dead');
    seedRegistry(tmp, sid, { taskId: 'bg-dead', pid: 525252, command: 'make build', description: 'the build', outputPath, startTime: Date.now() - 5000 }, 'build finished\n');

    pm = new ProcessManager(tmp, sid);
    const res = pm.rescanTasks({ isAlive: () => false });
    assert.deepEqual(res, { readopted: 0, reaped: 1 });
    assert.equal(pm.getStatus('bg-dead'), 'completed');
    const drained = pm.drainCompletedTasks();
    assert.equal(drained.length, 1);
    assert.equal(drained[0].taskId, 'bg-dead');
    assert.ok(drained[0].summary && drained[0].summary.includes('finished while the session was away'));
    // Its output is still readable after reconciliation.
    assert.ok(pm.getOutput('bg-dead')!.output.includes('build finished'));
  });

  it('a re-adopted task finalizes once its pid later dies', async () => {
    tmp = makeTmpDir();
    const sid = 'session-adopt-finalize';
    const outputPath = outputPathFor(tmp, sid, 'bg-later');
    seedRegistry(tmp, sid, { taskId: 'bg-later', pid: 626262, command: 'sleep 1', description: 'short sleeper', outputPath, startTime: Date.now() }, 'running\n');

    pm = new ProcessManager(tmp, sid);
    let alive = true;
    pm.rescanTasks({ isAlive: () => alive });
    assert.equal(pm.getStatus('bg-later'), 'running');
    // The pid dies; the unref'd liveness poll (ADOPT_POLL_INTERVAL_MS) finalizes it.
    alive = false;
    await waitFor(() => pm!.getStatus('bg-later') === 'completed', 6000);
    const drained = pm.drainCompletedTasks();
    assert.ok(drained.some((e) => e.taskId === 'bg-later' && !!e.summary && e.summary.includes('re-adopted task')));
  });

  it('spawn persists to the registry and a terminated task is dropped from it', async () => {
    tmp = makeTmpDir();
    const sid = 'session-persist';
    pm = new ProcessManager(tmp, sid);
    const taskId = pm.spawn('sleep 5', { cwd: tmp });
    const regPath = path.join(projectStateDir(tmp, path.join('tasks', sid)), TASK_REGISTRY_FILE);
    const afterSpawn = JSON.parse(fs.readFileSync(regPath, 'utf-8')) as PersistedTaskRecord[];
    // RED (revert persistRegistry/syncRegistry): the sidecar is never written → readFileSync throws.
    assert.equal(afterSpawn.length, 1);
    assert.equal(afterSpawn[0].taskId, taskId);
    pm.kill(taskId);
    const afterKill = JSON.parse(fs.readFileSync(regPath, 'utf-8')) as PersistedTaskRecord[];
    assert.equal(afterKill.length, 0);
  });

  it('is a no-op for a missing or corrupt sidecar', () => {
    tmp = makeTmpDir();
    pm = new ProcessManager(tmp, 'session-empty');
    assert.deepEqual(pm.rescanTasks(), { readopted: 0, reaped: 0 }); // no file
    const regPath = path.join(projectStateDir(tmp, path.join('tasks', 'session-empty')), TASK_REGISTRY_FILE);
    fs.writeFileSync(regPath, 'not json{', 'utf-8');
    assert.deepEqual(pm.rescanTasks(), { readopted: 0, reaped: 0 }); // corrupt
    fs.writeFileSync(regPath, JSON.stringify({ not: 'an array' }), 'utf-8');
    assert.deepEqual(pm.rescanTasks(), { readopted: 0, reaped: 0 }); // non-array
  });
});
