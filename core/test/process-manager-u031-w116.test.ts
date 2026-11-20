import { describe, it, afterEach, before, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  ProcessManager,
  TASK_REGISTRY_FILE,
  type PersistedTaskRecord,
} from '../src/process-manager';
import { projectStateDir } from '../src/reference-journal';

// ---------------------------------------------------------------------------
// U031-w116 — Parse-failure guard on external (persisted) task registry
// records before trusting them into tracked state.
//
// Parity with the reference's ITermBackend split-output guard
// (references/claude/claude-code-main/src/utils/swarm/backends/ITermBackend.ts:L217-L223):
// after parsing an id/handle out of external process output, an empty / garbage
// value must be REJECTED (explicit failure) instead of being pushed into tracked
// state and corrupting subsequent operations.
//
// AMI's resume path (rescanTasks) ingests an UNTRUSTED registry sidecar — it may
// be corrupt, hand-edited, or written by an old/crashed build. A record whose
// `taskId` is empty/whitespace, or whose `pid` is not a positive integer, must
// NOT be adopted/reaped into state (an empty taskId is unkillable + unlistable;
// a non-positive pid turns adoptRunning's `process.kill(-pid, ...)` into a signal
// to the WRONG process group).
// ---------------------------------------------------------------------------

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'pm-u031w116-'));
}

async function cleanupPm(p: ProcessManager | undefined): Promise<void> {
  if (!p) return;
  try { p.cleanup(); } catch { /* ignore */ }
  await new Promise((r) => setTimeout(r, 30));
}

function outputPathFor(cwd: string, sessionId: string, taskId: string): string {
  return path.join(projectStateDir(cwd, path.join('tasks', sessionId)), `${taskId || '_'}.output`);
}

function seedRegistry(cwd: string, sessionId: string, recs: PersistedTaskRecord[]): void {
  const tasksDir = projectStateDir(cwd, path.join('tasks', sessionId));
  fs.mkdirSync(tasksDir, { recursive: true });
  for (const rec of recs) {
    try { fs.writeFileSync(rec.outputPath, 'seeded\n', 'utf-8'); } catch { /* ignore bad path */ }
  }
  fs.writeFileSync(path.join(tasksDir, TASK_REGISTRY_FILE), JSON.stringify(recs), 'utf-8');
}

describe('U031-w116 rescanTasks parse-failure guard on registry records', () => {
  let pm: ProcessManager | undefined;
  let tmp: string;
  let dataDir: string;
  let prevDataDir: string | undefined;

  before(() => {
    prevDataDir = process.env.SUPERINFERENCE_DATA_DIR;
    dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pm-u031w116-data-'));
    process.env.SUPERINFERENCE_DATA_DIR = dataDir;
  });
  after(() => {
    if (prevDataDir === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
    else process.env.SUPERINFERENCE_DATA_DIR = prevDataDir;
    try { fs.rmSync(dataDir, { recursive: true, force: true }); } catch { /* ignore */ }
  });
  afterEach(async () => {
    await cleanupPm(pm);
    pm = undefined;
    if (tmp) fs.rmSync(tmp, { recursive: true, force: true });
  });

  it('rejects an empty-string taskId instead of adopting a corrupt entry', () => {
    tmp = makeTmpDir();
    const sid = 'session-empty-id';
    seedRegistry(tmp, sid, [
      { taskId: '', pid: 424242, command: 'sleep 100', description: 'corrupt', outputPath: outputPathFor(tmp, sid, ''), startTime: Date.now() },
    ]);
    pm = new ProcessManager(tmp, sid);
    // RED (old guard `typeof rec.taskId !== 'string'` accepts ''): the record is
    // adopted → readopted===1 and an unkillable empty-id entry lands in list().
    const res = pm.rescanTasks({ isAlive: () => true });
    assert.deepEqual(res, { readopted: 0, reaped: 0 });
    assert.equal(pm.list().length, 0);
  });

  it('rejects a whitespace-only taskId', () => {
    tmp = makeTmpDir();
    const sid = 'session-ws-id';
    seedRegistry(tmp, sid, [
      { taskId: '   ', pid: 424243, command: 'sleep 100', description: 'corrupt', outputPath: outputPathFor(tmp, sid, 'ws'), startTime: Date.now() },
    ]);
    pm = new ProcessManager(tmp, sid);
    const res = pm.rescanTasks({ isAlive: () => true });
    assert.deepEqual(res, { readopted: 0, reaped: 0 });
    assert.equal(pm.list().length, 0);
  });

  it('rejects a non-positive pid (would signal the wrong process group)', () => {
    tmp = makeTmpDir();
    const sid = 'session-bad-pid';
    seedRegistry(tmp, sid, [
      { taskId: 'bg-zero', pid: 0, command: 'sleep 100', description: 'pid 0', outputPath: outputPathFor(tmp, sid, 'bg-zero'), startTime: Date.now() },
      { taskId: 'bg-neg', pid: -5, command: 'sleep 100', description: 'negative pid', outputPath: outputPathFor(tmp, sid, 'bg-neg'), startTime: Date.now() },
    ]);
    pm = new ProcessManager(tmp, sid);
    // RED (old guard `typeof rec.pid !== 'number'` accepts 0 and -5): both get
    // adopted and adoptRunning would later `process.kill(-pid, ...)` the wrong group.
    const res = pm.rescanTasks({ isAlive: () => true });
    assert.deepEqual(res, { readopted: 0, reaped: 0 });
    assert.equal(pm.list().length, 0);
  });

  it('rejects a non-integer / NaN pid', () => {
    tmp = makeTmpDir();
    const sid = 'session-nan-pid';
    seedRegistry(tmp, sid, [
      { taskId: 'bg-nan', pid: Number.NaN, command: 'sleep 100', description: 'nan pid', outputPath: outputPathFor(tmp, sid, 'bg-nan'), startTime: Date.now() },
      { taskId: 'bg-frac', pid: 12.5, command: 'sleep 100', description: 'fractional pid', outputPath: outputPathFor(tmp, sid, 'bg-frac'), startTime: Date.now() },
    ]);
    pm = new ProcessManager(tmp, sid);
    const res = pm.rescanTasks({ isAlive: () => true });
    assert.deepEqual(res, { readopted: 0, reaped: 0 });
    assert.equal(pm.list().length, 0);
  });

  it('still adopts a valid record when a corrupt one is present (per-record skip, not whole-rescan abort)', () => {
    tmp = makeTmpDir();
    const sid = 'session-mixed';
    seedRegistry(tmp, sid, [
      { taskId: '', pid: 111111, command: 'bad', description: 'corrupt', outputPath: outputPathFor(tmp, sid, ''), startTime: Date.now() },
      { taskId: 'bg-good', pid: 222222, command: 'sleep 100', description: 'good', outputPath: outputPathFor(tmp, sid, 'bg-good'), startTime: Date.now() },
    ]);
    pm = new ProcessManager(tmp, sid);
    const res = pm.rescanTasks({ isAlive: () => true });
    assert.deepEqual(res, { readopted: 1, reaped: 0 });
    assert.equal(pm.getStatus('bg-good'), 'running');
    assert.ok(pm.list().some((t) => t.taskId === 'bg-good'));
    assert.ok(pm.list().every((t) => t.taskId !== ''));
  });
});
