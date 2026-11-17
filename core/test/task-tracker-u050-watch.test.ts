import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  DEFAULT_TASK_WATCH_DEBOUNCE_MS,
  resolveTaskWatchDebounceMs,
  reconcileExternalTaskChanges,
  watchTaskList,
  setTaskPersistPath,
  resetTaskState,
  taskTrackerTool,
} from '../src/tools/task-tracker';
import { projectStateDir } from '../src/reference-journal';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// U050 — cross-process task surfacing: a configurable debounce knob
// (resolveTaskWatchDebounceMs, mirroring Claude Code's DEBOUNCE_MS), a reconcile
// that diffs the on-disk task file against the in-memory store and returns ONLY
// another process's deltas (reconcileExternalTaskChanges), and the fs.watch-based
// live watcher (watchTaskList) that debounces bursts and feeds those deltas to a
// consumer. These are the building blocks the engine wires into its `task_updated`
// live feed (the push counterpart to the pull-based next/claim loop).
// ---------------------------------------------------------------------------

function makeDataDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'si-task-watch-'));
}

function persistFile(cwd: string): string {
  return path.join(projectStateDir(cwd), 'tasks.json');
}

function writeExternalTasks(cwd: string, tasks: Array<Record<string, unknown>>, nextId = 100): void {
  const file = persistFile(cwd);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, JSON.stringify({ nextId, tasks }, null, 2), 'utf-8');
}

function extTask(id: number, subject: string, status = 'pending'): Record<string, unknown> {
  return { id, subject, description: '', status, blocks: [], blockedBy: [] };
}

function ctx(cwd: string): ToolContext {
  return { cwd, abortSignal: new AbortController().signal };
}

describe('U050 resolveTaskWatchDebounceMs — env waterfall', () => {
  const saved = { ...process.env };
  afterEach(() => {
    delete process.env.AMI_TASK_WATCH_DEBOUNCE_MS;
    delete process.env.CLAUDE_CODE_TASK_WATCH_DEBOUNCE_MS;
    process.env.AMI_TASK_WATCH_DEBOUNCE_MS = saved.AMI_TASK_WATCH_DEBOUNCE_MS;
    process.env.CLAUDE_CODE_TASK_WATCH_DEBOUNCE_MS = saved.CLAUDE_CODE_TASK_WATCH_DEBOUNCE_MS;
    if (saved.AMI_TASK_WATCH_DEBOUNCE_MS === undefined) delete process.env.AMI_TASK_WATCH_DEBOUNCE_MS;
    if (saved.CLAUDE_CODE_TASK_WATCH_DEBOUNCE_MS === undefined) delete process.env.CLAUDE_CODE_TASK_WATCH_DEBOUNCE_MS;
  });

  it('defaults to DEBOUNCE_MS = 1000 (Claude parity) when nothing is set', () => {
    delete process.env.AMI_TASK_WATCH_DEBOUNCE_MS;
    delete process.env.CLAUDE_CODE_TASK_WATCH_DEBOUNCE_MS;
    assert.equal(DEFAULT_TASK_WATCH_DEBOUNCE_MS, 1000);
    assert.equal(resolveTaskWatchDebounceMs(), 1000);
  });

  it('an explicit positive integer wins over every env source', () => {
    process.env.AMI_TASK_WATCH_DEBOUNCE_MS = '250';
    assert.equal(resolveTaskWatchDebounceMs(42), 42);
  });

  it('AMI_TASK_WATCH_DEBOUNCE_MS beats the CLAUDE_CODE compat knob', () => {
    process.env.AMI_TASK_WATCH_DEBOUNCE_MS = '300';
    process.env.CLAUDE_CODE_TASK_WATCH_DEBOUNCE_MS = '900';
    assert.equal(resolveTaskWatchDebounceMs(), 300);
  });

  it('falls back to the CLAUDE_CODE compat knob when the AMI knob is unset', () => {
    delete process.env.AMI_TASK_WATCH_DEBOUNCE_MS;
    process.env.CLAUDE_CODE_TASK_WATCH_DEBOUNCE_MS = '750';
    assert.equal(resolveTaskWatchDebounceMs(), 750);
  });

  it('ignores a zero / negative / non-integer / unparseable value (never disables debouncing)', () => {
    delete process.env.CLAUDE_CODE_TASK_WATCH_DEBOUNCE_MS;
    process.env.AMI_TASK_WATCH_DEBOUNCE_MS = '0';
    assert.equal(resolveTaskWatchDebounceMs(), 1000);
    process.env.AMI_TASK_WATCH_DEBOUNCE_MS = '-5';
    assert.equal(resolveTaskWatchDebounceMs(), 1000);
    process.env.AMI_TASK_WATCH_DEBOUNCE_MS = '12.5';
    assert.equal(resolveTaskWatchDebounceMs(), 1000);
    process.env.AMI_TASK_WATCH_DEBOUNCE_MS = 'abc';
    assert.equal(resolveTaskWatchDebounceMs(), 1000);
    process.env.AMI_TASK_WATCH_DEBOUNCE_MS = '';
    assert.equal(resolveTaskWatchDebounceMs(), 1000);
    // An invalid explicit also falls through to env/default.
    assert.equal(resolveTaskWatchDebounceMs(0), 1000);
    assert.equal(resolveTaskWatchDebounceMs(-1), 1000);
  });
});

describe('U050 reconcileExternalTaskChanges — diff on-disk vs in-memory', () => {
  const dirs: string[] = [];
  afterEach(() => {
    resetTaskState();
    for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true });
    delete process.env.SUPERINFERENCE_DATA_DIR;
  });

  it('returns nothing when persistence is disabled', () => {
    resetTaskState(); // detaches persistPath
    assert.deepEqual(reconcileExternalTaskChanges(), []);
  });

  it('surfaces an externally-created task and reconciles it into memory', () => {
    const dataDir = makeDataDir(); dirs.push(dataDir);
    process.env.SUPERINFERENCE_DATA_DIR = dataDir;
    const cwd = '/repo/u050-create';
    setTaskPersistPath(cwd); // empty file → empty memory
    // Another process writes a brand-new task to the shared file.
    writeExternalTasks(cwd, [extTask(7, 'external work', 'pending')]);
    const changes = reconcileExternalTaskChanges();
    assert.deepEqual(changes, [{ taskId: 7, status: 'pending', subject: 'external work' }]);
    // Reconciled into memory → a second reconcile reports nothing (no re-emit).
    assert.deepEqual(reconcileExternalTaskChanges(), []);
  });

  it('surfaces an external status change (e.g. a teammate claimed it)', () => {
    const dataDir = makeDataDir(); dirs.push(dataDir);
    process.env.SUPERINFERENCE_DATA_DIR = dataDir;
    const cwd = '/repo/u050-status';
    writeExternalTasks(cwd, [extTask(3, 'shared task', 'pending')]);
    setTaskPersistPath(cwd); // loads #3 pending into memory
    assert.deepEqual(reconcileExternalTaskChanges(), []);
    // Teammate process flips it to in_progress on disk.
    writeExternalTasks(cwd, [extTask(3, 'shared task', 'in_progress')]);
    assert.deepEqual(reconcileExternalTaskChanges(), [{ taskId: 3, status: 'in_progress', subject: 'shared task' }]);
  });

  it('surfaces an external delete as a deleted change and drops it from memory', () => {
    const dataDir = makeDataDir(); dirs.push(dataDir);
    process.env.SUPERINFERENCE_DATA_DIR = dataDir;
    const cwd = '/repo/u050-delete';
    writeExternalTasks(cwd, [extTask(9, 'doomed', 'pending')]);
    setTaskPersistPath(cwd);
    // Another process removes it entirely.
    writeExternalTasks(cwd, []);
    assert.deepEqual(reconcileExternalTaskChanges(), [{ taskId: 9, status: 'deleted', subject: 'doomed' }]);
    assert.deepEqual(reconcileExternalTaskChanges(), []);
  });

  it('does NOT re-report this session\'s own mutation (self-write → no spurious change)', async () => {
    const dataDir = makeDataDir(); dirs.push(dataDir);
    process.env.SUPERINFERENCE_DATA_DIR = dataDir;
    const cwd = '/repo/u050-self';
    setTaskPersistPath(cwd);
    // Create a task through the tool — it writes BOTH memory and disk, so a
    // reconcile must find them consistent and report nothing.
    await taskTrackerTool.execute!({ action: 'create', subject: 'mine' }, ctx(cwd));
    assert.deepEqual(reconcileExternalTaskChanges(), []);
  });
});

describe('U050 watchTaskList — fs.watch + debounce', () => {
  const dirs: string[] = [];
  afterEach(() => {
    resetTaskState();
    for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true });
    delete process.env.SUPERINFERENCE_DATA_DIR;
    delete process.env.AMI_TASK_WATCH_DEBOUNCE_MS;
  });

  it('close() is idempotent and safe before any event', () => {
    const dataDir = makeDataDir(); dirs.push(dataDir);
    process.env.SUPERINFERENCE_DATA_DIR = dataDir;
    const cwd = '/repo/u050-close';
    setTaskPersistPath(cwd);
    const w = watchTaskList(cwd, () => { /* noop */ });
    assert.doesNotThrow(() => w.close());
    assert.doesNotThrow(() => w.close()); // second close is a no-op
  });

  it('fires onChange with the external delta after a debounced fs event', async () => {
    const dataDir = makeDataDir(); dirs.push(dataDir);
    process.env.SUPERINFERENCE_DATA_DIR = dataDir;
    process.env.AMI_TASK_WATCH_DEBOUNCE_MS = '15';
    const cwd = '/repo/u050-watch';
    setTaskPersistPath(cwd);
    let received: Array<{ taskId: number; status: string; subject: string }> = [];
    const done = new Promise<void>((resolve) => {
      const w = watchTaskList(cwd, (changes) => {
        received = changes;
        w.close();
        resolve();
      });
      // Simulate a teammate process writing a new task shortly after.
      setTimeout(() => writeExternalTasks(cwd, [extTask(5, 'watched task', 'pending')]), 20);
    });
    await Promise.race([
      done,
      new Promise<void>((_, rej) => setTimeout(() => rej(new Error('watcher did not fire')), 4000)),
    ]);
    assert.deepEqual(received, [{ taskId: 5, status: 'pending', subject: 'watched task' }]);
  });
});
