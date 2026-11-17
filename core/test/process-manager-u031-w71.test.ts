import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  ProcessManager,
  TASK_STATUS,
  isActiveTaskStatus,
  isTerminalTaskStatus,
  isBackgroundTask,
} from '../src/process-manager';

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-u031-w71-'));
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

function registerStub(pm: ProcessManager, taskId: string, opts: Partial<{
  toolUseId: string; pending: boolean; agentId: string;
}> = {}): void {
  pm.registerAgent(taskId, {
    command: `[agent] ${taskId}`,
    description: `agent ${taskId}`,
    outputPath: '',
    abortFn: () => { /* no child */ },
    ...(opts.toolUseId ? { toolUseId: opts.toolUseId } : {}),
    ...(opts.agentId ? { agentId: opts.agentId } : {}),
    ...(opts.pending ? { pending: true } : {}),
  });
}

// ---------------------------------------------------------------------------
// Feature 1 — GAP: `pending` (accepted-but-not-started) task status.
//   Claude's TaskStatus is pending|running|completed|failed|killed; a task is
//   created 'pending' and only transitions to 'running' when it actually starts,
//   so a task queued behind the concurrency cap is distinguishable from an
//   executing one. AMI's enum had no pending state.
// ---------------------------------------------------------------------------

describe('ProcessManager — `pending` task status (GAP: no queued-not-started state)', () => {
  let tmp: string | null = null;
  afterEach(() => {
    if (tmp) { try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ } tmp = null; }
  });

  it('includes `pending` in the exported TASK_STATUS enum', () => {
    assert.ok(TASK_STATUS.includes('pending'), 'TASK_STATUS must contain pending');
    assert.deepEqual([...TASK_STATUS], ['pending', 'running', 'completed', 'failed', 'killed']);
  });

  it('isActiveTaskStatus treats running AND pending as active, terminal as inactive', () => {
    assert.equal(isActiveTaskStatus('running'), true);
    assert.equal(isActiveTaskStatus('pending'), true);
    assert.equal(isActiveTaskStatus('completed'), false);
    assert.equal(isActiveTaskStatus('failed'), false);
    assert.equal(isActiveTaskStatus('killed'), false);
    assert.equal(isActiveTaskStatus(undefined), false);
    assert.equal(isActiveTaskStatus('bogus'), false);
  });

  it('isTerminalTaskStatus does NOT treat pending as terminal', () => {
    assert.equal(isTerminalTaskStatus('pending'), false);
    assert.equal(isTerminalTaskStatus('running'), false);
    assert.equal(isTerminalTaskStatus('completed'), true);
  });

  it('registerAgent({pending}) registers in the pending state', () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerStub(pm, 'a-pending', { pending: true });
    assert.equal(pm.getStatus('a-pending'), 'pending');
    // default (no pending flag) is still running
    registerStub(pm, 'a-running');
    assert.equal(pm.getStatus('a-running'), 'running');
  });

  it('markRunning transitions pending -> running and resets the clock to start time', async () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerStub(pm, 'a1', { pending: true });
    // elapsed should grow while pending (clock runs from registration)
    await new Promise((r) => setTimeout(r, 30));
    const before = pm.getOutput('a1')!.elapsedMs;
    assert.ok(before >= 20, `pending task should accrue wall-clock, got ${before}`);
    assert.equal(pm.markRunning('a1'), true);
    assert.equal(pm.getStatus('a1'), 'running');
    // startTime reset => elapsed drops back near zero
    const after = pm.getOutput('a1')!.elapsedMs;
    assert.ok(after < before, `markRunning should reset the clock (before=${before}, after=${after})`);
  });

  it('markRunning is a no-op for a non-pending or unknown task', () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerStub(pm, 'run', {}); // already running
    assert.equal(pm.markRunning('run'), false);
    assert.equal(pm.markRunning('nope'), false);
  });

  it('activeCount counts BOTH running and pending (unlike runningCount)', () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerStub(pm, 'r1', {});
    registerStub(pm, 'p1', { pending: true });
    registerStub(pm, 'p2', { pending: true });
    assert.equal(pm.runningCount(), 1, 'runningCount counts only running');
    assert.equal(pm.activeCount(), 3, 'activeCount counts running + pending');
    // a terminal task leaves the active set
    pm.updateEntry('r1', { status: 'completed' });
    assert.equal(pm.activeCount(), 2);
  });
});

// ---------------------------------------------------------------------------
// Feature 2 — GAP: per-task isBackgrounded flag + isBackgroundTask predicate +
//   single-slot foreground/re-background (foregroundedTaskId).
//   Claude's isBackgroundTask admits a task only when it is active (running|
//   pending) AND not explicitly foregrounded (isBackgrounded === false); Ctrl+B
//   re-backgrounds a single foregrounded task. AMI had no per-task flag.
// ---------------------------------------------------------------------------

describe('ProcessManager — per-task foreground/background (GAP: no per-task isBackgrounded)', () => {
  let tmp: string | null = null;
  afterEach(() => {
    if (tmp) { try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ } tmp = null; }
  });

  it('isBackgroundTask: active + not-foregrounded counts; foregrounded or terminal does not', () => {
    assert.equal(isBackgroundTask({ status: 'running' }), true);
    assert.equal(isBackgroundTask({ status: 'pending' }), true);
    assert.equal(isBackgroundTask({ status: 'running', isBackgrounded: true }), true);
    assert.equal(isBackgroundTask({ status: 'running', isBackgrounded: false }), false);
    assert.equal(isBackgroundTask({ status: 'completed' }), false);
    assert.equal(isBackgroundTask({ status: 'completed', isBackgrounded: true }), false);
  });

  it('foregroundTask excludes a task from the background indicator; backgroundTask re-admits it', () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerStub(pm, 't1', {});
    registerStub(pm, 't2', {});
    assert.equal(pm.backgroundTaskCount(), 2);
    assert.deepEqual(pm.listBackgroundTasks().map((t) => t.taskId).sort(), ['t1', 't2']);

    assert.equal(pm.foregroundTask('t1'), true);
    assert.equal(pm.getForegroundedTaskId(), 't1');
    assert.equal(pm.backgroundTaskCount(), 1, 't1 is foregrounded -> excluded');
    assert.deepEqual(pm.listBackgroundTasks().map((t) => t.taskId), ['t2']);
    assert.equal(pm.get('t1')!.isBackgrounded, false);

    assert.equal(pm.backgroundTask('t1'), true);
    assert.equal(pm.getForegroundedTaskId(), undefined, 'foreground slot cleared');
    assert.equal(pm.get('t1')!.isBackgrounded, true);
    assert.equal(pm.backgroundTaskCount(), 2);
  });

  it('only one task is foregrounded at a time — foregrounding a second re-backgrounds the first', () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerStub(pm, 't1', {});
    registerStub(pm, 't2', {});
    pm.foregroundTask('t1');
    pm.foregroundTask('t2');
    assert.equal(pm.getForegroundedTaskId(), 't2');
    assert.equal(pm.get('t1')!.isBackgrounded, true, 't1 re-backgrounded');
    assert.equal(pm.get('t2')!.isBackgrounded, false);
    assert.deepEqual(pm.listBackgroundTasks().map((t) => t.taskId), ['t1']);
  });

  it('foreground/background are no-ops for an unknown task', () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    assert.equal(pm.foregroundTask('nope'), false);
    assert.equal(pm.backgroundTask('nope'), false);
    assert.equal(pm.getForegroundedTaskId(), undefined);
  });

  it('backgroundTask does NOT clear the foreground slot when a DIFFERENT task is foregrounded', () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerStub(pm, 't1', {});
    registerStub(pm, 't2', {});
    pm.foregroundTask('t1');
    pm.backgroundTask('t2'); // t2 wasn't foregrounded
    assert.equal(pm.getForegroundedTaskId(), 't1', 'slot untouched');
  });

  it('a pending task still counts as a background task until it terminates', () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerStub(pm, 'p1', { pending: true });
    assert.equal(pm.backgroundTaskCount(), 1);
    pm.markRunning('p1');
    assert.equal(pm.backgroundTaskCount(), 1);
    pm.updateEntry('p1', { status: 'completed' });
    assert.equal(pm.backgroundTaskCount(), 0);
  });
});

// ---------------------------------------------------------------------------
// Feature 3 — GAP: toolUseId linking a task back to its originating tool_use.
//   Claude's TaskStateBase carries toolUseId so a background task can be mapped to
//   the tool call that spawned it. AMI had no such field or lookup.
// ---------------------------------------------------------------------------

describe('ProcessManager — toolUseId correlation (GAP: no tool-call linkage)', () => {
  let tmp: string | null = null;
  afterEach(() => {
    if (tmp) { try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ } tmp = null; }
  });

  it('registerAgent records toolUseId and findByToolUseId resolves it', () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    registerStub(pm, 'a1', { toolUseId: 'tu_abc' });
    registerStub(pm, 'a2', {}); // no toolUseId
    const found = pm.findByToolUseId('tu_abc');
    assert.ok(found, 'should find the task by its tool_use id');
    assert.equal(found!.taskId, 'a1');
    assert.equal(found!.toolUseId, 'tu_abc');
    assert.equal(pm.get('a2')!.toolUseId, undefined, 'omitted toolUseId stays undefined');
    assert.equal(pm.findByToolUseId('missing'), null);
    assert.equal(pm.findByToolUseId(''), null, 'empty id never matches');
  });

  it('spawn threads toolUseId onto the tracked task end-to-end', async () => {
    tmp = makeTmpDir();
    const pm = new ProcessManager(tmp);
    const id = pm.spawn('echo hi', { cwd: tmp, description: 'echo', toolUseId: 'tu_spawn' });
    const found = pm.findByToolUseId('tu_spawn');
    assert.ok(found);
    assert.equal(found!.taskId, id);
    // survives in the list() snapshot too
    assert.equal(pm.list().find((t) => t.taskId === id)!.toolUseId, 'tu_spawn');
    await waitFor(() => pm.getStatus(id) !== 'running');
  });
});
