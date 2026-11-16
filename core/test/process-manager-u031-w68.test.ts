import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  ProcessManager,
  buildCompletionSummary,
  formatByteCap,
  formatBackgroundCompletionNote,
  type CompletedTaskEvent,
} from '../src/process-manager';
import { taskListTool } from '../src/tools/task-list';

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-u031-w68-'));
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

// ---------------------------------------------------------------------------
// buildCompletionSummary — status-aware outcome phrasing (gap: summary on event)
// ---------------------------------------------------------------------------

describe('buildCompletionSummary', () => {
  it('phrases completed with and without an exit code', () => {
    assert.equal(buildCompletionSummary('completed', 0), 'completed (exit code: 0)');
    assert.equal(buildCompletionSummary('completed', null), 'completed');
    assert.equal(buildCompletionSummary('completed', undefined), 'completed');
  });

  it('phrases failed with and without an exit code', () => {
    assert.equal(buildCompletionSummary('failed', 1), 'failed with exit code 1');
    assert.equal(buildCompletionSummary('failed', null), 'failed');
  });

  it('phrases killed as a deliberate stop with no exit code', () => {
    assert.equal(buildCompletionSummary('killed', 137), 'was stopped');
    assert.equal(buildCompletionSummary('killed', null), 'was stopped');
  });

  it('falls back to completed wording for a legacy/undefined status', () => {
    assert.equal(buildCompletionSummary(undefined, 0), 'completed (exit code: 0)');
  });
});

describe('formatBackgroundCompletionNote', () => {
  it('prefers the pre-computed event.summary over re-deriving it', () => {
    const event: CompletedTaskEvent = {
      taskId: 'bg-1', exitCode: 1, command: 'x', description: 'x',
      outputPath: '', status: 'failed', summary: 'CUSTOM SUMMARY',
    };
    assert.ok(formatBackgroundCompletionNote(event).includes('CUSTOM SUMMARY'));
  });

  it('falls back to buildCompletionSummary when no summary is attached', () => {
    const event: CompletedTaskEvent = {
      taskId: 'bg-2', exitCode: 2, command: 'x', description: 'x',
      outputPath: '', status: 'failed',
    };
    assert.ok(formatBackgroundCompletionNote(event).includes('failed with exit code 2'));
  });
});

// ---------------------------------------------------------------------------
// formatByteCap — human-readable cap label (gap: parameterized cap display)
// ---------------------------------------------------------------------------

describe('formatByteCap', () => {
  it('renders MB / KB / raw bytes by magnitude', () => {
    assert.equal(formatByteCap(5 * 1024 * 1024), '5MB');
    assert.equal(formatByteCap(2 * 1024), '2KB');
    assert.equal(formatByteCap(20), '20B');
  });
});

// ---------------------------------------------------------------------------
// emitComplete attaches the summary to drained/buffered completions
// ---------------------------------------------------------------------------

describe('ProcessManager – completion summary on buffered events', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('sets a human-readable summary on the buffered completion', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo hi', { cwd: tmpDir });
    await waitForComplete(pm, id);
    const drained = pm.drainCompletedTasks();
    const event = drained.find(e => e.taskId === id);
    assert.ok(event, 'completion buffered');
    assert.equal(event!.status, 'completed');
    assert.equal(event!.summary, 'completed (exit code: 0)');
  });
});

// ---------------------------------------------------------------------------
// maxOutputBytes + runaway-output watchdog (gaps 71 + 78)
// ---------------------------------------------------------------------------

describe('ProcessManager – runaway output watchdog', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('KILLS a producer that exceeds the configurable maxOutputBytes cap', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    // Tiny cap so the very first lines of output trip the watchdog quickly.
    pm.maxOutputBytes = 15;
    const killedEvents: any[] = [];
    pm.on('killed', (e: any) => killedEvents.push(e));
    const id = pm.spawn('for i in $(seq 1 500); do echo AAAAAAAAAA; done', { cwd: tmpDir });
    const event = await waitForComplete(pm, id);
    // The task was stopped by the watchdog, not left running / completed.
    assert.equal(event.status, 'killed');
    const info = pm.getOutput(id);
    assert.equal(info!.status, 'killed');
    // A 'killed' introspection event fired with the cap-breach reason.
    const killed = killedEvents.find(e => e.taskId === id);
    assert.ok(killed, 'killed event emitted');
    assert.match(killed.reason, /output exceeded 15B cap/);
    // The diagnostic note was written into the output file.
    assert.match(info!.output, /output exceeded 15B cap — task killed/);
  });

  it('does not kill a producer that stays under the cap', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    pm.maxOutputBytes = 5 * 1024 * 1024;
    const id = pm.spawn('echo small', { cwd: tmpDir });
    const event = await waitForComplete(pm, id);
    assert.equal(event.status, 'completed');
  });
});

// ---------------------------------------------------------------------------
// isActive — composite liveness (gaps 72 + 73)
// ---------------------------------------------------------------------------

describe('ProcessManager – isActive composite liveness', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('is true for a running registered agent with a live abort signal', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const ac = new AbortController();
    pm.registerAgent('a-live', {
      command: '[agent] x', description: 'x', outputPath: path.join(tmpDir, 'a.out'),
      abortFn: () => ac.abort(), abortSignal: ac.signal,
    });
    assert.equal(pm.isActive('a-live'), true);
  });

  it('is false for a running registered agent whose signal was aborted', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const ac = new AbortController();
    pm.registerAgent('a-abort', {
      command: '[agent] x', description: 'x', outputPath: path.join(tmpDir, 'a.out'),
      abortFn: () => ac.abort(), abortSignal: ac.signal,
    });
    ac.abort();
    // status is still 'running' (not yet flipped) but liveness reflects the abort.
    assert.equal(pm.getStatus('a-abort'), 'running');
    assert.equal(pm.isActive('a-abort'), false);
  });

  it('is false for a registered agent with no stored signal (defaults inactive)', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    pm.registerAgent('a-nosig', {
      command: '[agent] x', description: 'x', outputPath: path.join(tmpDir, 'a.out'),
      abortFn: () => {},
    });
    assert.equal(pm.isActive('a-nosig'), false);
  });

  it('is true for a running bash task and false once terminal / for unknown ids', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('sleep 30', { cwd: tmpDir });
    assert.equal(pm.isActive(id), true);
    assert.equal(pm.isActive('nope'), false);
    pm.kill(id);
    assert.equal(pm.isActive(id), false);
  });
});

// ---------------------------------------------------------------------------
// error field (gap 30) + task_list surfacing of error + aborting
// ---------------------------------------------------------------------------

describe('ProcessManager – failed-agent error field', () => {
  let tmpDir: string;
  let pm: ProcessManager;
  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('stores and exposes an error message for a failed agent', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    pm.registerAgent('a-fail', {
      command: '[agent] builder', description: 'builder', outputPath: path.join(tmpDir, 'a.out'),
      abortFn: () => {},
    });
    pm.updateEntry('a-fail', { status: 'failed', exitCode: 1, error: 'boom: compile error' });
    const snap = pm.get('a-fail');
    assert.equal(snap!.status, 'failed');
    assert.equal(snap!.error, 'boom: compile error');
  });

  it('task_list surfaces the error for a failed agent and [aborting] for an aborting one', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    // a failed agent with an error
    pm.registerAgent('a-fail', {
      command: '[agent] builder', description: 'builder', outputPath: path.join(tmpDir, 'f.out'),
      abortFn: () => {},
    });
    pm.updateEntry('a-fail', { status: 'failed', exitCode: 1, error: 'boom: compile error' });
    // a running agent whose signal was aborted (aborting window)
    const ac = new AbortController();
    pm.registerAgent('a-aborting', {
      command: '[agent] worker', description: 'worker', outputPath: path.join(tmpDir, 'w.out'),
      abortFn: () => ac.abort(), abortSignal: ac.signal,
    });
    ac.abort();

    const result = await taskListTool.execute({}, { processManager: pm, cwd: tmpDir } as any);
    assert.equal(result.isError, false);
    assert.match(result.output, /error: boom: compile error/);
    assert.match(result.output, /\[aborting\]/);
  });
});
