import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { bashTool } from '../src/tools/bash';
import { taskListTool } from '../src/tools/task-list';
import { taskOutputTool } from '../src/tools/task-output';
import { taskKillTool } from '../src/tools/task-kill';
import { ProcessManager } from '../src/process-manager';
import type { ToolContext } from '../src/types';

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ami-bg-test-'));
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

// ---------------------------------------------------------------------------
// bash tool – run_in_background
// ---------------------------------------------------------------------------

describe('bashTool – run_in_background', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  function ctx(overrides?: Partial<ToolContext>): ToolContext {
    return {
      cwd: tmpDir,
      abortSignal: new AbortController().signal,
      processManager: pm,
      ...overrides,
    };
  }

  afterEach(async () => {
    pm?.cleanup();
    await new Promise(r => setTimeout(r, 100));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns task ID and PID immediately', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const result = await bashTool.execute(
      { command: 'sleep 30', run_in_background: true, description: 'Sleep test' },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('Background task started'));
    assert.ok(result.output.includes('Task ID: bg-'));
    assert.ok(result.output.includes('PID:'));
    assert.ok(result.output.includes('task_output'));
  });

  it('does not block — returns before command finishes', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const start = Date.now();
    const result = await bashTool.execute(
      { command: 'sleep 10', run_in_background: true },
      ctx(),
    );
    const elapsed = Date.now() - start;
    assert.ok(!result.isError);
    assert.ok(elapsed < 2000, `Should return quickly, took ${elapsed}ms`);
  });

  it('uses description from input', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const result = await bashTool.execute(
      { command: 'echo bg-desc', run_in_background: true, description: 'Custom desc' },
      ctx(),
    );
    const idMatch = result.output.match(/Task ID: (bg-\w+)/);
    assert.ok(idMatch);
    const task = pm.get(idMatch![1]);
    assert.equal(task!.description, 'Custom desc');
    await waitForComplete(pm, idMatch![1]);
  });

  it('falls back to command snippet for description', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const result = await bashTool.execute(
      { command: 'echo fallback-snippet', run_in_background: true },
      ctx(),
    );
    const idMatch = result.output.match(/Task ID: (bg-\w+)/);
    assert.ok(idMatch);
    const task = pm.get(idMatch![1]);
    assert.ok(task!.description.includes('echo fallback-snippet'));
    await waitForComplete(pm, idMatch![1]);
  });

  it('runs foreground when run_in_background is false', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const result = await bashTool.execute(
      { command: 'echo foreground', run_in_background: false },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('foreground'));
    assert.ok(!result.output.includes('Background task started'));
  });

  it('runs foreground when processManager is missing', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const result = await bashTool.execute(
      { command: 'echo no-pm', run_in_background: true },
      ctx({ processManager: undefined }),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('no-pm'));
    assert.ok(!result.output.includes('Background task started'));
  });

  it('still blocks git commit even with run_in_background', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const result = await bashTool.execute(
      { command: 'git commit -m "test"', run_in_background: true },
      ctx(),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('git_commit tool'));
  });

  it('still blocks self-kill even with run_in_background', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const result = await bashTool.execute(
      { command: `kill ${process.pid}`, run_in_background: true },
      ctx(),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('AMI itself'));
  });
});

// ---------------------------------------------------------------------------
// task_list tool
// ---------------------------------------------------------------------------

describe('taskListTool', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  function ctx(): ToolContext {
    return { cwd: tmpDir, abortSignal: new AbortController().signal, processManager: pm };
  }

  afterEach(async () => {
    pm?.cleanup();
    await new Promise(r => setTimeout(r, 100));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns "No background tasks" when empty', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const result = await taskListTool.execute({}, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('No background tasks'));
  });

  it('lists running tasks', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    pm.spawn('sleep 60', { cwd: tmpDir, description: 'Long sleep' });
    const result = await taskListTool.execute({}, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('Background tasks (1)'));
    assert.ok(result.output.includes('running'));
    assert.ok(result.output.includes('Long sleep'));
  });

  it('errors when processManager is unavailable', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const result = await taskListTool.execute({}, { cwd: tmpDir, abortSignal: new AbortController().signal });
    assert.ok(result.isError);
    assert.ok(result.output.includes('not available'));
  });
});

// ---------------------------------------------------------------------------
// task_output tool
// ---------------------------------------------------------------------------

describe('taskOutputTool', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  function ctx(): ToolContext {
    return { cwd: tmpDir, abortSignal: new AbortController().signal, processManager: pm };
  }

  afterEach(async () => {
    pm?.cleanup();
    await new Promise(r => setTimeout(r, 100));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns task status and output', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo task-output-test', { cwd: tmpDir });
    await waitForComplete(pm, id);
    const result = await taskOutputTool.execute({ task_id: id }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('Status: completed'));
    assert.ok(result.output.includes('Exit code: 0'));
    assert.ok(result.output.includes('task-output-test'));
  });

  it('errors for unknown task ID', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const result = await taskOutputTool.execute({ task_id: 'bg-nope' }, ctx());
    assert.ok(result.isError);
    assert.ok(result.output.includes('No background task found'));
  });

  it('errors when processManager is unavailable', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const result = await taskOutputTool.execute({ task_id: 'bg-x' }, { cwd: tmpDir, abortSignal: new AbortController().signal });
    assert.ok(result.isError);
    assert.ok(result.output.includes('not available'));
  });
});

// ---------------------------------------------------------------------------
// task_kill tool
// ---------------------------------------------------------------------------

describe('taskKillTool', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  function ctx(): ToolContext {
    return { cwd: tmpDir, abortSignal: new AbortController().signal, processManager: pm };
  }

  afterEach(async () => {
    pm?.cleanup();
    await new Promise(r => setTimeout(r, 100));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('kills a running task', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('sleep 60', { cwd: tmpDir });
    await new Promise(r => setTimeout(r, 100));
    const result = await taskKillTool.execute({ task_id: id }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('killed successfully'));
  });

  it('reports already-completed task', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo done', { cwd: tmpDir });
    await waitForComplete(pm, id);
    const result = await taskKillTool.execute({ task_id: id }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('already completed'));
  });

  it('errors for unknown task ID', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const result = await taskKillTool.execute({ task_id: 'bg-nope' }, ctx());
    assert.ok(result.isError);
    assert.ok(result.output.includes('No background task found'));
  });

  it('errors when processManager is unavailable', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const result = await taskKillTool.execute({ task_id: 'bg-x' }, { cwd: tmpDir, abortSignal: new AbortController().signal });
    assert.ok(result.isError);
    assert.ok(result.output.includes('not available'));
  });
});
