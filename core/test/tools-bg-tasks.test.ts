import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { taskOutputTool } from '../src/tools/task-output';
import { taskKillTool } from '../src/tools/task-kill';
import { taskListTool } from '../src/tools/task-list';
import { ProcessManager } from '../src/process-manager';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Tool definitions
// ---------------------------------------------------------------------------

describe('background task tools – definitions', () => {
  it('taskOutputTool has correct name and is read-only', () => {
    assert.equal(taskOutputTool.name, 'task_output');
    assert.equal(taskOutputTool.isReadOnly, true);
    assert.ok(taskOutputTool.inputSchema.required?.includes('task_id'));
  });

  it('taskKillTool has correct name and is not read-only', () => {
    assert.equal(taskKillTool.name, 'task_kill');
    assert.equal(taskKillTool.isReadOnly, false);
    assert.ok(taskKillTool.inputSchema.required?.includes('task_id'));
  });

  it('taskListTool has correct name and is read-only', () => {
    assert.equal(taskListTool.name, 'task_list');
    assert.equal(taskListTool.isReadOnly, true);
  });
});

// ---------------------------------------------------------------------------
// Missing processManager
// ---------------------------------------------------------------------------

describe('background task tools – no processManager', () => {
  it('task_output errors without processManager', async () => {
    const result = await taskOutputTool.execute({ task_id: 'x' }, ctx());
    assert.ok(result.isError);
    assert.ok(result.output.includes('not available'));
  });

  it('task_kill errors without processManager', async () => {
    const result = await taskKillTool.execute({ task_id: 'x' }, ctx());
    assert.ok(result.isError);
  });

  it('task_list errors without processManager', async () => {
    const result = await taskListTool.execute({}, ctx());
    assert.ok(result.isError);
  });
});

// ---------------------------------------------------------------------------
// With ProcessManager
// ---------------------------------------------------------------------------

describe('background task tools – with ProcessManager', () => {
  let pm: ProcessManager;
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-bgtask-'));
    pm = new ProcessManager(tmpDir);
  });

  it('task_list shows "No background tasks" when empty', async () => {
    const result = await taskListTool.execute({}, ctx({ processManager: pm }));
    assert.ok(!result.isError);
    assert.ok(result.output.includes('No background tasks'));
    pm.cleanup();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('task_list shows spawned tasks', async () => {
    const taskId = pm.spawn('echo hello', { cwd: tmpDir, description: 'test task' });
    await new Promise(r => setTimeout(r, 200));

    const result = await taskListTool.execute({}, ctx({ processManager: pm }));
    assert.ok(result.output.includes(taskId));
    assert.ok(result.output.includes('test task'));
    pm.cleanup();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('task_output returns output for existing task', async () => {
    const taskId = pm.spawn('echo test_output_123', { cwd: tmpDir });
    await new Promise(r => setTimeout(r, 500));

    const result = await taskOutputTool.execute({ task_id: taskId }, ctx({ processManager: pm }));
    assert.ok(!result.isError);
    assert.ok(result.output.includes('test_output_123'));
    assert.ok(result.output.includes('Status:'));
    pm.cleanup();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('task_output errors for non-existent task', async () => {
    const result = await taskOutputTool.execute({ task_id: 'fake-id' }, ctx({ processManager: pm }));
    assert.ok(result.isError);
    assert.ok(result.output.includes('No background task'));
    pm.cleanup();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('task_kill kills a running task', async () => {
    const taskId = pm.spawn('sleep 60', { cwd: tmpDir, description: 'long task' });
    await new Promise(r => setTimeout(r, 100));

    const result = await taskKillTool.execute({ task_id: taskId }, ctx({ processManager: pm }));
    assert.ok(!result.isError);
    assert.ok(result.output.includes('killed successfully'));
    pm.cleanup();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('task_kill reports already completed', async () => {
    // Directly set up a completed task entry to avoid node:test / child_process event loop issues
    const taskId = pm.spawn('sleep 60', { cwd: tmpDir });
    // Manually transition the process to completed via internal map
    const internal = (pm as any).processes.get(taskId);
    if (internal) {
      try { internal.proc.kill('SIGKILL'); } catch { /* ignore */ }
      internal.status = 'completed';
      internal.exitCode = 0;
    }

    const result = await taskKillTool.execute({ task_id: taskId }, ctx({ processManager: pm }));
    assert.ok(!result.isError);
    assert.ok(result.output.includes('already'));
    pm.cleanup();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('task_kill errors for non-existent task', async () => {
    const result = await taskKillTool.execute({ task_id: 'nope' }, ctx({ processManager: pm }));
    assert.ok(result.isError);
    pm.cleanup();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('task_output shows elapsed time formatting', async () => {
    const taskId = pm.spawn('echo x', { cwd: tmpDir });
    await new Promise(r => setTimeout(r, 200));

    const result = await taskOutputTool.execute({ task_id: taskId, tail: 10 }, ctx({ processManager: pm }));
    assert.ok(result.output.includes('Elapsed:'));
    pm.cleanup();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});
