import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { ProcessManager } from '../src/process-manager';

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-test-'));
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
  // Wait for killed processes' async close events to drain
  await new Promise(r => setTimeout(r, 100));
}

// ---------------------------------------------------------------------------
// Spawn and list
// ---------------------------------------------------------------------------

describe('ProcessManager – spawn', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns a task ID starting with bg-', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo hello', { cwd: tmpDir });
    assert.ok(id.startsWith('bg-'));
    await waitForComplete(pm, id);
  });

  it('task appears in list() as running', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('sleep 30', { cwd: tmpDir });
    const tasks = pm.list();
    assert.equal(tasks.length, 1);
    assert.equal(tasks[0].taskId, id);
    assert.equal(tasks[0].status, 'running');
  });

  it('get() returns the task by ID', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo hi', { cwd: tmpDir });
    const task = pm.get(id);
    assert.ok(task);
    assert.equal(task!.taskId, id);
    assert.ok(task!.pid > 0);
    await waitForComplete(pm, id);
  });

  it('get() returns null for unknown task ID', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    assert.equal(pm.get('bg-nonexistent'), null);
  });

  it('uses description when provided', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo test', { cwd: tmpDir, description: 'My task' });
    const task = pm.get(id);
    assert.equal(task!.description, 'My task');
    await waitForComplete(pm, id);
  });

  it('falls back to truncated command for description', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo default-desc', { cwd: tmpDir });
    const task = pm.get(id);
    assert.ok(task!.description.includes('echo default-desc'));
    await waitForComplete(pm, id);
  });
});

// ---------------------------------------------------------------------------
// Completion and events
// ---------------------------------------------------------------------------

describe('ProcessManager – completion', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('task transitions to completed on exit 0', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo done', { cwd: tmpDir });
    await waitForComplete(pm, id);
    const task = pm.get(id);
    assert.equal(task!.status, 'completed');
    assert.equal(task!.exitCode, 0);
  });

  it('task transitions to failed on non-zero exit', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('exit 42', { cwd: tmpDir });
    await waitForComplete(pm, id);
    const task = pm.get(id);
    assert.equal(task!.status, 'failed');
    assert.equal(task!.exitCode, 42);
  });

  it('emits complete event with taskId, exitCode, command, description', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo event-test', { cwd: tmpDir, description: 'Event test' });
    const event = await waitForComplete(pm, id);
    assert.equal(event.taskId, id);
    assert.equal(event.exitCode, 0);
    assert.ok(event.command.includes('echo event-test'));
    assert.equal(event.description, 'Event test');
  });
});

// ---------------------------------------------------------------------------
// Output capture
// ---------------------------------------------------------------------------

describe('ProcessManager – getOutput', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('captures stdout to disk and getOutput reads it', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo captured-output', { cwd: tmpDir });
    await waitForComplete(pm, id);
    const result = pm.getOutput(id);
    assert.ok(result);
    assert.ok(result!.output.includes('captured-output'));
    assert.equal(result!.status, 'completed');
    assert.equal(result!.exitCode, 0);
  });

  it('captures stderr to disk', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo stderr-msg >&2', { cwd: tmpDir });
    await waitForComplete(pm, id);
    const result = pm.getOutput(id);
    assert.ok(result!.output.includes('stderr-msg'));
  });

  it('returns null for unknown task ID', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    assert.equal(pm.getOutput('bg-nope'), null);
  });

  it('respects tailLines parameter', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('for i in $(seq 1 20); do echo "line$i"; done', { cwd: tmpDir });
    await waitForComplete(pm, id);
    const result = pm.getOutput(id, 5);
    assert.ok(result);
    const lines = result!.output.split('\n').filter(l => l.length > 0);
    assert.ok(lines.length <= 6, `Expected at most 6 non-empty lines, got ${lines.length}`);
    assert.ok(result!.output.includes('line20'));
  });

  it('includes elapsedMs in output', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo fast', { cwd: tmpDir });
    await waitForComplete(pm, id);
    const result = pm.getOutput(id);
    assert.ok(result!.elapsedMs >= 0);
  });
});

// ---------------------------------------------------------------------------
// Kill
// ---------------------------------------------------------------------------

describe('ProcessManager – kill', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('kills a running process and sets status to killed', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('sleep 60', { cwd: tmpDir });
    await new Promise(r => setTimeout(r, 100));
    const killed = pm.kill(id);
    assert.ok(killed);
    const task = pm.get(id);
    assert.equal(task!.status, 'killed');
  });

  it('returns false for unknown task ID', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    assert.equal(pm.kill('bg-unknown'), false);
  });

  it('returns false for already completed task', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo fast', { cwd: tmpDir });
    await waitForComplete(pm, id);
    assert.equal(pm.kill(id), false);
  });
});

// ---------------------------------------------------------------------------
// Running count
// ---------------------------------------------------------------------------

describe('ProcessManager – runningCount', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns 0 when no tasks', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    assert.equal(pm.runningCount(), 0);
  });

  it('counts only running tasks', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    pm.spawn('sleep 60', { cwd: tmpDir });
    pm.spawn('sleep 60', { cwd: tmpDir });
    const fastId = pm.spawn('echo fast', { cwd: tmpDir });
    await waitForComplete(pm, fastId);
    assert.equal(pm.runningCount(), 2);
  });
});

// ---------------------------------------------------------------------------
// Cleanup
// ---------------------------------------------------------------------------

describe('ProcessManager – cleanup', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  afterEach(async () => { await new Promise(r => setTimeout(r, 100)); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('kills all running processes', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id1 = pm.spawn('sleep 60', { cwd: tmpDir });
    const id2 = pm.spawn('sleep 60', { cwd: tmpDir });
    await new Promise(r => setTimeout(r, 100));
    pm.cleanup();
    assert.equal(pm.get(id1)!.status, 'killed');
    assert.equal(pm.get(id2)!.status, 'killed');
    assert.equal(pm.runningCount(), 0);
  });
});

// ---------------------------------------------------------------------------
// Tasks directory
// ---------------------------------------------------------------------------

describe('ProcessManager – tasks directory', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('creates .superinference/tasks directory', () => {
    tmpDir = makeTmpDir();
    const subDir = path.join(tmpDir, 'subproject');
    fs.mkdirSync(subDir, { recursive: true });
    pm = new ProcessManager(subDir);
    assert.ok(fs.existsSync(path.join(subDir, '.superinference', 'tasks')));
  });

  it('output file exists on disk after spawn', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo disk-test', { cwd: tmpDir });
    const task = pm.get(id);
    await waitForComplete(pm, id);
    assert.ok(fs.existsSync(task!.outputPath));
    const content = fs.readFileSync(task!.outputPath, 'utf-8');
    assert.ok(content.includes('disk-test'));
  });
});

// ---------------------------------------------------------------------------
// list() does not leak internal fields
// ---------------------------------------------------------------------------

describe('ProcessManager – list() shape', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('does not expose proc, outputFd, or bytesWritten', async () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo shape-test', { cwd: tmpDir });
    const tasks = pm.list();
    const task = tasks[0] as any;
    assert.equal(task.proc, undefined);
    assert.equal(task.outputFd, undefined);
    assert.equal(task.bytesWritten, undefined);
    await waitForComplete(pm, id);
  });
});

// ---------------------------------------------------------------------------
// monitorMcp — cleanup clears interval
// ---------------------------------------------------------------------------

describe('ProcessManager – monitorMcp', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  afterEach(async () => { await cleanupPm(pm); fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('monitorMcp creates a running entry', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.monitorMcp('test-server', async () => true, 60000);
    assert.ok(id.startsWith('monitor-'));
    const task = pm.get(id);
    assert.ok(task);
    assert.equal(task!.status, 'running');
  });

  it('kill() clears monitor interval without crashing', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    const id = pm.monitorMcp('test-server', async () => true, 60000);
    const killed = pm.kill(id);
    assert.ok(killed);
    const task = pm.get(id);
    assert.equal(task!.status, 'killed');
  });

  it('cleanup() clears monitor intervals', () => {
    tmpDir = makeTmpDir();
    pm = new ProcessManager(tmpDir);
    pm.monitorMcp('srv1', async () => true, 60000);
    pm.monitorMcp('srv2', async () => true, 60000);
    pm.cleanup();
    assert.equal(pm.runningCount(), 0);
  });
});
