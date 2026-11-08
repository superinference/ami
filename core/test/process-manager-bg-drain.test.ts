import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { ProcessManager } from '../src/process-manager';

// B4: a background task completion must be delivered durably — even if no
// 'complete' listener is attached when it fires (engine mid-API-call) or the task
// was the last running one. drainCompletedTasks() buffers every completion and
// returns each exactly once.
describe('ProcessManager.drainCompletedTasks (B4)', () => {
  let pm: ProcessManager;
  let tmp: string;
  beforeEach(() => { tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'si-pm-drain-')); pm = new ProcessManager(tmp); });
  afterEach(() => { fs.rmSync(tmp, { recursive: true, force: true }); });

  async function waitDone(id: string, timeoutMs = 5000): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const s = pm.getOutput(id)?.status;
      if (s === 'completed' || s === 'failed') return;
      await new Promise(r => setTimeout(r, 20));
    }
    throw new Error(`task ${id} did not finish in time`);
  }

  it('surfaces a completion that fired with NO listener attached', async () => {
    const id = pm.spawn('echo done', { cwd: tmp, description: 'echo test' });
    await waitDone(id); // we never attached a 'complete' listener

    const drained = pm.drainCompletedTasks();
    assert.ok(drained.some(e => e.taskId === id), 'completion must be buffered and drained');
    const ev = drained.find(e => e.taskId === id)!;
    assert.equal(ev.exitCode, 0);
    assert.equal(ev.command, 'echo done');
  });

  it('reports each completion exactly once (second drain is empty)', async () => {
    const id = pm.spawn('echo once', { cwd: tmp });
    await waitDone(id);
    const first = pm.drainCompletedTasks();
    assert.ok(first.some(e => e.taskId === id));
    const second = pm.drainCompletedTasks();
    assert.deepEqual(second, []);
  });

  it('a fresh manager has nothing to drain', () => {
    assert.deepEqual(pm.drainCompletedTasks(), []);
  });
});
