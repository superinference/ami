import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { ProcessManager } from '../src/process-manager';

// ---------------------------------------------------------------------------
// U001 gap (parity: the reference's clearConversation task teardown,
// commands/clear/conversation.ts:L146-166): on a conversation CLEAR, every
// non-preserved RUNNING task is killed + its on-disk output evicted, so a wiped
// conversation does not leave orphaned background shells / agents / monitors
// executing. Explicitly preserved background agents survive.
// Red-green: reverting ProcessManager.killForClear fails these.
// ---------------------------------------------------------------------------

const tmpDirs: string[] = [];
function makeTmpDir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-clear-'));
  tmpDirs.push(d);
  return d;
}
function makePm(): ProcessManager {
  return new ProcessManager(makeTmpDir());
}
function registerAgent(pm: ProcessManager, id: string, description: string): void {
  pm.registerAgent(id, {
    command: `[agent] ${description}`,
    description,
    outputPath: path.join(makeTmpDir(), `${id}.out`),
    abortFn: () => { /* no-op abort */ },
  });
}

after(() => {
  for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best-effort */ } }
});

describe('ProcessManager.killForClear', () => {
  it('kills every non-preserved running task and drops it from the table', async () => {
    const pm = makePm();
    registerAgent(pm, 'a1', 'orphan agent');
    const bashId = pm.spawn('sleep 5', { cwd: makeTmpDir() });
    const monId = pm.monitorMcp('srv', async () => true, 60_000);

    const stopped = pm.killForClear(new Set());
    const stoppedIds = stopped.map((s) => s.taskId).sort();
    assert.deepEqual(stoppedIds, ['a1', bashId, monId].sort());
    // All three are gone from the tracked table (output evicted via removeTask).
    assert.equal(pm.get('a1'), null);
    assert.equal(pm.get(bashId), null);
    assert.equal(pm.get(monId), null);

    pm.cleanup();
    await new Promise((r) => setTimeout(r, 50));
  });

  it('preserves the running tasks whose id is in the preserved set', async () => {
    const pm = makePm();
    registerAgent(pm, 'keep', 'alive teammate');
    registerAgent(pm, 'drop', 'orphan teammate');

    const stopped = pm.killForClear(new Set(['keep']));
    assert.deepEqual(stopped.map((s) => s.taskId), ['drop']);
    assert.equal(pm.getStatus('keep'), 'running');
    assert.equal(pm.get('drop'), null);

    pm.cleanup();
    await new Promise((r) => setTimeout(r, 50));
  });

  it('is a no-op (empty result) when no task is running', () => {
    const pm = makePm();
    assert.deepEqual(pm.killForClear(new Set()), []);
    pm.cleanup();
  });

  it('skips a terminal task and never resurrects it', () => {
    const pm = makePm();
    registerAgent(pm, 'a1', 'work');
    pm.kill('a1');
    assert.equal(pm.getStatus('a1'), 'killed');
    // A clear after a kill finds nothing running to tear down.
    assert.deepEqual(pm.killForClear(new Set()), []);
    pm.cleanup();
  });

  it('error-boundaries a failing per-task teardown and still clears the rest', async () => {
    const pm = makePm();
    registerAgent(pm, 'a1', 'first');
    registerAgent(pm, 'a2', 'second');
    // Make the FIRST kill throw to prove one failing teardown does not abort the
    // whole clear (parity: the reference's per-task try/catch + logError).
    const realKill = pm.kill.bind(pm);
    let calls = 0;
    (pm as unknown as { kill: ProcessManager['kill'] }).kill = ((id: string, opts?: { suppressCompletion?: boolean; reason?: string }) => {
      calls++;
      if (calls === 1) throw new Error('boom');
      return realKill(id, opts);
    }) as ProcessManager['kill'];

    assert.doesNotThrow(() => pm.killForClear(new Set()));
    // The second task was still torn down despite the first throwing.
    assert.equal(calls, 2);

    (pm as unknown as { kill: ProcessManager['kill'] }).kill = realKill;
    pm.cleanup();
    await new Promise((r) => setTimeout(r, 50));
  });
});
