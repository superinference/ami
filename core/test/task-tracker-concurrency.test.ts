import { describe, it, beforeEach, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  taskTrackerTool,
  resetTaskState,
  setTaskPersistPath,
} from '../src/tools/task-tracker';
import type { ToolContext } from '../src/types';

const ctx = {} as unknown as ToolContext;
const tmpDirs: string[] = [];
function mkTmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'tt-conc-'));
  tmpDirs.push(d);
  return d;
}

describe('task-tracker concurrency + claim semantics', () => {
  beforeEach(() => resetTaskState());
  after(() => {
    for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
  });

  it('serializes concurrent creates under the file lock (no lost update, no id reuse)', async () => {
    setTaskPersistPath(mkTmp());
    const results = await Promise.all(
      Array.from({ length: 15 }, (_, i) =>
        taskTrackerTool.execute({ action: 'create', subject: `t${i}` }, ctx),
      ),
    );
    const ids = results.map(r => {
      const m = r.output.match(/Task #(\d+)/);
      return m ? Number(m[1]) : -1;
    });
    const unique = new Set(ids);
    assert.equal(unique.size, 15, 'every concurrent create got a distinct id');
    assert.equal(Math.max(...ids), 15);
    assert.equal(Math.min(...ids), 1);
  });

  it('rejects unknown input keys (strict schema)', async () => {
    const r = await taskTrackerTool.execute({ action: 'list', taskId: '1' } as Record<string, unknown>, ctx);
    assert.equal(r.isError, true);
    assert.ok(r.output.includes('unknown input key'));
    assert.ok(r.output.includes('taskId'));
  });

  it('claim surfaces structured failure reasons', async () => {
    setTaskPersistPath(mkTmp());
    await taskTrackerTool.execute({ action: 'create', subject: 'work' }, ctx);
    // not found
    const nf = await taskTrackerTool.execute({ action: 'claim', task_id: '99' }, ctx);
    assert.ok(nf.isError && nf.output.includes('reason: task_not_found'));
    // claim once
    const ok = await taskTrackerTool.execute({ action: 'claim', task_id: '1', owner: 'alice' }, ctx);
    assert.equal(ok.isError, false);
    assert.ok(ok.output.includes('[in_progress]'));
    // already claimed
    const again = await taskTrackerTool.execute({ action: 'claim', task_id: '1', owner: 'bob' }, ctx);
    assert.ok(again.isError && again.output.includes('reason: already_claimed'));
  });

  it('agent-busy guard refuses a second open task for the same owner', async () => {
    setTaskPersistPath(mkTmp());
    await taskTrackerTool.execute({ action: 'create', subject: 'a' }, ctx);
    await taskTrackerTool.execute({ action: 'create', subject: 'b' }, ctx);
    const c1 = await taskTrackerTool.execute({ action: 'claim', task_id: '1', owner: 'alice', checkAgentBusy: true }, ctx);
    assert.equal(c1.isError, false);
    const c2 = await taskTrackerTool.execute({ action: 'claim', task_id: '2', owner: 'alice', checkAgentBusy: true }, ctx);
    assert.ok(c2.isError && c2.output.includes('reason: agent_busy'));
    assert.ok(c2.output.includes('#1'), 'names the task the agent is busy with');
    // Without the busy flag, the same second claim is allowed.
    const c3 = await taskTrackerTool.execute({ action: 'claim', task_id: '2', owner: 'alice' }, ctx);
    assert.equal(c3.isError, false);
  });

  it('claim blocked by an unresolved dependency reports reason: blocked', async () => {
    setTaskPersistPath(mkTmp());
    await taskTrackerTool.execute({ action: 'create', subject: 'dep' }, ctx);
    await taskTrackerTool.execute({ action: 'create', subject: 'main' }, ctx);
    await taskTrackerTool.execute({ action: 'update', task_id: '2', addBlockedBy: ['1'] }, ctx);
    const r = await taskTrackerTool.execute({ action: 'claim', task_id: '2', owner: 'alice' }, ctx);
    assert.ok(r.isError && r.output.includes('reason: blocked'));
    assert.ok(r.output.includes('#1'));
  });

  it('a second process sees the first process writes (fresh reload on mutate)', async () => {
    // Simulate two processes by pointing two sequential operations at the same
    // dir with a state reset in between (as a fresh process would start empty).
    const dir = mkTmp();
    setTaskPersistPath(dir);
    await taskTrackerTool.execute({ action: 'create', subject: 'from-proc-1' }, ctx);
    // "Process 2": clear in-memory state, repoint at the same dir.
    resetTaskState();
    setTaskPersistPath(dir);
    const created = await taskTrackerTool.execute({ action: 'create', subject: 'from-proc-2' }, ctx);
    // The second create must NOT reuse id 1 — it re-read proc-1's write.
    assert.ok(created.output.includes('Task #2'), 'id continues from disk high-water-mark');
    const list = await taskTrackerTool.execute({ action: 'list' }, ctx);
    assert.ok(list.output.includes('from-proc-1') && list.output.includes('from-proc-2'));
  });
});
