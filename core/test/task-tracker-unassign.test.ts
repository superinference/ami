import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  taskTrackerTool,
  setTaskPersistPath,
  resetTaskState,
  getTaskState,
  unassignTeammateTasks,
} from '../src/tools/task-tracker';
import {
  teamCreateTool,
  registerTeamMember,
  setTeamMemberActive,
  resetTeam,
} from '../src/tools/team';
import type { ToolContext } from '../src/types';

let tmp: string;
function ctx(): ToolContext {
  return { cwd: tmp, sessionId: 'sess-1', abortSignal: new AbortController().signal } as ToolContext;
}

describe('unassignTeammateTasks — cancellation/abort boundary (gap: unassign in-flight tasks on kill/shutdown)', () => {
  beforeEach(() => {
    resetTeam();
    resetTaskState();
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'task-unassign-'));
    setTaskPersistPath(tmp);
  });
  afterEach(() => {
    resetTeam();
    resetTaskState();
    try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ }
  });

  it('resets a teammate\'s unresolved tasks to pending/unowned and leaves completed + others untouched', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'A' }, ctx()); // #1
    await taskTrackerTool.execute({ action: 'create', subject: 'B' }, ctx()); // #2
    await taskTrackerTool.execute({ action: 'create', subject: 'C' }, ctx()); // #3
    await taskTrackerTool.execute({ action: 'create', subject: 'D' }, ctx()); // #4
    // alice claims #1 (in_progress) and owns #2 (completed); bob owns #3.
    await taskTrackerTool.execute({ action: 'claim', task_id: '1', owner: 'alice' }, ctx());
    await taskTrackerTool.execute({ action: 'update', task_id: '2', owner: 'alice', status: 'completed' }, ctx());
    await taskTrackerTool.execute({ action: 'claim', task_id: '3', owner: 'bob' }, ctx());
    // #4 is unowned pending.

    const result = await unassignTeammateTasks(['alice'], 'alice', 'terminated');

    // Only #1 (alice's unresolved task) is unassigned.
    assert.deepEqual(result.unassignedTasks, [{ id: 1, subject: 'A' }]);
    const t1 = getTaskState().get(1)!;
    assert.equal(t1.status, 'pending');
    assert.equal(t1.owner, undefined);
    // alice's completed task is untouched.
    const t2 = getTaskState().get(2)!;
    assert.equal(t2.status, 'completed');
    assert.equal(t2.owner, 'alice');
    // bob's task is untouched.
    const t3 = getTaskState().get(3)!;
    assert.equal(t3.status, 'in_progress');
    assert.equal(t3.owner, 'bob');
    // Notification wording for a termination, with the task list.
    assert.match(result.notificationMessage, /alice was terminated\./);
    assert.match(result.notificationMessage, /1 task\(s\) were unassigned: #1 "A"\./);
  });

  it('matches tasks owned by either the agentId or the display name', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'X' }, ctx()); // #1
    await taskTrackerTool.execute({ action: 'create', subject: 'Y' }, ctx()); // #2
    // One task owned by the id, one by the name.
    await taskTrackerTool.execute({ action: 'claim', task_id: '1', owner: 'agent-7' }, ctx());
    await taskTrackerTool.execute({ action: 'claim', task_id: '2', owner: 'worker' }, ctx());

    const result = await unassignTeammateTasks(['agent-7', 'worker'], 'worker', 'shutdown');
    assert.equal(result.unassignedTasks.length, 2);
    assert.equal(getTaskState().get(1)!.status, 'pending');
    assert.equal(getTaskState().get(2)!.status, 'pending');
    assert.match(result.notificationMessage, /worker has shut down\./);
  });

  it('with no owned unresolved tasks, returns an empty list and a bare notification', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'Z' }, ctx()); // #1 unowned
    const result = await unassignTeammateTasks(['ghost'], 'ghost', 'shutdown');
    assert.deepEqual(result.unassignedTasks, []);
    assert.equal(result.notificationMessage, 'ghost has shut down.');
  });

  it('wiring: setTeamMemberActive(false) releases the teammate\'s in-flight tasks back to the pool', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, ctx());
    registerTeamMember({ name: 'worker', agentId: 'a-1' });
    await taskTrackerTool.execute({ action: 'create', subject: 'job' }, ctx()); // #1
    await taskTrackerTool.execute({ action: 'claim', task_id: '1', owner: 'worker' }, ctx());
    assert.equal(getTaskState().get(1)!.status, 'in_progress');

    // Teammate exits -> marked inactive -> its unresolved task returns to the pool.
    setTeamMemberActive('worker', false);
    // The release runs under the task file lock; let it settle.
    await new Promise((r) => setTimeout(r, 100));

    const t1 = getTaskState().get(1)!;
    assert.equal(t1.status, 'pending', 'task should be reset to pending after teammate goes inactive');
    assert.equal(t1.owner, undefined, 'task owner should be cleared');
  });
});
