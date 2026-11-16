import { describe, it, beforeEach, afterEach, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  taskTrackerTool,
  resetTaskState,
  setTaskPersistPath,
  resolveTaskListId,
  sanitizeTaskListId,
  DEFAULT_TASKS_MODE_TASK_LIST_ID,
} from '../src/tools/task-tracker';
import { projectStateDir } from '../src/reference-journal';
import type { ToolContext } from '../src/types';

function ctx(over: Partial<ToolContext> = {}): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...over,
  } as ToolContext;
}

const tmpDirs: string[] = [];
function mkTmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'tt-u050-'));
  tmpDirs.push(d);
  return d;
}

const savedEnv = {
  ami: process.env.AMI_TASK_LIST_ID,
  claude: process.env.CLAUDE_CODE_TASK_LIST_ID,
  data: process.env.SUPERINFERENCE_DATA_DIR,
};
// Isolate all persisted task state into a dedicated temp data dir so the real
// ~/.ami is never touched by this suite.
const DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'tt-u050-data-'));
tmpDirs.push(DATA_DIR);

beforeEach(() => {
  delete process.env.AMI_TASK_LIST_ID;
  delete process.env.CLAUDE_CODE_TASK_LIST_ID;
  process.env.SUPERINFERENCE_DATA_DIR = DATA_DIR;
  resetTaskState();
});
afterEach(() => {
  resetTaskState();
  if (savedEnv.data === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
  else process.env.SUPERINFERENCE_DATA_DIR = savedEnv.data;
});
after(() => {
  if (savedEnv.ami === undefined) delete process.env.AMI_TASK_LIST_ID;
  else process.env.AMI_TASK_LIST_ID = savedEnv.ami;
  if (savedEnv.claude === undefined) delete process.env.CLAUDE_CODE_TASK_LIST_ID;
  else process.env.CLAUDE_CODE_TASK_LIST_ID = savedEnv.claude;
  if (savedEnv.data === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
  else process.env.SUPERINFERENCE_DATA_DIR = savedEnv.data;
  for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
});

// ---------------------------------------------------------------------------
// GAP: Claim rollback / orphaned-claim recovery — a "release" transition.
// ---------------------------------------------------------------------------
describe('task_tracker — release (claim rollback)', () => {
  it('reverts an in_progress claimed task to pending and clears the owner', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'work it' }, ctx());
    await taskTrackerTool.execute({ action: 'claim', task_id: '1', owner: 'alice' }, ctx());
    const r = await taskTrackerTool.execute({ action: 'release', task_id: '1' }, ctx());
    assert.equal(r.isError, false);
    assert.match(r.output, /Released task #1/);
    assert.match(r.output, /owner cleared/);
    assert.match(r.output, /reverted in_progress -> pending/);
    // State: pending, unowned -> surfaced again by `next`.
    const got = await taskTrackerTool.execute({ action: 'get', task_id: '1' }, ctx());
    assert.ok(got.output.includes('[pending]'));
    assert.ok(!got.output.includes('Owner:'));
    const next = await taskTrackerTool.execute({ action: 'next' }, ctx());
    assert.match(next.output, /Next available task:[\s\S]*#1/);
  });

  it('E2E: released task can be claimed by a different teammate', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'shared' }, ctx());
    await taskTrackerTool.execute({ action: 'claim', task_id: '1', owner: 'alice' }, ctx());
    // alice cannot start -> releases the claim.
    await taskTrackerTool.execute({ action: 'release', task_id: '1' }, ctx());
    const claimed = await taskTrackerTool.execute({ action: 'claim', task_id: '1', owner: 'bob' }, ctx());
    assert.equal(claimed.isError, false);
    assert.match(claimed.output, /\(owner: bob\)/);
  });

  it('releasing an unclaimed pending task is a non-error no-op', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'idle' }, ctx());
    const r = await taskTrackerTool.execute({ action: 'release', task_id: '1' }, ctx());
    assert.equal(r.isError, false);
    assert.match(r.output, /already unclaimed/);
    const got = await taskTrackerTool.execute({ action: 'get', task_id: '1' }, ctx());
    assert.ok(got.output.includes('[pending]'));
  });

  it('errors on a missing task id', async () => {
    const r = await taskTrackerTool.execute({ action: 'release' }, ctx());
    assert.equal(r.isError, true);
    assert.match(r.output, /task_id is required/);
  });

  it('errors for a non-existent task', async () => {
    const r = await taskTrackerTool.execute({ action: 'release', task_id: '99' }, ctx());
    assert.equal(r.isError, true);
    assert.match(r.output, /not found/);
    assert.match(r.output, /task_not_found/);
  });

  it('refuses to release a completed task', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'done' }, ctx());
    await taskTrackerTool.execute({ action: 'update', task_id: '1', status: 'completed' }, ctx());
    const r = await taskTrackerTool.execute({ action: 'release', task_id: '1' }, ctx());
    assert.equal(r.isError, true);
    assert.match(r.output, /already completed/);
    assert.match(r.output, /already_resolved/);
  });

  it('release is advertised in the tool action enum + description', () => {
    const enumVals = (taskTrackerTool.inputSchema.properties as Record<string, { enum?: string[] }>).action.enum;
    assert.ok(enumVals?.includes('release'));
    assert.match(taskTrackerTool.description, /"release"/);
  });
});

// ---------------------------------------------------------------------------
// GAP: task_list_id scoping (CLAUDE_CODE_TASK_LIST_ID / AMI_TASK_LIST_ID knob).
// ---------------------------------------------------------------------------
describe('task_tracker — task-list id scoping', () => {
  it('sanitizeTaskListId strips traversal/path-unsafe characters', () => {
    assert.equal(sanitizeTaskListId('../../etc/passwd'), 'etcpasswd');
    assert.equal(sanitizeTaskListId('team/one'), 'teamone');
    assert.equal(sanitizeTaskListId('  team-a_1  '), 'team-a_1');
    assert.equal(sanitizeTaskListId('***'), '');
    assert.equal(sanitizeTaskListId(42), '');
  });

  it('resolveTaskListId prefers explicit, then AMI env, then CLAUDE env, then default', () => {
    assert.equal(resolveTaskListId('explicit-x'), 'explicit-x');
    process.env.AMI_TASK_LIST_ID = 'ami-list';
    process.env.CLAUDE_CODE_TASK_LIST_ID = 'claude-list';
    assert.equal(resolveTaskListId(), 'ami-list');
    delete process.env.AMI_TASK_LIST_ID;
    assert.equal(resolveTaskListId(), 'claude-list');
    delete process.env.CLAUDE_CODE_TASK_LIST_ID;
    assert.equal(resolveTaskListId(), DEFAULT_TASKS_MODE_TASK_LIST_ID);
  });

  it('writes the default list to tasks.json and a scoped list to tasks-<id>.json', async () => {
    const dir = mkTmp();
    const stateDir = projectStateDir(dir);
    // Default list.
    setTaskPersistPath(dir);
    await taskTrackerTool.execute({ action: 'create', subject: 'default-task' }, ctx());
    assert.ok(fs.existsSync(path.join(stateDir, 'tasks.json')));
    // Scoped list.
    resetTaskState();
    setTaskPersistPath(dir, 'teamX');
    await taskTrackerTool.execute({ action: 'create', subject: 'scoped-task' }, ctx());
    assert.ok(fs.existsSync(path.join(stateDir, 'tasks-teamX.json')));
  });

  it('isolates tasks between two different list ids but shares within one', async () => {
    const dir = mkTmp();
    setTaskPersistPath(dir, 'alpha');
    await taskTrackerTool.execute({ action: 'create', subject: 'alpha-only' }, ctx());
    // Switch to beta: alpha's task must not be visible.
    resetTaskState();
    setTaskPersistPath(dir, 'beta');
    let list = await taskTrackerTool.execute({ action: 'list' }, ctx());
    assert.ok(!list.output.includes('alpha-only'));
    await taskTrackerTool.execute({ action: 'create', subject: 'beta-only' }, ctx());
    // Re-open alpha: sees alpha-only, not beta-only (separate files).
    resetTaskState();
    setTaskPersistPath(dir, 'alpha');
    list = await taskTrackerTool.execute({ action: 'list' }, ctx());
    assert.ok(list.output.includes('alpha-only'));
    assert.ok(!list.output.includes('beta-only'));
  });

  it('resolves the list id from the AMI_TASK_LIST_ID env knob when none is passed', async () => {
    const dir = mkTmp();
    process.env.AMI_TASK_LIST_ID = 'env-scoped';
    setTaskPersistPath(dir);
    await taskTrackerTool.execute({ action: 'create', subject: 'via-env' }, ctx());
    assert.ok(fs.existsSync(path.join(projectStateDir(dir), 'tasks-env-scoped.json')));
  });
});

// ---------------------------------------------------------------------------
// GAP: Graceful non-error "Task not found" on the read (get) path.
// ---------------------------------------------------------------------------
describe('task_tracker — graceful get not-found', () => {
  it('returns a non-error result for a missing id on get', async () => {
    const r = await taskTrackerTool.execute({ action: 'get', task_id: '7' }, ctx());
    assert.equal(r.isError, false);
    assert.match(r.output, /Task #7 not found\./);
  });
});

// ---------------------------------------------------------------------------
// GAP: Staleness guidance (re-read latest state via "get" before update/claim).
// ---------------------------------------------------------------------------
describe('task_tracker — staleness guidance', () => {
  it('documents reading latest state before update/claim in shared lists', () => {
    assert.match(taskTrackerTool.description, /Staleness/);
    assert.match(taskTrackerTool.description, /re-read the latest state with action "get"/);
  });
});
