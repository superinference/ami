import { describe, it, before, after, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  taskTrackerTool,
  resetTaskState,
  getTaskState,
  setTaskPersistPath,
} from '../src/tools/task-tracker';
import type { ToolContext } from '../src/types';

// Minimal no-op context: the persistence/formatting paths exercised here never
// touch the hook manager, so an empty object is sufficient.
const ctx = {} as unknown as ToolContext;

// Track every temp dir so we can clean up deterministically.
const tmpDirs: string[] = [];
function mkTmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'tt-cov-'));
  tmpDirs.push(d);
  return d;
}

function writeTasksFile(dir: string, data: unknown): string {
  const sub = path.join(dir, '.ami');
  fs.mkdirSync(sub, { recursive: true });
  const file = path.join(sub, 'tasks.json');
  fs.writeFileSync(file, JSON.stringify(data), 'utf-8');
  return file;
}

describe('task-tracker: persistence + formatting behavior', () => {
  beforeEach(() => {
    resetTaskState();
  });

  after(() => {
    for (const d of tmpDirs) {
      try {
        fs.rmSync(d, { recursive: true, force: true });
      } catch {
        /* ignore */
      }
    }
  });

  it('loadFromDisk hydrates tasks and advances nextId to maxId+1 even with out-of-order ids', async () => {
    const dir = mkTmp();
    // Ids deliberately NOT sorted ascending so the `if (t.id > maxId)` update
    // branch is taken (5 -> maxId) and then NOT taken (2 is not > 5).
    writeTasksFile(dir, {
      nextId: 3, // intentionally stale/lower than maxId+1
      tasks: [
        { id: 5, subject: 'five', description: '', status: 'pending', blocks: [], blockedBy: [] },
        { id: 2, subject: 'two', description: '', status: 'completed', blocks: [], blockedBy: [] },
      ],
    });

    setTaskPersistPath(dir);

    // Both tasks were loaded.
    const state = getTaskState();
    assert.equal(state.size, 2);
    assert.equal(state.get(5)?.subject, 'five');
    assert.equal(state.get(2)?.status, 'completed');

    // High-water-mark: stale stored nextId (3) is overridden by maxId+1 (6).
    const created = await taskTrackerTool.execute({ action: 'create', subject: 'next' }, ctx);
    assert.equal(created.isError, false);
    assert.match(created.output, /^Task #6 created/);
  });

  it('high-water-mark uses stored nextId when it exceeds maxId+1', async () => {
    const dir = mkTmp();
    writeTasksFile(dir, {
      nextId: 50,
      tasks: [
        { id: 1, subject: 'one', description: '', status: 'pending', blocks: [], blockedBy: [] },
      ],
    });
    setTaskPersistPath(dir);

    const created = await taskTrackerTool.execute({ action: 'create', subject: 'x' }, ctx);
    // Stored nextId (50) wins over maxId+1 (2).
    assert.match(created.output, /^Task #50 created/);
  });

  it('falsy stored nextId with no tasks falls back to 1', async () => {
    const dir = mkTmp();
    writeTasksFile(dir, { nextId: 0, tasks: [] });
    setTaskPersistPath(dir);

    assert.equal(getTaskState().size, 0);
    const created = await taskTrackerTool.execute({ action: 'create', subject: 'first' }, ctx);
    // max(0||1, 0+1) === 1
    assert.match(created.output, /^Task #1 created/);
  });

  it('loadFromDisk recovers to a fresh (empty) state on corrupt JSON', async () => {
    const dir = mkTmp();
    const sub = path.join(dir, '.ami');
    fs.mkdirSync(sub, { recursive: true });
    fs.writeFileSync(path.join(sub, 'tasks.json'), '{ this is not valid json', 'utf-8');

    // Seed in-memory state first; a corrupt file must NOT wipe it (parse throws
    // before tasks.clear() runs), proving the catch is a true no-op recovery.
    await taskTrackerTool.execute({ action: 'create', subject: 'survivor' }, ctx);
    const before = getTaskState().size;
    assert.equal(before, 1);

    setTaskPersistPath(dir);

    // Corrupt file -> catch -> in-memory state untouched.
    assert.equal(getTaskState().size, 1);
    assert.equal(getTaskState().get(1)?.subject, 'survivor');
  });

  it('missing tasks file leaves an empty fresh state (readFileSync throws -> catch)', () => {
    const dir = mkTmp(); // no .ami/tasks.json written
    setTaskPersistPath(dir);
    assert.equal(getTaskState().size, 0);
  });

  it('round-trips tasks (incl. dependency edges) to disk and back', async () => {
    const dir = mkTmp();
    setTaskPersistPath(dir);

    await taskTrackerTool.execute({ action: 'create', subject: 'A' }, ctx); // #1
    await taskTrackerTool.execute({ action: 'create', subject: 'B' }, ctx); // #2
    // #2 is blocked by #1.
    await taskTrackerTool.execute(
      { action: 'update', task_id: '2', addBlockedBy: ['1'] },
      ctx,
    );

    // File now exists on disk.
    const file = path.join(dir, '.ami', 'tasks.json');
    assert.ok(fs.existsSync(file));

    // Reload into a brand-new in-memory state by pointing at the same dir.
    resetTaskState();
    assert.equal(getTaskState().size, 0);
    setTaskPersistPath(dir);

    const state = getTaskState();
    assert.equal(state.size, 2);
    assert.deepEqual(state.get(2)?.blockedBy, [1]);
    assert.deepEqual(state.get(1)?.blocks, [2]);
  });

  it('formatTask falls back to the "[ ]" icon for an unknown persisted status', async () => {
    const dir = mkTmp();
    writeTasksFile(dir, {
      nextId: 2,
      tasks: [
        // status outside STATUS_ICONS keys -> `|| '[ ]'` fallback arm.
        { id: 1, subject: 'weird', description: '', status: 'archived', blocks: [], blockedBy: [] },
      ],
    });
    setTaskPersistPath(dir);

    const res = await taskTrackerTool.execute({ action: 'get', task_id: '1' }, ctx);
    assert.equal(res.isError, false);
    assert.ok(res.output.startsWith('[ ] Task #1: weird [archived]'));
  });

  it('unresolvedBlockers drops dangling (deleted) blocker ids when formatting', async () => {
    const dir = mkTmp();
    writeTasksFile(dir, {
      nextId: 3,
      tasks: [
        // blockedBy #99 (does not exist) + #2 (exists, completed) -> both resolved/dropped.
        { id: 1, subject: 'main', description: '', status: 'pending', blocks: [], blockedBy: [99, 2] },
        { id: 2, subject: 'done-dep', description: '', status: 'completed', blocks: [], blockedBy: [] },
      ],
    });
    setTaskPersistPath(dir);

    const res = await taskTrackerTool.execute({ action: 'get', task_id: '1' }, ctx);
    assert.equal(res.isError, false);
    // Dangling (#99 undefined dep) and completed (#2) are both filtered out,
    // so no "Blocked by" line is rendered.
    assert.doesNotMatch(res.output, /Blocked by/);
  });

  it('unresolvedBlockers keeps an incomplete existing blocker when formatting', async () => {
    const dir = mkTmp();
    writeTasksFile(dir, {
      nextId: 3,
      tasks: [
        { id: 1, subject: 'main', description: '', status: 'pending', blocks: [], blockedBy: [2] },
        { id: 2, subject: 'pending-dep', description: '', status: 'pending', blocks: [1], blockedBy: [] },
      ],
    });
    setTaskPersistPath(dir);

    const res = await taskTrackerTool.execute({ action: 'get', task_id: '1' }, ctx);
    assert.match(res.output, /Blocked by: #2/);
  });

  it('formatTask omits the Metadata line when all keys are underscore-prefixed', async () => {
    const dir = mkTmp();
    writeTasksFile(dir, {
      nextId: 2,
      tasks: [
        {
          id: 1,
          subject: 'hidden-meta',
          description: '',
          status: 'pending',
          metadata: { _internalOnly: true, _hidden: 1 },
          blocks: [],
          blockedBy: [],
        },
      ],
    });
    setTaskPersistPath(dir);

    const res = await taskTrackerTool.execute({ action: 'get', task_id: '1' }, ctx);
    // Every key starts with '_', so metaStr is empty and the Metadata line is skipped.
    assert.doesNotMatch(res.output, /Metadata:/);
  });

  it('formatTask renders Metadata when at least one visible (non-underscore) key exists', async () => {
    const dir = mkTmp();
    writeTasksFile(dir, {
      nextId: 2,
      tasks: [
        {
          id: 1,
          subject: 'with-meta',
          description: '',
          status: 'pending',
          metadata: { _hidden: 1, priority: 'high' },
          blocks: [],
          blockedBy: [],
        },
      ],
    });
    setTaskPersistPath(dir);

    const res = await taskTrackerTool.execute({ action: 'get', task_id: '1' }, ctx);
    assert.match(res.output, /Metadata: priority="high"/);
    assert.doesNotMatch(res.output, /_hidden/);
  });

  it('list hides tasks flagged _internal in metadata', async () => {
    const dir = mkTmp();
    writeTasksFile(dir, {
      nextId: 3,
      tasks: [
        { id: 1, subject: 'visible', description: '', status: 'pending', blocks: [], blockedBy: [] },
        { id: 2, subject: 'secret', description: '', status: 'pending', metadata: { _internal: true }, blocks: [], blockedBy: [] },
      ],
    });
    setTaskPersistPath(dir);

    const res = await taskTrackerTool.execute({ action: 'list' }, ctx);
    assert.match(res.output, /visible/);
    assert.doesNotMatch(res.output, /secret/);
  });
});
