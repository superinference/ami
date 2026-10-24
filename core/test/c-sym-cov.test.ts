import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  getWorkspaceIndexer,
  _resetIndexer,
} from '../src/tools/search-symbols';
import { WorkspaceIndexer } from '../src/workspace-indexer';
import { skillTool } from '../src/tools/skill';
import {
  taskTrackerTool,
  resetTaskState,
  setTaskPersistPath,
  getTaskState,
} from '../src/tools/task-tracker';
import type { ToolContext } from '../src/types';

const tick = () => new Promise((r) => setTimeout(r, 15));

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// search-symbols.ts — background build rejection (.catch arm)
// ---------------------------------------------------------------------------

describe('search-symbols — background build rejection', () => {
  afterEach(() => _resetIndexer());

  it('swallows a rejected buildIndex via the .catch arm', async () => {
    _resetIndexer();
    const m = mock.method(
      WorkspaceIndexer.prototype,
      'buildIndex',
      () => Promise.reject(new Error('index boom')),
    );
    try {
      const indexer = getWorkspaceIndexer(os.tmpdir());
      assert.ok(indexer);
      assert.equal(typeof indexer.searchSymbols, 'function');
      // Let the fire-and-forget .then()/.catch() chain settle. The .catch arm
      // resets the internal build promise; reaching here without an unhandled
      // rejection proves it ran.
      await tick();
    } finally {
      m.mock.restore();
      _resetIndexer();
    }
  });
});

// ---------------------------------------------------------------------------
// skill.ts — forwardAbort + MCP prompt that resolves to nothing
// ---------------------------------------------------------------------------

describe('skillTool — forwardAbort on parent abort', () => {
  it('forwards a parent abort to the sub-agent during execution', async () => {
    const ac = new AbortController();
    const r = await skillTool.execute(
      { skill: 'sub' },
      ctx({
        abortSignal: ac.signal,
        _skillManager: {
          getSkill: () => ({ name: 'sub', context: 'subagent' }),
          getSkillContent: () => 'do',
          listSkills: () => [],
        } as any,
        _providerConfig: { baseUrl: 'http://t', apiKey: 'k', model: 'm' },
        _engineFactory: (_cfg: any) => ({
          async *submit() {
            // Aborting the parent signal synchronously fires the 'abort'
            // listener (forwardAbort), which propagates to the sub-controller.
            ac.abort();
            yield { type: 'text_delta', text: 'x' };
          },
        }),
      }),
    );
    assert.ok(!r.isError);
    assert.ok(r.output.includes('x'));
  });
});

describe('skillTool — MCP prompt resolves to no result', () => {
  it('falls through to not-found when getPrompt yields nothing', async () => {
    const r = await skillTool.execute(
      { skill: 'mcp-null', args: 'a' },
      ctx({
        _skillManager: {
          getSkill: () => null,
          getSkillContent: () => null,
          listSkills: () => [],
        } as any,
        _mcpManager: {
          listPrompts: async () => [{ name: 'mcp-null', server: 'srv' }],
          getPrompt: async () => null,
        },
      }),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('not found'));
  });

  it('resolves an MCP prompt invoked without args (empty-args branch)', async () => {
    let seenArgs: any;
    const r = await skillTool.execute(
      { skill: 'mcp-noargs' },
      ctx({
        _skillManager: {
          getSkill: () => null,
          getSkillContent: () => null,
          listSkills: () => [],
        } as any,
        _mcpManager: {
          listPrompts: async () => [{ name: 'mcp-noargs', server: 'srv' }],
          getPrompt: async (_s: string, _n: string, a: any) => {
            seenArgs = a;
            return { messages: [{ role: 'user', content: 'x' }] };
          },
        },
      }),
    );
    assert.ok(!r.isError);
    assert.ok(r.output.includes('MCP Skill'));
    assert.deepEqual(seenArgs, {});
  });

  it('falls through when the MCP manager exposes no getPrompt method', async () => {
    const r = await skillTool.execute(
      { skill: 'mcp-nogetter' },
      ctx({
        _skillManager: {
          getSkill: () => null,
          getSkillContent: () => null,
          listSkills: () => [],
        } as any,
        _mcpManager: {
          listPrompts: async () => [{ name: 'mcp-nogetter', server: 'srv' }],
          // no getPrompt -> optional-chain short-circuits to undefined
        },
      }),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('not found'));
  });
});

// ---------------------------------------------------------------------------
// task-tracker.ts — hook rejection handling (.catch arms)
// ---------------------------------------------------------------------------

describe('task_tracker — hook rejection handling', () => {
  beforeEach(() => resetTaskState());
  afterEach(() => resetTaskState());

  it('swallows a rejected task-created hook', async () => {
    const r = await taskTrackerTool.execute(
      { action: 'create', subject: 'hooked' },
      ctx({
        _hookManager: {
          executeTaskCreated: () => Promise.reject(new Error('created boom')),
          executeTaskCompleted: () => Promise.resolve(),
        } as any,
      }),
    );
    assert.ok(!r.isError);
    await tick();
  });

  it('swallows a rejected task-completed hook', async () => {
    const hm = {
      executeTaskCreated: () => Promise.resolve(),
      executeTaskCompleted: () => Promise.reject(new Error('completed boom')),
    } as any;
    await taskTrackerTool.execute(
      { action: 'create', subject: 't' },
      ctx({ _hookManager: hm }),
    );
    const r = await taskTrackerTool.execute(
      { action: 'update', task_id: '1', status: 'completed' },
      ctx({ _hookManager: hm }),
    );
    assert.ok(!r.isError);
    await tick();
  });
});

// ---------------------------------------------------------------------------
// task-tracker.ts — dependency cleanup on removal (filter arrows)
// ---------------------------------------------------------------------------

describe('task_tracker — dependency cleanup on removal', () => {
  beforeEach(() => resetTaskState());
  afterEach(() => resetTaskState());

  // Build A(#1), B(#2), C(#3) where the surviving task B carries BOTH a
  // non-empty blocks array (B blocks C) and a non-empty blockedBy array
  // (B blocked by A). Removing A then drives both filter arrows in the
  // cleanup loop.
  async function seedChain(): Promise<void> {
    await taskTrackerTool.execute({ action: 'create', subject: 'A' }, ctx()); // #1
    await taskTrackerTool.execute({ action: 'create', subject: 'B' }, ctx()); // #2
    await taskTrackerTool.execute({ action: 'create', subject: 'C' }, ctx()); // #3
    await taskTrackerTool.execute(
      { action: 'update', task_id: '2', addBlocks: ['3'], addBlockedBy: ['1'] },
      ctx(),
    );
    assert.deepEqual(getTaskState().get(2)!.blocks, [3]);
    assert.deepEqual(getTaskState().get(2)!.blockedBy, [1]);
  }

  it('cleans up blocks/blockedBy edges when removing via update status=deleted', async () => {
    await seedChain();
    const r = await taskTrackerTool.execute(
      { action: 'update', task_id: '1', status: 'deleted' },
      ctx(),
    );
    assert.ok(!r.isError);
    assert.deepEqual(getTaskState().get(2)!.blockedBy, []);
    assert.deepEqual(getTaskState().get(2)!.blocks, [3]);
  });

  it('cleans up blocks/blockedBy edges when removing via the delete action', async () => {
    await seedChain();
    const r = await taskTrackerTool.execute(
      { action: 'delete', task_id: '1' },
      ctx(),
    );
    assert.ok(!r.isError);
    assert.deepEqual(getTaskState().get(2)!.blockedBy, []);
    assert.deepEqual(getTaskState().get(2)!.blocks, [3]);
  });
});

// ---------------------------------------------------------------------------
// task-tracker.ts — disk load/save branches + formatTask icon fallback
// ---------------------------------------------------------------------------

describe('task_tracker — disk load/save branches', () => {
  let dir: string;

  beforeEach(() => {
    resetTaskState();
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'c-tt-'));
  });

  afterEach(() => {
    resetTaskState();
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('loads stored tasks and falls back to nextId=1 when stored nextId is falsy', () => {
    const sdir = path.join(dir, '.superinference');
    fs.mkdirSync(sdir, { recursive: true });
    fs.writeFileSync(
      path.join(sdir, 'tasks.json'),
      JSON.stringify({
        nextId: 0,
        tasks: [
          { id: 3, subject: 'x', description: '', status: 'pending', blocks: [], blockedBy: [] },
          { id: 1, subject: 'y', description: '', status: 'pending', blocks: [], blockedBy: [] },
        ],
      }),
    );
    setTaskPersistPath(dir);
    assert.equal(getTaskState().size, 2);
  });

  it('loads a persisted file with an empty task list', () => {
    const sdir = path.join(dir, '.superinference');
    fs.mkdirSync(sdir, { recursive: true });
    fs.writeFileSync(
      path.join(sdir, 'tasks.json'),
      JSON.stringify({ nextId: 7, tasks: [] }),
    );
    setTaskPersistPath(dir);
    assert.equal(getTaskState().size, 0);
  });

  it('ignores an invalid tasks.json (load catch arm)', () => {
    const sdir = path.join(dir, '.superinference');
    fs.mkdirSync(sdir, { recursive: true });
    fs.writeFileSync(path.join(sdir, 'tasks.json'), '{ not valid json');
    setTaskPersistPath(dir);
    assert.equal(getTaskState().size, 0);
  });

  it('swallows a save failure when the persist directory cannot be created', async () => {
    // Point the persist root at a regular file so mkdir of its .superinference
    // child throws ENOTDIR, exercising saveToDisk's catch arm.
    const asFile = path.join(dir, 'afile');
    fs.writeFileSync(asFile, 'x');
    setTaskPersistPath(asFile);
    const r = await taskTrackerTool.execute(
      { action: 'create', subject: 'nofs' },
      ctx(),
    );
    assert.ok(!r.isError);
    assert.ok(!fs.existsSync(path.join(asFile, '.superinference', 'tasks.json')));
  });

  it('renders the default icon for an unrecognized status', async () => {
    resetTaskState();
    await taskTrackerTool.execute({ action: 'create', subject: 'weird' }, ctx());
    const t = getTaskState().get(1)!;
    (t as any).status = 'archived'; // not present in STATUS_ICONS
    const r = await taskTrackerTool.execute(
      { action: 'get', task_id: '1' },
      ctx(),
    );
    assert.ok(!r.isError);
    assert.ok(r.output.includes('[ ]'));
  });
});
