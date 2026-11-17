// Gap U011/w88: DEAD-CHILD detection + bounded prune-and-retry for a background
// sub-agent spawn (claudeRef utils/swarm/backends/ITermBackend.ts:L131-208 —
// dead-target detection with bounded prune-and-retry that distinguishes a dead
// resource from a systemic failure before mutating the running-agent roster;
// PaneBackendExecutor.ts:L163-175 leader-exit cleanup of a spawned entry).
//
// A background spawn registers its process-manager roster entry + resumable-agent
// registry BEFORE the sub-engine is constructed. If the engine factory throws (a
// DEAD CHILD), the OLD code let the exception reject out of the tool, leaving a
// phantom 'running' entry in the roster and a stale name->id registry entry that no
// task_output/resume could ever drain. The fix detects the dead child, retries a
// bounded number of times, and on persistent failure PRUNES the registered entries
// and returns a classified error.
//
// RED on revert: without buildSubEngineWithRetry + pruneDeadBackgroundAgent wired
// into the background path, `taskTool.execute` REJECTS (throws) on a throwing
// factory instead of returning an isError result, and the roster entry stays
// 'running' — both assertions below fail.

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  taskTool,
  resetAgentRegistry,
  getAgentByName,
  buildSubEngineWithRetry,
  pruneDeadBackgroundAgent,
  BACKGROUND_SPAWN_MAX_ATTEMPTS,
} from '../src/tools/task';
import { resetTeam } from '../src/tools/team';
import type { ToolContext } from '../src/types';

type Ev = Record<string, unknown>;

/** Minimal in-memory process-manager double recording roster mutations. */
function makeProcessManager() {
  const entries = new Map<string, Record<string, unknown>>();
  const idle = new Map<string, boolean>();
  const killed: string[] = [];
  return {
    entries,
    idle,
    killed,
    runningAgentCount() {
      let n = 0;
      for (const e of entries.values()) if (e.status === 'running') n++;
      return n;
    },
    registerAgent(taskId: string, info: Record<string, unknown>) {
      entries.set(taskId, { status: 'running', ...info });
    },
    updateEntry(taskId: string, patch: Record<string, unknown>) {
      entries.set(taskId, { ...(entries.get(taskId) ?? {}), ...patch });
    },
    markIdle(taskId: string, v: boolean) {
      idle.set(taskId, v);
      return true;
    },
    killTasksForAgent(taskId: string) {
      killed.push(taskId);
    },
    get flowManager() {
      return undefined;
    },
  };
}

let tmpDir: string;
beforeEach(() => {
  resetAgentRegistry();
  resetTeam();
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u011-w88-'));
});
afterEach(() => {
  resetAgentRegistry();
  resetTeam();
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

async function* okStream(_p: string) {
  yield { type: 'text_delta', text: 'ok' } as never;
}

function baseCtx(extra: Partial<ToolContext> = {}): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
    _emitChild: () => {},
    _hookManager: {
      executeSubagentStart: async () => {},
      executeSubagentStop: async () => {},
      executeTeammateIdle: async () => {},
    } as unknown as ToolContext['_hookManager'],
    ...extra,
  } as unknown as ToolContext;
}

// ---------------------------------------------------------------------------
// buildSubEngineWithRetry — bounded retry unit behavior
// ---------------------------------------------------------------------------
describe('buildSubEngineWithRetry — bounded dead-child retry', () => {
  it('returns the engine on the first successful attempt', () => {
    let calls = 0;
    const engine = buildSubEngineWithRetry((_c) => {
      calls++;
      return { submit: okStream, shutdown: () => {} };
    }, {} as never);
    assert.equal(calls, 1);
    assert.ok(engine.submit);
  });

  it('retries a transient failure and succeeds within the bound', () => {
    let calls = 0;
    const engine = buildSubEngineWithRetry((_c) => {
      calls++;
      if (calls < 2) throw new Error('transient dead resource');
      return { submit: okStream, shutdown: () => {} };
    }, {} as never);
    assert.equal(calls, 2);
    assert.ok(engine.submit);
  });

  it('rethrows the LAST error after exhausting the attempt bound (systemic failure)', () => {
    let calls = 0;
    assert.throws(
      () =>
        buildSubEngineWithRetry((_c) => {
          calls++;
          throw new Error(`systemic #${calls}`);
        }, {} as never),
      /systemic #2/,
    );
    assert.equal(calls, BACKGROUND_SPAWN_MAX_ATTEMPTS);
  });

  it('honors a custom maxAttempts', () => {
    let calls = 0;
    assert.throws(
      () =>
        buildSubEngineWithRetry((_c) => {
          calls++;
          throw new Error('x');
        }, {} as never, 4),
    );
    assert.equal(calls, 4);
  });
});

// ---------------------------------------------------------------------------
// pruneDeadBackgroundAgent — roster/registry teardown of a dead child
// ---------------------------------------------------------------------------
describe('pruneDeadBackgroundAgent — reverses spawn bookkeeping', () => {
  it('marks the roster entry failed, drops the registry name, and unlinks abort', () => {
    const pm = makeProcessManager();
    pm.registerAgent('agent-dead', { command: 'x' });
    const ctx = baseCtx({ processManager: pm as unknown as ToolContext['processManager'] });
    let unlinked = false;
    pruneDeadBackgroundAgent({
      context: ctx,
      taskId: 'agent-dead',
      agentName: 'worker',
      effectiveCwd: tmpDir,
      unlinkParentAbort: () => { unlinked = true; },
    });
    assert.equal(pm.entries.get('agent-dead')?.status, 'failed');
    assert.equal(pm.entries.get('agent-dead')?.exitCode, 1);
    assert.equal(unlinked, true);
    assert.equal(getAgentByName('worker'), undefined);
  });

  it('is best-effort when no processManager / abort / name is present', () => {
    const ctx = baseCtx();
    assert.doesNotThrow(() =>
      pruneDeadBackgroundAgent({ context: ctx, taskId: 'agent-x', effectiveCwd: tmpDir }),
    );
  });
});

// ---------------------------------------------------------------------------
// End-to-end through the task tool: a dead child must NOT reject or orphan a
// phantom 'running' roster entry.
// ---------------------------------------------------------------------------
describe('taskTool background spawn — dead-child safety (e2e)', () => {
  it('returns an error (not a rejection) and prunes the roster when the engine factory throws', async () => {
    const pm = makeProcessManager();
    const ctx = baseCtx({
      processManager: pm as unknown as ToolContext['processManager'],
      _engineFactory: () => { throw new Error('boom: dead backend'); },
    });

    const res = await taskTool.execute(
      { prompt: 'do the work', name: 'bgworker', run_in_background: true, mode: 'general' },
      ctx,
    );

    // The tool returns a classified error instead of rejecting.
    assert.equal(res.isError, true);
    assert.match(res.output, /failed to start background agent/);

    // The roster entry was registered then PRUNED to a terminal status — no phantom
    // 'running' agent is left behind.
    const rosterEntries = [...pm.entries.values()];
    assert.ok(rosterEntries.length >= 1, 'a roster entry was registered');
    for (const e of rosterEntries) {
      assert.notEqual(e.status, 'running', 'no phantom running entry remains');
      assert.equal(e.status, 'failed');
    }

    // The resumable-agent registry no longer resolves the dead agent's name.
    assert.equal(getAgentByName('bgworker'), undefined);
  });

  it('a healthy background spawn still starts normally (prune path is a no-op)', async () => {
    const pm = makeProcessManager();
    const ctx = baseCtx({
      processManager: pm as unknown as ToolContext['processManager'],
      _engineFactory: () => ({ submit: okStream, shutdown: () => {} }),
    });

    const res = await taskTool.execute(
      { prompt: 'do the work', name: 'liveworker', run_in_background: true, mode: 'general' },
      ctx,
    );
    assert.equal(res.isError, undefined);
    assert.match(res.output, /^Background agent started:/);
    // The live agent is addressable (not pruned).
    assert.ok(getAgentByName('liveworker'));
  });
});
