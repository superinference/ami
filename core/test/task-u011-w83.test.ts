import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  taskTool,
  resetAgentRegistry,
  resolveSpawnTaskId,
  generateTaskId,
} from '../src/tools/task';
import { ProcessManager } from '../src/process-manager';
import type { ToolContext, ToolDefinition, EngineConfig } from '../src/types';

type Ev = Record<string, unknown>;

interface Harness {
  isError?: boolean;
  output: string;
  cfg: EngineConfig | null;
  childEvents: Array<Record<string, unknown>>;
}

/**
 * Run the task tool against a fake engine, capturing the sub-engine config and
 * every relayed child event (so a test can read the subagent_start nodeId, which
 * IS the spawned taskId).
 */
async function runTask(
  input: Record<string, unknown>,
  ctx: Partial<ToolContext> = {},
  events: Ev[] = [{ type: 'text_delta', text: 'ok' }],
): Promise<Harness> {
  let cfg: EngineConfig | null = null;
  const childEvents: Array<Record<string, unknown>> = [];
  async function* fakeSubmit(_p: string) {
    for (const e of events) yield e as never;
  }
  const result = await taskTool.execute(input, {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
    _emitChild: (e: Record<string, unknown>) => { childEvents.push(e); },
    _engineFactory: (c) => { cfg = c; void (c.tools as ToolDefinition[]); return { submit: fakeSubmit, shutdown: () => {} }; },
    ...ctx,
  } as unknown as ToolContext);
  return { isError: result.isError, output: result.output, cfg, childEvents };
}

// ---------------------------------------------------------------------------
// Gap U011/w83: resolveSpawnTaskId — resume reuses the ORIGINAL taskId
// (claudeRef utils/task/framework.ts:L74-117 — resume re-registers under the same id)
describe('resolveSpawnTaskId — resume reuses the original taskId, fresh spawn mints a new one', () => {
  it('a fresh spawn (no resume) mints a new unique agent id', () => {
    const a = resolveSpawnTaskId(undefined);
    const b = resolveSpawnTaskId(undefined);
    assert.match(a, /^agent-[0-9a-f]+$/);
    assert.match(b, /^agent-[0-9a-f]+$/);
    assert.notEqual(a, b, 'two fresh spawns get distinct ids');
  });

  it('a resume carries the prior taskId forward verbatim', () => {
    const prior = generateTaskId();
    const resolved = resolveSpawnTaskId({ sessionId: 'sess-1', resolvedId: prior });
    assert.equal(resolved, prior, 'resume reuses the original taskId, no new id minted');
  });
});

// ---------------------------------------------------------------------------
// Gap U011/w83: ProcessManager.registerAgent carries startTime forward on resume
describe('ProcessManager.registerAgent — resume carries the original startTime forward', () => {
  it('a resumed re-registration under the same id keeps the original startTime', () => {
    const pm = new ProcessManager(os.tmpdir());
    const id = generateTaskId();
    pm.registerAgent(id, { command: 'c', description: 'd', outputPath: '/tmp/x', abortFn: () => {} });
    const t0 = pm.get(id)!.startTime;
    // Re-register the SAME id as a resume a moment later.
    const laterNow = Date.now() + 5_000;
    const realNow = Date.now;
    try {
      Date.now = () => laterNow;
      pm.registerAgent(id, { command: 'c', description: 'd', outputPath: '/tmp/x', abortFn: () => {}, resumed: true });
    } finally {
      Date.now = realNow;
    }
    assert.equal(pm.get(id)!.startTime, t0, 'resumed re-registration preserves the original startTime');
    assert.equal(pm.get(id)!.resumed, true, 'resume marker set');
  });

  it('a NON-resumed re-registration (same id) resets the clock', () => {
    const pm = new ProcessManager(os.tmpdir());
    const id = generateTaskId();
    pm.registerAgent(id, { command: 'c', description: 'd', outputPath: '/tmp/x', abortFn: () => {} });
    const t0 = pm.get(id)!.startTime;
    const laterNow = Date.now() + 5_000;
    const realNow = Date.now;
    try {
      Date.now = () => laterNow;
      pm.registerAgent(id, { command: 'c', description: 'd', outputPath: '/tmp/x', abortFn: () => {} });
    } finally {
      Date.now = realNow;
    }
    assert.equal(pm.get(id)!.startTime, laterNow, 'a fresh (non-resume) register resets startTime to now');
  });
});

// ---------------------------------------------------------------------------
// Gap U011/w83 (E2E/integration reproducer): resuming a named agent re-registers
// under the SAME taskId instead of minting a duplicate background entry.
// RED on revert: the old code called generateTaskId() unconditionally, so the resume
// surfaced a brand-new nodeId distinct from the original spawn's.
describe('task resume reuses the spawned agent taskId end-to-end (U011/w83)', () => {
  let tmp: string;
  let prevData: string | undefined;

  beforeEach(() => {
    resetAgentRegistry();
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u011-w83-'));
    prevData = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = tmp;
  });
  afterEach(() => {
    if (prevData === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
    else process.env.SUPERINFERENCE_DATA_DIR = prevData;
    try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* best effort */ }
    resetAgentRegistry();
  });

  it('a resume emits the SAME taskId (nodeId) as the original spawn', async () => {
    // 1) Fresh foreground spawn of a named agent.
    const spawn = await runTask({ prompt: 'start', mode: 'general', name: 'builder' }, { cwd: tmp });
    assert.ok(!spawn.isError, `spawn should succeed: ${spawn.output}`);
    const spawnStart = spawn.childEvents.find(e => e.type === 'subagent_start');
    assert.ok(spawnStart, 'spawn emitted a subagent_start');
    const originalId = String(spawnStart!.nodeId);
    assert.match(originalId, /^agent-[0-9a-f]+$/);

    // 2) Resume that agent by name.
    const resumed = await runTask({ prompt: 'again', resume: 'builder' }, { cwd: tmp });
    assert.ok(!resumed.isError, `resume should succeed: ${resumed.output}`);
    const resumeStart = resumed.childEvents.find(e => e.type === 'subagent_start');
    assert.ok(resumeStart, 'resume emitted a subagent_start');
    const resumedId = String(resumeStart!.nodeId);

    // The resume continues the SAME agent → same taskId (no duplicate entry).
    assert.equal(resumedId, originalId, 'resume re-registers under the original taskId');
  });
});
