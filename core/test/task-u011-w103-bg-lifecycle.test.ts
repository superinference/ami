import { describe, it, beforeEach, afterEach, before, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { taskTool, resetAgentRegistry, buildSubagentHookCtx } from '../src/tools/task';
import { teamCreateTool, resetTeam } from '../src/tools/team';
import type { ToolContext, ToolDefinition, EngineConfig } from '../src/types';
import type { SubagentContext } from '../src/hooks';

type Ev = Record<string, unknown>;

interface BgHarness {
  isError?: boolean;
  output: string;
  starts: SubagentContext[];
  stops: SubagentContext[];
}

let dataDir: string;
before(() => {
  dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u011-w103-'));
  process.env.SUPERINFERENCE_DATA_DIR = dataDir;
});
after(() => {
  delete process.env.SUPERINFERENCE_DATA_DIR;
  try { fs.rmSync(dataDir, { recursive: true, force: true }); } catch { /* best-effort */ }
});
beforeEach(() => { resetAgentRegistry(); resetTeam(); });
afterEach(() => { resetAgentRegistry(); resetTeam(); });

/**
 * Run the task tool in run_in_background mode against a fake engine and resolve only
 * once the detached drain has fired SubagentStop (or a short timeout elapses), so the
 * full background lifecycle (start on spawn → stop on completion) is observable.
 */
async function runBackground(
  input: Record<string, unknown>,
  ctx: Partial<ToolContext> = {},
  events: Ev[] = [{ type: 'text_delta', text: 'bg-done' }],
): Promise<BgHarness> {
  const starts: SubagentContext[] = [];
  const stops: SubagentContext[] = [];
  let resolveStop!: () => void;
  const stopped = new Promise<void>(res => { resolveStop = res; });
  async function* fakeSubmit(_p: string) {
    for (const e of events) yield e as never;
  }
  const hookManager = {
    executeSubagentStart: async (c: SubagentContext) => { starts.push(c); },
    executeSubagentStop: async (c: SubagentContext) => { stops.push(c); resolveStop(); },
    executeTeammateIdle: async () => {},
  };
  const result = await taskTool.execute(
    { ...input, run_in_background: true },
    {
      cwd: dataDir,
      abortSignal: new AbortController().signal,
      _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
      _emitChild: () => {},
      _hookManager: hookManager as unknown as ToolContext['_hookManager'],
      _engineFactory: (_c: EngineConfig) => {
        void (_c.tools as ToolDefinition[]);
        return { submit: fakeSubmit, shutdown: () => {} } as never;
      },
      ...ctx,
    } as unknown as ToolContext,
  );
  // Wait for the detached drain to terminate and fire SubagentStop.
  await Promise.race([
    stopped,
    new Promise<void>(res => setTimeout(res, 2000)),
  ]);
  return { isError: result.isError, output: result.output, starts, stops };
}

// ---------------------------------------------------------------------------
// Gap U011 (claudeRef utils/agentContext.ts + swarm async lifecycle): a BACKGROUND
// subagent must fire the SAME SubagentStart/Stop lifecycle hooks — carrying the
// discriminated-union agentContextKind — that a foreground subagent does. Before
// this, the background spawn path fired NO lifecycle hook at all.
describe('task spawn (background) — fires the subagent lifecycle hooks', () => {
  it('a background mode-only spawn fires SubagentStart exactly once with subagent attribution', async () => {
    const h = await runBackground({ prompt: 'do x' }, { sessionId: 'sess-parent' });
    assert.equal(h.isError, undefined);
    assert.equal(h.starts.length, 1, 'background spawn fires SubagentStart once');
    const c = h.starts[0];
    assert.equal(c.invocationKind, 'spawn');
    assert.equal(c.isBuiltIn, true);
    assert.equal(c.analyticsAgentType, 'general');
    assert.equal(c.agentContextKind, 'subagent');
    assert.equal(c.invokingRequestId, 'sess-parent');
    assert.match(c.chainId as string, /^chain-[0-9a-f]+$/);
    assert.equal(typeof c.depth, 'number');
  });

  it('a background spawn fires SubagentStop once after the drain completes', async () => {
    const h = await runBackground({ prompt: 'do x' }, { sessionId: 'sess-parent' });
    assert.equal(h.stops.length, 1, 'background spawn fires SubagentStop once on completion');
    // Start and stop describe the SAME agent boundary (same lineage chainId).
    assert.equal(h.stops[0].chainId, h.starts[0].chainId);
  });

  it('a background named spawn joined to the active team is tagged as a teammate', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, {
      cwd: dataDir, abortSignal: new AbortController().signal,
    } as unknown as ToolContext);
    const h = await runBackground({ prompt: 'do x', name: 'worker' }, { sessionId: 'sess-parent' });
    assert.equal(h.isError, undefined);
    assert.equal(h.starts.length, 1);
    assert.equal(h.starts[0].agentContextKind, 'teammate');
    assert.equal(h.stops[0]?.agentContextKind, 'teammate');
  });
});

// ---------------------------------------------------------------------------
// buildSubagentHookCtx — the shared builder used by BOTH spawn paths. The raw mode
// (not the effective one) drives the analytics label so a fork/explore spawn is
// bucketed by what the caller asked for.
describe('buildSubagentHookCtx — shared lifecycle payload builder', () => {
  const baseCtx = { sessionId: 'parent-sess', agentId: '' } as unknown as ToolContext;

  it('derives the analytics label from the RAW mode, not the effective mode', () => {
    const ctx = buildSubagentHookCtx({
      context: baseCtx, taskId: 't1', prompt: 'p',
      mode: 'general', rawMode: 'explore', resumed: false, isBuiltIn: true,
      subagentType: undefined, chainId: 'chain-abc', depth: 2, isTeammate: false,
    });
    assert.equal(ctx.mode, 'general', 'ctx.mode is the effective mode');
    assert.equal(ctx.analyticsAgentType, 'explore', 'analytics bucket is the raw requested mode');
    assert.equal(ctx.invocationKind, 'spawn');
    assert.equal(ctx.agentContextKind, 'subagent');
    assert.equal(ctx.depth, 2);
  });

  it('marks a resume + teammate and roots invokingRequestId at the parent agent id', () => {
    const ctx = buildSubagentHookCtx({
      context: { sessionId: 'sess', agentId: 'agent-parent' } as unknown as ToolContext,
      taskId: 't2', prompt: 'p', mode: 'general', rawMode: 'general',
      resumed: true, isBuiltIn: false, subagentType: 'custom-agent',
      chainId: 'chain-xyz', depth: 1, isTeammate: true,
    });
    assert.equal(ctx.invocationKind, 'resume');
    assert.equal(ctx.agentContextKind, 'teammate');
    // A user-defined (not built-in) agent's type is collapsed to the privacy-safe
    // bucket so a private repo's agent names never leak into analytics.
    assert.equal(ctx.analyticsAgentType, 'user-defined');
    assert.equal(ctx.invokingRequestId, 'agent-parent');
  });

  it('falls back to taskId for ids when no session/agent id is present', () => {
    const ctx = buildSubagentHookCtx({
      context: {} as unknown as ToolContext,
      taskId: 'only-id', prompt: 'p', mode: 'general', rawMode: 'general',
      resumed: false, isBuiltIn: true, subagentType: undefined,
      chainId: 'c', depth: 0, isTeammate: false,
    });
    assert.equal(ctx.parentSessionId, 'only-id');
    assert.equal(ctx.subagentSessionId, 'only-id');
    assert.equal(ctx.invokingRequestId, 'only-id');
  });
});
