import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  taskTool,
  resetAgentRegistry,
  resolveBubblePermissionMode,
  collapseAgentTypeForAnalytics,
  FORK_BUBBLE_PERMISSION_MODE,
} from '../src/tools/task';
import { teamCreateTool, resetTeam } from '../src/tools/team';
import type { ToolContext, ToolDefinition, EngineConfig } from '../src/types';
import type { SubagentContext } from '../src/hooks';
import type { AgentDefinition, SkillManagerLike } from '../src/skills';

type Ev = Record<string, unknown>;

interface Harness {
  isError?: boolean;
  output: string;
  cfg: EngineConfig | null;
  starts: SubagentContext[];
}

/**
 * Run the task tool against a fake engine, capturing the sub-engine config and
 * every SubagentStart hook context (the attribution/lineage payload under test).
 */
async function runTask(
  input: Record<string, unknown>,
  ctx: Partial<ToolContext> = {},
  events: Ev[] = [{ type: 'text_delta', text: 'ok' }],
): Promise<Harness> {
  let cfg: EngineConfig | null = null;
  const starts: SubagentContext[] = [];
  async function* fakeSubmit(_p: string) {
    for (const e of events) yield e as never;
  }
  const hookManager = {
    executeSubagentStart: async (c: SubagentContext) => { starts.push(c); },
    executeSubagentStop: async () => {},
    executeTeammateIdle: async () => {},
  };
  const result = await taskTool.execute(input, {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
    _emitChild: () => {},
    _hookManager: hookManager as unknown as ToolContext['_hookManager'],
    _engineFactory: (c) => { cfg = c; void (c.tools as ToolDefinition[]); return { submit: fakeSubmit, shutdown: () => {} }; },
    ...ctx,
  } as unknown as ToolContext);
  return { isError: result.isError, output: result.output, cfg, starts };
}

/** Minimal SkillManager that serves a single user-defined (project-source) agent. */
function userAgentSkillManager(agent: AgentDefinition): SkillManagerLike {
  return {
    getAgent: (name: string) => (name === agent.name ? agent : null),
    listAgents: () => [agent],
  } as unknown as SkillManagerLike;
}

beforeEach(() => { resetAgentRegistry(); resetTeam(); });
afterEach(() => { resetAgentRegistry(); resetTeam(); });

// ---------------------------------------------------------------------------
// Gap U011/w84: resolveBubblePermissionMode — fork-child 'bubble' sentinel
// (claudeRef tools/AgentTool/forkSubagent.ts:L67 — FORK_AGENT.permissionMode='bubble')
describe('resolveBubblePermissionMode — fork child defers to the parent stance', () => {
  it('rewrites the bubble sentinel to the parent live mode', () => {
    assert.equal(resolveBubblePermissionMode(FORK_BUBBLE_PERMISSION_MODE, 'acceptEdits'), 'acceptEdits');
    assert.equal(resolveBubblePermissionMode('bubble', 'deny-all'), 'deny-all');
  });

  it('falls back to ask when the parent mode is not threaded', () => {
    assert.equal(resolveBubblePermissionMode('bubble', undefined), 'ask');
  });

  it('passes any non-bubble value (and undefined) through unchanged', () => {
    assert.equal(resolveBubblePermissionMode('plan', 'auto-allow'), 'plan');
    assert.equal(resolveBubblePermissionMode(undefined, 'auto-allow'), undefined);
  });
});

// ---------------------------------------------------------------------------
// Gap U011/w84: collapseAgentTypeForAnalytics — privacy-safe analytics label
// (claudeRef utils/agentContext.ts:L41-42 — isBuiltIn + 'user-defined' collapse)
describe('collapseAgentTypeForAnalytics — privacy-safe analytics label', () => {
  it('reports a built-in type verbatim', () => {
    assert.equal(collapseAgentTypeForAnalytics('explore', true), 'explore');
    assert.equal(collapseAgentTypeForAnalytics('code-reviewer', true), 'code-reviewer');
  });

  it('collapses a user-defined agent name to the opaque bucket', () => {
    assert.equal(collapseAgentTypeForAnalytics('my-secret-internal-agent', false), 'user-defined');
  });

  it('reports unknown for a built-in with no type', () => {
    assert.equal(collapseAgentTypeForAnalytics(undefined, true), 'unknown');
    assert.equal(collapseAgentTypeForAnalytics('', true), 'unknown');
  });
});

// ---------------------------------------------------------------------------
// Gap U011/w84: permission_mode 'bubble' end-to-end through the spawn path
describe('task spawn — permission_mode bubble resolves to the parent stance', () => {
  it('a bubble spawn adopts the parent live permission mode (not an error)', async () => {
    const h = await runTask({ prompt: 'do x', permission_mode: 'bubble' }, {
      _parentPermissionMode: 'acceptEdits',
    });
    assert.equal(h.isError, undefined);
    assert.equal(h.cfg?.permissionMode, 'acceptEdits');
  });

  it('a bubble spawn with no parent mode threaded falls back to ask', async () => {
    const h = await runTask({ prompt: 'do x', permission_mode: 'bubble' });
    assert.equal(h.isError, undefined);
    assert.equal(h.cfg?.permissionMode, 'ask');
  });
});

// ---------------------------------------------------------------------------
// Gap U011/w84: subagent attribution + lineage carried on the SubagentStart ctx
// (claudeRef utils/agentContext.ts:L41-53, utils/forkedAgent.ts:L451-455)
describe('task spawn — SubagentStart ctx carries attribution + lineage', () => {
  it('a mode-only spawn fires SubagentStart exactly once with built-in attribution', async () => {
    const h = await runTask({ prompt: 'do x' }, { sessionId: 'sess-parent' });
    assert.equal(h.starts.length, 1, 'once-per-invocation emission');
    const c = h.starts[0];
    assert.equal(c.invocationKind, 'spawn');
    assert.equal(c.isBuiltIn, true);
    assert.equal(c.analyticsAgentType, 'general');
    assert.equal(c.agentContextKind, 'subagent');
    assert.equal(c.invokingRequestId, 'sess-parent');
    assert.equal(typeof c.chainId, 'string');
    assert.match(c.chainId as string, /^chain-[0-9a-f]+$/);
    assert.equal(typeof c.depth, 'number');
  });

  it('a nested spawn roots invokingRequestId at the immediate parent agent id', async () => {
    const h = await runTask({ prompt: 'do x' }, { sessionId: 'sess-parent', agentId: 'agent-parent-42' });
    assert.equal(h.starts[0].invokingRequestId, 'agent-parent-42');
  });

  it('a user-defined named agent is reported as not-built-in and collapsed for analytics', async () => {
    const agent: AgentDefinition = {
      name: 'secret-agent', description: 'd', systemPrompt: 'be secret',
      filePath: '/x/secret-agent.md', source: 'project',
    };
    const h = await runTask({ prompt: 'do x', subagent_type: 'secret-agent' }, {
      sessionId: 'sess-parent', _skillManager: userAgentSkillManager(agent),
    });
    assert.equal(h.isError, undefined);
    assert.equal(h.starts[0].isBuiltIn, false);
    assert.equal(h.starts[0].analyticsAgentType, 'user-defined');
  });

  it('a named spawn joined to the active team is tagged as a teammate', async () => {
    await teamCreateTool.execute({ team_name: 'sq' }, {
      cwd: process.cwd(), abortSignal: new AbortController().signal,
    } as unknown as ToolContext);
    const h = await runTask({ prompt: 'do x', name: 'worker' }, { sessionId: 'sess-parent' });
    assert.equal(h.isError, undefined);
    assert.equal(h.starts[0].agentContextKind, 'teammate');
  });
});
