import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  taskTool,
  resetAgentRegistry,
  defaultExploreModel,
  EXPLORE_MODEL_ENV,
  FORK_NO_SUBSPAWN_DIRECTIVE,
  SUBAGENT_RETURN_DIRECTIVE,
  buildCacheStatsClause,
  buildCriticalReminderBlock,
} from '../src/tools/task';
import type { ToolContext, ToolDefinition, EngineConfig } from '../src/types';

type Ev = Record<string, unknown>;

interface Harness {
  isError?: boolean;
  output: string;
  cfg: EngineConfig | null;
  submittedPrompt: string | null;
  subagentHookCtxs: Array<Record<string, unknown>>;
}

/** Run the task tool against a fake engine, capturing the sub-engine config, the
 *  submitted prompt, and every SubagentStart/Stop hook payload. */
async function runTask(
  input: Record<string, unknown>,
  ctx: Partial<ToolContext> = {},
  events: Ev[] = [{ type: 'text_delta', text: 'ok' }],
): Promise<Harness> {
  let cfg: EngineConfig | null = null;
  let submittedPrompt: string | null = null;
  const subagentHookCtxs: Array<Record<string, unknown>> = [];
  async function* fakeSubmit(p: string) {
    submittedPrompt = p;
    for (const e of events) yield e as never;
  }
  const result = await taskTool.execute(input, {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
    _hookManager: {
      executePreToolUse: async () => ({ action: 'allow' }),
      executeSubagentStart: async (c: Record<string, unknown>) => { subagentHookCtxs.push({ phase: 'start', ...c }); },
      executeSubagentStop: async (c: Record<string, unknown>) => { subagentHookCtxs.push({ phase: 'stop', ...c }); },
    } as unknown as ToolContext['_hookManager'],
    _engineFactory: (c) => { cfg = c; void (c.tools as ToolDefinition[]); return { submit: fakeSubmit, shutdown: () => {} }; },
    ...ctx,
  } as unknown as ToolContext);
  return { isError: result.isError, output: result.output, cfg, submittedPrompt, subagentHookCtxs };
}

// ---------------------------------------------------------------------------
// Gap: True parent-session attribution edge for subagents (agentContext.ts:L35-85)
describe('SubagentStart/Stop parentSessionId = invoking engine session (gap: parent attribution)', () => {
  beforeEach(() => resetAgentRegistry());

  it('reports the PARENT session id, not the child taskId', async () => {
    const { subagentHookCtxs, output } = await runTask(
      { prompt: 'go', mode: 'explore' },
      { sessionId: 'parent-session-123' },
    );
    const start = subagentHookCtxs.find(c => c.phase === 'start');
    assert.ok(start, 'SubagentStart hook fired');
    // RED on revert: the old code passed the child's taskId as parentSessionId.
    assert.equal(start!.parentSessionId, 'parent-session-123');
    const childId = String(start!.subagentSessionId);
    assert.notEqual(start!.parentSessionId, childId, 'parentSessionId must not be the child id');
    // The deliverable still completes.
    assert.ok(output.includes('ok'));
  });

  it('falls back to the child id in a bare context with no session id', async () => {
    const { subagentHookCtxs } = await runTask({ prompt: 'go', mode: 'explore' });
    const start = subagentHookCtxs.find(c => c.phase === 'start');
    assert.ok(typeof start!.parentSessionId === 'string' && (start!.parentSessionId as string).length > 0);
  });
});

// ---------------------------------------------------------------------------
// Gap: invocationKind ('spawn' | 'resume') attribution (agentContext.ts:L43-53)
describe('SubagentContext invocationKind (gap: spawn/resume attribution)', () => {
  beforeEach(() => resetAgentRegistry());

  it('a fresh spawn is tagged spawn', async () => {
    const { subagentHookCtxs } = await runTask({ prompt: 'go', mode: 'explore' });
    assert.equal(subagentHookCtxs.find(c => c.phase === 'start')!.invocationKind, 'spawn');
  });

  it('a resume of an existing agent is tagged resume', async () => {
    // First spawn registers the agent session under its name.
    await runTask({ prompt: 'start', mode: 'explore', name: 'scout' });
    const { subagentHookCtxs, isError, output } = await runTask({ prompt: 'again', resume: 'scout' });
    assert.ok(!isError, `resume should succeed, got: ${output}`);
    assert.equal(subagentHookCtxs.find(c => c.phase === 'start')!.invocationKind, 'resume');
  });
});

// ---------------------------------------------------------------------------
// Gap: explore agent defaults to a fast/cheap model (exploreAgent.ts:L76-82)
describe('defaultExploreModel + explore-mode fast default (gap: explore haiku)', () => {
  const saved = process.env[EXPLORE_MODEL_ENV];
  beforeEach(() => resetAgentRegistry());
  afterEach(() => {
    if (saved === undefined) delete process.env[EXPLORE_MODEL_ENV];
    else process.env[EXPLORE_MODEL_ENV] = saved;
  });

  it('downshifts a Claude-family parent to the fast model', () => {
    delete process.env[EXPLORE_MODEL_ENV];
    assert.equal(defaultExploreModel('claude-opus-4-8'), 'claude-haiku-4-5');
    assert.equal(defaultExploreModel(undefined), 'claude-haiku-4-5');
  });

  it('keeps a non-Claude parent on its own model', () => {
    delete process.env[EXPLORE_MODEL_ENV];
    assert.equal(defaultExploreModel('gpt-4o'), undefined);
  });

  it('AMI_EXPLORE_MODEL override wins; inherit keeps the parent', () => {
    process.env[EXPLORE_MODEL_ENV] = 'sonnet';
    assert.equal(defaultExploreModel('claude-opus-4-8'), 'claude-sonnet-4');
    process.env[EXPLORE_MODEL_ENV] = 'inherit';
    assert.equal(defaultExploreModel('claude-opus-4-8'), undefined);
  });

  it('an explore spawn runs on the fast model when no model is requested', async () => {
    delete process.env[EXPLORE_MODEL_ENV];
    const { cfg } = await runTask({ prompt: 'find x', mode: 'explore' });
    assert.equal(cfg!.provider!.model, 'claude-haiku-4-5');
  });

  it('an explicit model override is NOT downshifted', async () => {
    delete process.env[EXPLORE_MODEL_ENV];
    const { cfg } = await runTask({ prompt: 'find x', mode: 'explore', model: 'opus' });
    assert.equal(cfg!.provider!.model, 'claude-opus-4-8');
  });
});

// ---------------------------------------------------------------------------
// Gap: fork-child recursion guard (forkSubagent.ts:L73-89)
describe('fork no-subspawn recursion guard (gap: isInForkChild directive)', () => {
  beforeEach(() => resetAgentRegistry());

  it('a fork child is told not to spawn sub-agents, but the prompt still leads with the return directive', async () => {
    const { submittedPrompt } = await runTask({ prompt: 'continue the work', mode: 'fork' });
    assert.ok(submittedPrompt!.startsWith(SUBAGENT_RETURN_DIRECTIVE), 'fork still leads with the return directive');
    assert.ok(submittedPrompt!.includes(FORK_NO_SUBSPAWN_DIRECTIVE), 'fork carries the recursion guard');
  });

  it('a non-fork general subagent does NOT get the fork recursion guard', async () => {
    const { submittedPrompt } = await runTask({ prompt: 'do a thing', mode: 'general' });
    assert.ok(!submittedPrompt!.includes(FORK_NO_SUBSPAWN_DIRECTIVE));
  });
});

// ---------------------------------------------------------------------------
// Gap: criticalSystemReminder config knob (verificationAgent.ts:L150-151)
describe('criticalSystemReminder injected into the named agent (gap: critical reminder)', () => {
  beforeEach(() => resetAgentRegistry());

  it('buildCriticalReminderBlock wraps non-blank text and drops blanks', () => {
    assert.equal(buildCriticalReminderBlock(undefined), '');
    assert.equal(buildCriticalReminderBlock('   '), '');
    const block = buildCriticalReminderBlock('You MUST end with VERDICT:');
    assert.ok(block.includes('[CRITICAL'));
    assert.ok(block.includes('You MUST end with VERDICT:'));
  });

  it('a subagent_type with criticalSystemReminder appends it to the agent system prompt', async () => {
    const fakeSkillManager = {
      getAgent: (t: string) => t === 'verifier'
        ? {
          name: 'verifier', systemPrompt: 'VERIFIER IDENTITY', permissionMode: 'auto-allow',
          tools: ['file_read'], criticalSystemReminder: 'You CANNOT write files in the project.',
        }
        : undefined,
      listAgents: () => [{ name: 'verifier' }],
      getSkill: () => undefined,
    };
    const { cfg } = await runTask(
      { prompt: 'verify', subagent_type: 'verifier' },
      { _skillManager: fakeSkillManager as unknown as ToolContext['_skillManager'] },
    );
    assert.ok(cfg!.systemPrompt!.startsWith('VERIFIER IDENTITY'));
    assert.ok(cfg!.systemPrompt!.includes('You CANNOT write files in the project.'));
    assert.ok(cfg!.systemPrompt!.includes('[CRITICAL'));
  });
});

// ---------------------------------------------------------------------------
// Gap: per-fork/subagent max_output_tokens cap (forkedAgent.ts:L96-103)
describe('max_output_tokens caps the sub-engine output budget (gap: maxOutputTokens)', () => {
  beforeEach(() => resetAgentRegistry());

  it('sizes the sub-engine provider maxTokens', async () => {
    const { cfg } = await runTask({ prompt: 'short summary', mode: 'general', max_output_tokens: 512 });
    assert.equal(cfg!.provider!.maxTokens, 512);
  });

  it('a non-positive value is ignored (no cap)', async () => {
    const { cfg } = await runTask({ prompt: 'x', mode: 'general', max_output_tokens: 0 });
    assert.equal(cfg!.provider!.maxTokens, undefined);
  });

  it('the schema advertises max_output_tokens', () => {
    const props = taskTool.inputSchema.properties as Record<string, unknown>;
    assert.ok(props.max_output_tokens, 'max_output_tokens is advertised');
  });
});

// ---------------------------------------------------------------------------
// Gap: per-subagent cache-hit introspection (forkedAgent.ts:L606-654)
describe('cache-hit introspection in the completion stats footer (gap: cacheRead/cacheHitRate)', () => {
  beforeEach(() => resetAgentRegistry());

  it('buildCacheStatsClause renders tokens + rate, empty when no signal', () => {
    assert.equal(buildCacheStatsClause(0, undefined), '');
    assert.equal(buildCacheStatsClause(1200, 0.75), ', 1200 cached tokens, 75% cache hit');
    assert.equal(buildCacheStatsClause(500, undefined), ', 500 cached tokens');
  });

  it('the footer surfaces cache stats from usage_update', async () => {
    const { output } = await runTask(
      { prompt: 'go', mode: 'general' },
      {},
      [
        { type: 'text_delta', text: 'done' },
        { type: 'usage_update', stats: { totalTokens: 100, cachedPromptTokens: 80, cacheHitRate: 0.8 } },
      ],
    );
    assert.ok(output.includes('80 cached tokens'), `footer missing cache tokens, got: ${output}`);
    assert.ok(output.includes('80% cache hit'), `footer missing cache rate, got: ${output}`);
  });

  it('the footer omits the cache clause when no cache signal is reported', async () => {
    const { output } = await runTask({ prompt: 'go', mode: 'general' });
    assert.ok(!output.includes('cached tokens'), `footer should omit cache clause, got: ${output}`);
    assert.match(output, /\[Agent stats: \d+ tool calls, \d+ tokens, \d+(?:\.\d+)?s\]/);
  });
});
