import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  WorkflowRuntime,
  workflowAgentCacheKey,
  resolveToolScope,
  normalizeAgentModel,
  formatAgentCompletion,
  mapWithConcurrency,
  type AgentOptions,
} from '../src/tools/workflow-runtime';
import { workflowTool } from '../src/tools/workflow';
import type { ToolContext } from '../src/types';
import type { AgentDefinition } from '../src/skills';

// U134 (w83) — additive parity for core/core/src/tools/workflow-runtime.ts:
//   Gap 0 (MED): per-agent tool allow/deny scoping (tools/disallowedTools) on
//     AgentOptions + cache key + handler (parity: loadAgentsDir tools/disallowedTools).
//   Gap 1 (MED): sub-agent completion affordance — agent-complete carries
//     toolUseCount + tokenCount + measured durationMs (parity: AgentTool UI
//     `Done (N tool uses · M tokens · Xs)`).
//   Gap 2 (LOW): model normalization with an 'inherit' sentinel.
//   Gap 3 (LOW): per-agent reasoning effort threaded into the sub-engine thinking.
//   Gap 4 (LOW): sliding-window concurrency for parallel()/pipeline() so NON-agent
//     work is bounded (parity: generators.ts all(generators, concurrencyCap)).

const delay = (ms: number): Promise<void> => new Promise(r => setTimeout(r, ms));

// ─────────────────────────── Gap 0: tool scoping ───────────────────────────

describe('U134 Gap 0 — per-agent tool allow/deny scoping', () => {
  it('resolveToolScope: allowlist selects, disallow subtracts, deny wins over allow', () => {
    const all = [{ name: 'a' }, { name: 'b' }, { name: 'c' }];
    const allowed = resolveToolScope(all, ['a', 'b']);
    assert.deepEqual(allowed.tools.map(t => t.name), ['a', 'b']);
    assert.deepEqual(allowed.invalid, []);
    const denied = resolveToolScope(all, ['a', 'b'], ['b']);
    assert.deepEqual(denied.tools.map(t => t.name), ['a']);
  });

  it("resolveToolScope: '*' means all tools (no allow-filter), deny still applies", () => {
    const all = [{ name: 'a' }, { name: 'b' }];
    assert.deepEqual(resolveToolScope(all, ['*']).tools.map(t => t.name), ['a', 'b']);
    assert.deepEqual(resolveToolScope(all, ['*'], ['a']).tools.map(t => t.name), ['b']);
  });

  it('resolveToolScope: reports unknown allow names as invalid', () => {
    const all = [{ name: 'a' }];
    const { tools, invalid } = resolveToolScope(all, ['a', 'nope']);
    assert.deepEqual(tools.map(t => t.name), ['a']);
    assert.deepEqual(invalid, ['nope']);
  });

  it('resolveToolScope: deny-only scope narrows the full set', () => {
    const all = [{ name: 'a' }, { name: 'b' }];
    assert.deepEqual(resolveToolScope(all, undefined, ['a']).tools.map(t => t.name), ['b']);
  });

  it('cache key incorporates tools/disallowedTools and is order-independent', () => {
    const base = workflowAgentCacheKey('p');
    const withTools = workflowAgentCacheKey('p', { tools: ['a', 'b'] });
    // Order of the allow list must not change the key (sorted internally).
    const withToolsReordered = workflowAgentCacheKey('p', { tools: ['b', 'a'] });
    const withOneTool = workflowAgentCacheKey('p', { tools: ['a'] });
    const withDeny = workflowAgentCacheKey('p', { disallowedTools: ['x'] });
    assert.notEqual(withTools, base);
    assert.equal(withTools, withToolsReordered);
    assert.notEqual(withTools, withOneTool);
    assert.notEqual(withDeny, base);
  });

  it('agent() threads per-call tools/disallowedTools through to the handler opts', async () => {
    let seen: AgentOptions | undefined;
    const rt = new WorkflowRuntime({ agentHandler: async (_p, o) => { seen = o; return 'ok'; } });
    const ctx = rt.createContext(undefined);
    await ctx.agent('do', { tools: ['file_read'], disallowedTools: ['bash'] });
    assert.deepEqual(seen?.tools, ['file_read']);
    assert.deepEqual(seen?.disallowedTools, ['bash']);
  });
});

// ──────────────────── Gap 1: completion affordance ────────────────────

describe('U134 Gap 1 — completion affordance (tool uses · tokens · duration)', () => {
  it('formatAgentCompletion renders the reference wording', () => {
    assert.equal(
      formatAgentCompletion({ toolUseCount: 3, tokenCount: 1500, durationMs: 2000 }),
      'Done (3 tool uses · 1,500 tokens · 2s)',
    );
    // Singular tool use.
    assert.equal(formatAgentCompletion({ toolUseCount: 1, tokenCount: 10, durationMs: 0 }), 'Done (1 tool use · 10 tokens · 0s)');
    // Unknown stats are omitted; all-unknown degrades to a bare 'Done'.
    assert.equal(formatAgentCompletion({ durationMs: 1000 }), 'Done (1s)');
    assert.equal(formatAgentCompletion({}), 'Done');
  });

  it('agent-complete carries toolUseCount, tokenCount (reported) + measured durationMs', async () => {
    const rt = new WorkflowRuntime({
      agentHandler: async (_p, _o, _id, report) => {
        report?.({ toolUseCount: 4 });
        report?.({ tokenCount: 2048 });
        await delay(5);
        return 'r';
      },
    });
    const events: any[] = [];
    rt.on('agent-complete', e => events.push(e));
    const ctx = rt.createContext(undefined);
    await ctx.agent('p', { label: 'summ' });
    // RED (pre-fix): agent-complete carried only {label, phase, agentNumber}.
    assert.equal(events.length, 1);
    assert.equal(events[0].toolUseCount, 4);
    assert.equal(events[0].tokenCount, 2048);
    assert.equal(typeof events[0].durationMs, 'number');
    assert.ok(events[0].durationMs >= 0);
  });

  it('cached short-circuit agent-complete surfaces a duration (counts unknown)', async () => {
    const store = new Map<string, unknown>();
    const cache = { get: (k: string) => store.get(k), set: (k: string, v: unknown) => { store.set(k, v); } };
    const rt = new WorkflowRuntime({ agentHandler: async () => 'r', cache });
    const events: any[] = [];
    rt.on('agent-complete', e => events.push(e));
    const ctx = rt.createContext(undefined);
    await ctx.agent('p');          // populates cache
    await ctx.agent('p');          // cached short-circuit
    const cachedEvent = events.find(e => e.cached);
    assert.ok(cachedEvent, 'a cached agent-complete must be emitted');
    assert.equal(typeof cachedEvent.durationMs, 'number');
  });
});

// ──────────────────── Gap 2: model inherit sentinel ────────────────────

describe('U134 Gap 2 — normalizeAgentModel (inherit sentinel)', () => {
  it("maps 'inherit'/blank to undefined and trims otherwise", () => {
    assert.equal(normalizeAgentModel('inherit'), undefined);
    assert.equal(normalizeAgentModel('  INHERIT  '), undefined);
    assert.equal(normalizeAgentModel(''), undefined);
    assert.equal(normalizeAgentModel('   '), undefined);
    assert.equal(normalizeAgentModel(undefined), undefined);
    assert.equal(normalizeAgentModel('opus'), 'opus');
    assert.equal(normalizeAgentModel('  gpt-5 '), 'gpt-5');
  });
});

// ──────────────────── Gap 4: sliding-window scheduler ────────────────────

describe('U134 Gap 4 — mapWithConcurrency sliding window', () => {
  it('preserves input order while completing out of order', async () => {
    const items = [30, 10, 20];
    const out = await mapWithConcurrency(items, 3, async (ms, i) => { await delay(ms); return `${i}:${ms}`; });
    assert.deepEqual(out, ['0:30', '1:10', '2:20']);
  });

  it('never runs more than cap workers at once', async () => {
    let live = 0, peak = 0;
    await mapWithConcurrency(Array.from({ length: 8 }, (_, i) => i), 3, async () => {
      live++; peak = Math.max(peak, live); await delay(10); live--; return 1;
    });
    assert.ok(peak <= 3, `peak ${peak} must be <= 3`);
    assert.ok(peak >= 2, `peak ${peak} should overlap with a cap > 1`);
  });

  it('handles an empty list and a cap larger than the item count', async () => {
    assert.deepEqual(await mapWithConcurrency([], 4, async () => 1), []);
    assert.deepEqual(await mapWithConcurrency([1, 2], 10, async v => v * 2), [2, 4]);
  });
});

describe('U134 Gap 4 — parallel()/pipeline() bound NON-agent work', () => {
  afterEach(() => { delete process.env.AMI_WORKFLOW_PREFIX_STAGGER_MS; });

  it('parallel() caps concurrent non-agent thunks at maxConcurrent', async () => {
    process.env.AMI_WORKFLOW_PREFIX_STAGGER_MS = '0';
    const rt = new WorkflowRuntime({ agentHandler: async p => p, maxConcurrent: 2 });
    const ctx = rt.createContext(undefined);
    let live = 0, peak = 0;
    // These thunks never call agent(), so the per-agent semaphore does NOT bound
    // them. RED (pre-fix): parallel() launched all 6 at once → peak 6.
    const thunks = Array.from({ length: 6 }, () => async () => {
      live++; peak = Math.max(peak, live); await delay(15); live--; return 'x';
    });
    const res = await ctx.parallel(thunks);
    assert.equal(res.length, 6);
    assert.ok(peak <= 2, `non-agent parallel peak ${peak} must be <= maxConcurrent(2)`);
  });

  it('parallel() still returns null for a failed thunk, in order', async () => {
    process.env.AMI_WORKFLOW_PREFIX_STAGGER_MS = '0';
    const rt = new WorkflowRuntime({ agentHandler: async p => p, maxConcurrent: 4 });
    const ctx = rt.createContext(undefined);
    const res = await ctx.parallel([
      () => Promise.resolve('good'),
      () => Promise.reject(new Error('bad')),
      () => Promise.resolve('also'),
    ]);
    assert.deepEqual(res, ['good', null, 'also']);
  });

  it('pipeline() caps concurrent non-agent item chains at maxConcurrent', async () => {
    const rt = new WorkflowRuntime({ agentHandler: async p => p, maxConcurrent: 2 });
    const ctx = rt.createContext(undefined);
    let live = 0, peak = 0;
    const res = await ctx.pipeline(
      [1, 2, 3, 4, 5, 6],
      async (prev: number) => { live++; peak = Math.max(peak, live); await delay(15); live--; return prev * 2; },
    );
    assert.deepEqual(res, [2, 4, 6, 8, 10, 12]);
    assert.ok(peak <= 2, `pipeline peak ${peak} must be <= maxConcurrent(2)`);
  });
});

// ──────────── E2E through the workflow tool (handler + engine factory) ────────────

const dirs: string[] = [];
afterEach(() => { for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true }); });
function mkcwd(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wf-u134-'));
  dirs.push(d);
  return d;
}
function agentDef(partial: Partial<AgentDefinition> & { name: string }): AgentDefinition {
  return { description: 'test agent', systemPrompt: 'base prompt', filePath: '/virtual', ...partial };
}
function capturingCtx(
  cwd: string,
  agents: AgentDefinition[] = [],
  submit?: () => AsyncGenerator<any>,
): { ctx: ToolContext; configs: any[] } {
  const byName = new Map(agents.map(a => [a.name, a]));
  const configs: any[] = [];
  const defaultSubmit = async function* () { yield { type: 'text_delta', text: 'ok' } as any; };
  const ctx: any = {
    cwd,
    abortSignal: new AbortController().signal,
    _providerConfig: { provider: 'test', model: 'base-model', baseUrl: 'u', apiKey: 'k' },
    _engineFactory: (config: any) => {
      configs.push(config);
      return { submit: submit ?? defaultSubmit, shutdown() {} };
    },
    _skillManager: { getAgent: (n: string) => byName.get(n) ?? null, listAgents: () => agents },
  };
  return { ctx, configs };
}

describe('U134 E2E — workflow tool wires the new AgentOptions', () => {
  it('Gap 0: per-call tools/disallowedTools scope the sub-engine toolset', async () => {
    const { ctx, configs } = capturingCtx(mkcwd());
    const script = "export const meta = { name: 'm', description: 'd' }\nreturn await agent('do', { tools: ['file_read', 'grep', 'bash'], disallowedTools: ['bash'] })";
    const r = await workflowTool.execute!({ script }, ctx);
    assert.equal(r.isError, false);
    const names = (configs[0].tools as any[]).map(t => t.name);
    assert.ok(names.includes('file_read') && names.includes('grep'));
    assert.ok(!names.includes('bash'), 'per-call disallowedTools must subtract bash');
  });

  it('Gap 0: an unknown per-call tool errors and spawns nothing', async () => {
    const { ctx, configs } = capturingCtx(mkcwd());
    const script = "export const meta = { name: 'm', description: 'd' }\nreturn await agent('do', { tools: ['no_such_tool'] })";
    const r = await workflowTool.execute!({ script }, ctx);
    assert.equal(configs.length, 0);
    assert.match(r.output, /unknown tool\(s\): no_such_tool/);
  });

  it('Gap 2: model "inherit" falls back to the parent model', async () => {
    const { ctx, configs } = capturingCtx(mkcwd());
    const script = "export const meta = { name: 'm', description: 'd' }\nreturn await agent('do', { model: 'inherit' })";
    await workflowTool.execute!({ script }, ctx);
    // RED (pre-fix): 'inherit' flowed through as a literal model override.
    assert.equal(configs[0].provider.model, 'base-model');
  });

  it('Gap 3: per-call effort enables sub-engine thinking at that level', async () => {
    const { ctx, configs } = capturingCtx(mkcwd());
    const script = "export const meta = { name: 'm', description: 'd' }\nreturn await agent('do', { effort: 'high' })";
    await workflowTool.execute!({ script }, ctx);
    assert.deepEqual(configs[0].thinking, { enabled: true, level: 'high' });
  });

  it('Gap 3: an invalid effort does not enable thinking', async () => {
    const { ctx, configs } = capturingCtx(mkcwd());
    const script = "export const meta = { name: 'm', description: 'd' }\nreturn await agent('do', { effort: 'bogus' })";
    await workflowTool.execute!({ script }, ctx);
    assert.equal(configs[0].thinking, undefined);
  });

  it('Gap 3: an agentType effort applies when no per-call effort is given', async () => {
    const { ctx, configs } = capturingCtx(mkcwd(), [agentDef({ name: 'deep', effort: 'medium' })]);
    const script = "export const meta = { name: 'm', description: 'd' }\nreturn await agent('do', { agentType: 'deep' })";
    await workflowTool.execute!({ script }, ctx);
    assert.deepEqual(configs[0].thinking, { enabled: true, level: 'medium' });
  });

  it('Gap 1: the tool output surfaces a per-agent completion summary', async () => {
    const submit = async function* () {
      yield { type: 'tool_use_start', toolName: 'file_read', input: {} } as any;
      yield { type: 'tool_use_start', toolName: 'grep', input: {} } as any;
      yield { type: 'usage_update', stats: { totalTokens: 1234 } } as any;
      yield { type: 'text_delta', text: 'done' } as any;
    };
    const { ctx } = capturingCtx(mkcwd(), [], submit);
    const script = "export const meta = { name: 'm', description: 'd' }\nreturn await agent('do', { label: 'reader' })";
    const r = await workflowTool.execute!({ script }, ctx);
    // RED (pre-fix): no "Agents:" summary + no per-agent tool-use/token/duration line.
    assert.match(r.output, /Agents:/);
    assert.match(r.output, /reader: Done \(2 tool uses · 1,234 tokens · \d+s\)/);
  });
});
