import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  taskTool,
  resetAgentRegistry,
  resolvePromptGatePermissionMode,
  recordAgentMeta,
  getAgentMeta,
  EXPLORE_SUBAGENT_SYSTEM_PROMPT,
} from '../src/tools/task';
import type { ToolContext, EngineEvent, EngineConfig } from '../src/types';

interface RunCapture {
  isError?: boolean;
  output: string;
  /** Every EngineConfig the tool handed to the engine factory, in spawn order. */
  configs: EngineConfig[];
  /** Every prompt submitted to a spawned sub-engine, in spawn order. */
  prompts: string[];
}

/** Run the task tool against a fake engine, capturing the resolved sub-engine
 *  config(s) and submitted prompt(s). The fake engine streams a tiny clean turn so
 *  the foreground path produces a deliverable and completes. */
async function runTask(
  input: Record<string, unknown>,
  over: Partial<ToolContext> = {},
): Promise<RunCapture> {
  const configs: EngineConfig[] = [];
  const prompts: string[] = [];
  async function* fakeSubmit(p: string) {
    prompts.push(p);
    yield { type: 'text_delta', text: 'result' } as never;
    yield { type: 'turn_complete', content: 'result' } as never;
    yield { type: 'done', exitReason: 'end_turn' } as never;
  }
  const result = await taskTool.execute(input, {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
    _emitChild: () => {},
    _engineFactory: (cfg: EngineConfig) => {
      configs.push(cfg);
      return { submit: fakeSubmit, shutdown: () => {} };
    },
    ...over,
  } as unknown as ToolContext);
  return { isError: result.isError, output: result.output, configs, prompts };
}

// ---------------------------------------------------------------------------
// Gap: allowPermissionPrompts per-spawn flag (default false → auto-deny unlisted)
// (swarm/backends/types.ts:L222-224, InProcessBackend.ts:L128)
// ---------------------------------------------------------------------------
describe('resolvePromptGatePermissionMode — allow_permission_prompts gate', () => {
  it('returns undefined when the flag is absent (no contribution)', () => {
    assert.equal(resolvePromptGatePermissionMode(undefined), undefined);
    assert.equal(resolvePromptGatePermissionMode(null), undefined);
  });
  it('maps false → deny-all (unlisted tools auto-denied, never prompted)', () => {
    assert.equal(resolvePromptGatePermissionMode(false), 'deny-all');
    assert.equal(resolvePromptGatePermissionMode('false'), 'deny-all');
  });
  it('maps true → ask (interactive prompt handler)', () => {
    assert.equal(resolvePromptGatePermissionMode(true), 'ask');
    assert.equal(resolvePromptGatePermissionMode('true'), 'ask');
  });
});

describe('task tool — allow_permission_prompts resolves the subagent permission mode', () => {
  beforeEach(() => resetAgentRegistry());

  it('false → the subagent runs in deny-all (auto-deny unlisted tools)', async () => {
    const { configs } = await runTask({ prompt: 'go', description: 'do it', allow_permission_prompts: false });
    assert.equal(configs[0].permissionMode, 'deny-all');
  });

  it('true → the subagent runs in ask (interactive prompts allowed)', async () => {
    const { configs } = await runTask({ prompt: 'go', description: 'do it', allow_permission_prompts: true });
    assert.equal(configs[0].permissionMode, 'ask');
  });

  it('omitted → the existing default (ask for general) is unchanged', async () => {
    const { configs } = await runTask({ prompt: 'go', description: 'do it' });
    assert.equal(configs[0].permissionMode, 'ask');
  });

  it('an explicit permission_mode overrides the gate', async () => {
    const { configs } = await runTask({
      prompt: 'go', description: 'do it', allow_permission_prompts: false, permission_mode: 'acceptEdits',
    });
    assert.equal(configs[0].permissionMode, 'acceptEdits');
  });

  it('advertises allow_permission_prompts as a boolean in the input schema', () => {
    const prop = (taskTool.inputSchema.properties as Record<string, { type?: string }>).allow_permission_prompts;
    assert.ok(prop && prop.type === 'boolean');
  });
});

// ---------------------------------------------------------------------------
// Gap: worktree_path spawn option — point a subagent at an EXISTING worktree
// (swarm/backends/types.ts:L216-217)
// ---------------------------------------------------------------------------
describe('task tool — worktree_path (existing worktree) spawn option', () => {
  let base: string;
  let wt: string;
  beforeEach(() => {
    resetAgentRegistry();
    base = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ami-wt-')));
    wt = path.join(base, 'worktree');
    fs.mkdirSync(wt);
  });
  afterEach(() => fs.rmSync(base, { recursive: true, force: true }));

  it('runs the subagent in the supplied existing worktree + prepends the worktree notice', async () => {
    const { configs, prompts } = await runTask(
      { prompt: 'go', description: 'do it', worktree_path: wt },
      { cwd: base },
    );
    assert.equal(configs[0].cwd, wt);
    assert.match(prompts[0], /isolated git worktree at/);
    assert.match(prompts[0], new RegExp(wt.replace(/[/\\]/g, '\\$&')));
    // Caller owns the lifecycle — the supplied worktree is NOT auto-removed.
    assert.equal(fs.existsSync(wt), true);
  });

  it('rejects worktree_path combined with isolation:"worktree"', async () => {
    const { isError, output } = await runTask(
      { prompt: 'go', description: 'do it', worktree_path: wt, isolation: 'worktree' },
      { cwd: base },
    );
    assert.equal(isError, true);
    assert.match(output, /mutually exclusive/);
  });

  it('rejects worktree_path combined with cwd', async () => {
    const { isError, output } = await runTask(
      { prompt: 'go', description: 'do it', worktree_path: wt, cwd: base },
      { cwd: base },
    );
    assert.equal(isError, true);
    assert.match(output, /mutually exclusive/);
  });

  it('rejects a worktree_path outside the parent boundary', async () => {
    const outside = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ami-outside-')));
    try {
      const { isError, output } = await runTask(
        { prompt: 'go', description: 'do it', worktree_path: outside },
        { cwd: base },
      );
      assert.equal(isError, true);
      assert.match(output, /outside the parent/);
    } finally {
      fs.rmSync(outside, { recursive: true, force: true });
    }
  });

  it('advertises worktree_path in the input schema', () => {
    const prop = (taskTool.inputSchema.properties as Record<string, { type?: string }>).worktree_path;
    assert.ok(prop && prop.type === 'string');
  });
});

// ---------------------------------------------------------------------------
// Gap: resume restores the original agent's identity/defaults from metadata
// (resumeAgent.ts:L99-114)
// ---------------------------------------------------------------------------
describe('recordAgentMeta / getAgentMeta — resume identity store', () => {
  beforeEach(() => resetAgentRegistry());
  it('stores only the non-empty identity fields', () => {
    recordAgentMeta('agent-1', { agentType: 'reviewer', model: '', permissionMode: 'auto-allow', description: undefined });
    assert.deepEqual(getAgentMeta('agent-1'), { agentType: 'reviewer', permissionMode: 'auto-allow' });
  });
  it('resetAgentRegistry clears the metadata store', () => {
    recordAgentMeta('agent-2', { agentType: 'x' });
    resetAgentRegistry();
    assert.equal(getAgentMeta('agent-2'), undefined);
  });
});

describe('task tool — resume restores the original agent identity/defaults', () => {
  beforeEach(() => resetAgentRegistry());

  it('restores the agent_type (and thus its system prompt) when the resume call names none', async () => {
    const skillManager = {
      getAgent: () => ({ name: 'ag', systemPrompt: 'BE A NAMED AGENT' }),
      listAgents: () => [{ name: 'ag' }],
    };
    // Initial spawn under a named agent type.
    await runTask(
      { prompt: 'first', description: 'label', subagent_type: 'ag', name: 'worker' },
      { _skillManager: skillManager as never },
    );
    // Resume by name with NO subagent_type — the original identity is restored.
    const { configs } = await runTask(
      { prompt: 'again', resume: 'worker' },
      { _skillManager: skillManager as never },
    );
    assert.equal(configs[0].systemPrompt, 'BE A NAMED AGENT');
  });

  it('restores the permission_mode when the resume call sets none', async () => {
    await runTask({ prompt: 'first', description: 'label', permission_mode: 'acceptEdits', name: 'w2' });
    const { configs } = await runTask({ prompt: 'again', resume: 'w2' });
    assert.equal(configs[0].permissionMode, 'acceptEdits');
  });

  it('an explicit value on the resume call overrides the restored default', async () => {
    await runTask({ prompt: 'first', description: 'label', permission_mode: 'acceptEdits', name: 'w3' });
    const { configs } = await runTask({ prompt: 'again', resume: 'w3', permission_mode: 'deny-all' });
    assert.equal(configs[0].permissionMode, 'deny-all');
  });
});

// ---------------------------------------------------------------------------
// Gap: explore prompt — read-only prohibition wording + parallel-calls framing +
// fallback find/grep guidance (exploreAgent.ts:L14-54)
// ---------------------------------------------------------------------------
describe('EXPLORE_SUBAGENT_SYSTEM_PROMPT — explicit read-only + fast/parallel + fallback search', () => {
  it('states the explicit read-only prohibition including redirects/heredocs', () => {
    assert.match(EXPLORE_SUBAGENT_SYSTEM_PROMPT, /READ-ONLY/);
    assert.match(EXPLORE_SUBAGENT_SYSTEM_PROMPT, /redirect operators/i);
  });
  it('encourages spawning multiple parallel tool calls for speed', () => {
    assert.match(EXPLORE_SUBAGENT_SYSTEM_PROMPT, /parallel tool calls/i);
  });
  it('gives a fallback find/grep-via-bash search path', () => {
    assert.match(EXPLORE_SUBAGENT_SYSTEM_PROMPT, /find.*grep.*via bash/i);
  });
});

// ---------------------------------------------------------------------------
// Gap: background launch result surfaces the output file + a progress-check hint
// (AgentTool.tsx:L1327-1329)
// ---------------------------------------------------------------------------
describe('task tool — background launch result surfaces the output file and progress affordance', () => {
  let cwd: string;
  beforeEach(() => {
    resetAgentRegistry();
    cwd = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ami-bg-')));
  });
  afterEach(() => fs.rmSync(cwd, { recursive: true, force: true }));

  it('includes the output_file path and a non-blocking progress-check hint', async () => {
    const { output } = await runTask(
      { prompt: 'go', description: 'bg task', run_in_background: true },
      { cwd },
    );
    assert.match(output, /Output file:/);
    assert.match(output, /\.output/);
    assert.match(output, /check progress/i);
    assert.match(output, /task_output/);
  });
});
