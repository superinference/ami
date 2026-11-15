import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import * as child_process from 'child_process';
import {
  taskTool,
  resetAgentRegistry,
  uniqueAgentName,
  generateTaskId,
  getAgentByName,
  GIT_HARDENING_ARGS,
  DEFAULT_FORK_MAX_TURNS,
} from '../src/tools/task';
import type { ToolContext, ToolDefinition } from '../src/types';

type Ev = Record<string, unknown>;

/** Run the task tool with a fake engine, capturing the config + submitted prompt. */
async function runTask(
  input: Record<string, unknown>,
  ctx: Partial<ToolContext> = {},
  events: Ev[] = [{ type: 'text_delta', text: 'ok' }],
): Promise<{
  isError?: boolean;
  output: string;
  cfg: { provider?: { model?: string }; cwd?: string; maxTurns?: number; permissionMode?: string } | null;
  toolNames: string[];
  submittedPrompt: string | null;
  hookInput: Record<string, unknown> | null;
}> {
  let cfg: { maxTurns?: number; permissionMode?: string } | null = null;
  let toolNames: string[] = [];
  let submittedPrompt: string | null = null;
  let hookInput: Record<string, unknown> | null = null;
  async function* fakeSubmit(p: string) {
    submittedPrompt = p;
    for (const e of events) yield e as never;
  }
  const result = await taskTool.execute(input, {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
    _hookManager: ctx._hookManager ?? ({
      executePreToolUse: async (args: { toolInput?: Record<string, unknown> }) => { hookInput = args.toolInput ?? null; return { action: 'allow' }; },
      executeSubagentStart: async () => {},
      executeSubagentStop: async () => {},
    } as unknown as ToolContext['_hookManager']),
    _engineFactory: (c) => { cfg = c as typeof cfg; toolNames = (c.tools as ToolDefinition[]).map(t => t.name); return { submit: fakeSubmit }; },
    ...ctx,
  } as unknown as ToolContext);
  return { isError: result.isError, output: result.output, cfg, toolNames, submittedPrompt, hookInput };
}

// ---------------------------------------------------------------------------
describe('generateTaskId — high-entropy, symlink-resistant IDs (gap #34)', () => {
  it('produces a 16-char hex body (8 bytes = 2^64) with the given prefix', () => {
    const id = generateTaskId();
    assert.match(id, /^agent-[0-9a-f]{16}$/);
    assert.equal(generateTaskId('bg').slice(0, 3), 'bg-');
  });
  it('is effectively unique across many draws', () => {
    const seen = new Set<string>();
    for (let i = 0; i < 5000; i++) seen.add(generateTaskId());
    assert.equal(seen.size, 5000, 'no collisions across 5000 ids');
  });
  it('the task tool stamps a high-entropy id in its background result', async () => {
    const { output } = await runTask({ prompt: 'bg', run_in_background: true });
    assert.match(output, /Background agent started: agent-[0-9a-f]{16}/);
  });
});

// ---------------------------------------------------------------------------
describe('uniqueAgentName — collision suffixing (gaps #115/#116)', () => {
  beforeEach(() => resetAgentRegistry());

  it('returns the name verbatim when free, then suffixes on collision', async () => {
    assert.equal(uniqueAgentName('scout'), 'scout');
    await runTask({ prompt: 'a', mode: 'explore', name: 'scout' });
    assert.ok(getAgentByName('scout'), 'first registered under base name');
    // A second spawn reusing the name must NOT overwrite the first.
    await runTask({ prompt: 'b', mode: 'explore', name: 'scout' });
    assert.ok(getAgentByName('scout'), 'first agent still addressable');
    assert.ok(getAgentByName('scout-2'), 'second agent got a suffixed name');
  });

  it('skips already-taken suffixes', async () => {
    await runTask({ prompt: 'a', mode: 'explore', name: 'w' });
    await runTask({ prompt: 'b', mode: 'explore', name: 'w' });   // w-2
    assert.equal(uniqueAgentName('w'), 'w-3');
  });
});

// ---------------------------------------------------------------------------
describe('GIT_HARDENING_ARGS applied to auto-invoked git (gap #1)', () => {
  let repo: string;
  beforeEach(() => {
    resetAgentRegistry();
    repo = fs.mkdtempSync(path.join(os.tmpdir(), 'git-harden-'));
    child_process.execFileSync('git', ['init', '-q'], { cwd: repo });
    child_process.execFileSync('git', ['config', 'user.email', 't@t'], { cwd: repo });
    child_process.execFileSync('git', ['config', 'user.name', 't'], { cwd: repo });
    fs.writeFileSync(path.join(repo, 'f.txt'), 'x');
    child_process.execFileSync('git', ['add', '.'], { cwd: repo });
    child_process.execFileSync('git', ['commit', '-qm', 'init'], { cwd: repo });
  });
  afterEach(() => { fs.rmSync(repo, { recursive: true, force: true }); });

  it('the constant disables hooks, fsmonitor, and external diff', () => {
    assert.deepEqual(GIT_HARDENING_ARGS, [
      '-c', 'core.fsmonitor=false',
      '-c', 'core.hooksPath=/dev/null',
      '-c', 'diff.external=',
    ]);
  });

  it('the worktree-cleanup git status runs with fsmonitor disabled (side-effect proof)', async (t) => {
    // A `core.fsmonitor` program is executed by `git status` UNLESS disabled.
    // Point it at a script that drops a sentinel; the cleanup `git status` must
    // NOT fire it, proving GIT_HARDENING_ARGS' `core.fsmonitor=false` is applied.
    const sentinel = path.join(repo, 'FSMON_FIRED');
    const hook = path.join(repo, 'fsmon.sh');
    fs.writeFileSync(hook, `#!/bin/sh\ntouch "${sentinel}"\nexit 0\n`);
    fs.chmodSync(hook, 0o755);
    child_process.execFileSync('git', ['config', 'core.fsmonitor', hook], { cwd: repo });

    // Control probe: an UN-hardened `git status` must fire the sentinel here, or
    // this environment does not execute fsmonitor programs and the proof is moot.
    try { child_process.execFileSync('git', ['status', '--porcelain'], { cwd: repo }); } catch { /* protocol error is fine; program still ran */ }
    if (!fs.existsSync(sentinel)) { t.skip('environment does not execute core.fsmonitor programs'); return; }
    fs.rmSync(sentinel, { force: true });

    await runTask(
      { prompt: 'go', isolation: 'worktree' },
      { cwd: repo },
      [{ type: 'text_delta', text: 'no changes made' }],
    );
    assert.ok(
      !fs.existsSync(sentinel),
      'cleanup git status fired core.fsmonitor — hardening flags were NOT applied',
    );
  });
});

// ---------------------------------------------------------------------------
describe('combined allowlist + denylist on an agent def (gaps #75/#93/#94/#96)', () => {
  beforeEach(() => resetAgentRegistry());

  it('deny wins even when an allowlist is set', async () => {
    const skillManager = {
      getAgent: () => ({ name: 'planner', tools: ['file_read', 'grep', 'file_edit'], disallowedTools: ['file_edit'] }),
      listAgents: () => [{ name: 'planner' }],
    };
    const { toolNames } = await runTask(
      { prompt: 'plan', subagent_type: 'planner' },
      { _skillManager: skillManager as unknown as ToolContext['_skillManager'] },
    );
    assert.ok(toolNames.includes('file_read'), 'allowlisted read kept');
    assert.ok(toolNames.includes('grep'), 'allowlisted grep kept');
    assert.ok(!toolNames.includes('file_edit'), 'denylisted edit removed despite being allowlisted');
  });
});

// ---------------------------------------------------------------------------
describe('permission_mode input parameter (gap #48)', () => {
  beforeEach(() => resetAgentRegistry());

  it('schema advertises permission_mode distinct from mode', () => {
    const props = taskTool.inputSchema.properties as Record<string, { enum?: string[] }>;
    assert.ok('permission_mode' in props);
    assert.ok(props.permission_mode.enum!.includes('plan'));
    assert.ok(props.permission_mode.enum!.includes('acceptEdits'));
  });

  it('threads a valid permission_mode into the subagent config', async () => {
    const { cfg } = await runTask({ prompt: 'go', permission_mode: 'acceptEdits' });
    assert.equal(cfg!.permissionMode, 'acceptEdits');
  });

  it('rejects an invalid permission_mode listing valid values', async () => {
    const { isError, output, cfg } = await runTask({ prompt: 'go', permission_mode: 'nonsense' });
    assert.ok(isError);
    assert.match(output, /invalid permission_mode/);
    assert.match(output, /acceptEdits/);
    assert.equal(cfg, null, 'no engine created on invalid input');
  });

  it('permission_mode overrides the explore auto-allow default', async () => {
    const { cfg } = await runTask({ prompt: 'go', mode: 'explore', permission_mode: 'deny-all' });
    assert.equal(cfg!.permissionMode, 'deny-all');
  });
});

// ---------------------------------------------------------------------------
describe('fork maxTurns default (gap #83)', () => {
  beforeEach(() => resetAgentRegistry());

  it('a fork child gets DEFAULT_FORK_MAX_TURNS when no agent def sets one', async () => {
    const { cfg } = await runTask({ prompt: 'continue', mode: 'fork' });
    assert.equal(cfg!.maxTurns, DEFAULT_FORK_MAX_TURNS);
    assert.equal(DEFAULT_FORK_MAX_TURNS, 200);
  });

  it('a non-fork subagent has no forced turn budget', async () => {
    const { cfg } = await runTask({ prompt: 'go', mode: 'general' });
    assert.equal(cfg!.maxTurns, undefined);
  });
});

// ---------------------------------------------------------------------------
describe('named subagent_type gated through the preToolUse hook (gap #52)', () => {
  beforeEach(() => resetAgentRegistry());

  it('passes the SPECIFIC agent type (not the coarse mode) into the hook', async () => {
    let seen: Record<string, unknown> | null = null;
    const skillManager = {
      getAgent: () => ({ name: 'researcher', systemPrompt: 'research' }),
      listAgents: () => [{ name: 'researcher' }],
    };
    await runTask(
      { prompt: 'dig', subagent_type: 'researcher' },
      {
        _skillManager: skillManager as unknown as ToolContext['_skillManager'],
        _hookManager: {
          executePreToolUse: async (a: { toolInput?: Record<string, unknown> }) => { seen = a.toolInput ?? null; return { action: 'allow' }; },
          executeSubagentStart: async () => {},
          executeSubagentStop: async () => {},
        } as unknown as ToolContext['_hookManager'],
      },
    );
    assert.equal(seen!.subagent_type, 'researcher');
  });

  it('a deny rule on the agent type blocks the spawn and names it', async () => {
    const skillManager = {
      getAgent: () => ({ name: 'researcher', systemPrompt: 'research' }),
      listAgents: () => [{ name: 'researcher' }],
    };
    const { isError, output } = await runTask(
      { prompt: 'dig', subagent_type: 'researcher' },
      {
        _skillManager: skillManager as unknown as ToolContext['_skillManager'],
        _hookManager: {
          executePreToolUse: async () => ({ action: 'deny' }),
          executeSubagentStart: async () => {},
          executeSubagentStop: async () => {},
        } as unknown as ToolContext['_hookManager'],
      },
    );
    assert.ok(isError);
    assert.match(output, /"researcher" denied/);
  });
});

// ---------------------------------------------------------------------------
describe('foreground result affordances (gaps #54 / #80 / #79)', () => {
  beforeEach(() => resetAgentRegistry());

  it('substitutes an explicit no-output marker when the body is empty (gap #54)', async () => {
    const { output, isError } = await runTask({ prompt: 'quiet' }, {}, [{ type: 'turn_complete' }]);
    assert.ok(!isError);
    assert.match(output, /\(Subagent completed but returned no output\.\)/);
    assert.match(output, /\[Agent stats: \d+ tool calls/);
  });

  it('appends a continue-affordance naming the agent for non-explore runs (gap #80)', async () => {
    const { output } = await runTask({ prompt: 'work', mode: 'general', name: 'helper' });
    assert.match(output, /task\(\{ resume: "helper"/);
  });

  it('omits the continue affordance for one-shot explore runs (gap #79)', async () => {
    const { output } = await runTask({ prompt: 'look', mode: 'explore' });
    assert.ok(!output.includes('To continue this agent'), 'explore is one-shot — no continue hint');
  });
});

// ---------------------------------------------------------------------------
describe('max-turns and stall/abort outcome surfacing (gaps #108 / #106)', () => {
  const ORIG = process.env.AMI_SUBAGENT_STALL_TIMEOUT_MS;
  beforeEach(() => resetAgentRegistry());
  afterEach(() => {
    if (ORIG === undefined) delete process.env.AMI_SUBAGENT_STALL_TIMEOUT_MS;
    else process.env.AMI_SUBAGENT_STALL_TIMEOUT_MS = ORIG;
  });

  it('notes a max-turns stop as a capped partial (gap #108)', async () => {
    const { output } = await runTask({ prompt: 'go' }, {}, [
      { type: 'text_delta', text: 'partial' },
      { type: 'done', totalTurns: 5, exitReason: 'max_turns' },
    ]);
    assert.match(output, /reached max turns limit/);
  });

  it('a clean completion carries no max-turns note', async () => {
    const { output, isError } = await runTask({ prompt: 'go' }, {}, [
      { type: 'text_delta', text: 'all done' },
      { type: 'done', totalTurns: 1, exitReason: 'completed' },
    ]);
    assert.ok(!isError);
    assert.ok(!output.includes('reached max turns'));
  });

  it('an abort exitReason marks the result as a failure (gap #106)', async () => {
    const { isError, output } = await runTask({ prompt: 'go' }, {}, [
      { type: 'text_delta', text: 'some work' },
      { type: 'done', totalTurns: 2, exitReason: 'abort' },
    ]);
    assert.ok(isError, 'aborted run must be reported as an error, not a success');
    assert.match(output, /some work/);
  });

  it('an "Operation aborted" error event marks the result as a failure (gap #106)', async () => {
    const { isError } = await runTask({ prompt: 'go' }, {}, [
      { type: 'error', error: 'Operation aborted' },
    ]);
    assert.ok(isError);
  });
});

// ---------------------------------------------------------------------------
describe('subagent system-prompt coaching (gaps #69 / #72 / #73)', () => {
  it('explore prompt mentions thoroughness and multi-strategy search', async () => {
    const mod = await import('../src/tools/task');
    assert.match(mod.EXPLORE_SUBAGENT_SYSTEM_PROMPT, /thorough/i);
    assert.match(mod.EXPLORE_SUBAGENT_SYSTEM_PROMPT, /narrow down|alternate naming|multiple locations/i);
  });
  it('general subagent prompt includes broad→narrow search guidance', async () => {
    const mod = await import('../src/tools/task');
    assert.match(mod.SUBAGENT_SYSTEM_PROMPT, /start broad|narrow down|alternate naming/i);
  });
  it('mode schema documents the explore thoroughness affordance', () => {
    const props = taskTool.inputSchema.properties as Record<string, { description?: string }>;
    assert.match(props.mode.description!, /thorough/i);
  });
});

// ---------------------------------------------------------------------------
describe('background launch anti-overlap guidance (gap #53)', () => {
  beforeEach(() => resetAgentRegistry());
  it('steers the parent away from duplicating the background agent work', async () => {
    const { output } = await runTask({ prompt: 'bg', run_in_background: true });
    assert.match(output, /duplicate this agent's work/);
    assert.match(output, /non-overlapping/);
  });
});
