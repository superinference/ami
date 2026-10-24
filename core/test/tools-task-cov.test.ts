import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as child_process from 'child_process';

import {
  taskTool,
  getAgentByName,
  getAgentNameById,
  resetAgentRegistry,
  checkAgentCaps,
  shouldAllowNestedTask,
  getSubagentDepthContext,
  filterToolsForAgent,
  DEFAULT_MAX_AGENTS_PER_SESSION,
  DEFAULT_MAX_CONCURRENT_AGENTS,
} from '../src/tools/task';
import { scoutTool } from '../src/tools/scout';
import {
  teamCreateTool,
  teamDeleteTool,
  getCurrentTeam,
  resetTeam,
} from '../src/tools/team';
import { taskListTool } from '../src/tools/task-list';
import { createDefaultTools } from '../src/tools/index';
import { ProcessManager } from '../src/process-manager';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

function gen(events: any[]) {
  return async function* (_p: string) {
    for (const e of events) yield e;
  };
}

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

// ===========================================================================
// task.ts — pure helpers
// ===========================================================================

describe('task.ts pure helpers', () => {
  beforeEach(() => resetAgentRegistry());

  it('shouldAllowNestedTask compares depth to max', () => {
    assert.equal(shouldAllowNestedTask(0, 5), true);
    assert.equal(shouldAllowNestedTask(5, 5), false);
    assert.equal(shouldAllowNestedTask(6, 5), false);
  });

  it('getSubagentDepthContext increments depth and carries max', () => {
    assert.deepEqual(getSubagentDepthContext(2, 7), { _agentDepth: 3, _maxAgentDepth: 7 });
  });

  it('filterToolsForAgent — explore keeps only read-only tools', () => {
    const all = createDefaultTools(process.cwd()).getAll();
    const names = filterToolsForAgent(all, 'explore').map(t => t.name);
    assert.ok(names.includes('file_read'));
    assert.ok(!names.includes('file_edit'));
    assert.ok(!names.includes('bash'));
  });

  it('filterToolsForAgent — general at shallow depth keeps task tool', () => {
    const all = createDefaultTools(process.cwd()).getAll();
    const names = filterToolsForAgent(all, 'general', 0, 5).map(t => t.name);
    assert.ok(names.includes('task'));
  });

  it('filterToolsForAgent — general at/over max depth strips task tool', () => {
    const all = createDefaultTools(process.cwd()).getAll();
    const names = filterToolsForAgent(all, 'general', 5, 5).map(t => t.name);
    assert.ok(!names.includes('task'));
    assert.ok(names.includes('file_read'));
  });

  it('filterToolsForAgent — defaults maxDepth when omitted', () => {
    const all = createDefaultTools(process.cwd()).getAll();
    // depth undefined -> 0, maxDepth undefined -> DEFAULT (5); nesting allowed.
    const names = filterToolsForAgent(all, 'general').map(t => t.name);
    assert.ok(names.includes('task'));
  });

  it('checkAgentCaps — allows under caps', () => {
    assert.equal(checkAgentCaps({}), null);
    assert.equal(checkAgentCaps({ _agentSpawnCount: { value: 0 } }), null);
  });

  it('checkAgentCaps — blocks at session cap', () => {
    const err = checkAgentCaps({ _agentSpawnCount: { value: DEFAULT_MAX_AGENTS_PER_SESSION } });
    assert.ok(err && err.includes('session cap reached'));
  });

  it('checkAgentCaps — blocks at concurrent cap', () => {
    const fakePm = { runningAgentCount: () => DEFAULT_MAX_CONCURRENT_AGENTS } as any;
    const err = checkAgentCaps({ processManager: fakePm });
    assert.ok(err && err.includes('Concurrent agent cap reached'));
  });

  it('checkAgentCaps — allows when concurrent count is below cap', () => {
    const fakePm = { runningAgentCount: () => 0 } as any;
    assert.equal(checkAgentCaps({ processManager: fakePm, _maxConcurrentAgents: 3 }), null);
  });

  it('getAgentByName / getAgentNameById resolve a registered foreground agent', async () => {
    resetAgentRegistry();
    assert.equal(getAgentByName('scout1'), undefined);
    await taskTool.execute(
      { prompt: 'do work', mode: 'explore', name: 'scout1' },
      ctx({
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _engineFactory: () => ({ submit: gen([{ type: 'text_delta', text: 'ok' }]) }),
      }),
    );
    const id = getAgentByName('scout1');
    assert.ok(id && id.startsWith('agent-'), 'name should map to an agent id');
    assert.equal(getAgentNameById(id!), 'scout1', 'reverse lookup by id returns the name');
    assert.equal(getAgentNameById('agent-does-not-exist'), undefined, 'unknown id returns undefined');
  });
});

// ===========================================================================
// task.ts — resume path
// ===========================================================================

describe('taskTool — resume', () => {
  beforeEach(() => resetAgentRegistry());

  it('resumes by registered name (resolves to agent id)', async () => {
    await taskTool.execute(
      { prompt: 'first', mode: 'explore', name: 'resumable' },
      ctx({
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _engineFactory: () => ({ submit: gen([{ type: 'text_delta', text: 'ok' }]) }),
      }),
    );
    const id = getAgentByName('resumable')!;
    const r = await taskTool.execute({ prompt: 'follow up', resume: 'resumable' }, ctx());
    assert.ok(!r.isError);
    assert.ok(r.output.includes('[resume]'));
    assert.ok(r.output.includes(id), 'resolves the registered name to its id');
  });

  it('resumes by raw id when name is not registered', async () => {
    const r = await taskTool.execute({ prompt: 'follow up', resume: 'agent-raw-id' }, ctx());
    assert.ok(!r.isError);
    assert.ok(r.output.includes('agent-raw-id'));
  });
});

// ===========================================================================
// task.ts — caps & limiter gating in execute
// ===========================================================================

describe('taskTool — spawn gating', () => {
  beforeEach(() => resetAgentRegistry());

  it('blocks spawn when the session cap is already reached', async () => {
    const r = await taskTool.execute(
      { prompt: 'blocked', mode: 'explore' },
      ctx({ _agentSpawnCount: { value: DEFAULT_MAX_AGENTS_PER_SESSION } }),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('session cap reached'));
  });

  it('blocks spawn when the lifetime subagent budget is exhausted', async () => {
    const r = await taskTool.execute(
      { prompt: 'blocked', mode: 'explore' },
      ctx({
        subagentLimiter: {
          isLifetimeBudgetExhausted: () => true,
          maxLifetime: 7,
          recordSpawn: () => {},
        } as any,
      }),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('Session subagent limit reached (7)'));
  });

  it('records a spawn on the limiter for a normal foreground run', async () => {
    let recorded = 0;
    await taskTool.execute(
      { prompt: 'go', mode: 'explore' },
      ctx({
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        subagentLimiter: {
          isLifetimeBudgetExhausted: () => false,
          maxLifetime: 50,
          recordSpawn: () => { recorded++; },
        } as any,
        _engineFactory: () => ({ submit: gen([{ type: 'text_delta', text: 'ok' }]) }),
      }),
    );
    assert.equal(recorded, 1);
  });
});

// ===========================================================================
// task.ts — SUPERINFERENCE_SUBAGENT_MODEL env override
// ===========================================================================

describe('taskTool — env subagent model', () => {
  const ORIG = process.env.SUPERINFERENCE_SUBAGENT_MODEL;
  beforeEach(() => resetAgentRegistry());
  afterEach(() => {
    if (ORIG === undefined) delete process.env.SUPERINFERENCE_SUBAGENT_MODEL;
    else process.env.SUPERINFERENCE_SUBAGENT_MODEL = ORIG;
  });

  it('applies the env model (alias-resolved) when no model/agentModel is set', async () => {
    process.env.SUPERINFERENCE_SUBAGENT_MODEL = 'haiku';
    let cfg: any = null;
    await taskTool.execute(
      { prompt: 'go', mode: 'general' },
      ctx({
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _engineFactory: (c: any) => { cfg = c; return { submit: gen([{ type: 'text_delta', text: 'ok' }]) }; },
      }),
    );
    assert.equal(cfg.provider.model, 'claude-haiku-4-5');
  });

  it('passes a non-alias env model through verbatim', async () => {
    process.env.SUPERINFERENCE_SUBAGENT_MODEL = 'my-custom-model';
    let cfg: any = null;
    await taskTool.execute(
      { prompt: 'go', mode: 'general' },
      ctx({
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _engineFactory: (c: any) => { cfg = c; return { submit: gen([{ type: 'text_delta', text: 'ok' }]) }; },
      }),
    );
    assert.equal(cfg.provider.model, 'my-custom-model');
  });
});

// ===========================================================================
// task.ts — foreground extras: name registration + output budget spill
// ===========================================================================

describe('taskTool — foreground extras', () => {
  beforeEach(() => resetAgentRegistry());

  it('registers a name for a foreground agent', async () => {
    await taskTool.execute(
      { prompt: 'named run', mode: 'explore', name: 'fg-named' },
      ctx({
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _engineFactory: () => ({ submit: gen([{ type: 'text_delta', text: 'ok' }]) }),
      }),
    );
    assert.ok(getAgentByName('fg-named'));
  });

  it('caps the subagent transcript to outputBudget.taskMaxChars (spills overflow)', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'si-spill-'));
    try {
      const big = 'X'.repeat(5000);
      const r = await taskTool.execute(
        { prompt: 'chatty', mode: 'explore' },
        ctx({
          cwd: tmp,
          _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
          outputBudget: { taskMaxChars: 200 },
          _engineFactory: () => ({ submit: gen([{ type: 'text_delta', text: big }]) }),
        }),
      );
      assert.ok(!r.isError);
      // The full 5000-char body must not survive verbatim once capped at 200.
      assert.ok(!r.output.includes(big), 'oversized transcript must be spilled, not inlined');
      assert.ok(r.output.length < big.length);
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });
});

// ===========================================================================
// task.ts — git worktree isolation
// ===========================================================================

describe('taskTool — worktree isolation', () => {
  beforeEach(() => resetAgentRegistry());

  it('fails cleanly when a worktree cannot be created (non-git cwd)', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'si-nogit-'));
    try {
      const r = await taskTool.execute(
        { prompt: 'isolated', mode: 'explore', isolation: 'worktree' },
        ctx({
          cwd: tmp,
          _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
          _engineFactory: () => ({ submit: gen([{ type: 'text_delta', text: 'ok' }]) }),
        }),
      );
      assert.ok(r.isError);
      assert.ok(r.output.includes('Failed to create worktree'));
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('runs in an isolated worktree and cleans it up when no changes remain', async () => {
    const repo = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wt-'));
    const remote = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wt-remote-'));
    const g = (args: string, cwd = repo) =>
      child_process.execSync(`git ${args}`, { cwd, encoding: 'utf-8', stdio: 'pipe' });
    try {
      child_process.execSync('git init -q -b main', { cwd: repo });
      g('config user.email t@t.t');
      g('config user.name t');
      fs.writeFileSync(path.join(repo, 'a.txt'), 'hi\n');
      g('add -A');
      g('commit -qm init');
      child_process.execSync('git init -q --bare', { cwd: remote });
      g(`remote add origin "${remote}"`);
      g('push -q origin main');

      let runCwd = '';
      const r = await taskTool.execute(
        { prompt: 'isolated clean run', mode: 'explore', isolation: 'worktree' },
        ctx({
          cwd: repo,
          _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
          _engineFactory: (cfg: any) => {
            runCwd = cfg.cwd;
            return { submit: gen([{ type: 'text_delta', text: 'done' }]) };
          },
        }),
      );
      assert.ok(!r.isError, r.output);
      assert.ok(runCwd.includes(path.join('.superinference', 'worktrees')),
        'subagent must run inside the worktree path');
      assert.ok(r.output.includes('worktree cleaned up'),
        'a change-free worktree must be cleaned up');
      assert.ok(!fs.existsSync(runCwd), 'the worktree directory must be removed');
    } finally {
      fs.rmSync(repo, { recursive: true, force: true });
      fs.rmSync(remote, { recursive: true, force: true });
    }
  });
});

// ===========================================================================
// task.ts — background: stall timeout + abort function
// ===========================================================================

describe('taskTool — background lifecycle', () => {
  const ORIG = process.env.AMI_SUBAGENT_STALL_TIMEOUT_MS;
  beforeEach(() => resetAgentRegistry());
  afterEach(() => {
    if (ORIG === undefined) delete process.env.AMI_SUBAGENT_STALL_TIMEOUT_MS;
    else process.env.AMI_SUBAGENT_STALL_TIMEOUT_MS = ORIG;
  });

  it('terminates and marks failed when a background agent stalls', async () => {
    process.env.AMI_SUBAGENT_STALL_TIMEOUT_MS = '40';
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'si-bg-stall-'));
    const pm = new ProcessManager(tmp);
    let shutdownCalled = false;
    try {
      async function* stallingSubmit(): AsyncGenerator<any> {
        yield { type: 'text_delta', text: 'begin' };
        await sleep(220);
        yield { type: 'text_delta', text: 'late' };
      }
      const res = await taskTool.execute(
        { prompt: 'bg stall', run_in_background: true },
        ctx({
          cwd: tmp,
          _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
          processManager: pm,
          _engineFactory: () => ({ submit: stallingSubmit, shutdown: () => { shutdownCalled = true; } }),
        }),
      );
      const taskId = res.output.match(/agent-[a-f0-9]+/)![0];
      await sleep(400);
      const outputPath = path.join(tmp, '.superinference', 'tasks', `${taskId}.output`);
      const content = fs.readFileSync(outputPath, 'utf-8');
      assert.ok(content.includes('stalled'), `stall notice must be written, got: "${content}"`);
      assert.ok(shutdownCalled, 'stalled subagent engine must be shut down');
      const entry = (pm as any).processes.get(taskId);
      assert.equal(entry.status, 'failed');
    } finally {
      pm.cleanup();
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('registers an abort function that aborts the sub-agent when killed', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'si-bg-abort-'));
    const pm = new ProcessManager(tmp);
    let resolveHold!: () => void;
    const hold = new Promise<void>(r => { resolveHold = r; });
    try {
      async function* holdingSubmit(): AsyncGenerator<any> {
        yield { type: 'text_delta', text: 'running' };
        await hold;
      }
      const res = await taskTool.execute(
        { prompt: 'bg abort', run_in_background: true },
        ctx({
          cwd: tmp,
          _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
          processManager: pm,
          _engineFactory: () => ({ submit: holdingSubmit, shutdown: () => {} }),
        }),
      );
      const taskId = res.output.match(/agent-[a-f0-9]+/)![0];
      await sleep(40);
      const entry = (pm as any).processes.get(taskId);
      assert.ok(entry, 'agent must be registered');
      // Invoking the registered kill routes through the abortFn -> subAbort.abort()
      assert.doesNotThrow(() => entry.proc.kill());
      resolveHold();
      await sleep(60);
    } finally {
      pm.cleanup();
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });
});

// ===========================================================================
// task.ts — subagent_type nested-task stripping at max depth
// ===========================================================================

describe('taskTool — subagent_type at max nesting depth', () => {
  beforeEach(() => resetAgentRegistry());

  it('strips the task tool from an agent-def allow-list when depth >= maxDepth', async () => {
    let cfg: any = null;
    await taskTool.execute(
      { prompt: 'deep', subagent_type: 'deep-tools' },
      ctx({
        _agentDepth: 5,
        _maxAgentDepth: 5,
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _skillManager: {
          getAgent: () => ({ name: 'deep-tools', tools: ['file_read', 'grep', 'task'] }),
          listAgents: () => [],
        } as any,
        _engineFactory: (c: any) => { cfg = c; return { submit: gen([{ type: 'text_delta', text: 'ok' }]) }; },
      }),
    );
    const names = cfg.tools.map((t: any) => t.name);
    assert.ok(names.includes('file_read'));
    assert.ok(!names.includes('task'), 'task must be stripped at/over max depth even if allow-listed');
  });

  it('strips the task tool from an agent-def deny-list run when depth >= maxDepth', async () => {
    let cfg: any = null;
    await taskTool.execute(
      { prompt: 'deep', subagent_type: 'deep-deny' },
      ctx({
        _agentDepth: 6,
        _maxAgentDepth: 5,
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _skillManager: {
          getAgent: () => ({ name: 'deep-deny', disallowedTools: ['bash'] }),
          listAgents: () => [],
        } as any,
        _engineFactory: (c: any) => { cfg = c; return { submit: gen([{ type: 'text_delta', text: 'ok' }]) }; },
      }),
    );
    const names = cfg.tools.map((t: any) => t.name);
    assert.ok(!names.includes('bash'), 'deny-listed bash excluded');
    assert.ok(!names.includes('task'), 'task stripped past max depth');
    assert.ok(names.includes('file_read'));
  });
});

// ===========================================================================
// task.ts — abort forwarding + hook promise rejection handling
// ===========================================================================

describe('taskTool — abort & hook rejection handling', () => {
  beforeEach(() => resetAgentRegistry());

  it('forwards a parent abort to the sub-agent controller', async () => {
    const parent = new AbortController();
    let sawSecond = false;
    async function* submit(): AsyncGenerator<any> {
      yield { type: 'text_delta', text: 'a' };
      parent.abort(); // fires the once-listener -> forwardAbort -> subAbort.abort()
      yield { type: 'text_delta', text: 'b' };
      sawSecond = true;
    }
    const r = await taskTool.execute(
      { prompt: 'abortable', mode: 'explore' },
      ctx({
        abortSignal: parent.signal,
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _engineFactory: () => ({ submit }),
      }),
    );
    assert.ok(sawSecond, 'generator drains past the abort');
    assert.ok(r.output.includes('a'));
  });

  it('swallows rejected subagent-start/stop hooks on the success path', async () => {
    const r = await taskTool.execute(
      { prompt: 'hooks reject', mode: 'explore' },
      ctx({
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _hookManager: {
          executeSubagentStart: () => Promise.reject(new Error('start boom')),
          executeSubagentStop: () => Promise.reject(new Error('stop boom')),
        } as any,
        _engineFactory: () => ({ submit: gen([{ type: 'text_delta', text: 'ok' }]) }),
      }),
    );
    assert.ok(!r.isError, 'rejected hook promises must not fail the task');
    assert.ok(r.output.includes('ok'));
  });

  it('swallows a rejected subagent-stop hook on the error path', async () => {
    async function* boom(): AsyncGenerator<any> {
      yield { type: 'text_delta', text: 'partial' };
      throw new Error('stream failure');
    }
    const r = await taskTool.execute(
      { prompt: 'hooks reject on error', mode: 'explore' },
      ctx({
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _hookManager: {
          executeSubagentStart: async () => {},
          executeSubagentStop: () => Promise.reject(new Error('stop boom')),
        } as any,
        _engineFactory: () => ({ submit: boom }),
      }),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('stream failure'));
  });

  it('swallows a background IIFE failure when its output file cannot be opened', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'si-bg-iife-'));
    try {
      const r = await taskTool.execute(
        { prompt: 'bg iife throw', run_in_background: true },
        ctx({
          cwd: tmp,
          _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
          _engineFactory: () => {
            // Remove the tasks dir created synchronously by execute, so the
            // background IIFE's fs.openSync throws ENOENT and the IIFE rejects
            // into its trailing .catch(() => {}).
            fs.rmSync(path.join(tmp, '.superinference', 'tasks'), { recursive: true, force: true });
            return { submit: gen([{ type: 'text_delta', text: 'never written' }]) };
          },
        }),
      );
      assert.ok(r.output.includes('Background agent started'), 'sync part still succeeds');
      await sleep(60); // let the rejected IIFE settle without crashing the process
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });
});

// ===========================================================================
// task.ts — background event rendering + no-output + limiter/name
// ===========================================================================

describe('taskTool — background event streaming', () => {
  beforeEach(() => resetAgentRegistry());

  it('renders every background event type to the output file', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'si-bg-events-'));
    const pm = new ProcessManager(tmp);
    let recorded = 0;
    const bigOutput = Array.from({ length: 25 }, (_, i) => `line ${i}`).join('\n');
    try {
      const events = [
        { type: 'thinking_delta', text: '[thinking] ' },
        { type: 'text_delta', text: 'answer body ' },
        { type: 'tool_use_start', toolName: 'bash', input: { command: 'ls -la' } },
        { type: 'tool_use_start', toolName: 'file_read', input: { file_path: '/etc/hosts' } },
        { type: 'tool_use_start', toolName: 'glob', input: { pattern: '**/*' } },
        { type: 'tool_use_progress', data: 'progress chunk' },
        { type: 'tool_use_progress' }, // data undefined -> `|| ''`
        { type: 'tool_use_result', output: bigOutput },
        { type: 'tool_use_result', output: 'short out' },
        { type: 'usage_update', stats: { totalTokens: 123 } },
      ];
      const res = await taskTool.execute(
        { prompt: 'rich bg', run_in_background: true, name: 'bg-rich', description: 'rich label' },
        ctx({
          cwd: tmp,
          _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
          processManager: pm,
          subagentLimiter: {
            isLifetimeBudgetExhausted: () => false, maxLifetime: 99, recordSpawn: () => { recorded++; },
          } as any,
          _engineFactory: () => ({ submit: gen(events) }),
        }),
      );
      const taskId = res.output.match(/agent-[a-f0-9]+/)![0];
      assert.equal(getAgentByName('bg-rich'), taskId, 'bg agent name registered');
      assert.equal(recorded, 1, 'limiter.recordSpawn called on bg path');
      await sleep(120);
      const content = fs.readFileSync(path.join(tmp, '.superinference', 'tasks', `${taskId}.output`), 'utf-8');
      assert.ok(content.includes('[thinking] '), 'thinking_delta written');
      assert.ok(content.includes('answer body'), 'text_delta written');
      assert.ok(content.includes('ls -la'), 'tool_use_start command summary');
      assert.ok(content.includes('/etc/hosts'), 'tool_use_start file_path summary');
      assert.ok(content.includes('glob'), 'tool_use_start JSON-fallback summary');
      assert.ok(content.includes('progress chunk'), 'tool_use_progress written');
      assert.ok(content.includes('more lines)'), 'tool_use_result truncation suffix for >20 lines');
      assert.ok(content.includes('short out'), 'small tool_use_result written verbatim');
      const entry = (pm as any).processes.get(taskId);
      assert.equal(entry.tokenCount, 123, 'usage_update updates token count');
      assert.equal(entry.status, 'completed');
    } finally {
      pm.cleanup();
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('writes a placeholder when a background agent produces no output', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'si-bg-empty-'));
    try {
      const res = await taskTool.execute(
        { prompt: 'silent bg', run_in_background: true },
        ctx({
          cwd: tmp,
          _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
          _engineFactory: () => ({ submit: gen([{ type: 'turn_complete', content: null, toolCalls: [] }]) }),
        }),
      );
      const taskId = res.output.match(/agent-[a-f0-9]+/)![0];
      await sleep(80);
      const content = fs.readFileSync(path.join(tmp, '.superinference', 'tasks', `${taskId}.output`), 'utf-8');
      assert.ok(content.includes('subagent produced no output'));
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });
});

// ===========================================================================
// task.ts — foreground event handling: effort, stall-break, activity cap, usage
// ===========================================================================

describe('taskTool — foreground event handling', () => {
  const ORIG = process.env.AMI_SUBAGENT_STALL_TIMEOUT_MS;
  beforeEach(() => resetAgentRegistry());
  afterEach(() => {
    if (ORIG === undefined) delete process.env.AMI_SUBAGENT_STALL_TIMEOUT_MS;
    else process.env.AMI_SUBAGENT_STALL_TIMEOUT_MS = ORIG;
  });

  it('passes an explicit effort override into the subagent thinking config', async () => {
    let cfg: any = null;
    await taskTool.execute(
      { prompt: 'think hard', mode: 'explore', effort: 'high' },
      ctx({
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _parentThinking: { enabled: false, level: 'low' } as any,
        _engineFactory: (c: any) => { cfg = c; return { submit: gen([{ type: 'text_delta', text: 'ok' }]) }; },
      }),
    );
    assert.deepEqual(cfg.thinking, { enabled: true, level: 'high' });
  });

  it('breaks the foreground loop once a stall is detected mid-stream', async () => {
    process.env.AMI_SUBAGENT_STALL_TIMEOUT_MS = '40';
    async function* reyield(): AsyncGenerator<any> {
      yield { type: 'text_delta', text: 'early' };
      await sleep(220);           // exceed stall timeout -> stalled = true
      yield { type: 'text_delta', text: 'late' };   // re-entry hits `if (stalled) break`
    }
    const r = await taskTool.execute(
      { prompt: 'fg stall break', mode: 'explore' },
      ctx({
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _engineFactory: () => ({ submit: reyield }),
      }),
    );
    assert.ok(r.output.includes('timed out'), 'foreground stall message appended');
    assert.ok(!r.output.includes('late'), 'post-stall event is not consumed (loop broke)');
  });

  it('caps the recent-activity ring buffer past five tool calls and tolerates usage without totals', async () => {
    const events: any[] = [];
    for (let i = 0; i < 7; i++) events.push({ type: 'tool_use_start', toolName: `tool${i}` });
    events.push({ type: 'usage_update', stats: {} }); // totalTokens undefined -> `?? totalTokens`
    events.push({ type: 'text_delta', text: 'done after many tools' });
    const r = await taskTool.execute(
      { prompt: 'many tools', mode: 'general' },
      ctx({
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _engineFactory: () => ({ submit: gen(events) }),
      }),
    );
    assert.ok(!r.isError);
    assert.ok(r.output.includes('7 tool calls'), 'tool call count reflected in stats');
    assert.ok(r.output.includes('0 tokens'), 'missing usage totals default to 0');
  });
});

// ===========================================================================
// task.ts — subagent_type not-found with no available agents
// ===========================================================================

describe('taskTool — subagent_type not found (no agents)', () => {
  beforeEach(() => resetAgentRegistry());
  it('lists "(none)" when no agent definitions are registered', async () => {
    const r = await taskTool.execute(
      { prompt: 'x', subagent_type: 'ghost' },
      ctx({
        _skillManager: { getAgent: () => null, listAgents: () => [] } as any,
      }),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('(none)'));
  });
});

// ===========================================================================
// task.ts — worktree cleanup swallows git failures
// ===========================================================================

describe('taskTool — worktree cleanup error handling', () => {
  beforeEach(() => resetAgentRegistry());

  it('swallows git errors when inspecting the worktree for cleanup', async () => {
    const repo = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wtc-'));
    const g = (args: string) => child_process.execSync(`git ${args}`, { cwd: repo, stdio: 'pipe' });
    try {
      child_process.execSync('git init -q -b main', { cwd: repo });
      g('config user.email t@t.t');
      g('config user.name t');
      fs.writeFileSync(path.join(repo, 'a.txt'), 'hi\n');
      g('add -A');
      g('commit -qm init');

      const r = await taskTool.execute(
        { prompt: 'self-destruct worktree', mode: 'explore', isolation: 'worktree' },
        ctx({
          cwd: repo,
          _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
          _engineFactory: (cfg: any) => ({
            submit: async function* () {
              yield { type: 'text_delta', text: 'bye' };
              // Remove the worktree out from under the cleanup step so its
              // `git status` execSync throws and the catch {} swallows it.
              fs.rmSync(cfg.cwd, { recursive: true, force: true });
            },
          }),
        }),
      );
      assert.ok(!r.isError, r.output);
      assert.ok(!r.output.includes('cleaned up'), 'cleanup aborted by the swallowed git error');
    } finally {
      fs.rmSync(repo, { recursive: true, force: true });
    }
  });
});

// ===========================================================================
// scout.ts
// ===========================================================================

describe('scoutTool', () => {
  it('definition is read-only and named scout', () => {
    assert.equal(scoutTool.name, 'scout');
    assert.equal(scoutTool.isReadOnly, true);
    assert.ok(scoutTool.inputSchema.required?.includes('repo'));
  });

  it('clones an absolute-path repo, surfaces README + a query match', async () => {
    const src = fs.mkdtempSync(path.join(os.tmpdir(), 'scout-src-'));
    const work = fs.mkdtempSync(path.join(os.tmpdir(), 'scout-work-'));
    try {
      child_process.execSync('git init -q', { cwd: src });
      child_process.execSync('git config user.email t@t.t', { cwd: src });
      child_process.execSync('git config user.name t', { cwd: src });
      fs.writeFileSync(path.join(src, 'README.md'), '# Demo\nNEEDLE_TOKEN in readme\n');
      fs.writeFileSync(path.join(src, 'code.ts'), 'const NEEDLE_TOKEN = 1;\n');
      child_process.execSync('git add -A', { cwd: src });
      child_process.execSync('git commit -qm init', { cwd: src });

      const r = await scoutTool.execute({ repo: src, query: 'NEEDLE_TOKEN' }, ctx({ cwd: work }));
      assert.ok(!r.isError, r.output);
      assert.ok(r.output.includes('# Demo'), 'README content surfaced');
      assert.ok(r.output.includes('code.ts'), 'file listing surfaced');
      assert.ok(r.output.includes('Search results for "NEEDLE_TOKEN"'), 'query header present');
      assert.ok(r.output.includes('NEEDLE_TOKEN'), 'grep match surfaced');

      // Second call: cache hit (no re-clone, path already present).
      const cached = await scoutTool.execute({ repo: src }, ctx({ cwd: work }));
      assert.ok(!cached.isError);
      assert.ok(cached.output.includes('code.ts'));

      // Query with no match: grep returns non-zero -> caught, empty results.
      const noMatch = await scoutTool.execute({ repo: src, query: 'ZZ_NOT_PRESENT_ZZ' }, ctx({ cwd: work }));
      assert.ok(!noMatch.isError);
      assert.ok(!noMatch.output.includes('Search results for'), 'no results header when nothing matched');
    } finally {
      fs.rmSync(src, { recursive: true, force: true });
      fs.rmSync(work, { recursive: true, force: true });
    }
  });

  it('handles file:// URLs and a repo with no README (cache-hit path, no query)', async () => {
    const work = fs.mkdtempSync(path.join(os.tmpdir(), 'scout-nordme-'));
    try {
      // Pre-seed the cache so no clone happens; repoName derives from the raw repo string.
      const repoStr = 'file:///seeded/repo';
      const repoName = repoStr.replace(/[^a-zA-Z0-9_-]/g, '_');
      const cached = path.join(work, '.superinference', 'scout-cache', repoName);
      fs.mkdirSync(cached, { recursive: true });
      fs.writeFileSync(path.join(cached, 'main.go'), 'package main\n');

      const r = await scoutTool.execute({ repo: repoStr }, ctx({ cwd: work }));
      assert.ok(!r.isError, r.output);
      assert.ok(r.output.includes('main.go'));
      assert.ok(r.output.includes('### README:\n\n'), 'README section empty when none found');
    } finally {
      fs.rmSync(work, { recursive: true, force: true });
    }
  });

  it('expands owner/repo shorthand (else branch) via a seeded cache', async () => {
    const work = fs.mkdtempSync(path.join(os.tmpdir(), 'scout-short-'));
    try {
      const repoStr = 'octo/widget';
      const repoName = repoStr.replace(/[^a-zA-Z0-9_-]/g, '_');
      const cached = path.join(work, '.superinference', 'scout-cache', repoName);
      fs.mkdirSync(cached, { recursive: true });
      fs.writeFileSync(path.join(cached, 'readme.md'), 'lowercase readme body\n');
      const r = await scoutTool.execute({ repo: repoStr }, ctx({ cwd: work }));
      assert.ok(!r.isError);
      assert.ok(r.output.includes('lowercase readme body'), 'finds lowercase readme.md variant');
    } finally {
      fs.rmSync(work, { recursive: true, force: true });
    }
  });

  it('handles "." relative-path branch via a seeded cache', async () => {
    const work = fs.mkdtempSync(path.join(os.tmpdir(), 'scout-rel-'));
    try {
      const repoStr = './local-thing';
      const repoName = repoStr.replace(/[^a-zA-Z0-9_-]/g, '_');
      const cached = path.join(work, '.superinference', 'scout-cache', repoName);
      fs.mkdirSync(cached, { recursive: true });
      fs.writeFileSync(path.join(cached, 'x.py'), 'print(1)\n');
      const r = await scoutTool.execute({ repo: repoStr }, ctx({ cwd: work }));
      assert.ok(!r.isError);
      assert.ok(r.output.includes('x.py'));
    } finally {
      fs.rmSync(work, { recursive: true, force: true });
    }
  });

  it('returns an error when cloning a non-existent repo fails', async () => {
    const work = fs.mkdtempSync(path.join(os.tmpdir(), 'scout-fail-'));
    try {
      const r = await scoutTool.execute(
        { repo: '/nonexistent/path/to/repo-xyz' },
        ctx({ cwd: work }),
      );
      assert.ok(r.isError);
      assert.ok(r.output.includes('Error scouting'));
    } finally {
      fs.rmSync(work, { recursive: true, force: true });
    }
  });
});

// ===========================================================================
// team.ts
// ===========================================================================

describe('teamCreateTool / teamDeleteTool', () => {
  let tmp: string;
  beforeEach(() => {
    resetTeam();
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'si-team-cov-'));
  });
  afterEach(() => {
    resetTeam();
    fs.rmSync(tmp, { recursive: true, force: true });
  });

  it('creates a team with a description and persists team.json', async () => {
    const r = await teamCreateTool.execute(
      { team_name: 'alpha', description: 'research squad' },
      ctx({ cwd: tmp }),
    );
    assert.ok(!r.isError);
    const team = getCurrentTeam();
    assert.equal(team?.name, 'alpha');
    assert.equal(team?.description, 'research squad');
    const saved = JSON.parse(
      fs.readFileSync(path.join(tmp, '.superinference', 'teams', 'alpha', 'team.json'), 'utf-8'),
    );
    assert.equal(saved.name, 'alpha');
    assert.ok(fs.existsSync(path.join(tmp, '.superinference', 'teams', 'alpha', 'memory')));
  });

  it('refuses to create a second team while one exists', async () => {
    await teamCreateTool.execute({ team_name: 'alpha' }, ctx({ cwd: tmp }));
    const r = await teamCreateTool.execute({ team_name: 'beta' }, ctx({ cwd: tmp }));
    assert.ok(r.isError);
    assert.ok(r.output.includes('already exists'));
  });

  it('rejects an invalid team name', async () => {
    const r = await teamCreateTool.execute({ team_name: 'bad/name' }, ctx({ cwd: tmp }));
    assert.ok(r.isError);
    assert.ok(r.output.includes('alphanumeric'));
  });

  it('delete errors when there is no active team', async () => {
    const r = await teamDeleteTool.execute({}, ctx({ cwd: tmp }));
    assert.ok(r.isError);
    assert.ok(r.output.includes('No active team'));
  });

  it('deletes the active team and removes its directory', async () => {
    await teamCreateTool.execute({ team_name: 'gamma' }, ctx({ cwd: tmp }));
    const teamDir = path.join(tmp, '.superinference', 'teams', 'gamma');
    assert.ok(fs.existsSync(teamDir));
    const r = await teamDeleteTool.execute({}, ctx({ cwd: tmp }));
    assert.ok(!r.isError);
    assert.ok(r.output.includes('deleted'));
    assert.equal(getCurrentTeam(), null);
    assert.ok(!fs.existsSync(teamDir));
  });

  it('swallows rmSync failures during delete (catch branch) but still clears the team', async () => {
    // Create the team in a writable dir so currentTeam is set.
    await teamCreateTool.execute({ team_name: 'delta' }, ctx({ cwd: tmp }));
    // Build a second cwd where the computed teamDir cannot be removed:
    // make the parent `teams` directory read-only so unlink fails with EACCES.
    const roRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'si-team-ro-'));
    const teamsDir = path.join(roRoot, '.superinference', 'teams');
    const victimDir = path.join(teamsDir, 'delta');
    fs.mkdirSync(victimDir, { recursive: true });
    fs.writeFileSync(path.join(victimDir, 'f'), 'x');
    fs.chmodSync(teamsDir, 0o555);
    try {
      const r = await teamDeleteTool.execute({}, ctx({ cwd: roRoot }));
      // rmSync throws EACCES, is caught; team is still cleared and success returned.
      assert.ok(!r.isError);
      assert.ok(r.output.includes('deleted'));
      assert.equal(getCurrentTeam(), null);
      assert.ok(fs.existsSync(victimDir), 'the un-removable dir should remain (rm failed, was swallowed)');
    } finally {
      fs.chmodSync(teamsDir, 0o755);
      fs.rmSync(roRoot, { recursive: true, force: true });
    }
  });
});

// ===========================================================================
// task-list.ts
// ===========================================================================

describe('taskListTool', () => {
  it('definition is read-only', () => {
    assert.equal(taskListTool.name, 'task_list');
    assert.equal(taskListTool.isReadOnly, true);
  });

  it('errors when the process manager is unavailable', async () => {
    const r = await taskListTool.execute({}, ctx());
    assert.ok(r.isError);
    assert.ok(r.output.includes('not available'));
  });

  it('reports no tasks when the list is empty', async () => {
    const r = await taskListTool.execute({}, ctx({ processManager: { list: () => [] } as any }));
    assert.equal(r.isError, false);
    assert.ok(r.output.includes('No background tasks'));
  });

  it('renders every status and elapsed-time formatting branch', async () => {
    const now = Date.now();
    const tasks = [
      { taskId: 't-run', pid: 11, status: 'running', exitCode: null, startTime: now - 10, description: 'live job' },
      { taskId: 't-done', pid: 12, status: 'completed', exitCode: 0, startTime: now - 5000, description: 'done job' },
      { taskId: 't-kill', pid: 13, status: 'killed', exitCode: null, startTime: now - 120000, description: 'killed job' },
      { taskId: 't-fail', pid: 14, status: 'failed', exitCode: 2, startTime: now - 90000, description: 'failed job' },
    ];
    const r = await taskListTool.execute({}, ctx({ processManager: { list: () => tasks } as any }));
    assert.equal(r.isError, false);
    assert.ok(r.output.includes('Background tasks (4)'));
    // status arms
    assert.ok(r.output.includes('▶ running'));
    assert.ok(r.output.includes('✓ completed (exit 0)'));
    assert.ok(r.output.includes('✗ killed'));
    assert.ok(r.output.includes('✗ failed (exit 2)'));
    // elapsed-time arms: ms (<1s), s (<60s), m (>=60s)
    assert.ok(/\dms/.test(r.output), 'sub-second elapsed shown in ms');
    assert.ok(/\ds\b/.test(r.output), 'seconds elapsed shown in s');
    assert.ok(/\d\.\dm\b/.test(r.output), 'minutes elapsed shown in m');
  });
});
