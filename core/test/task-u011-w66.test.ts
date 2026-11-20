import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as child_process from 'child_process';

import {
  taskTool,
  resetAgentRegistry,
  INTERNAL_WORKER_TOOLS,
  ASYNC_AGENT_ALLOWED_TOOLS,
  buildCoordinatorWorkerContext,
  connectedMcpServerNames,
  readWorktreeBranch,
  buildWorktreeCompletionNotice,
} from '../src/tools/task';
import { projectStateDir } from '../src/reference-journal';
import { ProcessManager } from '../src/process-manager';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

// ===========================================================================
// U011-w66 — coordinator worker-capability affordance (getCoordinatorUserContext)
// ===========================================================================

describe('U011-w66 — coordinator worker-tools context', () => {
  it('lists the async worker tool set with the internal plumbing filtered out, sorted', () => {
    const content = buildCoordinatorWorkerContext();
    const expected = [...ASYNC_AGENT_ALLOWED_TOOLS]
      .filter(n => !INTERNAL_WORKER_TOOLS.has(n))
      .sort()
      .join(', ');
    assert.ok(content.startsWith('Workers you spawn with the task tool have access to these tools: '));
    assert.ok(content.includes(expected), `worker tools must be the filtered+sorted set, got: ${content}`);
    // structured_output is an INTERNAL coordination tool — it must NOT appear.
    assert.ok(!content.includes('structured_output'), 'internal plumbing must be filtered out');
  });

  it('is deterministically sorted (stable across calls for cache-friendliness)', () => {
    assert.equal(buildCoordinatorWorkerContext(), buildCoordinatorWorkerContext());
  });

  it('appends connected MCP server names when present, omits the clause otherwise', () => {
    const withMcp = buildCoordinatorWorkerContext(['github', 'linear']);
    assert.ok(withMcp.includes('connected MCP servers: github, linear'));
    const without = buildCoordinatorWorkerContext([]);
    assert.ok(!without.includes('MCP'), 'no MCP clause when no servers connected');
  });

  it('connectedMcpServerNames keeps only connected servers, deduped and sorted', () => {
    const c = ctx({
      _mcpManager: {
        listAllServers: () => [
          { name: 'zeta', state: 'connected' },
          { name: 'alpha', state: 'connected' },
          { name: 'alpha', state: 'connected' },
          { name: 'pending-one', state: 'connecting' },
          { name: 'broken', state: 'failed' },
        ],
      },
    } as unknown as Partial<ToolContext>);
    assert.deepEqual(connectedMcpServerNames(c), ['alpha', 'zeta']);
  });

  it('connectedMcpServerNames returns [] with no manager / no accessor / on throw', () => {
    assert.deepEqual(connectedMcpServerNames(ctx()), []);
    assert.deepEqual(connectedMcpServerNames(ctx({ _mcpManager: {} } as Partial<ToolContext>)), []);
    assert.deepEqual(
      connectedMcpServerNames(ctx({ _mcpManager: { listAllServers: () => { throw new Error('x'); } } } as unknown as Partial<ToolContext>)),
      [],
    );
  });

  it('END-TO-END: a coordinator spawn prepends the worker-capability context to its first turn', async () => {
    resetAgentRegistry();
    let capturedPrompt = '';
    async function* fakeSubmit(p: string) {
      capturedPrompt = p;
      yield { type: 'text_delta' as const, text: 'ok' };
    }
    await taskTool.execute(
      { prompt: 'orchestrate the refactor', mode: 'coordinator' },
      ctx({
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _engineFactory: () => ({ submit: fakeSubmit }),
        _mcpManager: { listAllServers: () => [{ name: 'github', state: 'connected' }] },
      } as unknown as Partial<ToolContext>),
    );
    assert.ok(capturedPrompt.startsWith('Workers you spawn with the task tool have access to these tools:'),
      `coordinator must be told its workers' tools, got: ${capturedPrompt.slice(0, 80)}`);
    assert.ok(capturedPrompt.includes('connected MCP servers: github'));
    assert.ok(capturedPrompt.includes('orchestrate the refactor'), 'original task must be preserved');
  });

  it('a non-coordinator spawn does NOT get the worker-capability context', async () => {
    resetAgentRegistry();
    let capturedPrompt = '';
    async function* fakeSubmit(p: string) { capturedPrompt = p; yield { type: 'text_delta' as const, text: 'ok' }; }
    await taskTool.execute(
      { prompt: 'just explore', mode: 'explore' },
      ctx({
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _engineFactory: () => ({ submit: fakeSubmit }),
      } as Partial<ToolContext>),
    );
    assert.ok(!capturedPrompt.includes('Workers you spawn with the task tool'));
  });
});

// ===========================================================================
// U011-w66 — worktree completion notice (path + branch)
// ===========================================================================

describe('U011-w66 — worktree completion notice', () => {
  it('buildWorktreeCompletionNotice includes path and branch, drops branch clause when absent', () => {
    const withBranch = buildWorktreeCompletionNotice('/tmp/wt', 'ami/agent-abc');
    assert.ok(withBranch.includes('/tmp/wt'));
    assert.ok(withBranch.includes('on branch ami/agent-abc'));
    const noBranch = buildWorktreeCompletionNotice('/tmp/wt');
    assert.ok(noBranch.includes('/tmp/wt'));
    assert.ok(!noBranch.includes('on branch'));
  });

  it('readWorktreeBranch returns the checked-out branch of a real repo, undefined for a non-git dir', async () => {
    const nogit = fs.mkdtempSync(path.join(os.tmpdir(), 'si-nogit-'));
    try {
      assert.equal(await readWorktreeBranch(nogit), undefined);
    } finally {
      fs.rmSync(nogit, { recursive: true, force: true });
    }
    const repo = fs.mkdtempSync(path.join(os.tmpdir(), 'si-repo-'));
    try {
      child_process.execSync('git init -q -b feature-x', { cwd: repo });
      child_process.execSync('git config user.email t@t.t', { cwd: repo });
      child_process.execSync('git config user.name t', { cwd: repo });
      fs.writeFileSync(path.join(repo, 'a.txt'), 'hi\n');
      child_process.execSync('git add -A && git commit -qm init', { cwd: repo });
      assert.equal(await readWorktreeBranch(repo), 'feature-x');
    } finally {
      fs.rmSync(repo, { recursive: true, force: true });
    }
  });

  it('END-TO-END: a RETAINED (dirty) subagent worktree surfaces its path + branch in the result', async () => {
    resetAgentRegistry();
    const repo = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wt-dirty-'));
    const remote = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wt-dirty-remote-'));
    const g = (args: string, cwd = repo) => child_process.execSync(`git ${args}`, { cwd, encoding: 'utf-8', stdio: 'pipe' });
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
        { prompt: 'make a change', mode: 'explore', isolation: 'worktree' },
        ctx({
          cwd: repo,
          _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
          _engineFactory: (cfg: { cwd: string }) => {
            runCwd = cfg.cwd;
            // The agent leaves an uncommitted change behind, so the worktree is retained.
            return {
              submit: async function* () {
                fs.writeFileSync(path.join(cfg.cwd, 'new-file.txt'), 'agent work\n');
                yield { type: 'text_delta' as const, text: 'done' };
              },
            };
          },
        } as unknown as Partial<ToolContext>),
      );
      assert.ok(!r.output.includes('worktree cleaned up'), 'a dirty worktree must NOT be cleaned up');
      assert.ok(r.output.includes(runCwd), `result must surface the retained worktree path, got: ${r.output}`);
      assert.ok(/on branch /.test(r.output), 'result must surface the worktree branch');
      assert.ok(fs.existsSync(runCwd), 'the dirty worktree must be preserved');
    } finally {
      fs.rmSync(repo, { recursive: true, force: true });
      fs.rmSync(remote, { recursive: true, force: true });
    }
  });
});

// ===========================================================================
// U011-w66 — background completion: killed vs failed + usage summary
// ===========================================================================

describe('U011-w66 — background terminal state + usage summary', () => {
  const ORIG = process.env.AMI_SUBAGENT_STALL_TIMEOUT_MS;
  beforeEach(() => resetAgentRegistry());
  afterEach(() => {
    if (ORIG === undefined) delete process.env.AMI_SUBAGENT_STALL_TIMEOUT_MS;
    else process.env.AMI_SUBAGENT_STALL_TIMEOUT_MS = ORIG;
  });

  it('a clean background run completes and writes a usage summary footer', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'si-bg-ok-'));
    const pm = new ProcessManager(tmp);
    try {
      async function* submit(): AsyncGenerator<any> {
        yield { type: 'tool_use_start', toolName: 'file_read', input: { file_path: '/x' } };
        yield { type: 'usage_update', stats: { totalTokens: 321 } };
        yield { type: 'text_delta', text: 'all good' };
      }
      const res = await taskTool.execute(
        { prompt: 'bg ok', run_in_background: true },
        ctx({ cwd: tmp, _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' }, processManager: pm,
          _engineFactory: () => ({ submit, shutdown: () => {} }) }),
      );
      const taskId = res.output.match(/agent-[a-f0-9]+/)![0];
      await sleep(120);
      const content = fs.readFileSync(path.join(projectStateDir(tmp, 'tasks'), `${taskId}.output`), 'utf-8');
      assert.ok(content.includes('[Agent stats: 1 tool calls, 321 tokens'), `usage footer missing, got: ${content}`);
      assert.equal((pm as any).processes.get(taskId).status, 'completed');
    } finally {
      pm.cleanup();
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('a genuine engine error marks the background agent FAILED (not killed)', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'si-bg-err-'));
    const pm = new ProcessManager(tmp);
    try {
      async function* submit(): AsyncGenerator<any> {
        yield { type: 'text_delta', text: 'start' };
        yield { type: 'error', error: 'provider exploded' };
      }
      const res = await taskTool.execute(
        { prompt: 'bg err', run_in_background: true },
        ctx({ cwd: tmp, _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' }, processManager: pm,
          _engineFactory: () => ({ submit, shutdown: () => {} }) }),
      );
      const taskId = res.output.match(/agent-[a-f0-9]+/)![0];
      await sleep(120);
      assert.equal((pm as any).processes.get(taskId).status, 'failed');
    } finally {
      pm.cleanup();
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('an externally-aborted background agent is marked KILLED (not failed)', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'si-bg-kill-'));
    const pm = new ProcessManager(tmp);
    let resolveHold!: () => void;
    const hold = new Promise<void>(r => { resolveHold = r; });
    try {
      async function* submit(): AsyncGenerator<any> {
        yield { type: 'text_delta', text: 'running' };
        await hold;
        yield { type: 'text_delta', text: 'never' };
      }
      const res = await taskTool.execute(
        { prompt: 'bg kill', run_in_background: true },
        ctx({ cwd: tmp, _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' }, processManager: pm,
          _engineFactory: () => ({ submit, shutdown: () => { resolveHold(); } }) }),
      );
      const taskId = res.output.match(/agent-[a-f0-9]+/)![0];
      await sleep(40);
      // External kill (task_kill → abortFn → subAbort.abort).
      pm.kill(taskId);
      resolveHold();
      await sleep(120);
      assert.equal((pm as any).processes.get(taskId).status, 'killed');
    } finally {
      pm.cleanup();
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });
});
