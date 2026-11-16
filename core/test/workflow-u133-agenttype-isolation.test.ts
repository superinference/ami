import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as child_process from 'child_process';
import { workflowTool } from '../src/tools/workflow';
import type { ToolContext } from '../src/types';
import type { AgentDefinition } from '../src/skills';

// U133 (continuation) — two previously-unwired Workflow tool knobs:
//   1. `agentType` — resolve a specialized agent definition (system prompt,
//      model, permission mode, tool allowlist) and apply it to the spawned
//      sub-engine. RED: the handler never consumed opts.agentType, so a workflow
//      calling agent(p, { agentType: 'reviewer' }) got a generic engine.
//   2. `isolation: 'worktree'` — run the agent inside an isolated git worktree.
//      RED: the handler built every sub-engine against context.cwd regardless of
//      opts.isolation.
// Each test discriminates the new behavior from the pre-fix behavior.

const dirs: string[] = [];
afterEach(() => { for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true }); });
function mkcwd(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wf-u133at-'));
  dirs.push(d);
  return d;
}

function agentDef(partial: Partial<AgentDefinition> & { name: string }): AgentDefinition {
  return {
    description: 'test agent',
    systemPrompt: 'base prompt',
    filePath: '/virtual',
    ...partial,
  };
}

/** Capturing engine factory + an optional skill-manager stub returning agents. */
function capturingCtx(cwd: string, agents: AgentDefinition[] = []): { ctx: ToolContext; configs: any[] } {
  const byName = new Map(agents.map(a => [a.name, a]));
  const configs: any[] = [];
  const ctx: any = {
    cwd,
    abortSignal: new AbortController().signal,
    _providerConfig: { provider: 'test', model: 'base-model', baseUrl: 'u', apiKey: 'k' },
    _engineFactory: (config: any) => {
      configs.push(config);
      return {
        async *submit() { yield { type: 'text_delta', text: 'ok' } as any; },
        shutdown() {},
      };
    },
    _skillManager: {
      getAgent: (name: string) => byName.get(name) ?? null,
      listAgents: () => agents,
    },
  };
  return { ctx, configs };
}

describe('U133 workflow — specialized agentType knob', () => {
  it('applies the resolved agent system prompt, model, and permission mode to the sub-engine', async () => {
    const cwd = mkcwd();
    const { ctx, configs } = capturingCtx(cwd, [
      agentDef({ name: 'reviewer', systemPrompt: 'You are a reviewer.', model: 'review-model', permissionMode: 'deny-all' }),
    ]);
    const script = "export const meta = { name: 'm', description: 'd' }\nreturn await agent('do', { agentType: 'reviewer' })";
    const r = await workflowTool.execute!({ script }, ctx);
    // RED (unwired): agentType was ignored — no systemPrompt, provider.model stays 'base-model', permissionMode 'auto-allow'.
    assert.equal(r.isError, false);
    assert.equal(configs.length, 1);
    assert.equal(configs[0].systemPrompt, 'You are a reviewer.');
    assert.equal(configs[0].provider.model, 'review-model');
    assert.equal(configs[0].permissionMode, 'deny-all');
  });

  it('restricts the sub-engine toolset to the agent tool allowlist (minus disallowed)', async () => {
    const cwd = mkcwd();
    const { ctx, configs } = capturingCtx(cwd, [
      agentDef({ name: 'readonly', tools: ['file_read', 'grep', 'bash'], disallowedTools: ['bash'] }),
    ]);
    const script = "export const meta = { name: 'm', description: 'd' }\nreturn await agent('do', { agentType: 'readonly' })";
    await workflowTool.execute!({ script }, ctx);
    const names: string[] = (configs[0].tools as any[]).map(t => t.name);
    assert.ok(names.includes('file_read') && names.includes('grep'));
    assert.ok(!names.includes('bash'), 'disallowed tool must be subtracted from the allowlist');
  });

  it('lets an explicit per-call model override the agent-type model', async () => {
    const cwd = mkcwd();
    const { ctx, configs } = capturingCtx(cwd, [
      agentDef({ name: 'reviewer', model: 'review-model' }),
    ]);
    const script = "export const meta = { name: 'm', description: 'd' }\nreturn await agent('do', { agentType: 'reviewer', model: 'opus-x' })";
    await workflowTool.execute!({ script }, ctx);
    assert.equal(configs[0].provider.model, 'opus-x');
  });

  it('returns a clear error (and spawns nothing) for an unknown agentType', async () => {
    const cwd = mkcwd();
    const { ctx, configs } = capturingCtx(cwd, [agentDef({ name: 'reviewer' })]);
    const script = "export const meta = { name: 'm', description: 'd' }\nreturn await agent('do', { agentType: 'nope' })";
    const r = await workflowTool.execute!({ script }, ctx);
    assert.equal(configs.length, 0, 'no sub-engine must be built for an unknown agent type');
    assert.match(r.output, /agentType .*nope.* not found/);
    assert.match(r.output, /Available agents: reviewer/);
  });

  it('errors when the agent allowlist names an unknown tool', async () => {
    const cwd = mkcwd();
    const { ctx, configs } = capturingCtx(cwd, [agentDef({ name: 'broken', tools: ['file_read', 'no_such_tool'] })]);
    const script = "export const meta = { name: 'm', description: 'd' }\nreturn await agent('do', { agentType: 'broken' })";
    const r = await workflowTool.execute!({ script }, ctx);
    assert.equal(configs.length, 0);
    assert.match(r.output, /unknown tool\(s\): no_such_tool/);
  });

  it('ignores a model of "inherit" (keeps the parent model)', async () => {
    const cwd = mkcwd();
    const { ctx, configs } = capturingCtx(cwd, [agentDef({ name: 'inheritor', model: 'inherit' })]);
    const script = "export const meta = { name: 'm', description: 'd' }\nreturn await agent('do', { agentType: 'inheritor' })";
    await workflowTool.execute!({ script }, ctx);
    assert.equal(configs[0].provider.model, 'base-model');
  });
});

describe('U133 workflow — per-agent worktree isolation knob', () => {
  it('runs the agent inside an isolated git worktree and cleans it up when change-free', async () => {
    const repo = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wf-wt-'));
    const remote = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wf-wt-remote-'));
    dirs.push(repo, remote);
    const g = (args: string, cwd = repo): string =>
      child_process.execSync(`git ${args}`, { cwd, encoding: 'utf-8', stdio: 'pipe' });
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
    const ctx: any = {
      cwd: repo,
      abortSignal: new AbortController().signal,
      _providerConfig: { provider: 'test', model: 'm', baseUrl: 'u', apiKey: 'k' },
      _engineFactory: (cfg: any) => {
        runCwd = cfg.cwd;
        return { async *submit() { yield { type: 'text_delta', text: 'done' } as any; }, shutdown() {} };
      },
    };
    const script = "export const meta = { name: 'm', description: 'd' }\nreturn await agent('do', { isolation: 'worktree' })";
    const r = await workflowTool.execute!({ script }, ctx);
    assert.equal(r.isError, false, r.output);
    // RED (unwired): cfg.cwd was always the repo root, never a worktree path.
    assert.ok(runCwd.includes(path.join('.ami', 'worktrees')), `agent must run in a worktree, got ${runCwd}`);
    assert.notEqual(runCwd, repo);
    // A change-free worktree is removed when the agent finishes.
    assert.ok(!fs.existsSync(runCwd), 'the change-free worktree must be cleaned up');
  });

  it('surfaces a clean error (no throw) when a worktree cannot be created (non-git cwd)', async () => {
    const cwd = mkcwd();
    let built = false;
    const ctx: any = {
      cwd,
      abortSignal: new AbortController().signal,
      _providerConfig: { provider: 'test', model: 'm', baseUrl: 'u', apiKey: 'k' },
      _engineFactory: () => { built = true; return { async *submit() {}, shutdown() {} }; },
    };
    const script = "export const meta = { name: 'm', description: 'd' }\nreturn await agent('do', { isolation: 'worktree' })";
    const r = await workflowTool.execute!({ script }, ctx);
    assert.equal(r.isError, false);
    assert.equal(built, false, 'no sub-engine should be built when the worktree cannot be created');
    assert.match(r.output, /failed to create isolated worktree/);
  });
});
