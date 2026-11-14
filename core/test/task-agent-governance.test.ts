import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { taskTool, resetAgentRegistry, ALL_AGENT_DISALLOWED_TOOLS, resolveSubagentModel } from '../src/tools/task';
import type { ToolContext, ToolDefinition } from '../src/types';

function skillManagerWith(tools: string[]) {
  return {
    getAgent: (t: string) => t === 'custom'
      ? { name: 'custom', systemPrompt: 'X', tools }
      : undefined,
    listAgents: () => [{ name: 'custom' }],
  };
}

async function runWithAgentTools(tools: string[]): Promise<{ isError?: boolean; output: string; toolNames: string[] }> {
  let toolNames: string[] = [];
  async function* fakeSubmit() { yield { type: 'text_delta' as const, text: 'ok' }; }
  const result = await taskTool.execute(
    { prompt: 'do it', subagent_type: 'custom' },
    {
      cwd: process.cwd(),
      abortSignal: new AbortController().signal,
      _providerConfig: { baseUrl: 'http://localhost', apiKey: 'k', model: 'm' },
      _skillManager: skillManagerWith(tools) as unknown as ToolContext['_skillManager'],
      _engineFactory: (cfg) => { toolNames = (cfg.tools as ToolDefinition[]).map(t => t.name); return { submit: fakeSubmit }; },
    } as ToolContext,
  );
  return { isError: result.isError, output: result.output, toolNames };
}

describe('task agent tool governance', () => {
  beforeEach(() => resetAgentRegistry());

  it('baseline denylist contains plan_mode and workflow', () => {
    assert.ok(ALL_AGENT_DISALLOWED_TOOLS.has('plan_mode'));
    assert.ok(ALL_AGENT_DISALLOWED_TOOLS.has('workflow'));
  });

  it('wildcard "*" allowlist grants all tools except the baseline denylist', async () => {
    const { isError, toolNames } = await runWithAgentTools(['*']);
    assert.equal(isError, undefined);
    assert.ok(toolNames.includes('file_read'));
    assert.ok(toolNames.includes('bash'));
    assert.ok(!toolNames.includes('plan_mode'), 'plan_mode stripped even under wildcard');
    assert.ok(!toolNames.includes('workflow'), 'workflow stripped even under wildcard');
  });

  it('rejects an allowlist naming an unknown tool', async () => {
    const { isError, output } = await runWithAgentTools(['file_read', 'nonexistent_tool']);
    assert.ok(isError);
    assert.ok(output.includes('nonexistent_tool'));
    assert.ok(output.includes('Available tools'));
  });

  it('strips workflow/plan_mode from an explicit allowlist that lists them', async () => {
    const { isError, toolNames } = await runWithAgentTools(['file_read', 'workflow', 'plan_mode']);
    assert.equal(isError, undefined);
    assert.ok(toolNames.includes('file_read'));
    assert.ok(!toolNames.includes('workflow'));
    assert.ok(!toolNames.includes('plan_mode'));
  });
});

describe('resolveSubagentModel anti-downgrade', () => {
  it('inherits the parent EXACT model when the alias matches the parent family', () => {
    assert.equal(resolveSubagentModel('opus', 'claude-opus-4-8'), 'claude-opus-4-8');
    assert.equal(resolveSubagentModel('sonnet', 'claude-sonnet-4-6'), 'claude-sonnet-4-6');
  });

  it('falls back to the generic alias when the parent is a different family', () => {
    assert.equal(resolveSubagentModel('opus', 'claude-sonnet-4-6'), 'claude-opus-4');
    assert.equal(resolveSubagentModel('haiku', 'gpt-4o'), 'claude-haiku-4-5');
  });

  it('passes a full model name through verbatim and returns undefined for no request', () => {
    assert.equal(resolveSubagentModel('claude-opus-4-8', 'claude-opus-4-8'), 'claude-opus-4-8');
    assert.equal(resolveSubagentModel('gpt-4o', 'claude-opus-4-8'), 'gpt-4o');
    assert.equal(resolveSubagentModel(undefined, 'claude-opus-4-8'), undefined);
  });

  it('task tool keeps a fork/child on the parent exact opus when model: opus is requested', async () => {
    let cfg: { provider?: { model?: string } } | null = null;
    async function* fakeSubmit() { yield { type: 'text_delta' as const, text: 'ok' }; }
    await taskTool.execute(
      { prompt: 'go', model: 'opus' },
      {
        cwd: process.cwd(),
        abortSignal: new AbortController().signal,
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
        _engineFactory: (c) => { cfg = c as typeof cfg; return { submit: fakeSubmit }; },
      } as unknown as ToolContext,
    );
    assert.equal(cfg!.provider!.model, 'claude-opus-4-8');
  });
});
