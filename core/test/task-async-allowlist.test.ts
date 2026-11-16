import { describe, it, before, after, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  taskTool,
  resetAgentRegistry,
  filterToolsForAsyncAgent,
  ASYNC_AGENT_ALLOWED_TOOLS,
} from '../src/tools/task';
import type { ToolContext, ToolDefinition } from '../src/types';

type Ev = Record<string, unknown>;

let tmpData: string;
let tmpCwd: string;
let prevData: string | undefined;

before(() => {
  tmpData = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-async-data-'));
  tmpCwd = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-async-cwd-'));
  prevData = process.env.SUPERINFERENCE_DATA_DIR;
  process.env.SUPERINFERENCE_DATA_DIR = tmpData;
});

after(() => {
  if (prevData === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
  else process.env.SUPERINFERENCE_DATA_DIR = prevData;
  fs.rmSync(tmpData, { recursive: true, force: true });
  fs.rmSync(tmpCwd, { recursive: true, force: true });
});

beforeEach(() => resetAgentRegistry());

/**
 * Run the task tool with a fake engine, capturing the tool names the spawned
 * sub-engine was configured with. Works for both foreground and background
 * spawns because both call `_engineFactory(subConfig)` synchronously.
 */
async function runTask(
  input: Record<string, unknown>,
  ctx: Partial<ToolContext> = {},
): Promise<{ isError?: boolean; output: string; toolNames: string[] }> {
  let toolNames: string[] = [];
  async function* fakeSubmit() {
    yield { type: 'text_delta', text: 'ok' } as never;
  }
  const result = await taskTool.execute(input, {
    cwd: tmpCwd,
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
    _engineFactory: (c) => {
      toolNames = (c.tools as ToolDefinition[]).map(t => t.name);
      return { submit: fakeSubmit, shutdown() {} };
    },
    ...ctx,
  } as unknown as ToolContext);
  return { isError: result.isError, output: result.output, toolNames };
}

describe('filterToolsForAsyncAgent (unit)', () => {
  it('narrows a mixed tool set to only the async allowlist', () => {
    const fake = (name: string): ToolDefinition => ({ name } as ToolDefinition);
    const mixed = [
      fake('file_read'), fake('bash'), fake('task'), fake('git_commit'),
      fake('send_message'), fake('grep'), fake('config'), fake('skill'),
    ];
    const out = filterToolsForAsyncAgent(mixed).map(t => t.name);
    assert.deepEqual(out.sort(), ['bash', 'file_read', 'grep', 'skill']);
    for (const n of out) assert.ok(ASYNC_AGENT_ALLOWED_TOOLS.has(n));
  });

  it('the allowlist excludes recursion/interactive tools', () => {
    for (const blocked of ['task', 'task_output', 'task_kill', 'plan_mode', 'send_message', 'workflow', 'config', 'git_commit', 'AskUserQuestion']) {
      assert.ok(!ASYNC_AGENT_ALLOWED_TOOLS.has(blocked), `${blocked} must NOT be async-allowed`);
    }
  });
});

describe('run_in_background sub-agent tool set is intersected with the async allowlist', () => {
  it('a background general agent gets NO task tool and only async-allowed tools', async () => {
    const { isError, toolNames } = await runTask({ prompt: 'do it', description: 'bg', run_in_background: true });
    assert.equal(isError, undefined);
    assert.ok(!toolNames.includes('task'), 'background agent must not be able to spawn nested task agents');
    assert.ok(!toolNames.includes('send_message'));
    assert.ok(!toolNames.includes('git_commit'));
    assert.ok(toolNames.length > 0);
    for (const n of toolNames) {
      assert.ok(ASYNC_AGENT_ALLOWED_TOOLS.has(n), `background tool ${n} not in allowlist`);
    }
  });

  it('a FOREGROUND general agent KEEPS task (discriminates the background-only filter)', async () => {
    const { toolNames } = await runTask({ prompt: 'do it', description: 'fg' });
    assert.ok(toolNames.includes('task'), 'foreground agent keeps nested task when depth allows');
  });

  it('string "true" also triggers the async restriction (provider boolean coercion)', async () => {
    const { toolNames } = await runTask({ prompt: 'do it', description: 'bg-str', run_in_background: 'true' });
    assert.ok(!toolNames.includes('task'));
    for (const n of toolNames) assert.ok(ASYNC_AGENT_ALLOWED_TOOLS.has(n));
  });

  it('a wildcard agent-def tools:["*"] cannot widen a background agent past the allowlist', async () => {
    const agentDef = { name: 'wide', systemPrompt: 'p', tools: ['*'] };
    const skillManager = {
      getAgent: (n: string) => (n === 'wide' ? agentDef : undefined),
      listAgents: () => [agentDef],
    };
    const { toolNames } = await runTask(
      { prompt: 'x', description: 'w', subagent_type: 'wide', run_in_background: true },
      { _skillManager: skillManager } as Partial<ToolContext>,
    );
    assert.ok(!toolNames.includes('task'), 'even ["*"] must not reintroduce task for a background agent');
    for (const n of toolNames) assert.ok(ASYNC_AGENT_ALLOWED_TOOLS.has(n));
  });

  it('a background agent-def whose tools resolve entirely outside the allowlist fails the empty-set guard', async () => {
    const agentDef = { name: 'committer', systemPrompt: 'p', tools: ['git_commit'] };
    const skillManager = {
      getAgent: (n: string) => (n === 'committer' ? agentDef : undefined),
      listAgents: () => [agentDef],
    };
    const { isError, output } = await runTask(
      { prompt: 'x', description: 'c', subagent_type: 'committer', run_in_background: true },
      { _skillManager: skillManager } as Partial<ToolContext>,
    );
    assert.equal(isError, true);
    assert.match(output, /tool set for this subagent is empty/);
  });
});
