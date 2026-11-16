import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  taskTool,
  resetAgentRegistry,
  getAgentByName,
  sanitizeAgentName,
  parsePositiveIntInput,
  EXPLORE_AGENT_MIN_QUERIES,
  EXPLORE_SUBAGENT_SYSTEM_PROMPT,
  ALL_AGENT_DISALLOWED_TOOLS,
} from '../src/tools/task';
import type { ToolContext, ToolDefinition } from '../src/types';

type Ev = Record<string, unknown>;

/** Run the task tool with a fake engine, capturing the config + tool names. */
async function runTask(
  input: Record<string, unknown>,
  ctx: Partial<ToolContext> = {},
  events: Ev[] = [{ type: 'text_delta', text: 'ok' }],
): Promise<{
  isError?: boolean;
  output: string;
  cfg: { maxTurns?: number; permissionMode?: string } | null;
  toolNames: string[];
}> {
  let cfg: { maxTurns?: number; permissionMode?: string } | null = null;
  let toolNames: string[] = [];
  async function* fakeSubmit() {
    for (const e of events) yield e as never;
  }
  const result = await taskTool.execute(input, {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
    _engineFactory: (c) => { cfg = c as typeof cfg; toolNames = (c.tools as ToolDefinition[]).map(t => t.name); return { submit: fakeSubmit }; },
    ...ctx,
  } as unknown as ToolContext);
  return { isError: result.isError, output: result.output, cfg, toolNames };
}

// ---------------------------------------------------------------------------
// gap: maxTurns as a direct fork/task parameter
// ---------------------------------------------------------------------------
describe('max_turns direct subagent parameter', () => {
  beforeEach(() => resetAgentRegistry());

  it('schema advertises an optional max_turns number', () => {
    const props = taskTool.inputSchema.properties as Record<string, { type?: string }>;
    assert.ok('max_turns' in props);
    assert.equal(props.max_turns.type, 'number');
    assert.ok(!taskTool.inputSchema.required?.includes('max_turns'));
  });

  it('threads a valid max_turns into a mode-only general subagent (none otherwise)', async () => {
    const { cfg } = await runTask({ prompt: 'go', mode: 'general', max_turns: 7 });
    assert.equal(cfg!.maxTurns, 7);
  });

  it('accepts a provider-serialized string max_turns', async () => {
    const { cfg } = await runTask({ prompt: 'go', mode: 'general', max_turns: '12' });
    assert.equal(cfg!.maxTurns, 12);
  });

  it('wins over an agent definition maxTurns', async () => {
    const skillManager = {
      getAgent: () => ({ name: 'builder', maxTurns: 50 }),
      listAgents: () => [{ name: 'builder' }],
    };
    const { cfg } = await runTask(
      { prompt: 'go', subagent_type: 'builder', max_turns: 3 },
      { _skillManager: skillManager as unknown as ToolContext['_skillManager'] },
    );
    assert.equal(cfg!.maxTurns, 3);
  });

  it('wins over the fork default turn budget', async () => {
    const { cfg } = await runTask({ prompt: 'go', mode: 'fork', max_turns: 9 });
    assert.equal(cfg!.maxTurns, 9);
  });

  it('an invalid (zero/negative/non-numeric) max_turns is ignored (fork keeps its default)', async () => {
    const zero = await runTask({ prompt: 'go', mode: 'fork', max_turns: 0 });
    assert.equal(zero.cfg!.maxTurns, 200);
    const neg = await runTask({ prompt: 'go', mode: 'general', max_turns: -4 });
    assert.equal(neg.cfg!.maxTurns, undefined);
    const junk = await runTask({ prompt: 'go', mode: 'general', max_turns: 'lots' });
    assert.equal(junk.cfg!.maxTurns, undefined);
  });
});

describe('parsePositiveIntInput', () => {
  it('coerces positive numbers and numeric strings', () => {
    assert.equal(parsePositiveIntInput(5), 5);
    assert.equal(parsePositiveIntInput('42'), 42);
  });
  it('rejects absent / zero / negative / fractional / non-numeric', () => {
    assert.equal(parsePositiveIntInput(undefined), undefined);
    assert.equal(parsePositiveIntInput(null), undefined);
    assert.equal(parsePositiveIntInput(0), undefined);
    assert.equal(parsePositiveIntInput(-1), undefined);
    assert.equal(parsePositiveIntInput(2.5), undefined);
    assert.equal(parsePositiveIntInput('abc'), undefined);
  });
});

// ---------------------------------------------------------------------------
// gap: agent-name sanitization against reserved routing characters
// ---------------------------------------------------------------------------
describe('sanitizeAgentName', () => {
  it('replaces the @ identity separator and * broadcast wildcard with -', () => {
    assert.equal(sanitizeAgentName('scout@team'), 'scout-team');
    assert.equal(sanitizeAgentName('a*b'), 'a-b');
  });
  it('collapses runs and trims stray separators', () => {
    assert.equal(sanitizeAgentName('@@lead@@'), 'lead');
    assert.equal(sanitizeAgentName('x@*@y'), 'x-y');
  });
  it('returns empty when the name was entirely reserved characters', () => {
    assert.equal(sanitizeAgentName('*'), '');
    assert.equal(sanitizeAgentName('@@@'), '');
  });
  it('leaves a clean name untouched', () => {
    assert.equal(sanitizeAgentName('researcher'), 'researcher');
  });
});

describe('agent name is sanitized before it becomes a routing key', () => {
  beforeEach(() => resetAgentRegistry());

  it('a name containing @ registers under its sanitized form', async () => {
    await runTask({ prompt: 'go', mode: 'explore', name: 'scout@red' });
    assert.ok(getAgentByName('scout-red'), 'registered under sanitized name');
    assert.equal(getAgentByName('scout@red'), undefined, 'raw @ name is not a key');
  });

  it('a broadcast-wildcard name (*) does not register under the wildcard key', async () => {
    await runTask({ prompt: 'go', mode: 'explore', name: '*' });
    assert.equal(getAgentByName('*'), undefined, 'the broadcast wildcard is never an agent key');
  });
});

// ---------------------------------------------------------------------------
// gap: empty-effective-toolset boundary guard
// ---------------------------------------------------------------------------
describe('empty effective tool set boundary guard', () => {
  beforeEach(() => resetAgentRegistry());

  it('errors (no engine) when an agent allowlist resolves to only baseline-denied tools', async () => {
    const skillManager = {
      getAgent: () => ({ name: 'void', tools: ['plan_mode', 'workflow'] }),
      listAgents: () => [{ name: 'void' }],
    };
    const { isError, output, cfg } = await runTask(
      { prompt: 'go', subagent_type: 'void' },
      { _skillManager: skillManager as unknown as ToolContext['_skillManager'] },
    );
    assert.ok(isError, 'spawn rejected');
    assert.match(output, /tool set for this subagent is empty/);
    assert.match(output, /void/);
    assert.equal(cfg, null, 'no engine created with a zero-tool set');
  });

  it('errors when an allowlist is fully cancelled by its own denylist', async () => {
    const skillManager = {
      getAgent: () => ({ name: 'nil', tools: ['file_read', 'grep'], disallowedTools: ['file_read', 'grep'] }),
      listAgents: () => [{ name: 'nil' }],
    };
    const { isError, output, cfg } = await runTask(
      { prompt: 'go', subagent_type: 'nil' },
      { _skillManager: skillManager as unknown as ToolContext['_skillManager'] },
    );
    assert.ok(isError);
    assert.match(output, /empty/);
    assert.equal(cfg, null);
  });

  it('a normal agent with real tools is NOT blocked by the guard', async () => {
    const skillManager = {
      getAgent: () => ({ name: 'ok', tools: ['file_read', 'grep'] }),
      listAgents: () => [{ name: 'ok' }],
    };
    const { isError, toolNames } = await runTask(
      { prompt: 'go', subagent_type: 'ok' },
      { _skillManager: skillManager as unknown as ToolContext['_skillManager'] },
    );
    assert.ok(!isError);
    assert.ok(toolNames.includes('file_read'));
  });
});

// ---------------------------------------------------------------------------
// gap: minimum-query boundary for the explore agent
// ---------------------------------------------------------------------------
describe('EXPLORE_AGENT_MIN_QUERIES floor', () => {
  it('exports the minimum-query constant (3)', () => {
    assert.equal(EXPLORE_AGENT_MIN_QUERIES, 3);
  });
  it('the explore prompt cites the floor so it guards under-exploration', () => {
    assert.match(EXPLORE_SUBAGENT_SYSTEM_PROMPT, new RegExp(`at least ${EXPLORE_AGENT_MIN_QUERIES} distinct searches`));
    assert.match(EXPLORE_SUBAGENT_SYSTEM_PROMPT, /before concluding something is absent/);
  });
});

// ---------------------------------------------------------------------------
// sanity: the universal denylist is the set the guard references
// ---------------------------------------------------------------------------
describe('ALL_AGENT_DISALLOWED_TOOLS floor', () => {
  it('contains the interactive/coordination tools that can never be re-added', () => {
    for (const t of ['plan_mode', 'workflow', 'AskUserQuestion', 'task_output', 'task_kill']) {
      assert.ok(ALL_AGENT_DISALLOWED_TOOLS.has(t), `${t} in floor`);
    }
  });
});
