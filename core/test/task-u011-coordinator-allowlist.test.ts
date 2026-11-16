import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  taskTool,
  resetAgentRegistry,
  filterToolsForAgent,
  parseAllowedAgentTypes,
  isAgentTypeAllowed,
  COORDINATOR_MODE_ALLOWED_TOOLS,
  COORDINATOR_SUBAGENT_SYSTEM_PROMPT,
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
  cfg: { permissionMode?: string; systemPrompt?: string } | null;
  toolNames: string[];
}> {
  let cfg: { permissionMode?: string; systemPrompt?: string } | null = null;
  let toolNames: string[] = [];
  async function* fakeSubmit() {
    for (const e of events) yield e as never;
  }
  const result = await taskTool.execute(input, {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
    _engineFactory: (c) => {
      cfg = c as typeof cfg;
      toolNames = (c.tools as ToolDefinition[]).map(t => t.name);
      return { submit: fakeSubmit };
    },
    ...ctx,
  } as unknown as ToolContext);
  return { isError: result.isError, output: result.output, cfg, toolNames };
}

const fakeTools = (names: string[]): ToolDefinition[] =>
  names.map(name => ({ name } as unknown as ToolDefinition));

// ---------------------------------------------------------------------------
// gaps U011 #7/#84: coordinator-mode restricted (orchestration-only) tool set
// ---------------------------------------------------------------------------
describe('coordinator mode restricted tool set', () => {
  beforeEach(() => resetAgentRegistry());

  it('exports the orchestration-only allowlist (spawn/message/output, no file/shell)', () => {
    assert.ok(COORDINATOR_MODE_ALLOWED_TOOLS.has('task'));
    assert.ok(COORDINATOR_MODE_ALLOWED_TOOLS.has('send_message'));
    assert.ok(COORDINATOR_MODE_ALLOWED_TOOLS.has('structured_output'));
    for (const forbidden of ['file_edit', 'bash', 'file_read', 'grep']) {
      assert.ok(!COORDINATOR_MODE_ALLOWED_TOOLS.has(forbidden), `${forbidden} excluded`);
    }
  });

  it('schema advertises "coordinator" as a valid mode', () => {
    const props = taskTool.inputSchema.properties as Record<string, { enum?: string[] }>;
    assert.ok(props.mode.enum?.includes('coordinator'));
  });

  it('filterToolsForAgent("coordinator") keeps only the orchestration allowlist', () => {
    const all = fakeTools(['task', 'send_message', 'structured_output', 'file_edit', 'bash', 'file_read']);
    const kept = filterToolsForAgent(all, 'coordinator', 0, 5).map(t => t.name);
    assert.deepEqual(kept.sort(), ['send_message', 'structured_output', 'task']);
  });

  it('coordinator drops "task" when nesting depth is exhausted (but keeps output/message)', () => {
    const all = fakeTools(['task', 'send_message', 'structured_output']);
    const kept = filterToolsForAgent(all, 'coordinator', 5, 5).map(t => t.name);
    assert.ok(!kept.includes('task'), 'task stripped past max depth');
    assert.deepEqual(kept.sort(), ['send_message', 'structured_output']);
  });

  it('spawns a coordinator with orchestration tools and NO file/shell tools', async () => {
    const { isError, toolNames, cfg } = await runTask({ prompt: 'orchestrate', mode: 'coordinator' });
    assert.ok(!isError, 'coordinator spawn ok');
    assert.ok(toolNames.includes('task'), 'has task (delegation)');
    assert.ok(toolNames.includes('structured_output'), 'has structured_output');
    for (const forbidden of ['file_edit', 'bash', 'file_read', 'grep', 'web_fetch']) {
      assert.ok(!toolNames.includes(forbidden), `coordinator must not get ${forbidden}`);
    }
    // universal floor still applies on top of the coordinator allowlist
    for (const t of toolNames) {
      assert.ok(!ALL_AGENT_DISALLOWED_TOOLS.has(t), `${t} should not survive the floor`);
    }
    assert.equal(cfg!.systemPrompt, COORDINATOR_SUBAGENT_SYSTEM_PROMPT);
  });

  it('the coordinator prompt tells it to delegate and that it has no file/shell tools', () => {
    assert.match(COORDINATOR_SUBAGENT_SYSTEM_PROMPT, /delegate/i);
    assert.match(COORDINATOR_SUBAGENT_SYSTEM_PROMPT, /task tool/i);
    assert.match(COORDINATOR_SUBAGENT_SYSTEM_PROMPT, /NO file, edit, or shell tools/);
  });
});

// ---------------------------------------------------------------------------
// gaps U011 #122/#123/#124: allowed_agent_types spawn-type restriction
// ---------------------------------------------------------------------------
describe('parseAllowedAgentTypes', () => {
  it('treats absent / null as no restriction', () => {
    assert.deepEqual(parseAllowedAgentTypes(undefined), { value: undefined });
    assert.deepEqual(parseAllowedAgentTypes(null), { value: undefined });
  });
  it('accepts an array of strings (incl. empty)', () => {
    assert.deepEqual(parseAllowedAgentTypes(['a', 'b']), { value: ['a', 'b'] });
    assert.deepEqual(parseAllowedAgentTypes([]), { value: [] });
  });
  it('rejects a non-array or non-string elements', () => {
    assert.ok('error' in parseAllowedAgentTypes('a'));
    assert.ok('error' in parseAllowedAgentTypes([1, 2]));
    assert.ok('error' in parseAllowedAgentTypes(['a', 3]));
  });
});

describe('isAgentTypeAllowed', () => {
  it('undefined allowlist permits any type', () => {
    assert.equal(isAgentTypeAllowed('researcher', undefined), true);
  });
  it('permits a member and rejects a non-member', () => {
    assert.equal(isAgentTypeAllowed('researcher', ['researcher', 'builder']), true);
    assert.equal(isAgentTypeAllowed('attacker', ['researcher', 'builder']), false);
  });
  it('an empty allowlist forbids every named type', () => {
    assert.equal(isAgentTypeAllowed('researcher', []), false);
  });
});

describe('allowed_agent_types enforcement in task.execute', () => {
  beforeEach(() => resetAgentRegistry());

  const skillManager = {
    getAgent: (n: string) => ({ name: n, tools: ['file_read', 'grep'] }),
    listAgents: () => [{ name: 'researcher' }, { name: 'builder' }],
  };

  it('schema advertises allowed_agent_types as a string array', () => {
    const props = taskTool.inputSchema.properties as Record<string, { type?: string; items?: { type?: string } }>;
    assert.equal(props.allowed_agent_types.type, 'array');
    assert.equal(props.allowed_agent_types.items?.type, 'string');
  });

  it('rejects a subagent_type outside the allowlist (before the agent is resolved)', async () => {
    const { isError, output, cfg } = await runTask(
      { prompt: 'go', subagent_type: 'builder', allowed_agent_types: ['researcher'] },
      { _skillManager: skillManager as unknown as ToolContext['_skillManager'] },
    );
    assert.ok(isError, 'spawn rejected');
    assert.match(output, /subagent_type "builder" is not permitted/);
    assert.match(output, /Allowed agent types: researcher/);
    assert.equal(cfg, null, 'no engine created for a forbidden type');
  });

  it('permits a subagent_type inside the allowlist', async () => {
    const { isError, toolNames } = await runTask(
      { prompt: 'go', subagent_type: 'researcher', allowed_agent_types: ['researcher'] },
      { _skillManager: skillManager as unknown as ToolContext['_skillManager'] },
    );
    assert.ok(!isError);
    assert.ok(toolNames.includes('file_read'));
  });

  it('an empty allowlist forbids every named subagent_type', async () => {
    const { isError, output } = await runTask(
      { prompt: 'go', subagent_type: 'researcher', allowed_agent_types: [] },
      { _skillManager: skillManager as unknown as ToolContext['_skillManager'] },
    );
    assert.ok(isError);
    assert.match(output, /Allowed agent types: \(none\)/);
  });

  it('does not restrict a mode-only spawn (no subagent_type)', async () => {
    const { isError } = await runTask({ prompt: 'go', mode: 'explore', allowed_agent_types: ['researcher'] });
    assert.ok(!isError, 'mode-only spawn is unaffected by the allowlist');
  });

  it('rejects a malformed allowed_agent_types value', async () => {
    const { isError, output } = await runTask({ prompt: 'go', allowed_agent_types: 'researcher' });
    assert.ok(isError);
    assert.match(output, /allowed_agent_types must be an array of strings/);
  });
});
