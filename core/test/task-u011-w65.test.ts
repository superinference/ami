import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  taskTool,
  resetAgentRegistry,
  canonicalAgentToolNames,
  isOneShotAgent,
  ONE_SHOT_BUILTIN_AGENT_TYPES,
  parseAllowedTools,
} from '../src/tools/task';
import type { ToolContext, ToolDefinition } from '../src/types';

type Ev = Record<string, unknown>;

/** Run the task tool with a fake engine, capturing the config + resolved tools. */
async function runTask(
  input: Record<string, unknown>,
  ctx: Partial<ToolContext> = {},
  events: Ev[] = [{ type: 'text_delta', text: 'ok' }],
): Promise<{ isError?: boolean; output: string; toolNames: string[] }> {
  let toolNames: string[] = [];
  async function* fakeSubmit() {
    for (const e of events) yield e as never;
  }
  const result = await taskTool.execute(input, {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
    _engineFactory: (c) => { toolNames = (c.tools as ToolDefinition[]).map(t => t.name); return { submit: fakeSubmit }; },
    ...ctx,
  } as unknown as ToolContext);
  return { isError: result.isError, output: result.output, toolNames };
}

function skillManagerWith(agent: Record<string, unknown>) {
  return {
    getAgent: (n: string) => (n === agent.name ? agent : null),
    listAgents: () => [agent],
  } as unknown as ToolContext['_skillManager'];
}

// ---------------------------------------------------------------------------
// gap: permission-rule parsing of agent-def tool specs (Bash(git:*), aliases)
// ---------------------------------------------------------------------------
describe('canonicalAgentToolNames', () => {
  it('passes a bare canonical name through unchanged', () => {
    assert.deepEqual(canonicalAgentToolNames(['bash', 'file_read']), ['bash', 'file_read']);
  });
  it('maps a cross-framework alias to the AMI tool name', () => {
    assert.deepEqual(canonicalAgentToolNames(['Bash', 'Read', 'Edit']), ['bash', 'file_read', 'file_edit']);
  });
  it('extracts the base tool name from a patterned permission spec', () => {
    assert.deepEqual(canonicalAgentToolNames(['Bash(git:*)', 'Read(src/**)']), ['bash', 'file_read']);
  });
});

describe('agent-def tools accept alias + patterned specs (U011)', () => {
  beforeEach(() => resetAgentRegistry());

  it('resolves an allowlist of aliases/patterns instead of failing as unknown', async () => {
    const { isError, toolNames } = await runTask(
      { prompt: 'go', subagent_type: 'builder' },
      { _skillManager: skillManagerWith({ name: 'builder', tools: ['Bash(git:*)', 'Read'] }) },
    );
    // RED on revert: `new Set(agentDef.tools)` treats "Bash(git:*)"/"Read" as
    // unknown tool names and returns an error instead of resolving them.
    assert.ok(!isError, `expected success, got: ${isError}`);
    assert.deepEqual([...toolNames].sort(), ['bash', 'file_read']);
  });

  it('a patterned disallowedTools entry still blocks its base tool', async () => {
    const { isError, toolNames } = await runTask(
      { prompt: 'go', subagent_type: 'readonly' },
      { _skillManager: skillManagerWith({ name: 'readonly', disallowedTools: ['Bash(git:*)', 'Write'] }) },
    );
    assert.ok(!isError);
    assert.ok(!toolNames.includes('bash'), 'Bash(git:*) should strip bash');
    assert.ok(!toolNames.includes('file_edit'), 'Write alias should strip file_edit');
    assert.ok(toolNames.includes('file_read'), 'unrelated read tools remain');
  });

  it('still rejects a genuinely unknown tool name', async () => {
    const { isError, output } = await runTask(
      { prompt: 'go', subagent_type: 'typo' },
      { _skillManager: skillManagerWith({ name: 'typo', tools: ['no_such_tool'] }) },
    );
    assert.ok(isError);
    assert.match(output, /unknown tool/i);
  });
});

// ---------------------------------------------------------------------------
// gap: ONE_SHOT_BUILTIN_AGENT_TYPES continuation-trailer gate
// ---------------------------------------------------------------------------
describe('isOneShotAgent', () => {
  it('treats explore mode as one-shot', () => {
    assert.equal(isOneShotAgent(undefined, 'explore'), true);
  });
  it('treats a named one-shot built-in type as one-shot', () => {
    assert.equal(isOneShotAgent('Plan', 'general'), true);
    assert.equal(isOneShotAgent('Explore', 'general'), true);
    assert.ok(ONE_SHOT_BUILTIN_AGENT_TYPES.has('Plan'));
  });
  it('a general continuable agent is not one-shot', () => {
    assert.equal(isOneShotAgent(undefined, 'general'), false);
    assert.equal(isOneShotAgent('builder', 'general'), false);
  });
});

describe('continuation trailer is gated by one-shot classification (U011)', () => {
  beforeEach(() => resetAgentRegistry());

  it('omits the continue hint for a named Plan agent', async () => {
    const { output } = await runTask(
      { prompt: 'plan it', subagent_type: 'Plan' },
      { _skillManager: skillManagerWith({ name: 'Plan', systemPrompt: 'plan stance' }) },
    );
    // RED on revert: the old `effectiveMode === 'explore'` gate still emits the
    // continue hint for a named Plan agent.
    assert.ok(!/To continue this agent/.test(output), `Plan output should not offer continuation: ${output}`);
  });

  it('keeps the continue hint for a general subagent', async () => {
    const { output } = await runTask({ prompt: 'do work', mode: 'general' });
    assert.match(output, /To continue this agent/);
  });

  it('still omits the continue hint for explore mode', async () => {
    const { output } = await runTask({ prompt: 'look', mode: 'explore' });
    assert.ok(!/To continue this agent/.test(output));
  });
});

// ---------------------------------------------------------------------------
// gap: per-spawn allowed_tools inline tool scope
// ---------------------------------------------------------------------------
describe('parseAllowedTools', () => {
  it('returns undefined when absent', () => {
    assert.deepEqual(parseAllowedTools(undefined), { value: undefined });
    assert.deepEqual(parseAllowedTools(null), { value: undefined });
  });
  it('accepts a string array', () => {
    assert.deepEqual(parseAllowedTools(['bash', 'grep']), { value: ['bash', 'grep'] });
  });
  it('rejects a non-array / non-string-element shape', () => {
    assert.ok('error' in parseAllowedTools('bash'));
    assert.ok('error' in parseAllowedTools([1, 2]));
  });
});

describe('allowed_tools per-spawn scope (U011)', () => {
  beforeEach(() => resetAgentRegistry());

  it('is advertised in the input schema', () => {
    const props = taskTool.inputSchema.properties as Record<string, { type?: string }>;
    assert.ok('allowed_tools' in props);
    assert.equal(props.allowed_tools.type, 'array');
  });

  it('intersects the resolved tool set to exactly the named tools', async () => {
    const { isError, toolNames } = await runTask({ prompt: 'go', mode: 'general', allowed_tools: ['file_read', 'grep'] });
    assert.ok(!isError);
    assert.deepEqual([...toolNames].sort(), ['file_read', 'grep']);
  });

  it('accepts alias + patterned specs', async () => {
    const { isError, toolNames } = await runTask({ prompt: 'go', mode: 'general', allowed_tools: ['Read', 'Bash(git:*)'] });
    assert.ok(!isError);
    assert.deepEqual([...toolNames].sort(), ['bash', 'file_read']);
  });

  it('rejects an unknown tool name rather than silently narrowing', async () => {
    const { isError, output } = await runTask({ prompt: 'go', mode: 'general', allowed_tools: ['file_read', 'ghost_tool'] });
    assert.ok(isError);
    assert.match(output, /allowed_tools names unknown tool/i);
  });

  it('rejects a malformed (non-string-array) allowed_tools', async () => {
    const { isError, output } = await runTask({ prompt: 'go', mode: 'general', allowed_tools: 'bash' });
    assert.ok(isError);
    assert.match(output, /allowed_tools must be an array of strings/);
  });

  it('an intersection that empties the set surfaces the empty-tool guard', async () => {
    // explore mode is read-only (no file_edit); scoping it to file_edit leaves nothing.
    const { isError, output } = await runTask({ prompt: 'go', mode: 'explore', allowed_tools: ['file_edit'] });
    assert.ok(isError);
    assert.match(output, /empty/i);
  });
});
