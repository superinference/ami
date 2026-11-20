import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  validateAgentType,
  validateSkillName,
  parseSystemPromptMode,
  SYSTEM_PROMPT_MODES,
  validateAgentDefinition,
  type AgentDefinition,
} from '../src/skills';
import {
  taskTool,
  resetAgentRegistry,
  SUBAGENT_SYSTEM_PROMPT,
  SYSTEM_PROMPT_MODES as TASK_SYSTEM_PROMPT_MODES,
} from '../src/tools/task';
import type { ToolContext, ToolDefinition, EngineConfig } from '../src/types';

// ---------------------------------------------------------------------------
// F1 — validateAgentType / validateSkillName (parity: the reference validateAgentType)
// ---------------------------------------------------------------------------
describe('validateAgentType / validateSkillName', () => {
  it('accepts a well-formed identifier', () => {
    assert.equal(validateAgentType('test-runner'), null);
    assert.equal(validateAgentType('abc'), null);
    assert.equal(validateAgentType('a1b'), null);
    assert.equal(validateSkillName('my-skill'), null);
  });

  it('requires a non-empty value', () => {
    assert.equal(validateAgentType(''), 'Agent type is required');
    assert.equal(validateSkillName(''), 'Skill name is required');
  });

  it('rejects an invalid charset / bad boundaries before checking length', () => {
    assert.match(validateAgentType('-abc')!, /start and end with alphanumeric/);
    assert.match(validateAgentType('abc-')!, /start and end with alphanumeric/);
    assert.match(validateAgentType('a b')!, /only letters, numbers, and hyphens/);
    assert.match(validateAgentType('under_score')!, /only letters, numbers, and hyphens/);
    // a single char fails the start-AND-end regex, matching the reference's ordering
    assert.match(validateAgentType('a')!, /start and end with alphanumeric/);
  });

  it('enforces the 3–50 character bounds', () => {
    assert.equal(validateAgentType('ab'), 'Agent type must be at least 3 characters long');
    assert.equal(
      validateAgentType('a' + 'b'.repeat(49) + 'c'),
      'Agent type must be less than 50 characters',
    );
    assert.equal(validateSkillName('xy'), 'Skill name must be at least 3 characters long');
  });
});

// ---------------------------------------------------------------------------
// F2 — parseSystemPromptMode + SYSTEM_PROMPT_MODES (shared canonical list)
// ---------------------------------------------------------------------------
describe('parseSystemPromptMode / SYSTEM_PROMPT_MODES', () => {
  it('exposes the canonical mode list and shares it with the task tool', () => {
    assert.deepEqual([...SYSTEM_PROMPT_MODES], ['default', 'replace', 'append']);
    // the task-tool re-export resolves to the SAME single source
    assert.deepEqual([...TASK_SYSTEM_PROMPT_MODES], [...SYSTEM_PROMPT_MODES]);
    assert.ok(SYSTEM_PROMPT_MODES === TASK_SYSTEM_PROMPT_MODES);
  });

  it('normalizes known modes case-insensitively', () => {
    assert.equal(parseSystemPromptMode('append'), 'append');
    assert.equal(parseSystemPromptMode('  Replace '), 'replace');
    assert.equal(parseSystemPromptMode('DEFAULT'), 'default');
  });

  it('returns undefined for unknown / non-string values', () => {
    assert.equal(parseSystemPromptMode('prepend'), undefined);
    assert.equal(parseSystemPromptMode(''), undefined);
    assert.equal(parseSystemPromptMode(42), undefined);
    assert.equal(parseSystemPromptMode(undefined), undefined);
  });
});

// ---------------------------------------------------------------------------
// validateAgentDefinition wiring for the new fields
// ---------------------------------------------------------------------------
describe('validateAgentDefinition — name + systemPromptMode', () => {
  const base: Partial<AgentDefinition> = { name: 'reviewer', systemPrompt: 'You review code.' };

  it('stays warning-free for a minimal valid definition', () => {
    const r = validateAgentDefinition(base);
    assert.equal(r.valid, true);
    assert.equal(r.warnings.length, 0);
  });

  it('warns (non-fatally) on a malformed name but still loads', () => {
    const r = validateAgentDefinition({ ...base, name: 'bad name!' });
    assert.equal(r.valid, true);
    assert.ok(r.warnings.some((w) => /only letters, numbers, and hyphens/.test(w)));
  });

  it('warns on an unknown systemPromptMode but stays valid', () => {
    const r = validateAgentDefinition({ ...base, systemPromptMode: 'prepend' as never });
    assert.equal(r.valid, true);
    assert.ok(r.warnings.some((w) => w.includes('systemPromptMode')));
  });

  it('accepts a valid systemPromptMode with no warning', () => {
    assert.equal(validateAgentDefinition({ ...base, systemPromptMode: 'append' }).warnings.length, 0);
  });
});

// ---------------------------------------------------------------------------
// Integration through the real task tool (engine factory stubbed) — F2 + F5
// ---------------------------------------------------------------------------
function skillManagerWith(agent: Partial<AgentDefinition>) {
  return {
    getAgent: (t: string) => (t === 'custom' ? { name: 'custom', ...agent } : undefined),
    getSkill: () => null,
    listAgents: () => [{ name: 'custom' }],
  };
}

async function spawn(agent: Partial<AgentDefinition>): Promise<{
  isError?: boolean;
  output: string;
  systemPrompt?: string;
  toolNames: string[];
}> {
  let systemPrompt: string | undefined;
  let toolNames: string[] = [];
  async function* fakeSubmit() {
    yield { type: 'text_delta' as const, text: 'ok' };
  }
  const result = await taskTool.execute(
    { prompt: 'do it', subagent_type: 'custom' },
    {
      cwd: process.cwd(),
      abortSignal: new AbortController().signal,
      _providerConfig: { baseUrl: 'http://localhost', apiKey: 'k', model: 'm' },
      _skillManager: skillManagerWith(agent) as unknown as ToolContext['_skillManager'],
      _engineFactory: (cfg: EngineConfig) => {
        systemPrompt = cfg.systemPrompt;
        toolNames = (cfg.tools as ToolDefinition[]).map((t) => t.name);
        return { submit: fakeSubmit };
      },
    } as unknown as ToolContext,
  );
  return { isError: result.isError, output: result.output, systemPrompt, toolNames };
}

describe('task tool — AgentDefinition.systemPromptMode (F2)', () => {
  beforeEach(() => resetAgentRegistry());

  const baseSnippet = 'You are a focused sub-agent';

  it("append layers the agent prompt AFTER the base subagent stance", async () => {
    const { isError, systemPrompt } = await spawn({
      systemPrompt: 'CUSTOM-AGENT-IDENTITY',
      systemPromptMode: 'append',
    });
    assert.equal(isError, undefined);
    assert.ok(systemPrompt!.includes(baseSnippet), 'base stance present');
    assert.ok(systemPrompt!.includes('CUSTOM-AGENT-IDENTITY'), 'agent prompt present');
    // base must come first, agent prompt after
    assert.ok(systemPrompt!.indexOf(baseSnippet) < systemPrompt!.indexOf('CUSTOM-AGENT-IDENTITY'));
    assert.ok(systemPrompt!.startsWith(SUBAGENT_SYSTEM_PROMPT));
  });

  it('replace / default / absent make the agent prompt authoritative (no base stance)', async () => {
    for (const mode of ['replace', 'default', undefined] as const) {
      const { systemPrompt } = await spawn({
        systemPrompt: 'CUSTOM-AGENT-IDENTITY',
        ...(mode ? { systemPromptMode: mode } : {}),
      });
      assert.equal(systemPrompt, 'CUSTOM-AGENT-IDENTITY', `mode=${mode}`);
      assert.ok(!systemPrompt!.includes(baseSnippet), `mode=${mode} has no base stance`);
    }
  });
});

describe('task tool — agent tools undefined vs empty semantics (F5)', () => {
  beforeEach(() => resetAgentRegistry());

  it('tools === undefined grants the full agent tool set (no error)', async () => {
    const { isError, toolNames } = await spawn({ systemPrompt: 'X' });
    assert.equal(isError, undefined);
    assert.ok(toolNames.length > 0);
    assert.ok(toolNames.includes('file_read'));
  });

  it('tools === [] is an EXPLICIT no-tools set → rejected by the zero-tool guard', async () => {
    const { isError, output } = await spawn({ systemPrompt: 'X', tools: [] });
    assert.ok(isError, 'an empty allowlist must not silently grant all tools');
    assert.match(output, /empty|unable to act/i);
  });
});
