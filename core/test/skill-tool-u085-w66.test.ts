import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  skillTool,
  filterToolsForSkill,
  toolCategory,
  TOOL_CATEGORY_TOKENS,
  skillBaseDir,
  withSkillBaseDir,
  skillActivationByline,
  SAFE_SKILL_PROPERTIES,
} from '../src/tools/skill';
import type { SkillDefinition, AgentDefinition } from '../src/skills';
import type { ToolContext, ToolDefinition, EngineEvent } from '../src/types';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: '/tmp/u085-w66',
    abortSignal: new AbortController().signal,
    _engineAddSystemReminder: () => {},
    _engineSetActiveSkill: () => {},
    ...overrides,
  } as unknown as ToolContext;
}

function manager(skill: SkillDefinition, agents: AgentDefinition[] = []) {
  return {
    getSkill: (n: string) => (n === skill.name ? skill : null),
    getSkillContent: (n: string) => (n === skill.name ? 'do the thing' : null),
    listSkills: () => [skill],
    getAgent: (n: string) => agents.find(a => a.name === n) ?? null,
    listAgents: () => agents,
  } as any;
}

function tool(name: string, opts: Partial<ToolDefinition> = {}): ToolDefinition {
  return {
    name,
    description: name,
    inputSchema: { type: 'object', properties: {}, required: [] },
    isReadOnly: false,
    execute: async () => ({ output: '' }),
    ...opts,
  } as ToolDefinition;
}

const ALL_TOOLS: ToolDefinition[] = [
  tool('file_read', { isReadOnly: true }),
  tool('grep', { isReadOnly: true }),
  tool('file_edit'),
  tool('file_write'),
  tool('bash'),
  tool('skill'),
  tool('mcp__ops__deploy', { isMcp: true }),
  tool('task'),
];

// ===========================================================================
// GAP U085 (low): tool-category taxonomy (read-only/edit/execution/mcp/other)
// ===========================================================================

describe('toolCategory — five-way taxonomy (parity with getToolBuckets)', () => {
  it('classifies each tool into its bucket', () => {
    assert.equal(toolCategory(tool('file_read', { isReadOnly: true })), 'read-only');
    assert.equal(toolCategory(tool('file_edit')), 'edit');
    assert.equal(toolCategory(tool('file_write')), 'edit');
    assert.equal(toolCategory(tool('bash')), 'execution');
    assert.equal(toolCategory(tool('mcp__x__y', { isMcp: true })), 'mcp');
    assert.equal(toolCategory(tool('task')), 'other');
  });

  it('mcp flag wins even for a read-only mcp tool', () => {
    assert.equal(toolCategory(tool('mcp__x__read', { isMcp: true, isReadOnly: true })), 'mcp');
  });

  it('exposes the accepted category tokens', () => {
    for (const t of ['read-only', 'edit', 'execution', 'mcp', 'other']) {
      assert.ok(TOOL_CATEGORY_TOKENS.has(t), `${t} should be a category token`);
    }
  });
});

describe('filterToolsForSkill — allow/deny by tool CLASS', () => {
  it('allows a whole class by category token', () => {
    const skill = { name: 's', description: '', content: '', filePath: '<test>', allowedTools: ['read-only'] } as SkillDefinition;
    const names = filterToolsForSkill(ALL_TOOLS, skill).map(t => t.name).sort();
    assert.deepEqual(names, ['file_read', 'grep']);
  });

  it('combines a category token with an explicit name', () => {
    const skill = { name: 's', description: '', content: '', filePath: '<test>', allowedTools: ['edit', 'bash'] } as SkillDefinition;
    const names = filterToolsForSkill(ALL_TOOLS, skill).map(t => t.name).sort();
    assert.deepEqual(names, ['bash', 'file_edit', 'file_write']);
  });

  it('denies a whole class by category token (case-insensitive)', () => {
    const skill = { name: 's', description: '', content: '', filePath: '<test>', disallowedTools: ['EXECUTION', 'MCP'] } as SkillDefinition;
    const names = filterToolsForSkill(ALL_TOOLS, skill).map(t => t.name);
    assert.ok(!names.includes('bash'));
    assert.ok(!names.includes('mcp__ops__deploy'));
    assert.ok(names.includes('file_edit'));
  });

  it('a category token never emits an unknown-tool warning', () => {
    const warnings: string[] = [];
    const skill = { name: 's', description: '', content: '', filePath: '<test>', allowedTools: ['read-only'] } as SkillDefinition;
    filterToolsForSkill(ALL_TOOLS, skill, w => warnings.push(w.kind));
    assert.ok(!warnings.includes('unknown-tool'));
  });
});

// ===========================================================================
// GAP U085 (low): automatic "Base directory for this skill" prompt prefix
// ===========================================================================

describe('skillBaseDir / withSkillBaseDir', () => {
  it('derives the directory of a disk-based skill', () => {
    const skill = { name: 's', description: '', content: '', filePath: '/home/u/.ami/skills/pdf/SKILL.md' } as SkillDefinition;
    assert.equal(skillBaseDir(skill), '/home/u/.ami/skills/pdf');
  });

  it('returns null for built-in / in-memory skills', () => {
    assert.equal(skillBaseDir({ name: 's', description: '', content: '', filePath: '<builtin>' } as SkillDefinition), null);
    assert.equal(skillBaseDir({ name: 's', description: '', content: '', filePath: '<test>' } as SkillDefinition), null);
    assert.equal(skillBaseDir(null), null);
  });

  it('prepends the base-dir pointer to disk-skill content, idempotently', () => {
    const skill = { name: 's', description: '', content: '', filePath: '/skills/pdf/SKILL.md' } as SkillDefinition;
    const once = withSkillBaseDir('BODY', skill);
    assert.match(once, /^Base directory for this skill: \/skills\/pdf\n\nBODY$/);
    assert.equal(withSkillBaseDir(once, skill), once, 'must not double-prepend');
  });

  it('is a no-op for a built-in skill', () => {
    const skill = { name: 's', description: '', content: '', filePath: '<builtin>' } as SkillDefinition;
    assert.equal(withSkillBaseDir('BODY', skill), 'BODY');
  });

  it('injects the base dir into an inline skill reminder', async () => {
    let reminder = '';
    const skill = { name: 'disk', description: '', content: '', filePath: '/skills/disk/SKILL.md', context: 'inline' } as SkillDefinition;
    await skillTool.execute(
      { skill: 'disk' },
      ctx({ _skillManager: manager(skill), _engineAddSystemReminder: (t: string) => { reminder = t; } }),
    );
    assert.match(reminder, /Base directory for this skill: \/skills\/disk/);
  });
});

// ===========================================================================
// GAP U085 (low): inline activation byline reports tool count + model
// ===========================================================================

describe('skillActivationByline', () => {
  it('joins base + tool count + model with a middot', () => {
    assert.equal(
      skillActivationByline('activated', 3, 'claude-x'),
      'activated · 3 tools allowed · model claude-x',
    );
  });
  it('singularizes one tool and omits absent parts', () => {
    assert.equal(skillActivationByline('activated', 1, undefined), 'activated · 1 tool allowed');
    assert.equal(skillActivationByline('activated', undefined, undefined), 'activated');
  });
});

describe('skillTool.execute — inline byline surfaces sandboxing', () => {
  it('reports restricted tool count + model for a restricted inline skill', async () => {
    const skill = {
      name: 'scoped', description: '', content: '', filePath: '<test>', context: 'inline',
      allowedTools: ['read-only'], model: 'claude-mini',
    } as SkillDefinition;
    const r = await skillTool.execute({ skill: 'scoped' }, ctx({ _skillManager: manager(skill) }));
    assert.ok(!r.isError);
    assert.match(r.output, /activated inline/);
    assert.match(r.output, /tools? allowed/);
    assert.match(r.output, /model claude-mini/);
    assert.equal(typeof (r.metadata as any).toolCount, 'number');
  });

  it('keeps the plain message for an unrestricted inline skill', async () => {
    const skill = { name: 'plain', description: '', content: '', filePath: '<test>', context: 'inline' } as SkillDefinition;
    const r = await skillTool.execute({ skill: 'plain' }, ctx({ _skillManager: manager(skill) }));
    assert.equal(r.output, 'Skill "plain" activated inline. Following its instructions.');
    assert.equal((r.metadata as any).toolCount, undefined);
  });
});

// ===========================================================================
// GAP U085 (medium): forked skill targets a NAMED agent type
// ===========================================================================

describe('skillTool.execute — fork bound to a named agent', () => {
  const agent: AgentDefinition = {
    name: 'reviewer',
    description: 'code reviewer',
    systemPrompt: 'You are a meticulous reviewer.',
    filePath: '<test>',
    model: 'agent-model',
    maxTurns: 7,
  };

  it('binds the sub-engine to the agent system prompt + model + maxTurns', async () => {
    let captured: any;
    const skill = { name: 'rev', description: '', content: '', filePath: '<test>', context: 'fork', agent: 'reviewer' } as SkillDefinition;
    const r = await skillTool.execute(
      { skill: 'rev' },
      ctx({
        _skillManager: manager(skill, [agent]),
        _providerConfig: { baseUrl: 'http://t', apiKey: 'k', model: 'parent-model' },
        _engineFactory: (cfg: any) => {
          captured = cfg;
          return { async *submit() { yield { type: 'text_delta', text: 'reviewed' } as EngineEvent; } } as any;
        },
      }),
    );
    assert.ok(!r.isError);
    assert.equal(captured.systemPrompt, 'You are a meticulous reviewer.');
    assert.equal(captured.maxTurns, 7);
    assert.equal(captured.provider.model, 'agent-model', 'agent model used as fallback');
    assert.match(r.output, /agent reviewer/);
    assert.equal((r.metadata as any).agent, 'reviewer');
  });

  it("the skill's own model override wins over the agent model", async () => {
    let captured: any;
    const skill = { name: 'rev', description: '', content: '', filePath: '<test>', context: 'fork', agent: 'reviewer', model: 'skill-model' } as SkillDefinition;
    await skillTool.execute(
      { skill: 'rev' },
      ctx({
        _skillManager: manager(skill, [agent]),
        _providerConfig: { baseUrl: 'http://t', apiKey: 'k', model: 'parent-model' },
        _engineFactory: (cfg: any) => {
          captured = cfg;
          return { async *submit() { yield { type: 'text_delta', text: 'x' } as EngineEvent; } } as any;
        },
      }),
    );
    assert.equal(captured.provider.model, 'skill-model');
  });

  it('errors on an unknown agent name instead of silently degrading', async () => {
    const skill = { name: 'rev', description: '', content: '', filePath: '<test>', context: 'fork', agent: 'ghost' } as SkillDefinition;
    const r = await skillTool.execute(
      { skill: 'rev' },
      ctx({
        _skillManager: manager(skill, [agent]),
        _providerConfig: { baseUrl: 'http://t', apiKey: 'k', model: 'm' },
        _engineFactory: () => ({ async *submit() {} }) as any,
      }),
    );
    assert.ok(r.isError);
    assert.match(r.output, /targets agent "ghost", which is not registered/);
    assert.match(r.output, /Available agents: reviewer/);
  });

  it('agent is a reviewed-safe property (auto-run eligible)', () => {
    assert.ok(SAFE_SKILL_PROPERTIES.has('agent'));
  });
});
