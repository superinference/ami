// U023 (w85) — the skill tool must treat a forked skill's `model: 'inherit'` as a
// sentinel (parity with the reference's loadSkillsDir, which maps frontmatter.model ===
// 'inherit' to "use the parent/session model"), NOT as a concrete model id.
//
// Red-green reproducer: BEFORE the fix the fork path did `if (skill?.model)` with no
// `!== 'inherit'` guard, so a skill declaring `model: inherit` reached
// resolveSkillModelOverride('inherit', <session>) and the sub-engine was launched with
// the BOGUS model id 'inherit' (and never fell through to the agent/session model).
// These tests assert the session/agent model is used instead; they fail on fix-revert.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { skillTool } from '../src/tools/skill';
import type { SkillDefinition, AgentDefinition } from '../src/skills';
import type { ToolContext, EngineEvent } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: '/tmp/u023-model-inherit-w85',
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

describe('skillTool.execute — forked skill model: inherit', () => {
  it('uses the parent/session model (not the literal "inherit") for a model: inherit skill', async () => {
    let captured: any;
    const skill = { name: 'inh', description: '', content: '', filePath: '<test>', context: 'fork', model: 'inherit' } as SkillDefinition;
    const r = await skillTool.execute(
      { skill: 'inh' },
      ctx({
        _skillManager: manager(skill),
        _providerConfig: { baseUrl: 'http://t', apiKey: 'k', model: 'parent-model' },
        _engineFactory: (cfg: any) => {
          captured = cfg;
          return { async *submit() { yield { type: 'text_delta', text: 'ok' } as EngineEvent; } } as any;
        },
      }),
    );
    assert.ok(!r.isError);
    assert.notEqual(captured.provider.model, 'inherit', 'the sentinel must not become the model id');
    assert.equal(captured.provider.model, 'parent-model');
  });

  it('falls through to the bound agent model when a model: inherit skill targets an agent', async () => {
    let captured: any;
    const agent: AgentDefinition = {
      name: 'reviewer', description: 'code reviewer',
      systemPrompt: 'You are a meticulous reviewer.', filePath: '<test>', model: 'agent-model',
    };
    const skill = { name: 'rev', description: '', content: '', filePath: '<test>', context: 'fork', agent: 'reviewer', model: 'inherit' } as SkillDefinition;
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
    assert.notEqual(captured.provider.model, 'inherit');
    assert.equal(captured.provider.model, 'agent-model');
  });

  it('still honors a real model override', async () => {
    let captured: any;
    const skill = { name: 'mdl', description: '', content: '', filePath: '<test>', context: 'fork', model: 'gpt-4o' } as SkillDefinition;
    await skillTool.execute(
      { skill: 'mdl' },
      ctx({
        _skillManager: manager(skill),
        _providerConfig: { baseUrl: 'http://t', apiKey: 'k', model: 'parent-model' },
        _engineFactory: (cfg: any) => {
          captured = cfg;
          return { async *submit() { yield { type: 'text_delta', text: 'x' } as EngineEvent; } } as any;
        },
      }),
    );
    assert.equal(captured.provider.model, 'gpt-4o');
  });
});
