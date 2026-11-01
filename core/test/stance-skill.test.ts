import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { isStanceSkill, type SkillDefinition } from '../src/skills';
import { skillTool } from '../src/tools/skill';
import type { ToolContext } from '../src/types';

function makeSkill(partial: Partial<SkillDefinition>): SkillDefinition {
  return {
    name: 'x',
    description: 'd',
    content: 'body',
    filePath: '<test>',
    ...partial,
  };
}

describe('isStanceSkill', () => {
  it('is true when the skill declares auto-allow patterns', () => {
    assert.equal(isStanceSkill(makeSkill({ autoAllowPatterns: ['nmap*'] })), true);
  });

  it('is true when the skill declares MCP servers', () => {
    assert.equal(
      isStanceSkill(makeSkill({ mcpServers: { kali: { command: '' } as any } })),
      true,
    );
  });

  it('is true when the skill declares MCP auto-allow patterns', () => {
    assert.equal(isStanceSkill(makeSkill({ mcpAutoAllowPatterns: ['mcp__kali__*'] })), true);
  });

  it('is true when the skill sets a default thinking level', () => {
    assert.equal(isStanceSkill(makeSkill({ defaultThinkingLevel: 'high' as any })), true);
  });

  it('is false for a plain skill with no stance capabilities', () => {
    assert.equal(isStanceSkill(makeSkill({})), false);
  });

  it('is false for an empty autoAllowPatterns / empty mcpServers', () => {
    assert.equal(isStanceSkill(makeSkill({ autoAllowPatterns: [], mcpServers: {} })), false);
  });

  it('is false for a disable-model-invocation workflow doc (no capabilities)', () => {
    assert.equal(isStanceSkill(makeSkill({ disableModelInvocation: true })), false);
  });
});

describe('skill tool — stance skills are never run as one-shot', () => {
  function ctxWith(skill: SkillDefinition): ToolContext {
    return {
      cwd: '/tmp',
      abortSignal: new AbortController().signal,
      _skillManager: {
        getSkill: (n: string) => (n === skill.name ? skill : null),
        getSkillContent: () => skill.content,
        listSkills: () => [skill],
        listAgents: () => [],
        getAgent: () => null,
      },
    } as unknown as ToolContext;
  }

  it('returns an informational (non-error) message for a stance skill, without spawning a sub-engine', async () => {
    const stance = makeSkill({ name: 'pentest', disableModelInvocation: true, mcpServers: { kali: { command: '' } as any } });
    const res = await skillTool.execute({ skill: 'pentest' }, ctxWith(stance));
    assert.equal(res.isError, false);
    assert.match(res.output, /stance skill/i);
    assert.match(res.output, /--skill pentest/);
  });

  it('still refuses a disable-model-invocation NON-stance skill', async () => {
    const workflow = makeSkill({ name: 'review-changes-workflow', disableModelInvocation: true });
    const res = await skillTool.execute({ skill: 'review-changes-workflow' }, ctxWith(workflow));
    assert.equal(res.isError, true);
    assert.match(res.output, /disableModelInvocation/);
  });
});
