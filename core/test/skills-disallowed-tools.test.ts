import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createDefaultTools } from '../src/tools/index';
import type { SkillDefinition } from '../src/skills';
import type { ToolDefinition } from '../src/types';

/**
 * Helper that replicates the tool-filtering logic from skill.ts.
 * We import and call filterToolsForSkill (to be exported) to test it in isolation.
 */
import { filterToolsForSkill } from '../src/tools/skill';

describe('skill disallowedTools filtering', () => {
  const allTools = createDefaultTools('/tmp').getAll();

  it('filters out a single disallowed tool', () => {
    const skill: Pick<SkillDefinition, 'allowedTools' | 'disallowedTools'> = {
      disallowedTools: ['bash'],
    };
    const result = filterToolsForSkill(allTools, skill as SkillDefinition);
    const names = result.map(t => t.name);
    assert.ok(!names.includes('bash'), 'bash should be filtered out');
    assert.ok(!names.includes('skill'), 'skill should always be filtered out');
    assert.ok(names.includes('file_read'), 'file_read should remain');
    assert.ok(names.includes('grep'), 'grep should remain');
  });

  it('filters out multiple disallowed tools', () => {
    const skill: Pick<SkillDefinition, 'allowedTools' | 'disallowedTools'> = {
      disallowedTools: ['bash', 'file_edit'],
    };
    const result = filterToolsForSkill(allTools, skill as SkillDefinition);
    const names = result.map(t => t.name);
    assert.ok(!names.includes('bash'), 'bash should be filtered out');
    assert.ok(!names.includes('file_edit'), 'file_edit should be filtered out');
    assert.ok(!names.includes('skill'), 'skill should always be filtered out');
    assert.ok(names.includes('file_read'), 'file_read should remain');
  });

  it('does not filter when disallowedTools is undefined', () => {
    const skill: Pick<SkillDefinition, 'allowedTools' | 'disallowedTools'> = {};
    const result = filterToolsForSkill(allTools, skill as SkillDefinition);
    const names = result.map(t => t.name);
    // Default: everything minus 'skill'
    assert.ok(!names.includes('skill'), 'skill should be filtered out');
    assert.ok(names.includes('bash'), 'bash should remain');
    assert.ok(names.includes('file_edit'), 'file_edit should remain');
  });

  it('does not filter when disallowedTools is empty', () => {
    const skill: Pick<SkillDefinition, 'allowedTools' | 'disallowedTools'> = {
      disallowedTools: [],
    };
    const result = filterToolsForSkill(allTools, skill as SkillDefinition);
    const names = result.map(t => t.name);
    assert.ok(!names.includes('skill'), 'skill should be filtered out');
    assert.ok(names.includes('bash'), 'bash should remain');
    assert.ok(names.includes('file_edit'), 'file_edit should remain');
  });

  it('applies disallowedTools AFTER allowedTools whitelist', () => {
    const skill: Pick<SkillDefinition, 'allowedTools' | 'disallowedTools'> = {
      allowedTools: ['bash', 'file_edit', 'grep'],
      disallowedTools: ['bash'],
    };
    const result = filterToolsForSkill(allTools, skill as SkillDefinition);
    const names = result.map(t => t.name);
    assert.deepEqual(names.sort(), ['file_edit', 'grep'].sort(),
      'only file_edit and grep should remain after whitelist then blacklist');
  });

  it('allowedTools only (no disallowedTools) works unchanged', () => {
    const skill: Pick<SkillDefinition, 'allowedTools' | 'disallowedTools'> = {
      allowedTools: ['bash', 'grep'],
    };
    const result = filterToolsForSkill(allTools, skill as SkillDefinition);
    const names = result.map(t => t.name);
    assert.deepEqual(names.sort(), ['bash', 'grep'].sort(),
      'only bash and grep should be in the result');
  });

  it('handles null skill (fallback: all minus skill)', () => {
    const result = filterToolsForSkill(allTools, null as any);
    const names = result.map(t => t.name);
    assert.ok(!names.includes('skill'), 'skill should be filtered out');
    assert.ok(names.includes('bash'), 'bash should remain');
  });

  it('handles undefined skill (fallback: all minus skill)', () => {
    const result = filterToolsForSkill(allTools, undefined as any);
    const names = result.map(t => t.name);
    assert.ok(!names.includes('skill'), 'skill should be filtered out');
    assert.ok(names.includes('bash'), 'bash should remain');
  });
});
