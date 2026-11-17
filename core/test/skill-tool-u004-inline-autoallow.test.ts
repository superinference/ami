import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { skillTool } from '../src/tools/skill';
import type { ToolContext } from '../src/types';

// ===========================================================================
// GAP U004 (skill.ts): an INLINE skill contributes its frontmatter
// `allowed-tools` scoped command patterns to the SESSION permission context via
// the `_engineSetActiveSkill` hook — the same canonical scopedToolAllowRules
// derivation the forked-skill path applies to the sub-engine, now applied to the
// inline (same-session) path too. Decoupled from isStanceSkill. Parity with
// Claude Code's security-review command, which injects a command's allowed-tools
// into alwaysAllowRules.command for the duration of that command.
// ===========================================================================

type Captured = { name: string | undefined; rules: any };

function manager(skill: any) {
  return {
    getSkill: (n: string) => (n === skill.name ? skill : null),
    getSkillContent: () => skill.content ?? 'do the thing',
    listSkills: () => [skill],
    listAgents: () => [],
    getAgent: () => null,
  } as any;
}

function ctx(skill: any, capture: Captured): ToolContext {
  return {
    cwd: '/tmp',
    abortSignal: new AbortController().signal,
    _skillManager: manager(skill),
    _engineAddSystemReminder: () => {},
    _engineSetActiveSkill: (name: string | undefined, rules?: any) => {
      capture.name = name;
      capture.rules = rules;
    },
  } as unknown as ToolContext;
}

describe('skill tool — inline activation forwards scoped allow rules to the engine', () => {
  it('forwards the skill\'s scoped Bash command patterns as allow rules', async () => {
    const skill = {
      name: 'reviewer', context: 'inline', content: 'review',
      allowedTools: ['Bash(git status *)', 'Read(~/**)'],
      filePath: '/skills/reviewer/SKILL.md',
    };
    const cap: Captured = { name: undefined, rules: undefined };
    const res = await skillTool.execute({ skill: 'reviewer' }, ctx(skill, cap));

    assert.equal(res.isError, undefined);
    assert.equal(cap.name, 'reviewer');
    assert.deepEqual(cap.rules, [
      { tool: 'bash', pattern: 'git status *', action: 'allow' },
      { tool: 'file_read', pattern: '~/**', action: 'allow' },
    ]);
  });

  it('forwards an EMPTY rule set for an inline skill with only name-level allowed-tools', async () => {
    const skill = {
      name: 'plain', context: 'inline', content: 'z',
      allowedTools: ['Bash', 'Read'], // no scope → scopedToolAllowRules yields []
      filePath: '/skills/plain/SKILL.md',
    };
    const cap: Captured = { name: 'sentinel', rules: 'sentinel' };
    await skillTool.execute({ skill: 'plain' }, ctx(skill, cap));

    assert.equal(cap.name, 'plain');
    assert.deepEqual(cap.rules, []);
  });

  it('forwards [] for an inline skill that declares no allowed-tools at all', async () => {
    const skill = { name: 'bare', context: 'inline', content: 'z', filePath: '/skills/bare/SKILL.md' };
    const cap: Captured = { name: 'sentinel', rules: 'sentinel' };
    await skillTool.execute({ skill: 'bare' }, ctx(skill, cap));

    assert.equal(cap.name, 'bare');
    assert.deepEqual(cap.rules, []);
  });

  it('clear:true deactivates the inline skill (name=undefined) so the engine removes the grant', async () => {
    const cap: Captured = { name: 'still-active', rules: [{ tool: 'bash', pattern: 'x', action: 'allow' }] };
    const res = await skillTool.execute(
      { clear: true },
      {
        cwd: '/tmp',
        abortSignal: new AbortController().signal,
        _engineSetActiveSkill: (name: string | undefined) => { cap.name = name; },
      } as unknown as ToolContext,
    );
    assert.match(res.output, /deactivated/i);
    assert.equal(cap.name, undefined);
  });
});
