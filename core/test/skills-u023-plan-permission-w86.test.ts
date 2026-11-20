// U023-w86 — two additive parity gaps on the core skills building blocks:
//
//  * `plan` agent/skill permission mode (parity with the reference's `plan` agent
//    permission mode): AGENT_PERMISSION_MODES + the AgentDefinition/SkillDefinition
//    `permissionMode` type must accept `'plan'` so an agent/skill frontmatter
//    declaring the read-only plan stance validates instead of warning. The engine's
//    PERMISSION_MODES already accepts `plan` and task.ts's PERMISSION_MODE_RANK ranks
//    it below `ask`, so a plan sub-agent reads/searches but is denied mutations.
//
//  * resolveSkillPrompt GRACEFUL DEGRADATION (parity with the reference's bundled
//    claude-api skill, whose dynamic prompt builder wraps its project-environment
//    detection in a try/catch and falls back to the static reference on failure): a
//    getPromptForCommand builder that THROWS must degrade to the skill's static
//    content rather than rejecting the content path.

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  AGENT_PERMISSION_MODES,
  validateAgentDefinition,
  resolveSkillPrompt,
  type AgentDefinition,
  type SkillDefinition,
} from '../src/skills';

describe('U023-w86 — plan permission mode', () => {
  it('AGENT_PERMISSION_MODES includes the read-only plan mode', () => {
    assert.deepEqual([...AGENT_PERMISSION_MODES], ['auto-allow', 'ask', 'deny-all', 'plan']);
  });

  it('validateAgentDefinition accepts permissionMode "plan" with no warning', () => {
    const r = validateAgentDefinition({
      name: 'planner',
      systemPrompt: 'You plan the work without touching files.',
      permissionMode: 'plan',
    });
    assert.equal(r.valid, true);
    assert.equal(r.errors.length, 0);
    assert.ok(
      !r.warnings.some((w) => w.includes('permission mode')),
      `no permission-mode warning expected, got: ${r.warnings.join(' | ')}`,
    );
  });

  it('still warns on an unrecognized permission mode (plan did not widen to everything)', () => {
    const r = validateAgentDefinition({
      name: 'bad',
      systemPrompt: 'body',
      // @ts-expect-error — exercise an invalid frontmatter value at runtime
      permissionMode: 'yolo',
    });
    assert.ok(r.warnings.some((w) => w.includes('Unknown permission mode "yolo"')));
    assert.ok(r.warnings.some((w) => w.includes('plan')));
  });

  it('accepts all four modes on the AgentDefinition type (compile-time + runtime)', () => {
    for (const mode of AGENT_PERMISSION_MODES) {
      const def: Partial<AgentDefinition> = {
        name: 'modetest',
        systemPrompt: 'a sufficiently long system prompt body',
        permissionMode: mode,
      };
      assert.equal(validateAgentDefinition(def).valid, true);
    }
  });
});

describe('U023-w86 — resolveSkillPrompt graceful degradation', () => {
  function mkSkill(over: Partial<SkillDefinition>): SkillDefinition {
    return {
      name: 'demo',
      description: 'a demo skill',
      content: 'static body',
      filePath: '<builtin>',
      ...over,
    };
  }

  it('falls back to static content when a sync builder throws', async () => {
    const skill = mkSkill({
      content: 'FALLBACK',
      getPromptForCommand: () => {
        throw new Error('env detection failed');
      },
    });
    assert.equal(await resolveSkillPrompt(skill, 'args'), 'FALLBACK');
  });

  it('falls back to static content when an async builder rejects', async () => {
    const skill = mkSkill({
      content: 'ASYNC FALLBACK',
      getPromptForCommand: async () => {
        throw new Error('async env detection failed');
      },
    });
    assert.equal(await resolveSkillPrompt(skill), 'ASYNC FALLBACK');
  });

  it('applies template variables to the fallback content', async () => {
    const skill = mkSkill({
      content: 'hello {{who}}',
      templateVars: { who: 'world' },
      getPromptForCommand: () => {
        throw new Error('boom');
      },
    });
    assert.equal(await resolveSkillPrompt(skill), 'hello world');
  });

  it('still prefers a successful builder over the static content (no regression)', async () => {
    const skill = mkSkill({ content: 'STATIC', getPromptForCommand: () => 'DYNAMIC' });
    assert.equal(await resolveSkillPrompt(skill), 'DYNAMIC');
  });
});
