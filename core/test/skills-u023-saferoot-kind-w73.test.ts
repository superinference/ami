import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  SAFE_SKILL_PROPERTIES as CANONICAL_SAFE,
  skillPluginRoot,
  skillKind,
  SKILL_KINDS,
} from '../src/skills';
import type { SkillDefinition } from '../src/skills';
import {
  SAFE_SKILL_PROPERTIES as GATE_SAFE,
  unsafeSkillProperties,
  skillHasOnlySafeProperties,
  skillTool,
} from '../src/tools/skill';
import type { ToolContext } from '../src/types';
import type { PermissionPromptResult } from '../src/permissions';

// ===========================================================================
// U023-w73 — consolidate the diverged SAFE_SKILL_PROPERTIES copies into ONE
// canonical union, add the skillRoot field + resolver and the kind discriminator.
//
// The permission gate in tools/skill.ts used to carry its own smaller copy of
// SAFE_SKILL_PROPERTIES that omitted several reviewed-safe declarative fields
// (shell, templateVars, immediate, skillRoot, files, getPromptForCommand,
// progressMessage). A skill legitimately declaring any of them was wrongly routed
// through the permission prompt. These tests fail (RED) against the pre-consolidation
// gate and pass (GREEN) once skill.ts re-exports the canonical set from skills.ts.
// ===========================================================================

describe('SAFE_SKILL_PROPERTIES — single canonical source of truth', () => {
  it('the skill-tool gate re-exports the SAME set as skills.ts (no divergence)', () => {
    assert.strictEqual(
      GATE_SAFE,
      CANONICAL_SAFE,
      'tools/skill.ts must re-export the canonical set, not keep its own copy',
    );
  });

  it('the canonical set covers the reviewed-safe declarative fields the gate was missing', () => {
    for (const k of [
      'shell',
      'templateVars',
      'immediate',
      'skillRoot',
      'files',
      'getPromptForCommand',
      'progressMessage',
      'kind',
    ]) {
      assert.ok(CANONICAL_SAFE.has(k), `expected ${k} to be reviewed-safe`);
    }
  });

  it('the canonical set still covers the stance/tool-restriction fields the gate carried', () => {
    for (const k of [
      'allowedTools',
      'disallowedTools',
      'permissionMode',
      'autoAllowPatterns',
      'mcpServers',
      'mcpAutoAllowPatterns',
      'mcpToolGuidance',
      'defaultThinkingLevel',
    ]) {
      assert.ok(CANONICAL_SAFE.has(k), `expected ${k} to remain reviewed-safe`);
    }
  });
});

describe('unsafeSkillProperties — safe declarative fields no longer flagged', () => {
  const baseSkill: SkillDefinition = {
    name: 'doc',
    description: 'd',
    content: 'c {{tone}}',
    filePath: '<test>',
  };

  it('treats shell/templateVars/immediate/skillRoot/progressMessage as safe', () => {
    const s = {
      ...baseSkill,
      shell: 'bash',
      templateVars: { tone: 'terse' },
      immediate: true,
      skillRoot: '/plugins/acme',
      progressMessage: 'loading',
    } as unknown as SkillDefinition;
    assert.deepEqual(unsafeSkillProperties(s), []);
    assert.equal(skillHasOnlySafeProperties(s), true);
  });

  it('treats files + getPromptForCommand as safe', () => {
    const s = {
      ...baseSkill,
      files: { 'ref.md': 'hi' },
      getPromptForCommand: (a: string) => a,
    } as unknown as SkillDefinition;
    assert.deepEqual(unsafeSkillProperties(s), []);
  });

  it('still flags a genuinely unreviewed property', () => {
    const s = { ...baseSkill, evilDirective: 'rm -rf /' } as unknown as SkillDefinition;
    assert.deepEqual(unsafeSkillProperties(s), ['evilDirective']);
    assert.equal(skillHasOnlySafeProperties(s), false);
  });
});

describe('skillPluginRoot — CLAUDE_PLUGIN_ROOT base-dir resolution', () => {
  it('returns an explicit skillRoot, trimmed', () => {
    assert.equal(skillPluginRoot({ skillRoot: '  /plugins/acme  ', filePath: '<builtin>' }), '/plugins/acme');
  });

  it('falls back to the directory of a real on-disk filePath', () => {
    assert.equal(skillPluginRoot({ filePath: '/home/u/.ami/skills/deploy/SKILL.md' }), '/home/u/.ami/skills/deploy');
  });

  it('returns undefined for a builtin/in-memory sentinel or a bare filename', () => {
    assert.equal(skillPluginRoot({ filePath: '<builtin>' }), undefined);
    assert.equal(skillPluginRoot({ filePath: 'SKILL.md' }), undefined);
    assert.equal(skillPluginRoot({ skillRoot: '   ', filePath: '<builtin>' }), undefined);
  });
});

describe('skillKind — workflow discriminator', () => {
  it('returns the declared kind, else undefined', () => {
    assert.equal(skillKind({ kind: 'workflow' }), 'workflow');
    assert.equal(skillKind({}), undefined);
  });

  it('SKILL_KINDS lists the accepted tokens', () => {
    assert.deepEqual([...SKILL_KINDS], ['workflow']);
  });
});

// ---------------------------------------------------------------------------
// End-to-end: the skill tool's safe-property gate must NOT prompt for a skill
// that uses only the now-canonical reviewed-safe declarative fields.
// ---------------------------------------------------------------------------

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: '/tmp/u023-w73',
    abortSignal: new AbortController().signal,
    _engineAddSystemReminder: () => {},
    _engineSetActiveSkill: () => {},
    ...overrides,
  } as unknown as ToolContext;
}

function manager(skill: SkillDefinition) {
  return {
    getSkill: (n: string) => (n === skill.name ? skill : null),
    getSkillContent: (n: string) => (n === skill.name ? skill.content : null),
    listSkills: () => [skill],
  } as unknown as SkillManagerLikeShim;
}
type SkillManagerLikeShim = ToolContext['_skillManager'];

describe('skillTool.execute — a skill using only safe declarative fields never prompts', () => {
  it('activates inline without a permission prompt even with NO handler available', async () => {
    const safeSkill = {
      name: 'guide',
      description: 'a workflow doc',
      content: 'follow the steps',
      filePath: '<test>',
      context: 'inline',
      kind: 'workflow',
      shell: 'bash',
      immediate: true,
      templateVars: { x: '1' },
      progressMessage: 'loading',
    } as unknown as SkillDefinition;

    const calls: string[] = [];
    const r = await skillTool.execute(
      { skill: 'guide' },
      ctx({
        _skillManager: manager(safeSkill),
        _permissionPromptHandler: {
          showPrompt: async (tool: string, summary: string) => {
            calls.push(`${tool}:${summary}`);
            return { action: 'deny' } as PermissionPromptResult;
          },
        } as unknown as ToolContext['_permissionPromptHandler'],
      }),
    );
    assert.ok(!r.isError, `expected safe skill to run, got: ${r.output}`);
    assert.match(r.output, /activated inline/);
    assert.equal(calls.length, 0, 'a skill with only reviewed-safe fields must not prompt');
  });
});
