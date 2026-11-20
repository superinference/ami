import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  isSkillImmediate,
  FRONTMATTER_SHELLS,
  parseShellFrontmatter,
  DEFAULT_SHELL,
  shellForExecution,
  formatToolNameList,
  progressiveArgumentHint,
  applySkillTemplateVariables,
  resolveSkillPrompt,
  isBuiltInAgent,
  validateAgentDefinition,
  SAFE_SKILL_PROPERTIES,
  skillHasOnlySafeProperties,
  type SkillDefinition,
  type AgentDefinition,
  type FrontmatterShell,
} from '../src/skills';

// ---------------------------------------------------------------------------
// U023-w70 — additive skill/agent config knobs + display formatters that mirror
// Claude Code building blocks AMI's core skills library was missing.
// ---------------------------------------------------------------------------

describe('isSkillImmediate (parity: the reference command `immediate`)', () => {
  it('is true only for an explicit immediate=true flag', () => {
    assert.equal(isSkillImmediate({ immediate: true }), true);
    assert.equal(isSkillImmediate({ immediate: false }), false);
    assert.equal(isSkillImmediate({}), false);
    // truthy-but-not-true values do not count (no accidental coercion)
    assert.equal(isSkillImmediate({ immediate: 1 as unknown as boolean }), false);
  });

  it('`immediate` is a declared field + a known safe property', () => {
    const skill: Partial<SkillDefinition> = { immediate: true };
    assert.equal(skill.immediate, true);
    assert.ok(SAFE_SKILL_PROPERTIES.has('immediate'));
    assert.equal(skillHasOnlySafeProperties({ name: 'x', immediate: true }), true);
  });
});

describe('parseShellFrontmatter (parity: the reference parseShellFrontmatter)', () => {
  it('accepts the known shells case-insensitively', () => {
    assert.equal(parseShellFrontmatter('bash'), 'bash');
    assert.equal(parseShellFrontmatter('PowerShell'), 'powershell');
    assert.equal(parseShellFrontmatter('  BASH  '), 'bash');
    assert.deepEqual([...FRONTMATTER_SHELLS], ['bash', 'powershell']);
  });

  it('returns undefined for absent/empty/unknown values (caller defaults to bash)', () => {
    assert.equal(parseShellFrontmatter(undefined), undefined);
    assert.equal(parseShellFrontmatter(null), undefined);
    assert.equal(parseShellFrontmatter(''), undefined);
    assert.equal(parseShellFrontmatter('   '), undefined);
    assert.equal(parseShellFrontmatter('zsh'), undefined);
    assert.equal(parseShellFrontmatter(42), undefined);
  });

  it('shellForExecution falls back to bash when unset', () => {
    assert.equal(DEFAULT_SHELL, 'bash');
    assert.equal(shellForExecution(undefined), 'bash');
    assert.equal(shellForExecution('powershell'), 'powershell');
  });

  it('`shell` is a declared SkillDefinition field + a safe property', () => {
    const skill: Partial<SkillDefinition> = { shell: 'powershell' as FrontmatterShell };
    assert.equal(skill.shell, 'powershell');
    assert.ok(SAFE_SKILL_PROPERTIES.has('shell'));
    assert.equal(skillHasOnlySafeProperties({ name: 'x', shell: 'bash' }), true);
  });
});

describe('formatToolNameList (parity: the reference getToolsDisplay 5-branch)', () => {
  it('undefined means "All tools"', () => {
    assert.equal(formatToolNameList(undefined), 'All tools');
  });
  it('empty list means "None"', () => {
    assert.equal(formatToolNameList([]), 'None');
  });
  it('single tool returns its name (or None when blank)', () => {
    assert.equal(formatToolNameList(['bash']), 'bash');
    assert.equal(formatToolNameList(['']), 'None');
  });
  it('a pair joins with " and "', () => {
    assert.equal(formatToolNameList(['bash', 'grep']), 'bash and grep');
  });
  it('three or more use an Oxford comma', () => {
    assert.equal(formatToolNameList(['a', 'b', 'c']), 'a, b, and c');
    assert.equal(formatToolNameList(['a', 'b', 'c', 'd']), 'a, b, c, and d');
  });
});

describe('progressiveArgumentHint (parity: the reference generateProgressiveArgumentHint)', () => {
  it('shows only the remaining unfilled argument names', () => {
    assert.equal(progressiveArgumentHint(['a', 'b', 'c'], []), '[a] [b] [c]');
    assert.equal(progressiveArgumentHint(['a', 'b', 'c'], ['x']), '[b] [c]');
    assert.equal(progressiveArgumentHint(['a', 'b', 'c'], ['x', 'y']), '[c]');
  });
  it('returns undefined once every arg is filled (or over-filled)', () => {
    assert.equal(progressiveArgumentHint(['a', 'b'], ['x', 'y']), undefined);
    assert.equal(progressiveArgumentHint(['a'], ['x', 'y']), undefined);
    assert.equal(progressiveArgumentHint([], []), undefined);
  });
});

describe('applySkillTemplateVariables (parity: the reference {{key}} SKILL_MODEL_VARS)', () => {
  it('substitutes known {{key}} tokens and leaves unknown ones intact', () => {
    const out = applySkillTemplateVariables(
      'Use {{model}} (ver {{version}}) and keep {{unknown}}.',
      { model: 'opus', version: '4.8' },
    );
    assert.equal(out, 'Use opus (ver 4.8) and keep {{unknown}}.');
  });
  it('replaces every occurrence of a token (global)', () => {
    assert.equal(
      applySkillTemplateVariables('{{m}}/{{m}}', { m: 'x' }),
      'x/x',
    );
  });
  it('keeps an empty-string mapping (distinct from absent)', () => {
    assert.equal(applySkillTemplateVariables('a{{k}}b', { k: '' }), 'ab');
  });
  it('no vars map leaves content untouched', () => {
    assert.equal(applySkillTemplateVariables('a{{k}}b', undefined), 'a{{k}}b');
  });
  it('`templateVars` is a known safe property', () => {
    assert.ok(SAFE_SKILL_PROPERTIES.has('templateVars'));
    assert.equal(
      skillHasOnlySafeProperties({ name: 'x', templateVars: { a: '1' } }),
      true,
    );
  });

  it('is wired into resolveSkillPrompt — static content path', async () => {
    const out = await resolveSkillPrompt(
      { content: 'Call {{model}}.', templateVars: { model: 'opus' } },
      '',
    );
    assert.equal(out, 'Call opus.');
  });

  it('is wired into resolveSkillPrompt — computed builder path', async () => {
    const out = await resolveSkillPrompt(
      {
        content: 'ignored',
        templateVars: { model: 'opus' },
        getPromptForCommand: (args: string) => `args=${args} model={{model}}`,
      },
      'hi',
    );
    assert.equal(out, 'args=hi model=opus');
  });
});

describe('isBuiltInAgent (parity: the reference isBuiltIn source discriminator)', () => {
  it('is true only when source === "built-in"', () => {
    assert.equal(isBuiltInAgent({ source: 'built-in' }), true);
    assert.equal(isBuiltInAgent({ source: 'plugin' }), false);
    assert.equal(isBuiltInAgent({ source: 'user' }), false);
    assert.equal(isBuiltInAgent({ source: 'project' }), false);
    assert.equal(isBuiltInAgent({}), false);
  });
});

describe('validateAgentDefinition — new knobs (omitClaudeMd/planModeRequired/descriptionColor)', () => {
  const base: Partial<AgentDefinition> = { name: 'my-agent', systemPrompt: 'do things' };

  it('accepts valid boolean omitClaudeMd / planModeRequired + a valid descriptionColor', () => {
    const r = validateAgentDefinition({
      ...base,
      omitClaudeMd: true,
      planModeRequired: false,
      descriptionColor: 'cyan',
    });
    assert.equal(r.valid, true);
    assert.deepEqual(r.warnings, []);
  });

  it('warns on an invalid omitClaudeMd value', () => {
    const r = validateAgentDefinition({ ...base, omitClaudeMd: 'yes' as unknown as boolean });
    assert.equal(r.valid, true);
    assert.ok(r.warnings.some((w) => /omitClaudeMd/.test(w)));
  });

  it('warns on an invalid planModeRequired value', () => {
    const r = validateAgentDefinition({ ...base, planModeRequired: 3 as unknown as boolean });
    assert.ok(r.warnings.some((w) => /planModeRequired/.test(w)));
  });

  it('warns on an unknown descriptionColor', () => {
    const r = validateAgentDefinition({ ...base, descriptionColor: 'chartreuse' as unknown as never });
    assert.ok(r.warnings.some((w) => /descriptionColor/.test(w)));
  });

  it('`omitClaudeMd` / `planModeRequired` / `descriptionColor` are declared AgentDefinition fields', () => {
    const agent: Partial<AgentDefinition> = {
      omitClaudeMd: true,
      planModeRequired: true,
      descriptionColor: 'red',
    };
    assert.equal(agent.omitClaudeMd, true);
    assert.equal(agent.planModeRequired, true);
    assert.equal(agent.descriptionColor, 'red');
  });
});
