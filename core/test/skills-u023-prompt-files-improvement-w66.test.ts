// U023 (w66) — the reference-parity skill/agent building blocks added to core/core/src/skills.ts:
//   * getPromptForCommand / resolveSkillPrompt — programmatic dynamic skill body
//       (parity with the reference's bundled-skill getPromptForCommand, e.g. /batch, /claude-api)
//   * files / getSkillFile — bundled companion-file resources with path-traversal safety
//       (parity with the reference's SKILL_FILES)
//   * progressMessage / skillProgressMessage — in-progress affordance with 'running' default
//       (parity with the reference's command progressMessage)
//   * estimateSkillFrontmatterTokens — per-skill frontmatter token estimate for /skills listing
//       (parity with the reference's estimateSkillFrontmatterTokens)
//   * getSystemPrompt / resolveAgentSystemPrompt — dynamic computed agent system prompt
//       (parity with the reference's agent getSystemPrompt(ctx))
//   * SkillUpdate / SkillImprovementSuggestion + parseSkillUpdates / isValidSkillImprovement /
//       formatSkillUpdates — structured skill-improvement descriptor
//       (parity with the reference's SkillImprovementSuggestion / SkillUpdate)
//
// Each assertion exercises the REAL exported function against an observable result.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  resolveSkillPrompt,
  getSkillFile,
  skillProgressMessage,
  DEFAULT_SKILL_PROGRESS_MESSAGE,
  estimateSkillFrontmatterTokens,
  resolveAgentSystemPrompt,
  parseSkillUpdates,
  isValidSkillImprovement,
  formatSkillUpdates,
  skillHasOnlySafeProperties,
  type SkillDefinition,
  type AgentDefinition,
  type SkillUpdate,
} from '../src/skills';
import { estimateTokens } from '../src/utils/tokens';

function mkSkill(over: Partial<SkillDefinition> = {}): SkillDefinition {
  return {
    name: 'demo',
    description: 'a demo skill',
    content: 'static body',
    filePath: '<builtin>',
    ...over,
  };
}

// ---------------------------------------------------------------------------
// resolveSkillPrompt — programmatic getPromptForCommand
// ---------------------------------------------------------------------------

describe('resolveSkillPrompt', () => {
  it('returns the static content when no builder is declared', async () => {
    assert.equal(await resolveSkillPrompt(mkSkill({ content: 'hello' })), 'hello');
  });

  it('invokes a synchronous getPromptForCommand with the raw args', async () => {
    const skill = mkSkill({
      getPromptForCommand: (args) => `built:${args}`,
    });
    assert.equal(await resolveSkillPrompt(skill, 'xyz'), 'built:xyz');
  });

  it('awaits an async getPromptForCommand', async () => {
    const skill = mkSkill({
      getPromptForCommand: async (args) => `async:${args.trim()}`,
    });
    assert.equal(await resolveSkillPrompt(skill, '  cmd  '), 'async:cmd');
  });

  it('defaults args to the empty string when omitted', async () => {
    const skill = mkSkill({ getPromptForCommand: (args) => `[${args}]` });
    assert.equal(await resolveSkillPrompt(skill), '[]');
  });

  it('coerces a non-string builder return to the empty string', async () => {
    const skill = mkSkill({
      // @ts-expect-error — exercise a malformed builder return at runtime
      getPromptForCommand: () => 123,
    });
    assert.equal(await resolveSkillPrompt(skill), '');
  });

  it('prefers the builder over static content when both exist', async () => {
    const skill = mkSkill({ content: 'STATIC', getPromptForCommand: () => 'DYNAMIC' });
    assert.equal(await resolveSkillPrompt(skill), 'DYNAMIC');
  });
});

// ---------------------------------------------------------------------------
// getSkillFile — bundled companion files + path safety
// ---------------------------------------------------------------------------

describe('getSkillFile', () => {
  const files = {
    'examples/cli.md': 'cli content',
    'examples/server.md': 'server content',
  };

  it('returns the inlined content for a known relative path', () => {
    assert.equal(getSkillFile(mkSkill({ files }), 'examples/cli.md'), 'cli content');
  });

  it('strips a leading "./" before lookup', () => {
    assert.equal(getSkillFile(mkSkill({ files }), './examples/server.md'), 'server content');
  });

  it('normalizes backslash separators to forward slashes', () => {
    assert.equal(getSkillFile(mkSkill({ files }), 'examples\\cli.md'), 'cli content');
  });

  it('returns null for an unknown path', () => {
    assert.equal(getSkillFile(mkSkill({ files }), 'examples/missing.md'), null);
  });

  it('returns null when the skill declares no files', () => {
    assert.equal(getSkillFile(mkSkill(), 'examples/cli.md'), null);
  });

  it('rejects "../" traversal', () => {
    const evil = { '../secret': 'x', 'examples/cli.md': 'ok' };
    assert.equal(getSkillFile(mkSkill({ files: evil }), '../secret'), null);
  });

  it('rejects an absolute path', () => {
    assert.equal(getSkillFile(mkSkill({ files: { '/etc/passwd': 'x' } }), '/etc/passwd'), null);
  });

  it('returns null for a non-string path argument', () => {
    // @ts-expect-error — exercise a non-string argument at runtime
    assert.equal(getSkillFile(mkSkill({ files }), 42), null);
  });
});

// ---------------------------------------------------------------------------
// skillProgressMessage — in-progress affordance + default
// ---------------------------------------------------------------------------

describe('skillProgressMessage', () => {
  it('falls back to the default "running" when absent', () => {
    assert.equal(skillProgressMessage(mkSkill()), DEFAULT_SKILL_PROGRESS_MESSAGE);
    assert.equal(DEFAULT_SKILL_PROGRESS_MESSAGE, 'running');
  });

  it('falls back to the default for a blank/whitespace message', () => {
    assert.equal(skillProgressMessage(mkSkill({ progressMessage: '   ' })), 'running');
  });

  it('returns the trimmed custom message when present', () => {
    assert.equal(skillProgressMessage(mkSkill({ progressMessage: '  analyzing…  ' })), 'analyzing…');
  });
});

// ---------------------------------------------------------------------------
// estimateSkillFrontmatterTokens
// ---------------------------------------------------------------------------

describe('estimateSkillFrontmatterTokens', () => {
  it('estimates from name + description + whenToUse joined by spaces', () => {
    const skill = mkSkill({ name: 'n', description: 'desc', whenToUse: 'when' });
    const expected = estimateTokens('n desc when');
    assert.equal(estimateSkillFrontmatterTokens(skill), expected);
  });

  it('skips absent/empty frontmatter fields', () => {
    const skill = mkSkill({ name: 'only', description: '', whenToUse: undefined });
    assert.equal(estimateSkillFrontmatterTokens(skill), estimateTokens('only'));
  });

  it('is zero for entirely empty frontmatter', () => {
    const skill = mkSkill({ name: '', description: '', whenToUse: undefined });
    assert.equal(estimateSkillFrontmatterTokens(skill), 0);
  });
});

// ---------------------------------------------------------------------------
// resolveAgentSystemPrompt — dynamic computed agent prompt
// ---------------------------------------------------------------------------

function mkAgent(over: Partial<AgentDefinition> = {}): AgentDefinition {
  return {
    name: 'a',
    description: 'd',
    systemPrompt: 'static prompt',
    filePath: '<builtin>',
    ...over,
  };
}

describe('resolveAgentSystemPrompt', () => {
  it('returns the static systemPrompt when no builder is declared', async () => {
    assert.equal(await resolveAgentSystemPrompt(mkAgent({ systemPrompt: 'hi' })), 'hi');
  });

  it('invokes getSystemPrompt with the runtime context', async () => {
    const agent = mkAgent({
      getSystemPrompt: (ctx) => `dyn:${(ctx as { lang?: string } | undefined)?.lang ?? 'none'}`,
    });
    assert.equal(await resolveAgentSystemPrompt(agent, { lang: 'ts' }), 'dyn:ts');
  });

  it('awaits an async builder', async () => {
    const agent = mkAgent({ getSystemPrompt: async () => 'computed' });
    assert.equal(await resolveAgentSystemPrompt(agent), 'computed');
  });

  it('coerces a non-string builder return to the empty string', async () => {
    // @ts-expect-error — malformed builder return
    const agent = mkAgent({ getSystemPrompt: () => null });
    assert.equal(await resolveAgentSystemPrompt(agent), '');
  });
});

// ---------------------------------------------------------------------------
// SkillUpdate / SkillImprovementSuggestion
// ---------------------------------------------------------------------------

describe('parseSkillUpdates', () => {
  it('keeps well-formed updates and trims fields', () => {
    const raw = [
      { section: '  intro ', change: ' be concise ', reason: ' user asked ' },
    ];
    assert.deepEqual(parseSkillUpdates(raw), [
      { section: 'intro', change: 'be concise', reason: 'user asked' },
    ]);
  });

  it('defaults a missing/invalid reason to the empty string', () => {
    assert.deepEqual(parseSkillUpdates([{ section: 's', change: 'c' }]), [
      { section: 's', change: 'c', reason: '' },
    ]);
  });

  it('drops entries missing section or change', () => {
    const raw = [
      { section: '', change: 'c' },
      { section: 's', change: '  ' },
      { change: 'c' },
      { section: 's', change: 'c' },
    ];
    assert.deepEqual(parseSkillUpdates(raw), [{ section: 's', change: 'c', reason: '' }]);
  });

  it('ignores non-object entries and non-array input', () => {
    assert.deepEqual(parseSkillUpdates([null, 'x', 42, ['a']]), []);
    assert.deepEqual(parseSkillUpdates('nope'), []);
    assert.deepEqual(parseSkillUpdates(undefined), []);
  });
});

describe('isValidSkillImprovement', () => {
  it('accepts a suggestion with a name and at least one valid update', () => {
    assert.equal(
      isValidSkillImprovement({ skillName: 'x', updates: [{ section: 's', change: 'c' }] }),
      true,
    );
  });

  it('rejects a blank/absent skillName', () => {
    assert.equal(isValidSkillImprovement({ skillName: '  ', updates: [{ section: 's', change: 'c' }] }), false);
    assert.equal(isValidSkillImprovement({ updates: [{ section: 's', change: 'c' }] }), false);
  });

  it('rejects a suggestion with no valid updates', () => {
    assert.equal(isValidSkillImprovement({ skillName: 'x', updates: [] }), false);
    assert.equal(isValidSkillImprovement({ skillName: 'x', updates: [{ section: '', change: '' }] }), false);
  });

  it('rejects non-object / null input', () => {
    assert.equal(isValidSkillImprovement(null), false);
    assert.equal(isValidSkillImprovement('x'), false);
    assert.equal(isValidSkillImprovement([{ section: 's', change: 'c' }]), false);
  });
});

describe('formatSkillUpdates', () => {
  it('renders a "- section: change" bullet list, excluding invalid entries', () => {
    const updates: SkillUpdate[] = [
      { section: 'a', change: 'do x', reason: 'r1' },
      { section: '', change: 'dropped', reason: '' },
      { section: 'b', change: 'do y', reason: 'r2' },
    ];
    assert.equal(formatSkillUpdates(updates), '- a: do x\n- b: do y');
  });

  it('returns an empty string for no valid updates', () => {
    assert.equal(formatSkillUpdates([]), '');
  });
});

// ---------------------------------------------------------------------------
// SAFE_SKILL_PROPERTIES — new fields must not force the permission gate
// ---------------------------------------------------------------------------

describe('skillHasOnlySafeProperties with the new fields', () => {
  it('treats files/getPromptForCommand/progressMessage as safe', () => {
    assert.equal(
      skillHasOnlySafeProperties({
        name: 'x',
        description: 'd',
        files: { 'a.md': 'x' },
        getPromptForCommand: () => 'p',
        progressMessage: 'running',
      }),
      true,
    );
  });
});
