// U023 — core building blocks for three Claude-parity skill fields:
//   * `aliases`         — alternate lookup names            → parseSkillAliases
//   * `isEnabled`       — runtime availability predicate     → isSkillEnabled
//   * `requiresArgument`/`missingArgumentMessage`            → missingSkillArgumentMessage
//     enforced by the skill tool before a skill body runs.
//
// Each assertion exercises the REAL exported function / tool against an observable
// result (no source-text scraping).

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { skillTool } from '../src/tools/skill';
import {
  parseSkillAliases,
  isSkillEnabled,
  missingSkillArgumentMessage,
  MAX_SKILL_ARG_NAMES,
  MAX_SKILL_ARG_NAME_LENGTH,
  type SkillDefinition,
} from '../src/skills';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// parseSkillAliases
// ---------------------------------------------------------------------------

describe('parseSkillAliases', () => {
  it('splits a comma/whitespace string and de-duplicates', () => {
    assert.deepEqual(parseSkillAliases('a, b  c, a'), ['a', 'b', 'c']);
  });

  it('accepts a string array', () => {
    assert.deepEqual(parseSkillAliases(['one', 'two']), ['one', 'two']);
  });

  it('returns [] for a non-string / non-array value', () => {
    assert.deepEqual(parseSkillAliases(undefined), []);
    assert.deepEqual(parseSkillAliases(42), []);
    assert.deepEqual(parseSkillAliases({}), []);
  });

  it('drops over-length names and caps the count', () => {
    const tooLong = 'x'.repeat(MAX_SKILL_ARG_NAME_LENGTH + 1);
    assert.deepEqual(parseSkillAliases([tooLong, 'ok']), ['ok']);
    const many = Array.from({ length: MAX_SKILL_ARG_NAMES + 10 }, (_, i) => `a${i}`);
    assert.equal(parseSkillAliases(many).length, MAX_SKILL_ARG_NAMES);
  });
});

// ---------------------------------------------------------------------------
// isSkillEnabled
// ---------------------------------------------------------------------------

describe('isSkillEnabled', () => {
  it('is enabled when no predicate is present', () => {
    assert.equal(isSkillEnabled({}), true);
  });

  it('honors a true/false predicate', () => {
    assert.equal(isSkillEnabled({ isEnabled: () => true }), true);
    assert.equal(isSkillEnabled({ isEnabled: () => false }), false);
  });

  it('treats a throwing predicate as DISABLED (fail closed)', () => {
    assert.equal(isSkillEnabled({ isEnabled: () => { throw new Error('boom'); } }), false);
  });

  it('treats a non-true return as disabled', () => {
    // A predicate that returns a truthy-but-not-true value is still disabled.
    assert.equal(isSkillEnabled({ isEnabled: (() => 1) as unknown as () => boolean }), false);
  });
});

// ---------------------------------------------------------------------------
// missingSkillArgumentMessage
// ---------------------------------------------------------------------------

describe('missingSkillArgumentMessage', () => {
  it('returns the author message verbatim when present', () => {
    assert.equal(
      missingSkillArgumentMessage({ name: 'batch', missingArgumentMessage: 'Provide an instruction.' }),
      'Provide an instruction.',
    );
  });

  it('generates a usage line that includes the argumentHint', () => {
    const msg = missingSkillArgumentMessage({ name: 'batch', argumentHint: '<instruction>' });
    assert.match(msg, /Skill "batch" requires an argument/);
    assert.match(msg, /<instruction>/);
  });

  it('falls back to a bare usage line with no hint', () => {
    const msg = missingSkillArgumentMessage({ name: 'solo' });
    assert.equal(msg, 'Skill "solo" requires an argument. Provide one and try again.');
  });

  it('ignores a blank author message and generates instead', () => {
    const msg = missingSkillArgumentMessage({ name: 'batch', missingArgumentMessage: '   ' });
    assert.match(msg, /requires an argument/);
  });
});

// ---------------------------------------------------------------------------
// skill tool — requiresArgument enforcement (end-to-end through skillTool.execute)
// ---------------------------------------------------------------------------

function makeSkill(partial: Partial<SkillDefinition>): SkillDefinition {
  return { name: 'x', description: 'd', content: 'body', filePath: '<test>', ...partial };
}

function managerOf(skills: SkillDefinition[]) {
  const byName = new Map(skills.map(s => [s.name, s]));
  const captured: { contentCalled: boolean } = { contentCalled: false };
  return {
    captured,
    manager: {
      getSkill: (n: string) => byName.get(n) ?? null,
      getSkillContent: (n: string) => { captured.contentCalled = true; return byName.has(n) ? 'content' : null; },
      listSkills: () => skills,
      listAgents: () => [],
      getAgent: () => null,
    },
  };
}

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: '/tmp',
    abortSignal: new AbortController().signal,
    _engineAddSystemReminder: () => {},
    _engineSetActiveSkill: () => {},
    ...overrides,
  } as unknown as ToolContext;
}

describe('skill tool — requiresArgument (U023)', () => {
  it('rejects an empty-arg invocation with the default message and never reads content', async () => {
    const h = managerOf([makeSkill({ name: 'batch', requiresArgument: true, context: 'inline' })]);
    const r = await skillTool.execute({ skill: 'batch' }, ctx({ _skillManager: h.manager as any }));
    assert.equal(r.isError, true);
    assert.match(r.output, /requires an argument/);
    assert.equal(h.captured.contentCalled, false, 'must short-circuit before getSkillContent');
  });

  it('rejects a whitespace-only argument', async () => {
    const h = managerOf([makeSkill({ name: 'batch', requiresArgument: true, context: 'inline' })]);
    const r = await skillTool.execute({ skill: 'batch', args: '   ' }, ctx({ _skillManager: h.manager as any }));
    assert.equal(r.isError, true);
    assert.match(r.output, /requires an argument/);
  });

  it('returns the custom missingArgumentMessage when provided', async () => {
    const h = managerOf([makeSkill({
      name: 'batch', requiresArgument: true, context: 'inline',
      missingArgumentMessage: 'Describe the batch change you want.',
    })]);
    const r = await skillTool.execute({ skill: 'batch' }, ctx({ _skillManager: h.manager as any }));
    assert.equal(r.isError, true);
    assert.equal(r.output, 'Describe the batch change you want.');
  });

  it('runs normally when an argument IS supplied', async () => {
    const h = managerOf([makeSkill({ name: 'batch', requiresArgument: true, context: 'inline' })]);
    const r = await skillTool.execute({ skill: 'batch', args: 'migrate X to Y' }, ctx({ _skillManager: h.manager as any }));
    assert.ok(!r.isError, r.output);
    assert.equal(h.captured.contentCalled, true);
  });

  it('is a no-op constraint for a skill that does not require an argument', async () => {
    const h = managerOf([makeSkill({ name: 'explain', context: 'inline' })]);
    const r = await skillTool.execute({ skill: 'explain' }, ctx({ _skillManager: h.manager as any }));
    assert.ok(!r.isError, r.output);
    assert.equal(h.captured.contentCalled, true);
  });
});
