import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { skillTool, resolveSkillTarget, type SkillResolution } from '../src/tools/skill';
import { MAX_SKILL_ARG_VALUE_LENGTH, type SkillDefinition } from '../src/skills';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

function makeSkill(partial: Partial<SkillDefinition>): SkillDefinition {
  return {
    name: 'x',
    description: 'd',
    content: 'body',
    filePath: '<test>',
    ...partial,
  };
}

/** A manager mock whose getSkill/getSkillContent only answer to the EXACT names
 *  it was seeded with — so name-resolution fallbacks are genuinely exercised
 *  (a verbatim-only lookup fails for a qualified/unqualified mismatch). */
function managerOf(skills: SkillDefinition[]) {
  const byName = new Map(skills.map(s => [s.name, s]));
  const captured: { name?: string; args?: Record<string, string> } = {};
  return {
    captured,
    manager: {
      getSkill: (n: string) => byName.get(n) ?? null,
      getSkillContent: (n: string, args?: Record<string, string>) => {
        captured.name = n;
        captured.args = args;
        return byName.has(n) ? (byName.get(n)!.content || 'content') : null;
      },
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

// ===========================================================================
// resolveSkillTarget — pure resolver (plugin:skill, dir:name, most-specific)
// ===========================================================================

describe('resolveSkillTarget — name resolution', () => {
  it('exact match wins (unscoped and fully-qualified)', () => {
    const skills = [makeSkill({ name: 'deploy' }), makeSkill({ name: 'web:deploy' })];
    const mgr = managerOf(skills).manager;
    assert.equal(resolveSkillTarget('deploy', mgr).canonicalName, 'deploy');
    assert.equal(resolveSkillTarget('web:deploy', mgr).canonicalName, 'web:deploy');
  });

  it('strips leading slash before resolving', () => {
    const mgr = managerOf([makeSkill({ name: 'commit' })]).manager;
    const r = resolveSkillTarget('/commit', mgr);
    assert.equal(r.canonicalName, 'commit');
    assert.ok(r.skill);
  });

  it('qualified request falls back to the bare skill segment', () => {
    // Only the unscoped "deploy" exists; a plugin/dir-qualified call resolves it.
    const mgr = managerOf([makeSkill({ name: 'deploy' })]).manager;
    const r = resolveSkillTarget('web:deploy', mgr);
    assert.ok(r.skill, 'web:deploy should fall back to deploy');
    assert.equal(r.canonicalName, 'deploy');
  });

  it('unqualified request resolves to a unique scoped variant (most-specific)', () => {
    const mgr = managerOf([makeSkill({ name: 'apps/web:deploy' })]).manager;
    const r = resolveSkillTarget('deploy', mgr);
    assert.ok(r.skill);
    assert.equal(r.canonicalName, 'apps/web:deploy');
  });

  it('unqualified request matching several scoped skills is ambiguous', () => {
    const mgr = managerOf([
      makeSkill({ name: 'web:deploy' }),
      makeSkill({ name: 'api:deploy' }),
    ]).manager;
    const r: SkillResolution = resolveSkillTarget('deploy', mgr);
    assert.equal(r.skill, null);
    assert.deepEqual(r.ambiguous?.sort(), ['api:deploy', 'web:deploy']);
  });

  it('genuinely unknown name resolves to null, no ambiguity', () => {
    const mgr = managerOf([makeSkill({ name: 'other' })]).manager;
    const r = resolveSkillTarget('nope', mgr);
    assert.equal(r.skill, null);
    assert.equal(r.ambiguous, undefined);
  });
});

// ===========================================================================
// GAP 1 — argument-safety: the ARGUMENTS passthrough value is capped
//   RED (revert sanitize): getSkillContent receives the full oversized value.
//   GREEN: value is capped to MAX_SKILL_ARG_VALUE_LENGTH.
// ===========================================================================

describe('skill tool — arg-safety (GAP 1)', () => {
  it('caps an oversized ARGUMENTS value before passing it to getSkillContent', async () => {
    const huge = 'a'.repeat(MAX_SKILL_ARG_VALUE_LENGTH * 2);
    const h = managerOf([makeSkill({ name: 'explain', context: 'inline' })]);
    const r = await skillTool.execute(
      { skill: 'explain', args: huge },
      ctx({ _skillManager: h.manager as any }),
    );
    assert.ok(!r.isError);
    // Discriminator: capped to the limit, NOT the full 2x value.
    assert.equal(h.captured.args?.ARGUMENTS?.length, MAX_SKILL_ARG_VALUE_LENGTH);
    assert.notEqual(h.captured.args?.ARGUMENTS?.length, huge.length);
  });

  it('leaves a normal-sized args string untouched', async () => {
    const h = managerOf([makeSkill({ name: 'explain', context: 'inline' })]);
    await skillTool.execute(
      { skill: 'explain', args: 'small args' },
      ctx({ _skillManager: h.manager as any }),
    );
    assert.equal(h.captured.args?.ARGUMENTS, 'small args');
  });

  it('omits the args object entirely when no args are passed', async () => {
    const h = managerOf([makeSkill({ name: 'explain', context: 'inline' })]);
    await skillTool.execute({ skill: 'explain' }, ctx({ _skillManager: h.manager as any }));
    assert.equal(h.captured.args, undefined);
  });
});

// ===========================================================================
// GAP 2 — plugin-qualified / directory-scoped resolution in execute()
//   RED (revert resolveSkillTarget use → getSkill(skillName)): a qualified
//   name whose exact form is not registered returns "not found".
//   GREEN: it resolves to the bare / scoped skill and activates it.
// ===========================================================================

describe('skill tool — qualified/scoped resolution (GAP 2)', () => {
  it('resolves a plugin/dir-qualified name to the bare skill', async () => {
    const h = managerOf([makeSkill({ name: 'deploy', context: 'inline', content: 'DEPLOY-BODY' })]);
    const r = await skillTool.execute(
      { skill: 'web:deploy' },
      ctx({ _skillManager: h.manager as any }),
    );
    // Discriminator: GREEN activates (isError false, canonical name used);
    // RED (verbatim lookup) would return a not-found error.
    assert.ok(!r.isError);
    assert.match(r.output, /activated inline/);
    assert.equal(h.captured.name, 'deploy');
  });

  it('resolves an unqualified name to a unique scoped skill', async () => {
    const h = managerOf([makeSkill({ name: 'apps/web:build', context: 'inline' })]);
    const r = await skillTool.execute(
      { skill: 'build' },
      ctx({ _skillManager: h.manager as any }),
    );
    assert.ok(!r.isError);
    assert.equal(h.captured.name, 'apps/web:build');
  });
});

// ===========================================================================
// GAP 3 — ambiguous unqualified name is surfaced, not silently guessed
//   RED (revert resolveSkillTarget use): "deploy" is not found verbatim and
//   the tool reports a generic "not found" error, NOT an ambiguity message.
//   GREEN: the tool reports an "ambiguous" error listing the scoped candidates.
// ===========================================================================

describe('skill tool — ambiguity surfacing (GAP 3)', () => {
  it('reports ambiguity with candidate names when several scoped skills match', async () => {
    const h = managerOf([
      makeSkill({ name: 'web:deploy', context: 'inline' }),
      makeSkill({ name: 'api:deploy', context: 'inline' }),
    ]);
    const r = await skillTool.execute(
      { skill: 'deploy' },
      ctx({ _skillManager: h.manager as any }),
    );
    assert.equal(r.isError, true);
    // Discriminator: GREEN says "ambiguous" and lists both; RED says "not found".
    assert.match(r.output, /ambiguous/i);
    assert.match(r.output, /web:deploy/);
    assert.match(r.output, /api:deploy/);
    assert.doesNotMatch(r.output, /not found/i);
  });

  it('a truly unknown unqualified name still reports not-found (not ambiguous)', async () => {
    const h = managerOf([makeSkill({ name: 'web:deploy', context: 'inline' })]);
    const r = await skillTool.execute(
      { skill: 'totally-unknown' },
      ctx({ _skillManager: h.manager as any }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /not found/i);
    assert.doesNotMatch(r.output, /ambiguous/i);
  });
});
