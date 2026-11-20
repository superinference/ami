// U023-w76 — resolveSkillName: the ordered, multi-strategy skill-name resolver
// (parity with the reference's runAgent `resolveSkillName`) used by agent `skills:`
// frontmatter preloading. Verifies exact (name/userFacingName/alias), plugin-
// qualified, and plugin-namespaced suffix matching + their precedence, plus the
// end-to-end fallback through buildPreloadedSkillsBlock.

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  resolveSkillName,
  buildPreloadedSkillsBlock,
  type SkillDefinition,
} from '../src/skills';

function skill(partial: Partial<SkillDefinition> & { name: string }): SkillDefinition {
  return {
    description: 'd',
    content: 'body',
    filePath: '<test>',
    ...partial,
  } as SkillDefinition;
}

describe('U023-w76 — resolveSkillName multi-strategy resolution', () => {
  it('strategy 1: exact match on canonical name', () => {
    const a = skill({ name: 'alpha' });
    const b = skill({ name: 'beta' });
    assert.equal(resolveSkillName('beta', [a, b]), b);
  });

  it('strategy 1: exact match on userFacingName', () => {
    const a = skill({ name: 'internal-id', userFacingName: 'Pretty Name' });
    assert.equal(resolveSkillName('Pretty Name', [a]), a);
  });

  it('strategy 1: exact match on an alias', () => {
    const a = skill({ name: 'alpha', aliases: ['a', 'al'] });
    assert.equal(resolveSkillName('al', [a]), a);
  });

  it('strategy 2: plugin-qualified match via pluginPrefix', () => {
    const a = skill({ name: 'myplugin:fmt' });
    // Bare "fmt" is not registered; the agent's plugin prefix qualifies it.
    assert.equal(resolveSkillName('fmt', [a], { pluginPrefix: 'myplugin' }), a);
  });

  it('strategy 2: plugin-qualified match against an alias', () => {
    const a = skill({ name: 'real', aliases: ['myplugin:fmt'] });
    assert.equal(resolveSkillName('fmt', [a], { pluginPrefix: 'myplugin' }), a);
  });

  it('strategy 3: suffix match on ":requested" for a plugin-namespaced skill', () => {
    const a = skill({ name: 'otherplugin:deploy' });
    // No bare "deploy", no plugin prefix given — suffix match still resolves it.
    assert.equal(resolveSkillName('deploy', [a]), a);
  });

  it('strategy 3: suffix match against an alias', () => {
    const a = skill({ name: 'real', aliases: ['pkg:lint'] });
    assert.equal(resolveSkillName('lint', [a]), a);
  });

  it('precedence: exact beats a plugin-namespaced suffix candidate', () => {
    const exact = skill({ name: 'build', content: 'exact' });
    const namespaced = skill({ name: 'plugin:build', content: 'namespaced' });
    // Both could match "build"; the exact bare name must win.
    assert.equal(resolveSkillName('build', [namespaced, exact]), exact);
  });

  it('precedence: plugin-qualified beats a bare suffix candidate', () => {
    const own = skill({ name: 'mine:run', content: 'own-plugin' });
    const other = skill({ name: 'other:run', content: 'other-plugin' });
    assert.equal(
      resolveSkillName('run', [other, own], { pluginPrefix: 'mine' }),
      own,
    );
  });

  it('returns null for a blank, whitespace, or non-string request', () => {
    const a = skill({ name: 'alpha' });
    assert.equal(resolveSkillName('', [a]), null);
    assert.equal(resolveSkillName('   ', [a]), null);
    assert.equal(resolveSkillName(undefined as unknown as string, [a]), null);
  });

  it('returns null when no strategy matches', () => {
    const a = skill({ name: 'alpha' });
    assert.equal(resolveSkillName('ghost', [a]), null);
    assert.equal(resolveSkillName('ghost', [a], { pluginPrefix: 'p' }), null);
  });

  it('trims the requested name before matching', () => {
    const a = skill({ name: 'alpha' });
    assert.equal(resolveSkillName('  alpha  ', [a]), a);
  });

  it('end-to-end: buildPreloadedSkillsBlock resolves a namespaced skill via resolveSkillName fallback', () => {
    // Simulate the task.ts wiring: exact getSkill first, then resolveSkillName.
    const namespaced = skill({ name: 'pkg:format', content: 'Format instructions.' });
    const candidates = [namespaced];
    const getSkill = (n: string) => (n === 'pkg:format' ? namespaced : null);
    const res = buildPreloadedSkillsBlock(
      ['format'], // agent references the bare name
      (n) => getSkill(n) ?? resolveSkillName(n, candidates),
    );
    assert.deepEqual(res.loaded, ['pkg:format']);
    assert.deepEqual(res.missing, []);
    assert.match(res.block, /## pkg:format\n\nFormat instructions\./);
  });
});
