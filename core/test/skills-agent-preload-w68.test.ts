// U023-w68 — buildPreloadedSkillsBlock: assemble the system-prompt block for an
// agent's `skills` preload allowlist (parity with Claude's runAgent skill
// preloading), reporting which skills loaded vs. which names could not be resolved.

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  buildPreloadedSkillsBlock,
  PRELOADED_SKILLS_HEADING,
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

describe('U023-w68 — buildPreloadedSkillsBlock', () => {
  it('inlines each resolved skill body under a ## heading and records loaded names', () => {
    const skills: Record<string, SkillDefinition> = {
      alpha: skill({ name: 'alpha', content: 'Alpha instructions.' }),
      beta: skill({ name: 'beta', content: 'Beta instructions.' }),
    };
    const res = buildPreloadedSkillsBlock(['alpha', 'beta'], (n) => skills[n] ?? null);
    assert.deepEqual(res.loaded, ['alpha', 'beta']);
    assert.deepEqual(res.missing, []);
    assert.ok(res.block.startsWith(PRELOADED_SKILLS_HEADING));
    assert.match(res.block, /## alpha\n\nAlpha instructions\./);
    assert.match(res.block, /## beta\n\nBeta instructions\./);
  });

  it('records a name with no matching skill as missing and omits it from the block', () => {
    const skills: Record<string, SkillDefinition> = {
      known: skill({ name: 'known', content: 'Known body.' }),
    };
    const res = buildPreloadedSkillsBlock(['known', 'ghost'], (n) => skills[n] ?? null);
    assert.deepEqual(res.loaded, ['known']);
    assert.deepEqual(res.missing, ['ghost']);
    assert.ok(!res.block.includes('ghost'));
  });

  it('treats a disabled skill (isEnabled → false) as missing', () => {
    const disabled = skill({ name: 'off', content: 'hidden body', isEnabled: () => false });
    const res = buildPreloadedSkillsBlock(['off'], () => disabled);
    assert.deepEqual(res.loaded, []);
    assert.deepEqual(res.missing, ['off']);
    assert.equal(res.block, '');
  });

  it('treats a blank-bodied skill as missing', () => {
    const blank = skill({ name: 'blank', content: '   \n  ' });
    const res = buildPreloadedSkillsBlock(['blank'], () => blank);
    assert.deepEqual(res.missing, ['blank']);
    assert.equal(res.block, '');
  });

  it('returns an empty block when nothing resolves', () => {
    const res = buildPreloadedSkillsBlock(['a', 'b'], () => null);
    assert.equal(res.block, '');
    assert.deepEqual(res.loaded, []);
    assert.deepEqual(res.missing, ['a', 'b']);
  });

  it('never inlines the same resolved skill twice', () => {
    const one = skill({ name: 'dup', content: 'Once.' });
    const res = buildPreloadedSkillsBlock(['dup', 'dup-alias'], () => one);
    // Both names resolve to the same skill object; it is inlined once.
    assert.deepEqual(res.loaded, ['dup']);
    assert.equal((res.block.match(/## dup\n/g) || []).length, 1);
  });

  it('handles an empty allowlist', () => {
    const res = buildPreloadedSkillsBlock([], () => null);
    assert.equal(res.block, '');
    assert.deepEqual(res.loaded, []);
    assert.deepEqual(res.missing, []);
  });
});
