// U023 (w77) — additive skill/agent building blocks in core/core/src/skills.ts:
//   1. `requiredEnv` declarative env/feature-flag gate on SkillDefinition, honored
//      by isSkillEnabled (parity with Claude's env-gated bundled skills).
//   2. `fileName` provenance on AgentDefinition + agentFileNameMismatch() detector
//      (parity with Claude's agentFileUtils filename-vs-agentType handling).
// Pure-function tests; the SkillManager wiring is covered in common/test.

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  requiredEnvSatisfied,
  isSkillEnabled,
  agentFileNameMismatch,
  SAFE_SKILL_PROPERTIES,
  skillHasOnlySafeProperties,
  type SkillDefinition,
  type AgentDefinition,
} from '../src/skills';

function skill(extra: Partial<SkillDefinition>): SkillDefinition {
  return {
    name: 's', description: 'd', content: 'body', filePath: '<test>', ...extra,
  };
}

describe('requiredEnvSatisfied', () => {
  it('is satisfied when there is no requirement', () => {
    assert.equal(requiredEnvSatisfied(undefined, {}), true);
    assert.equal(requiredEnvSatisfied([], { FOO: 'x' }), true);
  });

  it('requires EVERY named var to be set non-empty', () => {
    assert.equal(requiredEnvSatisfied(['FOO', 'BAR'], { FOO: '1', BAR: '2' }), true);
    assert.equal(requiredEnvSatisfied(['FOO', 'BAR'], { FOO: '1' }), false);
    assert.equal(requiredEnvSatisfied(['FOO'], { FOO: undefined }), false);
  });

  it('treats an empty / whitespace-only value as unset', () => {
    assert.equal(requiredEnvSatisfied(['FOO'], { FOO: '' }), false);
    assert.equal(requiredEnvSatisfied(['FOO'], { FOO: '   ' }), false);
    assert.equal(requiredEnvSatisfied(['FOO'], { FOO: ' x ' }), true);
  });

  it('ignores a non-string / empty-string entry (no requirement for that slot)', () => {
    // A malformed frontmatter list never crashes the gate.
    assert.equal(requiredEnvSatisfied(['', 'FOO'] as string[], { FOO: 'v' }), true);
    assert.equal(requiredEnvSatisfied([123 as unknown as string], {}), true);
  });
});

describe('isSkillEnabled honors requiredEnv alongside isEnabled', () => {
  it('hides a skill whose required env is unset', () => {
    const s = skill({ requiredEnv: ['NEEDS_ME'] });
    assert.equal(isSkillEnabled(s, {}), false);
    assert.equal(isSkillEnabled(s, { NEEDS_ME: 'yes' }), true);
  });

  it('still honors a false isEnabled predicate even when env is satisfied', () => {
    const s = skill({ requiredEnv: ['OK'], isEnabled: () => false });
    assert.equal(isSkillEnabled(s, { OK: '1' }), false);
  });

  it('requires BOTH gates to pass', () => {
    const s = skill({ requiredEnv: ['OK'], isEnabled: () => true });
    assert.equal(isSkillEnabled(s, {}), false);        // env fails
    assert.equal(isSkillEnabled(s, { OK: '1' }), true); // both pass
  });

  it('a skill with no gates is enabled', () => {
    assert.equal(isSkillEnabled(skill({}), {}), true);
  });

  it('a throwing predicate is treated as disabled (unchanged behavior)', () => {
    const s = skill({ isEnabled: () => { throw new Error('boom'); } });
    assert.equal(isSkillEnabled(s, {}), false);
  });

  it('defaults to process.env when no env is supplied', () => {
    const key = 'U023_REQENV_TEST_VAR';
    delete process.env[key];
    assert.equal(isSkillEnabled(skill({ requiredEnv: [key] })), false);
    process.env[key] = 'set';
    try {
      assert.equal(isSkillEnabled(skill({ requiredEnv: [key] })), true);
    } finally {
      delete process.env[key];
    }
  });
});

describe('requiredEnv is a SAFE skill property', () => {
  it('is in the SAFE allowlist and does not force the permission gate', () => {
    assert.equal(SAFE_SKILL_PROPERTIES.has('requiredEnv'), true);
    assert.equal(skillHasOnlySafeProperties({ name: 's', requiredEnv: ['FOO'] }), true);
  });
});

function agent(extra: Partial<AgentDefinition>): AgentDefinition {
  return { name: 'reviewer', description: 'd', systemPrompt: 'p', filePath: '<test>', ...extra };
}

describe('agentFileNameMismatch', () => {
  it('returns null when there is no filename', () => {
    assert.equal(agentFileNameMismatch(agent({})), null);
    assert.equal(agentFileNameMismatch(agent({ fileName: '   ' })), null);
  });

  it('returns null when the filename (sans extension) matches the name', () => {
    assert.equal(agentFileNameMismatch(agent({ name: 'reviewer', fileName: 'reviewer.md' })), null);
  });

  it('returns the filename-derived type when it diverges from the name', () => {
    assert.equal(
      agentFileNameMismatch(agent({ name: 'reviewer', fileName: 'code-checker.md' })),
      'code-checker',
    );
  });

  it('strips only a single trailing extension and keeps casing', () => {
    assert.equal(agentFileNameMismatch(agent({ name: 'x', fileName: 'My-Agent.md' })), 'My-Agent');
  });

  it('returns null for an extension-only filename with no base', () => {
    assert.equal(agentFileNameMismatch(agent({ name: 'x', fileName: '.md' })), null);
  });
});
