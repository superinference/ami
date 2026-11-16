// Behavioral coverage for the agent-availability + preload schema added for
// Claude parity: requiredMcpServers gating (hasRequiredMcpServers /
// filterAgentsByMcpRequirements), per-agent skills preload + allowedAgentTypes
// parsing/enforcement, per-subagent mcpServers field, and the forward-compatible
// SAFE_SKILL_PROPERTIES allowlist (skillHasOnlySafeProperties). Exercises the
// REAL exported functions in core/src/skills.ts against observable results.

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  parseAgentSkills,
  parseAllowedAgentTypes,
  canSpawnAgentType,
  hasRequiredMcpServers,
  filterAgentsByMcpRequirements,
  SAFE_SKILL_PROPERTIES,
  skillHasOnlySafeProperties,
  validateAgentDefinition,
  MAX_SKILL_ARG_NAMES,
  MAX_SKILL_ARG_NAME_LENGTH,
  type AgentDefinition,
} from '../src/skills';

const base: AgentDefinition = {
  name: 'worker',
  description: 'd',
  systemPrompt: 'do things',
  filePath: '<test>',
};

describe('parseAgentSkills / parseAllowedAgentTypes', () => {
  it('splits a comma/whitespace string and trims entries', () => {
    assert.deepEqual(parseAgentSkills('alpha, beta  gamma'), ['alpha', 'beta', 'gamma']);
    assert.deepEqual(parseAllowedAgentTypes('worker , researcher'), ['worker', 'researcher']);
  });

  it('accepts a string array and coerces non-strings', () => {
    assert.deepEqual(parseAgentSkills(['a', 'b']), ['a', 'b']);
    assert.deepEqual(parseAllowedAgentTypes([1 as unknown as string, 'x']), ['1', 'x']);
  });

  it('de-duplicates and drops empties', () => {
    assert.deepEqual(parseAgentSkills('a, a, , b'), ['a', 'b']);
  });

  it('keeps numeric-only names (unlike argument names)', () => {
    assert.deepEqual(parseAgentSkills('42, name'), ['42', 'name']);
  });

  it('drops over-length names and caps the count', () => {
    const long = 'x'.repeat(MAX_SKILL_ARG_NAME_LENGTH + 1);
    assert.deepEqual(parseAgentSkills([long, 'ok']), ['ok']);
    const many = Array.from({ length: MAX_SKILL_ARG_NAMES + 10 }, (_, i) => `s${i}`);
    assert.equal(parseAgentSkills(many).length, MAX_SKILL_ARG_NAMES);
  });

  it('returns [] for non-string/non-array values', () => {
    assert.deepEqual(parseAgentSkills(undefined), []);
    assert.deepEqual(parseAgentSkills(42), []);
    assert.deepEqual(parseAllowedAgentTypes({}), []);
    assert.deepEqual(parseAllowedAgentTypes(null), []);
  });
});

describe('canSpawnAgentType', () => {
  it('allows any type when unrestricted (undefined or empty)', () => {
    assert.equal(canSpawnAgentType({}, 'anything'), true);
    assert.equal(canSpawnAgentType({ allowedAgentTypes: [] }, 'anything'), true);
  });

  it('enforces the allowlist when present', () => {
    const a = { allowedAgentTypes: ['worker', 'researcher'] };
    assert.equal(canSpawnAgentType(a, 'worker'), true);
    assert.equal(canSpawnAgentType(a, 'intruder'), false);
  });
});

describe('hasRequiredMcpServers', () => {
  it('is available when there are no requirements', () => {
    assert.equal(hasRequiredMcpServers({}, []), true);
    assert.equal(hasRequiredMcpServers({ requiredMcpServers: [] }, []), true);
  });

  it('requires every pattern to match some server (case-insensitive substring)', () => {
    const agent = { requiredMcpServers: ['Kali', 'github'] };
    assert.equal(hasRequiredMcpServers(agent, ['kali-tools', 'github-mcp']), true);
    assert.equal(hasRequiredMcpServers(agent, ['kali-tools']), false); // github missing
    assert.equal(hasRequiredMcpServers(agent, []), false);
  });
});

describe('filterAgentsByMcpRequirements', () => {
  it('drops agents whose required servers are unavailable', () => {
    const agents: AgentDefinition[] = [
      { ...base, name: 'free' },
      { ...base, name: 'needs-kali', requiredMcpServers: ['kali'] },
      { ...base, name: 'needs-missing', requiredMcpServers: ['nonesuch'] },
    ];
    const kept = filterAgentsByMcpRequirements(agents, ['kali-tools']);
    assert.deepEqual(kept.map((a) => a.name), ['free', 'needs-kali']);
  });
});

describe('SAFE_SKILL_PROPERTIES / skillHasOnlySafeProperties', () => {
  it('lists known-safe property names', () => {
    for (const key of ['name', 'description', 'version', 'aliases', 'model', 'effort']) {
      assert.equal(SAFE_SKILL_PROPERTIES.has(key), true, key);
    }
  });

  it('returns true when all properties are safe', () => {
    assert.equal(
      skillHasOnlySafeProperties({ name: 'x', description: 'y', version: '1', aliases: ['a'] }),
      true,
    );
  });

  it('tolerates an unlisted property only when it carries no meaningful value', () => {
    assert.equal(skillHasOnlySafeProperties({ name: 'x', danger: undefined }), true);
    assert.equal(skillHasOnlySafeProperties({ name: 'x', danger: null }), true);
    assert.equal(skillHasOnlySafeProperties({ name: 'x', danger: [] }), true);
    assert.equal(skillHasOnlySafeProperties({ name: 'x', danger: {} }), true);
  });

  it('returns false for an unlisted property with a meaningful value', () => {
    assert.equal(skillHasOnlySafeProperties({ name: 'x', beforeRun: 'rm -rf /' }), false);
    assert.equal(skillHasOnlySafeProperties({ name: 'x', extra: { a: 1 } }), false);
    assert.equal(skillHasOnlySafeProperties({ name: 'x', extra: [1] }), false);
    assert.equal(skillHasOnlySafeProperties({ name: 'x', flag: false }), false);
  });
});

describe('validateAgentDefinition — skills + allowedAgentTypes', () => {
  it('warns on unknown preload skills when knownSkills is supplied', () => {
    const res = validateAgentDefinition(
      { ...base, skills: ['known', 'missing'] },
      { knownSkills: ['known'] },
    );
    assert.equal(res.valid, true);
    assert.ok(res.warnings.some((w) => w.includes('Unknown preload skill') && w.includes('missing')));
    assert.ok(!res.warnings.some((w) => w.includes('"known"')));
  });

  it('warns on unknown allowed agent types when knownAgentTypes is supplied', () => {
    const res = validateAgentDefinition(
      { ...base, allowedAgentTypes: ['worker', 'ghost'] },
      { knownAgentTypes: ['worker'] },
    );
    assert.ok(res.warnings.some((w) => w.includes('Unknown allowed agent type') && w.includes('ghost')));
  });

  it('does not warn about lists when no allowlist is supplied', () => {
    const res = validateAgentDefinition({ ...base, skills: ['x'], allowedAgentTypes: ['y'] });
    assert.equal(res.warnings.length, 0);
  });
});
