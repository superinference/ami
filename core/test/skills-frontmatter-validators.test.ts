// Behavioral coverage for the frontmatter coercion/validation building blocks
// in core/src/skills.ts: coerceDescriptionToString, parseEffort / isValidEffort,
// parseArgumentNames, and validateAgentDefinition. Each assertion exercises the
// REAL exported function against an observable result; no source-text scraping.

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  coerceDescriptionToString,
  EFFORT_LEVELS,
  parseEffort,
  isValidEffort,
  parseArgumentNames,
  AGENT_PERMISSION_MODES,
  validateAgentDefinition,
  MAX_SKILL_ARG_NAMES,
  MAX_SKILL_ARG_NAME_LENGTH,
  type AgentDefinition,
} from '../src/skills';

describe('coerceDescriptionToString', () => {
  it('trims a non-empty string', () => {
    assert.equal(coerceDescriptionToString('  hello  '), 'hello');
  });

  it('returns null for an empty string', () => {
    assert.equal(coerceDescriptionToString(''), null);
  });

  it('returns null for a whitespace-only string', () => {
    assert.equal(coerceDescriptionToString('   \n\t'), null);
  });

  it('stringifies a finite number', () => {
    assert.equal(coerceDescriptionToString(42), '42');
    assert.equal(coerceDescriptionToString(0), '0');
  });

  it('returns null for NaN / Infinity', () => {
    assert.equal(coerceDescriptionToString(NaN), null);
    assert.equal(coerceDescriptionToString(Infinity), null);
    assert.equal(coerceDescriptionToString(-Infinity), null);
  });

  it('stringifies a boolean', () => {
    assert.equal(coerceDescriptionToString(true), 'true');
    assert.equal(coerceDescriptionToString(false), 'false');
  });

  it('returns null for arrays, objects, null, and undefined', () => {
    assert.equal(coerceDescriptionToString(['a', 'b']), null);
    assert.equal(coerceDescriptionToString({ a: 1 }), null);
    assert.equal(coerceDescriptionToString(null), null);
    assert.equal(coerceDescriptionToString(undefined), null);
  });
});

describe('EFFORT_LEVELS / parseEffort / isValidEffort', () => {
  it('exposes the canonical level list', () => {
    assert.deepEqual([...EFFORT_LEVELS], ['low', 'medium', 'high', 'xhigh', 'max']);
  });

  it('accepts each named level and normalizes case + whitespace', () => {
    for (const level of EFFORT_LEVELS) {
      assert.equal(parseEffort(level), level);
    }
    assert.equal(parseEffort('  HIGH '), 'high');
    assert.equal(parseEffort('Max'), 'max');
  });

  it('accepts a non-negative integer number', () => {
    assert.equal(parseEffort(0), '0');
    assert.equal(parseEffort(7), '7');
  });

  it('accepts a non-negative integer string', () => {
    assert.equal(parseEffort('12'), '12');
  });

  it('rejects negative / non-integer numbers', () => {
    assert.equal(parseEffort(-1), undefined);
    assert.equal(parseEffort(1.5), undefined);
    assert.equal(parseEffort(NaN), undefined);
  });

  it('rejects unrecognized strings', () => {
    assert.equal(parseEffort('turbo'), undefined);
    assert.equal(parseEffort(''), undefined);
    assert.equal(parseEffort('-5'), undefined);
  });

  it('rejects non-string / non-number values', () => {
    assert.equal(parseEffort(undefined), undefined);
    assert.equal(parseEffort(null), undefined);
    assert.equal(parseEffort({}), undefined);
    assert.equal(parseEffort(true), undefined);
  });

  it('isValidEffort mirrors parseEffort success/failure', () => {
    assert.equal(isValidEffort('low'), true);
    assert.equal(isValidEffort(3), true);
    assert.equal(isValidEffort('nope'), false);
    assert.equal(isValidEffort({}), false);
  });
});

describe('parseArgumentNames', () => {
  it('splits a single string on commas and whitespace', () => {
    assert.deepEqual(parseArgumentNames('file, line  mode'), ['file', 'line', 'mode']);
  });

  it('accepts a string array', () => {
    assert.deepEqual(parseArgumentNames(['a', 'b']), ['a', 'b']);
  });

  it('stringifies non-string array entries', () => {
    assert.deepEqual(parseArgumentNames([true, 'x']), ['true', 'x']);
  });

  it('drops empty and whitespace-only names', () => {
    assert.deepEqual(parseArgumentNames(['', '   ', 'keep']), ['keep']);
    assert.deepEqual(parseArgumentNames(',,foo,,'), ['foo']);
  });

  it('drops numeric-only names (they collide with $1,$2)', () => {
    assert.deepEqual(parseArgumentNames(['1', '2', 'name']), ['name']);
  });

  it('de-duplicates names, preserving first occurrence', () => {
    assert.deepEqual(parseArgumentNames(['a', 'a', 'b']), ['a', 'b']);
  });

  it('drops over-long names', () => {
    const longName = 'x'.repeat(MAX_SKILL_ARG_NAME_LENGTH + 1);
    assert.deepEqual(parseArgumentNames([longName, 'ok']), ['ok']);
  });

  it('caps the count at MAX_SKILL_ARG_NAMES', () => {
    const many = Array.from({ length: MAX_SKILL_ARG_NAMES + 10 }, (_, i) => `n${i}`);
    const result = parseArgumentNames(many);
    assert.equal(result.length, MAX_SKILL_ARG_NAMES);
  });

  it('returns [] for non-string / non-array values', () => {
    assert.deepEqual(parseArgumentNames(undefined), []);
    assert.deepEqual(parseArgumentNames(null), []);
    assert.deepEqual(parseArgumentNames(42), []);
    assert.deepEqual(parseArgumentNames({ a: 1 }), []);
  });
});

describe('validateAgentDefinition', () => {
  const base: Partial<AgentDefinition> = {
    name: 'reviewer',
    systemPrompt: 'You review code.',
  };

  it('exposes the permission-mode allowlist', () => {
    assert.deepEqual([...AGENT_PERMISSION_MODES], ['auto-allow', 'ask', 'deny-all', 'plan']);
  });

  it('accepts a minimal valid definition', () => {
    const r = validateAgentDefinition(base);
    assert.equal(r.valid, true);
    assert.equal(r.errors.length, 0);
    assert.equal(r.warnings.length, 0);
  });

  it('errors on a missing or empty name', () => {
    assert.equal(validateAgentDefinition({ systemPrompt: 'body' }).valid, false);
    const r = validateAgentDefinition({ name: '   ', systemPrompt: 'body' });
    assert.equal(r.valid, false);
    assert.ok(r.errors.some((e) => e.includes('name')));
  });

  it('errors on a missing or empty body', () => {
    assert.equal(validateAgentDefinition({ name: 'x' }).valid, false);
    const r = validateAgentDefinition({ name: 'x', systemPrompt: '  ' });
    assert.equal(r.valid, false);
    assert.ok(r.errors.some((e) => e.includes('body')));
  });

  it('warns on an invalid permission mode but stays valid', () => {
    const r = validateAgentDefinition({ ...base, permissionMode: 'wide-open' as never });
    assert.equal(r.valid, true);
    assert.ok(r.warnings.some((w) => w.includes('permission mode')));
  });

  it('accepts a valid permission mode with no warning', () => {
    const r = validateAgentDefinition({ ...base, permissionMode: 'ask' });
    assert.equal(r.warnings.length, 0);
  });

  it('warns on an unknown model when knownModels is supplied', () => {
    const r = validateAgentDefinition({ ...base, model: 'gpt-made-up' }, { knownModels: ['sonnet'] });
    assert.ok(r.warnings.some((w) => w.includes('Unknown model')));
  });

  it('accepts a known model and the literal "inherit"', () => {
    assert.equal(validateAgentDefinition({ ...base, model: 'sonnet' }, { knownModels: ['sonnet'] }).warnings.length, 0);
    assert.equal(validateAgentDefinition({ ...base, model: 'inherit' }, { knownModels: ['sonnet'] }).warnings.length, 0);
  });

  it('skips model validation when knownModels is omitted', () => {
    const r = validateAgentDefinition({ ...base, model: 'anything' });
    assert.equal(r.warnings.length, 0);
  });

  it('does not warn when knownModels is supplied but the agent sets no model', () => {
    const r = validateAgentDefinition(base, { knownModels: ['sonnet'] });
    assert.equal(r.warnings.length, 0);
  });

  it('warns on unknown tools in allow and disallow lists', () => {
    const r = validateAgentDefinition(
      { ...base, tools: ['bash', 'bogus'], disallowedTools: ['also_bogus'] },
      { knownTools: ['bash', 'file_edit'] },
    );
    assert.ok(r.warnings.some((w) => w.includes('allowlist: "bogus"')));
    assert.ok(r.warnings.some((w) => w.includes('disallowed list: "also_bogus"')));
  });

  it('accepts known tools and tolerates absent tool lists', () => {
    const r = validateAgentDefinition({ ...base, tools: ['bash'] }, { knownTools: ['bash'] });
    assert.equal(r.warnings.length, 0);
    // no tools/disallowedTools fields → the ?? [] branches
    assert.equal(validateAgentDefinition(base, { knownTools: ['bash'] }).warnings.length, 0);
  });

  it('skips tool validation when knownTools is omitted', () => {
    const r = validateAgentDefinition({ ...base, tools: ['whatever'] });
    assert.equal(r.warnings.length, 0);
  });
});
