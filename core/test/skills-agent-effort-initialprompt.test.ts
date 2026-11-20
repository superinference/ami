// Unit coverage for the agent-definition schema fields added for the reference parity:
// `effort` (a thinking-effort level OR a non-negative integer) and `initialPrompt`
// (text prepended to the agent's first user turn). Exercises the REAL exported
// AgentDefinition type + validateAgentDefinition warnings in core/src/skills.ts.

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  validateAgentDefinition,
  EFFORT_LEVELS,
  type AgentDefinition,
} from '../src/skills';

const base: AgentDefinition = {
  name: 'a',
  description: 'd',
  systemPrompt: 'be an agent',
  filePath: '/x.md',
};

describe('AgentDefinition.effort / initialPrompt — type surface', () => {
  it('accepts a named level and an integer-string effort plus an initialPrompt', () => {
    const def: AgentDefinition = { ...base, effort: 'high', initialPrompt: 'always lint first' };
    assert.equal(def.effort, 'high');
    assert.equal(def.initialPrompt, 'always lint first');
    const intDef: AgentDefinition = { ...base, effort: '2048' };
    assert.equal(intDef.effort, '2048');
  });
});

describe('validateAgentDefinition — effort', () => {
  it('accepts every named effort level without a warning', () => {
    for (const level of EFFORT_LEVELS) {
      const res = validateAgentDefinition({ ...base, effort: level });
      assert.equal(res.valid, true);
      assert.deepEqual(res.warnings.filter((w) => w.includes('effort')), []);
    }
  });

  it('accepts a non-negative integer effort without a warning', () => {
    const res = validateAgentDefinition({ ...base, effort: '4096' });
    assert.equal(res.valid, true);
    assert.deepEqual(res.warnings.filter((w) => w.includes('effort')), []);
  });

  it('warns on an unrecognized effort value but keeps the agent loadable', () => {
    const res = validateAgentDefinition({ ...base, effort: 'turbo' });
    assert.equal(res.valid, true);
    assert.ok(res.warnings.some((w) => w.includes('Invalid effort "turbo"')), res.warnings.join('|'));
  });

  it('warns on a negative integer effort', () => {
    const res = validateAgentDefinition({ ...base, effort: '-5' });
    assert.ok(res.warnings.some((w) => w.includes('Invalid effort')), res.warnings.join('|'));
  });

  it('does not warn when effort is absent', () => {
    const res = validateAgentDefinition({ ...base });
    assert.deepEqual(res.warnings.filter((w) => w.includes('effort')), []);
  });
});
