// Behavioral coverage for the agent-definition schema fields added for Claude
// parity: color, memory scope, isolation mode, and the background flag — plus the
// validateAgentDefinition warnings for the first three. Exercises the REAL
// exported functions in core/src/skills.ts against observable results.

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  AGENT_COLORS,
  parseAgentColor,
  AGENT_MEMORY_SCOPES,
  parseMemoryScope,
  AGENT_ISOLATION_MODES,
  parseIsolationMode,
  parseBackgroundFlag,
  validateAgentDefinition,
  type AgentDefinition,
} from '../src/skills';

describe('parseAgentColor', () => {
  it('accepts every named palette color (case-insensitive, trimmed)', () => {
    for (const c of AGENT_COLORS) {
      assert.equal(parseAgentColor(c), c);
      assert.equal(parseAgentColor(`  ${c.toUpperCase()} `), c);
    }
  });

  it('maps the "automatic" sentinel to undefined (UI default)', () => {
    assert.equal(parseAgentColor('automatic'), undefined);
    assert.equal(parseAgentColor('AUTOMATIC'), undefined);
  });

  it('rejects unknown / non-string values', () => {
    assert.equal(parseAgentColor('chartreuse'), undefined);
    assert.equal(parseAgentColor(''), undefined);
    assert.equal(parseAgentColor(42), undefined);
    assert.equal(parseAgentColor(undefined), undefined);
    assert.equal(parseAgentColor(null), undefined);
    assert.equal(parseAgentColor({}), undefined);
  });
});

describe('parseMemoryScope', () => {
  it('accepts user / project / local (case-insensitive)', () => {
    for (const s of AGENT_MEMORY_SCOPES) {
      assert.equal(parseMemoryScope(s), s);
      assert.equal(parseMemoryScope(` ${s.toUpperCase()}`), s);
    }
  });

  it('rejects unknown / non-string values', () => {
    assert.equal(parseMemoryScope('session'), undefined);
    assert.equal(parseMemoryScope('none'), undefined);
    assert.equal(parseMemoryScope(''), undefined);
    assert.equal(parseMemoryScope(7), undefined);
    assert.equal(parseMemoryScope(undefined), undefined);
  });
});

describe('parseIsolationMode', () => {
  it('accepts worktree (case-insensitive)', () => {
    assert.equal(parseIsolationMode('worktree'), 'worktree');
    assert.equal(parseIsolationMode(' WorkTree '), 'worktree');
  });

  it('rejects remote (ant-only in Claude, no AMI analogue) and other values', () => {
    assert.equal(parseIsolationMode('remote'), undefined);
    assert.equal(parseIsolationMode('vm'), undefined);
    assert.equal(parseIsolationMode(''), undefined);
    assert.equal(parseIsolationMode(true), undefined);
  });

  it('exposes only worktree in the mode list', () => {
    assert.deepEqual([...AGENT_ISOLATION_MODES], ['worktree']);
  });
});

describe('parseBackgroundFlag', () => {
  it('true / "true" → true', () => {
    assert.deepEqual(parseBackgroundFlag(true), { value: true, invalid: false });
    assert.deepEqual(parseBackgroundFlag('true'), { value: true, invalid: false });
  });

  it('false / "false" / omitted → undefined (not invalid)', () => {
    assert.deepEqual(parseBackgroundFlag(false), { value: undefined, invalid: false });
    assert.deepEqual(parseBackgroundFlag('false'), { value: undefined, invalid: false });
    assert.deepEqual(parseBackgroundFlag(undefined), { value: undefined, invalid: false });
  });

  it('any other value is invalid and yields undefined', () => {
    assert.deepEqual(parseBackgroundFlag('yes'), { value: undefined, invalid: true });
    assert.deepEqual(parseBackgroundFlag(1), { value: undefined, invalid: true });
    assert.deepEqual(parseBackgroundFlag(null), { value: undefined, invalid: true });
    assert.deepEqual(parseBackgroundFlag({}), { value: undefined, invalid: true });
  });
});

describe('validateAgentDefinition — new field warnings', () => {
  // name must satisfy validateAgentType (3–50 chars, alnum/hyphen) so these cases
  // isolate the color/memory/isolation warnings from the name-format warning.
  const base: Partial<AgentDefinition> = { name: 'agent-a', systemPrompt: 'do things' };

  it('does not warn on valid color / memory / isolation', () => {
    const r = validateAgentDefinition({ ...base, color: 'blue', memory: 'project', isolation: 'worktree' });
    assert.equal(r.valid, true);
    assert.deepEqual(r.warnings, []);
  });

  it('warns on an invalid color', () => {
    const r = validateAgentDefinition({ ...base, color: 'mauve' as AgentDefinition['color'] });
    assert.equal(r.valid, true);
    assert.ok(r.warnings.some(w => /Unknown color "mauve"/.test(w)), r.warnings.join('|'));
  });

  it('warns on an invalid memory scope', () => {
    const r = validateAgentDefinition({ ...base, memory: 'session' as AgentDefinition['memory'] });
    assert.ok(r.warnings.some(w => /Unknown memory scope "session"/.test(w)), r.warnings.join('|'));
  });

  it('warns on an invalid isolation mode', () => {
    const r = validateAgentDefinition({ ...base, isolation: 'remote' as AgentDefinition['isolation'] });
    assert.ok(r.warnings.some(w => /Unknown isolation mode "remote"/.test(w)), r.warnings.join('|'));
  });
});
