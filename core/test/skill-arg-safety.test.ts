import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  MAX_SKILL_ARG_NAMES,
  MAX_SKILL_ARG_NAME_LENGTH,
  MAX_SKILL_ARG_VALUE_LENGTH,
  MAX_SKILL_ARG_TOTAL_CHARS,
  sanitizeSkillArgs,
  validateSkillArgs,
} from '../src/skills';

describe('Skill argument safety constants', () => {
  it('MAX_SKILL_ARG_NAMES is 64', () => {
    assert.equal(MAX_SKILL_ARG_NAMES, 64);
  });

  it('MAX_SKILL_ARG_NAME_LENGTH is 64', () => {
    assert.equal(MAX_SKILL_ARG_NAME_LENGTH, 64);
  });

  it('MAX_SKILL_ARG_VALUE_LENGTH is 10000', () => {
    assert.equal(MAX_SKILL_ARG_VALUE_LENGTH, 10_000);
  });

  it('MAX_SKILL_ARG_TOTAL_CHARS is 100000', () => {
    assert.equal(MAX_SKILL_ARG_TOTAL_CHARS, 100_000);
  });
});

describe('sanitizeSkillArgs', () => {
  it('preserves normal args unchanged', () => {
    const args = { foo: 'bar', baz: 'qux' };
    const result = sanitizeSkillArgs(args);
    assert.deepEqual(result, { foo: 'bar', baz: 'qux' });
  });

  it('handles empty args', () => {
    const result = sanitizeSkillArgs({});
    assert.deepEqual(result, {});
  });

  it('limits arg count to MAX_SKILL_ARG_NAMES', () => {
    const args: Record<string, string> = {};
    for (let i = 0; i < 100; i++) {
      args[`key${String(i).padStart(3, '0')}`] = `val${i}`;
    }
    const result = sanitizeSkillArgs(args);
    assert.equal(Object.keys(result).length, 64);
  });

  it('truncates long keys to MAX_SKILL_ARG_NAME_LENGTH', () => {
    const longKey = 'x'.repeat(200);
    const args = { [longKey]: 'value' };
    const result = sanitizeSkillArgs(args);
    const keys = Object.keys(result);
    assert.equal(keys.length, 1);
    assert.equal(keys[0].length, 64);
  });

  it('truncates long values to MAX_SKILL_ARG_VALUE_LENGTH', () => {
    const longVal = 'y'.repeat(20_000);
    const args = { key: longVal };
    const result = sanitizeSkillArgs(args);
    assert.equal(result.key.length, 10_000);
  });

  it('stops adding args when total chars would exceed limit', () => {
    const args: Record<string, string> = {};
    // Each entry: 4-char key + 9000-char value = ~9004 chars
    // 100k / 9004 ≈ 11 entries before hitting the cap
    for (let i = 0; i < 20; i++) {
      args[`k${String(i).padStart(2, '0')}`] = 'z'.repeat(9000);
    }
    const result = sanitizeSkillArgs(args);
    const totalChars = Object.entries(result).reduce(
      (sum, [k, v]) => sum + k.length + v.length, 0,
    );
    assert.ok(totalChars <= MAX_SKILL_ARG_TOTAL_CHARS);
    assert.ok(Object.keys(result).length < 20);
  });

  it('preserves key order (takes first N)', () => {
    const args: Record<string, string> = {};
    for (let i = 0; i < 100; i++) {
      args[`key${String(i).padStart(3, '0')}`] = `val${i}`;
    }
    const result = sanitizeSkillArgs(args);
    const keys = Object.keys(result);
    assert.equal(keys[0], 'key000');
    assert.equal(keys[63], 'key063');
  });

  it('handles single oversized value that exceeds total limit', () => {
    const args = { big: 'a'.repeat(200_000) };
    const result = sanitizeSkillArgs(args);
    // Value gets truncated to 10k, which is under total limit
    assert.equal(result.big.length, 10_000);
  });
});

describe('validateSkillArgs', () => {
  it('reports no warnings for normal args', () => {
    const result = validateSkillArgs({ foo: 'bar', baz: 'qux' });
    assert.equal(result.valid, true);
    assert.equal(result.warnings.length, 0);
  });

  it('reports no warnings for empty args', () => {
    const result = validateSkillArgs({});
    assert.equal(result.valid, true);
    assert.equal(result.warnings.length, 0);
  });

  it('warns about too many args', () => {
    const args: Record<string, string> = {};
    for (let i = 0; i < 100; i++) {
      args[`key${i}`] = `val${i}`;
    }
    const result = validateSkillArgs(args);
    assert.equal(result.valid, true);
    assert.ok(result.warnings.some(w => w.includes('Too many arguments')));
  });

  it('warns about long key names', () => {
    const longKey = 'k'.repeat(100);
    const result = validateSkillArgs({ [longKey]: 'v' });
    assert.equal(result.valid, true);
    assert.ok(result.warnings.some(w => w.includes('name too long')));
  });

  it('warns about long values', () => {
    const result = validateSkillArgs({ key: 'v'.repeat(20_000) });
    assert.equal(result.valid, true);
    assert.ok(result.warnings.some(w => w.includes('value too long')));
  });

  it('warns about total size exceeding limit', () => {
    const args: Record<string, string> = {};
    for (let i = 0; i < 20; i++) {
      args[`k${i}`] = 'x'.repeat(9000);
    }
    const result = validateSkillArgs(args);
    assert.equal(result.valid, true);
    assert.ok(result.warnings.some(w => w.includes('Total argument size')));
  });

  it('can accumulate multiple warnings', () => {
    const args: Record<string, string> = {};
    for (let i = 0; i < 100; i++) {
      args['k'.repeat(100) + i] = 'v'.repeat(20_000);
    }
    const result = validateSkillArgs(args);
    assert.ok(result.warnings.length >= 3);
  });
});
