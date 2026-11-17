// U023 (w72) — validateSkillArgs returns an ACTIONABLE verdict.
//
// Parity gap: AMI's validateSkillArgs previously hard-coded `valid: true` in every
// path, so a caller that trusts `.valid` to gate oversized/abusive invocation args
// could NEVER reject — the boolean was inert. Claude Code's comparable skill-input
// validators return a concrete negative result on a hard-cap breach. These tests
// lock in `valid: false` whenever ANY hard cap is exceeded, and `valid: true` for
// in-bounds args. RED before the skills.ts fix (the breach cases returned `true`).

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  MAX_SKILL_ARG_NAMES,
  MAX_SKILL_ARG_NAME_LENGTH,
  MAX_SKILL_ARG_VALUE_LENGTH,
  validateSkillArgs,
} from '../src/skills';

describe('validateSkillArgs — actionable verdict (U023 w72)', () => {
  it('is valid with no warnings for normal args', () => {
    const r = validateSkillArgs({ foo: 'bar', baz: 'qux' });
    assert.equal(r.valid, true);
    assert.deepEqual(r.warnings, []);
  });

  it('is valid for empty args', () => {
    const r = validateSkillArgs({});
    assert.equal(r.valid, true);
    assert.equal(r.warnings.length, 0);
  });

  it('is INVALID when there are too many argument names', () => {
    const args: Record<string, string> = {};
    for (let i = 0; i < MAX_SKILL_ARG_NAMES + 5; i++) args[`k${i}`] = 'v';
    const r = validateSkillArgs(args);
    assert.equal(r.valid, false);
    assert.ok(r.warnings.some(w => /Too many arguments/.test(w)));
  });

  it('is INVALID on an over-length argument name', () => {
    const longKey = 'k'.repeat(MAX_SKILL_ARG_NAME_LENGTH + 1);
    const r = validateSkillArgs({ [longKey]: 'v' });
    assert.equal(r.valid, false);
    assert.ok(r.warnings.some(w => /Argument name too long/.test(w)));
  });

  it('is INVALID on an over-length argument value', () => {
    const r = validateSkillArgs({ k: 'x'.repeat(MAX_SKILL_ARG_VALUE_LENGTH + 1) });
    assert.equal(r.valid, false);
    assert.ok(r.warnings.some(w => /Argument value too long/.test(w)));
  });

  it('is INVALID when the total argument size exceeds the cap', () => {
    const args: Record<string, string> = {};
    const chunk = 'z'.repeat(MAX_SKILL_ARG_VALUE_LENGTH);
    for (let i = 0; i < 20; i++) args[`k${i}`] = chunk;
    const r = validateSkillArgs(args);
    assert.equal(r.valid, false);
    assert.ok(r.warnings.some(w => /Total argument size/.test(w)));
  });

  it('is INVALID (not partially valid) when multiple caps are breached', () => {
    const args: Record<string, string> = {};
    for (let i = 0; i < MAX_SKILL_ARG_NAMES + 5; i++) {
      args['k'.repeat(MAX_SKILL_ARG_NAME_LENGTH + 1) + i] = 'v'.repeat(MAX_SKILL_ARG_VALUE_LENGTH + 1);
    }
    const r = validateSkillArgs(args);
    assert.equal(r.valid, false);
    assert.ok(r.warnings.length >= 3);
  });
});
