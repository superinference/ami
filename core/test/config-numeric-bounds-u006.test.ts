import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { validateEngineConfig, sanitizeEngineConfig } from '../src/types';
import { validateProjectConfigValues } from '../src/config';
import type { ProjectConfig } from '../src/config';

// U006 — numeric boundary enforcement on maxWebSearchesPerSession,
// bashOutputMaxChars and taskOutputMaxChars (parity: the reference's numeric
// settings min/max/nonnegative/int enforcement). Before this unit these three
// EngineConfig numeric knobs flowed past validateEngineConfig unchecked.

describe('U006 validateEngineConfig — maxWebSearchesPerSession bounds', () => {
  it('accepts a valid positive integer', () => {
    const r = validateEngineConfig({ maxWebSearchesPerSession: 200 });
    assert.equal(r.valid, true);
    assert.equal(r.warnings.length, 0);
  });

  it('rejects zero', () => {
    const r = validateEngineConfig({ maxWebSearchesPerSession: 0 });
    assert.equal(r.valid, false);
    assert.ok(r.warnings.some(w => w.includes('maxWebSearchesPerSession')));
  });

  it('rejects a negative value', () => {
    const r = validateEngineConfig({ maxWebSearchesPerSession: -5 });
    assert.equal(r.valid, false);
    assert.ok(r.warnings.some(w => w.includes('maxWebSearchesPerSession')));
  });

  it('rejects a non-integer value', () => {
    const r = validateEngineConfig({ maxWebSearchesPerSession: 2.5 });
    assert.equal(r.valid, false);
    assert.ok(r.warnings.some(w => w.includes('maxWebSearchesPerSession') && w.includes('integer')));
  });

  it('warns (but stays valid) above the soft maximum', () => {
    const r = validateEngineConfig({ maxWebSearchesPerSession: 10_001 });
    assert.equal(r.valid, true);
    assert.ok(r.warnings.some(w => w.includes('maxWebSearchesPerSession')));
  });
});

describe('U006 validateEngineConfig — bashOutputMaxChars / taskOutputMaxChars bounds', () => {
  for (const field of ['bashOutputMaxChars', 'taskOutputMaxChars'] as const) {
    it(`accepts a valid positive integer for ${field}`, () => {
      const r = validateEngineConfig({ [field]: 50_000 });
      assert.equal(r.valid, true);
      assert.equal(r.warnings.length, 0);
    });

    it(`rejects zero for ${field}`, () => {
      const r = validateEngineConfig({ [field]: 0 });
      assert.equal(r.valid, false);
      assert.ok(r.warnings.some(w => w.includes(field)));
    });

    it(`rejects a negative value for ${field}`, () => {
      const r = validateEngineConfig({ [field]: -1 });
      assert.equal(r.valid, false);
      assert.ok(r.warnings.some(w => w.includes(field)));
    });

    it(`rejects a non-integer value for ${field}`, () => {
      const r = validateEngineConfig({ [field]: 1024.5 });
      assert.equal(r.valid, false);
      assert.ok(r.warnings.some(w => w.includes(field) && w.includes('integer')));
    });

    it(`warns (but stays valid) above the soft maximum for ${field}`, () => {
      const r = validateEngineConfig({ [field]: 10_000_001 });
      assert.equal(r.valid, true);
      assert.ok(r.warnings.some(w => w.includes(field)));
    });
  }
});

describe('U006 sanitizeEngineConfig — clamps the new numeric knobs', () => {
  it('clamps maxWebSearchesPerSession up to the minimum and rounds', () => {
    const out = sanitizeEngineConfig({ maxWebSearchesPerSession: 0 });
    assert.equal(out.maxWebSearchesPerSession, 1);
    const rounded = sanitizeEngineConfig({ maxWebSearchesPerSession: 9.7 });
    assert.equal(rounded.maxWebSearchesPerSession, 10);
  });

  it('clamps bashOutputMaxChars / taskOutputMaxChars to >= 1 and rounds', () => {
    const out = sanitizeEngineConfig({ bashOutputMaxChars: -10, taskOutputMaxChars: 3.4 });
    assert.equal(out.bashOutputMaxChars, 1);
    assert.equal(out.taskOutputMaxChars, 3);
  });

  it('leaves valid values untouched', () => {
    const out = sanitizeEngineConfig({ maxWebSearchesPerSession: 200, bashOutputMaxChars: 50_000, taskOutputMaxChars: 50_000 });
    assert.equal(out.maxWebSearchesPerSession, 200);
    assert.equal(out.bashOutputMaxChars, 50_000);
    assert.equal(out.taskOutputMaxChars, 50_000);
  });
});

describe('U006 validateProjectConfigValues — surfaces the new numeric ranges per field', () => {
  it('flags an out-of-range maxWebSearchesPerSession from untrusted config.json', () => {
    const cfg = { maxWebSearchesPerSession: 0 } as ProjectConfig;
    const issues = validateProjectConfigValues(cfg);
    assert.ok(issues.some(i => i.field === 'maxWebSearchesPerSession'), 'expected a maxWebSearchesPerSession issue');
  });

  it('flags a non-integer bashOutputMaxChars', () => {
    const cfg = { bashOutputMaxChars: 1.5 } as ProjectConfig;
    const issues = validateProjectConfigValues(cfg);
    assert.ok(issues.some(i => i.field === 'bashOutputMaxChars'), 'expected a bashOutputMaxChars issue');
  });

  it('flags a negative taskOutputMaxChars', () => {
    const cfg = { taskOutputMaxChars: -1 } as ProjectConfig;
    const issues = validateProjectConfigValues(cfg);
    assert.ok(issues.some(i => i.field === 'taskOutputMaxChars'), 'expected a taskOutputMaxChars issue');
  });

  it('does not flag a non-number (base-type guard runs first, no range check)', () => {
    const cfg = { maxWebSearchesPerSession: 'nope' as unknown as number } as ProjectConfig;
    const issues = validateProjectConfigValues(cfg);
    const ranged = issues.filter(i => i.field === 'maxWebSearchesPerSession' && i.message.includes('must be'));
    // exactly one issue (the base-type one), never a second range issue on the string
    assert.equal(issues.filter(i => i.field === 'maxWebSearchesPerSession').length, 1);
    assert.ok(ranged.length >= 1);
  });

  it('accepts valid values with no issues', () => {
    const cfg = { maxWebSearchesPerSession: 200, bashOutputMaxChars: 50_000, taskOutputMaxChars: 50_000 } as ProjectConfig;
    const issues = validateProjectConfigValues(cfg);
    assert.equal(issues.filter(i => ['maxWebSearchesPerSession', 'bashOutputMaxChars', 'taskOutputMaxChars'].includes(i.field)).length, 0);
  });
});
