import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  parseEnvInt,
  validateBoundedEnvInt,
  collectEnvLimitDiagnostics,
  AMI_ENV_INT_LIMITS,
} from '../src/utils/env';

describe('parseEnvInt', () => {
  it('parses plain integers', () => {
    assert.equal(parseEnvInt('42', 0), 42);
    assert.equal(parseEnvInt('1000', 0), 1000);
    assert.equal(parseEnvInt('0', 5), 0);
  });

  it('parses scientific notation', () => {
    assert.equal(parseEnvInt('1e6', 0), 1_000_000);
    assert.equal(parseEnvInt('5e3', 0), 5000);
    assert.equal(parseEnvInt('2.5e2', 0), 250);
  });

  it('parses digit separators', () => {
    assert.equal(parseEnvInt('64_000', 0), 64000);
    assert.equal(parseEnvInt('1_000_000', 0), 1_000_000);
    assert.equal(parseEnvInt('10_00', 0), 1000);
  });

  it('truncates fractional values', () => {
    assert.equal(parseEnvInt('3.7', 0), 3);
    assert.equal(parseEnvInt('9.99', 0), 9);
  });

  it('returns fallback for undefined', () => {
    assert.equal(parseEnvInt(undefined, 42), 42);
  });

  it('returns fallback for empty string', () => {
    assert.equal(parseEnvInt('', 10), 10);
  });

  it('returns fallback for non-numeric string', () => {
    assert.equal(parseEnvInt('fast', 10), 10);
    assert.equal(parseEnvInt('abc', 5), 5);
  });

  it('returns fallback for NaN', () => {
    assert.equal(parseEnvInt('NaN', 7), 7);
  });

  it('returns fallback for Infinity', () => {
    assert.equal(parseEnvInt('Infinity', 8), 8);
    assert.equal(parseEnvInt('-Infinity', 8), 8);
  });

  it('handles negative values', () => {
    assert.equal(parseEnvInt('-5', 0), -5);
  });

  it('rejects out-of-range values back to the fallback with bounds', () => {
    assert.equal(parseEnvInt('5', 20, { min: 1, max: 1000 }), 5);
    assert.equal(parseEnvInt('0', 20, { min: 1, max: 1000 }), 20); // below min
    assert.equal(parseEnvInt('99999', 20, { min: 1, max: 1000 }), 20); // above max
    assert.equal(parseEnvInt('-3', 20, { min: 1, max: 1000 }), 20); // negative
    assert.equal(parseEnvInt('nonsense', 20, { min: 1, max: 1000 }), 20);
  });

  it('applies only the provided bound edge', () => {
    assert.equal(parseEnvInt('500', 1, { min: 10 }), 500); // no max → unbounded above
    assert.equal(parseEnvInt('5', 1, { min: 10 }), 1); // below min only
    assert.equal(parseEnvInt('500', 1, { max: 100 }), 1); // above max only
    assert.equal(parseEnvInt('50', 1, { max: 100 }), 50);
  });

  it('treats inclusive bounds as inclusive', () => {
    assert.equal(parseEnvInt('1', 99, { min: 1, max: 1000 }), 1);
    assert.equal(parseEnvInt('1000', 99, { min: 1, max: 1000 }), 1000);
  });
});

describe('validateBoundedEnvInt', () => {
  it('returns the default with valid status when unset or empty', () => {
    assert.deepEqual(validateBoundedEnvInt('X', undefined, 10, 100), { effective: 10, status: 'valid' });
    assert.deepEqual(validateBoundedEnvInt('X', '', 10, 100), { effective: 10, status: 'valid' });
  });

  it('accepts an in-range value', () => {
    assert.deepEqual(validateBoundedEnvInt('X', '42', 10, 100), { effective: 42, status: 'valid' });
  });

  it('flags non-numeric input as invalid and uses the default', () => {
    const r = validateBoundedEnvInt('X', 'abc', 10, 100);
    assert.equal(r.effective, 10);
    assert.equal(r.status, 'invalid');
    assert.match(r.message!, /invalid value "abc"/);
    assert.match(r.message!, /X:/);
  });

  it('rejects values below the lower bound (default min is 1)', () => {
    assert.equal(validateBoundedEnvInt('X', '0', 10, 100).status, 'invalid');
    assert.equal(validateBoundedEnvInt('X', '-5', 10, 100).status, 'invalid');
    assert.equal(validateBoundedEnvInt('X', '0', 10, 100).effective, 10);
  });

  it('honors a custom min of 0 (zero is a valid sentinel)', () => {
    assert.deepEqual(validateBoundedEnvInt('X', '0', 7, 100, { min: 0 }), { effective: 0, status: 'valid' });
    assert.equal(validateBoundedEnvInt('X', '-1', 7, 100, { min: 0 }).status, 'invalid');
  });

  it('caps a value above the upper limit and reports it', () => {
    const r = validateBoundedEnvInt('X', '5000', 10, 1000);
    assert.equal(r.effective, 1000);
    assert.equal(r.status, 'capped');
    assert.match(r.message!, /capped from 5000 to 1000/);
  });

  it('parses the same grammar as parseEnvInt (separators/scientific)', () => {
    assert.deepEqual(validateBoundedEnvInt('X', '2_000', 10, 5000), { effective: 2000, status: 'valid' });
    assert.equal(validateBoundedEnvInt('X', '1e9', 10, 1000).status, 'capped');
  });
});

describe('collectEnvLimitDiagnostics', () => {
  it('returns only the non-valid (invalid/capped) entries', () => {
    const diags = collectEnvLimitDiagnostics(
      [
        { name: 'A', defaultValue: 10, upperLimit: 100 },
        { name: 'B', defaultValue: 10, upperLimit: 100 },
        { name: 'C', defaultValue: 10, upperLimit: 100 },
      ],
      { A: '50', B: 'bogus', C: '9999' },
    );
    assert.equal(diags.length, 2);
    const byName = Object.fromEntries(diags.map((d) => [d.name, d]));
    assert.equal(byName.B.status, 'invalid');
    assert.equal(byName.C.status, 'capped');
    assert.equal(byName.C.effective, 100);
    assert.ok(!('A' in byName));
  });

  it('returns an empty array when all overrides are in range or unset', () => {
    assert.deepEqual(
      collectEnvLimitDiagnostics([{ name: 'A', defaultValue: 10, upperLimit: 100 }], {}),
      [],
    );
  });

  it('defaults to the AMI env-int catalog and the live process env', () => {
    const orig = process.env.SUPERINFERENCE_MAX_TOOL_CONCURRENCY;
    try {
      process.env.SUPERINFERENCE_MAX_TOOL_CONCURRENCY = '-1';
      const diags = collectEnvLimitDiagnostics();
      assert.ok(diags.some((d) => d.name === 'SUPERINFERENCE_MAX_TOOL_CONCURRENCY' && d.status === 'invalid'));
    } finally {
      if (orig === undefined) delete process.env.SUPERINFERENCE_MAX_TOOL_CONCURRENCY;
      else process.env.SUPERINFERENCE_MAX_TOOL_CONCURRENCY = orig;
    }
  });

  it('exposes a catalog that honors min:0 for subagent spawn depth', () => {
    const depth = AMI_ENV_INT_LIMITS.find((s) => s.name === 'CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH');
    assert.equal(depth?.min, 0);
    // A legitimate 0 depth must not be flagged.
    assert.deepEqual(
      collectEnvLimitDiagnostics(AMI_ENV_INT_LIMITS, { CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH: '0' }),
      [],
    );
  });
});
