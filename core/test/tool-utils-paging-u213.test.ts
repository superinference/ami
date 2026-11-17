import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { plural, extractNonNegativeInt } from '../src/tools/tool-utils';

describe('plural (U213)', () => {
  it('returns the singular word for 1', () => {
    assert.equal(plural(1, 'symbol'), 'symbol');
  });
  it('returns the default +s plural for non-1 counts', () => {
    assert.equal(plural(0, 'symbol'), 'symbols');
    assert.equal(plural(2, 'symbol'), 'symbols');
  });
  it('honors an explicit irregular plural', () => {
    assert.equal(plural(3, 'index', 'indices'), 'indices');
    assert.equal(plural(1, 'index', 'indices'), 'index');
  });
});

describe('extractNonNegativeInt (U213)', () => {
  it('returns the fallback when the key is undefined', () => {
    assert.deepEqual(extractNonNegativeInt(undefined, 'limit', 30), { value: 30 });
  });
  it('returns the fallback when the key is null', () => {
    assert.deepEqual(extractNonNegativeInt(null, 'offset', 0), { value: 0 });
  });
  it('accepts a valid non-negative integer', () => {
    assert.deepEqual(extractNonNegativeInt(7, 'limit', 30), { value: 7 });
    assert.deepEqual(extractNonNegativeInt(0, 'limit', 30), { value: 0 });
  });
  it('rejects a non-number with a validation error', () => {
    const r = extractNonNegativeInt('5', 'limit', 30);
    assert.ok(r.error?.isError);
    assert.match(r.error!.output, /Invalid "limit".*non-negative integer/);
  });
  it('rejects a fractional number', () => {
    const r = extractNonNegativeInt(2.5, 'offset', 0);
    assert.ok(r.error?.isError);
  });
  it('rejects a negative number', () => {
    const r = extractNonNegativeInt(-1, 'limit', 30);
    assert.ok(r.error?.isError);
  });
});
