import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { Reservoir, percentile, DEFAULT_RESERVOIR_SIZE } from '../src/utils/reservoir';

describe('percentile (linear interpolation)', () => {
  it('returns 0 for empty, the value for singletons', () => {
    assert.equal(percentile([], 50), 0);
    assert.equal(percentile([7], 99), 7);
  });

  it('interpolates between neighbouring ranks', () => {
    // [1,2,3,4,5]; p50 → rank 2 → 3; p25 → rank 1 → 2; p75 → rank 3 → 4
    assert.equal(percentile([5, 1, 3, 2, 4], 50), 3);
    assert.equal(percentile([1, 2, 3, 4, 5], 25), 2);
    assert.equal(percentile([1, 2, 3, 4, 5], 75), 4);
    // p10 of [1..5] → rank 0.4 → 1 + 1*0.4 = 1.4
    assert.equal(percentile([1, 2, 3, 4, 5], 10), 1.4);
  });

  it('clamps p to [0,100]', () => {
    assert.equal(percentile([1, 2, 3], -5), 1);
    assert.equal(percentile([1, 2, 3], 150), 3);
  });
});

describe('Reservoir', () => {
  it('retains all values below capacity and tracks count', () => {
    const r = new Reservoir(10);
    for (let i = 0; i < 5; i++) r.add(i);
    assert.equal(r.count, 5);
    assert.equal(r.sampleCount, 5);
    assert.deepEqual(r.values().sort((a, b) => a - b), [0, 1, 2, 3, 4]);
  });

  it('caps retained samples at size while counting all observations', () => {
    const r = new Reservoir(100);
    for (let i = 0; i < 10_000; i++) r.add(i);
    assert.equal(r.count, 10_000);
    assert.equal(r.sampleCount, 100);
  });

  it('uses a deterministic rand to exercise the replace branch', () => {
    // rand always 0 → j=0 < size → always replaces index 0
    const r = new Reservoir(2, () => 0);
    r.add(1); r.add(2); // fills reservoir
    r.add(3);           // replaces index 0 → [3,2]
    assert.deepEqual(r.values(), [3, 2]);
  });

  it('skips replacement when j >= size', () => {
    // rand always ~1 → j = floor(1 * seen) which is >= size → no replace
    const r = new Reservoir(2, () => 0.999999);
    r.add(1); r.add(2);
    r.add(3);
    assert.deepEqual(r.values(), [1, 2]);
  });

  it('computes percentiles over the sample and resets', () => {
    const r = new Reservoir(1000);
    for (let i = 1; i <= 100; i++) r.add(i);
    assert.ok(r.percentile(50) >= 49 && r.percentile(50) <= 52);
    r.reset();
    assert.equal(r.count, 0);
    assert.equal(r.percentile(50), 0);
  });

  it('defaults to DEFAULT_RESERVOIR_SIZE and floors fractional sizes', () => {
    assert.equal(DEFAULT_RESERVOIR_SIZE, 1024);
    const r = new Reservoir(2.9);
    r.add(1); r.add(2); r.add(3);
    assert.equal(r.sampleCount, 2);
  });
});
