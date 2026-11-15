import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  Reservoir,
  percentile,
  normalizeReservoirSize,
  DEFAULT_RESERVOIR_SIZE,
} from '../src/utils/reservoir';

/**
 * Additive-superset parity hardening for reservoir sampling.
 *
 * Each block targets a specific correctness/robustness gap and is written to be
 * RED-GREEN discriminating: it fails against the pre-hardening implementation
 * and passes after the fix. All randomness in assertions is deterministic
 * (injected `rand`), never Math.random.
 */

describe('reservoir parity — size normalization (normalizeReservoirSize)', () => {
  // GAP G1: a non-finite size (NaN) made `_size = Math.max(1, Math.floor(NaN)) = NaN`,
  // so `samples.length < NaN` was always false → the reservoir retained ZERO
  // samples and every percentile collapsed to 0.
  // RED (pre-fix): sampleCount === 0, percentile(50) === 0.
  it('a NaN size no longer silently retains zero samples', () => {
    const r = new Reservoir(Number.NaN);
    for (let i = 1; i <= 10; i++) r.add(i);
    assert.equal(r.count, 10);
    assert.ok(r.sampleCount > 0, 'reservoir must retain samples with a NaN size');
    assert.ok(r.percentile(50) > 0, 'percentile must be computable');
  });

  // +Infinity size previously meant Math.floor(Infinity)=Infinity → unbounded
  // retention, defeating the bounded-memory invariant. Now capped to default.
  it('a +Infinity size is capped to a finite default (bounded memory holds)', () => {
    assert.equal(normalizeReservoirSize(Number.POSITIVE_INFINITY), DEFAULT_RESERVOIR_SIZE);
    const r = new Reservoir(Number.POSITIVE_INFINITY);
    for (let i = 0; i < DEFAULT_RESERVOIR_SIZE + 500; i++) r.add(i);
    assert.equal(r.sampleCount, DEFAULT_RESERVOIR_SIZE);
  });

  it('normalizeReservoirSize clamps finite sizes to integer ≥ 1 and honours fallback', () => {
    assert.equal(normalizeReservoirSize(2.9), 2);
    assert.equal(normalizeReservoirSize(0), 1);
    assert.equal(normalizeReservoirSize(-5), 1);
    assert.equal(normalizeReservoirSize(Number.NaN, 7), 7);
    assert.equal(normalizeReservoirSize(Number.NEGATIVE_INFINITY), DEFAULT_RESERVOIR_SIZE);
  });
});

describe('reservoir parity — non-finite observations in add()', () => {
  // GAP G3a: a NaN stored in the sample poisons the comparison-based sort in
  // percentile (NaN compares false against everything), producing a garbage/
  // order-dependent tail. NaN is now ignored (not stored, not counted).
  // RED (pre-fix): count includes the NaN and the sample contains NaN.
  it('ignores NaN observations (not stored, not counted)', () => {
    const r = new Reservoir(10);
    r.add(1);
    r.add(Number.NaN);
    r.add(3);
    assert.equal(r.count, 2);
    assert.equal(r.sampleCount, 2);
    assert.deepEqual(r.values().sort((a, b) => a - b), [1, 3]);
    assert.ok(r.values().every(Number.isFinite));
  });

  // GAP G3b: +Infinity pins the tail forever. Now ignored.
  // RED (pre-fix): sample retains Infinity → percentile(99) === Infinity.
  it('ignores ±Infinity observations so the tail is not pinned', () => {
    const r = new Reservoir(10);
    for (let i = 1; i <= 5; i++) r.add(i);
    r.add(Number.POSITIVE_INFINITY);
    r.add(Number.NEGATIVE_INFINITY);
    assert.equal(r.count, 5);
    assert.ok(r.values().every(Number.isFinite));
    assert.ok(Number.isFinite(r.percentile(99)));
    assert.equal(r.percentile(100), 5);
  });

  // Defensive index guard (`j >= 0`) — branch-coverage only, NOT a red-green
  // reproducer: a negative/NaN array index sets a harmless *named* property that
  // never surfaces via the public API (values()/sampleCount), so behaviour is
  // identical pre/post fix. The guard keeps the backing array free of phantom
  // keys and exercises the false arm of the eviction-index condition.
  it('a buggy rand returning a negative index does not corrupt the sample', () => {
    const r = new Reservoir(2, () => -0.5); // j = floor(-0.5 * seen) < 0 → skip
    r.add(1);
    r.add(2); // fills reservoir
    r.add(3); // eviction attempt with j < 0 → no write
    assert.equal(r.sampleCount, 2);
    assert.deepEqual(r.values(), [1, 2]);
    assert.ok(r.values().every(Number.isFinite));
  });
});

describe('reservoir parity — percentile() robustness', () => {
  // GAP G3c: standalone percentile must survive non-finite entries by filtering
  // them, returning the percentile of the valid observations.
  // RED (pre-fix): sort with NaN present → non-deterministic/wrong result.
  it('filters non-finite entries before computing', () => {
    // Valid values are [1,2,3,4,5]; p50 must be 3 regardless of the NaN/Inf noise.
    assert.equal(percentile([1, Number.NaN, 2, Number.POSITIVE_INFINITY, 3, 4, Number.NEGATIVE_INFINITY, 5], 50), 3);
  });

  it('an all-non-finite input collapses to the empty-sample 0', () => {
    assert.equal(percentile([Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY], 95), 0);
  });

  it('a single finite value among noise returns that value', () => {
    assert.equal(percentile([Number.NaN, 42, Number.POSITIVE_INFINITY], 99), 42);
  });

  // GAP G2: a non-finite p previously leaked NaN (Math.max/Math.min(NaN) === NaN
  // → rank NaN → result NaN). Now treated as the 0th percentile (minimum).
  // RED (pre-fix): percentile([1,2,3], NaN) is NaN.
  it('a non-finite p is treated as the 0th percentile (minimum)', () => {
    assert.equal(percentile([3, 1, 2], Number.NaN), 1);
    assert.equal(percentile([3, 1, 2], Number.POSITIVE_INFINITY), 1);
  });

  it('still clamps finite p to [0,100] and interpolates (regression guard)', () => {
    assert.equal(percentile([1, 2, 3], -5), 1);
    assert.equal(percentile([1, 2, 3], 150), 3);
    assert.equal(percentile([1, 2, 3, 4, 5], 10), 1.4);
    assert.equal(percentile([], 50), 0);
  });
});
