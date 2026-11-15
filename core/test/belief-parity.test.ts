import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { BeliefTracker } from '../src/superinference/belief';

// ---------------------------------------------------------------------------
// belief-parity: correctness/robustness hardening of the SuperInference POMDP
// belief tracker. Each block documents the RED (pre-fix) behavior it catches so
// the test is a genuine red-green reproducer, not a tautology.
//
// These assertions FAIL on the unhardened belief.ts and PASS with the hardening:
//   1. clampBelief: non-finite belief collapses to the 0.25 floor, never NaN.
//   2. update(): a non-finite critic score is treated as the default 1.0.
//   3. entropy(): a non-finite argument returns 0, not NaN.
//   4. eig(): a non-finite intermediate yields a finite, non-negative EIG.
//   5. validation: non-finite λ+/λ-/κ are rejected with a RangeError.
// ---------------------------------------------------------------------------

// Fix 1 — NaN-safe belief clamp -------------------------------------------------
describe('belief-parity: NaN-safe belief clamp', () => {
  it('non-finite initialBelief collapses to the 0.25 floor (RED: was NaN)', () => {
    for (const bad of [NaN, Infinity, -Infinity]) {
      const tracker = new BeliefTracker({ enabled: true, initialBelief: bad });
      assert.ok(!Number.isNaN(tracker.belief), `belief was NaN for initialBelief=${bad}`);
      assert.equal(tracker.belief, 0.25);
    }
  });

  it('reset() after a non-finite initialBelief also yields a finite belief', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: NaN });
    tracker.update(true, 0.8);
    tracker.reset();
    assert.ok(Number.isFinite(tracker.belief));
    assert.equal(tracker.belief, 0.25);
  });

  it('finite initialBelief is unchanged by the hardened clamp (no regression)', () => {
    assert.equal(new BeliefTracker({ enabled: true, initialBelief: 0.5 }).belief, 0.5);
    assert.equal(new BeliefTracker({ enabled: true, initialBelief: 0.0 }).belief, 0.25);
    assert.equal(new BeliefTracker({ enabled: true, initialBelief: 1.0 }).belief, 0.95);
  });
});

// Fix 2 — non-finite critic score ----------------------------------------------
describe('belief-parity: non-finite critic score treated as default', () => {
  it('update(true, NaN) matches update(true) default 1.0 (RED: was corrupted)', () => {
    const nan = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    const def = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    nan.update(true, NaN);
    def.update(true);
    assert.ok(Number.isFinite(nan.belief), 'belief NaN after NaN score');
    // b = 0.5 + 0.35*(1.0 - 0.5) = 0.675 — NOT the 0.25 floor a bare NaN would clamp to.
    assert.ok(Math.abs(nan.belief - 0.675) < 1e-9, `expected 0.675, got ${nan.belief}`);
    assert.equal(nan.belief, def.belief);
  });

  it('update(true, Infinity) also falls back to the default score', () => {
    const inf = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    inf.update(true, Infinity);
    assert.ok(Math.abs(inf.belief - 0.675) < 1e-9, `expected 0.675, got ${inf.belief}`);
  });

  it('finite scores still clamp to [0,1] as before (no regression)', () => {
    const hi = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    hi.update(true, 5.0);
    assert.ok(Math.abs(hi.belief - 0.675) < 1e-9);
    const lo = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    lo.update(true, -1.0);
    assert.ok(Math.abs(lo.belief - 0.325) < 1e-9);
  });
});

// Fix 3 — entropy non-finite guard ---------------------------------------------
describe('belief-parity: entropy non-finite guard', () => {
  it('entropy(NaN) returns 0, not NaN (RED: bare <=0/>=1 guard missed NaN)', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    const h = tracker.entropy(NaN);
    assert.ok(!Number.isNaN(h), 'entropy(NaN) returned NaN');
    assert.equal(h, 0);
  });

  it('entropy on a tracker whose belief went non-finite stays finite', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: NaN });
    assert.ok(Number.isFinite(tracker.entropy()));
  });

  it('entropy of a valid belief is unchanged (no regression)', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    assert.ok(Math.abs(tracker.entropy(0.5) - 1.0) < 1e-9);
    assert.equal(tracker.entropy(0), 0);
    assert.equal(tracker.entropy(1), 0);
  });
});

// Fix 4 — eig finite guarantee -------------------------------------------------
describe('belief-parity: eig finite guarantee', () => {
  it('eig() stays finite when critic α is non-finite (RED: was NaN)', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5, criticAlpha: NaN });
    const e = tracker.eig();
    assert.ok(Number.isFinite(e), `eig() was non-finite: ${e}`);
    assert.ok(e >= 0);
  });

  it('getState().eig is finite under a non-finite critic β (RED: snapshot poisoned)', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5, criticBeta: NaN });
    const state = tracker.getState();
    assert.ok(Number.isFinite(state.eig), `state.eig was non-finite: ${state.eig}`);
  });

  it('eig() for normal params is unchanged (no regression)', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    assert.ok(Math.abs(tracker.eig() - 0.1052) < 0.005);
  });
});

// Fix 5 — validation rejects non-finite rates/threshold ------------------------
describe('belief-parity: validation rejects non-finite params', () => {
  it('constructor rejects non-finite lambdaPlus (RED: NaN slipped past)', () => {
    for (const bad of [NaN, Infinity, -Infinity]) {
      assert.throws(
        () => new BeliefTracker({ enabled: true, lambdaPlus: bad }),
        RangeError,
        `lambdaPlus=${bad} should throw`,
      );
    }
  });

  it('constructor rejects non-finite lambdaMinus (RED: NaN slipped past)', () => {
    for (const bad of [NaN, Infinity, -Infinity]) {
      assert.throws(
        () => new BeliefTracker({ enabled: true, lambdaMinus: bad }),
        RangeError,
        `lambdaMinus=${bad} should throw`,
      );
    }
  });

  it('applyRates rejects a non-finite rate (RED: NaN slipped past)', () => {
    const tracker = new BeliefTracker({ enabled: true });
    assert.throws(() => tracker.applyRates({ lambdaPlus: NaN, lambdaMinus: 0.6 }), RangeError);
    assert.throws(() => tracker.applyRates({ lambdaPlus: 0.35, lambdaMinus: Infinity }), RangeError);
  });

  it('applyThreshold rejects a non-finite κ (RED: NaN disabled confidence stop)', () => {
    const tracker = new BeliefTracker({ enabled: true });
    assert.throws(() => tracker.applyThreshold(NaN), RangeError);
    assert.throws(() => tracker.applyThreshold(Infinity), RangeError);
  });

  it('valid finite params still pass (no regression)', () => {
    assert.doesNotThrow(() => new BeliefTracker({ enabled: true, lambdaPlus: 1.0, lambdaMinus: 0.5 }));
    const tracker = new BeliefTracker({ enabled: true });
    assert.doesNotThrow(() => tracker.applyRates({ lambdaPlus: 0.4, lambdaMinus: 0.55 }));
    assert.doesNotThrow(() => tracker.applyThreshold(0.8));
  });
});
