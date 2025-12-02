import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { BeliefTracker } from '../src/superinference/belief';
import { DEFAULT_CONFIG } from '../src/superinference/types';

// ---------------------------------------------------------------------------
// Constructor & defaults
// ---------------------------------------------------------------------------

describe('BeliefTracker – constructor', () => {
  it('uses DEFAULT_CONFIG values when no config is provided', () => {
    const tracker = new BeliefTracker();
    assert.equal(tracker.belief, DEFAULT_CONFIG.initialBelief);
    assert.equal(tracker.step, 0);
  });

  it('merges partial config with defaults', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    assert.equal(tracker.belief, 0.5);
  });

  it('clamps initialBelief below 0.25 to 0.25', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.0 });
    assert.equal(tracker.belief, 0.25);
  });

  it('clamps initialBelief above 0.95 to 0.95', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 1.0 });
    assert.equal(tracker.belief, 0.95);
  });

  it('throws RangeError for lambdaPlus = 0', () => {
    assert.throws(() => new BeliefTracker({ enabled: true, lambdaPlus: 0 }), RangeError);
  });

  it('throws RangeError for lambdaPlus > 1', () => {
    assert.throws(() => new BeliefTracker({ enabled: true, lambdaPlus: 1.1 }), RangeError);
  });

  it('accepts lambdaPlus = 1.0', () => {
    assert.doesNotThrow(() => new BeliefTracker({ enabled: true, lambdaPlus: 1.0 }));
  });

  it('throws RangeError for lambdaMinus = 0', () => {
    assert.throws(() => new BeliefTracker({ enabled: true, lambdaMinus: 0 }), RangeError);
  });

  it('throws RangeError for lambdaMinus = 1.0', () => {
    assert.throws(() => new BeliefTracker({ enabled: true, lambdaMinus: 1.0 }), RangeError);
  });

  it('throws RangeError for negative lambdaMinus', () => {
    assert.throws(() => new BeliefTracker({ enabled: true, lambdaMinus: -0.5 }), RangeError);
  });

  it('accepts lambdaMinus = 0.5', () => {
    assert.doesNotThrow(() => new BeliefTracker({ enabled: true, lambdaMinus: 0.5 }));
  });
});

// ---------------------------------------------------------------------------
// update() — Eq. 1
// ---------------------------------------------------------------------------

describe('BeliefTracker – update()', () => {
  it('approved: b = b + lambda_plus * (s - b)', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    tracker.update(true, 0.8);
    // b = 0.5 + 0.35 * (0.8 - 0.5) = 0.5 + 0.105 = 0.605
    assert.ok(Math.abs(tracker.belief - 0.605) < 0.001);
  });

  it('rejected: b = lambdaMinus * b', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    tracker.update(false);
    // b = 0.6 * 0.5 = 0.3
    assert.ok(Math.abs(tracker.belief - 0.3) < 0.001);
  });

  it('defaults criticScore to 1.0 when not provided', () => {
    const t1 = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    const t2 = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    t1.update(true);
    t2.update(true, 1.0);
    assert.equal(t1.belief, t2.belief);
  });

  it('clamps criticScore to [0, 1]', () => {
    const tHigh = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    const tNormal = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    tHigh.update(true, 5.0);
    tNormal.update(true, 1.0);
    assert.equal(tHigh.belief, tNormal.belief);

    const tLow = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    const tZero = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    tLow.update(true, -2.0);
    tZero.update(true, 0.0);
    assert.equal(tLow.belief, tZero.belief);
  });

  it('increments step on each update', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    assert.equal(tracker.step, 0);
    tracker.update(true);
    assert.equal(tracker.step, 1);
    tracker.update(false);
    assert.equal(tracker.step, 2);
    tracker.update(true, 0.5);
    assert.equal(tracker.step, 3);
  });

  it('clamps belief to [0.25, 0.95] after approval', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.93 });
    tracker.update(true, 1.0); // Would push above 0.95
    assert.ok(tracker.belief <= 0.95);
  });

  it('clamps belief to [0.25, 0.95] after rejection', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.28 });
    tracker.update(false); // 0.6 * 0.28 = 0.168 -> clamped to 0.25
    assert.equal(tracker.belief, 0.25);
  });
});

// ---------------------------------------------------------------------------
// entropy() — Eq. 7
// ---------------------------------------------------------------------------

describe('BeliefTracker – entropy()', () => {
  it('H(0.5) = 1.0 (max entropy)', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    assert.ok(Math.abs(tracker.entropy() - 1.0) < 0.001);
  });

  it('H(0) = 0', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    assert.equal(tracker.entropy(0), 0);
  });

  it('H(1) = 0', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    assert.equal(tracker.entropy(1), 0);
  });

  it('H is symmetric: H(p) = H(1-p)', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    assert.ok(Math.abs(tracker.entropy(0.3) - tracker.entropy(0.7)) < 0.001);
  });

  it('uses current belief when no argument provided', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.3 });
    // H(0.3) ≈ 0.8813
    assert.ok(Math.abs(tracker.entropy() - 0.8813) < 0.01);
  });

  it('handles near-zero values without NaN', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    const h = tracker.entropy(1e-15);
    assert.ok(!Number.isNaN(h));
    assert.ok(Math.abs(h) < 0.01);
  });

  it('handles near-one values without NaN', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    const h = tracker.entropy(1 - 1e-15);
    assert.ok(!Number.isNaN(h));
    assert.ok(Math.abs(h) < 0.01);
  });
});

// ---------------------------------------------------------------------------
// eig() — Eq. 8
// ---------------------------------------------------------------------------

describe('BeliefTracker – eig()', () => {
  it('is non-negative', () => {
    for (const b of [0.25, 0.3, 0.5, 0.7, 0.9, 0.95]) {
      const tracker = new BeliefTracker({ enabled: true, initialBelief: b });
      assert.ok(tracker.eig() >= 0, `EIG should be >= 0 at b=${b}`);
    }
  });

  it('decreases as belief approaches certainty', () => {
    const low = new BeliefTracker({ enabled: true, initialBelief: 0.3 });
    const high = new BeliefTracker({ enabled: true, initialBelief: 0.9 });
    assert.ok(low.eig() > high.eig());
  });

  it('is finite after many updates', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.3 });
    for (let i = 0; i < 50; i++) tracker.update(true);
    assert.ok(Number.isFinite(tracker.eig()));
  });
});

// ---------------------------------------------------------------------------
// shouldStop() — S2.6
// ---------------------------------------------------------------------------

describe('BeliefTracker – shouldStop()', () => {
  it('returns confidence when belief >= threshold', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.91 });
    assert.equal(tracker.shouldStop().type, 'confidence');
  });

  it('returns diminishing_returns when EIG < threshold', () => {
    const tracker = new BeliefTracker({
      enabled: true,
      initialBelief: 0.95,
      confidenceThreshold: 0.99,
    });
    assert.equal(tracker.shouldStop().type, 'diminishing_returns');
  });

  it('returns budget when step >= maxSteps', () => {
    const tracker = new BeliefTracker({
      enabled: true,
      initialBelief: 0.5,
      maxSteps: 2,
      confidenceThreshold: 0.99,
      eigThreshold: 0,
    });
    tracker.update(true, 0.5);
    tracker.update(false);
    assert.equal(tracker.shouldStop().type, 'budget');
  });

  it('returns none when no criteria met', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    assert.equal(tracker.shouldStop().type, 'none');
  });

  it('confidence has priority over other criteria', () => {
    const tracker = new BeliefTracker({
      enabled: true,
      initialBelief: 0.95,
      confidenceThreshold: 0.9,
      eigThreshold: 10,  // would trigger diminishing_returns
      maxSteps: 0,       // would trigger budget
    });
    assert.equal(tracker.shouldStop().type, 'confidence');
  });

  it('diminishing_returns has priority over budget', () => {
    const tracker = new BeliefTracker({
      enabled: true,
      initialBelief: 0.5,
      confidenceThreshold: 0.99,
      eigThreshold: 10,  // triggers diminishing_returns
      maxSteps: 0,       // would also trigger budget
    });
    assert.equal(tracker.shouldStop().type, 'diminishing_returns');
  });
});

// ---------------------------------------------------------------------------
// getState()
// ---------------------------------------------------------------------------

describe('BeliefTracker – getState()', () => {
  it('returns all state fields', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    const state = tracker.getState();
    assert.equal(typeof state.value, 'number');
    assert.equal(typeof state.entropy, 'number');
    assert.equal(typeof state.eig, 'number');
    assert.equal(typeof state.step, 'number');
  });

  it('reflects current state', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    tracker.update(true, 0.8);
    const state = tracker.getState();
    assert.equal(state.value, tracker.belief);
    assert.equal(state.step, 1);
    assert.ok(Math.abs(state.entropy - tracker.entropy()) < 0.001);
  });
});

// ---------------------------------------------------------------------------
// reset()
// ---------------------------------------------------------------------------

describe('BeliefTracker – reset()', () => {
  it('restores initial belief and step', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.4 });
    tracker.update(true, 0.9);
    tracker.update(true, 0.9);
    assert.notEqual(tracker.belief, 0.4);
    assert.notEqual(tracker.step, 0);

    tracker.reset();
    assert.equal(tracker.belief, 0.4);
    assert.equal(tracker.step, 0);
  });
});

// ---------------------------------------------------------------------------
// Convergence behavior
// ---------------------------------------------------------------------------

describe('BeliefTracker – convergence', () => {
  it('converges to confidence threshold with repeated approvals', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.3 });
    let steps = 0;
    while (tracker.shouldStop().type === 'none' && steps < 100) {
      tracker.update(true);
      steps++;
    }
    assert.equal(tracker.shouldStop().type, 'confidence');
    assert.ok(steps <= 25, `Should converge within budget, took ${steps}`);
  });

  it('converges to minimum with repeated rejections', () => {
    const tracker = new BeliefTracker({
      enabled: true,
      initialBelief: 0.8,
      confidenceThreshold: 0.99,
    });
    for (let i = 0; i < 50; i++) tracker.update(false);
    assert.equal(tracker.belief, 0.25);
  });
});
