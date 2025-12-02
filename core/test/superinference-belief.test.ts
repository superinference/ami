import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';

import { BeliefTracker } from '../src/superinference/belief';
import { DEFAULT_CONFIG } from '../src/superinference/types';
import type { BeliefState, StopReason } from '../src/superinference/types';

// ---------------------------------------------------------------------------
// Belief update — Equation 1
// ---------------------------------------------------------------------------

describe('BeliefTracker – Equation 1 (belief update)', () => {
  it('approval: b_0=0.6, s=0.9, λ+=0.35 → b_1 ≈ 0.705', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.6 });
    tracker.update(true, 0.9);
    // b_1 = 0.6 + 0.35 * (0.9 - 0.6) = 0.6 + 0.105 = 0.705
    assert.ok(Math.abs(tracker.belief - 0.705) < 0.001,
      `Expected ~0.705, got ${tracker.belief}`);
  });

  it('rejection: b_0=0.6, λ-=0.6 → b_1 = 0.36', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.6 });
    tracker.update(false);
    // b_1 = 0.6 * 0.6 = 0.36
    assert.ok(Math.abs(tracker.belief - 0.36) < 0.001,
      `Expected 0.36, got ${tracker.belief}`);
  });

  it('approval without explicit score defaults s=1.0', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    tracker.update(true);
    // b_1 = 0.5 + 0.35 * (1.0 - 0.5) = 0.5 + 0.175 = 0.675
    assert.ok(Math.abs(tracker.belief - 0.675) < 0.001,
      `Expected ~0.675, got ${tracker.belief}`);
  });

  it('step counter increments on each update', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    assert.equal(tracker.step, 0);
    tracker.update(true);
    assert.equal(tracker.step, 1);
    tracker.update(false);
    assert.equal(tracker.step, 2);
  });
});

// ---------------------------------------------------------------------------
// Clamping — belief stays in [0.25, 0.95]
// ---------------------------------------------------------------------------

describe('BeliefTracker – clamping to [0.25, 0.95]', () => {
  it('belief never drops below 0.25 after repeated rejections', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.3 });
    for (let i = 0; i < 20; i++) {
      tracker.update(false);
    }
    assert.ok(tracker.belief >= 0.25,
      `Belief ${tracker.belief} fell below 0.25`);
    assert.equal(tracker.belief, 0.25);
  });

  it('belief never exceeds 0.95 after repeated approvals', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.8 });
    for (let i = 0; i < 20; i++) {
      tracker.update(true, 1.0);
    }
    assert.ok(tracker.belief <= 0.95,
      `Belief ${tracker.belief} exceeded 0.95`);
    assert.equal(tracker.belief, 0.95);
  });
});

// ---------------------------------------------------------------------------
// Entropy — Equation 7
// ---------------------------------------------------------------------------

describe('BeliefTracker – Equation 7 (entropy)', () => {
  it('H(0.5) = 1.0 bit (maximum uncertainty)', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    assert.ok(Math.abs(tracker.entropy() - 1.0) < 0.001,
      `Expected H(0.5)=1.0, got ${tracker.entropy()}`);
  });

  it('H(0.95) ≈ 0.286 bits (high confidence)', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.95 });
    const h = tracker.entropy();
    assert.ok(Math.abs(h - 0.286) < 0.01,
      `Expected H(0.95)≈0.286, got ${h}`);
  });

  it('H(0) = 0 and H(1) = 0 (boundary)', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    assert.equal(tracker.entropy(0), 0);
    assert.equal(tracker.entropy(1), 0);
  });

  it('entropy can be computed for an arbitrary belief value', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    const h = tracker.entropy(0.3);
    // H(0.3) = -0.3*log2(0.3) - 0.7*log2(0.7) ≈ 0.8813
    assert.ok(Math.abs(h - 0.8813) < 0.01,
      `Expected H(0.3)≈0.881, got ${h}`);
  });
});

// ---------------------------------------------------------------------------
// EIG — Equation 8
// ---------------------------------------------------------------------------

describe('BeliefTracker – Equation 8 (EIG)', () => {
  it('at b_0=0.3 (low confidence) EIG is high', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.3 });
    const eigVal = tracker.eig();
    // With low confidence, there is much information to gain
    assert.ok(eigVal > 0.01,
      `Expected EIG > 0.01 at b=0.3, got ${eigVal}`);
  });

  it('at b_0=0.95 (high confidence) EIG is near 0', () => {
    // Use a config where 0.95 won't be clamped to check EIG behavior
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.95 });
    const eigVal = tracker.eig();
    assert.ok(eigVal < 0.05,
      `Expected EIG < 0.05 at b=0.95, got ${eigVal}`);
  });

  it('EIG is always non-negative', () => {
    for (const b of [0.25, 0.3, 0.5, 0.7, 0.9, 0.95]) {
      const tracker = new BeliefTracker({ enabled: true, initialBelief: b });
      assert.ok(tracker.eig() >= 0,
        `EIG should be ≥ 0 at b=${b}, got ${tracker.eig()}`);
    }
  });

  it('EIG at b=0.3 > EIG at b=0.9 (more to learn at low confidence)', () => {
    const low = new BeliefTracker({ enabled: true, initialBelief: 0.3 });
    const high = new BeliefTracker({ enabled: true, initialBelief: 0.9 });
    assert.ok(low.eig() > high.eig(),
      `EIG(0.3)=${low.eig()} should exceed EIG(0.9)=${high.eig()}`);
  });
});

// ---------------------------------------------------------------------------
// Stopping criteria — §2.6
// ---------------------------------------------------------------------------

describe('BeliefTracker – §2.6 (stopping criteria)', () => {
  it('stops when belief ≥ κ (confidence)', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.91 });
    const reason = tracker.shouldStop();
    assert.equal(reason.type, 'confidence');
    assert.ok(reason.detail.includes('0.910'));
  });

  it('stops when EIG < τ (diminishing returns)', () => {
    // At b=0.95, EIG should be very low — but 0.95 also triggers confidence
    // threshold (κ=0.9). Use a higher κ to isolate diminishing returns.
    const tracker = new BeliefTracker({
      enabled: true,
      initialBelief: 0.95,
      confidenceThreshold: 0.99,  // raise κ so confidence doesn't trigger
    });
    const reason = tracker.shouldStop();
    assert.equal(reason.type, 'diminishing_returns');
  });

  it('stops when step ≥ N_max (budget)', () => {
    // Use alternating approve/reject to keep belief mid-range (EIG stays above τ)
    // and a low confidence threshold won't be reached.
    const tracker = new BeliefTracker({
      enabled: true,
      initialBelief: 0.5,
      maxSteps: 3,
      confidenceThreshold: 0.99,  // prevent confidence stop
      eigThreshold: 0,            // prevent diminishing returns stop
    });
    tracker.update(true, 0.6);  // step 1
    tracker.update(false);       // step 2
    tracker.update(false);       // step 3
    const reason = tracker.shouldStop();
    assert.equal(reason.type, 'budget');
    assert.ok(reason.detail.includes('N_max=3'));
  });

  it('continues when no criteria met', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    const reason = tracker.shouldStop();
    assert.equal(reason.type, 'none');
    assert.equal(reason.detail, 'Continue reasoning');
  });
});

// ---------------------------------------------------------------------------
// getState() snapshot
// ---------------------------------------------------------------------------

describe('BeliefTracker – getState()', () => {
  it('returns a consistent snapshot', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    const state = tracker.getState();
    assert.equal(state.value, 0.5);
    assert.ok(Math.abs(state.entropy - 1.0) < 0.001);
    assert.equal(state.step, 0);
    assert.ok(state.eig >= 0);
  });
});

// ---------------------------------------------------------------------------
// Reset
// ---------------------------------------------------------------------------

describe('BeliefTracker – reset()', () => {
  it('restores initial state after updates', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.3 });
    tracker.update(true, 0.9);
    tracker.update(true, 0.9);
    assert.notEqual(tracker.belief, 0.3);
    assert.notEqual(tracker.step, 0);

    tracker.reset();
    assert.equal(tracker.belief, 0.3);
    assert.equal(tracker.step, 0);
  });
});

// ---------------------------------------------------------------------------
// Paper practical example — §2.8
// ---------------------------------------------------------------------------

describe('BeliefTracker – §2.8 practical example', () => {
  it('step 0: b_0=0.3, EIG high → continue', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.3 });
    assert.equal(tracker.belief, 0.3);
    assert.ok(tracker.eig() > 0.01, 'EIG should be high at b=0.3');
    assert.equal(tracker.shouldStop().type, 'none');
  });

  it('step 1: approve → b_1 ≈ 0.545, continue', () => {
    // b_1 = 0.3 + 0.35*(1.0 - 0.3) = 0.3 + 0.245 = 0.545
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.3 });
    tracker.update(true);  // s defaults to 1.0
    assert.ok(Math.abs(tracker.belief - 0.545) < 0.001,
      `Expected ~0.545, got ${tracker.belief}`);
    assert.equal(tracker.shouldStop().type, 'none');
  });

  it('step 2: approve again → b_2 ≈ 0.704, continue', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.3 });
    tracker.update(true);  // b_1 ≈ 0.545
    tracker.update(true);  // b_2 = 0.545 + 0.35*(1.0 - 0.545) = 0.545 + 0.15925 ≈ 0.704
    assert.ok(Math.abs(tracker.belief - 0.704) < 0.01,
      `Expected ~0.704, got ${tracker.belief}`);
    assert.equal(tracker.shouldStop().type, 'none');
  });

  it('continued approvals eventually reach confidence threshold', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.3 });
    let steps = 0;
    while (tracker.shouldStop().type === 'none') {
      tracker.update(true);
      steps++;
      if (steps > 100) break; // safety
    }
    assert.equal(tracker.shouldStop().type, 'confidence');
    assert.ok(tracker.belief >= 0.9,
      `Expected belief ≥ 0.9, got ${tracker.belief}`);
    assert.ok(steps <= 25, `Should converge within budget, took ${steps} steps`);
  });
});

// ---------------------------------------------------------------------------
// DEFAULT_CONFIG
// ---------------------------------------------------------------------------

describe('DEFAULT_CONFIG', () => {
  it('has expected default values', () => {
    assert.equal(DEFAULT_CONFIG.enabled, true);
    assert.equal(DEFAULT_CONFIG.initialBelief, 0.3);
    assert.equal(DEFAULT_CONFIG.confidenceThreshold, 0.9);
    assert.equal(DEFAULT_CONFIG.eigThreshold, 0.01);
    assert.equal(DEFAULT_CONFIG.criticAlpha, 0.05);
    assert.equal(DEFAULT_CONFIG.criticBeta, 0.10);
    assert.equal(DEFAULT_CONFIG.lambdaPlus, 0.35);
    assert.equal(DEFAULT_CONFIG.lambdaMinus, 0.6);
    assert.equal(DEFAULT_CONFIG.maxSteps, 25);
  });
});

// ---------------------------------------------------------------------------
// Exact EIG computation
// ---------------------------------------------------------------------------

describe('BeliefTracker – exact EIG computation', () => {
  it('EIG at b=0.5 with default params ≈ 0.1052', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    // p+ = 0.5*(1-0.10) + 0.5*0.05 = 0.45 + 0.025 = 0.475
    // b_approved = max(0.25, min(0.95, 0.5 + 0.35*(1.0 - 0.5))) = 0.675
    // b_rejected = max(0.25, min(0.95, 0.6 * 0.5)) = 0.3
    // H(0.5) = 1.0
    // H(0.675) = -0.675*log2(0.675) - 0.325*log2(0.325) ≈ 0.9097
    // H(0.3) = -0.3*log2(0.3) - 0.7*log2(0.7) ≈ 0.8813
    // EIG = 1.0 - (0.475*0.9097 + 0.525*0.8813) = 1.0 - (0.4321 + 0.4627) = 1.0 - 0.8948 ≈ 0.1052
    const eigVal = tracker.eig();
    assert.ok(Math.abs(eigVal - 0.1052) < 0.005,
      `Expected EIG ≈ 0.1052, got ${eigVal}`);
  });

  it('intermediate EIG components match expected values', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    // Verify H(0.5) = 1.0
    assert.ok(Math.abs(tracker.entropy(0.5) - 1.0) < 0.001);
    // Verify H(0.675) ≈ 0.9097
    assert.ok(Math.abs(tracker.entropy(0.675) - 0.9097) < 0.005,
      `Expected H(0.675) ≈ 0.9097, got ${tracker.entropy(0.675)}`);
    // Verify H(0.3) ≈ 0.8813
    assert.ok(Math.abs(tracker.entropy(0.3) - 0.8813) < 0.005,
      `Expected H(0.3) ≈ 0.8813, got ${tracker.entropy(0.3)}`);
  });
});

// ---------------------------------------------------------------------------
// EIG with varied α/β
// ---------------------------------------------------------------------------

describe('BeliefTracker – EIG with varied α/β', () => {
  it('perfect critic (α=0, β=0) yields higher EIG than default at b=0.7', () => {
    // At b≠0.5 the p+ weighting differs, making the perfect critic dominate
    const perfect = new BeliefTracker({
      enabled: true, initialBelief: 0.7,
      criticAlpha: 0, criticBeta: 0,
    });
    const defaultTracker = new BeliefTracker({
      enabled: true, initialBelief: 0.7,
    });
    assert.ok(perfect.eig() > defaultTracker.eig(),
      `EIG(perfect)=${perfect.eig()} should exceed EIG(default)=${defaultTracker.eig()}`);
  });

  it('coin-flip critic (α=0.5, β=0.5) yields lower EIG than default at b=0.7', () => {
    // A coin-flip critic provides less useful signal, reducing EIG
    const coinFlip = new BeliefTracker({
      enabled: true, initialBelief: 0.7,
      criticAlpha: 0.5, criticBeta: 0.5,
    });
    const defaultTracker = new BeliefTracker({
      enabled: true, initialBelief: 0.7,
    });
    assert.ok(coinFlip.eig() < defaultTracker.eig(),
      `EIG(coin-flip)=${coinFlip.eig()} should be less than EIG(default)=${defaultTracker.eig()}`);
  });

  it('EIG(α=0.05,β=0.10) > EIG(α=0.3,β=0.3) at b=0.7', () => {
    const good = new BeliefTracker({
      enabled: true, initialBelief: 0.7,
      criticAlpha: 0.05, criticBeta: 0.10,
    });
    const weak = new BeliefTracker({
      enabled: true, initialBelief: 0.7,
      criticAlpha: 0.3, criticBeta: 0.3,
    });
    assert.ok(good.eig() > weak.eig(),
      `EIG(good critic)=${good.eig()} should exceed EIG(weak critic)=${weak.eig()}`);
  });
});

// ---------------------------------------------------------------------------
// Input validation
// ---------------------------------------------------------------------------

describe('BeliefTracker – input validation', () => {
  it('lambdaPlus=0 throws RangeError', () => {
    assert.throws(
      () => new BeliefTracker({ enabled: true, lambdaPlus: 0 }),
      RangeError,
    );
  });

  it('lambdaPlus=1.5 throws RangeError', () => {
    assert.throws(
      () => new BeliefTracker({ enabled: true, lambdaPlus: 1.5 }),
      RangeError,
    );
  });

  it('lambdaMinus=0 throws RangeError', () => {
    assert.throws(
      () => new BeliefTracker({ enabled: true, lambdaMinus: 0 }),
      RangeError,
    );
  });

  it('lambdaMinus=1.0 throws RangeError', () => {
    assert.throws(
      () => new BeliefTracker({ enabled: true, lambdaMinus: 1.0 }),
      RangeError,
    );
  });

  it('lambdaMinus=-0.1 throws RangeError', () => {
    assert.throws(
      () => new BeliefTracker({ enabled: true, lambdaMinus: -0.1 }),
      RangeError,
    );
  });

  it('valid lambdaPlus=1.0 does NOT throw', () => {
    assert.doesNotThrow(
      () => new BeliefTracker({ enabled: true, lambdaPlus: 1.0 }),
    );
  });

  it('valid lambdaMinus=0.5 does NOT throw', () => {
    assert.doesNotThrow(
      () => new BeliefTracker({ enabled: true, lambdaMinus: 0.5 }),
    );
  });

  it('initialBelief below 0.25 gets clamped to 0.25', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.1 });
    assert.equal(tracker.belief, 0.25);
  });

  it('initialBelief above 0.95 gets clamped to 0.95', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.99 });
    assert.equal(tracker.belief, 0.95);
  });
});

// ---------------------------------------------------------------------------
// criticScore clamping
// ---------------------------------------------------------------------------

describe('BeliefTracker – criticScore clamping', () => {
  it('update(true, 5.0) clamps s to 1.0', () => {
    const clamped = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    const normal = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    clamped.update(true, 5.0);
    normal.update(true, 1.0);
    assert.equal(clamped.belief, normal.belief);
  });

  it('update(true, -1.0) clamps s to 0.0', () => {
    const clamped = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    const normal = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    clamped.update(true, -1.0);
    normal.update(true, 0.0);
    assert.equal(clamped.belief, normal.belief);
  });

  it('belief matches hand-computed value with clamped score', () => {
    // update(true, 5.0) with b=0.5, s clamped to 1.0:
    // b = 0.5 + 0.35*(1.0 - 0.5) = 0.675
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    tracker.update(true, 5.0);
    assert.ok(Math.abs(tracker.belief - 0.675) < 0.001,
      `Expected 0.675, got ${tracker.belief}`);

    // update(true, -1.0) with b=0.5, s clamped to 0.0:
    // b = 0.5 + 0.35*(0.0 - 0.5) = 0.5 - 0.175 = 0.325
    const tracker2 = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    tracker2.update(true, -1.0);
    assert.ok(Math.abs(tracker2.belief - 0.325) < 0.001,
      `Expected 0.325, got ${tracker2.belief}`);
  });
});

// ---------------------------------------------------------------------------
// Stopping boundary cases
// ---------------------------------------------------------------------------

describe('BeliefTracker – stopping boundary cases', () => {
  it('belief exactly equal to κ triggers confidence stop', () => {
    const tracker = new BeliefTracker({
      enabled: true,
      initialBelief: 0.9,
      confidenceThreshold: 0.9,
      eigThreshold: 0,
    });
    // b=0.9, κ=0.9 → b >= κ → confidence stop
    assert.equal(tracker.shouldStop().type, 'confidence');
  });

  it('belief at κ - 0.001 does NOT trigger confidence stop', () => {
    const tracker = new BeliefTracker({
      enabled: true,
      initialBelief: 0.899,
      confidenceThreshold: 0.9,
      eigThreshold: 0,
    });
    assert.notEqual(tracker.shouldStop().type, 'confidence');
  });

  it('step exactly equal to N_max triggers budget stop', () => {
    const tracker = new BeliefTracker({
      enabled: true,
      initialBelief: 0.5,
      maxSteps: 2,
      confidenceThreshold: 0.99,
      eigThreshold: 0,
    });
    // Alternate to keep belief in mid-range
    tracker.update(true, 0.5);  // step 1
    tracker.update(false);       // step 2
    assert.equal(tracker.step, 2);
    assert.equal(tracker.shouldStop().type, 'budget');
  });

  it('priority: confidence checked before diminishing_returns before budget', () => {
    // Set up a state where all three criteria could trigger:
    // high belief (confidence), low EIG (diminishing_returns), step >= maxSteps (budget)
    const tracker = new BeliefTracker({
      enabled: true,
      initialBelief: 0.95,
      confidenceThreshold: 0.9,
      eigThreshold: 1.0,   // very high τ so diminishing_returns would trigger
      maxSteps: 0,          // step 0 >= 0, budget would trigger
    });
    // All three could fire, but confidence has priority
    assert.equal(tracker.shouldStop().type, 'confidence');

    // Now test diminishing_returns before budget
    const tracker2 = new BeliefTracker({
      enabled: true,
      initialBelief: 0.5,
      confidenceThreshold: 0.99,  // won't trigger confidence
      eigThreshold: 1.0,          // EIG will be < 1.0, so diminishing_returns triggers
      maxSteps: 0,                // budget would also trigger
    });
    assert.equal(tracker2.shouldStop().type, 'diminishing_returns');
  });
});

// ---------------------------------------------------------------------------
// Numerical stability
// ---------------------------------------------------------------------------

describe('BeliefTracker – numerical stability', () => {
  it('entropy(1e-15) returns ~0 without NaN', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    const h = tracker.entropy(1e-15);
    assert.ok(!Number.isNaN(h), `entropy(1e-15) returned NaN`);
    assert.ok(Math.abs(h) < 0.001, `Expected entropy(1e-15) ≈ 0, got ${h}`);
  });

  it('entropy(1 - 1e-15) returns ~0 without NaN', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    const h = tracker.entropy(1 - 1e-15);
    assert.ok(!Number.isNaN(h), `entropy(1 - 1e-15) returned NaN`);
    assert.ok(Math.abs(h) < 0.001, `Expected entropy(1 - 1e-15) ≈ 0, got ${h}`);
  });

  it('EIG after 100 consecutive approvals is ≥ 0 and finite', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.3 });
    for (let i = 0; i < 100; i++) {
      tracker.update(true, 1.0);
    }
    const eigVal = tracker.eig();
    assert.ok(Number.isFinite(eigVal), `EIG should be finite, got ${eigVal}`);
    assert.ok(eigVal >= 0, `EIG should be ≥ 0, got ${eigVal}`);
  });
});
