import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';

import { SuperInferenceEngine } from '../src/superinference/index';
import { DEFAULT_CONFIG } from '../src/superinference/types';

// ---------------------------------------------------------------------------
// 1. shouldContinue() returns {continue: true} at initial state
// ---------------------------------------------------------------------------

describe('SuperInferenceEngine – shouldContinue() at initial state', () => {
  it('returns continue: true with default config (b_0=0.3)', () => {
    const engine = new SuperInferenceEngine();
    const result = engine.shouldContinue();
    assert.equal(result.continue, true);
    assert.equal(result.reason.type, 'none');
  });

  it('returns continue: true with custom low initialBelief', () => {
    const engine = new SuperInferenceEngine({ initialBelief: 0.5 });
    const result = engine.shouldContinue();
    assert.equal(result.continue, true);
    assert.equal(result.reason.type, 'none');
  });
});

// ---------------------------------------------------------------------------
// 2. shouldContinue() returns {continue: false} when belief >= kappa
// ---------------------------------------------------------------------------

describe('SuperInferenceEngine – shouldContinue() stops at confidence threshold', () => {
  it('returns continue: false when belief is raised above kappa', () => {
    const engine = new SuperInferenceEngine({ initialBelief: 0.3 });
    // Drive belief above the default kappa=0.9 via repeated approvals
    while (engine.belief.belief < engine.config.confidenceThreshold) {
      engine.belief.update(true, 1.0);
    }
    const result = engine.shouldContinue();
    assert.equal(result.continue, false);
    assert.equal(result.reason.type, 'confidence');
  });

  it('returns continue: false when initialBelief already >= kappa', () => {
    const engine = new SuperInferenceEngine({
      initialBelief: 0.95,
      confidenceThreshold: 0.9,
    });
    const result = engine.shouldContinue();
    assert.equal(result.continue, false);
    assert.equal(result.reason.type, 'confidence');
  });

  it('returns continue: false with custom kappa', () => {
    const engine = new SuperInferenceEngine({
      initialBelief: 0.7,
      confidenceThreshold: 0.65,
    });
    const result = engine.shouldContinue();
    assert.equal(result.continue, false);
    assert.equal(result.reason.type, 'confidence');
  });
});

// ---------------------------------------------------------------------------
// 3. getState() returns correct shape with ppv computed from critic.ppv()
// ---------------------------------------------------------------------------

describe('SuperInferenceEngine – getState() shape and ppv', () => {
  it('returns object with value, entropy, eig, step, and ppv fields', () => {
    const engine = new SuperInferenceEngine();
    const state = engine.getState();
    assert.equal(typeof state.value, 'number');
    assert.equal(typeof state.entropy, 'number');
    assert.equal(typeof state.eig, 'number');
    assert.equal(typeof state.step, 'number');
    assert.equal(typeof state.ppv, 'number');
  });

  it('ppv equals critic.ppv(belief.value) at initial state', () => {
    const engine = new SuperInferenceEngine();
    const state = engine.getState();
    const expectedPpv = engine.critic.ppv(engine.belief.belief);
    assert.equal(state.ppv, expectedPpv);
  });

  it('ppv matches critic.ppv formula: PPV = (1-beta)*p / ((1-beta)*p + alpha*(1-p))', () => {
    const alpha = 0.05;
    const beta = 0.10;
    const engine = new SuperInferenceEngine({ criticAlpha: alpha, criticBeta: beta, initialBelief: 0.6 });
    const state = engine.getState();
    const p = 0.6;
    const expected = ((1 - beta) * p) / ((1 - beta) * p + alpha * (1 - p));
    assert.ok(Math.abs(state.ppv - expected) < 1e-10,
      `Expected ppv ~${expected}, got ${state.ppv}`);
  });

  it('state.value matches belief.belief', () => {
    const engine = new SuperInferenceEngine({ initialBelief: 0.5 });
    const state = engine.getState();
    assert.equal(state.value, 0.5);
  });

  it('state.step starts at 0', () => {
    const engine = new SuperInferenceEngine();
    const state = engine.getState();
    assert.equal(state.step, 0);
  });
});

// ---------------------------------------------------------------------------
// 4. getState().ppv changes as belief changes
// ---------------------------------------------------------------------------

describe('SuperInferenceEngine – ppv tracks belief changes', () => {
  it('ppv increases after approval raises belief', () => {
    const engine = new SuperInferenceEngine({ initialBelief: 0.3 });
    const ppvBefore = engine.getState().ppv;
    engine.belief.update(true, 0.9);
    const ppvAfter = engine.getState().ppv;
    assert.ok(ppvAfter > ppvBefore,
      `Expected ppv to increase from ${ppvBefore} to ${ppvAfter}`);
  });

  it('ppv decreases after rejection lowers belief', () => {
    const engine = new SuperInferenceEngine({ initialBelief: 0.6 });
    const ppvBefore = engine.getState().ppv;
    engine.belief.update(false);
    const ppvAfter = engine.getState().ppv;
    assert.ok(ppvAfter < ppvBefore,
      `Expected ppv to decrease from ${ppvBefore} to ${ppvAfter}`);
  });

  it('ppv is consistent with getState().value after multiple updates', () => {
    const engine = new SuperInferenceEngine({ initialBelief: 0.4 });
    engine.belief.update(true, 0.8);
    engine.belief.update(false);
    engine.belief.update(true, 1.0);
    const state = engine.getState();
    const expectedPpv = engine.critic.ppv(state.value);
    assert.equal(state.ppv, expectedPpv);
  });
});

// ---------------------------------------------------------------------------
// 5. reset() clears belief to initial and memoryGate entries
// ---------------------------------------------------------------------------

describe('SuperInferenceEngine – reset()', () => {
  it('restores belief to initialBelief after updates', () => {
    const engine = new SuperInferenceEngine({ initialBelief: 0.3 });
    engine.belief.update(true, 0.9);
    engine.belief.update(true, 0.9);
    assert.notEqual(engine.belief.belief, 0.3);

    engine.reset();
    assert.equal(engine.belief.belief, 0.3);
  });

  it('resets belief step counter to 0', () => {
    const engine = new SuperInferenceEngine();
    engine.belief.update(true);
    engine.belief.update(false);
    assert.equal(engine.belief.step, 2);

    engine.reset();
    assert.equal(engine.belief.step, 0);
  });

  it('clears memoryGate entries', () => {
    const engine = new SuperInferenceEngine();
    engine.memoryGate.gate('query1', 'result1', { approved: true, score: 0.8 }, 0.5, 1);
    engine.memoryGate.gate('query2', 'result2', { approved: true, score: 0.9 }, 0.6, 2);
    assert.equal(engine.memoryGate.getEntries().length, 2);

    engine.reset();
    assert.equal(engine.memoryGate.getEntries().length, 0);
  });

  it('after reset, shouldContinue returns true again', () => {
    const engine = new SuperInferenceEngine({ initialBelief: 0.3 });
    // Drive to confidence threshold
    while (engine.belief.belief < engine.config.confidenceThreshold) {
      engine.belief.update(true, 1.0);
    }
    assert.equal(engine.shouldContinue().continue, false);

    engine.reset();
    assert.equal(engine.shouldContinue().continue, true);
  });

  it('after reset, getState reflects initial values', () => {
    const engine = new SuperInferenceEngine({ initialBelief: 0.4 });
    engine.belief.update(true, 0.9);
    engine.reset();

    const state = engine.getState();
    assert.equal(state.value, 0.4);
    assert.equal(state.step, 0);
  });
});

// ---------------------------------------------------------------------------
// 6. Constructor forwards config to all sub-components
// ---------------------------------------------------------------------------

describe('SuperInferenceEngine – constructor config forwarding', () => {
  it('belief.belief matches initialBelief', () => {
    const engine = new SuperInferenceEngine({ initialBelief: 0.5 });
    assert.equal(engine.belief.belief, 0.5);
  });

  it('critic.alphaRate matches criticAlpha', () => {
    const engine = new SuperInferenceEngine({ criticAlpha: 0.08 });
    assert.equal(engine.critic.alphaRate, 0.08);
  });

  it('critic.betaRate matches criticBeta', () => {
    const engine = new SuperInferenceEngine({ criticBeta: 0.15 });
    assert.equal(engine.critic.betaRate, 0.15);
  });

  it('retriever.noiseLevel matches noiseLevel', () => {
    const engine = new SuperInferenceEngine({ noiseLevel: 0.25 });
    assert.equal(engine.retriever.noiseLevel, 0.25);
  });

  it('all config values forwarded together', () => {
    const engine = new SuperInferenceEngine({
      initialBelief: 0.45,
      criticAlpha: 0.07,
      criticBeta: 0.12,
      noiseLevel: 0.2,
    });
    assert.equal(engine.belief.belief, 0.45);
    assert.equal(engine.critic.alphaRate, 0.07);
    assert.equal(engine.critic.betaRate, 0.12);
    assert.equal(engine.retriever.noiseLevel, 0.2);
  });

  it('belief tracker receives full config (confidenceThreshold is forwarded)', () => {
    const engine = new SuperInferenceEngine({
      initialBelief: 0.95,
      confidenceThreshold: 0.99,
    });
    // If confidenceThreshold was forwarded, shouldStop won't trigger 'confidence'
    // because 0.95 < 0.99
    const reason = engine.shouldContinue().reason;
    assert.notEqual(reason.type, 'confidence');
  });
});

// ---------------------------------------------------------------------------
// 7. Default config used when no config provided
// ---------------------------------------------------------------------------

describe('SuperInferenceEngine – default config', () => {
  it('uses DEFAULT_CONFIG values when no config provided', () => {
    const engine = new SuperInferenceEngine();
    assert.equal(engine.config.enabled, DEFAULT_CONFIG.enabled);
    assert.equal(engine.config.initialBelief, DEFAULT_CONFIG.initialBelief);
    assert.equal(engine.config.confidenceThreshold, DEFAULT_CONFIG.confidenceThreshold);
    assert.equal(engine.config.eigThreshold, DEFAULT_CONFIG.eigThreshold);
    assert.equal(engine.config.criticAlpha, DEFAULT_CONFIG.criticAlpha);
    assert.equal(engine.config.criticBeta, DEFAULT_CONFIG.criticBeta);
    assert.equal(engine.config.lambdaPlus, DEFAULT_CONFIG.lambdaPlus);
    assert.equal(engine.config.lambdaMinus, DEFAULT_CONFIG.lambdaMinus);
    assert.equal(engine.config.maxSteps, DEFAULT_CONFIG.maxSteps);
    assert.equal(engine.config.noiseLevel, DEFAULT_CONFIG.noiseLevel);
    assert.equal(engine.config.successScore, DEFAULT_CONFIG.successScore);
    assert.equal(engine.config.errorScore, DEFAULT_CONFIG.errorScore);
  });

  it('sub-components use default values', () => {
    const engine = new SuperInferenceEngine();
    assert.equal(engine.belief.belief, 0.3);
    assert.equal(engine.critic.alphaRate, 0.05);
    assert.equal(engine.critic.betaRate, 0.10);
    assert.equal(engine.retriever.noiseLevel, 0.1);
  });

  it('explicit undefined config also uses defaults', () => {
    const engine = new SuperInferenceEngine(undefined);
    assert.equal(engine.config.initialBelief, DEFAULT_CONFIG.initialBelief);
    assert.equal(engine.belief.belief, 0.3);
  });
});

// ---------------------------------------------------------------------------
// 8. Config merging: partial config fills in defaults for unspecified fields
// ---------------------------------------------------------------------------

describe('SuperInferenceEngine – partial config merging', () => {
  it('specified fields override defaults, unspecified fields use defaults', () => {
    const engine = new SuperInferenceEngine({
      initialBelief: 0.5,
      noiseLevel: 0.3,
    });
    // Overridden
    assert.equal(engine.config.initialBelief, 0.5);
    assert.equal(engine.config.noiseLevel, 0.3);
    // Defaults for unspecified
    assert.equal(engine.config.confidenceThreshold, DEFAULT_CONFIG.confidenceThreshold);
    assert.equal(engine.config.eigThreshold, DEFAULT_CONFIG.eigThreshold);
    assert.equal(engine.config.criticAlpha, DEFAULT_CONFIG.criticAlpha);
    assert.equal(engine.config.criticBeta, DEFAULT_CONFIG.criticBeta);
    assert.equal(engine.config.lambdaPlus, DEFAULT_CONFIG.lambdaPlus);
    assert.equal(engine.config.lambdaMinus, DEFAULT_CONFIG.lambdaMinus);
    assert.equal(engine.config.maxSteps, DEFAULT_CONFIG.maxSteps);
    assert.equal(engine.config.enabled, DEFAULT_CONFIG.enabled);
  });

  it('only criticAlpha specified, other critic/retriever params use defaults', () => {
    const engine = new SuperInferenceEngine({ criticAlpha: 0.12 });
    assert.equal(engine.config.criticAlpha, 0.12);
    assert.equal(engine.config.criticBeta, DEFAULT_CONFIG.criticBeta);
    assert.equal(engine.config.noiseLevel, DEFAULT_CONFIG.noiseLevel);
    assert.equal(engine.config.initialBelief, DEFAULT_CONFIG.initialBelief);
    // Sub-components reflect merged config
    assert.equal(engine.critic.alphaRate, 0.12);
    assert.equal(engine.critic.betaRate, DEFAULT_CONFIG.criticBeta);
    assert.equal(engine.retriever.noiseLevel, DEFAULT_CONFIG.noiseLevel);
    assert.equal(engine.belief.belief, DEFAULT_CONFIG.initialBelief);
  });

  it('empty object config is identical to no config', () => {
    const engineDefault = new SuperInferenceEngine();
    const engineEmpty = new SuperInferenceEngine({});
    assert.deepEqual(engineDefault.config, engineEmpty.config);
  });

  it('merged config is stored as Required<SuperInferenceConfig>', () => {
    const engine = new SuperInferenceEngine({ maxSteps: 10 });
    // All fields should be present (not undefined)
    const config = engine.config;
    for (const key of Object.keys(DEFAULT_CONFIG)) {
      assert.notEqual((config as Record<string, unknown>)[key], undefined,
        `config.${key} should not be undefined`);
    }
  });
});
