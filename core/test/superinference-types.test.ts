import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { DEFAULT_CONFIG } from '../src/superinference/types';
import type { CriticDecision, SuperInferenceConfig } from '../src/superinference/types';

describe('SuperInferenceConfig.criticModel', () => {
  it("DEFAULT_CONFIG.criticModel is '' (inherit the main-loop model)", () => {
    assert.equal(DEFAULT_CONFIG.criticModel, '');
  });

  it('accepts a dedicated critic/reviewer model override', () => {
    const cfg: SuperInferenceConfig = { enabled: true, criticModel: 'cheap-reviewer' };
    assert.equal(cfg.criticModel, 'cheap-reviewer');
  });

  it('DEFAULT_CONFIG makes the critic model user-configurable by default', () => {
    assert.equal(DEFAULT_CONFIG.canUserConfigureCriticModel, true);
  });

  it('allows pinning the critic model (experiment-fixed) via canUserConfigureCriticModel=false', () => {
    const cfg: SuperInferenceConfig = { enabled: true, criticModel: 'pinned', canUserConfigureCriticModel: false };
    assert.equal(cfg.canUserConfigureCriticModel, false);
  });
});

describe('CriticDecision unavailability discriminant', () => {
  it('distinguishes an UNAVAILABLE critic (error) from a genuine rejection', () => {
    const rejection: CriticDecision = { approved: false, score: 0, reason: 'tests fail' };
    const unavailable: CriticDecision = {
      approved: false, score: 0, reason: 'Critic evaluation failed: timeout',
      unavailable: true, errorCode: 'ETIMEDOUT',
    };
    // A real rejection carries no unavailability marker...
    assert.equal(rejection.unavailable, undefined);
    // ...whereas an error-state fallback is explicitly flagged so the engine can
    // soften the belief floor instead of treating it as a substantive "no".
    assert.equal(unavailable.unavailable, true);
    assert.equal(unavailable.errorCode, 'ETIMEDOUT');
  });

  it('carries model attribution and a redacted-but-complete review state', () => {
    // A redacted review is COMPLETE (score/approval stand) — distinct from unavailable.
    const redacted: CriticDecision = {
      approved: true, score: 0.82, model: 'reviewer-pro', redacted: true,
    };
    assert.equal(redacted.model, 'reviewer-pro');
    assert.equal(redacted.redacted, true);
    assert.equal(redacted.unavailable, undefined);
  });
});
