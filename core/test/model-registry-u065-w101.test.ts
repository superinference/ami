import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  MODEL_OVERRIDES_AFFORDANCE,
  resolveOverriddenModel,
} from '../src/model-registry';

// U065 (w101): the settings-driven `modelOverrides` affordance must carry a
// canonical introspection descriptor so the knob (key, map shape, config scopes,
// and capability-preserving semantics) is discoverable — the parity gap was the
// absent /config-or-introspection surface DESCRIBING the already-wired override
// resolution/apply path. These assertions lock the descriptor shape (the manifest
// entry + the dead-code consumer key off it).
describe('MODEL_OVERRIDES_AFFORDANCE — settings modelOverrides descriptor', () => {
  it('names the real settings key honored by the resolver', () => {
    assert.equal(MODEL_OVERRIDES_AFFORDANCE.settingsKey, 'modelOverrides');
  });

  it('documents the canonical-id → provider-string map shape', () => {
    assert.match(MODEL_OVERRIDES_AFFORDANCE.valueShape, /canonicalModelId/);
    assert.match(MODEL_OVERRIDES_AFFORDANCE.valueShape, /providerModelString/);
  });

  it('lists every config-waterfall scope that may declare the map', () => {
    assert.deepEqual(
      [...MODEL_OVERRIDES_AFFORDANCE.scopes],
      ['managed', 'project', 'global', 'local'],
    );
  });

  it('marks the override as capability-preserving (canonical id drives lookups)', () => {
    assert.equal(MODEL_OVERRIDES_AFFORDANCE.capabilityPreserving, true);
  });

  it('carries a description naming the Bedrock ARN use + the canonical-id lookup semantics', () => {
    const d = MODEL_OVERRIDES_AFFORDANCE.description;
    assert.ok(d.length > 40, 'description is non-trivial');
    assert.match(d, /Bedrock|ARN/);
    assert.match(d, /context-window|capability/);
  });

  it('the described inverse resolver round-trips an override string to its canonical id', () => {
    // The descriptor documents the behavior resolveOverriddenModel implements:
    // a provider override string maps back to the canonical id it overrode.
    const overrides = { 'claude-opus-4': 'arn:aws:bedrock:us-east-1:1:profile/opus' };
    assert.equal(
      resolveOverriddenModel('arn:aws:bedrock:us-east-1:1:profile/opus', overrides),
      'claude-opus-4',
    );
    // A non-override string is returned unchanged.
    assert.equal(resolveOverriddenModel('claude-sonnet-4', overrides), 'claude-sonnet-4');
  });
});
