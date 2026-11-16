/**
 * U065 — provider-aware model deprecation parity additions:
 *  - DEPRECATED_MODELS now carries PER-HOSTING-CLASS retirement dates and
 *    getModelDeprecationWarning resolves the date for the active provider.
 *    Ref utils/model/deprecation.ts:L21-L101 (provider-keyed retirementDates).
 *  - toApiProviderClass maps an AMI provider id to firstParty/bedrock/vertex/
 *    foundry. Ref utils/model/providers.ts (APIProvider union).
 *
 * Run: cd core/core && node --import tsx --test test/model-capabilities-u065.test.ts
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  getModelDeprecationWarning,
  toApiProviderClass,
} from '../src/model-capabilities';

describe('toApiProviderClass (U065)', () => {
  it('maps bedrock / vertex / foundry providers', () => {
    assert.equal(toApiProviderClass('amazon-bedrock'), 'bedrock');
    assert.equal(toApiProviderClass('anthropic-vertex'), 'vertex');
    assert.equal(toApiProviderClass('google-vertex'), 'vertex');
    assert.equal(toApiProviderClass('azure-openai'), 'foundry');
  });

  it('defaults anything else (incl. undefined/null) to firstParty', () => {
    assert.equal(toApiProviderClass('anthropic'), 'firstParty');
    assert.equal(toApiProviderClass('openrouter'), 'firstParty');
    assert.equal(toApiProviderClass(undefined), 'firstParty');
    assert.equal(toApiProviderClass(null), 'firstParty');
  });
});

describe('getModelDeprecationWarning — provider-aware dates (U065)', () => {
  it('keeps the first-party date as the default (back-compat single-arg)', () => {
    const w = getModelDeprecationWarning('claude-3-opus-20240229');
    assert.ok(w);
    assert.match(w, /Claude 3 Opus/);
    assert.match(w, /January 5, 2026/);
  });

  it('resolves a DIFFERENT retirement date per hosting provider', () => {
    // Claude 3.7 Sonnet retires on different dates per host.
    assert.match(getModelDeprecationWarning('claude-3-7-sonnet', 'anthropic') ?? '', /February 19, 2026/);
    assert.match(getModelDeprecationWarning('claude-3-7-sonnet', 'amazon-bedrock') ?? '', /April 28, 2026/);
    assert.match(getModelDeprecationWarning('claude-3-7-sonnet', 'anthropic-vertex') ?? '', /May 11, 2026/);
  });

  it('does NOT warn on a host where the model is not retiring (null date)', () => {
    // Claude 3.5 Haiku only retires first-party.
    assert.ok(getModelDeprecationWarning('claude-3-5-haiku', 'anthropic'));
    assert.equal(getModelDeprecationWarning('claude-3-5-haiku', 'amazon-bedrock'), null);
  });

  it('returns null for current models and empty/non-string ids', () => {
    assert.equal(getModelDeprecationWarning('claude-opus-4-6', 'anthropic'), null);
    assert.equal(getModelDeprecationWarning('', 'anthropic'), null);
    assert.equal(getModelDeprecationWarning(null), null);
    assert.equal(getModelDeprecationWarning(undefined), null);
  });
});
