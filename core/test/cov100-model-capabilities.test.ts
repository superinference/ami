import { describe, it, after, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  getContextWindow,
  discoverFromModelList,
  getModelCapabilities,
  isReasoningModel,
  requiresResponsesAPI,
  clearDynamicCapabilities,
  DEFAULT_CONTEXT_WINDOW,
} from '../src/model-capabilities';

// These tests target the specific branch arms that the existing model-*
// test files leave uncovered (branch% 97.78). Each asserts the REAL behaviour
// of the uncovered path, not merely that the function can be called.

describe('getContextWindow: 1M marker on an id absent from the table', () => {
  // Covers the FINAL return's `oneMillion === true` arm (line 110): no table
  // key matches the id, yet the [-_]1m beta marker is present, so the window
  // must widen to 1M rather than fall back to the 128k default. The existing
  // 1m tests all use ids that DO match a table key (the line-108 arm), leaving
  // this no-match arm uncovered.
  it('widens an unknown model with a -1m suffix to the 1M window', () => {
    // "zzz-custom-1m" contains no CONTEXT_WINDOWS key substring.
    assert.equal(getContextWindow('zzz-custom-1m'), 1_000_000);
  });

  it('widens an unknown model with a [1m] marker to the 1M window', () => {
    assert.equal(getContextWindow('wholly-unknown[1m]'), 1_000_000);
  });

  it('still returns the default for an unknown model without the marker', () => {
    // Sanity contrast: same no-table-match path, marker absent -> default.
    assert.equal(getContextWindow('zzz-custom-xyz'), DEFAULT_CONTEXT_WINDOW);
  });
});

describe('parseGptMajorVersion via discoverFromModelList', () => {
  // parseGptMajorVersion is internal; it is reached through
  // discoverFromModelList -> classifyOpenAIModel. The existing tests only hit
  // the "digitEnd !== -1" arm (e.g. "gpt-5.6" splits at the dot). These cover
  // the two untouched arms.
  beforeEach(() => clearDynamicCapabilities());
  after(() => clearDynamicCapabilities());

  it('all-digits-after-prefix id (digitEnd === -1) classifies as reasoning', () => {
    // afterPrefix "7" has no non-digit char -> search(/\D/) === -1 -> the
    // `? afterPrefix` arm is taken, majorStr = "7", major 7 >= 5 -> reasoning.
    const result = discoverFromModelList([{ id: 'gpt-7' }]);
    assert.ok(result.registered.includes('gpt-7'));
    const caps = getModelCapabilities('gpt-7');
    assert.ok(caps, 'gpt-7 should classify as a known capability set');
    assert.equal(caps?.reasoning, true);
    assert.equal(caps?.temperatureMustBeUnset, true);
    assert.equal(isReasoningModel('gpt-7'), true);
  });

  it('non-digit-first gpt id (empty majorStr) classifies as null', () => {
    // afterPrefix "oss" starts with a non-digit -> digitEnd === 0 ->
    // majorStr === "" -> the `if (!majorStr) return null;` arm fires, so the
    // model is registered with a null capability set.
    const result = discoverFromModelList([{ id: 'gpt-oss' }]);
    assert.ok(result.registered.includes('gpt-oss'));
    // Registered explicitly as null (distinct from "never seen").
    assert.equal(getModelCapabilities('gpt-oss'), null);
    assert.equal(isReasoningModel('gpt-oss'), false);
  });
});

describe('requiresResponsesAPI: gpt id without a leading version number', () => {
  // Covers the `if (!versionMatch) return false;` arm (line 381): the id is a
  // gpt-* model but the text after "gpt-" does not start with digits, so no
  // version can be parsed and the model does not require /v1/responses.
  it('returns false when no version number follows the gpt- prefix', () => {
    assert.equal(requiresResponsesAPI('gpt-oss'), false);
  });

  it('contrast: a >= 5.6 version still requires the responses API', () => {
    assert.equal(requiresResponsesAPI('gpt-5.6-luna'), true);
  });

  it('contrast: a < 5.6 version does not require the responses API', () => {
    assert.equal(requiresResponsesAPI('gpt-4.1'), false);
  });
});

describe('getModelCapabilities: non-string input guard', () => {
  // Covers `if (typeof modelId !== 'string') return null;` (line 410). Callers
  // can pass runtime values that are not strings; the guard must return null
  // rather than throw on .toLowerCase().
  it('returns null for a numeric model id', () => {
    assert.equal(getModelCapabilities(123 as unknown as string), null);
  });

  it('returns null for undefined', () => {
    assert.equal(getModelCapabilities(undefined as unknown as string), null);
  });

  it('isReasoningModel is false for a non-string id', () => {
    assert.equal(isReasoningModel({} as unknown as string), false);
  });
});
