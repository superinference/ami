import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  MODEL_SHORT_KEYS,
  MODEL_ALIASES,
  isModelShortKey,
  isModelAlias,
  resolveModelAlias,
} from '../src/model-registry';

// Version-specific short model keys (opus45, sonnet46, …) — a user-facing selector
// that pins a concrete generation without the dated, provider-prefixed literal.
// (Parity gap: "stable short model keys as a user-facing selector".)
describe('model short keys — version-specific selectors', () => {
  it('maps the reference-parity ModelKey set to concrete ids', () => {
    assert.equal(MODEL_SHORT_KEYS['opus45'], 'claude-opus-4-5');
    assert.equal(MODEL_SHORT_KEYS['opus46'], 'claude-opus-4-6');
    assert.equal(MODEL_SHORT_KEYS['opus41'], 'claude-opus-4-1');
    assert.equal(MODEL_SHORT_KEYS['opus40'], 'claude-opus-4');
    assert.equal(MODEL_SHORT_KEYS['sonnet45'], 'claude-sonnet-4-5');
    assert.equal(MODEL_SHORT_KEYS['sonnet46'], 'claude-sonnet-4-6');
    assert.equal(MODEL_SHORT_KEYS['sonnet40'], 'claude-sonnet-4');
    assert.equal(MODEL_SHORT_KEYS['sonnet37'], 'claude-3-7-sonnet');
    assert.equal(MODEL_SHORT_KEYS['sonnet35'], 'claude-3-5-sonnet');
    assert.equal(MODEL_SHORT_KEYS['haiku45'], 'claude-haiku-4-5');
    assert.equal(MODEL_SHORT_KEYS['haiku35'], 'claude-3-5-haiku');
  });

  it('isModelShortKey recognizes keys, trimmed + case-insensitively', () => {
    assert.equal(isModelShortKey('opus45'), true);
    assert.equal(isModelShortKey('OPUS45'), true);
    assert.equal(isModelShortKey('  sonnet46  '), true);
    // Family aliases and full ids are NOT short keys.
    assert.equal(isModelShortKey('opus'), false);
    assert.equal(isModelShortKey('claude-opus-4-5'), false);
    assert.equal(isModelShortKey(''), false);
    // A prototype member name must not be mistaken for a key.
    assert.equal(isModelShortKey('toString'), false);
    assert.equal(isModelShortKey('constructor'), false);
  });

  it('short keys are disjoint from the bare family aliases', () => {
    for (const k of Object.keys(MODEL_SHORT_KEYS)) {
      assert.equal(isModelAlias(k), false, `${k} should not also be a family alias`);
      assert.equal((MODEL_ALIASES as readonly string[]).includes(k), false);
    }
  });

  it('resolveModelAlias expands short keys to their concrete id', () => {
    assert.equal(resolveModelAlias('opus45'), 'claude-opus-4-5');
    assert.equal(resolveModelAlias('SONNET46'), 'claude-sonnet-4-6');
    assert.equal(resolveModelAlias('  haiku45 '), 'claude-haiku-4-5');
  });

  it('resolveModelAlias still resolves family aliases and passes concrete/unknown ids through', () => {
    // Family aliases unchanged (regression lock).
    assert.equal(resolveModelAlias('opus'), 'claude-opus-4');
    assert.equal(resolveModelAlias('sonnet'), 'claude-sonnet-4');
    // Concrete / unknown ids are not aliases → undefined (caller passes verbatim).
    assert.equal(resolveModelAlias('claude-opus-4-5'), undefined);
    assert.equal(resolveModelAlias('gpt-4o'), undefined);
    assert.equal(resolveModelAlias(''), undefined);
  });
});
