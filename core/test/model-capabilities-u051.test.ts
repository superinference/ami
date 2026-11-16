/**
 * U051 — model-capabilities parity additions:
 *  - getContextWindow: exact-match-first + longest-id-first substring resolution
 *    (ref utils/model/modelCapabilities.ts:L53-L83 sortForMatching / exact-before-substring).
 *  - THINKING_LEVEL_DESCRIPTIONS / getThinkingLevelDescription (ref commands/effort/effort.tsx:L173-L176).
 *  - getModelDeprecationWarning + DEPRECATED_MODELS (ref utils/model/deprecation.ts:L28-L101).
 *
 * Run: cd core/core && node --import tsx --test test/model-capabilities-u051.test.ts
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  getContextWindow,
  registerModelCapabilities,
  clearDynamicCapabilities,
  discoverFromModelList,
  THINKING_LEVELS,
  THINKING_LEVEL_DESCRIPTIONS,
  getThinkingLevelDescription,
  getModelDeprecationWarning,
} from '../src/model-capabilities';

afterEach(() => {
  clearDynamicCapabilities();
  delete process.env.SUPERINFERENCE_DISABLE_1M_CONTEXT;
  delete process.env.SUPERINFERENCE_MAX_CONTEXT_TOKENS;
});

describe('getContextWindow — exact-match-first + longest-id-first (U051)', () => {
  it('still resolves the known static-table windows (no regression)', () => {
    assert.equal(getContextWindow('claude-opus-4'), 200000);
    assert.equal(getContextWindow('gpt-4o'), 128000);
    assert.equal(getContextWindow('o1-mini'), 128000);
    assert.equal(getContextWindow('gpt-4o-2024-08-06'), 128000);
    assert.equal(getContextWindow('unknown-model-xyz'), 128000);
  });

  it('keeps the 1M-beta uplift for the gated families', () => {
    assert.equal(getContextWindow('claude-sonnet-4'), 1_000_000);
  });

  it('prefers an exact id match over a shorter family substring key', () => {
    // A runtime-discovered id whose exact length-based ordering could otherwise be
    // shadowed. 'qwen2.5-7b' (131072) must win over the shorter 'qwen2.5' (32768).
    assert.equal(getContextWindow('qwen2.5-7b'), 131072);
    assert.equal(getContextWindow('qwen2.5'), 32768);
  });

  it('resolves most-specific substring regardless of registration order', () => {
    // Register a SHORT family key AFTER a longer one so insertion order would,
    // without longest-first sorting, return the short key for the long id.
    discoverFromModelList([
      { id: 'acme-7b', max_input_tokens: 40000 },
      { id: 'acme-7b-turbo-20250101', max_input_tokens: 90000 },
    ]);
    // The full id is an exact match → its own window.
    assert.equal(getContextWindow('acme-7b-turbo-20250101'), 90000);
    // A longer-than-any-key id falls back to substring; the longest matching key
    // ('acme-7b-turbo-20250101', 22 chars) wins over 'acme-7b' (7 chars).
    assert.equal(getContextWindow('vendor/acme-7b-turbo-20250101-preview'), 90000);
    // A bare short id still resolves to the short key.
    assert.equal(getContextWindow('acme-7b'), 40000);
  });

  it('honors a registered id that is an exact (case-insensitive) match', () => {
    discoverFromModelList([{ id: 'Fancy-Model-XL', context_length: 262144 }]);
    assert.equal(getContextWindow('fancy-model-xl'), 262144);
    assert.equal(getContextWindow('FANCY-MODEL-XL'), 262144);
  });
});

describe('THINKING_LEVEL_DESCRIPTIONS / getThinkingLevelDescription (U051)', () => {
  it('has a description for every thinking level', () => {
    for (const level of THINKING_LEVELS) {
      const d = THINKING_LEVEL_DESCRIPTIONS[level];
      assert.equal(typeof d, 'string');
      assert.ok(d.length > 0, `level ${level} must have a non-empty description`);
    }
  });

  it('getThinkingLevelDescription returns the description for a valid level', () => {
    assert.equal(getThinkingLevelDescription('max'), THINKING_LEVEL_DESCRIPTIONS.max);
    assert.equal(getThinkingLevelDescription('off'), THINKING_LEVEL_DESCRIPTIONS.off);
  });

  it('getThinkingLevelDescription returns undefined for an unknown level', () => {
    assert.equal(getThinkingLevelDescription('bogus'), undefined);
    assert.equal(getThinkingLevelDescription(''), undefined);
  });
});

describe('getModelDeprecationWarning (U051)', () => {
  it('warns for a deprecated model (substring + dated snapshot + gateway prefix)', () => {
    const w = getModelDeprecationWarning('claude-3-opus-20240229');
    assert.ok(w && /Claude 3 Opus/.test(w), w ?? 'expected a warning');
    assert.ok(/retired on/.test(w!));
    assert.ok(getModelDeprecationWarning('openrouter/anthropic/claude-3-7-sonnet'));
    assert.ok(getModelDeprecationWarning('claude-3-5-haiku-latest'));
  });

  it('returns null for a current model', () => {
    assert.equal(getModelDeprecationWarning('claude-opus-4-6'), null);
    assert.equal(getModelDeprecationWarning('gpt-4o'), null);
  });

  it('returns null for an empty / non-string id', () => {
    assert.equal(getModelDeprecationWarning(''), null);
    assert.equal(getModelDeprecationWarning(null), null);
    assert.equal(getModelDeprecationWarning(undefined), null);
    assert.equal(getModelDeprecationWarning(123 as unknown as string), null);
  });
});
