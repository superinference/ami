/**
 * U051 (w69) — model-capabilities parity additions:
 *  - hasUltrathinkKeyword / findThinkingTriggerPositions (inline reasoning
 *    escalation keyword) — parity with references/claude .../utils/thinking.ts.
 *  - get3PModelFallbackSuggestion (version-downgrade fallback on a 3P endpoint)
 *    — parity with references/claude .../services/api/errors.ts:L936-L959.
 */
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  hasUltrathinkKeyword,
  findThinkingTriggerPositions,
  get3PModelFallbackSuggestion,
} from '../src/model-capabilities';

describe('hasUltrathinkKeyword (U051 w69)', () => {
  it('matches the bare keyword case-insensitively anywhere in the text', () => {
    assert.equal(hasUltrathinkKeyword('ultrathink'), true);
    assert.equal(hasUltrathinkKeyword('please Ultrathink this one'), true);
    assert.equal(hasUltrathinkKeyword('ULTRATHINK!'), true);
    assert.equal(hasUltrathinkKeyword('think hard,\nultrathink\nfinally'), true);
  });

  it('respects word boundaries and rejects empty / non-string input', () => {
    assert.equal(hasUltrathinkKeyword('ultrathinking'), false);
    assert.equal(hasUltrathinkKeyword('myultrathink'), false);
    assert.equal(hasUltrathinkKeyword('just thinking'), false);
    assert.equal(hasUltrathinkKeyword(''), false);
    assert.equal(hasUltrathinkKeyword(undefined as unknown as string), false);
    assert.equal(hasUltrathinkKeyword(123 as unknown as string), false);
  });
});

describe('findThinkingTriggerPositions (U051 w69)', () => {
  it('returns every match with exact [start,end) offsets', () => {
    const text = 'a ultrathink b Ultrathink';
    const pos = findThinkingTriggerPositions(text);
    assert.equal(pos.length, 2);
    assert.deepEqual(pos[0], { word: 'ultrathink', start: 2, end: 12 });
    assert.equal(text.slice(pos[0].start, pos[0].end), 'ultrathink');
    assert.equal(pos[1].word, 'Ultrathink');
    assert.equal(text.slice(pos[1].start, pos[1].end), 'Ultrathink');
  });

  it('returns [] for keyword-free, empty, or non-string input', () => {
    assert.deepEqual(findThinkingTriggerPositions('no keyword'), []);
    assert.deepEqual(findThinkingTriggerPositions('ultrathinking'), []);
    assert.deepEqual(findThinkingTriggerPositions(''), []);
    assert.deepEqual(findThinkingTriggerPositions(null as unknown as string), []);
  });

  it('does not leak regex lastIndex state between calls', () => {
    // A shared /g instance would skip the first match on the 2nd call.
    assert.equal(findThinkingTriggerPositions('ultrathink').length, 1);
    assert.equal(findThinkingTriggerPositions('ultrathink').length, 1);
  });
});

describe('get3PModelFallbackSuggestion (U051 w69)', () => {
  const saved = { ...process.env };
  afterEach(() => {
    for (const k of Object.keys(process.env)) if (!(k in saved)) delete process.env[k];
    Object.assign(process.env, saved);
  });

  const asThirdParty = () => {
    for (const k of ['ANTHROPIC_BASE_URL', 'OPENAI_BASE_URL', 'AI_BASE_URL', 'PROVIDER_BASE_URL', 'OLLAMA_BASE_URL']) {
      delete process.env[k];
    }
    process.env.ANTHROPIC_BASE_URL = 'https://gateway.example/v1';
  };
  const asFirstParty = () => {
    for (const k of ['ANTHROPIC_BASE_URL', 'OPENAI_BASE_URL', 'AI_BASE_URL', 'PROVIDER_BASE_URL', 'OLLAMA_BASE_URL']) {
      delete process.env[k];
    }
  };

  it('walks the version-downgrade chain on a 3P endpoint', () => {
    asThirdParty();
    assert.equal(get3PModelFallbackSuggestion('claude-opus-4-6'), 'claude-opus-4-1');
    assert.equal(get3PModelFallbackSuggestion('claude-sonnet-4-6'), 'claude-sonnet-4-5');
    assert.equal(get3PModelFallbackSuggestion('claude-sonnet-4-5'), 'claude-sonnet-4');
    // underscore separators and gateway prefixes still match
    assert.equal(get3PModelFallbackSuggestion('vendor/claude_opus_4_6'), 'claude-opus-4-1');
  });

  it('returns undefined for first-party, unknown ids, or bad input', () => {
    asFirstParty();
    assert.equal(get3PModelFallbackSuggestion('claude-opus-4-6'), undefined);
    asThirdParty();
    assert.equal(get3PModelFallbackSuggestion('claude-opus-4-1'), undefined);
    assert.equal(get3PModelFallbackSuggestion('gpt-4o'), undefined);
    assert.equal(get3PModelFallbackSuggestion(''), undefined);
    assert.equal(get3PModelFallbackSuggestion(null), undefined);
  });
});
