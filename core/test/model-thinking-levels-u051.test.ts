import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  getDefaultThinkingLevel,
  cycleThinkingLevel,
  THINKING_LEVELS,
  type ThinkingLevel,
} from '../src/model-capabilities';

// ---------------------------------------------------------------------------
// getDefaultThinkingLevel — per-model `auto`/default level resolver (U051)
// ---------------------------------------------------------------------------
describe('getDefaultThinkingLevel (U051)', () => {
  it('returns the Claude family default (medium) for a reasoning model', () => {
    assert.equal(getDefaultThinkingLevel('claude-opus-4-6'), 'medium');
    assert.equal(getDefaultThinkingLevel('claude-sonnet-4-5'), 'medium');
  });

  it('honors a per-model default distinct from medium', () => {
    // Gemini 2.5-flash is classified with a `low` default thinking level.
    assert.equal(getDefaultThinkingLevel('gemini-2.5-flash'), 'low');
  });

  it('returns off for a non-reasoning model (cannot think)', () => {
    assert.equal(getDefaultThinkingLevel('gpt-4o'), 'off');
    assert.equal(getDefaultThinkingLevel('claude-haiku-4'), 'off');
  });

  it('returns off for an empty or unknown id', () => {
    assert.equal(getDefaultThinkingLevel(''), 'off');
    assert.equal(getDefaultThinkingLevel('totally-made-up-model'), 'off');
  });
});

// ---------------------------------------------------------------------------
// cycleThinkingLevel — wraparound effort cycling with clamp-to-high (U051)
// ---------------------------------------------------------------------------
describe('cycleThinkingLevel (U051)', () => {
  it('steps up one rung', () => {
    assert.equal(cycleThinkingLevel('off', 1), 'low');
    assert.equal(cycleThinkingLevel('low', 1), 'medium');
    assert.equal(cycleThinkingLevel('medium', 1), 'high');
    assert.equal(cycleThinkingLevel('high', 1), 'max');
  });

  it('steps down one rung', () => {
    assert.equal(cycleThinkingLevel('max', -1), 'high');
    assert.equal(cycleThinkingLevel('low', -1), 'off');
  });

  it('wraps around at both ends', () => {
    assert.equal(cycleThinkingLevel('max', 1), 'off');
    assert.equal(cycleThinkingLevel('off', -1), 'max');
  });

  it('excludes the max rung when includeMax is false (clamp to high)', () => {
    assert.equal(cycleThinkingLevel('high', 1, false), 'off'); // wraps without max
    assert.equal(cycleThinkingLevel('medium', 1, false), 'high');
    // A current level of `max` is not in the reduced scale → starts from the first rung.
    assert.equal(cycleThinkingLevel('max', 1, false), 'low');
  });

  it('treats an unknown current level as the first rung', () => {
    assert.equal(cycleThinkingLevel('bogus' as ThinkingLevel, 1), 'low');
    assert.equal(cycleThinkingLevel('bogus' as ThinkingLevel, -1), 'max');
  });

  it('every cycle result is a valid thinking level', () => {
    for (const lvl of THINKING_LEVELS) {
      assert.ok((THINKING_LEVELS as readonly string[]).includes(cycleThinkingLevel(lvl, 1)));
      assert.ok((THINKING_LEVELS as readonly string[]).includes(cycleThinkingLevel(lvl, -1)));
    }
  });
});
