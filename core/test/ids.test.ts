/**
 * Unit tests for the canonical UUID boundary validator (core/src/ids.ts).
 *
 * Parity target: the reference's `validateUuid`
 * (references/claude/claude-code-main/src/utils/uuid.ts:L12-L17) — accept
 * `unknown`, type-guard to a string, match the anchored 8-4-4-4-12 regex, and
 * return the narrowed string or `null`, never throwing.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import {
  validateUuid,
  shortRequestId,
  HUMAN_ID_ALPHABET,
  HUMAN_ID_LENGTH,
  ID_AVOID_SUBSTRINGS,
} from '../src/ids';

describe('validateUuid', () => {
  it('accepts a canonical lower-case UUID and returns it narrowed', () => {
    const u = 'a3f2c1b4-d5e6-47a8-9b0c-1d2e3f4a5b6c';
    const out = validateUuid(u);
    assert.equal(out, u);
    // Type-narrowing: the result is usable as a string without a cast.
    assert.equal(typeof out, 'string');
  });

  it('accepts an upper-case UUID (case-insensitive)', () => {
    const u = 'A3F2C1B4-D5E6-47A8-9B0C-1D2E3F4A5B6C';
    assert.equal(validateUuid(u), u);
  });

  it('accepts every crypto.randomUUID() value (the ids AMI actually mints)', () => {
    for (let i = 0; i < 50; i++) {
      const u = randomUUID();
      assert.equal(validateUuid(u), u);
    }
  });

  it('returns null for a non-string (number, object, array, null, undefined, boolean)', () => {
    for (const bad of [123, {}, [], null, undefined, true, Symbol('x') as unknown]) {
      assert.equal(validateUuid(bad), null);
    }
  });

  it('returns null for the empty string', () => {
    assert.equal(validateUuid(''), null);
  });

  it('rejects wrong grouping / length', () => {
    assert.equal(validateUuid('a3f2c1b4d5e647a89b0c1d2e3f4a5b6c'), null); // no dashes
    assert.equal(validateUuid('a3f2c1b4-d5e6-47a8-9b0c-1d2e3f4a5b6'), null); // 11 in last group
    assert.equal(validateUuid('a3f2c1b4-d5e6-47a8-9b0c-1d2e3f4a5b6cc'), null); // 13 in last group
    assert.equal(validateUuid('a3f2c1b4-d5e6-47a8-1d2e3f4a5b6c'), null); // missing a group
  });

  it('rejects non-hex characters', () => {
    assert.equal(validateUuid('g3f2c1b4-d5e6-47a8-9b0c-1d2e3f4a5b6c'), null); // leading g
    assert.equal(validateUuid('z3f2c1b4-d5e6-47a8-9b0c-1d2e3f4a5b6c'), null);
  });

  it('is anchored — rejects a canonical UUID with surrounding text', () => {
    const u = 'a3f2c1b4-d5e6-47a8-9b0c-1d2e3f4a5b6c';
    assert.equal(validateUuid(` ${u}`), null);
    assert.equal(validateUuid(`${u} `), null);
    assert.equal(validateUuid(`prefix-${u}`), null);
    assert.equal(validateUuid(`${u}\n${u}`), null); // multiline not allowed
  });

  it('rejects AMI\'s own opaque (non-UUID) id shapes — proving this is NOT a session-id validator', () => {
    assert.equal(validateUuid('session-20261118-3'), null);
    assert.equal(validateUuid('agent-a3f2c1b4'), null);
    assert.equal(validateUuid('wt-a3f2c1b4'), null);
  });
});

describe('shortRequestId (human-facing short id)', () => {
  it('produces a HUMAN_ID_LENGTH code over the confusable-free alphabet', () => {
    assert.equal(HUMAN_ID_LENGTH, 5);
    const code = shortRequestId('toolu_abc123');
    assert.equal(code.length, HUMAN_ID_LENGTH);
    for (const ch of code) {
      assert.ok(HUMAN_ID_ALPHABET.includes(ch), `char ${ch} not in alphabet`);
    }
  });

  it('never emits the confusable glyph "l" (looks like 1/I)', () => {
    assert.equal(HUMAN_ID_ALPHABET.includes('l'), false);
    assert.equal(HUMAN_ID_ALPHABET.length, 25);
    for (let i = 0; i < 20000; i++) {
      assert.equal(shortRequestId(`src-${i}`).includes('l'), false);
    }
  });

  it('is letters-only (no digits — phone users never toggle keyboard modes)', () => {
    for (let i = 0; i < 2000; i++) {
      assert.match(shortRequestId(`tool-${i}`), /^[a-z]{5}$/);
    }
  });

  it('is deterministic — the same source always yields the same code', () => {
    assert.equal(shortRequestId('toolu_xyz'), shortRequestId('toolu_xyz'));
    assert.equal(shortRequestId(''), shortRequestId(''));
  });

  it('never surfaces a blocklisted substring, across a large sample (exercises re-hash)', () => {
    for (let i = 0; i < 20000; i++) {
      const code = shortRequestId(`x${i}`);
      for (const bad of ID_AVOID_SUBSTRINGS) {
        assert.equal(code.includes(bad), false, `code ${code} contains ${bad} for x${i}`);
      }
    }
  });

  it('re-hashes a source whose first-pass code spells a blocklisted word', () => {
    // `toolu_447` hashes (unsalted) to `zypee` which contains the blocklisted
    // substring `pee`; shortRequestId must salt-rehash to a clean code.
    const code = shortRequestId('toolu_447');
    assert.equal(code, 'rzbdn');
    assert.notEqual(code, 'zypee');
    for (const bad of ID_AVOID_SUBSTRINGS) {
      assert.equal(code.includes(bad), false);
    }
  });

  it('handles the empty string and non-ASCII input without throwing', () => {
    assert.match(shortRequestId(''), /^[a-z]{5}$/);
    assert.match(shortRequestId('日本語-🔥'), /^[a-z]{5}$/);
  });

  it('spreads distinct sources across many distinct codes', () => {
    const codes = new Set<string>();
    for (let i = 0; i < 1000; i++) codes.add(shortRequestId(`req-${i}`));
    // FNV-1a over varied inputs should give near-unique codes at this scale.
    assert.ok(codes.size > 950, `only ${codes.size} distinct codes`);
  });

  it('exposes a frozen, non-empty blocklist', () => {
    assert.ok(Object.isFrozen(ID_AVOID_SUBSTRINGS));
    assert.ok(ID_AVOID_SUBSTRINGS.length > 0);
    assert.ok(ID_AVOID_SUBSTRINGS.includes('fuck'));
  });
});
