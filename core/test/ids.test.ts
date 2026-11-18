/**
 * Unit tests for the canonical UUID boundary validator (core/src/ids.ts).
 *
 * Parity target: Claude Code's `validateUuid`
 * (references/claude/claude-code-main/src/utils/uuid.ts:L12-L17) — accept
 * `unknown`, type-guard to a string, match the anchored 8-4-4-4-12 regex, and
 * return the narrowed string or `null`, never throwing.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { validateUuid } from '../src/ids';

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
