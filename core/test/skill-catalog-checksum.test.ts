import { test } from 'node:test';
import assert from 'node:assert/strict';
import { validateCatalogIndex, isHexSha256 } from '../src/skill-catalog';

const HASH = 'a'.repeat(64);

test('isHexSha256 accepts a 64-char lowercase hex and rejects others', () => {
  assert.equal(isHexSha256(HASH), true);
  assert.equal(isHexSha256('A'.repeat(64)), false); // uppercase
  assert.equal(isHexSha256('a'.repeat(63)), false); // too short
  assert.equal(isHexSha256('z'.repeat(64)), false); // non-hex
  assert.equal(isHexSha256(123), false);
  assert.equal(isHexSha256(undefined), false);
});

test('validateCatalogIndex parses valid per-file checksums', () => {
  const r = validateCatalogIndex({
    skills: [{ name: 's', files: ['SKILL.md'], checksums: { 'SKILL.md': HASH } }],
  });
  assert.equal(r.valid, true);
  assert.deepEqual(r.index!.skills[0].checksums, { 'SKILL.md': HASH });
});

test('validateCatalogIndex flags checksum for unknown file', () => {
  const r = validateCatalogIndex({
    skills: [{ name: 's', files: ['SKILL.md'], checksums: { 'other.md': HASH } }],
  });
  // Skill still valid (files present) but a warning is recorded, checksum dropped.
  assert.ok(r.errors.some((e) => /unknown\/unsafe file/.test(e)));
  assert.equal(r.index!.skills[0].checksums, undefined);
});

test('validateCatalogIndex flags malformed checksum digest', () => {
  const r = validateCatalogIndex({
    skills: [{ name: 's', files: ['SKILL.md'], checksums: { 'SKILL.md': 'nope' } }],
  });
  assert.ok(r.errors.some((e) => /invalid SHA-256 checksum/.test(e)));
  assert.equal(r.index!.skills[0].checksums, undefined);
});

test('validateCatalogIndex flags non-object checksums', () => {
  const r = validateCatalogIndex({
    skills: [{ name: 's', files: ['SKILL.md'], checksums: ['x'] }],
  });
  assert.ok(r.errors.some((e) => /"checksums" must be an object/.test(e)));
});
