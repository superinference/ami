import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'crypto';
import { stableStringify, stableChecksum } from '../src/utils/stable-hash';

test('stableStringify sorts object keys recursively (order-insensitive)', () => {
  const a = stableStringify({ b: 1, a: { d: 4, c: 3 } });
  const b = stableStringify({ a: { c: 3, d: 4 }, b: 1 });
  assert.equal(a, b);
  assert.equal(a, '{"a":{"c":3,"d":4},"b":1}');
});

test('stableStringify preserves array order (position is significant)', () => {
  assert.equal(stableStringify([3, 1, 2]), '[3,1,2]');
  assert.notEqual(stableStringify([1, 2]), stableStringify([2, 1]));
});

test('stableStringify handles null/undefined/primitive without throwing', () => {
  assert.equal(stableStringify(null), 'null');
  assert.equal(stableStringify(undefined), 'undefined');
  assert.equal(stableStringify(42), '42');
  assert.equal(stableStringify('x'), '"x"');
  // undefined inside an object round-trips via String(undefined)
  assert.equal(stableStringify({ a: undefined }), '{"a":undefined}');
});

test('stableChecksum is sha256:<hex> over the deep-sorted serialization', () => {
  const value = { z: 1, a: [{ y: 2, x: 1 }] };
  const expected =
    'sha256:' + createHash('sha256').update(stableStringify(value)).digest('hex');
  assert.equal(stableChecksum(value), expected);
  assert.match(stableChecksum(value), /^sha256:[0-9a-f]{64}$/);
});

test('stableChecksum is identical for key-reordered but structurally-equal inputs', () => {
  assert.equal(
    stableChecksum({ name: 'c', version: '1', skills: [{ name: 's' }] }),
    stableChecksum({ skills: [{ name: 's' }], version: '1', name: 'c' }),
  );
});

test('stableChecksum differs when content changes', () => {
  assert.notEqual(
    stableChecksum({ name: 'c', version: '1' }),
    stableChecksum({ name: 'c', version: '2' }),
  );
});
