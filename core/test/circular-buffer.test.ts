import { test } from 'node:test';
import assert from 'node:assert/strict';
import { CircularBuffer } from '../src/utils/circular-buffer';

test('CircularBuffer: empty state', () => {
  const b = new CircularBuffer<number>(3);
  assert.equal(b.capacity, 3);
  assert.equal(b.size, 0);
  assert.equal(b.isEmpty, true);
  assert.deepEqual(b.toArray(), []);
  assert.deepEqual(b.getRecent(5), []);
  assert.equal(b.last, undefined);
});

test('CircularBuffer: fills up to capacity in insertion order', () => {
  const b = new CircularBuffer<number>(3);
  b.add(1).add(2);
  assert.equal(b.size, 2);
  assert.equal(b.isEmpty, false);
  assert.deepEqual(b.toArray(), [1, 2]);
  assert.equal(b.last, 2);
  b.add(3);
  assert.equal(b.size, 3);
  assert.deepEqual(b.toArray(), [1, 2, 3]);
});

test('CircularBuffer: evicts oldest, keeps newest N in order across wrap', () => {
  const b = new CircularBuffer<number>(3);
  for (const n of [1, 2, 3, 4, 5]) b.add(n);
  assert.equal(b.size, 3);
  // oldest->newest, 1 and 2 evicted
  assert.deepEqual(b.toArray(), [3, 4, 5]);
  assert.equal(b.last, 5);
});

test('CircularBuffer: getRecent returns trailing items across the wrap boundary', () => {
  const b = new CircularBuffer<string>(4);
  for (const s of ['a', 'b', 'c', 'd', 'e', 'f']) b.add(s); // retains c,d,e,f
  assert.deepEqual(b.getRecent(2), ['e', 'f']);
  assert.deepEqual(b.getRecent(4), ['c', 'd', 'e', 'f']);
  // count larger than size clamps to whole buffer
  assert.deepEqual(b.getRecent(99), ['c', 'd', 'e', 'f']);
  // non-positive count returns empty
  assert.deepEqual(b.getRecent(0), []);
  assert.deepEqual(b.getRecent(-1), []);
});

test('CircularBuffer: toArray returns a fresh array safe to mutate', () => {
  const b = new CircularBuffer<number>(3);
  b.add(3).add(1).add(2);
  const arr = b.toArray();
  arr.sort((x, y) => x - y);
  // mutating the returned array must not affect the buffer
  assert.deepEqual(b.toArray(), [3, 1, 2]);
});

test('CircularBuffer: clear resets size/head but preserves capacity', () => {
  const b = new CircularBuffer<number>(2);
  b.add(1).add(2).add(3);
  b.clear();
  assert.equal(b.size, 0);
  assert.equal(b.isEmpty, true);
  assert.deepEqual(b.toArray(), []);
  assert.equal(b.last, undefined);
  // reusable after clear; head reset so ordering is correct again
  b.add(9).add(8);
  assert.deepEqual(b.toArray(), [9, 8]);
});

test('CircularBuffer: rejects a non-positive / non-integer capacity', () => {
  assert.throws(() => new CircularBuffer<number>(0), RangeError);
  assert.throws(() => new CircularBuffer<number>(-3), RangeError);
  assert.throws(() => new CircularBuffer<number>(2.5), RangeError);
  assert.throws(() => new CircularBuffer<number>(Number.NaN), RangeError);
});

test('CircularBuffer: capacity 1 keeps only the most recent item', () => {
  const b = new CircularBuffer<number>(1);
  b.add(1).add(2).add(3);
  assert.equal(b.size, 1);
  assert.deepEqual(b.toArray(), [3]);
  assert.equal(b.last, 3);
  assert.deepEqual(b.getRecent(1), [3]);
});
