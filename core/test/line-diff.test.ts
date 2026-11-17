import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { computeLineDiff } from '../src/line-diff';

describe('computeLineDiff (core, canonical LCS)', () => {
  it('counts pure additions', () => {
    assert.deepEqual(computeLineDiff('l1\nl2', 'l1\nl2\nl3'), { linesAdded: 1, linesRemoved: 0 });
  });

  it('counts pure deletions', () => {
    assert.deepEqual(computeLineDiff('l1\nl2\nl3', 'l1\nl2'), { linesAdded: 0, linesRemoved: 1 });
  });

  it('counts an in-place edit as add+remove', () => {
    assert.deepEqual(computeLineDiff('a\nB\nc', 'a\nX\nc'), { linesAdded: 1, linesRemoved: 1 });
  });

  it('reports a reorder as moves (LCS, not multiset)', () => {
    assert.deepEqual(computeLineDiff('a\nb', 'b\na'), { linesAdded: 1, linesRemoved: 1 });
  });

  it('is 0/0 for identical content and empty/empty', () => {
    assert.deepEqual(computeLineDiff('same\ncontent', 'same\ncontent'), { linesAdded: 0, linesRemoved: 0 });
    assert.deepEqual(computeLineDiff('', ''), { linesAdded: 0, linesRemoved: 0 });
  });

  it('treats a non-empty new file as all-added', () => {
    assert.deepEqual(computeLineDiff('', 'a\nb\nc'), { linesAdded: 3, linesRemoved: 0 });
  });
});
