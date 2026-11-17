import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { graphemes, truncateActivity } from '../src/tools/text-truncate';

describe('text-truncate — graphemes', () => {
  it('splits ASCII into one cluster per char', () => {
    assert.deepEqual(graphemes('abc'), ['a', 'b', 'c']);
  });

  it('keeps a multi-code-unit emoji as a single cluster', () => {
    // Family emoji is multiple code points joined by ZWJ — one grapheme.
    assert.equal(graphemes('👨‍👩‍👧').length, 1);
  });
});

describe('text-truncate — truncateActivity', () => {
  it('returns a within-budget single-line string verbatim', () => {
    assert.equal(truncateActivity('hello', 40), 'hello');
  });

  it('clips an over-width string with a trailing ellipsis within budget', () => {
    const out = truncateActivity('x'.repeat(100), 10);
    assert.equal(out, 'xxxxxxxxx…'); // 9 chars + ellipsis = width 10
    assert.equal(graphemes(out).length, 10);
  });

  it('does not split a multi-code-unit grapheme at the cut boundary', () => {
    const out = truncateActivity('😀😀😀😀😀', 3);
    // 2 emoji + ellipsis; no lone surrogate.
    assert.equal(out, '😀😀…');
  });

  it('collapses a multi-line value to its first line with a marker when singleLine', () => {
    assert.equal(truncateActivity('one\ntwo\nthree', 40, true), 'one…');
  });

  it('width-truncates the first line when it alone exceeds the budget', () => {
    const out = truncateActivity('a'.repeat(50) + '\nrest', 10, true);
    assert.equal(graphemes(out).length, 10);
    assert.ok(out.endsWith('…'));
    assert.ok(!out.includes('rest'));
  });

  it('ignores newlines when singleLine is false', () => {
    assert.equal(truncateActivity('one\ntwo', 40, false), 'one\ntwo');
  });

  it('collapses to a bare ellipsis for a non-positive budget', () => {
    assert.equal(truncateActivity('anything', 0), '…');
    assert.equal(truncateActivity('anything', -5), '…');
  });
});
