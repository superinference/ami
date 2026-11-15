import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { expandBraces, estimateBraceExpansions } from '../src/brace-expansion';

describe('expandBraces', () => {
  it('expands a single group', () => {
    assert.deepEqual(expandBraces('{a,b,c}'), ['a', 'b', 'c']);
  });

  it('expands with prefix and suffix', () => {
    assert.deepEqual(expandBraces('src/{a,b}.ts'), ['src/a.ts', 'src/b.ts']);
  });

  it('expands multiple groups (cartesian product)', () => {
    const result = expandBraces('{a,b}.{ts,js}');
    assert.deepEqual(result, ['a.ts', 'a.js', 'b.ts', 'b.js']);
  });

  it('handles nested braces', () => {
    const result = expandBraces('{a,{b,c}}');
    assert.deepEqual(result, ['a', 'b', 'c']);
  });

  it('passes through strings without braces', () => {
    assert.deepEqual(expandBraces('plain.ts'), ['plain.ts']);
  });

  it('handles empty alternative', () => {
    const result = expandBraces('{,b}');
    assert.deepEqual(result, ['', 'b']);
  });

  it('handles single-item brace (no comma)', () => {
    const result = expandBraces('{a}');
    assert.deepEqual(result, ['{a}']);
  });

  it('throws when expansion exceeds default budget', () => {
    // 2^20 = 1,048,576 — well over 1000 default budget
    const pattern = Array.from({ length: 20 }, () => '{a,b}').join('');
    assert.throws(() => expandBraces(pattern), /exceeded budget/);
  });

  it('throws when expansion exceeds custom budget', () => {
    assert.throws(() => expandBraces('{a,b,c}.{d,e,f}', 5), /exceeded budget.*9.*max 5/);
  });

  it('respects custom budget when under limit', () => {
    const result = expandBraces('{a,b}.{c,d}', 10);
    assert.equal(result.length, 4);
  });

  it('handles deeply nested braces', () => {
    const result = expandBraces('{a,{b,{c,d}}}');
    assert.deepEqual(result, ['a', 'b', 'c', 'd']);
  });

  it('handles real-world CLAUDE.md path pattern', () => {
    const result = expandBraces('src/{components,utils}/{index,types}.ts');
    assert.deepEqual(result, [
      'src/components/index.ts',
      'src/components/types.ts',
      'src/utils/index.ts',
      'src/utils/types.ts',
    ]);
  });

  it('handles escaped braces', () => {
    const result = expandBraces('\\{a,b\\}');
    assert.deepEqual(result, ['\\{a,b\\}']);
  });

  it('handles three groups', () => {
    const result = expandBraces('{a,b}{c,d}{e,f}');
    assert.equal(result.length, 8);
    assert.ok(result.includes('ace'));
    assert.ok(result.includes('bdf'));
  });

  it('handles empty string', () => {
    assert.deepEqual(expandBraces(''), ['']);
  });

  it('handles brace at start', () => {
    assert.deepEqual(expandBraces('{x,y}z'), ['xz', 'yz']);
  });

  it('handles brace at end', () => {
    assert.deepEqual(expandBraces('z{x,y}'), ['zx', 'zy']);
  });
});

describe('estimateBraceExpansions', () => {
  it('returns 1 for no braces', () => {
    assert.equal(estimateBraceExpansions('plain.ts'), 1);
  });

  it('returns correct count for single group', () => {
    assert.equal(estimateBraceExpansions('{a,b,c}'), 3);
  });

  it('returns correct count for multiple groups', () => {
    assert.equal(estimateBraceExpansions('{a,b}.{c,d}'), 4);
  });

  it('returns large count for many groups', () => {
    const pattern = Array.from({ length: 20 }, () => '{a,b}').join('');
    assert.ok(estimateBraceExpansions(pattern) > 1_000_000);
  });

  it('returns correct count for three groups', () => {
    assert.equal(estimateBraceExpansions('{a,b}{c,d}{e,f}'), 8);
  });
});

// ---------------------------------------------------------------------------
// Edge cases — escape handling inside braces, unmatched braces
// ---------------------------------------------------------------------------

describe('expandBraces — escape and edge cases', () => {
  it('handles escaped comma inside braces', () => {
    const result = expandBraces('{a\\,b,c}');
    assert.equal(result.length, 2);
    assert.ok(result.includes('a\\,b'));
    assert.ok(result.includes('c'));
  });

  it('handles escaped braces inside braces', () => {
    const result = expandBraces('{a,\\{b}');
    assert.equal(result.length, 2);
    assert.ok(result.includes('a'));
    assert.ok(result.includes('\\{b'));
  });

  it('returns pattern unchanged for single alternative in braces', () => {
    const result = expandBraces('{single}');
    assert.deepEqual(result, ['{single}']);
  });

  it('returns pattern unchanged for unmatched opening brace', () => {
    const result = expandBraces('{unclosed');
    assert.deepEqual(result, ['{unclosed']);
  });

  it('handles nested braces', () => {
    const result = expandBraces('{a,{b,c}}');
    assert.ok(result.includes('a'));
    assert.ok(result.includes('b'));
    assert.ok(result.includes('c'));
  });

  it('handles escape at end of pattern', () => {
    const result = expandBraces('test\\');
    assert.deepEqual(result, ['test\\']);
  });

  it('handles escape before closing brace', () => {
    const result = expandBraces('{a,b\\}}');
    assert.ok(result.length > 0);
  });
});

// ---------------------------------------------------------------------------
// Sequence / range brace expansion ({N..M}, {a..z}, {N..M..S})
// ---------------------------------------------------------------------------

describe('expandBraces — sequence/range expansion', () => {
  it('expands a numeric range', () => {
    assert.deepEqual(expandBraces('{1..5}'), ['1', '2', '3', '4', '5']);
  });

  it('expands a numeric range with prefix and suffix', () => {
    assert.deepEqual(expandBraces('file{1..3}.txt'), ['file1.txt', 'file2.txt', 'file3.txt']);
  });

  it('expands a descending numeric range', () => {
    assert.deepEqual(expandBraces('{5..1}'), ['5', '4', '3', '2', '1']);
  });

  it('expands a lowercase letter range', () => {
    assert.deepEqual(expandBraces('{a..e}'), ['a', 'b', 'c', 'd', 'e']);
  });

  it('expands an uppercase letter range', () => {
    assert.deepEqual(expandBraces('{A..C}'), ['A', 'B', 'C']);
  });

  it('expands a descending letter range', () => {
    assert.deepEqual(expandBraces('{e..a}'), ['e', 'd', 'c', 'b', 'a']);
  });

  it('expands a numeric range with a step', () => {
    assert.deepEqual(expandBraces('{1..10..2}'), ['1', '3', '5', '7', '9']);
  });

  it('expands a letter range with a step', () => {
    assert.deepEqual(expandBraces('{a..g..2}'), ['a', 'c', 'e', 'g']);
  });

  it('preserves zero-padding in numeric ranges', () => {
    assert.deepEqual(expandBraces('{01..03}'), ['01', '02', '03']);
  });

  it('zero-pads to the widest endpoint', () => {
    assert.deepEqual(expandBraces('{08..11}'), ['08', '09', '10', '11']);
  });

  it('expands a negative numeric range', () => {
    assert.deepEqual(expandBraces('{-1..1}'), ['-1', '0', '1']);
  });

  it('combines a range with a comma group (cartesian product)', () => {
    assert.deepEqual(expandBraces('{1..2}{a,b}'), ['1a', '1b', '2a', '2b']);
  });

  it('handles the real-world log{1..9}.txt pattern', () => {
    const result = expandBraces('log{1..9}.txt');
    assert.equal(result.length, 9);
    assert.ok(result.includes('log1.txt'));
    assert.ok(result.includes('log9.txt'));
  });

  it('leaves a mixed-type range unexpanded', () => {
    assert.deepEqual(expandBraces('{1..a}'), ['{1..a}']);
  });

  it('leaves a multi-character non-numeric range unexpanded', () => {
    assert.deepEqual(expandBraces('{aa..bb}'), ['{aa..bb}']);
  });

  it('leaves a zero-step range unexpanded', () => {
    assert.deepEqual(expandBraces('{1..5..0}'), ['{1..5..0}']);
  });

  it('leaves a non-integer step unexpanded', () => {
    assert.deepEqual(expandBraces('{1..5..x}'), ['{1..5..x}']);
  });

  it('throws when a large range exceeds the budget', () => {
    assert.throws(() => expandBraces('{1..100000}'), /exceeded budget/);
  });

  it('respects a custom budget for a range under the limit', () => {
    const result = expandBraces('{1..10}', 50);
    assert.equal(result.length, 10);
  });
});

describe('estimateBraceExpansions — range cardinality', () => {
  it('counts a numeric range', () => {
    assert.equal(estimateBraceExpansions('{1..100}'), 100);
  });

  it('counts a stepped numeric range', () => {
    assert.equal(estimateBraceExpansions('{1..10..2}'), 5);
  });

  it('counts a letter range', () => {
    assert.equal(estimateBraceExpansions('{a..e}'), 5);
  });

  it('caps an explosive range', () => {
    assert.ok(estimateBraceExpansions('{1..2000000}') > 1_000_000);
  });

  it('treats a non-range comma-less group as a single alternative', () => {
    assert.equal(estimateBraceExpansions('{single}'), 1);
  });
});

// ---------------------------------------------------------------------------
// Nesting-aware budget estimation (combinatorial-explosion guard)
// ---------------------------------------------------------------------------

describe('estimateBraceExpansions — nesting-aware counting', () => {
  it('counts braces nested inside an alternative', () => {
    // Old flat estimator counted only the top-level group (2); the true
    // expansion is 4 because each alternative contains a nested {a,b}.
    assert.equal(estimateBraceExpansions('{{a,b}p,{a,b}q}'), 8);
    assert.equal(expandBraces('{{a,b}p,{a,b}q}').length, 4);
  });

  it('counts a simple nested alternative as an upper bound', () => {
    assert.equal(estimateBraceExpansions('{a,{b,c}}'), 4);
  });

  it('blocks a nested combinatorial explosion the flat estimate missed', () => {
    // Each group expands to 4 (2 alternatives * nested {a,b}); five chained
    // groups expand to 4^5 = 1024 > the 1000 default budget. A flat,
    // top-level-only estimate would see only 2^5 = 32 and wrongly allow it.
    const group = '{{a,b}x,{a,b}y}';
    const pattern = group.repeat(5);
    assert.throws(() => expandBraces(pattern), /exceeded budget/);
  });
});
