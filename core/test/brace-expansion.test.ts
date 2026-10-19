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
