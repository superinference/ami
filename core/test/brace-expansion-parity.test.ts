import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { expandBraces, estimateBraceExpansions } from '../src/brace-expansion';

// ---------------------------------------------------------------------------
// Parity gaps closed in brace-expansion.ts:
//   1. Numeric / character sequence ranges ({N..M}, {a..z}).
//   2. Optional step ({N..M..S}), descending ranges, negative endpoints.
//   3. Zero-padded numeric ranges ({01..03}).
//   4. Malformed / mixed-type ranges left literal.
//   5. Nested-depth estimate (DoS-guard upper bound) + range cardinality cap.
// Each describe below is a RED-GREEN reproducer: it fails on origin/main
// (where ranges are not expanded and the estimate is top-level-only) and
// passes with the fix.
// ---------------------------------------------------------------------------

describe('brace-expansion parity — numeric sequence ranges', () => {
  it('expands an ascending numeric range {1..5}', () => {
    assert.deepEqual(expandBraces('{1..5}'), ['1', '2', '3', '4', '5']);
  });

  it('expands a descending numeric range {5..1}', () => {
    assert.deepEqual(expandBraces('{5..1}'), ['5', '4', '3', '2', '1']);
  });

  it('expands a range with prefix and suffix', () => {
    assert.deepEqual(expandBraces('f{1..3}.ts'), ['f1.ts', 'f2.ts', 'f3.ts']);
  });

  it('expands negative endpoints {-2..2}', () => {
    assert.deepEqual(expandBraces('{-2..2}'), ['-2', '-1', '0', '1', '2']);
  });

  it('combines a range group with a comma group (cartesian product)', () => {
    assert.deepEqual(expandBraces('{1..2}{a,b}'), ['1a', '1b', '2a', '2b']);
  });
});

describe('brace-expansion parity — step sequences', () => {
  it('honors a positive step {1..10..2}', () => {
    assert.deepEqual(expandBraces('{1..10..2}'), ['1', '3', '5', '7', '9']);
  });

  it('applies the step magnitude regardless of sign for descending ranges', () => {
    assert.deepEqual(expandBraces('{10..1..3}'), ['10', '7', '4', '1']);
  });

  it('treats a zero step as a literal group (never loops forever)', () => {
    assert.deepEqual(expandBraces('{1..5..0}'), ['{1..5..0}']);
  });

  it('treats a non-integer step as a literal group', () => {
    assert.deepEqual(expandBraces('{1..5..x}'), ['{1..5..x}']);
  });
});

describe('brace-expansion parity — character ranges', () => {
  it('expands an ascending character range {a..e}', () => {
    assert.deepEqual(expandBraces('{a..e}'), ['a', 'b', 'c', 'd', 'e']);
  });

  it('expands a descending character range {C..A}', () => {
    assert.deepEqual(expandBraces('{C..A}'), ['C', 'B', 'A']);
  });

  it('expands a character range with a step {a..g..2}', () => {
    assert.deepEqual(expandBraces('{a..g..2}'), ['a', 'c', 'e', 'g']);
  });
});

describe('brace-expansion parity — zero padding', () => {
  it('preserves leading-zero width across the range {01..03}', () => {
    assert.deepEqual(expandBraces('{01..03}'), ['01', '02', '03']);
  });

  it('pads to the widest endpoint {08..11}', () => {
    assert.deepEqual(expandBraces('{08..11}'), ['08', '09', '10', '11']);
  });

  it('does not pad when neither endpoint carries a leading zero', () => {
    assert.deepEqual(expandBraces('{8..11}'), ['8', '9', '10', '11']);
  });
});

describe('brace-expansion parity — malformed ranges stay literal', () => {
  it('leaves a mixed-type range {1..a} unexpanded', () => {
    assert.deepEqual(expandBraces('{1..a}'), ['{1..a}']);
  });

  it('leaves a multi-character non-numeric range {aa..zz} unexpanded', () => {
    assert.deepEqual(expandBraces('{aa..zz}'), ['{aa..zz}']);
  });

  it('leaves an empty range body {..} unexpanded', () => {
    assert.deepEqual(expandBraces('{..}'), ['{..}']);
  });
});

describe('brace-expansion parity — nested-depth estimate (DoS guard)', () => {
  it('multiplies nested groups, not just top-level ones', () => {
    // Old top-level-only estimate returned 2; the real expansion is 3 and the
    // upper-bound estimate is 4. The estimate must be >= the true count.
    assert.equal(estimateBraceExpansions('{a,{b,c}}'), 4);
    assert.ok(estimateBraceExpansions('{a,{b,c}}') >= expandBraces('{a,{b,c}}').length);
  });

  it('accounts for alternatives whose members each contain a group', () => {
    // Real expansion count is 4 (ap, bp, aq, bq); upper bound is 4.
    const est = estimateBraceExpansions('{{a,b}p,{a,b}q}');
    assert.ok(est >= expandBraces('{{a,b}p,{a,b}q}').length);
    assert.equal(expandBraces('{{a,b}p,{a,b}q}').length, 4);
  });

  it('counts a sequence range toward the budget estimate', () => {
    assert.equal(estimateBraceExpansions('{1..5}'), 5);
    assert.equal(estimateBraceExpansions('{1..10..2}'), 5);
  });

  it('rejects a huge range before enumerating it', () => {
    assert.throws(() => expandBraces('{1..100000}'), /exceeded budget/);
  });

  it('short-circuits a range that blows past the estimate cap', () => {
    const est = estimateBraceExpansions('{1..100000000}');
    assert.ok(est > 1_000_000);
    assert.ok(Number.isFinite(est));
  });
});
