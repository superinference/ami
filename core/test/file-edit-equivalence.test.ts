// Unit U039 — edit-equivalence helper (parity: Claude Code's
// FileEditTool/utils.ts areFileEditsEquivalent): apply-both-and-compare edit
// equivalence, including equal-error equivalence, reusing the ONE canonical edit
// engine (fuzzyFindAndReplace) so a verdict reflects the tool's real output.
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  applyEditsToContent,
  areFileEditsEquivalent,
  type FileEditLike,
} from '../src/tools/file-edit-equivalence';

const ORIGINAL = 'alpha\nbeta\ngamma\n';

describe('applyEditsToContent', () => {
  it('applies a single exact replacement', () => {
    assert.equal(
      applyEditsToContent(ORIGINAL, [{ old_string: 'beta', new_string: 'BETA' }]),
      'alpha\nBETA\ngamma\n',
    );
  });

  it('applies edits in order, each seeing the prior result', () => {
    const out = applyEditsToContent(ORIGINAL, [
      { old_string: 'alpha', new_string: 'one' },
      { old_string: 'gamma', new_string: 'three' },
    ]);
    assert.equal(out, 'one\nbeta\nthree\n');
  });

  it('honors replace_all for a repeated needle', () => {
    const out = applyEditsToContent('x x x', [
      { old_string: 'x', new_string: 'y', replace_all: true },
    ]);
    assert.equal(out, 'y y y');
  });

  it('normalizes CRLF so line endings do not create a false divergence', () => {
    const out = applyEditsToContent('a\r\nb\r\n', [{ old_string: 'b', new_string: 'B' }]);
    assert.equal(out, 'a\nB\n');
  });

  it('throws a deterministic message for an empty needle', () => {
    assert.throws(
      () => applyEditsToContent(ORIGINAL, [{ old_string: '', new_string: 'x' }]),
      /old_string must not be empty/,
    );
  });

  it('throws "not found" when the needle is absent', () => {
    assert.throws(
      () => applyEditsToContent(ORIGINAL, [{ old_string: 'zzz', new_string: 'x' }]),
      /old_string not found/,
    );
  });

  it('throws an ambiguity message for a multi-match needle without replace_all', () => {
    assert.throws(
      () => applyEditsToContent('x x x', [{ old_string: 'x', new_string: 'y' }]),
      /matches 3 locations/,
    );
  });
});

describe('areFileEditsEquivalent', () => {
  it('literal fast path: byte-identical edit sets are equivalent', () => {
    const a: FileEditLike[] = [{ old_string: 'beta', new_string: 'BETA' }];
    const b: FileEditLike[] = [{ old_string: 'beta', new_string: 'BETA' }];
    assert.equal(areFileEditsEquivalent(a, b, ORIGINAL), true);
  });

  it('literal fast path distinguishes replace_all', () => {
    const a: FileEditLike[] = [{ old_string: 'beta', new_string: 'BETA', replace_all: true }];
    const b: FileEditLike[] = [{ old_string: 'beta', new_string: 'BETA' }];
    // Different inputs → falls to semantic path; same single match → same result → equivalent.
    assert.equal(areFileEditsEquivalent(a, b, ORIGINAL), true);
  });

  it('semantic path: different phrasing producing the same file is equivalent', () => {
    // One set does it in two edits, the other in one — same resulting file.
    const a: FileEditLike[] = [
      { old_string: 'alpha', new_string: 'A' },
      { old_string: 'beta', new_string: 'B' },
    ];
    const b: FileEditLike[] = [
      { old_string: 'alpha\nbeta', new_string: 'A\nB' },
    ];
    assert.equal(areFileEditsEquivalent(a, b, ORIGINAL), true);
  });

  it('non-equivalent edits producing different files are not equivalent', () => {
    const a: FileEditLike[] = [{ old_string: 'beta', new_string: 'B' }];
    const b: FileEditLike[] = [{ old_string: 'gamma', new_string: 'G' }];
    assert.equal(areFileEditsEquivalent(a, b, ORIGINAL), false);
  });

  it('both-fail-identically are equivalent (reproducible failure)', () => {
    const a: FileEditLike[] = [{ old_string: 'zzz', new_string: 'x' }];
    const b: FileEditLike[] = [{ old_string: 'zzz', new_string: 'y' }];
    // Different inputs, both throw "not found" → equal errors → equivalent.
    assert.equal(areFileEditsEquivalent(a, b, ORIGINAL), true);
  });

  it('both-fail-with-different-errors are not equivalent', () => {
    const a: FileEditLike[] = [{ old_string: 'zzz', new_string: 'x' }]; // not found
    const b: FileEditLike[] = [{ old_string: '', new_string: 'y' }]; // empty needle
    assert.equal(areFileEditsEquivalent(a, b, ORIGINAL), false);
  });

  it('one-succeeds-one-fails are not equivalent', () => {
    const a: FileEditLike[] = [{ old_string: 'beta', new_string: 'B' }]; // succeeds
    const b: FileEditLike[] = [{ old_string: 'zzz', new_string: 'x' }]; // fails
    assert.equal(areFileEditsEquivalent(a, b, ORIGINAL), false);
  });
});
