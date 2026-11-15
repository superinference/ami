import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  fuzzyFindAndReplace,
  STRATEGY_NAMES,
  type FuzzyResult,
} from '../src/tools/fuzzy-match';

// ===========================================================================
// Core-parity hardening for the edit fuzzy matcher (additive superset vs the
// reference edit matcher). Each describe block is a RED-GREEN reproducer: it
// discriminates the hardened behavior from the pre-fix behavior. Reverting the
// corresponding change in src/tools/fuzzy-match.ts turns the block red.
// ===========================================================================

// ---------------------------------------------------------------------------
// GAP 1 — empty old_string must be rejected, never clobber the file.
//
// Pre-fix: an empty old_string fell through to the line/escape strategies and
// either matched a blank line (line_trimmed) or reported every byte offset as a
// match (escape indexOf of ''), silently rewriting the file. Parity requires a
// locatable anchor; empty input is a clean typed failure.
//
// RED: remove the `if (oldString === '')` guard at the top of
//      fuzzyFindAndReplace -> error is no longer 'old_string is empty'
//      (becomes a spurious replacement or an ambiguity error).
// ---------------------------------------------------------------------------
describe('parity: empty old_string is rejected up front', () => {
  it('returns a clean typed failure instead of clobbering a blank line', () => {
    const content = 'line one\n\nline three\n';
    const r: FuzzyResult = fuzzyFindAndReplace(content, '', 'INSERTED');
    assert.equal(r.newContent, null);
    assert.equal(r.strategy, null);
    assert.equal(r.matchCount, 0);
    assert.equal(r.error, 'old_string is empty');
  });

  it('does not depend on the file containing a blank line', () => {
    const r = fuzzyFindAndReplace('solid\ncontent\n', '', 'X');
    assert.equal(r.newContent, null);
    assert.equal(r.error, 'old_string is empty');
  });
});

// ---------------------------------------------------------------------------
// GAP 2 — a whitespace-only old_string must not match the file's blank lines.
//
// Pre-fix: "   " normalized to '' under line_trimmed/whitespace_normalized and
// matched the file's single blank line, silently replacing it. A pattern that
// normalizes to nothing but empty lines has no distinctive anchor.
//
// RED: remove the `if (normPatternLines.every(l => l === '')) return []` guard
//      in findByLine -> the whitespace-only pattern matches the blank line and
//      newContent becomes non-null.
// ---------------------------------------------------------------------------
describe('parity: whitespace-only old_string does not match blank lines', () => {
  it('falls through to not-found instead of replacing a blank line', () => {
    const content = 'alpha\n\nbeta\n';
    const r = fuzzyFindAndReplace(content, '   ', 'X');
    assert.equal(r.newContent, null);
    assert.match(r.error ?? '', /not found/);
  });

  it('a genuine exact run of spaces still matches (guard is line-based only)', () => {
    // Three literal spaces exist exactly once -> exact strategy still resolves.
    const content = 'a   b and more text';
    const r = fuzzyFindAndReplace(content, '   ', '_');
    assert.equal(r.strategy, 'exact');
    assert.equal(r.newContent, 'a_b and more text');
  });
});

// ---------------------------------------------------------------------------
// GAP 3 — block-anchor strategy: match a >=3-line block on its first and last
// (trimmed) lines when the interior drifted. This is the headline anchor-based
// parity gap; without it an edit whose block body changed is "not found".
//
// RED: remove the { name: 'block_anchor', find: findBlockAnchor } entry from
//      STRATEGIES -> these matches become not-found (strategy null).
// ---------------------------------------------------------------------------
describe('parity: block_anchor matches a block whose interior drifted', () => {
  it('matches on first+last lines, ignoring a changed middle line', () => {
    const content = 'function f() {\n  const x = 1;\n  return x;\n}\n';
    const old = 'function f() {\n  const y = 2;\n  return x;\n}';
    const r = fuzzyFindAndReplace(content, old, 'function f() {\n  return 42;\n}');
    assert.equal(r.strategy, 'block_anchor');
    assert.equal(r.matchCount, 1);
    // Position reporting: only the anchored block is replaced; nothing else moves.
    assert.equal(r.newContent, 'function f() {\n  return 42;\n}\n');
  });

  it('is the last resort — exact still wins when the block is verbatim', () => {
    const content = 'open\n  mid\nclose\n';
    const r = fuzzyFindAndReplace(content, 'open\n  mid\nclose', 'X');
    assert.equal(r.strategy, 'exact');
  });

  it('block_anchor is the final name in the strictness order', () => {
    assert.equal(STRATEGY_NAMES[STRATEGY_NAMES.length - 1], 'block_anchor');
  });
});

// ---------------------------------------------------------------------------
// GAP 3b — block_anchor ambiguity detection. Two blocks sharing the same first
// and last lines must surface as an ambiguity error, not a silent pick.
//
// RED: remove the block_anchor STRATEGIES entry -> matchCount/ambiguity error
//      disappear (becomes not-found).
// ---------------------------------------------------------------------------
describe('parity: block_anchor enforces a unique match', () => {
  it('reports >1 candidates as an ambiguity error', () => {
    const content = 'if (a) {\n  x();\n}\nif (a) {\n  y();\n}\n';
    const r = fuzzyFindAndReplace(content, 'if (a) {\n  z();\n}', 'Q');
    assert.equal(r.strategy, 'block_anchor');
    assert.equal(r.matchCount, 2);
    assert.equal(r.newContent, null);
    assert.match(r.error ?? '', /2 locations/);
  });
});

// ---------------------------------------------------------------------------
// GAP 3c — block_anchor safety bounds. Blank boundary lines are not distinctive
// enough to anchor on, so a >=3-line pattern with empty first/last lines must
// NOT anchor-match blank-bounded regions of the file.
//
// RED: remove the `if (firstAnchor.length === 0 || lastAnchor.length === 0)`
//      guard in findBlockAnchor -> the blank-anchored pattern matches and
//      newContent becomes non-null.
// ---------------------------------------------------------------------------
describe('parity: block_anchor refuses blank boundary anchors', () => {
  it('does not anchor a block on empty first/last lines', () => {
    const content = '\nxxx\n\nyyy\nzzz\n';
    const old = '\nQQQ\n\n'; // 3 logical lines ['', 'QQQ', ''] — both anchors blank
    const r = fuzzyFindAndReplace(content, old, 'Z');
    assert.equal(r.newContent, null);
    assert.match(r.error ?? '', /not found/);
  });
});
