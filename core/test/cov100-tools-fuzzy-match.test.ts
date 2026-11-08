import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  fuzzyFindAndReplace,
  reindentReplacement,
  findClosestLines,
} from '../src/tools/fuzzy-match';

// ===========================================================================
// Branch-coverage top-up for fuzzy-match.ts.
//
// These assert REAL behavior of paths the existing suite leaves cold:
//   * buildPositionMap's length-changing walk (literal `\n`, em-dash, smart
//     quote) — exercised only when escape_normalized wins AND the ORIGINAL
//     content (not just the pattern) holds the length-changing character.
//   * preserveQuoteStyle's curly-single-quote re-styling.
//   * reindentReplacement's first-line fallback when newString's first line
//     does not begin with old_string's base indent.
// ===========================================================================

describe('fuzzyFindAndReplace – escape_normalized position mapping (buildPositionMap walk)', () => {
  it('maps a literal "\\n" in the file back to original offsets (2-char → 1-char)', () => {
    // File holds the two literal characters backslash + n (not a newline).
    // normalizeEscapes turns `\n` into a real newline, shrinking the string by
    // one char, so buildPositionMap must walk the strings and take the
    // `\\n -> \n` branch. The pattern uses a real newline so only
    // escape_normalized can match (line strategies can't span a window larger
    // than the single logical content line).
    const content = 'a\\nb'; // a, backslash, n, b
    const result = fuzzyFindAndReplace(content, 'a\nb', 'a\nZZ');
    assert.equal(result.strategy, 'escape_normalized');
    assert.equal(result.matchCount, 1);
    assert.equal(result.error, null);
    // Whole region matched → fully replaced with the (reindented) newString.
    assert.equal(result.newContent, 'a\nZZ');
  });

  it('maps a literal "\\n" AND a smart single quote together', () => {
    // Combines a length-shrinking `\n` with a same-length smart-quote
    // replacement so the walk hits BOTH the `\\n` branch and the
    // charCode>127 → <128 smart-quote branch of buildPositionMap.
    const content = 'a\\n‘b'; // a, backslash, n, ‘, b
    const result = fuzzyFindAndReplace(content, "a\n'b", 'a\nXY');
    assert.equal(result.strategy, 'escape_normalized');
    assert.equal(result.error, null);
    assert.equal(result.newContent, 'a\nXY');
  });

  it('maps an em-dash in the file (1-char → 2-char expansion)', () => {
    // File contains a real em-dash; normalizeEscapes expands it to `--`,
    // growing the string, so buildPositionMap takes the em-dash branch that
    // advances the normalized index by two while holding the original index.
    const content = 'value — other'; // em-dash in the FILE
    const result = fuzzyFindAndReplace(content, 'value -- other', 'value ++ other');
    assert.equal(result.strategy, 'escape_normalized');
    assert.equal(result.matchCount, 1);
    assert.equal(result.error, null);
    assert.equal(result.newContent, 'value ++ other');
  });
});

describe('fuzzyFindAndReplace – preserveQuoteStyle (curly single quotes)', () => {
  it('re-styles ASCII single quotes in the replacement to match curly quotes in the file', () => {
    // The matched file region holds curly single quotes; the replacement uses
    // ASCII apostrophes. preserveQuoteStyle converts them to opening/closing
    // curly quotes so the edit preserves the file's typographic style.
    const content = 'x = ‘old’;';
    const result = fuzzyFindAndReplace(content, '‘old’;', "'new';");
    assert.equal(result.error, null);
    assert.equal(result.strategy, 'exact');
    // Opening ASCII quote → ‘ , closing → ’
    assert.equal(result.newContent, 'x = ‘new’;');
  });

  it('leaves the replacement untouched when it already contains a curly single quote', () => {
    // hasCurlySingle is true but the replacement already includes ‘, so the
    // re-styling guard short-circuits and no ASCII quotes are converted.
    const content = 'x = ‘old’;';
    const result = fuzzyFindAndReplace(content, '‘old’;', "‘kept' here;");
    assert.equal(result.error, null);
    // The lone ASCII apostrophe survives unchanged (no conversion ran).
    assert.equal(result.newContent, "x = ‘kept' here;");
  });
});

describe('reindentReplacement – first-line fallback', () => {
  it('prefixes file indent + trimmed first line when new line lacks old base indent', () => {
    // old_string's first line is indented 4 spaces; new_string's first line
    // has NO leading whitespace, so it cannot `startsWith` the old base
    // indent. The fallback trims and prepends the file's base indent.
    const result = reindentReplacement('  code', '    code', 'newcode');
    assert.equal(result, '  newcode');
  });

  it('applies the fallback per-call independent of later lines', () => {
    const fileRegion = '  a\n  b';
    const oldString = '    a\n    b';
    // First new line "x" does not start with the 4-space old indent.
    const newString = 'x\n    y';
    const result = reindentReplacement(fileRegion, oldString, newString);
    const lines = result.split('\n');
    // First line: fileBaseIndent ("  ") + "x".
    assert.equal(lines[0], '  x');
  });
});

describe('similarity early-equality path (via findClosestLines)', () => {
  it('scores an identical trimmed line as a perfect 1.0 match', () => {
    // When a content line trims to exactly the target, similarity() returns 1
    // through its `a === b` fast path.
    const content = 'alpha\n  target line  \nbeta';
    const results = findClosestLines(content, ['target line']);
    const exact = results.find(r => r.line.trim() === 'target line');
    assert.ok(exact, 'expected the identical line to be scored');
    assert.equal(exact!.similarity, 1);
  });
});
