import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  fuzzyFindAndReplace,
  findClosestLines,
  reindentReplacement,
} from '../src/tools/fuzzy-match';

// ---------------------------------------------------------------------------
// fuzzyFindAndReplace – exact strategy
// ---------------------------------------------------------------------------

describe('fuzzyFindAndReplace – exact strategy', () => {
  it('replaces exact match in content', () => {
    const result = fuzzyFindAndReplace('hello world', 'hello', 'goodbye');
    assert.equal(result.error, null);
    assert.equal(result.strategy, 'exact');
    assert.equal(result.matchCount, 1);
    assert.equal(result.newContent, 'goodbye world');
  });

  it('returns error when no match found (all strategies exhausted)', () => {
    const result = fuzzyFindAndReplace('hello world', 'nothere', 'x');
    assert.ok(result.error !== null);
    assert.equal(result.matchCount, 0);
    assert.equal(result.newContent, null);
    assert.equal(result.strategy, null);
  });

  it('returns error when multiple matches found', () => {
    const result = fuzzyFindAndReplace('foo bar foo', 'foo', 'baz');
    assert.ok(result.error !== null);
    assert.equal(result.matchCount, 2);
    assert.equal(result.newContent, null);
    assert.ok(result.error!.includes('found 2 times'));
  });

  it('handles multi-line exact match', () => {
    const content = 'line1\nline2\nline3\nline4';
    const result = fuzzyFindAndReplace(content, 'line2\nline3', 'replaced2\nreplaced3');
    assert.equal(result.error, null);
    assert.equal(result.strategy, 'exact');
    assert.equal(result.newContent, 'line1\nreplaced2\nreplaced3\nline4');
  });

  it('handles single-character pattern', () => {
    const result = fuzzyFindAndReplace('abcabc', 'x', 'y');
    assert.ok(result.error !== null);
    assert.equal(result.matchCount, 0);
  });
});

// ---------------------------------------------------------------------------
// fuzzyFindAndReplace – line_trimmed strategy
// ---------------------------------------------------------------------------

describe('fuzzyFindAndReplace – line_trimmed strategy', () => {
  it('matches when file has extra trailing whitespace per line', () => {
    const content = '  hello  \n  world  ';
    const result = fuzzyFindAndReplace(content, 'hello\nworld', 'hi\nearth');
    assert.equal(result.error, null);
    assert.ok(result.strategy === 'line_trimmed' || result.strategy === 'exact' || result.strategy === 'whitespace_normalized' || result.strategy === 'indentation_flexible');
    assert.ok(result.newContent !== null);
  });
});

// ---------------------------------------------------------------------------
// fuzzyFindAndReplace – whitespace_normalized strategy
// ---------------------------------------------------------------------------

describe('fuzzyFindAndReplace – whitespace_normalized strategy', () => {
  it('matches when whitespace is collapsed differently', () => {
    const content = 'const   x   =   1;';
    const result = fuzzyFindAndReplace(content, 'const x = 1;', 'const x = 2;');
    assert.equal(result.error, null);
    assert.ok(result.newContent !== null);
  });
});

// ---------------------------------------------------------------------------
// fuzzyFindAndReplace – indentation_flexible strategy
// ---------------------------------------------------------------------------

describe('fuzzyFindAndReplace – indentation_flexible strategy', () => {
  it('matches when indentation differs', () => {
    const content = '    const x = 1;\n    const y = 2;';
    const result = fuzzyFindAndReplace(content, 'const x = 1;\nconst y = 2;', 'const x = 10;\nconst y = 20;');
    assert.equal(result.error, null);
    assert.ok(result.newContent !== null);
  });
});

// ---------------------------------------------------------------------------
// fuzzyFindAndReplace – escape_normalized strategy
// ---------------------------------------------------------------------------

describe('fuzzyFindAndReplace – escape_normalized strategy', () => {
  it('matches when smart quotes are used instead of ASCII quotes', () => {
    const content = "const x = 'hello';";
    const result = fuzzyFindAndReplace(content, "const x = ‘hello’;", 'const x = "world";');
    assert.equal(result.error, null);
    assert.ok(result.newContent !== null);
  });

  it('matches em-dash vs double dash', () => {
    const content = 'value -- other';
    const result = fuzzyFindAndReplace(content, 'value — other', 'value + other');
    assert.equal(result.error, null);
    assert.ok(result.newContent !== null);
  });
});

// ---------------------------------------------------------------------------
// reindentReplacement
// ---------------------------------------------------------------------------

describe('reindentReplacement', () => {
  it('returns newString unchanged when indentation matches', () => {
    const result = reindentReplacement('  hello', '  hello', '  goodbye');
    assert.equal(result, '  goodbye');
  });

  it('adjusts indentation when file has more indent than old_string', () => {
    const fileRegion = '    const x = 1;\n    const y = 2;';
    const oldString = 'const x = 1;\nconst y = 2;';
    const newString = 'const x = 10;\nconst y = 20;';

    const result = reindentReplacement(fileRegion, oldString, newString);
    assert.ok(result.includes('    const x = 10;'));
    assert.ok(result.includes('    const y = 20;'));
  });

  it('adjusts indentation when file has less indent than old_string', () => {
    const fileRegion = 'const x = 1;\nconst y = 2;';
    const oldString = '    const x = 1;\n    const y = 2;';
    const newString = '    const x = 10;\n    const y = 20;';

    const result = reindentReplacement(fileRegion, oldString, newString);
    assert.ok(result.includes('const x = 10;'));
  });

  it('handles single-line replacement', () => {
    const result = reindentReplacement('    line', 'line', 'newline');
    assert.equal(result, '    newline');
  });

  it('handles first line with different pattern from old base indent', () => {
    const fileRegion = '  code';
    const oldString = '    code';
    const newString = '    newcode';
    const result = reindentReplacement(fileRegion, oldString, newString);
    assert.ok(result.includes('newcode'));
  });
});

// ---------------------------------------------------------------------------
// findClosestLines
// ---------------------------------------------------------------------------

describe('findClosestLines', () => {
  it('finds similar lines in content', () => {
    const content = 'const fooBar = 1;\nconst bazQux = 2;\nconst fooQuux = 3;';
    const results = findClosestLines(content, ['const fooBaz = 1;']);
    assert.ok(results.length > 0);
    // fooBar should be most similar
    assert.ok(results[0].line.includes('foo'));
  });

  it('returns results sorted by similarity (descending)', () => {
    const content = 'abc\nabc123\nxyz';
    const results = findClosestLines(content, ['abc']);
    for (let i = 1; i < results.length; i++) {
      assert.ok(results[i - 1].similarity >= results[i].similarity);
    }
  });

  it('respects maxResults parameter', () => {
    const content = 'aaa\naab\naac\naad\naae';
    const results = findClosestLines(content, ['aaa'], 2);
    assert.ok(results.length <= 2);
  });

  it('returns empty array for empty searchLines', () => {
    const results = findClosestLines('content', []);
    assert.equal(results.length, 0);
  });

  it('returns empty array for empty first search line', () => {
    const results = findClosestLines('content', ['']);
    assert.equal(results.length, 0);
  });

  it('returns empty array when no lines are similar enough', () => {
    const content = 'xxxxxxxxxx';
    const results = findClosestLines(content, ['yyyyyyyyyyy']);
    // Similarity threshold is 0.4 — completely different strings should yield no results
    assert.equal(results.length, 0);
  });

  it('includes line numbers (1-based)', () => {
    const content = 'first\nsecond\nthird';
    const results = findClosestLines(content, ['second']);
    const match = results.find(r => r.line.includes('second'));
    if (match) {
      assert.equal(match.lineNumber, 2);
    }
  });

  it('perfect match has similarity 1.0', () => {
    const content = 'exact match line';
    const results = findClosestLines(content, ['exact match line']);
    assert.ok(results.length > 0);
    assert.equal(results[0].similarity, 1);
  });
});
