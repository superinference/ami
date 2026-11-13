import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  fuzzyFindAndReplace,
  reindentReplacement,
  findClosestLines,
  STRATEGY_NAMES,
  type StrategyName,
  type FuzzyResult,
} from '../src/tools/fuzzy-match';
import { levenshteinDistance, suggestSimilarPaths } from '../src/tools/path-suggest';

// ===========================================================================
// POV: fuzzy-match.ts + path-suggest.ts — strategy-span matrix.
//
// Strategy find() helpers are private; the only public entry is
// fuzzyFindAndReplace, which tries strategies in STRATEGY_NAMES order and the
// FIRST strategy with exactly one match wins. We therefore craft old/new/content
// triples that force each *reachable* strategy to win, and lock the observable
// result (strategy + the exact span, verified via the reconstructed newContent).
//
// All EXPECTED values are computed by calling the real code (fuzzyFindAndReplace,
// levenshteinDistance, path.join) — never by scraping source text.
// ===========================================================================

describe('STRATEGY_NAMES ordering contract', () => {
  it('is the exact strictness-ordered 5-tuple', () => {
    assert.deepEqual([...STRATEGY_NAMES], [
      'exact',
      'line_trimmed',
      'whitespace_normalized',
      'indentation_flexible',
      'escape_normalized',
    ]);
  });

  it('every entry is a distinct string', () => {
    assert.equal(new Set(STRATEGY_NAMES).size, STRATEGY_NAMES.length);
  });
});

// ---------------------------------------------------------------------------
// exact
// ---------------------------------------------------------------------------

describe('fuzzyFindAndReplace — exact strategy wins & span is verbatim', () => {
  it('single hit replaces only the matched span, strategy=exact', () => {
    const r = fuzzyFindAndReplace('alpha BETA gamma', 'BETA', 'DELTA');
    assert.equal(r.strategy, 'exact');
    assert.equal(r.matchCount, 1);
    assert.equal(r.error, null);
    // Span = [6,10); exact replacement is verbatim newString.
    assert.equal(r.newContent, 'alpha DELTA gamma');
  });

  it('match at offset 0', () => {
    const r = fuzzyFindAndReplace('head tail', 'head', 'HEAD');
    assert.equal(r.strategy, 'exact');
    assert.equal(r.newContent, 'HEAD tail');
  });

  it('match at end of content', () => {
    const r = fuzzyFindAndReplace('head tail', 'tail', 'TAIL');
    assert.equal(r.strategy, 'exact');
    assert.equal(r.newContent, 'head TAIL');
  });

  it('multi-line verbatim block', () => {
    const content = 'a\nb\nc\nd';
    const r = fuzzyFindAndReplace(content, 'b\nc', 'B\nC');
    assert.equal(r.strategy, 'exact');
    assert.equal(r.newContent, 'a\nB\nC\nd');
  });

  it('empty newString deletes exactly the matched span', () => {
    const r = fuzzyFindAndReplace('xYz', 'Y', '');
    assert.equal(r.strategy, 'exact');
    assert.equal(r.newContent, 'xz');
  });
});

// ---------------------------------------------------------------------------
// line_trimmed
// ---------------------------------------------------------------------------

describe('fuzzyFindAndReplace — line_trimmed strategy wins', () => {
  it('matches when file lines carry extra leading+trailing whitespace', () => {
    // 'foo\nbar' is NOT an exact substring (file has '  foo  \n  bar  '),
    // so exact finds 0; line_trimmed finds exactly 1.
    const r = fuzzyFindAndReplace('  foo  \n  bar  ', 'foo\nbar', 'X\nY');
    assert.equal(r.strategy, 'line_trimmed');
    assert.equal(r.matchCount, 1);
    assert.equal(r.error, null);
    // Whole file is the span; reindent pushes new lines to the file's 2-space indent.
    assert.equal(r.newContent, '  X\n  Y');
  });

  it('preserves surrounding lines outside the matched block', () => {
    // Two-line pattern: the embedded newline + surrounding spaces mean the
    // pattern is not a verbatim substring, so exact finds 0 and line_trimmed wins.
    const content = 'keep\n   mid  \n   end  \ntail';
    const r = fuzzyFindAndReplace(content, 'mid\nend', 'MID\nEND');
    assert.equal(r.strategy, 'line_trimmed');
    assert.equal(r.newContent, 'keep\n   MID\n   END\ntail');
  });
});

// ---------------------------------------------------------------------------
// whitespace_normalized
// ---------------------------------------------------------------------------

describe('fuzzyFindAndReplace — whitespace_normalized strategy wins', () => {
  it('collapses internal whitespace runs that trim alone cannot fix', () => {
    // Internal multi-spaces: exact fails, line_trimmed (trim only) fails,
    // whitespace_normalized (collapse internal runs) matches.
    const r = fuzzyFindAndReplace('const   x   =   1;', 'const x = 1;', 'const x = 2;');
    assert.equal(r.strategy, 'whitespace_normalized');
    assert.equal(r.matchCount, 1);
    assert.equal(r.error, null);
    assert.equal(r.newContent, 'const x = 2;');
  });

  it('matches tabs-vs-spaces internal gaps', () => {
    const r = fuzzyFindAndReplace('a\t\tb', 'a b', 'a-B');
    assert.equal(r.strategy, 'whitespace_normalized');
    assert.equal(r.newContent, 'a-B');
  });
});

// ---------------------------------------------------------------------------
// indentation_flexible — documented as UNREACHABLE as a winner.
//
// line_trimmed removes leading+trailing; indentation_flexible removes leading
// only. Any window indentation_flexible matches, line_trimmed also matches
// (equal strings stay equal after also stripping identical trailing ws), so
// line_trimmed always resolves first (win on count 1, or error on count >1).
// We lock that an indentation-only difference resolves via line_trimmed.
// ---------------------------------------------------------------------------

describe('fuzzyFindAndReplace — indentation-only diffs resolve via line_trimmed', () => {
  it('leading-indent mismatch is handled by line_trimmed, not indentation_flexible', () => {
    const content = '    const x = 1;\n    const y = 2;';
    const r = fuzzyFindAndReplace(content, 'const x = 1;\nconst y = 2;', 'const x = 10;\nconst y = 20;');
    assert.equal(r.error, null);
    assert.equal(r.strategy, 'line_trimmed');
    // Reindent restores the file's 4-space indent.
    assert.equal(r.newContent, '    const x = 10;\n    const y = 20;');
  });

  it('no crafted input selects indentation_flexible as winner', () => {
    // Sweep a battery of indent/trailing permutations; indentation_flexible
    // must never be the winning strategy (it is structurally subsumed).
    const cases: Array<[string, string]> = [
      ['    hi', 'hi'],
      ['\thi', 'hi'],
      ['hi  ', 'hi'],
      ['   a\n   b', 'a\nb'],
      ['a \nb ', 'a\nb'],
    ];
    for (const [content, oldS] of cases) {
      const r = fuzzyFindAndReplace(content, oldS, 'Z');
      if (r.strategy !== null) {
        assert.notEqual(r.strategy, 'indentation_flexible');
      }
    }
  });
});

// ---------------------------------------------------------------------------
// escape_normalized
// ---------------------------------------------------------------------------

describe('fuzzyFindAndReplace — escape_normalized strategy wins', () => {
  it('smart single quotes in pattern match ASCII file content', () => {
    const r = fuzzyFindAndReplace("x = 'hi'", 'x = ‘hi’', 'x = "ho"');
    assert.equal(r.strategy, 'escape_normalized');
    assert.equal(r.matchCount, 1);
    assert.equal(r.error, null);
    // File region is ASCII, so quote-style preservation does not kick in.
    assert.equal(r.newContent, 'x = "ho"');
  });

  it('em-dash in pattern matches ASCII double-dash in file (identity position map)', () => {
    const r = fuzzyFindAndReplace('value -- other', 'value — other', 'value + other');
    assert.equal(r.strategy, 'escape_normalized');
    assert.equal(r.newContent, 'value + other');
  });

  it('em-dash in FILE matches double-dash pattern (length-changing position map)', () => {
    // content shorter than normalized ('—' -> '--'): exercises buildPositionMap.
    const r = fuzzyFindAndReplace('a—b', 'a--b', 'XY');
    assert.equal(r.strategy, 'escape_normalized');
    assert.equal(r.newContent, 'XY');
  });

  it('preserves curly double quotes present in the matched file region', () => {
    // Region has “”; replacement uses ASCII " — preserveQuoteStyle
    // converts them back to curly in the output.
    const r = fuzzyFindAndReplace('say “hi”', 'say "hi"', 'say "yo"');
    assert.equal(r.strategy, 'escape_normalized');
    assert.equal(r.newContent, 'say “yo”');
  });

  it('literal \\n escape in pattern matches a real newline in the file', () => {
    const r = fuzzyFindAndReplace('line1\nline2', 'line1\\nline2', 'A\\nB');
    assert.equal(r.strategy, 'escape_normalized');
    assert.ok(r.error === null && r.newContent !== null);
  });
});

// ---------------------------------------------------------------------------
// no-match & ambiguous-match degenerate paths
// ---------------------------------------------------------------------------

describe('fuzzyFindAndReplace — failure shapes', () => {
  it('no strategy matches -> null result with not-found error', () => {
    const r: FuzzyResult = fuzzyFindAndReplace('hello world', 'zzz-absent', 'x');
    assert.equal(r.newContent, null);
    assert.equal(r.strategy, null);
    assert.equal(r.matchCount, 0);
    assert.match(r.error ?? '', /not found/);
  });

  it('exact finds >1 -> stops at exact with matchCount and ambiguity error', () => {
    const r = fuzzyFindAndReplace('foo bar foo baz foo', 'foo', 'Q');
    assert.equal(r.strategy, 'exact');
    assert.equal(r.matchCount, 3);
    assert.equal(r.newContent, null);
    assert.match(r.error ?? '', /3 locations/);
  });

  it('line_trimmed finds >1 (exact found 0) -> errors at line_trimmed', () => {
    // 'a\nb' is not an exact substring, but trims to two windows.
    const content = '  a  \n  b  \nsep\n  a  \n  b  ';
    const r = fuzzyFindAndReplace(content, 'a\nb', 'Z');
    assert.equal(r.strategy, 'line_trimmed');
    assert.equal(r.matchCount, 2);
    assert.equal(r.newContent, null);
    assert.match(r.error ?? '', /2 locations/);
  });

  it('single-char pattern absent from content -> not found', () => {
    const r = fuzzyFindAndReplace('abcabc', '#', '!');
    assert.equal(r.matchCount, 0);
    assert.equal(r.strategy, null);
  });
});

// ---------------------------------------------------------------------------
// reindentReplacement — pure indentation math
// ---------------------------------------------------------------------------

describe('reindentReplacement', () => {
  it('returns newString unchanged when base indents already match', () => {
    assert.equal(reindentReplacement('  a', '  a', '  b'), '  b');
    assert.equal(reindentReplacement('a', 'a', 'b'), 'b');
  });

  it('adds indentation when the file is more deeply indented', () => {
    const out = reindentReplacement('    x = 1;\n    y = 2;', 'x = 1;\ny = 2;', 'x = 9;\ny = 8;');
    assert.equal(out, '    x = 9;\n    y = 8;');
  });

  it('removes indentation when the file is less indented than old_string', () => {
    const out = reindentReplacement('x = 1;\ny = 2;', '    x = 1;\n    y = 2;', '    x = 9;\n    y = 8;');
    const lines = out.split('\n');
    assert.equal(lines[0], 'x = 9;');
    assert.equal(lines[1], 'y = 8;');
  });

  it('never strips more leading whitespace than a continuation line has', () => {
    // delta = -4, but the continuation line only has 2 leading spaces.
    const out = reindentReplacement('x\n  y', '    x\n  y', '    x\n  y');
    const lines = out.split('\n');
    assert.equal(lines[0], 'x');
    assert.equal(lines[1], 'y'); // 2 spaces removed (min of 4 and 2)
  });

  it('first line without the old base prefix is re-based on trimmed content', () => {
    const out = reindentReplacement('  code', '    code', '    newcode');
    assert.equal(out, '  newcode');
  });

  it('single-line add-indent', () => {
    assert.equal(reindentReplacement('      line', 'line', 'fresh'), '      fresh');
  });
});

// ---------------------------------------------------------------------------
// findClosestLines
// ---------------------------------------------------------------------------

describe('findClosestLines', () => {
  it('ranks the most-similar line first and is sorted descending', () => {
    const content = 'const fooBar = 1;\nconst fooBaz = 1;\nconst zzz = 9;';
    const out = findClosestLines(content, ['const fooBaz = 1;']);
    assert.ok(out.length > 0);
    assert.equal(out[0].similarity, 1); // identical line exists
    for (let i = 1; i < out.length; i++) {
      assert.ok(out[i - 1].similarity >= out[i].similarity);
    }
  });

  it('reports 1-based line numbers', () => {
    const content = 'first\nsecond\nthird';
    const out = findClosestLines(content, ['second']);
    const hit = out.find(r => r.line === 'second');
    assert.ok(hit);
    assert.equal(hit!.lineNumber, 2);
  });

  it('honors maxResults', () => {
    const content = 'aaa\naab\naac\naad\naae';
    assert.ok(findClosestLines(content, ['aaa'], 2).length <= 2);
  });

  it('empty searchLines -> empty', () => {
    assert.deepEqual(findClosestLines('x\ny', []), []);
  });

  it('blank first search line -> empty', () => {
    assert.deepEqual(findClosestLines('x\ny', ['   ']), []);
  });

  it('nothing above the 0.4 similarity floor -> empty', () => {
    assert.deepEqual(findClosestLines('zzzzzzzz', ['wwwwwwwwww']), []);
  });
});

// ---------------------------------------------------------------------------
// levenshteinDistance — known pairs (symmetry + triangle sanity)
// ---------------------------------------------------------------------------

describe('levenshteinDistance known pairs', () => {
  const pairs: Array<[string, string, number]> = [
    ['', '', 0],
    ['', 'abc', 3],
    ['abc', '', 3],
    ['abc', 'abc', 0],
    ['cat', 'bat', 1],
    ['kitten', 'sitting', 3],
    ['sunday', 'saturday', 3],
    ['flaw', 'lawn', 2],
    ['gumbo', 'gambol', 2],
    ['book', 'back', 2],
    ['ab', 'ba', 2],
    ['a', 'b', 1],
    ['abcdef', 'azced', 3],
  ];

  for (const [a, b, d] of pairs) {
    it(`d("${a}","${b}") = ${d}`, () => {
      assert.equal(levenshteinDistance(a, b), d);
    });
  }

  it('is symmetric for every pair', () => {
    for (const [a, b] of pairs) {
      assert.equal(levenshteinDistance(a, b), levenshteinDistance(b, a));
    }
  });

  it('distance to self is always 0', () => {
    for (const s of ['', 'x', 'hello', 'a b c']) {
      assert.equal(levenshteinDistance(s, s), 0);
    }
  });
});

// ---------------------------------------------------------------------------
// suggestSimilarPaths — ranking verified against the real levenshteinDistance
// ---------------------------------------------------------------------------

describe('suggestSimilarPaths', () => {
  let tmp: string;

  before(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'pov-path-suggest-'));
    fs.mkdirSync(path.join(tmp, 'src'), { recursive: true });
    for (const f of ['alpha.ts', 'alphabet.ts', 'beta.ts', 'index.ts']) {
      fs.writeFileSync(path.join(tmp, 'src', f), '');
    }
  });

  after(() => {
    fs.rmSync(tmp, { recursive: true, force: true });
  });

  // Recompute the expected suggestion ordering using the REAL levenshtein.
  function expectedSuggestions(dir: string, base: string, maxResults = 3, maxDistance = 3): string[] {
    const entries = fs.readdirSync(dir);
    return entries
      .map(name => ({ name, distance: levenshteinDistance(base.toLowerCase(), name.toLowerCase()) }))
      .filter(e => e.distance > 0 && e.distance <= maxDistance)
      .sort((a, b) => a.distance - b.distance)
      .slice(0, maxResults)
      .map(e => path.join(dir, e.name));
  }

  it('returns suggestions ranked nearest-first, matching levenshtein order', () => {
    const srcDir = path.join(tmp, 'src');
    const missing = path.join(srcDir, 'alpba.ts');
    const got = suggestSimilarPaths(missing, tmp);
    const want = expectedSuggestions(srcDir, 'alpba.ts');
    assert.deepEqual(got, want);
    // 'alpha.ts' (distance 1) must be the top suggestion.
    assert.equal(path.basename(got[0]), 'alpha.ts');
  });

  it('is case-insensitive when ranking entries', () => {
    const srcDir = path.join(tmp, 'src');
    const got = suggestSimilarPaths(path.join(srcDir, 'ALPBA.TS'), tmp);
    assert.ok(got.some(s => path.basename(s) === 'alpha.ts'));
  });

  it('returns [] when the path exists exactly', () => {
    assert.deepEqual(suggestSimilarPaths(path.join(tmp, 'src', 'index.ts'), tmp), []);
  });

  it('honors maxResults', () => {
    const got = suggestSimilarPaths(path.join(tmp, 'src', 'aaa.ts'), tmp, 1);
    assert.ok(got.length <= 1);
  });

  it('honors a tighter maxDistance (excludes farther entries)', () => {
    const srcDir = path.join(tmp, 'src');
    const got = suggestSimilarPaths(path.join(srcDir, 'alpba.ts'), tmp, 5, 1);
    const want = expectedSuggestions(srcDir, 'alpba.ts', 5, 1);
    assert.deepEqual(got, want);
    // every returned entry is within distance 1
    for (const g of got) {
      assert.ok(levenshteinDistance('alpba.ts', path.basename(g).toLowerCase()) <= 1);
    }
  });

  it('resolves a relative missing path against cwd', () => {
    const got = suggestSimilarPaths(path.join('src', 'alpba.ts'), tmp);
    assert.ok(got.some(s => path.basename(s) === 'alpha.ts'));
  });

  it('returns [] when the parent directory is unreadable/missing', () => {
    const got = suggestSimilarPaths(path.join(tmp, 'no-such-dir', 'file.ts'), tmp);
    assert.deepEqual(got, []);
  });

  it('returns [] when nothing is within the distance budget', () => {
    const got = suggestSimilarPaths(path.join(tmp, 'src', 'zzzzzzzzzzzzzz.qqq'), tmp);
    assert.deepEqual(got, []);
  });
});
