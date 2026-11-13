import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { isGrepNoMatchExit, stripGrepFlags } from '../src/tools/grep';
import { GLOB_RIPGREP_ENV, isRipgrepGlobEnabled } from '../src/tools/glob';
import { expandBraces, estimateBraceExpansions } from '../src/brace-expansion';

// POV: pure helpers of the grep / glob / brace-expansion tools. Behavior is
// asserted by calling the real functions; expected values are derived by
// executing the semantics, never by reading source text.

describe('isGrepNoMatchExit', () => {
  it('treats exit code 1 as "no match" (not an error)', () => {
    assert.equal(isGrepNoMatchExit(1), true);
  });

  it('exit code 0 (matches found) is NOT a no-match code', () => {
    assert.equal(isGrepNoMatchExit(0), false);
  });

  it('exit codes > 1 are real errors, not no-match', () => {
    for (const code of [2, 3, 127, 130, 255]) {
      assert.equal(isGrepNoMatchExit(code), false, `code ${code}`);
    }
  });

  it('negative and large codes are not no-match', () => {
    assert.equal(isGrepNoMatchExit(-1), false);
    assert.equal(isGrepNoMatchExit(100000), false);
  });

  it('only the exact integer 1 qualifies', () => {
    // No coercion surprises: 1.0 is still 1 in JS numbers.
    assert.equal(isGrepNoMatchExit(1.0), true);
    assert.equal(isGrepNoMatchExit(1.5), false);
  });
});

describe('stripGrepFlags', () => {
  it('returns a flagless pattern unchanged', () => {
    assert.equal(stripGrepFlags('foo'), 'foo');
    assert.equal(stripGrepFlags('BLP|Bell'), 'BLP|Bell');
  });

  it('strips a single leading flag with its trailing whitespace', () => {
    assert.equal(stripGrepFlags('-l pattern'), 'pattern');
  });

  it('strips multiple space-separated leading flags', () => {
    assert.equal(stripGrepFlags('-r -c pattern'), 'pattern');
  });

  it('strips a combined flag cluster', () => {
    assert.equal(stripGrepFlags('-li needle'), 'needle');
    assert.equal(stripGrepFlags('-rniw thing'), 'thing');
  });

  it('only strips flags built from the [lcrniw] alphabet', () => {
    // -x is not in the recognized set -> left intact.
    assert.equal(stripGrepFlags('-x pattern'), '-x pattern');
    // Mixed valid+invalid cluster is not a recognized flag token.
    assert.equal(stripGrepFlags('-lx pattern'), '-lx pattern');
  });

  it('requires whitespace after the flag to strip it', () => {
    // No space -> the regex (which demands \\s+) does not match.
    assert.equal(stripGrepFlags('-lpattern'), '-lpattern');
  });

  it('returns the original when the pattern is ENTIRELY flags (never empties the search)', () => {
    // '-l ' matches the whole string; stripped === '' is falsy, so the original
    // is returned rather than an empty pattern.
    assert.equal(stripGrepFlags('-l '), '-l ');
    assert.equal(stripGrepFlags('-r -c '), '-r -c ');
  });

  it('a pattern that merely starts with a dash but is not a flag is left alone', () => {
    assert.equal(stripGrepFlags('-notaflag'), '-notaflag');
  });

  it('empty string stays empty (|| pattern yields original empty string)', () => {
    assert.equal(stripGrepFlags(''), '');
  });

  it('does not strip flags that appear after the first token', () => {
    assert.equal(stripGrepFlags('pattern -l'), 'pattern -l');
  });

  it('tolerates multiple spaces between flag and pattern', () => {
    assert.equal(stripGrepFlags('-l    pattern'), 'pattern');
  });
});

describe('GLOB_RIPGREP_ENV constant', () => {
  it('is the documented env var name', () => {
    assert.equal(GLOB_RIPGREP_ENV, 'AMI_GLOB_RG');
  });
});

describe('isRipgrepGlobEnabled', () => {
  const on = (val: string) => isRipgrepGlobEnabled({ [GLOB_RIPGREP_ENV]: val } as NodeJS.ProcessEnv);

  it('is OFF when the var is unset', () => {
    assert.equal(isRipgrepGlobEnabled({} as NodeJS.ProcessEnv), false);
  });

  it('is OFF for empty string', () => {
    assert.equal(on(''), false);
  });

  it('is OFF for the recognized falsy words', () => {
    for (const v of ['0', 'false', 'no', 'off']) {
      assert.equal(on(v), false, v);
    }
  });

  it('is OFF regardless of case for falsy words', () => {
    for (const v of ['FALSE', 'No', 'OFF', 'Off']) {
      assert.equal(on(v), false, v);
    }
  });

  it('trims surrounding whitespace before interpreting', () => {
    assert.equal(on('  off  '), false);
    assert.equal(on('  1  '), true);
    assert.equal(on('   '), false); // whitespace-only trims to '' -> off
  });

  it('is ON for any other non-empty value', () => {
    for (const v of ['1', 'true', 'yes', 'on', 'enabled', 'anything']) {
      assert.equal(on(v), true, v);
    }
  });

  it('is ON for uppercase truthy words', () => {
    assert.equal(on('TRUE'), true);
    assert.equal(on('YES'), true);
  });

  it('defaults to process.env when no arg is passed', () => {
    const prev = process.env[GLOB_RIPGREP_ENV];
    try {
      delete process.env[GLOB_RIPGREP_ENV];
      assert.equal(isRipgrepGlobEnabled(), false);
      process.env[GLOB_RIPGREP_ENV] = '1';
      assert.equal(isRipgrepGlobEnabled(), true);
      process.env[GLOB_RIPGREP_ENV] = 'off';
      assert.equal(isRipgrepGlobEnabled(), false);
    } finally {
      if (prev === undefined) delete process.env[GLOB_RIPGREP_ENV];
      else process.env[GLOB_RIPGREP_ENV] = prev;
    }
  });
});

describe('expandBraces', () => {
  it('returns the pattern verbatim when there is no brace', () => {
    assert.deepEqual(expandBraces('plain'), ['plain']);
    assert.deepEqual(expandBraces(''), ['']);
    assert.deepEqual(expandBraces('src/**/*.ts'), ['src/**/*.ts']);
  });

  it('expands a single group with prefix (a{b,c} -> ab ac)', () => {
    assert.deepEqual(expandBraces('a{b,c}'), ['ab', 'ac']);
  });

  it('expands prefix+suffix around the group', () => {
    assert.deepEqual(expandBraces('src/{a,b}.ts'), ['src/a.ts', 'src/b.ts']);
  });

  it('expands two groups as a cartesian product in order', () => {
    assert.deepEqual(expandBraces('{a,b}.{ts,js}'), ['a.ts', 'a.js', 'b.ts', 'b.js']);
  });

  it('expands three groups preserving left-to-right nesting order', () => {
    assert.deepEqual(expandBraces('{a,b}{c,d}{e,f}'), [
      'ace', 'acf', 'ade', 'adf', 'bce', 'bcf', 'bde', 'bdf',
    ]);
  });

  it('expands nested groups ({a,{b,c}} -> a b c)', () => {
    assert.deepEqual(expandBraces('{a,{b,c}}'), ['a', 'b', 'c']);
  });

  it('expands a nested group inside an alternative', () => {
    assert.deepEqual(expandBraces('x{1,{2,3}}y'), ['x1y', 'x2y', 'x3y']);
  });

  it('leaves a single-alternative group literal (no comma -> {a} stays)', () => {
    assert.deepEqual(expandBraces('{a}'), ['{a}']);
  });

  it('leaves an empty brace group literal ({} stays)', () => {
    assert.deepEqual(expandBraces('{}'), ['{}']);
  });

  it('keeps an unterminated brace literal', () => {
    assert.deepEqual(expandBraces('{a,b'), ['{a,b']);
  });

  it('produces an empty alternative when a comma is adjacent (a{,b} -> a ab)', () => {
    assert.deepEqual(expandBraces('a{,b}'), ['a', 'ab']);
  });

  it('respects escaped braces (does not expand \\{a,b\\})', () => {
    assert.deepEqual(expandBraces('\\{a,b\\}'), ['\\{a,b\\}']);
  });

  it('throws when the estimated expansion exceeds the budget', () => {
    assert.throws(
      () => expandBraces('{a,b}{c,d}{e,f}', 4),
      /Brace expansion exceeded budget/,
    );
  });

  it('does not throw when exactly at the budget', () => {
    // {a,b}{c,d} -> 4 expansions, budget 4 is allowed (only >budget throws).
    assert.deepEqual(expandBraces('{a,b}{c,d}', 4), ['ac', 'ad', 'bc', 'bd']);
  });

  it('honors a raised budget', () => {
    const out = expandBraces('{a,b}{c,d}{e,f}', 1000);
    assert.equal(out.length, 8);
  });

  it('a budget of 0 rejects any real expansion', () => {
    assert.throws(() => expandBraces('{a,b}', 0), /exceeded budget/);
  });

  it('no-brace pattern bypasses the budget entirely', () => {
    assert.deepEqual(expandBraces('plain', 0), ['plain']);
  });
});

describe('estimateBraceExpansions', () => {
  it('is 1 for a brace-free pattern', () => {
    assert.equal(estimateBraceExpansions('plain'), 1);
    assert.equal(estimateBraceExpansions(''), 1);
  });

  it('counts the alternatives of a single group', () => {
    assert.equal(estimateBraceExpansions('a{b,c}'), 2);
    assert.equal(estimateBraceExpansions('{a,b,c}'), 3);
  });

  it('multiplies groups (cartesian estimate)', () => {
    assert.equal(estimateBraceExpansions('{a,b}{c,d}'), 4);
    assert.equal(estimateBraceExpansions('{a,b}{c,d}{e,f}'), 8);
  });

  it('matches the real expansion length for well-formed patterns', () => {
    const p = '{a,b}.{ts,js,tsx}';
    assert.equal(estimateBraceExpansions(p), expandBraces(p).length);
  });

  it('counts only top-level commas inside a group (nested commas folded)', () => {
    // Outer group has alternatives "a" and "{b,c}" -> estimate 2 at this level.
    assert.equal(estimateBraceExpansions('{a,{b,c}}'), 2);
  });

  it('ignores escaped braces when estimating', () => {
    assert.equal(estimateBraceExpansions('\\{a,b\\}'), 1);
  });

  it('counts an unterminated group as 1 (never closes depth 0)', () => {
    assert.equal(estimateBraceExpansions('{a,b'), 1);
  });

  it('short-circuits once the estimate blows past a million', () => {
    // Six 10-way groups -> 1,000,000 exactly then the 7th tips it over and the
    // loop returns early. The result is a large number, and crucially finite.
    const huge = '{a,b,c,d,e,f,g,h,i,j}'.repeat(7);
    const est = estimateBraceExpansions(huge);
    assert.ok(est > 1_000_000, `expected >1e6, got ${est}`);
    assert.ok(Number.isFinite(est));
  });

  it('agrees with the budget gate: estimate>budget is exactly what expandBraces rejects', () => {
    const p = '{a,b}{c,d}{e,f}';
    const est = estimateBraceExpansions(p); // 8
    assert.equal(est, 8);
    assert.throws(() => expandBraces(p, est - 1), /exceeded budget/);
    assert.doesNotThrow(() => expandBraces(p, est));
  });
});
