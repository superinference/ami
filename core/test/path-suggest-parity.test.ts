import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  suggestSimilarPaths,
  suggestPathCorrection,
  suggestByBasename,
  levenshteinDistance,
} from '../src/tools/path-suggest';

// ---------------------------------------------------------------------------
// Parity superset vs the reference path correction. Each describe block maps to a
// labeled ADDITIVE fix and is red-green validated (fails if the fix is
// reverted, passes with it; no block passes both ways).
// ---------------------------------------------------------------------------

describe('path-suggest parity: case-only correction (Fix A)', () => {
  let tmp: string;

  before(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'psug-case-'));
    fs.writeFileSync(path.join(tmp, 'README.md'), '');
    fs.writeFileSync(path.join(tmp, 'LICENSE'), '');
  });
  after(() => fs.rmSync(tmp, { recursive: true, force: true }));

  // RED (revert): with the old `dist > 0` filter a case-only variant has
  // lowercased distance 0 and is dropped -> [] -> assertion fails.
  it('suggests a sibling that differs only by letter case', () => {
    const got = suggestSimilarPaths(path.join(tmp, 'readme.md'), tmp);
    assert.ok(
      got.some(s => path.basename(s) === 'README.md'),
      `expected README.md, got ${JSON.stringify(got)}`,
    );
  });

  it('ranks the case-only match ahead of typo matches', () => {
    // 'reaadme.md' is distance 1 from README.md (case-insensitive) — a typo,
    // not a pure case match — so only the real case path exists here; assert the
    // case correction is first when present alongside other near names.
    fs.writeFileSync(path.join(tmp, 'README.mdx'), '');
    const got = suggestSimilarPaths(path.join(tmp, 'readme.md'), tmp);
    assert.equal(path.basename(got[0]), 'README.md');
  });
});

describe('path-suggest parity: bounds hardening (Fix D)', () => {
  let tmp: string;

  before(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'psug-bounds-'));
    for (const f of ['foo1.ts', 'foo2.ts', 'foo3.ts']) {
      fs.writeFileSync(path.join(tmp, f), '');
    }
  });
  after(() => fs.rmSync(tmp, { recursive: true, force: true }));

  // RED (revert): without clamping, maxResults = -1 becomes slice(0, -1) which
  // returns all-but-last entry (a non-empty list) instead of nothing.
  it('negative maxResults yields no suggestions (not a from-end slice)', () => {
    const got = suggestSimilarPaths(path.join(tmp, 'foo.ts'), tmp, -1);
    assert.deepEqual(got, []);
  });

  it('non-finite maxResults is treated as zero', () => {
    const got = suggestSimilarPaths(path.join(tmp, 'foo.ts'), tmp, NaN);
    assert.deepEqual(got, []);
  });

  it('positive integers still behave exactly as before', () => {
    const got = suggestSimilarPaths(path.join(tmp, 'foo.ts'), tmp, 2);
    assert.ok(got.length <= 2 && got.length > 0);
  });
});

describe('path-suggest parity: structured did-you-mean (Fix B)', () => {
  let tmp: string;

  before(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'psug-rich-'));
    fs.mkdirSync(path.join(tmp, 'src'), { recursive: true });
    fs.writeFileSync(path.join(tmp, 'src', 'index.ts'), '');
    fs.writeFileSync(path.join(tmp, 'src', 'Config.ts'), '');
  });
  after(() => fs.rmSync(tmp, { recursive: true, force: true }));

  it('reports kind "none" when the path already exists', () => {
    const r = suggestPathCorrection(path.join(tmp, 'src', 'index.ts'), tmp);
    assert.deepEqual(r, { kind: 'none', suggestions: [] });
  });

  it('reports kind "exact-case" for a case-only sibling', () => {
    const r = suggestPathCorrection(path.join(tmp, 'src', 'config.ts'), tmp);
    assert.equal(r.kind, 'exact-case');
    assert.ok(r.suggestions.some(s => path.basename(s) === 'Config.ts'));
  });

  it('reports kind "typo" for a near sibling', () => {
    const r = suggestPathCorrection(path.join(tmp, 'src', 'indx.ts'), tmp);
    assert.equal(r.kind, 'typo');
    assert.ok(r.suggestions.some(s => path.basename(s) === 'index.ts'));
  });

  it('corrects a misspelled directory component (kind "missing-dir")', () => {
    // 'scr' does not exist; the real dir is 'src'. Old suggestSimilarPaths only
    // reads dirname(abs) and returns [] when it is missing — this recovers it.
    const r = suggestPathCorrection(path.join(tmp, 'scr', 'index.ts'), tmp);
    assert.equal(r.kind, 'missing-dir');
    assert.ok(
      r.suggestions.some(s => s.endsWith(path.join('src', 'index.ts'))),
      JSON.stringify(r),
    );
  });

  it('reports kind "none" when nothing is close enough', () => {
    const r = suggestPathCorrection(path.join(tmp, 'src', 'zzzzzzzzzz.qqq'), tmp);
    assert.deepEqual(r, { kind: 'none', suggestions: [] });
  });

  it('reports kind "none" when the grandparent is also missing', () => {
    const r = suggestPathCorrection(
      path.join(tmp, 'no-dir', 'sub', 'file.ts'),
      tmp,
    );
    assert.deepEqual(r, { kind: 'none', suggestions: [] });
  });
});

describe('path-suggest parity: basename-wide search (Fix C)', () => {
  let tmp: string;

  before(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'psug-basename-'));
    fs.mkdirSync(path.join(tmp, 'a', 'b'), { recursive: true });
    fs.mkdirSync(path.join(tmp, 'node_modules', 'pkg'), { recursive: true });
    fs.writeFileSync(path.join(tmp, 'a', 'b', 'helper.ts'), '');
    fs.writeFileSync(path.join(tmp, 'node_modules', 'pkg', 'helper.ts'), '');
  });
  after(() => fs.rmSync(tmp, { recursive: true, force: true }));

  it('finds a file by basename elsewhere in the tree', () => {
    // Caller guessed the wrong directory; the real file lives under a/b/.
    const got = suggestByBasename(path.join(tmp, 'wrong', 'helper.ts'), tmp);
    assert.ok(
      got.some(s => s === path.join(tmp, 'a', 'b', 'helper.ts')),
      JSON.stringify(got),
    );
  });

  it('skips node_modules/.git when searching', () => {
    const got = suggestByBasename(path.join(tmp, 'helper.ts'), tmp);
    assert.ok(!got.some(s => s.includes(`${path.sep}node_modules${path.sep}`)));
  });

  it('matches near basenames within the distance budget', () => {
    const got = suggestByBasename(path.join(tmp, 'helpr.ts'), tmp, { maxDistance: 2 });
    assert.ok(got.some(s => path.basename(s) === 'helper.ts'), JSON.stringify(got));
  });

  it('respects maxDepth', () => {
    // helper.ts is at depth 2 (a/b/helper.ts); depth 1 must not find it.
    const got = suggestByBasename(path.join(tmp, 'helper.ts'), tmp, { maxDepth: 1 });
    assert.ok(!got.some(s => s === path.join(tmp, 'a', 'b', 'helper.ts')));
  });

  it('respects maxResults and returns ranked, de-duplicated paths', () => {
    const got = suggestByBasename(path.join(tmp, 'helper.ts'), tmp, { maxResults: 1 });
    assert.ok(got.length <= 1);
    assert.equal(new Set(got).size, got.length);
  });

  it('returns [] when the search root is unreadable', () => {
    const got = suggestByBasename('helper.ts', path.join(tmp, 'does-not-exist'));
    assert.deepEqual(got, []);
  });
});

describe('path-suggest parity: regression guard (existing contract preserved)', () => {
  let tmp: string;

  before(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'psug-regress-'));
    fs.mkdirSync(path.join(tmp, 'src'), { recursive: true });
    for (const f of ['alpha.ts', 'alphabet.ts', 'beta.ts', 'index.ts']) {
      fs.writeFileSync(path.join(tmp, 'src', f), '');
    }
  });
  after(() => fs.rmSync(tmp, { recursive: true, force: true }));

  // The clamp + case-only additions must NOT change ranking for ordinary typo
  // inputs — mirror the deepEqual contract the matrix test relies on.
  it('typo ranking is byte-identical to the levenshtein recomputation', () => {
    const srcDir = path.join(tmp, 'src');
    const missing = path.join(srcDir, 'alpba.ts');
    const want = fs
      .readdirSync(srcDir)
      .map(name => ({ name, distance: levenshteinDistance('alpba.ts', name.toLowerCase()) }))
      .filter(e => e.distance > 0 && e.distance <= 3)
      .sort((a, b) => a.distance - b.distance)
      .slice(0, 3)
      .map(e => path.join(srcDir, e.name));
    assert.deepEqual(suggestSimilarPaths(missing, tmp), want);
  });
});
