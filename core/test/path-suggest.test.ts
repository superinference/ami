import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { suggestSimilarPaths, levenshteinDistance } from '../src/tools/path-suggest';

describe('levenshteinDistance', () => {
  it('returns 0 for identical strings', () => {
    assert.equal(levenshteinDistance('hello', 'hello'), 0);
  });

  it('returns length for empty vs non-empty', () => {
    assert.equal(levenshteinDistance('', 'abc'), 3);
    assert.equal(levenshteinDistance('abc', ''), 3);
  });

  it('returns 1 for single char difference', () => {
    assert.equal(levenshteinDistance('cat', 'bat'), 1);
  });

  it('handles insertions', () => {
    assert.equal(levenshteinDistance('test', 'tests'), 1);
  });

  it('handles deletions', () => {
    assert.equal(levenshteinDistance('tests', 'test'), 1);
  });

  it('handles transpositions', () => {
    assert.equal(levenshteinDistance('ab', 'ba'), 2);
  });
});

describe('suggestSimilarPaths', () => {
  let tmpDir: string;

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'path-suggest-'));
    fs.mkdirSync(path.join(tmpDir, 'src'), { recursive: true });
    fs.writeFileSync(path.join(tmpDir, 'src', 'index.ts'), '');
    fs.writeFileSync(path.join(tmpDir, 'src', 'utils.ts'), '');
    fs.writeFileSync(path.join(tmpDir, 'src', 'config.ts'), '');
    fs.writeFileSync(path.join(tmpDir, 'package.json'), '');
    fs.writeFileSync(path.join(tmpDir, 'tsconfig.json'), '');
  });

  after(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('suggests similar filenames in the same directory', () => {
    const suggestions = suggestSimilarPaths(path.join(tmpDir, 'src', 'indx.ts'), tmpDir);
    assert.ok(suggestions.length > 0);
    assert.ok(suggestions.some(s => s.includes('index.ts')));
  });

  it('returns empty for exact match', () => {
    const suggestions = suggestSimilarPaths(path.join(tmpDir, 'src', 'index.ts'), tmpDir);
    assert.equal(suggestions.length, 0);
  });

  it('suggests paths for missing extension', () => {
    const suggestions = suggestSimilarPaths(path.join(tmpDir, 'src', 'index'), tmpDir);
    assert.ok(suggestions.some(s => s.includes('index.ts')));
  });

  it('limits suggestions to maxResults', () => {
    const suggestions = suggestSimilarPaths(path.join(tmpDir, 'src', 'x'), tmpDir, 2);
    assert.ok(suggestions.length <= 2);
  });

  it('returns empty for completely different path', () => {
    const suggestions = suggestSimilarPaths(path.join(tmpDir, 'src', 'zzzzzzzzzzz.xyz'), tmpDir);
    assert.equal(suggestions.length, 0, 'no suggestions for very different paths');
  });

  it('handles non-existent parent directory', () => {
    const suggestions = suggestSimilarPaths(path.join(tmpDir, 'nonexistent', 'file.ts'), tmpDir);
    assert.ok(Array.isArray(suggestions));
  });
});
