import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  WorkspaceIndexer,
  gitignoreToIgnoreGlobs,
} from '../src/workspace-indexer';

// ---------------------------------------------------------------------------
// .gitignore honoring in the non-git (glob fallback) discovery branch.
//
// buildIndex() falls back to a raw glob when the workspace is not a git work
// tree. These tests verify that the author's ignore rules are honored in that
// branch so excluded files (secrets, build output) are never read, symbol-
// indexed, or surfaced via getSummary(). os.tmpdir() is not inside a git tree,
// so buildIndex deterministically takes the glob fallback here.
//
// RED-GREEN: reverting the collectGitignoreGlobs wiring in buildIndex (or the
// gitignoreToIgnoreGlobs translation) makes the excluded files appear in the
// index — these assertions then fail.
// ---------------------------------------------------------------------------

const tmpDirs: string[] = [];

function mkTmp(prefix = 'wsidx-gi-'): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  const real = fs.realpathSync(dir);
  tmpDirs.push(real);
  return real;
}

function write(dir: string, rel: string, content: string): string {
  const abs = path.join(dir, rel);
  fs.mkdirSync(path.dirname(abs), { recursive: true });
  fs.writeFileSync(abs, content);
  return abs;
}

afterEach(() => {
  while (tmpDirs.length) {
    const d = tmpDirs.pop()!;
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ }
  }
});

// ---------------------------------------------------------------------------
// Pure translation: gitignoreToIgnoreGlobs
// ---------------------------------------------------------------------------

describe('gitignoreToIgnoreGlobs', () => {
  it('translates a bare name to match at any depth (file + dir forms)', () => {
    assert.deepEqual(gitignoreToIgnoreGlobs('secret.ts'), ['**/secret.ts', '**/secret.ts/**']);
  });

  it('anchors a leading-slash pattern to the root', () => {
    assert.deepEqual(gitignoreToIgnoreGlobs('/secret.ts'), ['secret.ts', 'secret.ts/**']);
  });

  it('anchors a path-bearing pattern to the root', () => {
    assert.deepEqual(gitignoreToIgnoreGlobs('src/secret.ts'), ['src/secret.ts', 'src/secret.ts/**']);
  });

  it('emits only the dir-contents form for a trailing-slash (directory) pattern', () => {
    assert.deepEqual(gitignoreToIgnoreGlobs('build/'), ['**/build/**']);
  });

  it('skips blank lines and # comments', () => {
    assert.deepEqual(gitignoreToIgnoreGlobs('\n# a comment\n\n   \n'), []);
  });

  it('skips negation (!) lines conservatively', () => {
    assert.deepEqual(gitignoreToIgnoreGlobs('!keep.ts'), []);
  });

  it('unescapes a leading \\# / \\! literal', () => {
    assert.deepEqual(gitignoreToIgnoreGlobs('\\#literal.ts'), ['**/#literal.ts', '**/#literal.ts/**']);
    assert.deepEqual(gitignoreToIgnoreGlobs('\\!literal.ts'), ['**/!literal.ts', '**/!literal.ts/**']);
  });

  it('ignores trailing whitespace on a pattern', () => {
    assert.deepEqual(gitignoreToIgnoreGlobs('secret.ts   '), ['**/secret.ts', '**/secret.ts/**']);
  });

  it('skips a line that is only a slash', () => {
    assert.deepEqual(gitignoreToIgnoreGlobs('/'), []);
  });

  it('anchors a bare name under baseRelDir for nested gitignores', () => {
    assert.deepEqual(gitignoreToIgnoreGlobs('local.ts', 'sub'), ['sub/**/local.ts', 'sub/**/local.ts/**']);
  });

  it('anchors a path-bearing pattern directly under baseRelDir', () => {
    assert.deepEqual(gitignoreToIgnoreGlobs('/local.ts', 'sub'), ['sub/local.ts', 'sub/local.ts/**']);
  });
});

// ---------------------------------------------------------------------------
// Integration: buildIndex honors ignore rules in the glob fallback branch
// ---------------------------------------------------------------------------

function relPaths(idx: WorkspaceIndexer): string[] {
  return idx.getFiles().map(f => f.relativePath.replace(/\\/g, '/')).sort();
}

describe('WorkspaceIndexer buildIndex — .gitignore honoring (glob fallback)', () => {
  it('does not index a file excluded by the root .gitignore', async () => {
    const root = mkTmp();
    write(root, 'package.json', '{"name":"fixture"}\n');
    write(root, 'src/keep.ts', 'export function keepMe(): void {}\n');
    write(root, 'src/secret.ts', 'export function secretKey(): void {}\n');
    write(root, '.gitignore', 'secret.ts\n');

    const idx = new WorkspaceIndexer(root);
    await idx.buildIndex();

    const paths = relPaths(idx);
    assert.ok(paths.includes('src/keep.ts'), 'kept file is indexed');
    assert.ok(!paths.includes('src/secret.ts'), 'ignored file must NOT be indexed');
    // The excluded symbol must not leak into search or the summary.
    assert.deepEqual(idx.searchSymbols('secretKey'), []);
    assert.ok(!idx.getSummary().includes('secret.ts'));
  });

  it('honors a directory (trailing-slash) ignore pattern', async () => {
    const root = mkTmp();
    write(root, 'package.json', '{"name":"fixture"}\n');
    write(root, 'src/app.ts', 'export function app(): void {}\n');
    write(root, 'generated/out.ts', 'export function generatedThing(): void {}\n');
    write(root, '.gitignore', 'generated/\n');

    const idx = new WorkspaceIndexer(root);
    await idx.buildIndex();

    const paths = relPaths(idx);
    assert.ok(paths.includes('src/app.ts'));
    assert.ok(!paths.some(p => p.startsWith('generated/')), 'files under an ignored dir must be excluded');
  });

  it('honors an anchored (/name) pattern only at the root', async () => {
    const root = mkTmp();
    write(root, 'package.json', '{"name":"fixture"}\n');
    write(root, 'config.ts', 'export function rootConfig(): void {}\n');
    write(root, 'src/config.ts', 'export function nestedConfig(): void {}\n');
    write(root, '.gitignore', '/config.ts\n');

    const idx = new WorkspaceIndexer(root);
    await idx.buildIndex();

    const paths = relPaths(idx);
    assert.ok(!paths.includes('config.ts'), 'anchored pattern excludes root file');
    assert.ok(paths.includes('src/config.ts'), 'anchored pattern leaves nested same-name file');
  });

  it('honors a nested .gitignore scoped to its own directory', async () => {
    const root = mkTmp();
    write(root, 'package.json', '{"name":"fixture"}\n');
    write(root, 'pkg/local.ts', 'export function pkgLocal(): void {}\n');
    write(root, 'other/local.ts', 'export function otherLocal(): void {}\n');
    write(root, 'pkg/.gitignore', 'local.ts\n');

    const idx = new WorkspaceIndexer(root);
    await idx.buildIndex();

    const paths = relPaths(idx);
    assert.ok(!paths.includes('pkg/local.ts'), 'nested .gitignore excludes its own dir');
    assert.ok(paths.includes('other/local.ts'), 'nested .gitignore does not affect siblings');
  });

  it('honors .git/info/exclude', async () => {
    const root = mkTmp();
    write(root, 'package.json', '{"name":"fixture"}\n');
    write(root, 'src/keep.ts', 'export function keepThis(): void {}\n');
    write(root, 'src/excluded.ts', 'export function excludedThing(): void {}\n');
    // A bare .git/info/exclude without a real repo: `git ls-files` fails, so
    // buildIndex stays on the glob fallback and the exclude file is honored.
    write(root, path.join('.git', 'info', 'exclude'), 'excluded.ts\n');

    const idx = new WorkspaceIndexer(root);
    await idx.buildIndex();

    const paths = relPaths(idx);
    assert.ok(paths.includes('src/keep.ts'));
    assert.ok(!paths.includes('src/excluded.ts'), '.git/info/exclude must be honored');
  });

  it('indexes everything when there are no ignore rules', async () => {
    const root = mkTmp();
    write(root, 'package.json', '{"name":"fixture"}\n');
    write(root, 'src/a.ts', 'export function aFn(): void {}\n');
    write(root, 'src/b.ts', 'export function bFn(): void {}\n');

    const idx = new WorkspaceIndexer(root);
    await idx.buildIndex();

    const paths = relPaths(idx);
    assert.ok(paths.includes('src/a.ts'));
    assert.ok(paths.includes('src/b.ts'));
  });
});
