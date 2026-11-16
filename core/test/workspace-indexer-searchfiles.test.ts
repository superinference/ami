/**
 * Integration test: WorkspaceIndexer.searchFiles wires the real buildIndex
 * (disk walk) into the scalable FileIndex (file-path fuzzy ranking). Proves the
 * end-to-end path a `/files <query>` finder drives — the indexer collects the
 * workspace file set, FileIndex ranks whole paths, source files edge out tests.
 */
import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { WorkspaceIndexer } from '../src/workspace-indexer';

describe('WorkspaceIndexer.searchFiles', () => {
  let tmpDir: string;
  let indexer: WorkspaceIndexer;

  before(async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wsi-files-'));
    fs.writeFileSync(path.join(tmpDir, 'package.json'), '{}');
    fs.mkdirSync(path.join(tmpDir, 'src', 'components'), { recursive: true });
    fs.mkdirSync(path.join(tmpDir, 'src', 'utils'), { recursive: true });
    fs.writeFileSync(path.join(tmpDir, 'src', 'components', 'Button.ts'), 'export const x = 1;\n');
    fs.writeFileSync(path.join(tmpDir, 'src', 'utils', 'helpers.ts'), 'export const y = 2;\n');
    fs.writeFileSync(path.join(tmpDir, 'src', 'components', 'Button.test.ts'), 'export const z = 3;\n');
    indexer = new WorkspaceIndexer(tmpDir);
    await indexer.buildIndex();
  });

  after(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('finds a file by a bare fragment anywhere in the tree (relative paths)', () => {
    const res = indexer.searchFiles('button', 10);
    const paths = res.map(r => r.path);
    assert.ok(paths.includes('src/components/Button.ts'), paths.join(','));
    // Best match floats to score 0.0.
    assert.equal(res[0]!.score, 0.0);
  });

  it('ranks the source file above its test namesake', () => {
    const res = indexer.searchFiles('button', 10);
    const paths = res.map(r => r.path);
    const src = paths.indexOf('src/components/Button.ts');
    const test = paths.indexOf('src/components/Button.test.ts');
    assert.ok(src >= 0 && test >= 0, paths.join(','));
    assert.ok(src < test, `source must outrank test: ${paths.join(',')}`);
  });

  it('respects the limit and returns [] for a non-positive limit', () => {
    assert.deepEqual(indexer.searchFiles('button', 0), []);
    assert.equal(indexer.searchFiles('ts', 1).length, 1);
  });

  it('returns [] for a query that matches no path', () => {
    assert.deepEqual(indexer.searchFiles('zzqq', 10), []);
  });
});
