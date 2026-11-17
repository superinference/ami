import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  filterGitIgnoredLocations,
  _gitIgnoreFilterHooks,
} from '../src/tools/lsp-gitignore-filter';

interface Loc { uri: string }
const loc = (p: string): Loc => ({ uri: `file://${p}` });
const uriOf = (l: Loc) => l.uri;

afterEach(() => mock.restoreAll());

describe('filterGitIgnoredLocations (U213)', () => {
  it('drops locations whose path git reports as ignored (exit 0 + stdout)', async () => {
    mock.method(_gitIgnoreFilterHooks, 'execFileAsync', async () => ({
      stdout: '/proj/dist/bundle.js\n',
      stderr: '',
    }));
    const items = [loc('/proj/src/a.ts'), loc('/proj/dist/bundle.js')];
    const out = await filterGitIgnoredLocations(items, uriOf, '/proj');
    assert.deepEqual(out.map(uriOf), ['file:///proj/src/a.ts']);
  });

  it('keeps everything when nothing is ignored (git exit 1 → empty stdout)', async () => {
    mock.method(_gitIgnoreFilterHooks, 'execFileAsync', async () => ({ stdout: '', stderr: '' }));
    const items = [loc('/proj/src/a.ts'), loc('/proj/src/b.ts')];
    const out = await filterGitIgnoredLocations(items, uriOf, '/proj');
    assert.equal(out.length, 2);
  });

  it('degrades to pass-through when git is not a repo (exit 128 rejects, no stdout)', async () => {
    mock.method(_gitIgnoreFilterHooks, 'execFileAsync', async () => {
      const err = Object.assign(new Error('not a git repository'), { code: 128 });
      throw err;
    });
    const items = [loc('/proj/src/a.ts')];
    const out = await filterGitIgnoredLocations(items, uriOf, '/proj');
    assert.equal(out.length, 1);
  });

  it('harvests stdout off a thrown error (some platforms reject non-empty stdout)', async () => {
    mock.method(_gitIgnoreFilterHooks, 'execFileAsync', async () => {
      throw Object.assign(new Error('x'), { code: 1, stdout: '/proj/vendor/x.ts\n' });
    });
    const items = [loc('/proj/src/a.ts'), loc('/proj/vendor/x.ts')];
    const out = await filterGitIgnoredLocations(items, uriOf, '/proj');
    assert.deepEqual(out.map(uriOf), ['file:///proj/src/a.ts']);
  });

  it('batches check-ignore in groups of 50', async () => {
    const batches: number[] = [];
    mock.method(_gitIgnoreFilterHooks, 'execFileAsync', async (_cmd: string, args: string[]) => {
      // args = ['check-ignore', ...paths]
      batches.push(args.length - 1);
      return { stdout: '', stderr: '' };
    });
    const items = Array.from({ length: 120 }, (_, i) => loc(`/proj/f${i}.ts`));
    await filterGitIgnoredLocations(items, uriOf, '/proj');
    assert.deepEqual(batches, [50, 50, 20]);
  });

  it('returns the input unchanged for an empty list (no subprocess)', async () => {
    let called = 0;
    mock.method(_gitIgnoreFilterHooks, 'execFileAsync', async () => {
      called++;
      return { stdout: '', stderr: '' };
    });
    const out = await filterGitIgnoredLocations([] as Loc[], uriOf, '/proj');
    assert.equal(out.length, 0);
    assert.equal(called, 0);
  });

  it('skips items with no URI and de-dupes repeated URIs into one path', async () => {
    const seenBatches: string[][] = [];
    mock.method(_gitIgnoreFilterHooks, 'execFileAsync', async (_cmd: string, args: string[]) => {
      seenBatches.push(args.slice(1));
      return { stdout: '', stderr: '' };
    });
    const items: Loc[] = [loc('/proj/a.ts'), loc('/proj/a.ts'), { uri: undefined as unknown as string }];
    await filterGitIgnoredLocations(items, uriOf, '/proj');
    // Only the one unique, defined path is checked.
    assert.deepEqual(seenBatches, [['/proj/a.ts']]);
  });

  it('passes the 5s timeout to the subprocess', async () => {
    let opts: { timeout?: number } | undefined;
    mock.method(_gitIgnoreFilterHooks, 'execFileAsync', async (_c: string, _a: string[], o: { timeout?: number }) => {
      opts = o;
      return { stdout: '', stderr: '' };
    });
    await filterGitIgnoredLocations([loc('/proj/a.ts')], uriOf, '/proj');
    assert.equal(opts?.timeout, 5_000);
  });
});
