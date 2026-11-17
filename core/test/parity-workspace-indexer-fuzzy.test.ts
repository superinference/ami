import { describe, it, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  WorkspaceIndexer,
  fuzzySymbolScore,
  MAX_SYMBOL_QUERY_LEN,
  indexerHooks,
} from '../src/workspace-indexer';

// ---------------------------------------------------------------------------
// Helpers — fixtures in fresh temp dirs (no coupling to the on-disk .ts tree).
// ---------------------------------------------------------------------------

function makeProjectDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wsi-fuzzy-'));
  fs.writeFileSync(path.join(dir, 'package.json'), '{}');
  return dir;
}
function cleanup(dir: string): void {
  fs.rmSync(dir, { recursive: true, force: true });
}
function write(dir: string, rel: string, content: string): string {
  const abs = path.join(dir, rel);
  fs.mkdirSync(path.dirname(abs), { recursive: true });
  fs.writeFileSync(abs, content);
  return abs;
}

// ---------------------------------------------------------------------------
// Gap: Fuzzy (subsequence) symbol/file search with weighted scoring
// ---------------------------------------------------------------------------

describe('searchSymbols — fuzzy subsequence matching', () => {
  it('matches a non-contiguous subsequence query (old substring test could not)', async () => {
    const dir = makeProjectDir();
    try {
      write(dir, 'wsi.ts', 'export class WorkspaceIndexer {}\n');
      const idx = new WorkspaceIndexer(dir);
      await idx.indexFile(path.join(dir, 'wsi.ts'));

      // 'wspidx' is a subsequence of 'WorkspaceIndexer' but NOT a substring.
      const names = idx.searchSymbols('wspidx').map(s => s.name);
      assert.ok(
        names.includes('WorkspaceIndexer'),
        'subsequence query must match WorkspaceIndexer',
      );
    } finally {
      cleanup(dir);
    }
  });
});

describe('fuzzySymbolScore — weighted scoring', () => {
  it('returns null for a non-subsequence and for an empty needle', () => {
    assert.equal(fuzzySymbolScore('hello', 'xyz'), null);
    assert.equal(fuzzySymbolScore('hello', ''), null);
  });

  it('rewards a boundary match over a scattered one (equal length)', () => {
    const boundary = fuzzySymbolScore('a_b_cd', 'ab')!;
    const scattered = fuzzySymbolScore('aXXXXb', 'ab')!;
    assert.ok(boundary !== null && scattered !== null);
    assert.ok(boundary > scattered, 'boundary bonus should outrank gaps');
  });

  it('rewards a camelCase boundary over a plain consecutive match', () => {
    const camel = fuzzySymbolScore('aB', 'ab')!;
    const plain = fuzzySymbolScore('ab', 'ab')!;
    assert.ok(camel > plain, 'camelCase bonus should apply');
  });
});

// ---------------------------------------------------------------------------
// Gap: Complete-scan ranking vs insertion-order early break
// ---------------------------------------------------------------------------

describe('searchSymbols — complete scan (no insertion-order early break)', () => {
  it('surfaces the best match even when defined after limit*3 weaker ones', async () => {
    const dir = makeProjectDir();
    try {
      const weak = Array.from(
        { length: 20 },
        (_, i) => `export function fnXyz${i}() {}`,
      );
      // The exact match is defined LAST, so an insertion-order early-break at
      // limit*3 (=15) would never collect it.
      const body = weak.join('\n') + '\nexport function xyz() {}\n';
      write(dir, 'many.ts', body);
      const idx = new WorkspaceIndexer(dir);
      await idx.indexFile(path.join(dir, 'many.ts'));

      const results = idx.searchSymbols('xyz', 5);
      assert.equal(results[0]?.name, 'xyz', 'global best must rank first');
    } finally {
      cleanup(dir);
    }
  });

  it('preserves exact > starts-with > contains ordering', async () => {
    const dir = makeProjectDir();
    try {
      write(
        dir,
        's.ts',
        [
          'export function run() {}',
          'export function runner() {}',
          'export function prerun() {}',
        ].join('\n'),
      );
      const idx = new WorkspaceIndexer(dir);
      await idx.indexFile(path.join(dir, 's.ts'));
      const names = idx.searchSymbols('run').map(s => s.name);
      assert.deepEqual(names, ['run', 'runner', 'prerun']);
    } finally {
      cleanup(dir);
    }
  });
});

// ---------------------------------------------------------------------------
// Gap: Test-file de-prioritization heuristic in ranking
// ---------------------------------------------------------------------------

describe('searchSymbols — test-path de-prioritization', () => {
  it('ranks the implementation above an equally-named test symbol', async () => {
    const dir = makeProjectDir();
    try {
      write(dir, 'src/handler.test.ts', 'export function handler() {}\n');
      write(dir, 'src/impl.ts', 'export function handler() {}\n');
      const idx = new WorkspaceIndexer(dir);
      // Index the TEST file first so insertion order favors it; only the
      // test-path penalty can flip the implementation to the top.
      await idx.indexFile(path.join(dir, 'src/handler.test.ts'));
      await idx.indexFile(path.join(dir, 'src/impl.ts'));

      const top = idx.searchSymbols('handler')[0];
      assert.ok(top?.filePath.endsWith('impl.ts'), 'non-test file ranks first');
    } finally {
      cleanup(dir);
    }
  });
});

// ---------------------------------------------------------------------------
// Gap: MAX_QUERY_LEN query-length cap knob (DoS / resource guard)
// ---------------------------------------------------------------------------

describe('searchSymbols — query-length cap', () => {
  it('exposes a 64-char cap constant', () => {
    assert.equal(MAX_SYMBOL_QUERY_LEN, 64);
  });

  it('clamps an oversized query to the cap before scanning', async () => {
    const dir = makeProjectDir();
    try {
      const longName = 'a'.repeat(MAX_SYMBOL_QUERY_LEN);
      write(dir, 'z.ts', `export const ${longName} = 1;\n`);
      const idx = new WorkspaceIndexer(dir);
      await idx.indexFile(path.join(dir, 'z.ts'));

      // Clamped needle = first 64 'a's (a subsequence of the name); the trailing
      // 'b's past the cap are dropped. Without the clamp the needle contains 'b'
      // chars absent from the name and nothing matches.
      const query = 'a'.repeat(MAX_SYMBOL_QUERY_LEN) + 'b'.repeat(40);
      const names = idx.searchSymbols(query).map(s => s.name);
      assert.ok(names.includes(longName), 'clamped query must still match');
    } finally {
      cleanup(dir);
    }
  });

  it('returns [] for a non-positive limit', async () => {
    const dir = makeProjectDir();
    try {
      write(dir, 'z.ts', 'export function foo() {}\n');
      const idx = new WorkspaceIndexer(dir);
      await idx.indexFile(path.join(dir, 'z.ts'));
      assert.deepEqual(idx.searchSymbols('foo', 0), []);
      assert.deepEqual(idx.searchSymbols(''), []);
    } finally {
      cleanup(dir);
    }
  });
});

// ---------------------------------------------------------------------------
// Gap: Timeout/resource limit on the git subprocess used for file enumeration
// ---------------------------------------------------------------------------

describe('getGitTrackedFiles — bounded git subprocess', () => {
  it('passes a 5s timeout to the git ls-files invocation', async () => {
    const dir = makeProjectDir();
    write(dir, 'a.ts', 'export function fromGit() {}\n');
    let capturedTimeout: number | undefined;
    const execMock = mock.method(
      indexerHooks,
      'execFileNoThrow',
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (async (_cmd: string, _args: string[], opts: any) => {
        capturedTimeout = opts?.timeout;
        return { stdout: 'a.ts\0', stderr: '', code: 0 };
      }) as unknown as typeof indexerHooks.execFileNoThrow,
    );
    try {
      const idx = new WorkspaceIndexer(dir);
      await idx.buildIndex();
      assert.equal(capturedTimeout, 5_000, 'git subprocess must be time-bounded');
      assert.ok(idx.searchSymbols('fromGit').length > 0, 'indexed the git file');
    } finally {
      execMock.mock.restore();
      cleanup(dir);
    }
  });

  it('falls back to the glob scan when the git subprocess fails', async () => {
    const dir = makeProjectDir();
    write(dir, 'b.ts', 'export function fromGlob() {}\n');
    // The never-throw wrapper resolves a failure as a non-zero `code` (it does
    // not throw); getGitTrackedFiles treats that as "no git listing".
    const execMock = mock.method(
      indexerHooks,
      'execFileNoThrow',
      (async () => ({ stdout: '', stderr: 'simulated git timeout', code: 1, error: 'simulated git timeout' })) as unknown as typeof indexerHooks.execFileNoThrow,
    );
    try {
      const idx = new WorkspaceIndexer(dir);
      const res = await idx.buildIndex();
      assert.ok(res.fileCount > 0, 'glob fallback must still index files');
      assert.ok(idx.searchSymbols('fromGlob').length > 0);
    } finally {
      execMock.mock.restore();
      cleanup(dir);
    }
  });
});
