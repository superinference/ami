import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  searchSymbolsTool,
  _resetIndexer,
  indexerHooks,
  indexBuildTimeoutMs,
} from '../src/tools/search-symbols';
import type { WorkspaceIndexer } from '../src/workspace-indexer';
import type { ToolContext } from '../src/types';

function ctx(): ToolContext {
  return { cwd: '/tmp/parity-symbols', abortSignal: new AbortController().signal };
}

function fakeIndexer(opts: { build: () => Promise<unknown>; cwd?: string }): WorkspaceIndexer {
  return {
    cwd: opts.cwd ?? '/tmp/parity-symbols',
    buildIndex: opts.build,
    // Mirror the real buildIndexAsync contract: `done` is the build promise,
    // and `queryable` settles once the build settles (mirrors the real
    // indexer's finally-marked queryable), so a never-resolving build also has a
    // never-resolving queryable (exercising the timeout path).
    buildIndexAsync: () => {
      const done = opts.build();
      const queryable = Promise.resolve(done).then(() => {}, () => {});
      return { queryable, done };
    },
    searchSymbols: () => [],
    getStats: () => ({ fileCount: 0, symbolCount: 0 }),
  } as unknown as WorkspaceIndexer;
}

const origCreate = indexerHooks.create;

beforeEach(() => { _resetIndexer(); });
afterEach(() => {
  _resetIndexer();
  indexerHooks.create = origCreate;
  delete process.env.AMI_SYMBOL_INDEX_TIMEOUT_MS;
});

describe('search_symbols – index build timeout', () => {
  it('parses the timeout env flag with a sane default', () => {
    assert.equal(indexBuildTimeoutMs({} as NodeJS.ProcessEnv), 10_000);
    assert.equal(indexBuildTimeoutMs({ AMI_SYMBOL_INDEX_TIMEOUT_MS: '250' } as unknown as NodeJS.ProcessEnv), 250);
    assert.equal(indexBuildTimeoutMs({ AMI_SYMBOL_INDEX_TIMEOUT_MS: 'nope' } as unknown as NodeJS.ProcessEnv), 10_000);
    assert.equal(indexBuildTimeoutMs({ AMI_SYMBOL_INDEX_TIMEOUT_MS: '0' } as unknown as NodeJS.ProcessEnv), 10_000);
  });

  it('does not block forever on a stuck index build', async () => {
    process.env.AMI_SYMBOL_INDEX_TIMEOUT_MS = '40';
    let searched = false;
    indexerHooks.create = () => fakeIndexer({
      build: () => new Promise(() => { /* never resolves */ }),
    });
    // Replace searchSymbols to observe it still runs over the partial index.
    const base = fakeIndexer({ build: () => new Promise(() => {}) });
    (base as unknown as { searchSymbols: () => unknown[] }).searchSymbols = () => { searched = true; return []; };
    indexerHooks.create = () => base;

    const started = Date.now();
    const result = await searchSymbolsTool.execute({ query: 'foo' }, ctx());
    const elapsed = Date.now() - started;
    assert.ok(elapsed < 2000, `should return promptly, took ${elapsed}ms`);
    assert.equal(searched, true);
    assert.ok(/No symbols matching/.test(result.output), result.output);
  });
});

describe('search_symbols – honors turn cancellation', () => {
  it('returns promptly without searching when the turn is already aborted', async () => {
    let searched = false;
    const base = fakeIndexer({ build: () => Promise.resolve({}) });
    (base as unknown as { searchSymbols: () => unknown[] }).searchSymbols = () => {
      searched = true;
      return [];
    };
    indexerHooks.create = () => base;

    const ac = new AbortController();
    ac.abort();
    const result = await searchSymbolsTool.execute(
      { query: 'foo' },
      { cwd: '/tmp/parity-symbols', abortSignal: ac.signal } as ToolContext,
    );

    assert.equal(result.isError, true);
    assert.ok(/aborted/i.test(result.output), result.output);
    assert.equal(searched, false, 'must not search an aborted turn');
  });

  it('stops waiting on the build and aborts when the turn is cancelled mid-build', async () => {
    // Timeout high so only the abort (not the timeout) can end the wait.
    process.env.AMI_SYMBOL_INDEX_TIMEOUT_MS = '5000';
    let searched = false;
    const base = fakeIndexer({ build: () => new Promise(() => { /* never resolves */ }) });
    (base as unknown as { searchSymbols: () => unknown[] }).searchSymbols = () => {
      searched = true;
      return [];
    };
    indexerHooks.create = () => base;

    const ac = new AbortController();
    const started = Date.now();
    const p = searchSymbolsTool.execute(
      { query: 'foo' },
      { cwd: '/tmp/parity-symbols', abortSignal: ac.signal } as ToolContext,
    );
    setTimeout(() => ac.abort(), 20);
    const result = await p;
    const elapsed = Date.now() - started;

    assert.ok(elapsed < 2000, `should abort promptly, took ${elapsed}ms`);
    assert.equal(result.isError, true);
    assert.ok(/aborted/i.test(result.output), result.output);
    assert.equal(searched, false, 'must not search after an aborted wait');
  });
});

describe('search_symbols – type filter applies after the result cap', () => {
  it('finds a matching-type symbol ranked past the top-N cap', async () => {
    // Deterministic ranked master list: 30 functions, then one class ranked 31st.
    // searchSymbols honors the limit arg, so a pre-filter top-30 slice would hide it.
    const ranked = [] as Array<{ name: string; type: string; filePath: string; line: number; language: string }>;
    for (let i = 0; i < 30; i++) {
      ranked.push({ name: `fn${i}`, type: 'function', filePath: '/tmp/parity-symbols/a.ts', line: i + 1, language: 'ts' });
    }
    ranked.push({ name: 'Target', type: 'class', filePath: '/tmp/parity-symbols/a.ts', line: 99, language: 'ts' });

    const base = fakeIndexer({ build: () => Promise.resolve({}) });
    (base as unknown as { searchSymbols: (q: string, limit: number) => unknown[] }).searchSymbols =
      (_q: string, limit: number) => ranked.slice(0, limit);
    (base as unknown as { getStats: () => unknown }).getStats =
      () => ({ fileCount: 1, symbolCount: ranked.length });
    indexerHooks.create = () => base;

    const result = await searchSymbolsTool.execute({ query: 'x', type: 'class' }, ctx());

    assert.ok(!result.isError, result.output);
    assert.ok(result.output.includes('Target'), result.output);
    assert.ok(result.output.includes('(class)'), result.output);
  });
});

describe('search_symbols – build failure is retried', () => {
  it('rebuilds on the next call after a failed build instead of serving a dead index', async () => {
    let calls = 0;
    indexerHooks.create = () => {
      calls++;
      const willFail = calls === 1;
      return fakeIndexer({
        build: () => (willFail ? Promise.reject(new Error('transient fs error')) : Promise.resolve({})),
      });
    };

    await searchSymbolsTool.execute({ query: 'foo' }, ctx());
    assert.equal(calls, 1, 'first call builds once');

    // The first build rejected → the second call must rebuild (retry), not reuse.
    await searchSymbolsTool.execute({ query: 'foo' }, ctx());
    assert.equal(calls, 2, 'failed build is retried on the next call');

    // A successful build is cached — no third rebuild.
    await searchSymbolsTool.execute({ query: 'foo' }, ctx());
    assert.equal(calls, 2, 'successful build is reused');
  });
});
