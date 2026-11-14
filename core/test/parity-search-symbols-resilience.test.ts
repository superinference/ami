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
