import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  WorkspaceIndexer,
  indexBuildConfig,
  indexerHooks,
} from '../src/workspace-indexer';
import { searchSymbolsTool, _resetIndexer } from '../src/tools/search-symbols';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// U109 async-build cluster — parity with the reference's file-index
// loadFromFileListAsync (references native-ts/file-index/index.ts L34-133):
//   - CHUNK_MS time-based async-build knob (indexBuildConfig.chunkMs)
//   - Time-based cooperative yielding during the build (indexerHooks.yieldToEventLoop)
//   - Progressive queryability — search a partial index while the build continues
//     ({ queryable, done } contract from buildIndexAsync)
// ---------------------------------------------------------------------------

function mkTmp(prefix = 'wsidx-async-'): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}
function write(root: string, rel: string, content: string): void {
  const abs = path.join(root, rel);
  fs.mkdirSync(path.dirname(abs), { recursive: true });
  fs.writeFileSync(abs, content);
}

const ORIG = { chunkMs: indexBuildConfig.chunkMs, interval: indexBuildConfig.chunkCheckInterval };

afterEach(() => {
  indexBuildConfig.chunkMs = ORIG.chunkMs;
  indexBuildConfig.chunkCheckInterval = ORIG.interval;
  mock.restoreAll();
});

// ===========================================================================
// Gap — CHUNK_MS knob + time-based cooperative yielding
// ===========================================================================

describe('U109 async — cooperative yielding on the CHUNK_MS budget', () => {
  it('yields to the event loop during the build when the time budget is exceeded', async () => {
    const root = mkTmp();
    try {
      write(root, 'package.json', '{}');
      for (let i = 0; i < 4; i++) write(root, `f${i}.ts`, `export function fn${i}() {}\n`);

      // Force a yield after every file: check every file, budget below any
      // elapsed wall-clock time.
      indexBuildConfig.chunkCheckInterval = 1;
      indexBuildConfig.chunkMs = -1;
      const yieldSpy = mock.method(indexerHooks, 'yieldToEventLoop');

      const idx = new WorkspaceIndexer(root);
      const result = await idx.buildIndex();

      assert.equal(result.fileCount, 4, 'all files indexed');
      assert.ok(yieldSpy.mock.callCount() >= 1, `expected cooperative yields, got ${yieldSpy.mock.callCount()}`);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it('does NOT yield when the per-file time budget is never exceeded', async () => {
    const root = mkTmp();
    try {
      write(root, 'package.json', '{}');
      for (let i = 0; i < 4; i++) write(root, `f${i}.ts`, `export function fn${i}() {}\n`);

      // A huge budget means the elapsed check never trips → no yields.
      indexBuildConfig.chunkCheckInterval = 1;
      indexBuildConfig.chunkMs = 60_000;
      const yieldSpy = mock.method(indexerHooks, 'yieldToEventLoop');

      const idx = new WorkspaceIndexer(root);
      await idx.buildIndex();

      assert.equal(yieldSpy.mock.callCount(), 0, 'no yields under a huge time budget');
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

// ===========================================================================
// Gap — progressive queryability ({ queryable, done })
// ===========================================================================

describe('U109 async — progressive queryability of a partial index', () => {
  it('queryable resolves on the first chunk; the index is searchable before done', async () => {
    const root = mkTmp();
    try {
      write(root, 'package.json', '{}');
      for (let i = 0; i < 3; i++) write(root, `f${i}.ts`, `export function fn${i}() {}\n`);

      // Yield (and mark queryable) after the very first file.
      indexBuildConfig.chunkCheckInterval = 1;
      indexBuildConfig.chunkMs = -1;

      const idx = new WorkspaceIndexer(root);
      const { queryable, done } = idx.buildIndexAsync();

      await queryable;
      // After the first yield the build is suspended on setImmediate (a macrotask),
      // so our microtask continuation sees EXACTLY the partial prefix: 1 file.
      assert.equal(idx.getFiles().length, 1, 'partial index is queryable before the build finishes');
      const first = idx.getFiles()[0]!;
      const partialHit = idx.searchSymbols(first.symbols[0]!.name).map(s => s.name);
      assert.ok(partialHit.includes(first.symbols[0]!.name), 'search works against the partial index');

      const result = await done;
      assert.equal(result.fileCount, 3, 'done resolves with the full index');
      assert.equal(result.cached, false);
      assert.equal(idx.getFiles().length, 3, 'all files indexed once done resolves');
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it('queryable still settles for a cached (no-op) build so a waiter never hangs', async () => {
    const root = mkTmp();
    try {
      write(root, 'package.json', '{}');
      write(root, 'a.ts', 'export function foo() {}\n');
      const idx = new WorkspaceIndexer(root);
      await idx.buildIndex(); // prime the cache

      const { queryable, done } = idx.buildIndexAsync();
      await queryable; // must not hang even though no chunk boundary is hit
      const result = await done;
      assert.equal(result.cached, true, 'second identical build is cached');
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it('queryable settles even when the build has no project root (skip path)', async () => {
    const root = mkTmp();
    try {
      const deep = path.join(root, 'nested', 'deep');
      fs.mkdirSync(deep, { recursive: true });
      const idx = new WorkspaceIndexer(deep);
      const { queryable, done } = idx.buildIndexAsync();
      await queryable;
      const result = await done;
      assert.equal(result.discovery, 'none');
      assert.equal(result.skippedReason, 'no project root found');
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

// ===========================================================================
// E2E — the search_symbols tool serves results off the async/partial index
// ===========================================================================

describe('U109 async — search_symbols tool works end-to-end over the async build', () => {
  beforeEach(() => { _resetIndexer(); });
  afterEach(() => { _resetIndexer(); });

  it('finds a symbol through the chunked background build wired into the tool', async () => {
    const root = mkTmp();
    try {
      write(root, 'package.json', '{}');
      write(root, 'svc.ts', 'export class PaymentService {}\nexport function chargeCard() {}\n');

      // Force frequent yields so the background build exercises the chunked path.
      indexBuildConfig.chunkCheckInterval = 1;
      indexBuildConfig.chunkMs = -1;

      const ctx: ToolContext = { cwd: root, abortSignal: new AbortController().signal };
      const result = await searchSymbolsTool.execute({ query: 'PaymentService' }, ctx);
      assert.ok(!result.isError, result.output);
      assert.ok(result.output.includes('PaymentService'), result.output);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
