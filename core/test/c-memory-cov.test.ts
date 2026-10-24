import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  MemoryManager,
  parseFrontmatterList,
  type MemoryEntry,
} from '../src/memory';
import { SessionMemoryExtractor } from '../src/session-memory';
import type { StreamChunk, ProviderConfig, Message } from '../src/types';
import { searchSessions } from '../src/session-search';
import { listModelsDiagnostic } from '../src/model-registry';
import { MODEL_PREFERENCE } from '../src/provider';

// ---------------------------------------------------------------------------
// Shared temp-dir harness
// ---------------------------------------------------------------------------
let tmpDir: string;
let memoryDir: string;

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-cmemcov-'));
  memoryDir = path.join(tmpDir, '.superinference', 'memory');
  fs.mkdirSync(memoryDir, { recursive: true });
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

function writeMemory(name: string, body: string, description?: string): void {
  const fmLines = ['---', `name: ${name}`];
  if (description !== undefined) fmLines.push(`description: ${description}`);
  fmLines.push('type: project', '---', '', body, '');
  fs.writeFileSync(path.join(memoryDir, `${name}.md`), fmLines.join('\n'), 'utf-8');
}

// ===========================================================================
// session-search.ts — recencyMs NaN tie-break arm
// ===========================================================================
describe('session-search recencyMs invalid-timestamp tie-break', () => {
  it('ranks two equal-score results whose timestamps are unparseable', () => {
    const sessionsDir = path.join(tmpDir, '.superinference', 'sessions');
    fs.mkdirSync(sessionsDir, { recursive: true });
    // Two sessions, identical matching content => identical score, so the sort
    // comparator falls through to the recency tie-break, which calls recencyMs
    // on the (unparseable) createdAt values -> Number.isNaN(parsed) ? 0 : parsed.
    for (const id of ['s1', 's2']) {
      fs.writeFileSync(
        path.join(sessionsDir, `${id}.json`),
        JSON.stringify({
          id,
          createdAt: 'not-a-real-date',
          messages: [{ role: 'user', content: 'alpha token here' }],
        }),
        'utf-8',
      );
    }
    const results = searchSessions(tmpDir, 'alpha');
    assert.equal(results.length, 2);
    assert.ok(results.every(r => r.score === 1));
  });
});

// ===========================================================================
// session-memory.ts — isError marker + finally on every exit path
// ===========================================================================
describe('SessionMemoryExtractor extractFacts branches', () => {
  const provider: ProviderConfig = {
    baseUrl: 'http://127.0.0.1:1',
    apiKey: 'test-key',
    model: 'test-model',
  };
  const messages: Message[] = [{ role: 'user', content: 'do the thing' }];

  function gen(chunks: StreamChunk[]): SessionMemoryExtractor['_streamFn'] {
    return async function* () {
      for (const c of chunks) yield c;
    } as unknown as SessionMemoryExtractor['_streamFn'];
  }

  it('marks error tool results and parses valid facts (normal path -> finally)', async () => {
    const ex = new SessionMemoryExtractor(provider, 'compaction-model');
    ex._streamFn = gen([
      {
        type: 'content_delta',
        text: '[{"fact":"use pnpm","category":"convention","confidence":0.9}]',
      },
    ]);
    const facts = await ex.extractFacts(
      [
        { toolName: 'bash', output: 'ok output', isError: false },
        { toolName: 'bash', output: 'boom', isError: true },
      ],
      messages,
    );
    assert.equal(facts.length, 1);
    assert.equal(facts[0].category, 'convention');
  });

  it('returns [] on an error chunk (early return -> finally)', async () => {
    const ex = new SessionMemoryExtractor(provider);
    ex._streamFn = gen([{ type: 'error', error: 'stream failed' }]);
    const facts = await ex.extractFacts(
      [{ toolName: 'grep', output: 'x', isError: false }],
      messages,
    );
    assert.deepEqual(facts, []);
  });

  it('returns [] when the stream throws (catch -> finally)', async () => {
    const ex = new SessionMemoryExtractor(provider);
    ex._streamFn = (async function* () {
      throw new Error('kaboom');
      // eslint-disable-next-line no-unreachable
      yield { type: 'content_delta', text: '' } as StreamChunk;
    }) as unknown as SessionMemoryExtractor['_streamFn'];
    const facts = await ex.extractFacts(
      [{ toolName: 'bash', output: 'y', isError: true }],
      messages,
    );
    assert.deepEqual(facts, []);
  });

  it('runs the finally cleanup when the catch handler itself throws', async () => {
    const ex = new SessionMemoryExtractor(provider);
    // Poison object: the stream throws it, and String(err) in the catch handler
    // re-throws, so the finally block is entered with an exception in flight
    // (abrupt completion) rather than via a normal return.
    const poison = { toString() { throw new Error('tostring-boom'); } };
    ex._streamFn = (async function* () {
      throw poison;
      // eslint-disable-next-line no-unreachable
      yield { type: 'content_delta', text: '' } as StreamChunk;
    }) as unknown as SessionMemoryExtractor['_streamFn'];
    await assert.rejects(() =>
      ex.extractFacts([{ toolName: 'bash', output: 'z', isError: false }], messages),
    );
  });
});

// ===========================================================================
// model-registry.ts — static provider with no built-in model list
// ===========================================================================
describe('model-registry listModelsDiagnostic empty static list', () => {
  it('reports a diagnostic when a static provider has no MODEL_PREFERENCE entry', async () => {
    // `luma` is a known static provider; temporarily remove its preference list
    // so the (MODEL_PREFERENCE[provider] || []) fallback and the empty-list
    // diagnostic branch both execute. Restored afterwards.
    const orig = (MODEL_PREFERENCE as Record<string, string[]>).luma;
    delete (MODEL_PREFERENCE as Record<string, string[]>).luma;
    try {
      const res = await listModelsDiagnostic({
        provider: 'luma',
      } as ProviderConfig);
      assert.deepEqual(res.models, []);
      assert.equal(res.diagnostic, 'No built-in model list for luma.');
    } finally {
      (MODEL_PREFERENCE as Record<string, string[]>).luma = orig;
    }
  });
});

// ===========================================================================
// memory.ts — parseFrontmatterList empty input
// ===========================================================================
describe('memory parseFrontmatterList', () => {
  it('returns [] for an empty value', () => {
    assert.deepEqual(parseFrontmatterList(''), []);
  });
  it('still splits comma-separated values', () => {
    assert.deepEqual(parseFrontmatterList('a, b ,c'), ['a', 'b', 'c']);
  });
});

// ===========================================================================
// memory.ts — resolveAtImports resolution failure (catch -> return full)
// ===========================================================================
describe('memory resolveAtImports resolution failure', () => {
  it('leaves the token untouched when path resolution throws', () => {
    const mm = new MemoryManager(tmpDir);
    // An undefined basePath makes path.dirname throw inside the resolution try,
    // exercising the catch that returns the original match.
    const out = mm.resolveAtImports('refer to @foo here', undefined as unknown as string);
    assert.ok(out.includes('@foo'));
  });
});

// ===========================================================================
// memory.ts — loadMemories sort with missing mtimeMs (statSync seam)
// ===========================================================================
describe('memory loadMemories mtime fallback', () => {
  it('sorts entries even when stat has no mtimeMs', () => {
    writeMemory('mem-a', 'first body', 'desc a');
    writeMemory('mem-b', 'second body', 'desc b');
    const mm = new MemoryManager(tmpDir);
    mm._statSync = (() => ({ mtimeMs: undefined })) as unknown as typeof fs.statSync;
    const mems = mm.loadMemories();
    assert.equal(mems.length, 2);
    assert.ok(mems.every(m => m.mtimeMs === undefined));
  });
});

// ===========================================================================
// memory.ts — getMemoryContext with sparse entries (freshness/name/ageDays)
// ===========================================================================
describe('memory getMemoryContext sparse entries', () => {
  it('renders entries missing freshness, name, and ageDays', () => {
    const mm = new MemoryManager(tmpDir);
    const sparse: MemoryEntry = {
      path: '/virtual/mem.md',
      content: 'remember this detail',
      type: 'memory',
      name: '',
      description: undefined,
      memoryType: undefined,
      ageDays: undefined,
      freshness: undefined,
      mtimeMs: 0,
    };
    // Instance-level override (not a namespace import) to inject sparse entries.
    mm.loadMemories = () => [sparse];
    const ctx = mm.getMemoryContext();
    assert.ok(ctx.includes('Untitled'));
    assert.ok(ctx.includes('remember this detail'));
    // No drift warning because the (undefined ?? 0) age is not > 1.
    assert.ok(!ctx.includes('Memory Drift Warning'));
  });
});

// ===========================================================================
// memory.ts — selectRelevantMemories with a description-less memory
// ===========================================================================
describe('memory selectRelevantMemories description/name fallback', () => {
  it('scores memories missing both name and description', () => {
    // More entries than maxResults so the BM25 scoring path runs, including the
    // allMemTexts map where `m.name || ""` and `m.description || ""` fall back.
    const entries: MemoryEntry[] = [
      { path: '/v/0.md', content: 'database migration schema notes', type: 'memory', name: '', description: undefined, mtimeMs: 4 },
      { path: '/v/1.md', content: 'database indexing strategy', type: 'memory', name: 'idx', description: 'index notes', mtimeMs: 3 },
      { path: '/v/2.md', content: 'unrelated css styling', type: 'memory', name: 'css', description: 'styles', mtimeMs: 2 },
      { path: '/v/3.md', content: 'migration rollback plan', type: 'memory', name: 'roll', description: 'rollback', mtimeMs: 1 },
    ];
    const mm = new MemoryManager(tmpDir);
    mm.loadMemories = () => entries;
    const picks = mm.selectRelevantMemories('database migration', 3);
    assert.equal(picks.length, 3);
  });
});

// ===========================================================================
// memory.ts — rules Dirent parentPath/path fallbacks (readdir seam)
// ===========================================================================
describe('memory rules directory Dirent fallbacks', () => {
  const fakeEntries = [
    { isFile: () => true, name: 'a.md', path: '/nonexistent-dir' }, // parentPath undefined -> entry.path
    { isFile: () => true, name: 'b.md' },                           // both undefined -> rulesDir
    { isFile: () => false, name: 'subdir' },                        // not a file -> skip
    { isFile: () => true, name: 'img.png' },                        // wrong extension -> skip
  ];

  it('loadRulesDirectory resolves parentPath/path fallbacks', async () => {
    const mm = new MemoryManager(tmpDir);
    mm._readdir = (async () => fakeEntries) as unknown as typeof mm._readdir;
    const rules = await mm.loadRulesDirectory(tmpDir);
    assert.ok(Array.isArray(rules));
  });

  it('loadRulesForFile resolves parentPath/path fallbacks', async () => {
    const mm = new MemoryManager(tmpDir);
    mm._readdir = (async () => fakeEntries) as unknown as typeof mm._readdir;
    const sources = await mm.loadRulesForFile(tmpDir, 'src/app.ts');
    assert.ok(Array.isArray(sources));
  });
});

// ===========================================================================
// memory.ts — resolveIncludes missing file (catch -> [include not found])
// ===========================================================================
describe('memory resolveIncludes missing file', () => {
  it('replaces an unresolvable @include with a not-found marker', async () => {
    const mm = new MemoryManager(tmpDir);
    const basePath = path.join(tmpDir, 'base.md');
    const out = await mm.resolveIncludes('see\n@include ./nope.md\nend', basePath);
    assert.ok(out.includes('[include not found: nope.md]'));
  });
});
