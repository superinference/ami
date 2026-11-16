import { describe, it, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { execFileSync } from 'child_process';

import {
  WorkspaceIndexer,
  fuzzySymbolScore,
  SUMMARY_MAX_CHARS,
  SUMMARY_SNAPSHOT_CAVEAT,
  SUMMARY_TRUNCATION_HINT,
} from '../src/workspace-indexer';

// ---------------------------------------------------------------------------
// U109 — parity improvements to core/src/workspace-indexer.ts:
//   1. Smart-case symbol search (auto case-sensitivity from query casing)
//   2. Truncation affordance hint in getSummary()
//   3. Snapshot/staleness caveat on the injected summary
//   4. Change-detection caching to skip redundant full rebuilds
//   5. Stage-level diagnostic introspection from buildIndex()
// Fixtures live in fresh temp dirs (no coupling to the on-disk source tree).
// ---------------------------------------------------------------------------

function mkTmp(prefix = 'wsidx-u109-'): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}
function write(root: string, rel: string, content: string): void {
  const abs = path.join(root, rel);
  fs.mkdirSync(path.dirname(abs), { recursive: true });
  fs.writeFileSync(abs, content);
}

// ===========================================================================
// Gap 1 — Smart-case search
// ===========================================================================

describe('U109 gap1 — smart-case symbol search', () => {
  it('fuzzySymbolScore: default (case-insensitive) matches regardless of query casing', () => {
    assert.notEqual(fuzzySymbolScore('WorkspaceIndexer', 'wi'), null);
    assert.notEqual(fuzzySymbolScore('WorkspaceIndexer', 'wkidx'), null);
  });

  it('fuzzySymbolScore: caseSensitive=true requires exact-case subsequence', () => {
    // 'W' matches at 0, 'I' matches the capital I in "Indexer".
    assert.notEqual(fuzzySymbolScore('WorkspaceIndexer', 'WI', true), null);
    // An all-lowercase haystack has no uppercase chars to match a cased needle.
    assert.equal(fuzzySymbolScore('workspaceindexer', 'WI', true), null);
  });

  it('searchSymbols: an all-lowercase query matches case-insensitively', async () => {
    const root = mkTmp();
    try {
      write(root, 'package.json', '{}');
      write(root, 'a.ts', 'export function getData() {}\nexport class GetData {}\n');
      const idx = new WorkspaceIndexer(root);
      await idx.buildIndex();
      const names = idx.searchSymbols('getdata').map(s => s.name);
      assert.ok(names.includes('getData'), `expected getData in ${JSON.stringify(names)}`);
      assert.ok(names.includes('GetData'), `expected GetData in ${JSON.stringify(names)}`);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it('searchSymbols: a mixed-case query is case-sensitive (does not fold case)', async () => {
    const root = mkTmp();
    try {
      write(root, 'package.json', '{}');
      write(root, 'a.ts', 'export function getData() {}\nexport class GetData {}\n');
      const idx = new WorkspaceIndexer(root);
      await idx.buildIndex();
      // 'GetData' has uppercase → case-sensitive → must NOT return the lowercase
      // 'getData' function, only the exact-cased 'GetData' class.
      const names = idx.searchSymbols('GetData').map(s => s.name);
      assert.ok(names.includes('GetData'), `expected GetData in ${JSON.stringify(names)}`);
      assert.ok(!names.includes('getData'), `getData must NOT match case-sensitive query; got ${JSON.stringify(names)}`);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

// ===========================================================================
// Gaps 2 & 3 — getSummary affordances
// ===========================================================================

describe('U109 gap2/3 — getSummary snapshot caveat + truncation affordance', () => {
  it('prepends the snapshot/staleness caveat', async () => {
    const root = mkTmp();
    try {
      write(root, 'package.json', '{}');
      write(root, 'a.ts', 'export function foo() {}\nexport class Bar {}\n');
      const idx = new WorkspaceIndexer(root);
      await idx.buildIndex();
      const summary = idx.getSummary();
      assert.ok(summary.startsWith(SUMMARY_SNAPSHOT_CAVEAT), `summary should lead with caveat: ${summary.slice(0, 60)}`);
      assert.ok(summary.includes('Workspace:'));
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it('appends an actionable hint when truncated, staying within the budget', async () => {
    const root = mkTmp();
    try {
      write(root, 'package.json', '{}');
      // Very long class names (summary lists only the top 5, so names must be
      // long enough to overflow the budget) interlinked so they rank as key
      // symbols by dependents.
      const pad = 'X'.repeat(90);
      for (let i = 0; i < 8; i++) {
        const imp = i > 0 ? `import { VeryLongClassName${pad}${i - 1} } from './f${i - 1}';\n` : '';
        write(root, `f${i}.ts`, `${imp}export class VeryLongClassName${pad}${i} {}\n`);
      }
      const idx = new WorkspaceIndexer(root);
      await idx.buildIndex();
      const summary = idx.getSummary();
      assert.ok(summary.length <= SUMMARY_MAX_CHARS, `summary len ${summary.length} > ${SUMMARY_MAX_CHARS}`);
      assert.ok(summary.endsWith(SUMMARY_TRUNCATION_HINT), `truncated summary must end with the hint; tail: ${JSON.stringify(summary.slice(-70))}`);
      assert.ok(!summary.endsWith('...'), 'must use the actionable hint, not a bare ellipsis slice');
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

// ===========================================================================
// Gap 4 — change-detection caching
// ===========================================================================

describe('U109 gap4 — change-detection caching skips redundant rebuilds', () => {
  it('glob path: a second build with an identical file set is cached (no re-parse)', async () => {
    const root = mkTmp();
    try {
      write(root, 'package.json', '{}');
      write(root, 'a.ts', 'export function foo() {}\n');
      const idx = new WorkspaceIndexer(root);

      const first = await idx.buildIndex();
      assert.equal(first.cached, false, 'first build is never cached');
      assert.equal(first.discovery, 'glob');

      const spy = mock.method(idx, 'indexFile');
      const second = await idx.buildIndex();
      assert.equal(second.cached, true, 'unchanged file set must be cached');
      assert.equal(spy.mock.callCount(), 0, 'cached build must not re-parse any file');
      assert.equal(second.stages.parseMs, 0);
      spy.mock.restore();
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it('glob path: adding a file invalidates the cache and re-parses', async () => {
    const root = mkTmp();
    try {
      write(root, 'package.json', '{}');
      write(root, 'a.ts', 'export function foo() {}\n');
      const idx = new WorkspaceIndexer(root);
      await idx.buildIndex();

      write(root, 'b.ts', 'export class Bar {}\n');
      const after = await idx.buildIndex();
      assert.equal(after.cached, false, 'a changed file set must rebuild');
      assert.ok(idx.searchSymbols('bar').some(s => s.name === 'Bar'), 'new symbol must be indexed');
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it('git path: an unchanged .git/index is cached without re-parsing', async () => {
    const root = mkTmp();
    try {
      execFileSync('git', ['init', '-q'], { cwd: root });
      execFileSync('git', ['config', 'user.email', 'u@e.org'], { cwd: root });
      execFileSync('git', ['config', 'user.name', 'u'], { cwd: root });
      write(root, 'a.ts', 'export function foo() {}\n');
      execFileSync('git', ['add', 'a.ts'], { cwd: root });
      execFileSync('git', ['commit', '-qm', 'init'], { cwd: root });

      const idx = new WorkspaceIndexer(root);
      const first = await idx.buildIndex();
      assert.equal(first.discovery, 'git');
      assert.equal(first.cached, false);

      const spy = mock.method(idx, 'indexFile');
      const second = await idx.buildIndex();
      assert.equal(second.cached, true, 'unchanged .git/index must be cached');
      assert.equal(spy.mock.callCount(), 0, 'cached git build must not re-parse');
      spy.mock.restore();
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

// ===========================================================================
// Gap 5 — stage-level diagnostics
// ===========================================================================

describe('U109 gap5 — buildIndex stage diagnostics', () => {
  it('reports discovery path, per-stage timings and cap flag', async () => {
    const root = mkTmp();
    try {
      write(root, 'package.json', '{}');
      write(root, 'a.ts', 'export function foo() {}\n');
      const idx = new WorkspaceIndexer(root);
      const r = await idx.buildIndex();
      assert.equal(r.discovery, 'glob');
      assert.equal(r.maxFilesCapped, false);
      assert.equal(typeof r.stages.discoveryMs, 'number');
      assert.equal(typeof r.stages.parseMs, 'number');
      assert.ok(r.stages.parseMs >= 0 && r.stages.discoveryMs >= 0);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it('reports discovery="none" when there is no project root', async () => {
    const root = mkTmp();
    try {
      // No project marker → findProjectRoot returns null → skipped build.
      const idx = new WorkspaceIndexer(path.join(root, 'nested', 'deep'));
      fs.mkdirSync(path.join(root, 'nested', 'deep'), { recursive: true });
      const r = await idx.buildIndex();
      assert.equal(r.discovery, 'none');
      assert.equal(r.skippedReason, 'no project root found');
      assert.equal(r.cached, false);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
