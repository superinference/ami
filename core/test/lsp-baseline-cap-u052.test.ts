/**
 * U052 — Bounded-memory retention for the LSP diagnostic baseline tracker.
 *
 * PARITY (additive, resource-exhaustion boundary): the reference bounds its
 * cross-session diagnostic bookkeeping with `LRUCache({ max: MAX_DELIVERED_FILES
 * = 500 })` (services/lsp/LSPDiagnosticRegistry.ts L46-56) to "prevent unbounded
 * memory growth in long sessions". AMI's `DiagnosticTracker` keeps a per-file
 * baseline (array of Diagnostic objects) keyed by normalized path and, before
 * this change, only ever grew — a long session editing thousands of distinct
 * files retained every baseline until an explicit `reset()`. These tests lock in
 * the LRU cap + least-recently-used eviction while proving the delta semantics of
 * the files that remain tracked are unchanged.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  DiagnosticTracker,
  MAX_TRACKED_BASELINE_FILES,
  type DiagnosticFile,
  type DiagnosticSource,
  type Diagnostic,
} from '../src/lsp/diagnostic-tracking';

function diag(message: string): Diagnostic {
  return {
    message,
    severity: 'Error',
    range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } },
  };
}

/**
 * Fake source that returns a file whose `uri` equals the requested path (so the
 * tracker's path-mismatch guard passes) with a configurable per-path diagnostic
 * set. A path mapped to `null` yields no diagnostics (empty-baseline path).
 */
function fakeSource(map: Record<string, Diagnostic[] | null> = {}): DiagnosticSource {
  return {
    async getDiagnostics(filePath: string): Promise<DiagnosticFile | null> {
      const d = map[filePath];
      if (d === null) return null;
      return { uri: filePath, diagnostics: d ?? [diag(`issue in ${filePath}`)] };
    },
  };
}

describe('MAX_TRACKED_BASELINE_FILES', () => {
  it('matches the reference MAX_DELIVERED_FILES (500)', () => {
    assert.equal(MAX_TRACKED_BASELINE_FILES, 500);
  });
});

describe('DiagnosticTracker bounded retention (LRU)', () => {
  it('evicts the least-recently-used baseline once the cap is exceeded', async () => {
    const tracker = new DiagnosticTracker(fakeSource(), 2);
    await tracker.beforeFileEdited('/tmp/a.ts');
    await tracker.beforeFileEdited('/tmp/b.ts');
    assert.equal(tracker.trackedFileCount, 2);
    assert.ok(tracker.hasBaseline('/tmp/a.ts'));
    assert.ok(tracker.hasBaseline('/tmp/b.ts'));

    // Third distinct file exceeds the cap of 2 → the LRU (a) is evicted.
    await tracker.beforeFileEdited('/tmp/c.ts');
    assert.equal(tracker.trackedFileCount, 2);
    assert.equal(tracker.hasBaseline('/tmp/a.ts'), false);
    assert.ok(tracker.hasBaseline('/tmp/b.ts'));
    assert.ok(tracker.hasBaseline('/tmp/c.ts'));
  });

  it('refreshes recency on re-store so a recently touched file survives eviction', async () => {
    const tracker = new DiagnosticTracker(fakeSource(), 2);
    await tracker.beforeFileEdited('/tmp/a.ts');
    await tracker.beforeFileEdited('/tmp/b.ts');
    // Re-touch `a` → it becomes most-recently-used, making `b` the LRU.
    await tracker.beforeFileEdited('/tmp/a.ts');
    await tracker.beforeFileEdited('/tmp/c.ts');
    assert.equal(tracker.trackedFileCount, 2);
    assert.ok(tracker.hasBaseline('/tmp/a.ts'), 'recently re-touched file survives');
    assert.equal(tracker.hasBaseline('/tmp/b.ts'), false, 'stale file is evicted');
    assert.ok(tracker.hasBaseline('/tmp/c.ts'));
  });

  it('the roll-forward in getNewDiagnostics refreshes recency and stays bounded', async () => {
    // Stateful source: empty at baseline-capture time, a new diagnostic after the
    // edit — so getNewDiagnostics reports a genuine delta AND rolls the baseline
    // forward (which must also refresh LRU recency).
    const current: Record<string, Diagnostic[]> = { '/tmp/a.ts': [], '/tmp/b.ts': [] };
    const src: DiagnosticSource = {
      async getDiagnostics(fp: string): Promise<DiagnosticFile | null> {
        return { uri: fp, diagnostics: current[fp] ?? [] };
      },
    };
    const tracker = new DiagnosticTracker(src, 2);
    await tracker.beforeFileEdited('/tmp/a.ts'); // baseline: []
    await tracker.beforeFileEdited('/tmp/b.ts'); // baseline: []
    current['/tmp/a.ts'] = [diag('a-new')];
    // Reading `a` surfaces the delta AND rolls its baseline forward → `a` becomes
    // MRU, `b` the LRU.
    const fresh = await tracker.getNewDiagnostics(['/tmp/a.ts']);
    assert.equal(fresh.length, 1);
    assert.equal(fresh[0].diagnostics[0].message, 'a-new');

    await tracker.beforeFileEdited('/tmp/c.ts');
    assert.equal(tracker.trackedFileCount, 2);
    assert.ok(tracker.hasBaseline('/tmp/a.ts'));
    assert.equal(tracker.hasBaseline('/tmp/b.ts'), false);
    assert.ok(tracker.hasBaseline('/tmp/c.ts'));
  });

  it('does not evict while within the cap (no behavior change for normal sessions)', async () => {
    const tracker = new DiagnosticTracker(fakeSource(), 500);
    for (let i = 0; i < 50; i++) await tracker.beforeFileEdited(`/tmp/f${i}.ts`);
    assert.equal(tracker.trackedFileCount, 50);
    for (let i = 0; i < 50; i++) assert.ok(tracker.hasBaseline(`/tmp/f${i}.ts`));
  });

  it('clamps a non-positive / non-finite cap to the production default', async () => {
    for (const bad of [0, -5, NaN, Infinity]) {
      const tracker = new DiagnosticTracker(fakeSource(), bad);
      // The clamp keeps the bound functional: adding a couple files must not
      // evict (default cap is 500) and must not wedge the eviction loop.
      await tracker.beforeFileEdited('/tmp/a.ts');
      await tracker.beforeFileEdited('/tmp/b.ts');
      assert.equal(tracker.trackedFileCount, 2, `cap ${bad} should clamp to default`);
    }
  });

  it('reset clears all tracked baselines', async () => {
    const tracker = new DiagnosticTracker(fakeSource(), 10);
    await tracker.beforeFileEdited('/tmp/a.ts');
    await tracker.beforeFileEdited('/tmp/b.ts');
    assert.equal(tracker.trackedFileCount, 2);
    tracker.reset();
    assert.equal(tracker.trackedFileCount, 0);
    assert.equal(tracker.hasBaseline('/tmp/a.ts'), false);
  });

  it('default construction uses the production cap (1 arg still works)', () => {
    const tracker = new DiagnosticTracker(fakeSource());
    assert.equal(tracker.trackedFileCount, 0);
  });
});
