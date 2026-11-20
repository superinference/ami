/**
 * U-engine1 (gap: per-query-loop reset of tracking state + last-processed
 * timestamp cache).
 *
 * PARITY (additive): the reference's diagnosticTracking keeps a
 * `lastProcessedTimestamps: Map<path, number>` recording when each file was last
 * snapshotted, and `handleQueryStart` reset()s every tracking map at the start of
 * each new query loop so stale diagnostics from a prior turn don't leak into the
 * next turn's "new" set (diagnosticTracking.ts:L321-343). AMI's
 * {@link DiagnosticTracker} previously tracked only the per-file baseline with no
 * processed-timestamp cache. These tests lock in:
 *   - a last-processed timestamp is recorded in lock-step with the baseline
 *     (capture + roll-forward), using an injectable clock;
 *   - the timestamp cache follows LRU eviction (never outgrows the baseline);
 *   - reset() clears the timestamp cache alongside the baseline.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  DiagnosticTracker,
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

function fakeSource(map: Record<string, Diagnostic[] | null> = {}): DiagnosticSource {
  return {
    async getDiagnostics(filePath: string): Promise<DiagnosticFile | null> {
      const d = map[filePath];
      if (d === null) return null;
      return { uri: filePath, diagnostics: d ?? [diag(`issue in ${filePath}`)] };
    },
  };
}

/** A deterministic, monotonically-advancing injectable clock. */
function fakeClock(start = 1000, step = 10): () => number {
  let t = start - step;
  return () => (t += step);
}

describe('DiagnosticTracker last-processed-timestamp cache', () => {
  it('records a timestamp when a baseline is captured (beforeFileEdited)', async () => {
    const tracker = new DiagnosticTracker(fakeSource(), 10, fakeClock(1000, 10));
    assert.equal(tracker.getLastProcessedTimestamp('/tmp/a.ts'), undefined);
    await tracker.beforeFileEdited('/tmp/a.ts');
    assert.equal(tracker.getLastProcessedTimestamp('/tmp/a.ts'), 1000);
    await tracker.beforeFileEdited('/tmp/b.ts');
    assert.equal(tracker.getLastProcessedTimestamp('/tmp/b.ts'), 1010);
  });

  it('refreshes the timestamp when the baseline is rolled forward (getNewDiagnostics)', async () => {
    const current: Record<string, Diagnostic[]> = { '/tmp/a.ts': [] };
    const src: DiagnosticSource = {
      async getDiagnostics(fp: string): Promise<DiagnosticFile | null> {
        return { uri: fp, diagnostics: current[fp] ?? [] };
      },
    };
    const tracker = new DiagnosticTracker(src, 10, fakeClock(1000, 10));
    await tracker.beforeFileEdited('/tmp/a.ts'); // t=1000
    assert.equal(tracker.getLastProcessedTimestamp('/tmp/a.ts'), 1000);
    current['/tmp/a.ts'] = [diag('a-new')];
    await tracker.getNewDiagnostics(['/tmp/a.ts']); // roll forward → t=1010
    assert.equal(tracker.getLastProcessedTimestamp('/tmp/a.ts'), 1010);
  });

  it('normalizes the path for timestamp lookups (file:// vs plain path)', async () => {
    const tracker = new DiagnosticTracker(fakeSource(), 10, fakeClock(5000, 1));
    await tracker.beforeFileEdited('/tmp/a.ts');
    // Lookup via a file:// URI resolves to the same normalized key.
    assert.equal(tracker.getLastProcessedTimestamp('file:///tmp/a.ts'), 5000);
  });

  it('drops the timestamp when its baseline is LRU-evicted (never outgrows the baseline)', async () => {
    const tracker = new DiagnosticTracker(fakeSource(), 2, fakeClock(1, 1));
    await tracker.beforeFileEdited('/tmp/a.ts');
    await tracker.beforeFileEdited('/tmp/b.ts');
    assert.notEqual(tracker.getLastProcessedTimestamp('/tmp/a.ts'), undefined);
    // Third file evicts the LRU (a) — its timestamp must be dropped too.
    await tracker.beforeFileEdited('/tmp/c.ts');
    assert.equal(tracker.hasBaseline('/tmp/a.ts'), false);
    assert.equal(tracker.getLastProcessedTimestamp('/tmp/a.ts'), undefined,
      'evicted file must not leak a stale timestamp');
    assert.notEqual(tracker.getLastProcessedTimestamp('/tmp/b.ts'), undefined);
    assert.notEqual(tracker.getLastProcessedTimestamp('/tmp/c.ts'), undefined);
  });

  it('reset() clears the timestamp cache alongside the baseline (per-query-loop reset)', async () => {
    const tracker = new DiagnosticTracker(fakeSource(), 10, fakeClock());
    await tracker.beforeFileEdited('/tmp/a.ts');
    await tracker.beforeFileEdited('/tmp/b.ts');
    assert.notEqual(tracker.getLastProcessedTimestamp('/tmp/a.ts'), undefined);
    tracker.reset();
    assert.equal(tracker.trackedFileCount, 0);
    assert.equal(tracker.getLastProcessedTimestamp('/tmp/a.ts'), undefined);
    assert.equal(tracker.getLastProcessedTimestamp('/tmp/b.ts'), undefined);
  });

  it('defaults to the real Date.now clock when none is injected (3-arg optional)', async () => {
    const before = Date.now();
    const tracker = new DiagnosticTracker(fakeSource());
    await tracker.beforeFileEdited('/tmp/a.ts');
    const ts = tracker.getLastProcessedTimestamp('/tmp/a.ts');
    assert.ok(ts !== undefined && ts >= before, 'default clock records a real epoch-ms timestamp');
  });
});
