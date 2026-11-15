/**
 * Additive parity hardening for lsp/diagnostic-tracking.ts.
 *
 * Each test targets ONE correctness gap and is red-green validated: it FAILS
 * when the corresponding fix is reverted and PASSES with it applied.
 *
 *  A. getNewDiagnostics path-mismatch guard — a wrong-file response must not be
 *     reported as a delta nor poison the baseline (beforeFileEdited already
 *     guards this; the post-edit path must too).
 *  B. formatNewDiagnosticsSummary must not count files with zero diagnostics.
 *  C. dedupeDiagnostics collapses structurally-identical diagnostics; the delta
 *     from getNewDiagnostics is deduped.
 *  D. convertPublishDiagnostics must not throw on a diagnostic with a missing
 *     range (defaults to a zero range).
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  DiagnosticTracker,
  dedupeDiagnostics,
  convertPublishDiagnostics,
  formatNewDiagnosticsSummary,
  normalizeDiagnosticPath,
  type Diagnostic,
  type DiagnosticFile,
  type DiagnosticSource,
  type PublishDiagnosticsParams,
} from '../src/lsp/diagnostic-tracking';

function diag(message: string, line = 0, severity: Diagnostic['severity'] = 'Error'): Diagnostic {
  return { message, severity, range: { start: { line, character: 0 }, end: { line, character: 1 } } };
}

class FakeSource implements DiagnosticSource {
  constructor(public responses: Map<string, DiagnosticFile | null | 'throw'>) {}
  async getDiagnostics(filePath: string): Promise<DiagnosticFile | null> {
    const key = normalizeDiagnosticPath(filePath);
    const r = this.responses.get(key);
    if (r === 'throw') throw new Error('unsupported');
    return r ?? null;
  }
}

describe('parity A — getNewDiagnostics path-mismatch guard', () => {
  const file = '/proj/src/a.ts';
  const key = normalizeDiagnosticPath(file);

  it('a wrong-file post-edit response is not a delta and does not poison the baseline', async () => {
    const baseDiag = diag('pre-existing');
    const source = new FakeSource(new Map([[key, { uri: `file://${file}`, diagnostics: [baseDiag] }]]));
    const tracker = new DiagnosticTracker(source);
    await tracker.beforeFileEdited(file);

    // Source now returns diagnostics for a DIFFERENT file.
    source.responses.set(key, { uri: 'file:///proj/src/OTHER.ts', diagnostics: [diag('foreign', 7)] });

    // Old (reverted) behavior: the foreign diagnostic is reported as "new"
    // (length 1) and rolled into the baseline. Guard makes the delta empty.
    const delta = await tracker.getNewDiagnostics([file]);
    assert.equal(delta.length, 0);

    // Baseline must be untouched: a matching response that reintroduces only the
    // original diagnostic yields no delta (proves [baseDiag] is still baselined,
    // not the poisoned foreign set).
    source.responses.set(key, { uri: `file://${file}`, diagnostics: [baseDiag] });
    const delta2 = await tracker.getNewDiagnostics([file]);
    assert.equal(delta2.length, 0);
  });
});

describe('parity B — summary ignores zero-diagnostic files in the file count', () => {
  it('counts only files that carry a diagnostic', () => {
    const files: DiagnosticFile[] = [
      { uri: 'file:///a.ts', diagnostics: [diag('a')] },
      { uri: 'file:///b.ts', diagnostics: [] }, // cleared file left in the list
    ];
    // Old (reverted): "Found 1 new diagnostic issue in 2 files".
    assert.equal(formatNewDiagnosticsSummary(files), 'Found 1 new diagnostic issue in 1 file');
  });

  it('still counts multiple real files correctly', () => {
    const files: DiagnosticFile[] = [
      { uri: 'file:///a.ts', diagnostics: [diag('a'), diag('b', 1)] },
      { uri: 'file:///b.ts', diagnostics: [diag('c')] },
      { uri: 'file:///c.ts', diagnostics: [] },
    ];
    assert.equal(formatNewDiagnosticsSummary(files), 'Found 3 new diagnostic issues in 2 files');
  });
});

describe('parity C — dedupeDiagnostics', () => {
  it('collapses structurally-identical diagnostics, preserving first-seen order', () => {
    const a = diag('dup', 1);
    const aClone = diag('dup', 1);
    const b = diag('other', 2);
    const out = dedupeDiagnostics([a, aClone, b, aClone]);
    assert.equal(out.length, 2);
    assert.equal(out[0].message, 'dup');
    assert.equal(out[1].message, 'other');
  });

  it('keeps diagnostics that differ in any field', () => {
    const out = dedupeDiagnostics([
      diag('m', 1, 'Error'),
      diag('m', 1, 'Warning'), // differs in severity
      diag('m', 2, 'Error'), // differs in range
    ]);
    assert.equal(out.length, 3);
  });

  it('getNewDiagnostics dedups a server that publishes a new diagnostic twice', async () => {
    const file = '/proj/src/dup.ts';
    const key = normalizeDiagnosticPath(file);
    const source = new FakeSource(new Map([[key, { uri: `file://${file}`, diagnostics: [] }]]));
    const tracker = new DiagnosticTracker(source);
    await tracker.beforeFileEdited(file);

    // Edit introduces the SAME diagnostic twice.
    source.responses.set(key, {
      uri: `file://${file}`,
      diagnostics: [diag('new issue', 3), diag('new issue', 3)],
    });
    const delta = await tracker.getNewDiagnostics([file]);
    assert.equal(delta.length, 1);
    // Old (reverted): 2 duplicate entries. Deduped: 1.
    assert.equal(delta[0].diagnostics.length, 1);
  });
});

describe('parity D — convertPublishDiagnostics tolerates a missing range', () => {
  it('does not throw and defaults to a zero range', () => {
    const params = {
      uri: 'file:///x.ts',
      diagnostics: [
        { message: 'no-range', severity: 1 }, // range omitted entirely
        { message: 'partial', severity: 2, range: { start: { line: 5 } } }, // partial range
      ],
    } as unknown as PublishDiagnosticsParams;

    let out: Diagnostic[] = [];
    // Old (reverted): reading d.range.start.line threw → whole batch lost.
    assert.doesNotThrow(() => { out = convertPublishDiagnostics(params); });
    assert.equal(out.length, 2);
    assert.deepEqual(out[0].range, { start: { line: 0, character: 0 }, end: { line: 0, character: 0 } });
    assert.equal(out[1].range.start.line, 5);
    assert.equal(out[1].range.start.character, 0);
    assert.equal(out[1].range.end.line, 0);
    // A valid diagnostic after a malformed one is preserved, not dropped.
    assert.equal(out[1].message, 'partial');
  });
});
