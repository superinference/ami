import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as path from 'node:path';

import {
  DiagnosticTracker,
  mapLspSeverity,
  convertPublishDiagnostics,
  areDiagnosticsEqual,
  normalizeDiagnosticPath,
  diagnosticPathsEqual,
  uriToPath,
  diagnosticSeveritySymbol,
  formatNewDiagnosticsSummary,
  formatDiagnosticsDetail,
  type Diagnostic,
  type DiagnosticFile,
  type DiagnosticSource,
} from '../src/lsp/diagnostic-tracking';

function diag(message: string, line = 0, severity: Diagnostic['severity'] = 'Error'): Diagnostic {
  return { message, severity, range: { start: { line, character: 0 }, end: { line, character: 1 } } };
}

/** A programmable DiagnosticSource for tracker tests. */
class FakeSource implements DiagnosticSource {
  constructor(public responses: Map<string, DiagnosticFile | null | 'throw'>) {}
  async getDiagnostics(filePath: string): Promise<DiagnosticFile | null> {
    const key = normalizeDiagnosticPath(filePath);
    const r = this.responses.get(key);
    if (r === 'throw') throw new Error('unsupported');
    return r ?? null;
  }
}

describe('severity + conversion helpers', () => {
  it('maps LSP numeric severities', () => {
    assert.equal(mapLspSeverity(1), 'Error');
    assert.equal(mapLspSeverity(2), 'Warning');
    assert.equal(mapLspSeverity(3), 'Info');
    assert.equal(mapLspSeverity(4), 'Hint');
    assert.equal(mapLspSeverity(undefined), 'Error');
    assert.equal(mapLspSeverity(99), 'Error');
  });

  it('converts publishDiagnostics, coercing numeric code to string', () => {
    const out = convertPublishDiagnostics({
      uri: 'file:///x.ts',
      diagnostics: [
        { message: 'm', severity: 2, range: { start: { line: 1, character: 2 }, end: { line: 1, character: 3 } }, source: 'tsc', code: 2322 },
        { message: 'n', range: { start: { line: 0, character: 0 }, end: { line: 0, character: 0 } }, code: null as unknown as string },
      ],
    });
    assert.equal(out[0].severity, 'Warning');
    assert.equal(out[0].code, '2322');
    assert.equal(out[1].severity, 'Error');
    assert.equal(out[1].code, undefined);
  });

  it('handles a missing diagnostics array', () => {
    assert.deepEqual(convertPublishDiagnostics({ uri: 'file:///x', diagnostics: undefined as never }), []);
  });
});

describe('path helpers', () => {
  it('uriToPath strips file:// and decodes', () => {
    assert.equal(uriToPath('file:///tmp/a%20b.ts'), '/tmp/a b.ts');
    assert.equal(uriToPath('/plain/path.ts'), '/plain/path.ts');
  });

  it('normalize + equality treat uri and path the same', () => {
    assert.ok(diagnosticPathsEqual('file:///tmp/a.ts', '/tmp/a.ts'));
    assert.equal(normalizeDiagnosticPath('file:///tmp/a.ts'), path.resolve('/tmp/a.ts'));
  });

  it('areDiagnosticsEqual compares all fields', () => {
    assert.ok(areDiagnosticsEqual(diag('a'), diag('a')));
    assert.ok(!areDiagnosticsEqual(diag('a'), diag('b')));
    assert.ok(!areDiagnosticsEqual(diag('a', 0), diag('a', 5)));
  });
});

describe('DiagnosticTracker baseline + diff', () => {
  const file = '/proj/src/a.ts';
  const key = normalizeDiagnosticPath(file);

  it('reports only NEW diagnostics introduced after the edit, then rolls baseline forward', async () => {
    const existing = diag('pre-existing');
    const source = new FakeSource(new Map([[key, { uri: `file://${file}`, diagnostics: [existing] }]]));
    const tracker = new DiagnosticTracker(source);

    await tracker.beforeFileEdited(file);
    assert.ok(tracker.hasBaseline(file));

    // After the edit the file has the old one PLUS a new error.
    const fresh = diag('new error', 3);
    source.responses.set(key, { uri: `file://${file}`, diagnostics: [existing, fresh] });

    const delta = await tracker.getNewDiagnostics([file]);
    assert.equal(delta.length, 1);
    assert.equal(delta[0].diagnostics.length, 1);
    assert.equal(delta[0].diagnostics[0].message, 'new error');

    // Baseline rolled forward: a second call with no further change → no delta.
    const delta2 = await tracker.getNewDiagnostics([file]);
    assert.equal(delta2.length, 0);
  });

  it('path-mismatch guard: a wrong returned file does NOT poison the baseline', async () => {
    const source = new FakeSource(new Map([[key, { uri: 'file:///other/wrong.ts', diagnostics: [diag('x')] }]]));
    const tracker = new DiagnosticTracker(source);
    await tracker.beforeFileEdited(file);
    assert.ok(!tracker.hasBaseline(file));
  });

  it('empty-baseline fallback: no diagnostics returned still registers later issues as new', async () => {
    const source = new FakeSource(new Map([[key, null]]));
    const tracker = new DiagnosticTracker(source);
    await tracker.beforeFileEdited(file);
    assert.ok(tracker.hasBaseline(file));

    source.responses.set(key, { uri: `file://${file}`, diagnostics: [diag('introduced')] });
    const delta = await tracker.getNewDiagnostics([file]);
    assert.equal(delta.length, 1);
    assert.equal(delta[0].diagnostics[0].message, 'introduced');
  });

  it('beforeFileEdited swallows a source that throws (unsupported server)', async () => {
    const source = new FakeSource(new Map([[key, 'throw']]));
    const tracker = new DiagnosticTracker(source);
    await tracker.beforeFileEdited(file);
    assert.ok(!tracker.hasBaseline(file));
  });

  it('getNewDiagnostics ignores files without a baseline', async () => {
    const source = new FakeSource(new Map([[key, { uri: `file://${file}`, diagnostics: [diag('a')] }]]));
    const tracker = new DiagnosticTracker(source);
    const delta = await tracker.getNewDiagnostics([file]); // no baseline captured
    assert.equal(delta.length, 0);
  });

  it('getNewDiagnostics over all baselines + skips a throwing source', async () => {
    const source = new FakeSource(new Map([[key, { uri: `file://${file}`, diagnostics: [] }]]));
    const tracker = new DiagnosticTracker(source);
    await tracker.beforeFileEdited(file);
    source.responses.set(key, 'throw');
    const delta = await tracker.getNewDiagnostics();
    assert.equal(delta.length, 0);
  });

  it('reset clears baselines', async () => {
    const source = new FakeSource(new Map([[key, { uri: `file://${file}`, diagnostics: [] }]]));
    const tracker = new DiagnosticTracker(source);
    await tracker.beforeFileEdited(file);
    tracker.reset();
    assert.ok(!tracker.hasBaseline(file));
  });

  it('getNewDiagnostics falls back to a synthetic uri when source returns null post-baseline', async () => {
    const source = new FakeSource(new Map([[key, { uri: `file://${file}`, diagnostics: [diag('base')] }]]));
    const tracker = new DiagnosticTracker(source);
    await tracker.beforeFileEdited(file);
    source.responses.set(key, null); // current → none; nothing new
    const delta = await tracker.getNewDiagnostics([file]);
    assert.equal(delta.length, 0);
  });
});

describe('formatting', () => {
  it('summary uses correct singular/plural and omits when empty', () => {
    assert.equal(formatNewDiagnosticsSummary([]), '');
    const oneOne: DiagnosticFile[] = [{ uri: 'file:///a.ts', diagnostics: [diag('a')] }];
    assert.equal(formatNewDiagnosticsSummary(oneOne), 'Found 1 new diagnostic issue in 1 file');
    const many: DiagnosticFile[] = [
      { uri: 'file:///a.ts', diagnostics: [diag('a'), diag('b', 1)] },
      { uri: 'file:///b.ts', diagnostics: [diag('c')] },
    ];
    assert.equal(formatNewDiagnosticsSummary(many), 'Found 3 new diagnostic issues in 2 files');
  });

  it('detail renders severity symbol, 1-based position, code and source', () => {
    const files: DiagnosticFile[] = [{
      uri: 'file:///proj/a.ts',
      diagnostics: [{ message: 'bad', severity: 'Warning', source: 'tsc', code: 'E1', range: { start: { line: 4, character: 2 }, end: { line: 4, character: 9 } } }],
    }];
    const out = formatDiagnosticsDetail(files);
    assert.ok(out.includes('/proj/a.ts'));
    assert.ok(out.includes('[Line 5:3]'));
    assert.ok(out.includes('bad'));
    assert.ok(out.includes('[E1]'));
    assert.ok(out.includes('(tsc)'));
    assert.ok(out.includes(diagnosticSeveritySymbol('Warning')));
  });

  it('severity symbol covers all names', () => {
    for (const s of ['Error', 'Warning', 'Info', 'Hint'] as const) {
      assert.ok(diagnosticSeveritySymbol(s).length > 0);
    }
  });
});
