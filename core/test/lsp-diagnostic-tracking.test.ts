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
  diagnosticHeaderPath,
  formatNewDiagnosticsSummary,
  formatDiagnosticsDetail,
  capDiagnosticVolume,
  isUncPath,
  lspFileSizeBytes,
  validateLspFilePath,
  MAX_DIAGNOSTICS_PER_FILE,
  MAX_TOTAL_DIAGNOSTICS,
  MAX_DIAGNOSTICS_SUMMARY_CHARS,
  type Diagnostic,
  type DiagnosticFile,
  type DiagnosticSource,
} from '../src/lsp/diagnostic-tracking';
import * as fs from 'node:fs';
import * as os from 'node:os';

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

  it('uriToPath keeps a malformed percent-escape raw (decode catch branch)', () => {
    // A lone '%' is an invalid escape → decodeURIComponent throws → keep raw.
    assert.equal(uriToPath('file:///tmp/100%.ts'), '/tmp/100%.ts');
  });

  it('uriToPath + normalizeDiagnosticPath handle Windows drive URIs', () => {
    const orig = Object.getOwnPropertyDescriptor(process, 'platform')!;
    Object.defineProperty(process, 'platform', { value: 'win32', configurable: true });
    try {
      // file:///C:/… → drop the leading slash, and the key is lowercased.
      assert.equal(uriToPath('file:///C:/Users/x.ts'), 'C:/Users/x.ts');
      assert.equal(normalizeDiagnosticPath('file:///C:/Users/X.TS'), normalizeDiagnosticPath('c:/users/x.ts'));
    } finally {
      Object.defineProperty(process, 'platform', orig);
    }
  });

  it('diagnosticSeveritySymbol falls back for an out-of-enum severity', () => {
    assert.equal(diagnosticSeveritySymbol('Error'), '✖');
    assert.equal(diagnosticSeveritySymbol('Bogus' as Diagnostic['severity']), '•');
  });

  it('convertPublishDiagnostics maps null/undefined code to undefined', () => {
    const out = convertPublishDiagnostics({
      uri: 'file:///x.ts',
      diagnostics: [
        { message: 'a', range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } }, code: null as unknown as undefined },
        { message: 'b', range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } } },
      ],
    });
    assert.equal(out[0].code, undefined);
    assert.equal(out[1].code, undefined);
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

  it('diagnosticHeaderPath guards a missing/empty uri instead of throwing', () => {
    assert.equal(diagnosticHeaderPath(undefined), '<unknown location>');
    assert.equal(diagnosticHeaderPath(''), '<unknown location>');
    assert.equal(diagnosticHeaderPath('file:///proj/a.ts'), '/proj/a.ts');
  });

  it('detail renders a malformed file (undefined uri) as <unknown location> and still shows other files', () => {
    const files = [
      // A malformed DiagnosticFile whose uri is missing (bad publishDiagnostics
      // payload). The old code called uriToPath(undefined) which threw.
      { uri: undefined as unknown as string, diagnostics: [diag('orphan')] },
      { uri: 'file:///proj/ok.ts', diagnostics: [diag('real')] },
    ];
    let out = '';
    assert.doesNotThrow(() => { out = formatDiagnosticsDetail(files); });
    assert.ok(out.includes('<unknown location>'));
    assert.ok(out.includes('orphan'));
    // The valid file after the malformed one is still rendered (not dropped).
    assert.ok(out.includes('/proj/ok.ts'));
    assert.ok(out.includes('real'));
  });
});

describe('path + size safety helpers', () => {
  it('isUncPath detects backslash and forward-slash UNC prefixes', () => {
    assert.equal(isUncPath('\\\\server\\share\\f'), true);
    assert.equal(isUncPath('//server/share/f'), true);
    assert.equal(isUncPath('/home/user/f.ts'), false);
    assert.equal(isUncPath('C:/x'), false);
  });

  it('lspFileSizeBytes returns a size for a real file and undefined for a missing one', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lsp-size-'));
    try {
      const f = path.join(dir, 'a.ts');
      fs.writeFileSync(f, 'hello');
      assert.equal(lspFileSizeBytes(f), 5);
      assert.equal(lspFileSizeBytes(path.join(dir, 'missing.ts')), undefined);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('validateLspFilePath classifies existence / type / access with distinct error codes', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lsp-validate-'));
    try {
      const f = path.join(dir, 'a.ts');
      fs.writeFileSync(f, 'hello');

      // Regular file → ok, carries the stat'd size (reused for the size cap).
      const ok = validateLspFilePath(f);
      assert.equal(ok.ok, true);
      if (ok.ok) assert.equal(ok.sizeBytes, 5);

      // Missing path → ENOENT, errorCode 1.
      const missing = validateLspFilePath(path.join(dir, 'missing.ts'));
      assert.equal(missing.ok, false);
      if (!missing.ok) {
        assert.equal(missing.errorCode, 1);
        assert.match(missing.message, /File does not exist/);
      }

      // Directory → exists but not a regular file, errorCode 2.
      const asDir = validateLspFilePath(dir);
      assert.equal(asDir.ok, false);
      if (!asDir.ok) {
        assert.equal(asDir.errorCode, 2);
        assert.match(asDir.message, /Path is not a file/);
      }

      // Non-ENOENT stat failure (name too long → ENAMETOOLONG), errorCode 4.
      const tooLong = validateLspFilePath(path.join(dir, 'x'.repeat(5000)));
      assert.equal(tooLong.ok, false);
      if (!tooLong.ok) {
        assert.equal(tooLong.errorCode, 4);
        assert.match(tooLong.message, /Cannot access file/);
      }
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe('capDiagnosticVolume', () => {
  const mk = (uri: string, n: number, severity: Diagnostic['severity'] = 'Error'): DiagnosticFile => ({
    uri,
    diagnostics: Array.from({ length: n }, (_v, i) => diag(`m${i}`, i, severity)),
  });

  it('caps each file to MAX_DIAGNOSTICS_PER_FILE', () => {
    const out = capDiagnosticVolume([mk('file:///a.ts', MAX_DIAGNOSTICS_PER_FILE + 5)]);
    assert.equal(out[0].diagnostics.length, MAX_DIAGNOSTICS_PER_FILE);
  });

  it('caps the running total to MAX_TOTAL_DIAGNOSTICS and drops emptied files', () => {
    const files = Array.from({ length: 6 }, (_v, i) => mk(`file:///f${i}.ts`, MAX_DIAGNOSTICS_PER_FILE));
    const out = capDiagnosticVolume(files);
    const total = out.reduce((s, f) => s + f.diagnostics.length, 0);
    assert.equal(total, MAX_TOTAL_DIAGNOSTICS); // 3 files * 10 = 30; files 4-6 dropped
    assert.equal(out.length, 3);
  });

  it('sorts severity-first so errors survive truncation over hints', () => {
    const file: DiagnosticFile = {
      uri: 'file:///a.ts',
      diagnostics: [
        ...Array.from({ length: MAX_DIAGNOSTICS_PER_FILE }, (_v, i) => diag(`hint${i}`, i, 'Hint')),
        diag('the-error', 99, 'Error'),
      ],
    };
    const out = capDiagnosticVolume([file]);
    assert.equal(out[0].diagnostics.length, MAX_DIAGNOSTICS_PER_FILE);
    assert.ok(out[0].diagnostics.some((d) => d.message === 'the-error'));
  });

  it('does not mutate its input', () => {
    const input = [mk('file:///a.ts', MAX_DIAGNOSTICS_PER_FILE + 2)];
    capDiagnosticVolume(input);
    assert.equal(input[0].diagnostics.length, MAX_DIAGNOSTICS_PER_FILE + 2);
  });
});

describe('formatDiagnosticsDetail truncation', () => {
  it('caps the rendered block at MAX_DIAGNOSTICS_SUMMARY_CHARS with a marker', () => {
    const file: DiagnosticFile = {
      uri: 'file:///a.ts',
      diagnostics: Array.from({ length: 200 }, (_v, i) => diag('x'.repeat(100), i)),
    };
    const out = formatDiagnosticsDetail([file]);
    assert.ok(out.length <= MAX_DIAGNOSTICS_SUMMARY_CHARS);
    assert.ok(out.endsWith('…[truncated]'));
  });
});
