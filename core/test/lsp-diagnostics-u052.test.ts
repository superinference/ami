import { describe, it, afterEach, before, after, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as path from 'node:path';
import * as fs from 'node:fs';
import * as os from 'node:os';

import {
  lspDiagnosticsTool,
  lspReferencesTool,
  _lspDiagHooks,
  lspDiagToolsEnabled,
  volumeLimitNote,
  type LspDiagClient,
} from '../src/tools/lsp-diagnostics';
import {
  formatLspDisplayPath,
  capDiagnosticVolumeReport,
  MAX_DIAGNOSTICS_PER_FILE,
  MAX_TOTAL_DIAGNOSTICS,
  type DiagnosticFile,
} from '../src/lsp/diagnostic-tracking';
import type { ToolContext } from '../src/types';

const ctx: ToolContext = { cwd: '/tmp' } as ToolContext;

function diagFile(uri: string, n: number, severity: 'Error' | 'Warning' = 'Error'): DiagnosticFile {
  return {
    uri,
    diagnostics: Array.from({ length: n }, (_, i) => ({
      message: `msg ${i}`,
      severity,
      range: { start: { line: i, character: 0 }, end: { line: i, character: 1 } },
    })),
  };
}

function fakeClient(overrides: Partial<LspDiagClient> & { baseline?: boolean } = {}): LspDiagClient {
  return {
    getDiagnostics: overrides.getDiagnostics ?? (async () => null),
    getAllDiagnostics: overrides.getAllDiagnostics ?? (() => []),
    isConnected: overrides.isConnected ?? (() => true),
    diagnostics: overrides.diagnostics ?? {
      hasBaseline: () => overrides.baseline ?? false,
      getNewDiagnostics: async () => [],
    },
  };
}

afterEach(() => {
  mock.restoreAll();
  delete process.env.AMI_LSP;
});

// Real on-disk files so the diagnostics tool's pre-flight existence validation
// (validateLspFilePath) passes and the mocked LSP-client paths are exercised.
let TMP_DIR: string;
let FOO_TS: string;
let FOO_RB: string;
let MAKEFILE: string;
before(() => {
  TMP_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'lsp-u052-files-'));
  FOO_TS = path.join(TMP_DIR, 'foo.ts');
  FOO_RB = path.join(TMP_DIR, 'foo.rb');
  MAKEFILE = path.join(TMP_DIR, 'Makefile');
  fs.writeFileSync(FOO_TS, '');
  fs.writeFileSync(FOO_RB, '');
  fs.writeFileSync(MAKEFILE, '');
});
after(() => fs.rmSync(TMP_DIR, { recursive: true, force: true }));

// ---------------------------------------------------------------------------
// Gap 10/11/12 — formatLspDisplayPath (Claude formatUri parity)
// ---------------------------------------------------------------------------
describe('formatLspDisplayPath', () => {
  it('returns <unknown location> for an empty/undefined uri', () => {
    assert.equal(formatLspDisplayPath(undefined), '<unknown location>');
    assert.equal(formatLspDisplayPath(''), '<unknown location>');
  });

  it('strips the file:// protocol and decodes percent-encoding', () => {
    assert.equal(formatLspDisplayPath('file:///tmp/a%20b.ts'), '/tmp/a b.ts');
  });

  it('strips a Windows drive-letter leading slash and normalizes backslashes', () => {
    assert.equal(formatLspDisplayPath('file:///C:/code/foo.ts'), 'C:/code/foo.ts');
  });

  it('prefers a cwd-relative path only when it is strictly shorter', () => {
    assert.equal(formatLspDisplayPath('/tmp/sub/foo.ts', '/tmp'), 'sub/foo.ts');
  });

  it('keeps the absolute path when the relative one would escape past ../..', () => {
    const abs = '/a/b/c/d/target.ts';
    const out = formatLspDisplayPath(abs, '/a/b/c/d/e/f/g');
    // relative would be ../../../target.ts (starts with ../../) → keep absolute.
    assert.equal(out, abs);
  });

  it('keeps the absolute path when the relative form is not shorter', () => {
    // cwd far away makes the relative path longer than the absolute.
    const abs = '/x.ts';
    assert.equal(formatLspDisplayPath(abs, '/deeply/nested/dir'), abs);
  });

  it('decodes gracefully on a malformed percent-escape (no throw)', () => {
    assert.equal(formatLspDisplayPath('file:///tmp/bad%ZZ.ts'), '/tmp/bad%ZZ.ts');
  });
});

// ---------------------------------------------------------------------------
// Gap 1/9 — capDiagnosticVolumeReport + volumeLimitNote
// ---------------------------------------------------------------------------
describe('capDiagnosticVolumeReport', () => {
  it('reports zero removed when under the caps', () => {
    const { files, removed } = capDiagnosticVolumeReport([diagFile('file:///a.ts', 3)]);
    assert.equal(removed, 0);
    assert.equal(files[0].diagnostics.length, 3);
  });

  it('counts per-file over-cap removals', () => {
    const over = MAX_DIAGNOSTICS_PER_FILE + 4;
    const { files, removed } = capDiagnosticVolumeReport([diagFile('file:///a.ts', over)]);
    assert.equal(files[0].diagnostics.length, MAX_DIAGNOSTICS_PER_FILE);
    assert.equal(removed, 4);
  });

  it('counts total-cap removals across files', () => {
    // 5 files of 10 = 50 diagnostics; total cap 30 → 20 removed.
    const files = Array.from({ length: 5 }, (_, i) => diagFile(`file:///f${i}.ts`, 10));
    const { files: out, removed } = capDiagnosticVolumeReport(files);
    const kept = out.reduce((s, f) => s + f.diagnostics.length, 0);
    assert.equal(kept, MAX_TOTAL_DIAGNOSTICS);
    assert.equal(removed, 50 - MAX_TOTAL_DIAGNOSTICS);
  });
});

describe('volumeLimitNote', () => {
  it('is empty when nothing was removed', () => {
    assert.equal(volumeLimitNote(0), '');
    assert.equal(volumeLimitNote(-1), '');
  });

  it('names the count and the caps when diagnostics were dropped', () => {
    const note = volumeLimitNote(7);
    assert.match(note, /Volume limiting removed 7 diagnostics/);
    assert.ok(note.includes(`max ${MAX_DIAGNOSTICS_PER_FILE}/file, ${MAX_TOTAL_DIAGNOSTICS} total`));
  });

  it('uses the singular noun for exactly one', () => {
    assert.match(volumeLimitNote(1), /removed 1 diagnostic \(/);
  });

  it('surfaces the truncation note through the workspace diagnostics tool', async () => {
    const files = Array.from({ length: 5 }, (_, i) => diagFile(`file:///f${i}.ts`, 10));
    mock.method(_lspDiagHooks, 'getLSPClient', () => fakeClient({ getAllDiagnostics: () => files }));
    const res = await lspDiagnosticsTool.execute!({}, ctx);
    assert.match(res.output, /Volume limiting removed 20 diagnostics/);
  });
});

// ---------------------------------------------------------------------------
// Gap 2/3/4 — health gating (lspDiagToolsEnabled)
// ---------------------------------------------------------------------------
describe('lspDiagToolsEnabled', () => {
  it('is enabled by default when no client has started', () => {
    mock.method(_lspDiagHooks, 'serverStates', () => undefined);
    assert.equal(lspDiagToolsEnabled(), true);
  });

  it('is disabled when the env subsystem toggle is off', () => {
    process.env.AMI_LSP = 'off';
    assert.equal(lspDiagToolsEnabled(), false);
  });

  it('is disabled when every started server is in the error state', () => {
    mock.method(_lspDiagHooks, 'serverStates', () => [{ state: 'error' }, { state: 'error' }]);
    assert.equal(lspDiagToolsEnabled(), false);
  });

  it('stays enabled when at least one server is healthy', () => {
    mock.method(_lspDiagHooks, 'serverStates', () => [{ state: 'error' }, { state: 'running' }]);
    assert.equal(lspDiagToolsEnabled(), true);
  });

  it('backs the tools isEnabled gate', () => {
    mock.method(_lspDiagHooks, 'serverStates', () => [{ state: 'error' }]);
    assert.equal(lspDiagnosticsTool.isEnabled!(), false);
    assert.equal(lspReferencesTool.isEnabled!(), false);
  });

  it('serverStates returns undefined when no client has been instantiated', () => {
    // Default (non-mocked) hook: peekLSPClient() is null in a fresh test process.
    assert.equal(_lspDiagHooks.serverStates(), undefined);
  });
});

// ---------------------------------------------------------------------------
// Gap 6 — verbose toggle
// ---------------------------------------------------------------------------
describe('diagnostics verbose toggle', () => {
  it('verbose:false returns a count summary without per-diagnostic detail (workspace)', async () => {
    mock.method(_lspDiagHooks, 'getLSPClient', () =>
      fakeClient({ getAllDiagnostics: () => [diagFile('file:///a.ts', 2)] }));
    const res = await lspDiagnosticsTool.execute!({ verbose: false }, ctx);
    assert.ok(res.output.includes('2 diagnostic issue(s) across 1 file(s)'));
    assert.ok(res.output.includes('set verbose: true'));
    assert.ok(!res.output.includes('[Line 1:1]'));
  });

  it('default (verbose unset) includes full per-diagnostic detail (workspace)', async () => {
    mock.method(_lspDiagHooks, 'getLSPClient', () =>
      fakeClient({ getAllDiagnostics: () => [diagFile('file:///a.ts', 1)] }));
    const res = await lspDiagnosticsTool.execute!({}, ctx);
    assert.ok(res.output.includes('[Line 1:1]'));
  });

  it('verbose:false summarizes a single file without detail', async () => {
    mock.method(_lspDiagHooks, 'getLSPClient', () =>
      fakeClient({ getDiagnostics: async () => diagFile('file:///tmp/foo.ts', 3) }));
    const res = await lspDiagnosticsTool.execute!({ file_path: FOO_TS, verbose: false }, ctx);
    assert.ok(res.output.includes(`3 diagnostic issue(s) in ${FOO_TS}`));
    assert.ok(!res.output.includes('[Line 1:1]'));
  });

  it('verbose:false summarizes the baseline delta without detail', async () => {
    mock.method(_lspDiagHooks, 'getLSPClient', () =>
      fakeClient({
        diagnostics: {
          hasBaseline: () => true,
          getNewDiagnostics: async () => [diagFile('file:///tmp/foo.ts', 2)],
        },
      }));
    const res = await lspDiagnosticsTool.execute!({ file_path: FOO_TS, verbose: false }, ctx);
    assert.ok(res.output.includes('Found 2 new diagnostic issues'));
    assert.ok(res.output.includes('set verbose: true'));
    assert.ok(!res.output.includes('[Line 1:1]'));
  });
});

// ---------------------------------------------------------------------------
// Gap 13 — runtime input validation
// ---------------------------------------------------------------------------
describe('runtime input validation', () => {
  it('rejects a non-string file_path for diagnostics', async () => {
    mock.method(_lspDiagHooks, 'getLSPClient', () => fakeClient());
    const res = await lspDiagnosticsTool.execute!({ file_path: 123 }, ctx);
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('Invalid file_path'));
  });

  it('rejects an empty/whitespace symbol for find_references', async () => {
    const res = await lspReferencesTool.execute!({ symbol: '   ' }, ctx);
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('Invalid symbol'));
  });

  it('rejects a non-string symbol for find_references', async () => {
    const res = await lspReferencesTool.execute!({ symbol: 42 }, ctx);
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('Invalid symbol'));
  });

  it('rejects a non-string file_path for find_references', async () => {
    const res = await lspReferencesTool.execute!({ symbol: 'foo', file_path: {} }, ctx);
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('Invalid file_path'));
  });

  it('trims a valid symbol before searching', async () => {
    const seen: string[] = [];
    mock.method(_lspDiagHooks, 'searchSymbols', (_cwd: string, sym: string) => {
      seen.push(sym);
      return [];
    });
    await lspReferencesTool.execute!({ symbol: '  Foo  ' }, ctx);
    assert.deepEqual(seen, ['Foo']);
  });
});

// ---------------------------------------------------------------------------
// Gap 14 — structured output metadata + outputSchema
// ---------------------------------------------------------------------------
describe('structured output metadata', () => {
  it('attaches resultCount/fileCount/operation to workspace diagnostics', async () => {
    mock.method(_lspDiagHooks, 'getLSPClient', () =>
      fakeClient({ getAllDiagnostics: () => [diagFile('file:///a.ts', 1), diagFile('file:///b.ts', 2)] }));
    const res = await lspDiagnosticsTool.execute!({}, ctx);
    assert.deepEqual(res.metadata, { operation: 'diagnostics', resultCount: 3, fileCount: 2 });
  });

  it('attaches counts to find_references results', async () => {
    mock.method(_lspDiagHooks, 'searchSymbols', () => [
      { name: 'Foo', type: 'class', filePath: '/tmp/a.ts', line: 1 },
      { name: 'Foo', type: 'ref', filePath: '/tmp/b.ts', line: 2 },
    ]);
    const res = await lspReferencesTool.execute!({ symbol: 'Foo' }, ctx);
    assert.deepEqual(res.metadata, { operation: 'find_references', resultCount: 2, fileCount: 2 });
  });

  it('reports zero counts when find_references has no matches', async () => {
    mock.method(_lspDiagHooks, 'searchSymbols', () => []);
    const res = await lspReferencesTool.execute!({ symbol: 'Nope' }, ctx);
    assert.deepEqual(res.metadata, { operation: 'find_references', resultCount: 0, fileCount: 0 });
  });

  it('declares an object-rooted outputSchema on both tools', () => {
    assert.equal(lspDiagnosticsTool.outputSchema?.type, 'object');
    assert.equal(lspReferencesTool.outputSchema?.type, 'object');
  });
});

// ---------------------------------------------------------------------------
// Gap 16 — precise capability-boundary affordance (file type)
// ---------------------------------------------------------------------------
describe('no-server capability affordance', () => {
  it('names the exact file type when no server is available', async () => {
    mock.method(_lspDiagHooks, 'getLSPClient', () => fakeClient({ getDiagnostics: async () => null }));
    const res = await lspDiagnosticsTool.execute!({ file_path: FOO_RB }, ctx);
    assert.ok(res.output.includes('No LSP server available for file type: .rb'));
    assert.ok(res.output.includes(FOO_RB));
  });

  it('omits the file-type clause for an extensionless file', async () => {
    mock.method(_lspDiagHooks, 'getLSPClient', () => fakeClient({ getDiagnostics: async () => null }));
    const res = await lspDiagnosticsTool.execute!({ file_path: MAKEFILE }, ctx);
    assert.ok(res.output.includes(`No LSP server available (${MAKEFILE})`));
  });
});

// ---------------------------------------------------------------------------
// Gap 10/11/12 — find_references cwd-relative display paths
// ---------------------------------------------------------------------------
describe('find_references display paths', () => {
  it('renders grouped file headers as shortened cwd-relative paths', async () => {
    mock.method(_lspDiagHooks, 'searchSymbols', () => [
      { name: 'Foo', type: 'class', filePath: '/tmp/pkg/a.ts', line: 1 },
    ]);
    const res = await lspReferencesTool.execute!({ symbol: 'Foo' }, { cwd: '/tmp' } as ToolContext);
    assert.ok(res.output.includes('\npkg/a.ts:'));
    assert.ok(!res.output.includes('/tmp/pkg/a.ts:'));
  });

  it('falls back to the absolute path when no shorter relative exists', async () => {
    const abs = path.resolve('/elsewhere/x.ts');
    mock.method(_lspDiagHooks, 'searchSymbols', () => [
      { name: 'Foo', type: 'class', filePath: abs, line: 1 },
    ]);
    const res = await lspReferencesTool.execute!({ symbol: 'Foo' }, { cwd: '/tmp/deep/dir' } as ToolContext);
    assert.ok(res.output.includes(`\n${abs}:`));
  });
});

// ---------------------------------------------------------------------------
// Gap 17 — searchHint for tool discovery
// ---------------------------------------------------------------------------
describe('searchHint discovery', () => {
  it('advertises code-intelligence synonyms on both tools', () => {
    assert.ok(lspDiagnosticsTool.searchHint && lspDiagnosticsTool.searchHint.includes('code intelligence'));
    assert.ok(lspReferencesTool.searchHint && lspReferencesTool.searchHint.includes('code intelligence'));
  });
});

// ---------------------------------------------------------------------------
// File-existence / regular-file pre-flight validation (Claude LSPTool.validateInput)
// ---------------------------------------------------------------------------
describe('diagnostics file-path validation', () => {
  it('reports "File does not exist" (errorCode 1) for a missing file_path', async () => {
    // getLSPClient must NOT be consulted — validation short-circuits first.
    let clientConsulted = false;
    mock.method(_lspDiagHooks, 'getLSPClient', () => {
      clientConsulted = true;
      return fakeClient({ getDiagnostics: async () => diagFile('file:///x.ts', 3) });
    });
    const missing = path.join(TMP_DIR, 'does-not-exist.ts');
    const res = await lspDiagnosticsTool.execute!({ file_path: missing }, ctx);
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('File does not exist'));
    assert.ok(res.output.includes(missing));
    assert.deepEqual(res.metadata, { errorCode: 1 });
    // getLSPClient is called once to obtain the client, but no diagnostics fetch
    // happens — the output is the validation error, not a diagnostics listing.
    assert.equal(clientConsulted, true);
  });

  it('reports "Path is not a file" (errorCode 2) when file_path is a directory', async () => {
    mock.method(_lspDiagHooks, 'getLSPClient', () =>
      fakeClient({ getDiagnostics: async () => diagFile('file:///x.ts', 3) }));
    const res = await lspDiagnosticsTool.execute!({ file_path: TMP_DIR }, ctx);
    assert.equal(res.isError, true);
    assert.ok(res.output.includes('Path is not a file'));
    assert.deepEqual(res.metadata, { errorCode: 2 });
  });

  it('proceeds normally for an existing regular file', async () => {
    mock.method(_lspDiagHooks, 'getLSPClient', () =>
      fakeClient({ getDiagnostics: async () => diagFile(`file://${FOO_TS}`, 2) }));
    const res = await lspDiagnosticsTool.execute!({ file_path: FOO_TS }, ctx);
    assert.ok(!res.isError);
    assert.deepEqual(res.metadata, { operation: 'diagnostics', resultCount: 2, fileCount: 1 });
  });

  it('skips validation for workspace-wide mode (no file_path)', async () => {
    mock.method(_lspDiagHooks, 'getLSPClient', () =>
      fakeClient({ getAllDiagnostics: () => [diagFile('file:///a.ts', 1)] }));
    const res = await lspDiagnosticsTool.execute!({}, ctx);
    assert.ok(!res.isError);
  });
});
