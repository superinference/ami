import { describe, it, afterEach } from 'node:test';
import { mock } from 'node:test';
import * as assert from 'node:assert/strict';

import { lspDiagnosticsTool, lspReferencesTool, _lspDiagHooks, SEARCH_SYMBOL_CAP, type LspDiagClient } from '../src/tools/lsp-diagnostics';
import { MAX_LSP_FILE_SIZE_BYTES, type DiagnosticFile } from '../src/lsp/diagnostic-tracking';
import type { ToolContext } from '../src/types';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

const ctx: ToolContext = { cwd: '/tmp' } as ToolContext;

function diagFile(uri: string, messages: string[]): DiagnosticFile {
  return {
    uri,
    diagnostics: messages.map((m, i) => ({
      message: m,
      severity: 'Error' as const,
      range: { start: { line: i, character: 0 }, end: { line: i, character: 1 } },
    })),
  };
}

/** Build a fake LSP client for the diagnostics tool. */
function fakeClient(overrides: Partial<LspDiagClient> & { baseline?: boolean } = {}): LspDiagClient {
  return {
    getDiagnostics: overrides.getDiagnostics ?? (async () => null),
    getAllDiagnostics: overrides.getAllDiagnostics ?? (() => []),
    isConnected: overrides.isConnected ?? (() => false),
    diagnostics: overrides.diagnostics ?? {
      hasBaseline: () => overrides.baseline ?? false,
      getNewDiagnostics: async () => [],
    },
  };
}

afterEach(() => mock.restoreAll());

describe('lspDiagnosticsTool', () => {
  it('has correct name and schema', () => {
    assert.equal(lspDiagnosticsTool.name, 'diagnostics');
    assert.ok(lspDiagnosticsTool.isReadOnly);
    assert.ok(lspDiagnosticsTool.isConcurrencySafe);
  });

  it('returns the new-vs-baseline delta summary when a baseline exists', async () => {
    const fake = fakeClient({
      diagnostics: {
        hasBaseline: () => true,
        getNewDiagnostics: async () => [diagFile('file:///tmp/foo.ts', ['type error here'])],
      },
    });
    mock.method(_lspDiagHooks, 'getLSPClient', () => fake);
    const res = await lspDiagnosticsTool.execute!({ file_path: '/tmp/foo.ts' }, ctx);
    assert.ok(res.output.includes('Found 1 new diagnostic issue in 1 file'));
    assert.ok(res.output.includes('type error here'));
  });

  it('reports no new diagnostics when the baseline-diff is empty', async () => {
    const fake = fakeClient({ diagnostics: { hasBaseline: () => true, getNewDiagnostics: async () => [] } });
    mock.method(_lspDiagHooks, 'getLSPClient', () => fake);
    const res = await lspDiagnosticsTool.execute!({ file_path: '/tmp/foo.ts' }, ctx);
    assert.ok(res.output.includes('No new diagnostics'));
  });

  it('returns current diagnostics for a file with no baseline', async () => {
    const fake = fakeClient({ getDiagnostics: async () => diagFile('file:///tmp/foo.ts', ['unused var']) });
    mock.method(_lspDiagHooks, 'getLSPClient', () => fake);
    const res = await lspDiagnosticsTool.execute!({ file_path: '/tmp/foo.ts' }, ctx);
    assert.ok(res.output.includes('unused var'));
  });

  it('says no diagnostics when the file is clean', async () => {
    const fake = fakeClient({ getDiagnostics: async () => diagFile('file:///tmp/foo.ts', []) });
    mock.method(_lspDiagHooks, 'getLSPClient', () => fake);
    const res = await lspDiagnosticsTool.execute!({ file_path: '/tmp/foo.ts' }, ctx);
    assert.ok(res.output.includes('No diagnostics for'));
  });

  it('reports no server available when getDiagnostics returns null', async () => {
    const fake = fakeClient({ getDiagnostics: async () => null });
    mock.method(_lspDiagHooks, 'getLSPClient', () => fake);
    const res = await lspDiagnosticsTool.execute!({ file_path: '/tmp/foo.rb' }, ctx);
    assert.ok(res.output.includes('No LSP server available'));
  });

  it('returns a workspace-wide summary when no file_path', async () => {
    const fake = fakeClient({ getAllDiagnostics: () => [diagFile('file:///tmp/a.ts', ['e1']), diagFile('file:///tmp/b.ts', ['e2', 'e3'])] });
    mock.method(_lspDiagHooks, 'getLSPClient', () => fake);
    const res = await lspDiagnosticsTool.execute!({}, ctx);
    assert.ok(res.output.includes('3 diagnostic issue(s) across 2 file(s)'));
  });

  it('reports a clean workspace when a server is connected but has no diagnostics', async () => {
    const fake = fakeClient({ getAllDiagnostics: () => [], isConnected: () => true });
    mock.method(_lspDiagHooks, 'getLSPClient', () => fake);
    const res = await lspDiagnosticsTool.execute!({}, ctx);
    assert.ok(res.output.includes('No diagnostics reported'));
  });

  it('surfaces the still-indexing pending count when a connected server has not published yet', async () => {
    const fake = fakeClient({ getAllDiagnostics: () => [], isConnected: () => true });
    (fake as LspDiagClient & { getPendingDiagnosticCount: () => number }).getPendingDiagnosticCount = () => 2;
    mock.method(_lspDiagHooks, 'getLSPClient', () => fake);
    const res = await lspDiagnosticsTool.execute!({}, ctx);
    assert.ok(res.output.includes('No diagnostics reported'));
    assert.ok(res.output.includes('2 file(s) still awaiting diagnostics'));
  });

  it('omits the pending note when nothing is awaiting diagnostics', async () => {
    const fake = fakeClient({ getAllDiagnostics: () => [], isConnected: () => true });
    (fake as LspDiagClient & { getPendingDiagnosticCount: () => number }).getPendingDiagnosticCount = () => 0;
    mock.method(_lspDiagHooks, 'getLSPClient', () => fake);
    const res = await lspDiagnosticsTool.execute!({}, ctx);
    assert.ok(res.output.includes('No diagnostics reported'));
    assert.ok(!res.output.includes('awaiting diagnostics'));
  });

  it('reports a disconnected affordance when no server is connected', async () => {
    const fake = fakeClient({ getAllDiagnostics: () => [], isConnected: () => false });
    mock.method(_lspDiagHooks, 'getLSPClient', () => fake);
    const res = await lspDiagnosticsTool.execute!({}, ctx);
    assert.ok(res.output.includes('No LSP server is currently connected'));
  });

  it('skips a UNC file path before any filesystem/LSP work (NTLM-leak guard)', async () => {
    const fake = fakeClient();
    mock.method(_lspDiagHooks, 'getLSPClient', () => fake);
    const res = await lspDiagnosticsTool.execute!({ file_path: '//server/share/foo.ts' }, ctx);
    assert.ok(res.output.includes('Skipping diagnostics for UNC path'));
  });

  it('refuses a file over the LSP size limit before querying the server', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lsp-diag-'));
    try {
      const big = path.join(dir, 'big.ts');
      fs.writeFileSync(big, '');
      fs.truncateSync(big, MAX_LSP_FILE_SIZE_BYTES + 1); // sparse
      let queried = false;
      const fake = fakeClient({ getDiagnostics: async () => { queried = true; return null; } });
      mock.method(_lspDiagHooks, 'getLSPClient', () => fake);
      const res = await lspDiagnosticsTool.execute!({ file_path: big }, ctx);
      assert.ok(res.output.includes('File too large for LSP analysis'));
      assert.equal(queried, false); // short-circuited before touching the client
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('volume-caps a flood of workspace diagnostics to 30 total before formatting', async () => {
    const many = (uri: string, n: number): DiagnosticFile => ({
      uri,
      diagnostics: Array.from({ length: n }, (_v, i) => ({
        message: `issue ${i}`,
        severity: 'Error' as const,
        range: { start: { line: i, character: 0 }, end: { line: i, character: 1 } },
      })),
    });
    const fake = fakeClient({ getAllDiagnostics: () => [many('file:///tmp/a.ts', 50), many('file:///tmp/b.ts', 50)] });
    mock.method(_lspDiagHooks, 'getLSPClient', () => fake);
    const res = await lspDiagnosticsTool.execute!({}, ctx);
    // 50+50 raw → capped at 10/file (under the 30 total) → 20 reported.
    assert.ok(res.output.includes('20 diagnostic issue(s) across 2 file(s)'));
  });

  it('falls back gracefully when the client lookup throws', async () => {
    mock.method(_lspDiagHooks, 'getLSPClient', () => { throw new Error('no lsp'); });
    const res = await lspDiagnosticsTool.execute!({}, ctx);
    assert.ok(res.output.includes('LSP not available'));
    assert.equal(res.isError, undefined);
  });

  it('falls back gracefully when a diagnostics query throws', async () => {
    const fake = fakeClient({ getAllDiagnostics: () => { throw new Error('boom'); } });
    mock.method(_lspDiagHooks, 'getLSPClient', () => fake);
    const res = await lspDiagnosticsTool.execute!({}, ctx);
    assert.ok(res.output.includes('LSP not available'));
  });

  it('falls back gracefully when the file-path query throws', async () => {
    const fake = fakeClient({ getDiagnostics: async () => { throw new Error('boom'); } });
    mock.method(_lspDiagHooks, 'getLSPClient', () => fake);
    const res = await lspDiagnosticsTool.execute!({ file_path: '/tmp/foo.ts' }, ctx);
    assert.ok(res.output.includes('LSP not available'));
  });
});

describe('lspReferencesTool', () => {
  it('has correct name and schema', () => {
    assert.equal(lspReferencesTool.name, 'find_references');
    assert.ok(lspReferencesTool.isReadOnly);
    assert.ok(lspReferencesTool.isConcurrencySafe);
    assert.deepEqual(lspReferencesTool.inputSchema.required, ['symbol']);
  });

  it('returns results or no-match message for a symbol', async () => {
    const result = await lspReferencesTool.execute!({ symbol: 'xyzNonExistent999' }, ctx);
    assert.ok(result.output.includes('xyzNonExistent999'));
  });

  it('includes symbol name in output', async () => {
    const result = await lspReferencesTool.execute!({ symbol: 'MyClass' }, ctx);
    assert.ok(result.output.includes('MyClass'));
  });

  it('gives an actionable message when nothing matches', async () => {
    mock.method(_lspDiagHooks, 'searchSymbols', () => []);
    const res = await lspReferencesTool.execute!({ symbol: 'nope' }, ctx);
    assert.ok(res.output.includes('No references found for "nope"'));
    assert.ok(res.output.includes('indexer has not scanned'));
  });

  it('groups references by file with a singular/plural count header', async () => {
    mock.method(_lspDiagHooks, 'searchSymbols', () => [
      { name: 'foo', type: 'function', filePath: 'a.ts', line: 3 },
      { name: 'foo', type: 'call', filePath: 'a.ts', line: 7 },
      { name: 'foo', type: 'call', filePath: 'b.ts', line: 1 },
    ]);
    const res = await lspReferencesTool.execute!({ symbol: 'foo' }, ctx);
    assert.ok(res.output.includes('Found 3 references to "foo" across 2 files:'));
    assert.ok(res.output.includes('a.ts:'));
    assert.ok(res.output.includes('  foo (function) - Line 3'));
    assert.ok(res.output.includes('b.ts:'));
  });

  it('uses singular nouns for a single match in a single file', async () => {
    mock.method(_lspDiagHooks, 'searchSymbols', () => [{ name: 'foo', type: 'function', filePath: 'a.ts', line: 3 }]);
    const res = await lspReferencesTool.execute!({ symbol: 'foo' }, ctx);
    assert.ok(res.output.includes('Found 1 reference to "foo" across 1 file:'));
  });

  it('discloses truncation when the result list hits the cap', async () => {
    mock.method(_lspDiagHooks, 'searchSymbols', () =>
      Array.from({ length: SEARCH_SYMBOL_CAP }, (_v, i) => ({ name: 'foo', type: 'call', filePath: `f${i}.ts`, line: 1 })),
    );
    const res = await lspReferencesTool.execute!({ symbol: 'foo' }, ctx);
    assert.ok(res.output.includes(`showing first ${SEARCH_SYMBOL_CAP} matches`));
  });
});
