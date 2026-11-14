import { describe, it, afterEach } from 'node:test';
import { mock } from 'node:test';
import * as assert from 'node:assert/strict';

import { lspDiagnosticsTool, lspReferencesTool, _lspDiagHooks, type LspDiagClient } from '../src/tools/lsp-diagnostics';
import type { DiagnosticFile } from '../src/lsp/diagnostic-tracking';
import type { ToolContext } from '../src/types';

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

  it('reports a clean workspace when there are no diagnostics', async () => {
    const fake = fakeClient({ getAllDiagnostics: () => [] });
    mock.method(_lspDiagHooks, 'getLSPClient', () => fake);
    const res = await lspDiagnosticsTool.execute!({}, ctx);
    assert.ok(res.output.includes('No diagnostics reported'));
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
});
