import { describe, it, afterEach, before, after } from 'node:test';
import { mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { lspTool, _lspToolHooks } from '../src/tools/lsp';
import { MAX_LSP_FILE_SIZE_BYTES } from '../src/lsp/diagnostic-tracking';
import type { ToolContext } from '../src/types';

const ctx: ToolContext = { cwd: '/proj' } as ToolContext;

/** Build a fake LSP client that records calls and returns scripted results. */
function fakeClient(script: (method: string, params: unknown) => unknown) {
  const calls: Array<{ method: string; params: unknown }> = [];
  return {
    calls,
    async request(_fp: string, _cwd: string, method: string, params: unknown) {
      calls.push({ method, params });
      return script(method, params);
    },
  };
}

afterEach(() => mock.restoreAll());

describe('lspTool schema', () => {
  it('has the operation enum with all nine ops and requires operation + file_path', () => {
    assert.equal(lspTool.name, 'lsp');
    assert.ok(lspTool.isReadOnly);
    const op = lspTool.inputSchema.properties!.operation as { enum: string[] };
    assert.equal(op.enum.length, 9);
    assert.ok(op.enum.includes('prepareCallHierarchy'));
    assert.ok(op.enum.includes('incomingCalls'));
    assert.deepEqual(lspTool.inputSchema.required, ['operation', 'file_path']);
  });
});

describe('lspTool dispatch + formatting', () => {
  it('goToDefinition maps to textDocument/definition with 0-based position', async () => {
    const fake = fakeClient(() => [{ uri: 'file:///proj/a.ts', range: { start: { line: 5, character: 2 }, end: { line: 5, character: 8 } } }]);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'goToDefinition', file_path: 'a.ts', line: 10, character: 4 }, ctx);
    assert.equal(fake.calls[0].method, 'textDocument/definition');
    assert.deepEqual((fake.calls[0].params as { position: unknown }).position, { line: 9, character: 3 });
    assert.ok(res.output.includes('Found 1 definition:'));
    assert.ok(res.output.includes('/proj/a.ts:6:3'));
  });

  it('goToDefinition with multiple sites uses the plural noun', async () => {
    const fake = fakeClient(() => [
      { uri: 'file:///proj/a.ts', range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } } },
      { uri: 'file:///proj/b.ts', range: { start: { line: 2, character: 0 }, end: { line: 2, character: 1 } } },
    ]);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'goToDefinition', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.ok(res.output.includes('Found 2 definitions:'));
  });

  it('findReferences with a single site uses the singular header', async () => {
    const fake = fakeClient(() => [{ uri: 'file:///proj/a.ts', range: { start: { line: 3, character: 1 }, end: { line: 3, character: 2 } } }]);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'findReferences', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.ok(res.output.includes('Found 1 reference:'));
    assert.ok(res.output.includes('/proj/a.ts:4:2'));
  });

  it('filters malformed (uri-less) reference entries before formatting', async () => {
    const fake = fakeClient(() => [
      { range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } } },
      { uri: 'file:///proj/a.ts', range: { start: { line: 1, character: 0 }, end: { line: 1, character: 1 } } },
    ]);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'findReferences', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    // One entry dropped → single valid reference remains.
    assert.ok(res.output.includes('Found 1 reference:'));
    assert.ok(res.output.includes('/proj/a.ts:2:1'));
  });

  it('findReferences includes includeDeclaration and lists all locations', async () => {
    const fake = fakeClient(() => [
      { uri: 'file:///proj/a.ts', range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } } },
      { uri: 'file:///proj/b.ts', range: { start: { line: 4, character: 2 }, end: { line: 4, character: 3 } } },
    ]);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'findReferences', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.equal((fake.calls[0].params as { context: { includeDeclaration: boolean } }).context.includeDeclaration, true);
    // Count header + per-file grouping (singular/plural) with 1-based Line L:C.
    assert.ok(res.output.includes('Found 2 references across 2 files:'));
    assert.ok(res.output.includes('/proj/a.ts:'));
    assert.ok(res.output.includes('/proj/b.ts:'));
    assert.ok(res.output.includes('Line 1:1'));
    assert.ok(res.output.includes('Line 5:3'));
  });

  it('hover extracts MarkupContent value', async () => {
    const fake = fakeClient(() => ({ contents: { kind: 'markdown', value: 'the docs' } }));
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'hover', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.equal(res.output, 'the docs');
  });

  it('documentSymbol flattens hierarchical symbols and needs no position', async () => {
    const fake = fakeClient(() => [{ name: 'Outer', kind: 5, range: { start: { line: 0, character: 0 }, end: { line: 9, character: 0 } }, children: [{ name: 'inner', kind: 6, range: { start: { line: 1, character: 2 }, end: { line: 1, character: 8 } } }] }]);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'documentSymbol', file_path: 'a.ts' }, ctx);
    assert.equal(fake.calls[0].method, 'textDocument/documentSymbol');
    assert.ok(res.output.includes('Outer'));
    assert.ok(res.output.includes('inner'));
  });

  it('workspaceSymbol maps to workspace/symbol with empty query', async () => {
    const fake = fakeClient(() => [{ name: 'Thing', kind: 5, location: { uri: 'file:///proj/x.ts', range: { start: { line: 2, character: 0 }, end: { line: 2, character: 4 } } } }]);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'workspaceSymbol', file_path: 'a.ts' }, ctx);
    assert.equal(fake.calls[0].method, 'workspace/symbol');
    assert.ok(res.output.includes('Thing'));
  });

  it('incomingCalls is a two-step prepare → calls round-trip', async () => {
    const fake = fakeClient((method) => {
      if (method === 'textDocument/prepareCallHierarchy') return [{ name: 'target', uri: 'file:///proj/a.ts', range: { start: { line: 1, character: 0 }, end: { line: 1, character: 6 } } }];
      return [{ from: { name: 'caller', uri: 'file:///proj/b.ts', range: { start: { line: 8, character: 0 }, end: { line: 8, character: 6 } } } }];
    });
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'incomingCalls', file_path: 'a.ts', line: 2, character: 1 }, ctx);
    assert.equal(fake.calls[0].method, 'textDocument/prepareCallHierarchy');
    assert.equal(fake.calls[1].method, 'callHierarchy/incomingCalls');
    assert.deepEqual((fake.calls[1].params as { item: { name: string } }).item.name, 'target');
    assert.ok(res.output.includes('caller'));
  });

  it('outgoingCalls uses callHierarchy/outgoingCalls', async () => {
    const fake = fakeClient((method) => {
      if (method === 'textDocument/prepareCallHierarchy') return [{ name: 't', uri: 'file:///proj/a.ts', range: { start: { line: 1, character: 0 }, end: { line: 1, character: 6 } } }];
      return [{ to: { name: 'callee', uri: 'file:///proj/c.ts', range: { start: { line: 3, character: 0 }, end: { line: 3, character: 6 } } } }];
    });
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'outgoingCalls', file_path: 'a.ts', line: 2, character: 1 }, ctx);
    assert.equal(fake.calls[1].method, 'callHierarchy/outgoingCalls');
    assert.ok(res.output.includes('callee'));
  });

  it('prepareCallHierarchy with no item reports nothing found', async () => {
    const fake = fakeClient(() => []);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'prepareCallHierarchy', file_path: 'a.ts', line: 2, character: 1 }, ctx);
    assert.ok(res.output.includes('No call hierarchy item'));
  });

  it('incomingCalls with no prepared item short-circuits', async () => {
    const fake = fakeClient(() => []);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'incomingCalls', file_path: 'a.ts', line: 2, character: 1 }, ctx);
    assert.equal(fake.calls.length, 1); // never issued the second request
    assert.ok(res.output.includes('No call hierarchy item'));
  });
});

describe('lspTool formatting edge cases', () => {
  it('wraps a single (non-array) definition location', async () => {
    const fake = fakeClient(() => ({ uri: 'file:///proj/a.ts', range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } } }));
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'goToDefinition', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.ok(res.output.includes('/proj/a.ts:1:1'));
  });

  it('reports no results for an empty location array', async () => {
    const fake = fakeClient(() => []);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'goToImplementation', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.ok(res.output.includes('No implementation found'));
  });

  it('reports an actionable "no definition" message when the server returns null', async () => {
    const fake = fakeClient(() => null);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'goToDefinition', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.ok(res.output.includes('No definition found'));
    assert.ok(res.output.includes('not indexed by the LSP server'));
  });

  it('documentSymbol handles a flat SymbolInformation[] shape with kind labels', async () => {
    const fake = fakeClient(() => [{ name: 'Flat', kind: 12, location: { uri: 'file:///proj/a.ts', range: { start: { line: 2, character: 0 }, end: { line: 2, character: 4 } } } }]);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'documentSymbol', file_path: 'a.ts' }, ctx);
    assert.ok(res.output.includes('Flat (Function)'));
    assert.ok(res.output.includes('Line 3'));
  });

  it('documentSymbol renders kind labels, indentation and detail for a hierarchy', async () => {
    const fake = fakeClient(() => [{ name: 'Outer', kind: 5, detail: 'class Outer', range: { start: { line: 0, character: 0 }, end: { line: 9, character: 0 } }, children: [{ name: 'inner', kind: 6, range: { start: { line: 1, character: 2 }, end: { line: 1, character: 8 } } }] }]);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'documentSymbol', file_path: 'a.ts' }, ctx);
    assert.ok(res.output.includes('Document symbols:'));
    assert.ok(res.output.includes('Outer (Class) class Outer - Line 1'));
    assert.ok(res.output.includes('  inner (Method) - Line 2'));
  });

  it('workspaceSymbol groups by file with kind labels and container', async () => {
    const fake = fakeClient(() => [{ name: 'Thing', kind: 5, containerName: 'ns', location: { uri: 'file:///proj/x.ts', range: { start: { line: 2, character: 0 }, end: { line: 2, character: 4 } } } }]);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'workspaceSymbol', file_path: 'a.ts' }, ctx);
    assert.ok(res.output.includes('Found 1 symbol in workspace:'));
    assert.ok(res.output.includes('Thing (Class) - Line 3 in ns'));
  });

  it('documentSymbol / workspaceSymbol report actionable empty messages', async () => {
    const fake = fakeClient(() => []);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const r1 = await lspTool.execute!({ operation: 'documentSymbol', file_path: 'a.ts' }, ctx);
    assert.ok(r1.output.includes('No symbols found in document'));
    const r2 = await lspTool.execute!({ operation: 'workspaceSymbol', file_path: 'a.ts' }, ctx);
    assert.ok(r2.output.includes('No symbols found in workspace'));
  });

  it('documentSymbol / workspaceSymbol handle a non-array (null) server result', async () => {
    const fake = fakeClient(() => null);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const r1 = await lspTool.execute!({ operation: 'documentSymbol', file_path: 'a.ts' }, ctx);
    assert.ok(r1.output.includes('No symbols found in document'));
    const r2 = await lspTool.execute!({ operation: 'workspaceSymbol', file_path: 'a.ts' }, ctx);
    assert.ok(r2.output.includes('No symbols found in workspace'));
  });

  it('findReferences reports an actionable message for an empty result', async () => {
    const fake = fakeClient(() => []);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'findReferences', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.ok(res.output.includes('No references found'));
  });

  it('hover handles string, array and value-less object forms', async () => {
    mock.method(_lspToolHooks, 'getLSPClient', () => fakeClient(() => ({ contents: 'plain string' })));
    assert.equal((await lspTool.execute!({ operation: 'hover', file_path: 'a.ts', line: 1, character: 1 }, ctx)).output, 'plain string');
    mock.restoreAll();
    mock.method(_lspToolHooks, 'getLSPClient', () => fakeClient(() => ({ contents: ['a', { value: 'b' }] })));
    assert.ok((await lspTool.execute!({ operation: 'hover', file_path: 'a.ts', line: 1, character: 1 }, ctx)).output.includes('b'));
    mock.restoreAll();
    mock.method(_lspToolHooks, 'getLSPClient', () => fakeClient(() => ({ contents: null })));
    assert.ok((await lspTool.execute!({ operation: 'hover', file_path: 'a.ts', line: 1, character: 1 }, ctx)).output.includes('No hover'));
    mock.restoreAll();
    mock.method(_lspToolHooks, 'getLSPClient', () => fakeClient(() => ({ contents: { kind: 'x' } })));
    assert.ok((await lspTool.execute!({ operation: 'hover', file_path: 'a.ts', line: 1, character: 1 }, ctx)).output.includes('kind'));
  });

  it('incoming/outgoing calls report empty when the calls list is empty', async () => {
    const fake = fakeClient((method) => (method === 'textDocument/prepareCallHierarchy' ? [{ name: 't', uri: 'file:///proj/a.ts', range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } } }] : []));
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const inc = await lspTool.execute!({ operation: 'incomingCalls', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.ok(inc.output.includes('No incoming calls'));
    mock.restoreAll();
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const out = await lspTool.execute!({ operation: 'outgoingCalls', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.ok(out.output.includes('No outgoing calls'));
  });
});

describe('lspTool guards', () => {
  it('rejects an invalid operation', async () => {
    const res = await lspTool.execute!({ operation: 'frobnicate', file_path: 'a.ts' }, ctx);
    assert.ok(res.isError);
    assert.ok(res.output.includes('Valid:'));
  });

  it('requires line/character for position ops', async () => {
    const res = await lspTool.execute!({ operation: 'hover', file_path: 'a.ts' }, ctx);
    assert.ok(res.isError);
    assert.ok(res.output.includes('requires 1-based'));
  });

  it('returns a graceful no-server message when no server handles the file', async () => {
    const fake = fakeClient(() => undefined);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'hover', file_path: 'a.unknown', line: 1, character: 1 }, ctx);
    assert.ok(res.output.includes('No LSP server available'));
    assert.ok(!res.isError);
  });

  it('surfaces a thrown error as a structured error result', async () => {
    const fake = { async request() { throw new Error('kaboom'); } };
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'hover', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.ok(res.isError);
    assert.ok(res.output.includes('kaboom'));
  });

  it('resolves an absolute file_path unchanged', async () => {
    const fake = fakeClient(() => ({ contents: 'x' }));
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'hover', file_path: '/abs/a.ts', line: 1, character: 1 }, ctx);
    assert.equal(res.output, 'x');
  });

  it('exercises the real (default) getLSPClient hook', () => {
    // No mock: drives the lazy-require default implementation (returns the
    // process-wide singleton). Must not throw.
    const client = _lspToolHooks.getLSPClient();
    assert.ok(client);
    assert.equal(typeof client.request, 'function');
  });

  it('formats a populated prepareCallHierarchy result', async () => {
    const fake = fakeClient(() => [
      { name: 'myFunc', uri: 'file:///proj/a.ts', range: { start: { line: 4, character: 2 }, end: { line: 4, character: 8 } } },
    ]);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'prepareCallHierarchy', file_path: 'a.ts', line: 5, character: 3 }, ctx);
    assert.ok(res.output.includes('myFunc'));
    assert.ok(res.output.includes('/proj/a.ts:5')); // 0-based line 4 → 1-based 5
  });

  it('formats a LocationLink (targetUri/targetRange) definition result', async () => {
    const fake = fakeClient(() => [
      { targetUri: 'file:///proj/d.ts', targetRange: { start: { line: 9, character: 0 }, end: { line: 9, character: 4 } } },
    ]);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'goToDefinition', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.ok(res.output.includes('/proj/d.ts:10:1')); // 0-based 9 → 1-based 10
  });

  it('formats a location with a missing range (defensive 0:0 fallback)', async () => {
    const fake = fakeClient(() => [{ uri: 'file:///proj/e.ts' }]);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'findReferences', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.ok(res.output.includes('/proj/e.ts:0:0'));
  });
});

describe('lspTool call-hierarchy enrichment', () => {
  it('incomingCalls groups by file with kind labels and call-site ranges', async () => {
    const fake = fakeClient((method) => {
      if (method === 'textDocument/prepareCallHierarchy') return [{ name: 't', uri: 'file:///proj/a.ts', range: { start: { line: 1, character: 0 }, end: { line: 1, character: 6 } } }];
      return [
        { from: { name: 'caller1', kind: 12, uri: 'file:///proj/b.ts', range: { start: { line: 8, character: 0 }, end: { line: 8, character: 6 } } }, fromRanges: [{ start: { line: 9, character: 2 }, end: { line: 9, character: 8 } }] },
        { from: { name: 'caller2', kind: 6, uri: 'file:///proj/c.ts', range: { start: { line: 3, character: 0 }, end: { line: 3, character: 6 } } } },
      ];
    });
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'incomingCalls', file_path: 'a.ts', line: 2, character: 1 }, ctx);
    assert.ok(res.output.includes('Found 2 incoming calls:'));
    assert.ok(res.output.includes('caller1 (Function) - Line 9'));
    assert.ok(res.output.includes('[calls at: 10:3]'));
    assert.ok(res.output.includes('caller2 (Method) - Line 4'));
    assert.ok(res.output.includes('/proj/b.ts:'));
    assert.ok(res.output.includes('/proj/c.ts:'));
  });

  it('outgoingCalls annotates call sites with "called from"', async () => {
    const fake = fakeClient((method) => {
      if (method === 'textDocument/prepareCallHierarchy') return [{ name: 't', uri: 'file:///proj/a.ts', range: { start: { line: 1, character: 0 }, end: { line: 1, character: 6 } } }];
      return [{ to: { name: 'callee', kind: 12, uri: 'file:///proj/c.ts', range: { start: { line: 3, character: 0 }, end: { line: 3, character: 6 } } }, fromRanges: [{ start: { line: 5, character: 4 }, end: { line: 5, character: 9 } }] }];
    });
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'outgoingCalls', file_path: 'a.ts', line: 2, character: 1 }, ctx);
    assert.ok(res.output.includes('Found 1 outgoing call:'));
    assert.ok(res.output.includes('[called from: 6:5]'));
  });

  it('filters call entries whose from/to is undefined', async () => {
    const fake = fakeClient((method) => {
      if (method === 'textDocument/prepareCallHierarchy') return [{ name: 't', uri: 'file:///proj/a.ts', range: { start: { line: 1, character: 0 }, end: { line: 1, character: 6 } } }];
      return [{}, { from: { name: 'realCaller', uri: 'file:///proj/b.ts', range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } } } }];
    });
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'incomingCalls', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.ok(res.output.includes('Found 1 incoming call:'));
    assert.ok(res.output.includes('realCaller'));
  });

  it('prepareCallHierarchy renders multiple items and the Unknown kind fallback', async () => {
    const fake = fakeClient(() => [
      { name: 'one', uri: 'file:///proj/a.ts', range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } } },
      { name: 'two', kind: 5, detail: 'exported', uri: 'file:///proj/b.ts', range: { start: { line: 4, character: 0 }, end: { line: 4, character: 1 } } },
    ]);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'prepareCallHierarchy', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.ok(res.output.includes('Found 2 call hierarchy items:'));
    assert.ok(res.output.includes('one (Unknown)'));
    assert.ok(res.output.includes('two (Class)'));
    assert.ok(res.output.includes('[exported]')); // detail suffix
  });

  it('renders <unknown location> for a call item with a missing uri', async () => {
    const fake = fakeClient(() => [{ name: 'noUri', kind: 12, range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } } }]);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'prepareCallHierarchy', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.ok(res.output.includes('noUri (Function) - <unknown location>'));
  });
});

describe('lspTool safety guards', () => {
  let dir: string;
  before(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lsp-tool-')); });
  after(() => { if (dir) fs.rmSync(dir, { recursive: true, force: true }); });

  it('skips a UNC file path before touching the filesystem', async () => {
    const res = await lspTool.execute!({ operation: 'hover', file_path: '//host/share/f.ts', line: 1, character: 1 }, ctx);
    assert.ok(res.output.includes('UNC path'));
    assert.ok(!res.isError);
  });

  it('refuses a file that exceeds the LSP size limit', async () => {
    const big = path.join(dir, 'big.ts');
    fs.writeFileSync(big, '');
    fs.truncateSync(big, MAX_LSP_FILE_SIZE_BYTES + 1); // sparse file, no real write
    const res = await lspTool.execute!({ operation: 'hover', file_path: big, line: 1, character: 1 }, ctx);
    assert.ok(res.output.includes('File too large for LSP analysis'));
    assert.ok(res.output.includes('exceeds 10MB limit'));
  });

  it('allows a normally-sized existing file through the size guard', async () => {
    const small = path.join(dir, 'small.ts');
    fs.writeFileSync(small, 'const x = 1;\n');
    const fake = fakeClient(() => ({ contents: 'ok' }));
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'hover', file_path: small, line: 1, character: 1 }, ctx);
    assert.equal(res.output, 'ok');
  });

  it('rejects a zero/negative/fractional line or character', async () => {
    for (const bad of [{ line: 0, character: 1 }, { line: 1, character: -2 }, { line: 1.5, character: 1 }]) {
      const res = await lspTool.execute!({ operation: 'hover', file_path: 'a.ts', ...bad }, ctx);
      assert.ok(res.isError);
      assert.ok(res.output.includes('positive integer'));
    }
  });
});
