import { describe, it, afterEach } from 'node:test';
import { mock } from 'node:test';
import * as assert from 'node:assert/strict';

import { lspTool, _lspToolHooks } from '../src/tools/lsp';
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
    assert.ok(res.output.includes('/proj/a.ts:6:3'));
  });

  it('findReferences includes includeDeclaration and lists all locations', async () => {
    const fake = fakeClient(() => [
      { uri: 'file:///proj/a.ts', range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } } },
      { uri: 'file:///proj/b.ts', range: { start: { line: 4, character: 2 }, end: { line: 4, character: 3 } } },
    ]);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'findReferences', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.equal((fake.calls[0].params as { context: { includeDeclaration: boolean } }).context.includeDeclaration, true);
    assert.ok(res.output.includes('/proj/a.ts:1:1'));
    assert.ok(res.output.includes('/proj/b.ts:5:3'));
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
    assert.ok(res.output.includes('No goToImplementation results'));
  });

  it('reports "No result" when the server returns null', async () => {
    const fake = fakeClient(() => null);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'goToDefinition', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.ok(res.output.includes('No result'));
  });

  it('documentSymbol handles a flat SymbolInformation[] shape', async () => {
    const fake = fakeClient(() => [{ name: 'Flat', kind: 12, location: { uri: 'file:///proj/a.ts', range: { start: { line: 2, character: 0 }, end: { line: 2, character: 4 } } } }]);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'documentSymbol', file_path: 'a.ts' }, ctx);
    assert.ok(res.output.includes('Flat'));
    assert.ok(res.output.includes('/proj/a.ts:3:1'));
  });

  it('documentSymbol / workspaceSymbol report empty', async () => {
    const fake = fakeClient(() => []);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const r1 = await lspTool.execute!({ operation: 'documentSymbol', file_path: 'a.ts' }, ctx);
    assert.ok(r1.output.includes('No symbols'));
    const r2 = await lspTool.execute!({ operation: 'workspaceSymbol', file_path: 'a.ts' }, ctx);
    assert.ok(r2.output.includes('No workspace symbols'));
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
});
