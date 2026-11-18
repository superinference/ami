import { describe, it, afterEach } from 'node:test';
import { mock } from 'node:test';
import * as assert from 'node:assert/strict';

import { lspTool, _lspToolHooks, countSymbols, computeLspCounts } from '../src/tools/lsp';
import type { ToolContext } from '../src/types';

const ctx: ToolContext = { cwd: '/proj' } as ToolContext;

/** Fake LSP client returning scripted results per method. */
function fakeClient(script: (method: string, params: unknown) => unknown) {
  return {
    async request(_fp: string, _cwd: string, method: string, params: unknown) {
      return script(method, params);
    },
  };
}

afterEach(() => mock.restoreAll());

describe('U495 — countSymbols (recursive nested-symbol counting)', () => {
  it('totals a flat list by length', () => {
    assert.equal(countSymbols([
      { name: 'a', kind: 12, range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } } },
      { name: 'b', kind: 12, range: { start: { line: 1, character: 0 }, end: { line: 1, character: 1 } } },
    ]), 2);
  });

  it('descends into nested children at every depth', () => {
    const r = { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } };
    // Class → 2 methods; one method → 1 nested variable. Total = 1 + 2 + 1 = 4.
    const symbols = [
      {
        name: 'C', kind: 5, range: r, children: [
          { name: 'm1', kind: 6, range: r, children: [{ name: 'v', kind: 13, range: r }] },
          { name: 'm2', kind: 6, range: r },
        ],
      },
    ];
    assert.equal(countSymbols(symbols), 4);
  });

  it('treats an empty children array as a leaf', () => {
    const r = { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } };
    assert.equal(countSymbols([{ name: 'x', kind: 5, range: r, children: [] }]), 1);
  });
});

describe('U495 — computeLspCounts per operation', () => {
  const r = (line: number) => ({ start: { line, character: 0 }, end: { line, character: 2 } });

  it('documentSymbol (hierarchical) uses the recursive total, fileCount 1', () => {
    const symbols = [
      { name: 'Outer', kind: 5, range: r(0), children: [{ name: 'inner', kind: 6, range: r(1) }] },
    ];
    assert.deepEqual(computeLspCounts('documentSymbol', symbols), { resultCount: 2, fileCount: 1 });
  });

  it('documentSymbol (flat SymbolInformation) counts by length', () => {
    const symbols = [
      { name: 'Flat', kind: 12, location: { uri: 'file:///proj/a.ts', range: r(2) } },
      { name: 'Flat2', kind: 12, location: { uri: 'file:///proj/a.ts', range: r(3) } },
    ];
    assert.deepEqual(computeLspCounts('documentSymbol', symbols), { resultCount: 2, fileCount: 1 });
  });

  it('documentSymbol drops malformed entries before counting', () => {
    assert.deepEqual(computeLspCounts('documentSymbol', [null, undefined, 5]), { resultCount: 0, fileCount: 0 });
    assert.deepEqual(computeLspCounts('documentSymbol', null), { resultCount: 0, fileCount: 0 });
  });

  it('goToDefinition counts valid locations and distinct files', () => {
    const result = [
      { uri: 'file:///proj/a.ts', range: r(0) },
      { uri: 'file:///proj/a.ts', range: r(1) },
      { uri: 'file:///proj/b.ts', range: r(2) },
      { range: r(3) }, // malformed — no uri
    ];
    assert.deepEqual(computeLspCounts('goToDefinition', result), { resultCount: 3, fileCount: 2 });
  });

  it('goToDefinition normalizes a LocationLink and a single (non-array) result', () => {
    const link = { targetUri: 'file:///proj/a.ts', targetRange: r(0), targetSelectionRange: r(0) };
    assert.deepEqual(computeLspCounts('goToDefinition', link), { resultCount: 1, fileCount: 1 });
  });

  it('findReferences counts valid locations and distinct files', () => {
    const result = [
      { uri: 'file:///proj/a.ts', range: r(0) },
      { uri: 'file:///proj/b.ts', range: r(1) },
    ];
    assert.deepEqual(computeLspCounts('findReferences', result), { resultCount: 2, fileCount: 2 });
    assert.deepEqual(computeLspCounts('findReferences', null), { resultCount: 0, fileCount: 0 });
  });

  it('hover is 1/0 by presence', () => {
    assert.deepEqual(computeLspCounts('hover', { contents: 'docs' }), { resultCount: 1, fileCount: 1 });
    assert.deepEqual(computeLspCounts('hover', null), { resultCount: 0, fileCount: 0 });
  });

  it('workspaceSymbol counts valid symbols and distinct files', () => {
    const result = [
      { name: 'A', kind: 5, location: { uri: 'file:///proj/x.ts', range: r(0) } },
      { name: 'B', kind: 5, location: { uri: 'file:///proj/y.ts', range: r(1) } },
      { name: 'bad' }, // no location
    ];
    assert.deepEqual(computeLspCounts('workspaceSymbol', result), { resultCount: 2, fileCount: 2 });
  });

  it('prepareCallHierarchy counts items and distinct files', () => {
    const result = [
      { name: 't', uri: 'file:///proj/a.ts', range: r(0) },
      { name: 'u', uri: 'file:///proj/a.ts', range: r(1) },
    ];
    assert.deepEqual(computeLspCounts('prepareCallHierarchy', result), { resultCount: 2, fileCount: 1 });
  });

  it('incomingCalls / outgoingCalls count calls and distinct caller/callee files', () => {
    const incoming = [
      { from: { name: 'c1', uri: 'file:///proj/b.ts', range: r(0) } },
      { from: { name: 'c2', uri: 'file:///proj/c.ts', range: r(1) } },
    ];
    assert.deepEqual(computeLspCounts('incomingCalls', incoming), { resultCount: 2, fileCount: 2 });
    const outgoing = [{ to: { name: 'd1', uri: 'file:///proj/d.ts', range: r(0) } }];
    assert.deepEqual(computeLspCounts('outgoingCalls', outgoing), { resultCount: 1, fileCount: 1 });
  });
});

describe('U495 — lspTool surfaces counts as result metadata (end-to-end)', () => {
  it('documentSymbol metadata.resultCount is the recursive nested total', async () => {
    const r = { start: { line: 0, character: 0 }, end: { line: 9, character: 0 } };
    const fake = fakeClient(() => [
      { name: 'Outer', kind: 5, range: r, children: [
        { name: 'inner', kind: 6, range: r, children: [{ name: 'deep', kind: 13, range: r }] },
      ] },
    ]);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'documentSymbol', file_path: 'a.ts' }, ctx);
    // Outer + inner + deep = 3 (would be 1 without recursion into children).
    assert.deepEqual(res.metadata, { resultCount: 3, fileCount: 1 });
    // The rendered output is unchanged (no count injected into the text).
    assert.ok(res.output.includes('Document symbols:'));
  });

  it('findReferences metadata carries resultCount + distinct fileCount', async () => {
    const fake = fakeClient(() => [
      { uri: 'file:///proj/a.ts', range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } } },
      { uri: 'file:///proj/b.ts', range: { start: { line: 1, character: 0 }, end: { line: 1, character: 1 } } },
    ]);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'findReferences', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.deepEqual(res.metadata, { resultCount: 2, fileCount: 2 });
  });

  it('incomingCalls metadata reflects the expanded call list, not the prepared item', async () => {
    const fake = fakeClient((method) => {
      if (method === 'textDocument/prepareCallHierarchy') {
        return [{ name: 'target', uri: 'file:///proj/a.ts', range: { start: { line: 1, character: 0 }, end: { line: 1, character: 6 } } }];
      }
      return [
        { from: { name: 'c1', uri: 'file:///proj/b.ts', range: { start: { line: 8, character: 0 }, end: { line: 8, character: 6 } } } },
        { from: { name: 'c2', uri: 'file:///proj/c.ts', range: { start: { line: 2, character: 0 }, end: { line: 2, character: 6 } } } },
      ];
    });
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'incomingCalls', file_path: 'a.ts', line: 2, character: 1 }, ctx);
    assert.deepEqual(res.metadata, { resultCount: 2, fileCount: 2 });
  });
});
