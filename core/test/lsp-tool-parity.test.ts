import { describe, it, afterEach } from 'node:test';
import { mock } from 'node:test';
import * as assert from 'node:assert/strict';

import { lspTool, _lspToolHooks } from '../src/tools/lsp';
import type { ToolContext } from '../src/types';

const ctx: ToolContext = { cwd: '/proj' } as ToolContext;

/** Fake LSP client returning a scripted result for every request. */
function fakeClient(script: (method: string, params: unknown) => unknown) {
  return {
    async request(_fp: string, _cwd: string, method: string, params: unknown) {
      return script(method, params);
    },
  };
}

afterEach(() => mock.restoreAll());

/**
 * Additive parity coverage for lspTool. Each block is a red-green reproducer: the
 * assertion discriminates the fixed behavior from the pre-fix behavior noted in
 * its comment, so reverting the corresponding change in lsp.ts turns it red.
 */
describe('lspTool parity — hover MarkedString language fence', () => {
  it('renders a { language, value } MarkedString as a fenced code block (was: language dropped)', async () => {
    const fake = fakeClient(() => ({ contents: { language: 'typescript', value: 'const x: number' } }));
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'hover', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    // RED (pre-fix): output === 'const x: number' (no fence, no language tag).
    assert.equal(res.output, '```typescript\nconst x: number\n```');
    assert.ok(res.output.includes('```typescript'));
    assert.ok(!res.isError);
  });

  it('fences each entry of a MarkedString[] hover array', async () => {
    const fake = fakeClient(() => ({ contents: [{ language: 'go', value: 'func F()' }, 'docs line'] }));
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'hover', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.ok(res.output.includes('```go\nfunc F()\n```'));
    assert.ok(res.output.includes('docs line'));
  });

  it('still renders a MarkupContent { kind, value } as its bare value (no regression)', async () => {
    const fake = fakeClient(() => ({ contents: { kind: 'markdown', value: 'the docs' } }));
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'hover', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.equal(res.output, 'the docs');
  });

  it('treats an empty-string language as a plain value (no empty fence)', async () => {
    const fake = fakeClient(() => ({ contents: { language: '', value: 'plain' } }));
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'hover', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.equal(res.output, 'plain');
  });
});

describe('lspTool parity — empty hover falls back to no-hover message', () => {
  it('reports "No hover information" for an empty-string contents (was: blank output)', async () => {
    const fake = fakeClient(() => ({ contents: '' }));
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'hover', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    // RED (pre-fix): res.output === '' (empty, unactionable).
    assert.ok(res.output.includes('No hover information available'));
    assert.notEqual(res.output.trim(), '');
  });

  it('reports "No hover information" for an empty contents array', async () => {
    const fake = fakeClient(() => ({ contents: [] }));
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'hover', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.ok(res.output.includes('No hover information available'));
  });

  it('reports "No hover information" for an array of empty-string entries', async () => {
    const fake = fakeClient(() => ({ contents: ['', '   '] }));
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'hover', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.ok(res.output.includes('No hover information available'));
  });

  it('still returns non-empty hover text unchanged (no regression)', async () => {
    const fake = fakeClient(() => ({ contents: 'real hover' }));
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'hover', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.equal(res.output, 'real hover');
  });
});

describe('lspTool parity — documentSymbol tolerates malformed entries', () => {
  it('degrades to the empty-result message for an all-null array (was: thrown TypeError)', async () => {
    const fake = fakeClient(() => [null]);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'documentSymbol', file_path: 'a.ts' }, ctx);
    // RED (pre-fix): `'location' in arr[0]` throws → "Error performing documentSymbol".
    assert.ok(!res.isError);
    assert.ok(res.output.includes('No symbols found in document'));
  });

  it('drops null entries but still formats the valid hierarchical symbols', async () => {
    const fake = fakeClient(() => [
      null,
      { name: 'Keep', kind: 5, range: { start: { line: 0, character: 0 }, end: { line: 3, character: 0 } } },
    ]);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'documentSymbol', file_path: 'a.ts' }, ctx);
    assert.ok(!res.isError);
    assert.ok(res.output.includes('Keep (Class)'));
  });

  it('drops null entries but still detects the flat SymbolInformation shape', async () => {
    const fake = fakeClient(() => [
      null,
      { name: 'Flat', kind: 12, location: { uri: 'file:///proj/a.ts', range: { start: { line: 2, character: 0 }, end: { line: 2, character: 4 } } } },
    ]);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'documentSymbol', file_path: 'a.ts' }, ctx);
    assert.ok(!res.isError);
    assert.ok(res.output.includes('Flat (Function)'));
  });
});
