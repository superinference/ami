import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import { lspTool, _lspToolHooks } from '../src/tools/lsp';
import { _gitIgnoreFilterHooks } from '../src/tools/lsp-gitignore-filter';
import type { ToolContext } from '../src/types';

const ctx: ToolContext = { cwd: '/proj' } as ToolContext;

function fakeClient(script: (method: string, params: unknown) => unknown) {
  return {
    async request(_fp: string, _cwd: string, method: string, params: unknown) {
      return script(method, params);
    },
  };
}

afterEach(() => mock.restoreAll());

describe('lspTool – gitignore filtering of results (U213)', () => {
  it('drops gitignored files from findReferences output', async () => {
    mock.method(_lspToolHooks, 'getLSPClient', () => fakeClient(() => [
      { uri: 'file:///proj/src/a.ts', range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } } },
      { uri: 'file:///proj/dist/a.js', range: { start: { line: 1, character: 0 }, end: { line: 1, character: 1 } } },
    ]));
    // dist/ is gitignored.
    mock.method(_gitIgnoreFilterHooks, 'execFileAsync', async () => ({ stdout: '/proj/dist/a.js\n', stderr: '' }));

    const res = await lspTool.execute!({ operation: 'findReferences', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.ok(res.output.includes('src/a.ts') || res.output.includes('/proj/src/a.ts'), res.output);
    assert.ok(!res.output.includes('dist/a.js'), res.output);
    // Only one reference survived → singular header.
    assert.ok(res.output.includes('Found 1 reference:'), res.output);
  });

  it('drops gitignored files from goToDefinition output', async () => {
    mock.method(_lspToolHooks, 'getLSPClient', () => fakeClient(() => [
      { uri: 'file:///proj/vendor/lib.d.ts', range: { start: { line: 2, character: 0 }, end: { line: 2, character: 1 } } },
    ]));
    mock.method(_gitIgnoreFilterHooks, 'execFileAsync', async () => ({ stdout: '/proj/vendor/lib.d.ts\n', stderr: '' }));

    const res = await lspTool.execute!({ operation: 'goToDefinition', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    // The only definition was gitignored → actionable empty message.
    assert.match(res.output, /No definition found/);
  });

  it('drops gitignored files from workspaceSymbol output', async () => {
    mock.method(_lspToolHooks, 'getLSPClient', () => fakeClient(() => [
      { name: 'Keep', kind: 12, location: { uri: 'file:///proj/src/a.ts', range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } } } },
      { name: 'Drop', kind: 12, location: { uri: 'file:///proj/build/gen.ts', range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } } } },
    ]));
    mock.method(_gitIgnoreFilterHooks, 'execFileAsync', async () => ({ stdout: '/proj/build/gen.ts\n', stderr: '' }));

    const res = await lspTool.execute!({ operation: 'workspaceSymbol', file_path: 'a.ts' }, ctx);
    assert.ok(res.output.includes('Keep'), res.output);
    assert.ok(!res.output.includes('Drop'), res.output);
  });

  it('leaves results untouched when nothing is gitignored', async () => {
    mock.method(_lspToolHooks, 'getLSPClient', () => fakeClient(() => [
      { uri: 'file:///proj/src/a.ts', range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } } },
      { uri: 'file:///proj/src/b.ts', range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } } },
    ]));
    mock.method(_gitIgnoreFilterHooks, 'execFileAsync', async () => ({ stdout: '', stderr: '' }));

    const res = await lspTool.execute!({ operation: 'findReferences', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.ok(res.output.includes('Found 2 references'), res.output);
  });

  it('does not gitignore-filter documentSymbol (explicitly-opened file)', async () => {
    let checkIgnoreCalls = 0;
    mock.method(_lspToolHooks, 'getLSPClient', () => fakeClient(() => [
      { name: 'Foo', kind: 12, range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } } },
    ]));
    mock.method(_gitIgnoreFilterHooks, 'execFileAsync', async () => {
      checkIgnoreCalls++;
      return { stdout: '', stderr: '' };
    });
    const res = await lspTool.execute!({ operation: 'documentSymbol', file_path: 'a.ts' }, ctx);
    assert.ok(res.output.includes('Foo'), res.output);
    assert.equal(checkIgnoreCalls, 0);
  });
});
