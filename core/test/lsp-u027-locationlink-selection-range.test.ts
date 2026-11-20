/**
 * w84-U027 — LocationLink normalization prefers `targetSelectionRange`.
 *
 * Parity: the reference's `toLocation` (tools/LSPTool/LSPTool.ts L620-631) collapses
 * a `LocationLink` into a `Location` using `targetSelectionRange || targetRange` —
 * i.e. when a server returns both the full declaration span (`targetRange`) and
 * the narrower identifier span (`targetSelectionRange`), the identifier span is
 * preferred so go-to-definition points at the SYMBOL, not the first line of its
 * enclosing block.
 *
 * Before this unit AMI's `toLocation` only read `targetRange`, so a `LocationLink`
 * carrying a distinct `targetSelectionRange` rendered the wrong line:char. This
 * test locks in the selection-range preference while the existing
 * `targetRange`-only LocationLink test (lsp-tool.test.ts) locks the fallback.
 */
import { describe, it, afterEach } from 'node:test';
import { mock } from 'node:test';
import * as assert from 'node:assert/strict';

import { lspTool, _lspToolHooks } from '../src/tools/lsp';
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

describe('w84-U027 LocationLink targetSelectionRange preference', () => {
  it('uses targetSelectionRange over targetRange when both are present', async () => {
    const fake = fakeClient(() => [
      {
        targetUri: 'file:///proj/d.ts',
        // Full declaration span — the block starts at line 9 (0-based).
        targetRange: { start: { line: 9, character: 0 }, end: { line: 15, character: 1 } },
        // Identifier span — the symbol itself sits at line 11, char 4 (0-based).
        targetSelectionRange: { start: { line: 11, character: 4 }, end: { line: 11, character: 10 } },
      },
    ]);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'goToDefinition', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    // 0-based (11,4) → 1-based 12:5 (the symbol), NOT 10:1 (the block start).
    assert.ok(res.output.includes('/proj/d.ts:12:5'), `expected selection range, got: ${res.output}`);
    assert.ok(!res.output.includes('/proj/d.ts:10:1'), `must not use targetRange: ${res.output}`);
  });

  it('falls back to targetRange when no targetSelectionRange is present', async () => {
    const fake = fakeClient(() => [
      {
        targetUri: 'file:///proj/d.ts',
        targetRange: { start: { line: 9, character: 0 }, end: { line: 9, character: 4 } },
      },
    ]);
    mock.method(_lspToolHooks, 'getLSPClient', () => fake);
    const res = await lspTool.execute!({ operation: 'goToDefinition', file_path: 'a.ts', line: 1, character: 1 }, ctx);
    assert.ok(res.output.includes('/proj/d.ts:10:1'), `expected targetRange fallback, got: ${res.output}`);
  });
});
