import { describe, it, mock, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { lspDiagnosticsTool, _lspDiagHooks } from '../src/tools/lsp-diagnostics';
import type { LspDiagClient } from '../src/tools/lsp-diagnostics';
import type { ToolContext } from '../src/types';
import { getInMemoryErrors, REDACT_PLACEHOLDER } from '../src/index';

const ctx: ToolContext = { cwd: '/tmp' } as ToolContext;

afterEach(() => {
  mock.restoreAll();
});

function throwingClient(message: string): LspDiagClient {
  return {
    getDiagnostics: async () => null,
    getAllDiagnostics: () => {
      throw new Error(message);
    },
    isConnected: () => true,
    diagnostics: { hasBaseline: () => false, getNewDiagnostics: async () => [] },
  };
}

// INTEGRATION: an LSP failure mid-work is persisted via logError. The error text
// is scrubbed of local file paths, URLs and secrets by toLoggableError BEFORE it
// lands in the in-memory error ring / debug log (so a bug report / diagnostics
// dump cannot leak the username, machine layout or an embedded token). This is the
// gap-2 wiring: revert the toLoggableError() call in lsp-diagnostics.ts and the
// path + URL below survive in the ring (pushErrorRing only scrubs credentials),
// so this test goes RED.
describe('lsp-diagnostics persisted error is scrubbed (toLoggableError wiring)', () => {
  it('redacts absolute paths, URLs and secrets from the persisted diagnostics error', async () => {
    const secret = 'ghp_' + 'a'.repeat(36);
    const raw = `LSP indexer crashed at ${process.cwd()}/src/server.ts talking to https://lsp.internal.corp/rpc with ${secret}`;
    mock.method(_lspDiagHooks, 'getLSPClient', () => throwingClient(raw));

    const before = getInMemoryErrors().length;
    const res = await lspDiagnosticsTool.execute!({}, ctx);
    // User-facing behaviour unchanged (benign fallback).
    assert.equal(res.output, 'LSP not available. Run the project linter/compiler manually.');

    const added = getInMemoryErrors().slice(before);
    assert.ok(added.length >= 1, 'an error entry should have been recorded');
    const entry = added.find(e => e.error.includes('lsp-diagnostics'));
    assert.ok(entry, 'the lsp-diagnostics error should be in the ring');
    const text = entry!.error;
    // Discriminators — raw sensitive spans must NOT survive persistence.
    assert.ok(!text.includes(`${process.cwd()}/src/server.ts`), 'absolute path must be scrubbed');
    assert.ok(!text.includes('lsp.internal.corp'), 'internal URL must be scrubbed');
    assert.ok(!text.includes(secret), 'secret must be scrubbed');
    // Placeholders prove the scrub ran.
    assert.ok(text.includes('[URL]'), 'URL placeholder present');
    assert.ok(text.includes(REDACT_PLACEHOLDER), 'secret placeholder present');
    assert.ok(text.includes('[CWD]') || text.includes('[HOME]'), 'path placeholder present');
  });
});
