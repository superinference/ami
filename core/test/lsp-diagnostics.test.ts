import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { lspDiagnosticsTool, lspReferencesTool } from '../src/tools/lsp-diagnostics';
import type { ToolContext } from '../src/types';

const ctx: ToolContext = { cwd: '/tmp' } as ToolContext;

describe('lspDiagnosticsTool', () => {
  it('has correct name and schema', () => {
    assert.equal(lspDiagnosticsTool.name, 'diagnostics');
    assert.ok(lspDiagnosticsTool.isReadOnly);
    assert.ok(lspDiagnosticsTool.isConcurrencySafe);
  });

  it('returns file-specific message when file_path given', async () => {
    const result = await lspDiagnosticsTool.execute!({ file_path: '/tmp/foo.ts' }, ctx);
    assert.ok(result.output.includes('/tmp/foo.ts'));
    assert.ok(result.output.includes('Diagnostics'));
  });

  it('returns workspace message when no file_path', async () => {
    const result = await lspDiagnosticsTool.execute!({}, ctx);
    assert.ok(result.output.includes('Workspace'));
  });

  it('returns fallback when LSP is not available (no file_path)', async () => {
    const result = await lspDiagnosticsTool.execute!({}, ctx);
    assert.ok(result.output.length > 0);
    assert.equal(result.isError, undefined);
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
