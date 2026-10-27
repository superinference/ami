import { describe, it, mock, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { createMcpConnectTool } from '../src/tools/mcp-connect';
import { listMcpResourcesTool, readMcpResourceTool } from '../src/tools/mcp-resources';
import { lspDiagnosticsTool, lspReferencesTool, _lspDiagHooks } from '../src/tools/lsp-diagnostics';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: '/tmp',
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

/** Build a fake MCP manager whose individual methods can be customised. */
function fakeManager(impl: Record<string, unknown>): ToolContext {
  return ctx({ _mcpManager: impl as unknown as ToolContext['_mcpManager'] });
}

afterEach(() => {
  mock.restoreAll();
});

// ===========================================================================
// mcp-connect.ts
// ===========================================================================

describe('createMcpConnectTool', () => {
  it('normalizes the server name into a safe tool name', () => {
    const tool = createMcpConnectTool('My Server!42', 'desc text');
    // Every non-[A-Za-z0-9_] char becomes `_`.
    assert.equal(tool.name, 'mcp__My_Server_42__connect');
    assert.ok(tool.isReadOnly);
    assert.ok(tool.description.includes('My Server!42'));
    assert.ok(tool.description.includes('desc text'));
  });

  it('errors when the MCP manager is not initialized', async () => {
    const tool = createMcpConnectTool('srv', 'd');
    const res = await tool.execute!({}, ctx({ _mcpManager: undefined }));
    assert.equal(res.isError, true);
    assert.match(res.output, /MCP not initialized/);
  });

  it('connects and lists discovered tool names on success', async () => {
    let connectedWith: string | undefined;
    const tool = createMcpConnectTool('data-svc', 'd');
    const context = fakeManager({
      ensureConnected: async (name: string) => { connectedWith = name; },
      getToolsForServer: () => [{ name: 'alpha' }, { name: 'beta' }],
    });
    const res = await tool.execute!({}, context);
    assert.equal(res.isError, undefined);
    assert.equal(connectedWith, 'data-svc');
    assert.match(res.output, /Connected to MCP server 'data-svc'\. 2 tools available: alpha, beta/);
  });

  it('reports a connection failure from an Error (uses .message)', async () => {
    const tool = createMcpConnectTool('bad', 'd');
    const context = fakeManager({
      ensureConnected: async () => { throw new Error('image pull denied'); },
      getToolsForServer: () => [],
    });
    const res = await tool.execute!({}, context);
    assert.equal(res.isError, true);
    assert.match(res.output, /Failed to connect to MCP server 'bad': image pull denied/);
  });

  it('reports a connection failure from a non-Error throw (uses String)', async () => {
    const tool = createMcpConnectTool('bad', 'd');
    const context = fakeManager({
      ensureConnected: async () => { throw 'raw-string-failure'; },
      getToolsForServer: () => [],
    });
    const res = await tool.execute!({}, context);
    assert.equal(res.isError, true);
    assert.match(res.output, /Failed to connect to MCP server 'bad': raw-string-failure/);
  });
});

// ===========================================================================
// mcp-resources.ts — listMcpResourcesTool
// ===========================================================================

describe('listMcpResourcesTool', () => {
  it('errors when the MCP manager is not initialized', async () => {
    const res = await listMcpResourcesTool.execute!({}, ctx({ _mcpManager: undefined }));
    assert.equal(res.isError, true);
    assert.match(res.output, /MCP not initialized/);
  });

  it('reports no resources when the manager returns undefined', async () => {
    const context = fakeManager({ listResources: async () => undefined });
    const res = await listMcpResourcesTool.execute!({}, context);
    assert.equal(res.isError, undefined);
    assert.equal(res.output, 'No MCP resources available.');
  });

  it('reports no resources when the manager returns an empty array', async () => {
    const context = fakeManager({ listResources: async () => [] });
    const res = await listMcpResourcesTool.execute!({}, context);
    assert.equal(res.output, 'No MCP resources available.');
  });

  it('formats resources and passes the server filter through', async () => {
    let filter: string | undefined = 'UNSET';
    const context = fakeManager({
      listResources: async (server?: string) => {
        filter = server;
        return [
          { uri: 'file://a.txt', name: 'A', mimeType: 'text/plain' },
          { uri: 'file://b.bin' }, // missing name + mimeType → defaults
        ];
      },
    });
    const res = await listMcpResourcesTool.execute!({ server: 'srv1' }, context);
    assert.equal(filter, 'srv1');
    assert.match(res.output, /file:\/\/a\.txt — A \(text\/plain\)/);
    // Missing name -> '', missing mimeType -> 'unknown'
    assert.match(res.output, /file:\/\/b\.bin —  \(unknown\)/);
  });

  it('defaults the server filter to undefined when omitted', async () => {
    let filter: string | undefined = 'UNSET';
    const context = fakeManager({
      listResources: async (server?: string) => { filter = server; return []; },
    });
    await listMcpResourcesTool.execute!({}, context);
    assert.equal(filter, undefined);
  });

  it('reports an Error from listResources (uses .message)', async () => {
    const context = fakeManager({ listResources: async () => { throw new Error('boom-list'); } });
    const res = await listMcpResourcesTool.execute!({}, context);
    assert.equal(res.isError, true);
    assert.match(res.output, /Error listing MCP resources: boom-list/);
  });

  it('reports a non-Error throw from listResources (uses String)', async () => {
    const context = fakeManager({ listResources: async () => { throw 42; } });
    const res = await listMcpResourcesTool.execute!({}, context);
    assert.equal(res.isError, true);
    assert.match(res.output, /Error listing MCP resources: 42/);
  });
});

// ===========================================================================
// mcp-resources.ts — readMcpResourceTool
// ===========================================================================

describe('readMcpResourceTool', () => {
  it('errors when the MCP manager is not initialized', async () => {
    const res = await readMcpResourceTool.execute!(
      { server: 's', uri: 'u' },
      ctx({ _mcpManager: undefined }),
    );
    assert.equal(res.isError, true);
    assert.match(res.output, /MCP not initialized/);
  });

  it('returns a string result verbatim (string branch)', async () => {
    let args: unknown[] = [];
    const context = fakeManager({
      readResource: async (...a: unknown[]) => { args = a; return 'plain text body'; },
    });
    const res = await readMcpResourceTool.execute!({ server: 'srvX', uri: 'res://1' }, context);
    assert.deepEqual(args.slice(0, 2), ['srvX', 'res://1']);
    assert.ok((args[2] as { signal?: unknown }).signal instanceof AbortSignal);
    assert.equal(res.output, 'plain text body');
  });

  it('JSON-stringifies a non-string result (object branch)', async () => {
    const context = fakeManager({
      readResource: async () => ({ hello: 'world', n: 3 }),
    });
    const res = await readMcpResourceTool.execute!({ server: 's', uri: 'u' }, context);
    assert.equal(res.output, JSON.stringify({ hello: 'world', n: 3 }));
  });

  it('spills an oversized string result to disk', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'si-mcp-read-'));
    try {
      const big = 'x'.repeat(100_001);
      const context = fakeManager({ readResource: async () => big });
      const res = await readMcpResourceTool.execute!(
        { server: 's', uri: 'u' },
        ctx({ cwd: tmp, _mcpManager: (context as ToolContext)._mcpManager }),
      );
      assert.ok(res.output.length < big.length, 'output should be truncated');
      assert.match(res.output, /persisted to/);
      const spillDir = path.join(tmp, '.superinference', 'tool-results');
      assert.ok(fs.readdirSync(spillDir).length >= 1, 'a spill file should exist');
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('reports an Error from readResource (uses .message)', async () => {
    const context = fakeManager({ readResource: async () => { throw new Error('boom-read'); } });
    const res = await readMcpResourceTool.execute!({ server: 's', uri: 'u' }, context);
    assert.equal(res.isError, true);
    assert.match(res.output, /Error reading MCP resource: boom-read/);
  });

  it('reports a non-Error throw from readResource (uses String)', async () => {
    const context = fakeManager({ readResource: async () => { throw { code: 'X' }; } });
    const res = await readMcpResourceTool.execute!({ server: 's', uri: 'u' }, context);
    assert.equal(res.isError, true);
    assert.match(res.output, /Error reading MCP resource:/);
  });
});

// ===========================================================================
// lsp-diagnostics.ts — lspDiagnosticsTool
// ===========================================================================

describe('lspDiagnosticsTool', () => {
  it('returns a file-specific message when file_path is provided', async () => {
    const res = await lspDiagnosticsTool.execute!({ file_path: '/w/foo.ts' }, ctx());
    assert.equal(res.isError, undefined);
    assert.match(res.output, /\[Diagnostics for \/w\/foo\.ts\]/);
  });

  it('returns a workspace message when no file_path is provided', async () => {
    const res = await lspDiagnosticsTool.execute!({}, ctx());
    assert.equal(res.isError, undefined);
    assert.match(res.output, /\[Workspace diagnostics\]/);
  });

  it('falls back gracefully when LSP client acquisition throws (catch branch)', async () => {
    mock.method(_lspDiagHooks, 'getLSPClient', () => { throw new Error('no lsp'); });
    const res = await lspDiagnosticsTool.execute!({ file_path: '/w/foo.ts' }, ctx());
    assert.equal(res.output, 'LSP not available. Run the project linter/compiler manually.');
    assert.equal(res.isError, undefined);
  });

  it('exercises the real (default) getLSPClient hook without throwing', async () => {
    // No mock: drives the default lazy-require implementation.
    const res = await lspDiagnosticsTool.execute!({}, ctx());
    assert.ok(res.output.length > 0);
  });
});

// ===========================================================================
// lsp-diagnostics.ts — lspReferencesTool
// ===========================================================================

describe('lspReferencesTool', () => {
  it('formats found references (results + map branch)', async () => {
    mock.method(_lspDiagHooks, 'searchSymbols', () => [
      { name: 'MyClass', type: 'class', filePath: '/w/a.ts', line: 10 },
      { name: 'myFn', type: 'function', filePath: '/w/b.ts', line: 3 },
    ]);
    const res = await lspReferencesTool.execute!({ symbol: 'My' }, ctx());
    assert.equal(res.isError, undefined);
    assert.match(res.output, /References for "My":/);
    assert.match(res.output, /MyClass \(class\) in \/w\/a\.ts:10/);
    assert.match(res.output, /myFn \(function\) in \/w\/b\.ts:3/);
  });

  it('reports no references when the indexer finds nothing', async () => {
    mock.method(_lspDiagHooks, 'searchSymbols', () => []);
    const res = await lspReferencesTool.execute!({ symbol: 'nope' }, ctx());
    assert.equal(res.isError, undefined);
    assert.equal(res.output, 'No references found for "nope".');
  });

  it('falls back to grep guidance when the indexer throws (catch branch)', async () => {
    mock.method(_lspDiagHooks, 'searchSymbols', () => { throw new Error('index boom'); });
    const res = await lspReferencesTool.execute!({ symbol: 'boomSym' }, ctx());
    assert.equal(res.isError, true);
    assert.equal(res.output, 'Error searching for "boomSym". Use grep as fallback.');
  });

  it('exercises the real (default) searchSymbols hook over a temp workspace', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'si-lsp-ref-'));
    try {
      fs.writeFileSync(path.join(tmp, 'package.json'), '{}');
      fs.writeFileSync(path.join(tmp, 'x.ts'), 'export function zzUnique() { return 1; }\n');
      // No mock: drives the default getWorkspaceIndexer path. The index builds
      // asynchronously, so results may be empty at call time — either branch is
      // valid; we only assert the default path executes without throwing.
      const res = await lspReferencesTool.execute!({ symbol: 'zzUnique' }, ctx({ cwd: tmp }));
      assert.ok(res.output.includes('zzUnique'));
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });
});
