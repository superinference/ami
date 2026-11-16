/**
 * Parity tests for the MCP `authenticate` pseudo-tool's authorization-URL
 * surfacing, structured status enum, tri-state reconnect reporting, and the
 * `/mcp` recovery affordance (src/tools/mcp-auth.ts) — additive parity with
 * Claude Code's McpAuthTool (McpAuthOutput.status: 'auth_url'|'unsupported'|'error').
 *
 * Each test is a RED-GREEN reproducer: it discriminates the hardened behavior
 * from the pre-fix behavior (it fails if the corresponding change is reverted).
 *
 * Gaps covered (confirmed, mcp-auth.ts):
 *   - Structured output schema with status enum incl. `auth_url` + authUrl field.
 *   - Tri-state post-authentication reconnect reporting (connected/needs-auth/failed).
 *   - Distinct completion message for silent (cached-token) auth vs no-op.
 *   - Failure/unsupported message points to the actionable `/mcp` recovery affordance.
 *   - Rich description noting the tool returns an authorization URL to share.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createMcpAuthTool, MCP_AUTH_STATUS } from '../src/tools/mcp-auth';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

describe('mcp-auth — structured status enum includes auth_url', () => {
  it('MCP_AUTH_STATUS carries the auth_url discriminator', () => {
    // RED on revert: enum was ['ok','unsupported','error'] with no auth_url.
    assert.ok(MCP_AUTH_STATUS.includes('auth_url'));
    for (const s of ['ok', 'auth_url', 'unsupported', 'error']) {
      assert.ok((MCP_AUTH_STATUS as readonly string[]).includes(s));
    }
  });
});

describe('mcp-auth — tri-state post-reconnect reporting', () => {
  it('returns status "auth_url" (not a false "ok") when reconnect leaves the server needing consent', async () => {
    const tool = createMcpAuthTool('needs-consent', { transport: 'http' });
    const authUrl =
      'https://as.example.com/authorize?client_id=x&state=y&response_type=code';
    const r = await tool.execute({}, ctx({
      _mcpManager: {
        // reconnect resolves without throwing, but the server is still not ready
        // because the interactive browser leg has not completed.
        getServerStatus: () => ({ state: 'disconnected' }),
        reconnect: async () => {},
        getPendingAuthUrl: () => authUrl,
      },
    } as Partial<ToolContext>));
    // RED on revert: old code reported status 'ok' with "already connected".
    assert.ok(!r.isError, 'auth_url is informational, not an error');
    assert.equal(r.metadata?.status, 'auth_url');
    assert.equal(r.metadata?.auth_url, authUrl);
    assert.ok(r.output.includes(authUrl), 'authorization URL is relayed to the model');
    assert.ok(!r.output.includes('already connected'));
  });

  it('redacts credential-bearing params from the surfaced URL on the needs-consent path', async () => {
    const tool = createMcpAuthTool('leaky-consent', { transport: 'sse' });
    const leaky =
      'https://as.example.com/cb?access_token=supersecrettokenvalue1234567890&state=z';
    const r = await tool.execute({}, ctx({
      _mcpManager: {
        getServerStatus: () => ({ state: 'disconnected' }),
        reconnect: async () => {},
        getPendingAuthUrl: () => leaky,
      },
    } as Partial<ToolContext>));
    assert.equal(r.metadata?.status, 'auth_url');
    assert.ok(!r.output.includes('supersecrettokenvalue1234567890'), 'token must not leak');
    assert.ok(!(r.metadata!.auth_url as string).includes('supersecrettokenvalue1234567890'));
    assert.ok((r.metadata!.auth_url as string).includes('state=z'));
  });

  it('reports a successful silent (cached-token) reconnect as status "ok" reconnected', async () => {
    const tool = createMcpAuthTool('silent-ok', { transport: 'http' });
    let connected = false;
    const r = await tool.execute({}, ctx({
      _mcpManager: {
        getServerStatus: () => ({ state: connected ? 'ready' : 'disconnected' }),
        reconnect: async () => { connected = true; },
        // No pending URL: auth completed without needing interactive consent.
        getPendingAuthUrl: () => undefined,
      },
    } as Partial<ToolContext>));
    assert.ok(!r.isError);
    assert.equal(r.metadata?.status, 'ok');
    assert.equal(r.metadata?.reconnected, true);
    assert.ok(r.output.includes('Authentication completed'));
  });
});

describe('mcp-auth — /mcp recovery affordance', () => {
  it('unsupported (stdio) message points at /mcp', async () => {
    const tool = createMcpAuthTool('stdio-srv', { transport: 'stdio' });
    const r = await tool.execute({}, ctx({ _mcpManager: { reconnect: async () => {} } } as Partial<ToolContext>));
    assert.equal(r.metadata?.status, 'unsupported');
    // RED on revert: old message only mentioned .ami/mcp.json, never /mcp.
    assert.ok(r.output.includes('/mcp'));
  });

  it('failure message points at /mcp', async () => {
    const tool = createMcpAuthTool('fail-srv', { transport: 'http' });
    const r = await tool.execute({}, ctx({
      _mcpManager: {
        getServerStatus: () => ({ state: 'disconnected' }),
        reconnect: async () => { throw new Error('boom'); },
        getPendingAuthUrl: () => undefined,
      },
    } as Partial<ToolContext>));
    assert.ok(r.isError);
    assert.ok(r.output.includes('/mcp'));
  });
});

describe('mcp-auth — description advertises the auth_url affordance', () => {
  it('description explains it returns an authorization URL to share in headless runs', () => {
    const tool = createMcpAuthTool('desc-srv', { transport: 'http' });
    assert.ok(tool.description.includes('auth_url'));
    assert.ok(/authorization URL/i.test(tool.description));
  });
});
