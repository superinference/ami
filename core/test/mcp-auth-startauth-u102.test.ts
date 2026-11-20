/**
 * Parity tests (U102) for the NON-BLOCKING, model-driven OAuth flow:
 *   - McpManager.startAuth races the consent URL (emitted via the
 *     'oauth-authorization-url' event) against silent flow completion, returns
 *     the URL immediately WITHOUT waiting for the background reconnect, and
 *     honors an AbortSignal.
 *   - The `authenticate` tool prefers startAuth and surfaces the URL inline
 *     (status "auth_url") the moment it is available — mirroring the reference's
 *     skipBrowserOpen McpAuthTool (start → capture URL → return → reconnect in
 *     background).
 *
 * Each test discriminates the new behavior from the pre-fix blocking path
 * (RED on revert): e.g. a mock whose reconnect never resolves would HANG the
 * old `await reconnect()` tool path, but startAuth returns as soon as the URL
 * event fires.
 */
import { describe, it, mock } from 'node:test';
import assert from 'node:assert/strict';
import { McpManager } from '../src/mcp/manager';
import { createMcpAuthTool } from '../src/tools/mcp-auth';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

const never = () => new Promise<void>(() => {});

describe('McpManager.startAuth — non-blocking race', () => {
  it('returns the authorization URL as soon as the consent leg emits it, without waiting for reconnect', async () => {
    const mgr = new McpManager();
    const url = 'https://as.example.com/authorize?client_id=x&state=y&response_type=code';
    // reconnect emits the URL mid-flow then BLOCKS forever on the browser leg.
    mock.method(mgr, 'reconnect', async (name: string) => {
      mgr.emit('oauth-authorization-url', name, url);
      await never();
    });
    const res = await mgr.startAuth('srv');
    assert.equal(res.authUrl, url, 'URL won the race against the still-pending reconnect');
  });

  it('resolves silently (no URL) when the flow completes without a consent leg', async () => {
    const mgr = new McpManager();
    mock.method(mgr, 'reconnect', async () => { /* cached-token: completes, no URL */ });
    const res = await mgr.startAuth('silent');
    assert.equal(res.authUrl, undefined);
  });

  it('propagates a pre-URL reconnect failure', async () => {
    const mgr = new McpManager();
    mock.method(mgr, 'reconnect', async () => { throw new Error('discovery failed'); });
    await assert.rejects(() => mgr.startAuth('boom'), /discovery failed/);
  });

  it('rejects immediately when the signal is already aborted', async () => {
    const mgr = new McpManager();
    const reconnectSpy = mock.method(mgr, 'reconnect', async () => {});
    const ac = new AbortController();
    ac.abort();
    await assert.rejects(() => mgr.startAuth('srv', { signal: ac.signal }), /aborted before it started/);
    assert.equal(reconnectSpy.mock.callCount(), 0, 'no reconnect is started once pre-aborted');
  });

  it('rejects when the signal fires mid-flight (in-flight OAuth cancellation)', async () => {
    const mgr = new McpManager();
    mock.method(mgr, 'reconnect', async () => { await never(); });
    const ac = new AbortController();
    const p = mgr.startAuth('srv', { signal: ac.signal });
    queueMicrotask(() => ac.abort());
    await assert.rejects(() => p, /was aborted/);
  });
});

describe('mcp-auth tool — prefers startAuth active flow', () => {
  it('surfaces status "auth_url" immediately even while reconnect is still pending', async () => {
    const tool = createMcpAuthTool('active', { transport: 'http' });
    const url = 'https://as.example.com/authorize?client_id=x&state=y';
    let reconnectCalled = false;
    const r = await tool.execute({}, ctx({
      _mcpManager: {
        // If the tool fell back to the blocking reconnect path this would hang.
        reconnect: async () => { reconnectCalled = true; await never(); },
        startAuth: async () => ({ authUrl: url }),
        getServerStatus: () => ({ state: 'connecting' }),
      },
    } as Partial<ToolContext>));
    assert.ok(!r.isError, 'auth_url is informational');
    assert.equal(r.metadata?.status, 'auth_url');
    assert.equal(r.metadata?.auth_url, url);
    assert.ok(r.output.includes(url));
    assert.equal(reconnectCalled, false, 'startAuth path is used, not blocking reconnect');
  });

  it('redacts credential params from the startAuth-surfaced URL', async () => {
    const tool = createMcpAuthTool('leaky', { transport: 'http' });
    const leaky = 'https://as.example.com/cb?code=liveauthorizationcode123456&state=z';
    const r = await tool.execute({}, ctx({
      _mcpManager: {
        startAuth: async () => ({ authUrl: leaky }),
        getServerStatus: () => ({ state: 'connecting' }),
      },
    } as Partial<ToolContext>));
    assert.equal(r.metadata?.status, 'auth_url');
    assert.ok(!r.output.includes('liveauthorizationcode123456'), 'authorization code must not leak');
    assert.ok(!(r.metadata!.auth_url as string).includes('liveauthorizationcode123456'));
    assert.ok((r.metadata!.auth_url as string).includes('state=z'));
  });

  it('passes context.abortSignal through to startAuth', async () => {
    const tool = createMcpAuthTool('abortable', { transport: 'http' });
    const ac = new AbortController();
    let seen: AbortSignal | undefined;
    await tool.execute({}, ctx({
      abortSignal: ac.signal,
      _mcpManager: {
        startAuth: async (_n: string, opts?: { signal?: AbortSignal }) => { seen = opts?.signal; return {}; },
        getServerStatus: () => ({ state: 'ready' }),
      },
    } as Partial<ToolContext>));
    assert.equal(seen, ac.signal, 'the tool forwards its abort signal to the OAuth flow');
  });

  it('reports a silent (cached-token) completion as status "ok" reconnected', async () => {
    const tool = createMcpAuthTool('silent-ok', { transport: 'http' });
    let state = 'disconnected';
    const r = await tool.execute({}, ctx({
      _mcpManager: {
        startAuth: async () => { state = 'ready'; return {}; },
        getServerStatus: () => ({ state }),
      },
    } as Partial<ToolContext>));
    assert.ok(!r.isError);
    assert.equal(r.metadata?.status, 'ok');
    assert.equal(r.metadata?.reconnected, true);
  });

  it('surfaces a startAuth failure as status "error"', async () => {
    const tool = createMcpAuthTool('fail', { transport: 'http' });
    const r = await tool.execute({}, ctx({
      _mcpManager: {
        startAuth: async () => { throw new Error('token endpoint 500'); },
        getServerStatus: () => ({ state: 'disconnected' }),
      },
    } as Partial<ToolContext>));
    assert.ok(r.isError);
    assert.equal(r.metadata?.status, 'error');
    assert.ok(r.output.includes('/mcp'));
  });
});
