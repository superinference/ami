/**
 * Parity tests (U103) for the `authenticate` tool's `force` re-login option and
 * the manager's {@link McpManager.forgetOAuthToken} that backs it.
 *
 * Claude Code's `mcp xaa login --force` (xaaIdpCommand.ts:L158-L196) ignores any
 * cached id_token and re-logs in — useful after a server-side revocation — and,
 * absent `--force`, short-circuits when a valid token is cached. AMI previously
 * re-authenticated only reactively on a 401 and the `authenticate` tool just
 * reconnected (reusing a still-valid cached token), so there was no user/model
 * -facing way to proactively discard a cached credential. These tests lock the
 * new behavior:
 *   - `force:true` clears the persisted OAuth token BEFORE (re)connecting, so a
 *     still-valid cached credential is discarded and a fresh flow runs.
 *   - without `force`, the already-connected no-op is reported (and hints at
 *     `force` to re-login) — never silently re-authenticating.
 *   - `forgetOAuthToken` only touches OAuth http/sse servers, reports whether a
 *     credential existed, and is a no-op for stdio/non-OAuth servers.
 *
 * RED on revert: a mock manager asserts the tool calls `forgetOAuthToken` ONLY
 * when `force` is set and BEFORE the reconnect — reverting the tool change makes
 * the call count / ordering assertions fail.
 */
import { describe, it, mock } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
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

describe('mcp-auth tool — force re-login', () => {
  it('clears the cached credential (forgetOAuthToken) BEFORE reconnect when force:true', async () => {
    const tool = createMcpAuthTool('revoked', { transport: 'http' });
    const calls: string[] = [];
    const r = await tool.execute({ force: true }, ctx({
      _mcpManager: {
        forgetOAuthToken: (n: string) => { calls.push(`forget:${n}`); return true; },
        reconnect: async (n: string) => { calls.push(`reconnect:${n}`); },
        // No startAuth → falls back to the blocking reconnect path.
        getServerStatus: () => ({ state: 'ready' }),
      },
    } as Partial<ToolContext>));
    assert.ok(!r.isError);
    assert.deepEqual(calls, ['forget:revoked', 'reconnect:revoked'], 'credential is discarded BEFORE the reconnect');
    assert.equal(r.metadata?.status, 'ok');
    assert.equal(r.metadata?.forced, true, 'forced flag surfaced for introspection');
    assert.match(r.output, /Re-authentication completed/);
  });

  it('does NOT clear the cached credential when force is absent (plain reconnect)', async () => {
    const tool = createMcpAuthTool('cached', { transport: 'http' });
    let forgetCalls = 0;
    const r = await tool.execute({}, ctx({
      _mcpManager: {
        forgetOAuthToken: () => { forgetCalls++; return true; },
        reconnect: async () => {},
        getServerStatus: () => ({ state: 'ready' }),
      },
    } as Partial<ToolContext>));
    assert.equal(forgetCalls, 0, 'a cached, valid token is NOT discarded without force');
    assert.equal(r.metadata?.status, 'ok');
    assert.equal(r.metadata?.forced, undefined, 'forced flag absent on a non-forced call');
    assert.match(r.output, /already connected/);
    assert.match(r.output, /force/, 'the no-op output hints at forcing a fresh login');
  });

  it('forwards force through the startAuth active-flow path too', async () => {
    const tool = createMcpAuthTool('active', { transport: 'http' });
    const url = 'https://as.example.com/authorize?client_id=x&state=y';
    const calls: string[] = [];
    const r = await tool.execute({ force: true }, ctx({
      _mcpManager: {
        forgetOAuthToken: (n: string) => { calls.push(`forget:${n}`); return true; },
        startAuth: async (n: string) => { calls.push(`startAuth:${n}`); return { authUrl: url }; },
        getServerStatus: () => ({ state: 'connecting' }),
      },
    } as Partial<ToolContext>));
    assert.deepEqual(calls, ['forget:active', 'startAuth:active'], 'force clears before startAuth');
    assert.equal(r.metadata?.status, 'auth_url');
    assert.equal(r.metadata?.forced, true);
  });

  it('treats only force===true as forcing (a non-boolean/false value is a plain reconnect)', async () => {
    const tool = createMcpAuthTool('srv', { transport: 'http' });
    let forgetCalls = 0;
    for (const bad of [false, 'true', 1, undefined]) {
      forgetCalls = 0;
      await tool.execute({ force: bad } as Record<string, unknown>, ctx({
        _mcpManager: {
          forgetOAuthToken: () => { forgetCalls++; return true; },
          reconnect: async () => {},
          getServerStatus: () => ({ state: 'ready' }),
        },
      } as Partial<ToolContext>));
      assert.equal(forgetCalls, 0, `force=${String(bad)} must NOT discard the credential`);
    }
  });

  it('advertises the force option in the tool input schema', () => {
    const tool = createMcpAuthTool('srv', { transport: 'http' });
    const props = (tool.inputSchema as { properties?: Record<string, unknown> }).properties ?? {};
    assert.ok('force' in props, 'force is an input property so the model can request a re-login');
    assert.equal((props.force as { type?: string }).type, 'boolean');
  });
});

describe('McpManager.forgetOAuthToken', () => {
  function mgrWithConfig(cfg: Record<string, unknown>): { mgr: McpManager; dir: string } {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-mcp-forget-'));
    const mgr = new McpManager();
    // Seed the private config map the method reads from.
    (mgr as unknown as { configs: Map<string, unknown> }).configs.set('s', cfg);
    return { mgr, dir };
  }

  it('reports true and removes the persisted token file for an OAuth http server', () => {
    const { mgr } = mgrWithConfig({ transport: 'http', url: 'https://srv.example.com', oauth: {} });
    const filePath = (mgr as unknown as { mcpTokenFilePath: (n: string, c: unknown) => string })
      .mcpTokenFilePath('s', { transport: 'http', url: 'https://srv.example.com', oauth: {} });
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    fs.writeFileSync(filePath, JSON.stringify({ accessToken: 'tok', tokenType: 'Bearer' }));
    assert.equal(fs.existsSync(filePath), true);
    const cleared = mgr.forgetOAuthToken('s');
    assert.equal(cleared, true, 'reports that a persisted credential existed');
    assert.equal(fs.existsSync(filePath), false, 'the token file is purged');
  });

  it('reports false (no-op) when no credential is persisted yet', () => {
    const { mgr } = mgrWithConfig({ transport: 'sse', url: 'https://srv.example.com', oauth: {} });
    assert.equal(mgr.forgetOAuthToken('s'), false);
  });

  it('is a no-op for a stdio / non-OAuth server', () => {
    const mgr = new McpManager();
    (mgr as unknown as { configs: Map<string, unknown> }).configs.set('s', { transport: 'stdio', command: 'x' });
    assert.equal(mgr.forgetOAuthToken('s'), false);
    assert.equal(mgr.forgetOAuthToken('unknown'), false, 'unknown server is a no-op too');
  });

  it('drops the needs-auth skip cache so a forced re-auth re-probes', () => {
    const { mgr } = mgrWithConfig({ transport: 'http', url: 'https://srv.example.com', oauth: {} });
    const clearSpy = mock.method(mgr, 'clearNeedsAuthCache', () => {});
    mgr.forgetOAuthToken('s');
    assert.equal(clearSpy.mock.callCount(), 1);
    assert.equal(clearSpy.mock.calls[0].arguments[0], 's');
  });
});
