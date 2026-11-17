/**
 * U018 (w83) — pre-authentication for lazy-connect MCP servers.
 *
 * Parity gap (MCPAgentServerMenu.tsx:L56-81): agent/skill-scoped http/sse OAuth
 * servers only connect when the stance that declares them is pinned, so their
 * first tool call pays the full interactive OAuth consent cost mid-run. Claude's
 * "Authenticate" menu action builds a temp OAuth config {type, url} and runs
 * `performMCPOAuthFlow` WITHOUT connecting the server, caching the credential
 * before first use. AMI previously could only authenticate by (re)connecting a
 * live server (reconnect → ensureConnected).
 *
 * `McpManager.authenticate(name)` adds that pre-auth entry point: it drives the
 * SAME McpOAuthSession as a live connect (via the shared `buildOAuthProvider`
 * helper) but resolves WITHOUT spawning the transport or discovering tools.
 *
 * RED on revert: before `authenticate` existed the method call throws
 * `authenticate is not a function`; the success path proves a token is cached
 * with the server still in the un-connected `pending` state.
 *
 * All network I/O is through the injected `_fetch` seam — pure and fast.
 */
import { describe, it, before, after, beforeEach, afterEach, mock } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { McpManager } from '../src/mcp/manager';
import type { FetchLike } from '../src/mcp/oauth';
import type { McpServerConfig } from '../src/mcp/manager';

const SERVER = 'https://rs.example/mcp';
const PRM_URL = 'https://rs.example/.well-known/oauth-protected-resource/mcp';
const AS_META = 'https://as.example/.well-known/oauth-authorization-server';

function fullRoutes(extra: Record<string, { ok?: boolean; status?: number; body: unknown }> = {}) {
  return {
    [PRM_URL]: { body: { resource: SERVER, authorization_servers: ['https://as.example'], scopes_supported: ['read'] } },
    [AS_META]: { body: { issuer: 'https://as.example', authorization_endpoint: 'https://as.example/authorize', token_endpoint: 'https://as.example/token', registration_endpoint: 'https://as.example/register' } },
    'https://as.example/register': { body: { client_id: 'dyn-cid' } },
    'https://as.example/token': { body: { access_token: 'AT', token_type: 'Bearer', refresh_token: 'RT', expires_in: 3600 } },
    ...extra,
  };
}

function fakeFetch(routes: Record<string, { ok?: boolean; status?: number; body: unknown }>): { fn: FetchLike; calls: string[] } {
  const calls: string[] = [];
  const fn: FetchLike = async (url) => {
    calls.push(String(url));
    const r = routes[String(url)];
    if (!r) return { ok: false, status: 404, json: async () => ({}) } as Awaited<ReturnType<FetchLike>>;
    return { ok: r.ok ?? true, status: r.status ?? 200, json: async () => r.body } as Awaited<ReturnType<FetchLike>>;
  };
  return { fn, calls };
}

const oauthHttpConfig = (): McpServerConfig => ({
  command: '',
  transport: 'http',
  url: SERVER,
  oauth: {},
});

describe('McpManager.authenticate — pre-auth for lazy-connect OAuth servers', () => {
  let tmp: string;
  let prevDataDir: string | undefined;

  before(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'u018-w83-preauth-'));
    prevDataDir = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = tmp;
  });
  after(() => {
    if (prevDataDir === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
    else process.env.SUPERINFERENCE_DATA_DIR = prevDataDir;
    fs.rmSync(tmp, { recursive: true, force: true });
  });

  it('rejects an unknown server with the valid-name guidance error', async () => {
    const mgr = new McpManager();
    await assert.rejects(() => mgr.authenticate('nope'), /Unknown MCP server "nope"/);
  });

  it('rejects a stdio (non-OAuth) server — nothing to pre-authenticate', async () => {
    const mgr = new McpManager();
    mgr.addPendingServer('local-stdio', { command: 'node', args: ['server.js'] });
    await assert.rejects(() => mgr.authenticate('local-stdio'), /not an OAuth http\/sse server/);
  });

  it('rejects an http server that has no oauth block', async () => {
    const mgr = new McpManager();
    mgr.addPendingServer('remote-noauth', { command: '', transport: 'http', url: SERVER });
    await assert.rejects(() => mgr.authenticate('remote-noauth'), /not an OAuth http\/sse server/);
  });

  it('rejects immediately when the signal is already aborted (before building the provider)', async () => {
    const mgr = new McpManager();
    const spy = mock.method(mgr as unknown as { buildOAuthProvider: () => unknown }, 'buildOAuthProvider', () => undefined);
    mgr.addPendingServer('srv', oauthHttpConfig());
    const ac = new AbortController();
    ac.abort();
    await assert.rejects(() => mgr.authenticate('srv', { signal: ac.signal }), /aborted before it started/);
    assert.equal(spy.mock.callCount(), 0, 'no provider is built once pre-aborted');
  });

  it('runs the full OAuth flow and caches a credential WITHOUT connecting the server (E2E)', async () => {
    const mgr = new McpManager();
    const { fn, calls } = fakeFetch(fullRoutes());
    mgr._fetch = fn;
    let authorizedUrl = '';
    mgr.setOAuthAuthorizer(async ({ authorizationUrl }) => { authorizedUrl = authorizationUrl; return { code: 'the-code' }; });

    mgr.addPendingServer('agent-srv', oauthHttpConfig());
    assert.equal(mgr.getServerState('agent-srv'), 'pending', 'declared but not connected');

    const urls: Array<{ name: string; url: string }> = [];
    mgr.on('oauth-authorization-url', (name, url) => urls.push({ name, url }));

    const res = await mgr.authenticate('agent-srv');
    assert.deepEqual(res, { authenticated: true });

    // The consent leg ran through the SAME manager path as a live connect.
    assert.match(authorizedUrl, /code_challenge=/);
    assert.deepEqual(urls, [{ name: 'agent-srv', url: authorizedUrl }]);
    assert.equal(mgr.getPendingAuthUrl('agent-srv'), authorizedUrl);
    // Dynamic client registration happened (no clientId supplied).
    assert.ok(calls.includes('https://as.example/register'));
    assert.ok(calls.includes('https://as.example/token'));

    // Crucially: the transport was NEVER spawned — the server is still pending,
    // not `ready`/`connecting`. (RED signal for a connect-based implementation.)
    assert.equal(mgr.getServerState('agent-srv'), 'pending', 'no transport spawned by pre-auth');
  });

  it('maps a no-auth-required flow to { authenticated: false }', async () => {
    const mgr = new McpManager();
    mock.method(
      mgr as unknown as { buildOAuthProvider: () => unknown },
      'buildOAuthProvider',
      () => ({ onUnauthorized: async () => false, authorizationHeader: async () => null }),
    );
    mgr.addPendingServer('silent', oauthHttpConfig());
    const res = await mgr.authenticate('silent');
    assert.deepEqual(res, { authenticated: false });
  });

  it('aborts the in-flight OAuth flow when the signal fires mid-flight', async () => {
    const mgr = new McpManager();
    // Discovery fetch hangs until the per-request abort signal fires, so cancelling
    // the external signal must propagate through the per-server oauth controller.
    const hangingFetch: FetchLike = (_url, init) =>
      new Promise((_resolve, reject) => {
        const sig = init?.signal;
        if (sig?.aborted) { reject(new Error('fetch aborted')); return; }
        sig?.addEventListener('abort', () => reject(new Error('fetch aborted')), { once: true });
      });
    mgr._fetch = hangingFetch;
    mgr.addPendingServer('cancel-srv', oauthHttpConfig());

    const ac = new AbortController();
    const p = mgr.authenticate('cancel-srv', { signal: ac.signal });
    queueMicrotask(() => ac.abort());
    await assert.rejects(() => p, /aborted/);
    assert.equal(mgr.getServerState('cancel-srv'), 'pending', 'still never connected after a cancelled pre-auth');
  });
});
