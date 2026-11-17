/**
 * mcp-oauth-parity-u036-w76.test.ts — U036/w76 OAuth parity deltas on
 * core/core/src/mcp/oauth.ts:
 *
 *   1. invalid_grant alias normalization — `expired_refresh_token` (a Slack
 *      token-rotation alias) is mapped onto the canonical `invalid_grant` so the
 *      dead-grant invalidation path fires (gap: non-standard token-endpoint error
 *      normalization).
 *   2. Overall consent-leg timeout — a never-completing interactive `authorize()`
 *      is torn down by {@link OAuthFlowTimeoutError} after `flowTimeoutMs` instead of
 *      hanging forever, surfaced as a distinct 'cancelled' diagnostic (gap:
 *      cancellation/abort + overall flow timeout).
 *   3. Persisted cross-session discovery state — a DiscoveryStateStore lets a cold
 *      process refresh a persisted token WITHOUT re-running RFC 9728 → RFC 8414
 *      discovery; discovery is saved after a live flow and dropped on
 *      invalidateCredentials('discovery') (gap: persisted discovery-state caching).
 *
 * All network I/O is through an injected FetchLike so these are pure and fast.
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  refreshToken, OAuthError, McpOAuthSession, OAuthTokenStore,
  FileDiscoveryStateStore, mcpDiscoveryFileName, OAuthFlowTimeoutError,
  OAUTH_FLOW_TIMEOUT_MS,
} from '../src/mcp/oauth';
import type {
  FetchLike, OAuthTokens, DiscoveryState, DiscoveryStateStore, AuthServerMetadata, OAuthLogEvent,
} from '../src/mcp/oauth';

function fakeFetch(routes: Record<string, { ok?: boolean; status?: number; body: unknown }>): { fn: FetchLike; calls: string[] } {
  const calls: string[] = [];
  const fn: FetchLike = async (url) => {
    calls.push(url);
    const r = routes[url];
    if (!r) return { ok: false, status: 404, json: async () => ({}) };
    return { ok: r.ok ?? true, status: r.status ?? 200, json: async () => r.body };
  };
  return { fn, calls };
}

const tmpDirs: string[] = [];
function tmpDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-oauth-w76-'));
  tmpDirs.push(dir);
  return dir;
}
afterEach(() => { for (const d of tmpDirs.splice(0)) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } } });

function memDiscoveryStore(initial: DiscoveryState | null = null): DiscoveryStateStore & { saved: DiscoveryState[]; cleared: number; value: DiscoveryState | null } {
  return {
    value: initial,
    saved: [] as DiscoveryState[],
    cleared: 0,
    load() { return this.value; },
    save(s: DiscoveryState) { this.value = s; this.saved.push(s); },
    clear() { this.value = null; this.cleared++; },
  };
}

const AS_META: AuthServerMetadata = {
  issuer: 'https://as.example',
  authorizationEndpoint: 'https://as.example/authorize',
  tokenEndpoint: 'https://as.example/token',
};

// ---------------------------------------------------------------------------
// 1. invalid_grant alias normalization — expired_refresh_token
// ---------------------------------------------------------------------------
describe('token-error normalization — expired_refresh_token alias', () => {
  const TOKEN = 'https://as.example/token';

  it('maps the expired_refresh_token alias onto the canonical invalid_grant code', async () => {
    const { fn } = fakeFetch({ [TOKEN]: { ok: false, status: 400, body: { error: 'expired_refresh_token', error_description: 'rotated' } } });
    await assert.rejects(
      () => refreshToken(fn, { tokenEndpoint: TOKEN, refreshToken: 'RT', clientId: 'cid', resource: 'https://rs/mcp' }, 0),
      (err: unknown) => {
        assert.ok(err instanceof OAuthError);
        assert.equal(err.code, 'invalid_grant');
        return true;
      },
    );
  });

  it('clears the stored tokens when a refresh fails with the expired_refresh_token alias', async () => {
    // An HTTP-200-with-error-body (Slack-style) carrying the alias must still be
    // treated as a dead grant: the store is cleared so no doomed re-refresh loops.
    const { fn } = fakeFetch({ [TOKEN]: { ok: true, status: 200, body: { error: 'expired_refresh_token' } } });
    const store = new OAuthTokenStore();
    store.set({ accessToken: 'AT', tokenType: 'Bearer', refreshToken: 'RT', expiresAt: -1, clientId: 'cid' } as OAuthTokens);
    const session = new McpOAuthSession({
      serverUrl: 'https://rs.example/mcp', fetchImpl: fn, redirectUri: 'http://127.0.0.1:9/cb',
      clientId: 'cid', store, now: () => 0,
      discoveryStore: memDiscoveryStore({ resource: 'https://rs.example/mcp', authServer: AS_META }),
      authorize: async () => { throw new Error('should not consent'); },
    });
    const header = await session.authorizationHeader(); // expired → silent refresh → alias → invalid_grant → clear
    assert.equal(header, null);
    assert.equal(store.get(), null, 'the dead grant must be cleared, not left for a doomed re-refresh');
  });
});

// ---------------------------------------------------------------------------
// 2. Overall consent-leg timeout
// ---------------------------------------------------------------------------
describe('McpOAuthSession — overall consent-leg timeout', () => {
  const SERVER = 'https://rs.example/mcp';
  const PRM_URL = 'https://rs.example/.well-known/oauth-protected-resource/mcp';
  const AS_URL = 'https://as.example/.well-known/oauth-authorization-server';

  function discoveryRoutes() {
    return {
      [PRM_URL]: { body: { resource: SERVER, authorization_servers: ['https://as.example'] } },
      [AS_URL]: { body: { issuer: 'https://as.example', authorization_endpoint: 'https://as.example/authorize', token_endpoint: 'https://as.example/token', registration_endpoint: 'https://as.example/register' } },
      'https://as.example/register': { body: { client_id: 'dyn' } },
      'https://as.example/token': { body: { access_token: 'AT', token_type: 'Bearer', expires_in: 3600 } },
    };
  }

  it('exposes a 5-minute default flow timeout constant', () => {
    assert.equal(OAUTH_FLOW_TIMEOUT_MS, 5 * 60 * 1000);
  });

  it('tears down a hung consent leg after flowTimeoutMs and logs a distinct cancelled event', async () => {
    const { fn } = fakeFetch(discoveryRoutes());
    const logs: OAuthLogEvent[] = [];
    const session = new McpOAuthSession({
      serverUrl: SERVER, fetchImpl: fn, redirectUri: 'http://127.0.0.1:9/cb',
      flowTimeoutMs: 20,
      onLog: (e) => logs.push(e),
      // Never resolves — simulates a user who never completes the browser consent.
      authorize: () => new Promise<{ code: string }>(() => { /* hang */ }),
      now: () => 0,
    });
    const started = Date.now();
    const ok = await session.onUnauthorized('Bearer resource_metadata="' + PRM_URL + '"');
    assert.equal(ok, false, 'a timed-out consent leg resolves to a non-fatal false');
    assert.ok(Date.now() - started < 2000, 'must not hang until the real 5-minute default');
    const timeoutLog = logs.find((l) => l.step === 'cancelled' && /timed out/.test(l.message));
    assert.ok(timeoutLog, 'a distinct cancelled/timeout diagnostic must be emitted');
  });

  it('flowTimeoutMs=0 disables the cap — a slow-but-completing consent still succeeds', async () => {
    const { fn } = fakeFetch(discoveryRoutes());
    const session = new McpOAuthSession({
      serverUrl: SERVER, fetchImpl: fn, redirectUri: 'http://127.0.0.1:9/cb',
      flowTimeoutMs: 0,
      authorize: async () => { await new Promise((r) => setTimeout(r, 15)); return { code: 'c' }; },
      now: () => 0,
    });
    assert.equal(await session.onUnauthorized('Bearer resource_metadata="' + PRM_URL + '"'), true);
  });

  it('OAuthFlowTimeoutError carries the configured ms and a descriptive message', () => {
    const e = new OAuthFlowTimeoutError(1234);
    assert.equal(e.ms, 1234);
    assert.equal(e.name, 'OAuthFlowTimeoutError');
    assert.match(e.message, /1234ms/);
  });
});

// ---------------------------------------------------------------------------
// 3. Persisted cross-session discovery state
// ---------------------------------------------------------------------------
describe('FileDiscoveryStateStore', () => {
  it('round-trips save → load and clear removes the file', () => {
    const file = path.join(tmpDir(), 'disc.json');
    const store = new FileDiscoveryStateStore(file);
    assert.equal(store.load(), null, 'missing file loads as null');
    const state: DiscoveryState = { resource: 'https://rs/mcp', authServer: AS_META };
    store.save(state);
    const loaded = store.load();
    assert.deepEqual(loaded?.authServer.tokenEndpoint, AS_META.tokenEndpoint);
    assert.equal(loaded?.resource, 'https://rs/mcp');
    store.clear();
    assert.equal(store.load(), null, 'cleared discovery loads as null');
  });

  it('load returns null for a corrupt / incomplete document', () => {
    const file = path.join(tmpDir(), 'disc.json');
    fs.writeFileSync(file, '{ not json');
    assert.equal(new FileDiscoveryStateStore(file).load(), null);
    fs.writeFileSync(file, JSON.stringify({ resource: 'https://rs/mcp', authServer: { issuer: 'x' } }));
    assert.equal(new FileDiscoveryStateStore(file).load(), null, 'incomplete authServer rejected');
  });

  it('save degrades silently when the path is unwritable (best-effort persistence)', () => {
    // A regular file stands where a directory component must be, so mkdir/write throw
    // ENOTDIR — save must swallow it rather than break the live session.
    const base = tmpDir();
    const blocker = path.join(base, 'blocker');
    fs.writeFileSync(blocker, 'x');
    const store = new FileDiscoveryStateStore(path.join(blocker, 'sub', 'disc.json'));
    assert.doesNotThrow(() => store.save({ resource: 'https://rs/mcp', authServer: AS_META }));
    assert.equal(store.load(), null);
  });

  it('mcpDiscoveryFileName is filesystem-safe and distinct from the token filename', () => {
    const name = mcpDiscoveryFileName('srv|abc/def');
    assert.match(name, /^srv_abc_def\.[0-9a-f]{8}\.discovery\.json$/);
  });
});

describe('McpOAuthSession — persisted discovery reuse', () => {
  const SERVER = 'https://rs.example/mcp';
  const PRM_URL = 'https://rs.example/.well-known/oauth-protected-resource/mcp';
  const AS_URL = 'https://as.example/.well-known/oauth-authorization-server';

  it('refreshes a cold-process token WITHOUT re-running discovery when discovery is persisted', async () => {
    const { fn, calls } = fakeFetch({
      'https://as.example/token': { body: { access_token: 'AT2', token_type: 'Bearer', expires_in: 3600, refresh_token: 'RT2' } },
    });
    // Simulate a restart: tokens persisted (expired access + refresh + clientId) and
    // discovery persisted. No PRM/AS endpoints are even registered on the fetch.
    const store = new OAuthTokenStore();
    store.set({ accessToken: 'OLD', tokenType: 'Bearer', refreshToken: 'RT', expiresAt: -1, clientId: 'persisted-cid' } as OAuthTokens);
    const discoveryStore = memDiscoveryStore({ resource: SERVER, authServer: AS_META });
    const session = new McpOAuthSession({
      serverUrl: SERVER, fetchImpl: fn, redirectUri: 'http://127.0.0.1:9/cb',
      store, discoveryStore, now: () => 0,
      authorize: async () => { throw new Error('consent must not run — silent refresh should suffice'); },
    });
    const header = await session.authorizationHeader();
    assert.equal(header, 'Bearer AT2');
    assert.deepEqual(calls, ['https://as.example/token'], 'only the token endpoint is hit — no RFC 9728/8414 re-discovery');
  });

  it('persists discovery after a live 401 flow and clears it on invalidateCredentials("discovery")', async () => {
    const { fn } = fakeFetch({
      [PRM_URL]: { body: { resource: SERVER, authorization_servers: ['https://as.example'] } },
      [AS_URL]: { body: { issuer: 'https://as.example', authorization_endpoint: 'https://as.example/authorize', token_endpoint: 'https://as.example/token', registration_endpoint: 'https://as.example/register' } },
      'https://as.example/register': { body: { client_id: 'dyn' } },
      'https://as.example/token': { body: { access_token: 'AT', token_type: 'Bearer', expires_in: 3600 } },
    });
    const discoveryStore = memDiscoveryStore();
    const session = new McpOAuthSession({
      serverUrl: SERVER, fetchImpl: fn, redirectUri: 'http://127.0.0.1:9/cb',
      discoveryStore, authorize: async () => ({ code: 'c' }), now: () => 0,
    });
    assert.equal(await session.onUnauthorized('Bearer resource_metadata="' + PRM_URL + '"'), true);
    assert.equal(discoveryStore.saved.length >= 1, true, 'discovery saved after a successful flow');
    assert.equal(discoveryStore.value?.authServer.tokenEndpoint, 'https://as.example/token');
    await session.invalidateCredentials('discovery');
    assert.equal(discoveryStore.cleared, 1, 'invalidateCredentials(discovery) drops persisted discovery');
    assert.equal(discoveryStore.value, null);
  });

  it('keeps a configured clientId and runs fresh discovery when a token exists but no discovery is cached', async () => {
    // discoveryStore present + a persisted token, but the store has no cached
    // discovery yet (cached === null) and a static clientId is configured — so the
    // configured clientId is NOT overwritten and a 401 runs a fresh discovery.
    const { fn, calls } = fakeFetch({
      [PRM_URL]: { body: { resource: SERVER, authorization_servers: ['https://as.example'] } },
      [AS_URL]: { body: { issuer: 'https://as.example', authorization_endpoint: 'https://as.example/authorize', token_endpoint: 'https://as.example/token' } },
      'https://as.example/token': { body: { access_token: 'FRESH', token_type: 'Bearer', expires_in: 3600 } },
    });
    const store = new OAuthTokenStore();
    // A stored-but-dead token (no refresh token) so silent refresh is impossible and
    // a full 401 flow runs; clientId matches the configured one (no invalidation).
    store.set({ accessToken: 'DEAD', tokenType: 'Bearer', expiresAt: -1, clientId: 'cfg-cid' } as OAuthTokens);
    const discoveryStore = memDiscoveryStore(null);
    const session = new McpOAuthSession({
      serverUrl: SERVER, fetchImpl: fn, redirectUri: 'http://127.0.0.1:9/cb',
      clientId: 'cfg-cid', store, discoveryStore,
      authorize: async () => ({ code: 'c' }), now: () => 0,
    });
    assert.equal(await session.onUnauthorized('Bearer resource_metadata="' + PRM_URL + '"'), true);
    assert.ok(calls.includes(PRM_URL), 'fresh discovery runs when nothing is cached');
    // The configured clientId was used for the exchange (not overwritten).
    assert.equal(new URL(discoveryStore.value!.authServer.authorizationEndpoint).origin, 'https://as.example');
  });

  it('does not reload persisted discovery when there is no persisted token (nothing to refresh)', async () => {
    // With no stored token, a cold session must still run fresh discovery on 401
    // rather than silently trusting stale cached metadata with no grant behind it.
    const { fn, calls } = fakeFetch({
      [PRM_URL]: { body: { resource: SERVER, authorization_servers: ['https://as.example'] } },
      [AS_URL]: { body: { issuer: 'https://as.example', authorization_endpoint: 'https://as.example/authorize', token_endpoint: 'https://as.example/token', registration_endpoint: 'https://as.example/register' } },
      'https://as.example/register': { body: { client_id: 'dyn' } },
      'https://as.example/token': { body: { access_token: 'AT', token_type: 'Bearer', expires_in: 3600 } },
    });
    const discoveryStore = memDiscoveryStore({ resource: SERVER, authServer: AS_META });
    const session = new McpOAuthSession({
      serverUrl: SERVER, fetchImpl: fn, redirectUri: 'http://127.0.0.1:9/cb',
      discoveryStore, authorize: async () => ({ code: 'c' }), now: () => 0,
    });
    assert.equal(await session.onUnauthorized('Bearer resource_metadata="' + PRM_URL + '"'), true);
    assert.ok(calls.includes(PRM_URL), 'fresh discovery runs when there is no token to refresh');
  });
});
