/**
 * mcp-oauth-parity-w66.test.ts — W66 OAuth parity batch for
 * core/core/src/mcp/oauth.ts (+ client/manager wiring). Covers:
 *   A. RFC 7009 token revocation — revokeToken(), AS revocation_endpoint parse,
 *      McpOAuthSession.revoke(), McpClient.revokeAuth, manager removeServer wiring.
 *   B. error_uri / error_description surfacing on discovery, DCR and token errors.
 *   C. Reactive silent force-refresh on a 401 (clock-drift) before full re-consent.
 *   D. Generation-counter stale-invalidation guard for in-flight refreshes.
 *   E. Iterate ALL advertised authorization_servers with per-AS error accumulation
 *      and grant-type capability filtering.
 *   F. Configurable proactive refresh-buffer window.
 *
 * All network I/O is through an injected FetchLike so these are pure and fast.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'events';

import {
  revokeToken, pickClientAuthMethod, normalizeTokenError, extractOAuthErrorBody,
  OAuthError, discoverAuthServer, registerClient, OAuthTokenStore, McpOAuthSession,
} from '../src/mcp/oauth';
import type { FetchLike } from '../src/mcp/oauth';
import { McpClient } from '../src/mcp/client';
import { McpManager } from '../src/mcp/manager';

interface Call { url: string; init?: { method?: string; headers?: Record<string, string>; body?: string } }
function fakeFetch(routes: Record<string, { ok?: boolean; status?: number; body?: unknown }>): { fn: FetchLike; calls: Call[] } {
  const calls: Call[] = [];
  const fn: FetchLike = async (url, init) => {
    calls.push({ url, init });
    const r = routes[url];
    if (!r) return { ok: false, status: 404, json: async () => ({}) };
    return { ok: r.ok ?? true, status: r.status ?? 200, json: async () => r.body ?? {} };
  };
  return { fn, calls };
}

const SERVER = 'https://rs.example/mcp';
const PRM_URL = 'https://rs.example/.well-known/oauth-protected-resource/mcp';
const AS_META = 'https://as.example/.well-known/oauth-authorization-server';
const REVOKE = 'https://as.example/revoke';

function fullRoutes(extra: Record<string, { ok?: boolean; status?: number; body?: unknown }> = {}) {
  return {
    [PRM_URL]: { body: { resource: SERVER, authorization_servers: ['https://as.example'] } },
    [AS_META]: { body: {
      issuer: 'https://as.example',
      authorization_endpoint: 'https://as.example/authorize',
      token_endpoint: 'https://as.example/token',
      registration_endpoint: 'https://as.example/register',
    } },
    'https://as.example/register': { body: { client_id: 'dcr-cid' } },
    'https://as.example/token': { body: { access_token: 'AT', token_type: 'Bearer', refresh_token: 'RT', expires_in: 3600 } },
    ...extra,
  };
}

// ---------------------------------------------------------------------------
// B — error_uri / error_description surfacing
// ---------------------------------------------------------------------------
describe('W66 OAuth error-body surfacing (error / error_description / error_uri)', () => {
  it('extractOAuthErrorBody renders code + description + uri into a suffix', () => {
    const r = extractOAuthErrorBody({ error: 'invalid_client', error_description: 'bad creds', error_uri: 'https://e.example/x' });
    assert.equal(r.code, 'invalid_client');
    assert.equal(r.description, 'bad creds');
    assert.equal(r.uri, 'https://e.example/x');
    assert.equal(r.suffix, ': invalid_client (bad creds) — see https://e.example/x');
  });
  it('extractOAuthErrorBody falls back to description-only when no code', () => {
    assert.equal(extractOAuthErrorBody({ error_description: 'boom' }).suffix, ': boom');
    assert.equal(extractOAuthErrorBody({}).suffix, '');
  });
  it('normalizeTokenError carries error_uri onto the OAuthError', () => {
    const e = normalizeTokenError(400, { error: 'invalid_grant', error_description: 'expired', error_uri: 'https://e.example/g' });
    assert.ok(e instanceof OAuthError);
    assert.equal(e.code, 'invalid_grant');
    assert.equal(e.uri, 'https://e.example/g');
    assert.match(e.message, /invalid_grant \(expired\) — see https:\/\/e\.example\/g/);
  });
  it('discoverAuthServer surfaces the server error body on a non-ok discovery', async () => {
    const { fn } = fakeFetch({ [AS_META]: { ok: false, status: 503, body: { error: 'temporarily_unavailable', error_description: 'maintenance' } } });
    await assert.rejects(
      () => discoverAuthServer(fn, 'https://as.example'),
      (e: unknown) => e instanceof OAuthError && e.code === 'temporarily_unavailable' && /maintenance/.test((e as Error).message),
    );
  });
  it('registerClient surfaces the server error body on a non-ok registration', async () => {
    const { fn } = fakeFetch({ 'https://as.example/register': { ok: false, status: 400, body: { error: 'invalid_redirect_uri', error_description: 'not allowed' } } });
    await assert.rejects(
      () => registerClient(fn, 'https://as.example/register', ['https://127.0.0.1:9/cb']),
      (e: unknown) => e instanceof OAuthError && e.code === 'invalid_redirect_uri' && /not allowed/.test((e as Error).message),
    );
  });
});

// ---------------------------------------------------------------------------
// A — RFC 7009 token revocation primitives
// ---------------------------------------------------------------------------
describe('W66 pickClientAuthMethod', () => {
  it('returns none without a secret, regardless of advertised list', () => {
    assert.equal(pickClientAuthMethod(['client_secret_basic'], false), 'none');
  });
  it('defaults to client_secret_basic with a secret and no advertised list', () => {
    assert.equal(pickClientAuthMethod(undefined, true), 'client_secret_basic');
  });
  it('prefers basic, then post, from the advertised list', () => {
    assert.equal(pickClientAuthMethod(['client_secret_post', 'client_secret_basic'], true), 'client_secret_basic');
    assert.equal(pickClientAuthMethod(['client_secret_post'], true), 'client_secret_post');
    assert.equal(pickClientAuthMethod(['private_key_jwt'], true), 'client_secret_basic');
  });
});

describe('W66 revokeToken (RFC 7009)', () => {
  it('posts token + hint and uses a Basic Authorization header for client_secret_basic', async () => {
    const { fn, calls } = fakeFetch({ [REVOKE]: { body: {} } });
    const ok = await revokeToken(fn, {
      revocationEndpoint: REVOKE, token: 'RT', tokenTypeHint: 'refresh_token',
      clientId: 'cid', clientSecret: 'sec', authMethod: 'client_secret_basic',
    });
    assert.equal(ok, true);
    const c = calls[0];
    assert.equal(c.init!.method, 'POST');
    assert.equal(c.init!.headers!.Authorization, `Basic ${Buffer.from('cid:sec').toString('base64')}`);
    const form = new URLSearchParams(c.init!.body);
    assert.equal(form.get('token'), 'RT');
    assert.equal(form.get('token_type_hint'), 'refresh_token');
    assert.equal(form.get('client_id'), 'cid');
    assert.equal(form.get('client_secret'), null); // basic puts it in the header, not the body
  });
  it('puts the client_secret in the body for client_secret_post', async () => {
    const { fn, calls } = fakeFetch({ [REVOKE]: { body: {} } });
    await revokeToken(fn, { revocationEndpoint: REVOKE, token: 'AT', clientId: 'cid', clientSecret: 'sec', authMethod: 'client_secret_post' });
    assert.equal(calls[0].init!.headers!.Authorization, undefined);
    assert.equal(new URLSearchParams(calls[0].init!.body).get('client_secret'), 'sec');
  });
  it('falls back to a Bearer-authenticated retry on a 401 (non-RFC-7009 server)', async () => {
    let n = 0;
    const fn: FetchLike = async (url, init) => {
      n++;
      if (n === 1) return { ok: false, status: 401, json: async () => ({}) };
      // second attempt must carry the Bearer header
      assert.equal((init!.headers as Record<string, string>).Authorization, 'Bearer RT');
      return { ok: true, status: 200, json: async () => ({}) };
    };
    const ok = await revokeToken(fn, { revocationEndpoint: REVOKE, token: 'RT', tokenTypeHint: 'refresh_token', clientId: 'cid' });
    assert.equal(ok, true);
    assert.equal(n, 2);
  });
  it('returns false on a non-401 error without retry', async () => {
    const { fn, calls } = fakeFetch({ [REVOKE]: { ok: false, status: 400, body: {} } });
    assert.equal(await revokeToken(fn, { revocationEndpoint: REVOKE, token: 'RT' }), false);
    assert.equal(calls.length, 1);
  });
  it('rejects a plaintext non-loopback revocation endpoint', async () => {
    const { fn } = fakeFetch({});
    await assert.rejects(() => revokeToken(fn, { revocationEndpoint: 'http://as.example/revoke', token: 'RT' }), /https/);
  });
});

describe('W66 AuthServerMetadata parses revocation_endpoint', () => {
  it('captures revocation_endpoint and revocation_endpoint_auth_methods_supported', async () => {
    const { fn } = fakeFetch({ [AS_META]: { body: {
      issuer: 'https://as.example', authorization_endpoint: 'https://as.example/authorize',
      token_endpoint: 'https://as.example/token', revocation_endpoint: REVOKE,
      revocation_endpoint_auth_methods_supported: ['client_secret_post'],
    } } });
    const meta = await discoverAuthServer(fn, 'https://as.example');
    assert.equal(meta.revocationEndpoint, REVOKE);
    assert.deepEqual(meta.revocationEndpointAuthMethodsSupported, ['client_secret_post']);
  });
});

describe('W66 McpOAuthSession.revoke()', () => {
  it('revokes the refresh token FIRST then the access token, then clears the store', async () => {
    const { fn, calls } = fakeFetch(fullRoutes({
      [AS_META]: { body: {
        issuer: 'https://as.example', authorization_endpoint: 'https://as.example/authorize',
        token_endpoint: 'https://as.example/token', revocation_endpoint: REVOKE,
      } },
    }));
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', authorize: async () => ({ code: 'c' }), now: () => 0 });
    await session.onUnauthorized(null); // obtain AT + RT, populate authMeta
    assert.equal(await session.authorizationHeader(), 'Bearer AT');
    const before = calls.length;
    await session.revoke();
    const hints = calls.slice(before).filter((c) => c.url === REVOKE).map((c) => new URLSearchParams(c.init!.body).get('token_type_hint'));
    assert.deepEqual(hints, ['refresh_token', 'access_token']);
    assert.equal(await session.authorizationHeader(), null); // store cleared
  });
  it('clears the store without any network call when no revocation endpoint is advertised', async () => {
    const { fn, calls } = fakeFetch(fullRoutes());
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', authorize: async () => ({ code: 'c' }), now: () => 0 });
    await session.onUnauthorized(null);
    const before = calls.length;
    await session.revoke();
    assert.equal(calls.slice(before).filter((c) => c.url === REVOKE).length, 0);
    assert.equal(await session.authorizationHeader(), null);
  });
  it('is a no-op (only clears) when there are no stored tokens', async () => {
    const { fn, calls } = fakeFetch(fullRoutes());
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', authorize: async () => ({ code: 'c' }), now: () => 0 });
    await session.revoke();
    assert.equal(calls.length, 0); // never even discovered — nothing to revoke
    assert.equal(await session.authorizationHeader(), null);
  });
  it('discovers the revocation endpoint on demand when tokens exist but the AS was never discovered', async () => {
    const routes = fullRoutes({
      [AS_META]: { body: {
        issuer: 'https://as.example', authorization_endpoint: 'https://as.example/authorize',
        token_endpoint: 'https://as.example/token', revocation_endpoint: REVOKE,
      } },
    });
    const store = new OAuthTokenStore();
    // Session A authenticates and populates the shared store.
    const a = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fakeFetch(routes).fn, redirectUri: 'r', clientId: 'cid', store, authorize: async () => ({ code: 'c' }), now: () => 0 });
    await a.onUnauthorized(null);
    // Session B shares the store but has NEVER discovered the AS (authMeta is null).
    const { fn, calls } = fakeFetch(routes);
    const b = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', store, authorize: async () => ({ code: 'c' }), now: () => 0 });
    await b.revoke();
    assert.ok(calls.some((c) => c.url === PRM_URL), 'revoke discovered the AS on demand');
    assert.ok(calls.some((c) => c.url === REVOKE), 'revoke hit the revocation endpoint');
    assert.equal(store.get(), null);
  });

  it('onForbidden step-up revokes the abandoned token server-side before re-consent', async () => {
    const { fn, calls } = fakeFetch(fullRoutes({
      [AS_META]: { body: {
        issuer: 'https://as.example', authorization_endpoint: 'https://as.example/authorize',
        token_endpoint: 'https://as.example/token', revocation_endpoint: REVOKE,
      } },
      [PRM_URL]: { body: { resource: SERVER, authorization_servers: ['https://as.example'], scopes_supported: ['read'] } },
    }));
    let authorizeCalls = 0;
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', scope: 'read', authorize: async () => { authorizeCalls++; return { code: 'c' }; }, now: () => 0 });
    await session.onUnauthorized(null);
    assert.equal(authorizeCalls, 1);
    const before = calls.length;
    const ok = await session.onForbidden('Bearer error="insufficient_scope", scope="admin"');
    assert.equal(ok, true);
    assert.equal(authorizeCalls, 2); // full re-consent happened
    assert.ok(calls.slice(before).some((c) => c.url === REVOKE), 'the abandoned token was revoked server-side');
  });
});

// ---------------------------------------------------------------------------
// C — reactive silent force-refresh on a 401 (clock drift)
// ---------------------------------------------------------------------------
describe('W66 reactive force-refresh on a 401 (clock drift)', () => {
  it('a 401 with a locally-valid token triggers a silent refresh, not a new consent', async () => {
    const { fn, calls } = fakeFetch(fullRoutes({
      'https://as.example/token': { body: { access_token: 'AT1', token_type: 'Bearer', refresh_token: 'RT', expires_in: 3600 } },
    }));
    let authorizeCalls = 0;
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', authorize: async () => { authorizeCalls++; return { code: 'c' }; }, now: () => 0 });
    await session.onUnauthorized(null);           // consent #1 → AT1 (still locally valid)
    assert.equal(authorizeCalls, 1);
    const refreshesBefore = calls.filter((c) => c.url === 'https://as.example/token').length;
    // Server 401s despite the token being locally valid (clock drift).
    const ok = await session.onUnauthorized('Bearer');
    assert.equal(ok, true);
    assert.equal(authorizeCalls, 1, 'no new browser consent — a silent refresh recovered the 401');
    assert.ok(calls.filter((c) => c.url === 'https://as.example/token').length > refreshesBefore, 'a refresh POST was made');
  });
  it('falls back to a full re-consent when the silent refresh fails', async () => {
    let t = 0;
    const { fn } = fakeFetch(fullRoutes({
      'https://as.example/token': { body: { access_token: 'AT1', token_type: 'Bearer', refresh_token: 'RT', expires_in: 3600 } },
    }));
    let authorizeCalls = 0;
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', authorize: async () => { authorizeCalls++; return { code: 'c' }; }, now: () => t });
    await session.onUnauthorized(null);
    assert.equal(authorizeCalls, 1);
    // Break the refresh (token endpoint returns invalid_grant) → silent refresh fails.
    const broken = fakeFetch(fullRoutes({ 'https://as.example/token': { ok: false, status: 400, body: { error: 'invalid_grant' } } }));
    (session as unknown as { opts: { fetchImpl: FetchLike } }).opts.fetchImpl = broken.fn;
    t = 1;
    const ok = await session.onUnauthorized('Bearer');
    assert.equal(ok, false); // the broken token endpoint also fails the re-consent exchange
    assert.equal(authorizeCalls, 2, 'full interactive re-consent was attempted after the silent refresh failed');
  });
});

// ---------------------------------------------------------------------------
// D — generation-counter stale-invalidation guard
// ---------------------------------------------------------------------------
describe('W66 OAuthTokenStore generation guard', () => {
  it('applyRefresh commits when the generation is unchanged', () => {
    const store = new OAuthTokenStore();
    const gen = store.getGeneration();
    assert.equal(store.applyRefresh({ accessToken: 'AT', tokenType: 'Bearer' }, gen), true);
    assert.equal(store.get()!.accessToken, 'AT');
  });
  it('applyRefresh discards a stale refresh superseded by a clear()', () => {
    const store = new OAuthTokenStore();
    store.set({ accessToken: 'OLD', tokenType: 'Bearer' });
    const gen = store.getGeneration();
    store.clear(); // a sign-out lands while the refresh was in flight
    assert.equal(store.applyRefresh({ accessToken: 'STALE', tokenType: 'Bearer' }, gen), false);
    assert.equal(store.get(), null, 'the cleared state is preserved, not clobbered by the stale token');
  });
  it('a clear() during an in-flight session refresh discards the refreshed token', async () => {
    let release: () => void = () => {};
    const gate = new Promise<void>((r) => { release = r; });
    let t = 0;
    const fn: FetchLike = async (url, init) => {
      if (url === 'https://as.example/token' && new URLSearchParams(init?.body).get('grant_type') === 'refresh_token') {
        await gate; // hold the refresh open so we can clear() mid-flight
        return { ok: true, status: 200, json: async () => ({ access_token: 'REFRESHED', token_type: 'Bearer', refresh_token: 'RT2', expires_in: 3600 }) };
      }
      const r = fullRoutes()[url as keyof ReturnType<typeof fullRoutes>] as { ok?: boolean; status?: number; body?: unknown } | undefined;
      return r ? { ok: r.ok ?? true, status: r.status ?? 200, json: async () => r.body ?? {} } : { ok: false, status: 404, json: async () => ({}) };
    };
    const store = new OAuthTokenStore();
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', store, authorize: async () => ({ code: 'c' }), now: () => t });
    await session.onUnauthorized(null); // AT (expires_in 3600)
    t = 10 ** 12; // force expiry → next header read refreshes
    const refreshPromise = session.authorizationHeader();
    store.clear(); // user signs out while the refresh POST is still in flight
    release();
    const header = await refreshPromise;
    assert.equal(header, null, 'the stale refreshed token must not resurrect the cleared session');
    assert.equal(store.get(), null);
  });
});

// ---------------------------------------------------------------------------
// E — iterate ALL advertised authorization_servers
// ---------------------------------------------------------------------------
describe('W66 iterate all advertised authorization_servers', () => {
  const AS2_META = 'https://as2.example/.well-known/oauth-authorization-server';
  function twoAsRoutes(first: { ok?: boolean; status?: number; body?: unknown }) {
    return {
      [PRM_URL]: { body: { resource: SERVER, authorization_servers: ['https://as.example', 'https://as2.example'] } },
      [AS_META]: first,
      [AS2_META]: { body: {
        issuer: 'https://as2.example', authorization_endpoint: 'https://as2.example/authorize',
        token_endpoint: 'https://as2.example/token',
      } },
      'https://as2.example/token': { body: { access_token: 'AT2', token_type: 'Bearer', expires_in: 3600 } },
    };
  }
  it('skips a first AS whose discovery 404s and uses the second', async () => {
    const { fn } = fakeFetch(twoAsRoutes({ ok: false, status: 404 }));
    let authUrl = '';
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', authorize: async ({ authorizationUrl }) => { authUrl = authorizationUrl; return { code: 'c' }; }, now: () => 0 });
    assert.equal(await session.onUnauthorized(null), true);
    assert.match(authUrl, /^https:\/\/as2\.example\/authorize/);
    assert.equal(await session.authorizationHeader(), 'Bearer AT2');
  });
  it('skips a first AS that cannot do authorization_code and uses the capable second', async () => {
    const { fn } = fakeFetch(twoAsRoutes({ body: {
      issuer: 'https://as.example', authorization_endpoint: 'https://as.example/authorize',
      token_endpoint: 'https://as.example/token', grant_types_supported: ['client_credentials'],
    } }));
    let authUrl = '';
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', authorize: async ({ authorizationUrl }) => { authUrl = authorizationUrl; return { code: 'c' }; }, now: () => 0 });
    assert.equal(await session.onUnauthorized(null), true);
    assert.match(authUrl, /^https:\/\/as2\.example\/authorize/);
  });
  it('returns false when EVERY advertised AS fails (and no path-aware fallback applies)', async () => {
    const { fn } = fakeFetch({
      [PRM_URL]: { body: { resource: SERVER, authorization_servers: ['https://as.example', 'https://as2.example'] } },
      [AS_META]: { ok: false, status: 500 },
      [AS2_META]: { ok: false, status: 500 },
    });
    let authorizeCalled = false;
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', authorize: async () => { authorizeCalled = true; return { code: 'c' }; }, now: () => 0 });
    assert.equal(await session.onUnauthorized(null), false);
    assert.equal(authorizeCalled, false);
  });
  it('rejects a path-aware fallback AS that cannot do authorization_code', async () => {
    const serverUrl = 'https://mcp.example.com/tenant1';
    const prmUrl = 'https://mcp.example.com/.well-known/oauth-protected-resource/tenant1';
    const asUrl = 'https://mcp.example.com/.well-known/oauth-authorization-server/tenant1';
    const { fn } = fakeFetch({
      [prmUrl]: { ok: false, status: 404 },
      [asUrl]: { body: {
        issuer: 'https://mcp.example.com/tenant1', authorization_endpoint: 'https://mcp.example.com/authorize',
        token_endpoint: 'https://mcp.example.com/token', grant_types_supported: ['client_credentials'],
      } },
    });
    let authorizeCalled = false;
    const session = new McpOAuthSession({ serverUrl, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', authorize: async () => { authorizeCalled = true; return { code: 'c' }; }, now: () => 0 });
    assert.equal(await session.onUnauthorized('Bearer'), false);
    assert.equal(authorizeCalled, false);
  });
});

// ---------------------------------------------------------------------------
// F — configurable proactive refresh-buffer window
// ---------------------------------------------------------------------------
describe('W66 configurable refresh buffer', () => {
  it('OAuthTokenStore treats a token as expired within the configured buffer', () => {
    const store = new OAuthTokenStore(5 * 60_000); // 5-minute refresh-ahead window
    store.set({ accessToken: 'AT', tokenType: 'Bearer', expiresAt: 10 * 60_000 });
    assert.equal(store.isExpired(4 * 60_000), false); // 6 min left → still live
    assert.equal(store.isExpired(6 * 60_000), true);  // 4 min left → inside the 5-min buffer
  });
  it('McpOAuthSession applies refreshBufferMs to its default in-memory store', async () => {
    const { fn } = fakeFetch(fullRoutes({
      'https://as.example/token': { body: { access_token: 'AT', token_type: 'Bearer', refresh_token: 'RT', expires_in: 600 } },
    }));
    let t = 0;
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', refreshBufferMs: 5 * 60_000, authorize: async () => ({ code: 'c' }), now: () => t });
    await session.onUnauthorized(null); // AT expires at 600s
    t = 400_000; // 200s left — inside the 5-min (300s) buffer → a refresh is triggered
    const refreshed = await session.authorizationHeader();
    assert.equal(refreshed, 'Bearer AT'); // refreshed silently
  });
});

// ---------------------------------------------------------------------------
// A (wiring) — McpClient.revokeAuth + McpManager.removeServer
// ---------------------------------------------------------------------------
describe('W66 McpClient.revokeAuth wiring', () => {
  it('delegates to the OAuth provider revoke()', async () => {
    let revoked = false;
    const client = new McpClient({ command: '', oauth: {
      authorizationHeader: async () => null,
      onUnauthorized: async () => false,
      onForbidden: async () => false,
      revoke: async () => { revoked = true; },
    } });
    await client.revokeAuth();
    assert.equal(revoked, true);
  });
  it('is a no-op for a provider without revoke() / for no provider', async () => {
    const noRevoke = new McpClient({ command: '', oauth: { authorizationHeader: async () => null, onUnauthorized: async () => false, onForbidden: async () => false } });
    await assert.doesNotReject(() => noRevoke.revokeAuth());
    const noOauth = new McpClient({ command: '' });
    await assert.doesNotReject(() => noOauth.revokeAuth());
  });
});

interface SessionLike { onUnauthorized(h: string | null): Promise<boolean>; revoke?(): Promise<void> }
class FakeClient extends EventEmitter {
  state = 'ready';
  serverInfo: unknown = { name: 'fake', version: '1' };
  instructions: string | null = null;
  uptime: number | null = 1;
  tools: unknown[] = [];
  resources: unknown[] = [];
  prompts: unknown[] = [];
  revokePromise: Promise<void> | undefined;
  readonly oauth?: SessionLike;
  constructor(oauth?: SessionLike) { super(); this.oauth = oauth; }
  async connect(): Promise<void> {}
  async connectHTTP(): Promise<void> {}
  async connectSSE(): Promise<void> {}
  async listTools(): Promise<unknown[]> { return this.tools; }
  async listResources(): Promise<unknown[]> { return this.resources; }
  async listPrompts(): Promise<unknown[]> { return this.prompts; }
  async getPrompt(): Promise<unknown> { return { messages: [] }; }
  async readResource(): Promise<unknown> { return {}; }
  async callTool(): Promise<unknown> { return { ok: true }; }
  async reconnect(): Promise<void> {}
  // Mirrors the real McpClient.revokeAuth contract the manager depends on.
  async revokeAuth(): Promise<void> { this.revokePromise = this.oauth?.revoke?.() ?? Promise.resolve(); return this.revokePromise; }
  disconnect(): void {}
  setHeaders(): void {}
}

describe('W66 McpManager.removeServer revokes the server tokens (E2E)', () => {
  it('fires server-side revocation + clears persisted credentials on forget', async () => {
    const { fn, calls } = fakeFetch(fullRoutes({
      [AS_META]: { body: {
        issuer: 'https://as.example', authorization_endpoint: 'https://as.example/authorize',
        token_endpoint: 'https://as.example/token', revocation_endpoint: REVOKE,
      } },
    }));
    const mgr = new McpManager();
    mgr._fetch = fn;
    let captured: FakeClient | undefined;
    mgr._createClient = ((cfg: { oauth?: SessionLike }) => {
      captured = new FakeClient(cfg.oauth);
      return captured;
    }) as never;
    mgr.setOAuthAuthorizer(async () => ({ code: 'the-code' }));
    mgr.addPendingServer('oauth-srv', { command: '', transport: 'http', url: SERVER, oauth: { clientId: 'cid' } });
    await mgr.ensureConnected('oauth-srv');
    // Drive an auth so the real session (behind the FakeClient) holds revocable
    // tokens + a discovered revocation endpoint.
    await captured!.oauth!.onUnauthorized('Bearer resource_metadata="' + PRM_URL + '"');
    const before = calls.length;
    mgr.removeServer('oauth-srv');           // forget/sign-out
    await captured!.revokePromise;           // await the fire-and-forget revoke
    assert.ok(calls.slice(before).some((c) => c.url === REVOKE), 'removeServer revoked the tokens server-side');
  });
});
