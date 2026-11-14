/**
 * mcp-oauth.test.ts — unit tests for the MCP OAuth 2.1 protocol module.
 * All network I/O is through an injected FetchLike, so these are pure and fast.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { createHash } from 'node:crypto';

import {
  generatePkce, generateState, parseWwwAuthenticate, wellKnown,
  discoverProtectedResource, discoverAuthServer, registerClient,
  buildAuthorizationUrl, exchangeCode, refreshToken, OAuthTokenStore,
  McpOAuthSession,
} from '../src/mcp/oauth';
import type { FetchLike } from '../src/mcp/oauth';

const b64url = (s: Buffer) => s.toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');

function fakeFetch(routes: Record<string, { ok?: boolean; status?: number; body: unknown }>): { fn: FetchLike; calls: Array<{ url: string; init?: unknown }> } {
  const calls: Array<{ url: string; init?: unknown }> = [];
  const fn: FetchLike = async (url, init) => {
    calls.push({ url, init });
    const r = routes[url];
    if (!r) return { ok: false, status: 404, json: async () => ({}) };
    return { ok: r.ok ?? true, status: r.status ?? 200, json: async () => r.body };
  };
  return { fn, calls };
}

describe('generatePkce / generateState', () => {
  it('produces an S256 challenge = base64url(sha256(verifier)) with url-safe charset', () => {
    const p = generatePkce();
    assert.equal(p.method, 'S256');
    assert.ok(p.verifier.length >= 43);
    assert.doesNotMatch(p.verifier, /[+/=]/);
    assert.doesNotMatch(p.challenge, /[+/=]/);
    assert.equal(p.challenge, b64url(createHash('sha256').update(p.verifier).digest()));
  });
  it('generateState returns a non-empty value that varies', () => {
    assert.notEqual(generateState(), generateState());
  });
});

describe('parseWwwAuthenticate', () => {
  it('extracts resource_metadata, realm and error', () => {
    const r = parseWwwAuthenticate('Bearer realm="mcp", error="invalid_token", resource_metadata="https://rs/.well-known/oauth-protected-resource"');
    assert.equal(r.resourceMetadata, 'https://rs/.well-known/oauth-protected-resource');
    assert.equal(r.realm, 'mcp');
    assert.equal(r.error, 'invalid_token');
  });
  it('returns {} for null/empty/paramless headers', () => {
    assert.deepEqual(parseWwwAuthenticate(null), {});
    assert.deepEqual(parseWwwAuthenticate(''), {});
    assert.deepEqual(parseWwwAuthenticate('Bearer'), {});
  });
});

describe('wellKnown', () => {
  it('inserts .well-known between host and issuer path (RFC 8414 §3.1, path-preserving)', () => {
    assert.equal(wellKnown('https://rs.example/mcp/v1', 'oauth-authorization-server'), 'https://rs.example/.well-known/oauth-authorization-server/mcp/v1');
    assert.equal(wellKnown('https://rs.example', 'oauth-authorization-server'), 'https://rs.example/.well-known/oauth-authorization-server');
    assert.equal(wellKnown('https://rs.example/', 'oauth-protected-resource'), 'https://rs.example/.well-known/oauth-protected-resource');
  });
});

describe('discoverProtectedResource', () => {
  it('parses resource + authorization_servers', async () => {
    const { fn } = fakeFetch({ 'https://rs/.well-known/oauth-protected-resource': { body: { resource: 'https://rs/mcp', authorization_servers: ['https://as'], scopes_supported: ['a'] } } });
    const md = await discoverProtectedResource(fn, 'https://rs/.well-known/oauth-protected-resource');
    assert.deepEqual(md, { resource: 'https://rs/mcp', authorizationServers: ['https://as'], scopesSupported: ['a'] });
  });
  it('throws when authorization_servers is missing/empty', async () => {
    const { fn } = fakeFetch({ 'u': { body: { authorization_servers: [] } } });
    await assert.rejects(() => discoverProtectedResource(fn, 'u'), /no authorization_servers/);
  });
  it('throws on a non-ok response', async () => {
    const { fn } = fakeFetch({ 'u': { ok: false, status: 500, body: {} } });
    await assert.rejects(() => discoverProtectedResource(fn, 'u'), /discovery failed \(500\)/);
  });
});

describe('discoverAuthServer', () => {
  it('parses endpoints from the issuer well-known doc', async () => {
    const { fn } = fakeFetch({ 'https://as/.well-known/oauth-authorization-server': { body: { issuer: 'https://as', authorization_endpoint: 'https://as/authorize', token_endpoint: 'https://as/token', registration_endpoint: 'https://as/register', code_challenge_methods_supported: ['S256'] } } });
    const md = await discoverAuthServer(fn, 'https://as');
    assert.equal(md.authorizationEndpoint, 'https://as/authorize');
    assert.equal(md.tokenEndpoint, 'https://as/token');
    assert.equal(md.registrationEndpoint, 'https://as/register');
    assert.deepEqual(md.codeChallengeMethodsSupported, ['S256']);
  });
  it('throws when endpoints are missing', async () => {
    const { fn } = fakeFetch({ 'https://as/.well-known/oauth-authorization-server': { body: { issuer: 'https://as' } } });
    await assert.rejects(() => discoverAuthServer(fn, 'https://as'), /missing endpoints/);
  });
});

describe('registerClient', () => {
  it('posts a registration and returns the issued client_id/secret', async () => {
    const { fn, calls } = fakeFetch({ 'https://as/register': { body: { client_id: 'cid', client_secret: 'sec' } } });
    const out = await registerClient(fn, 'https://as/register', ['http://127.0.0.1:0/cb']);
    assert.deepEqual(out, { clientId: 'cid', clientSecret: 'sec' });
    const body = JSON.parse((calls[0].init as { body: string }).body);
    assert.deepEqual(body.redirect_uris, ['http://127.0.0.1:0/cb']);
    assert.equal(body.token_endpoint_auth_method, 'none');
  });
  it('throws on non-ok and on a missing client_id', async () => {
    await assert.rejects(() => registerClient(fakeFetch({ 'u': { ok: false, status: 400, body: {} } }).fn, 'u', []), /registration failed \(400\)/);
    await assert.rejects(() => registerClient(fakeFetch({ 'u': { body: {} } }).fn, 'u', []), /missing client_id/);
  });
});

describe('buildAuthorizationUrl', () => {
  it('includes PKCE, resource indicator and optional scope', () => {
    const url = new URL(buildAuthorizationUrl({
      authorizationEndpoint: 'https://as/authorize', clientId: 'cid', redirectUri: 'http://127.0.0.1:7/cb',
      resource: 'https://rs/mcp', codeChallenge: 'chal', state: 'st', scope: 'read write',
    }));
    assert.equal(url.searchParams.get('response_type'), 'code');
    assert.equal(url.searchParams.get('client_id'), 'cid');
    assert.equal(url.searchParams.get('code_challenge'), 'chal');
    assert.equal(url.searchParams.get('code_challenge_method'), 'S256');
    assert.equal(url.searchParams.get('resource'), 'https://rs/mcp');
    assert.equal(url.searchParams.get('scope'), 'read write');
  });
  it('omits scope when not provided', () => {
    const url = new URL(buildAuthorizationUrl({ authorizationEndpoint: 'https://as/a', clientId: 'c', redirectUri: 'r', resource: 'rs', codeChallenge: 'x', state: 's' }));
    assert.equal(url.searchParams.get('scope'), null);
  });
});

describe('exchangeCode / refreshToken', () => {
  const NOW = 1_000_000;
  it('exchanges a code and computes expiresAt from expires_in', async () => {
    const { fn, calls } = fakeFetch({ 'https://as/token': { body: { access_token: 'AT', token_type: 'Bearer', refresh_token: 'RT', expires_in: 3600, scope: 'read' } } });
    const tok = await exchangeCode(fn, { tokenEndpoint: 'https://as/token', code: 'c', verifier: 'v', clientId: 'cid', redirectUri: 'r', resource: 'rs' }, NOW);
    assert.equal(tok.accessToken, 'AT');
    assert.equal(tok.refreshToken, 'RT');
    assert.equal(tok.expiresAt, NOW + 3600 * 1000);
    const form = new URLSearchParams((calls[0].init as { body: string }).body);
    assert.equal(form.get('grant_type'), 'authorization_code');
    assert.equal(form.get('code_verifier'), 'v');
    assert.equal(form.get('resource'), 'rs');
  });
  it('defaults token_type to Bearer and leaves expiresAt undefined without expires_in', async () => {
    const { fn } = fakeFetch({ 'https://as/token': { body: { access_token: 'AT' } } });
    const tok = await refreshToken(fn, { tokenEndpoint: 'https://as/token', refreshToken: 'RT', clientId: 'cid', resource: 'rs' }, NOW);
    assert.equal(tok.tokenType, 'Bearer');
    assert.equal(tok.expiresAt, undefined);
  });
  it('throws on a non-ok token endpoint and on a missing access_token', async () => {
    await assert.rejects(() => refreshToken(fakeFetch({ 'u': { ok: false, status: 401, body: {} } }).fn, { tokenEndpoint: 'u', refreshToken: 'r', clientId: 'c', resource: 'rs' }, NOW), /token endpoint error \(401\)/);
    await assert.rejects(() => exchangeCode(fakeFetch({ 'u': { body: {} } }).fn, { tokenEndpoint: 'u', code: 'c', verifier: 'v', clientId: 'c', redirectUri: 'r', resource: 'rs' }, NOW), /missing access_token/);
  });
});

describe('OAuthTokenStore', () => {
  it('returns no header when empty and after clear', () => {
    const s = new OAuthTokenStore();
    assert.equal(s.authorizationHeader(0), null);
    assert.equal(s.get(), null);
    assert.equal(s.isExpired(0), true);
    assert.equal(s.hasRefreshToken(), false);
    s.set({ accessToken: 'AT', tokenType: 'Bearer', refreshToken: 'RT' });
    assert.equal(s.hasRefreshToken(), true);
    s.clear();
    assert.equal(s.get(), null);
  });
  it('returns a header for a live token and null once expired (with skew)', () => {
    const s = new OAuthTokenStore(1000); // 1s skew
    s.set({ accessToken: 'AT', tokenType: 'Bearer', expiresAt: 10_000 });
    assert.equal(s.authorizationHeader(5_000), 'Bearer AT');
    assert.equal(s.isExpired(5_000), false);
    // within skew window → treated as expired
    assert.equal(s.authorizationHeader(9_500), null);
    assert.equal(s.isExpired(9_500), true);
  });
  it('treats a token with no expiresAt as non-expiring', () => {
    const s = new OAuthTokenStore();
    s.set({ accessToken: 'AT', tokenType: 'Bearer' });
    assert.equal(s.isExpired(Number.MAX_SAFE_INTEGER), false);
    assert.equal(s.authorizationHeader(Number.MAX_SAFE_INTEGER), 'Bearer AT');
  });
});

describe('McpOAuthSession', () => {
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

  it('returns null before any token, then runs the full flow on 401 (DCR + consent + exchange)', async () => {
    const { fn, calls } = fakeFetch(fullRoutes());
    let authorizedUrl = '';
    const session = new McpOAuthSession({
      serverUrl: SERVER, fetchImpl: fn, redirectUri: 'http://127.0.0.1:9/cb',
      authorize: async ({ authorizationUrl }) => { authorizedUrl = authorizationUrl; return { code: 'the-code' }; },
      now: () => 1000,
    });
    assert.equal(await session.authorizationHeader(), null); // nothing yet
    const ok = await session.onUnauthorized('Bearer resource_metadata="' + PRM_URL + '"');
    assert.equal(ok, true);
    assert.match(authorizedUrl, /code_challenge=/);
    assert.match(authorizedUrl, /resource=https%3A%2F%2Frs.example%2Fmcp/);
    assert.equal(await session.authorizationHeader(), 'Bearer AT');
    // DCR happened because no clientId was supplied.
    assert.ok(calls.some((c) => c.url === 'https://as.example/register'));
  });

  it('falls back to the well-known PRM URL when the 401 carries no metadata pointer', async () => {
    const { fn } = fakeFetch(fullRoutes());
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', authorize: async () => ({ code: 'c' }), now: () => 0 });
    assert.equal(await session.onUnauthorized(null), true);
  });

  it('refreshes an expired token without re-consent', async () => {
    let t = 0;
    const { fn, calls } = fakeFetch({
      ...fullRoutes(),
      'https://as.example/token': { body: { access_token: 'AT1', token_type: 'Bearer', refresh_token: 'RT', expires_in: 100 } },
    });
    let authorizeCalls = 0;
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', authorize: async () => { authorizeCalls++; return { code: 'c' }; }, now: () => t });
    await session.onUnauthorized(null);                 // obtains AT1 (expires at 100s)
    assert.equal(await session.authorizationHeader(), 'Bearer AT1');
    t = 200_000;                                        // now past expiry
    const header = await session.authorizationHeader(); // triggers a refresh (no new consent)
    assert.equal(header, 'Bearer AT1');
    assert.equal(authorizeCalls, 1);                    // consent happened only once
    assert.ok(calls.filter((c) => c.url === 'https://as.example/token').length >= 2);
  });

  it('returns false when discovery fails, and null header stays', async () => {
    const { fn } = fakeFetch({}); // every route 404s
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', authorize: async () => ({ code: 'c' }), now: () => 0 });
    assert.equal(await session.onUnauthorized(null), false);
    assert.equal(await session.authorizationHeader(), null);
  });

  it('returns false when no client_id and no registration endpoint', async () => {
    const { fn } = fakeFetch({
      [PRM_URL]: { body: { resource: SERVER, authorization_servers: ['https://as.example'] } },
      [AS_META]: { body: { issuer: 'https://as.example', authorization_endpoint: 'https://as.example/authorize', token_endpoint: 'https://as.example/token' } },
    });
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', authorize: async () => ({ code: 'c' }), now: () => 0 });
    assert.equal(await session.onUnauthorized(null), false);
  });

  it('returns false when the user cancels consent (authorize throws)', async () => {
    const { fn } = fakeFetch(fullRoutes());
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', authorize: async () => { throw new Error('user closed browser'); }, now: () => 0 });
    assert.equal(await session.onUnauthorized(null), false);
  });

  it('does not refresh when the refresh itself fails (returns null header)', async () => {
    let t = 0;
    const { fn } = fakeFetch({
      ...fullRoutes(),
      'https://as.example/token': { body: { access_token: 'AT1', token_type: 'Bearer', refresh_token: 'RT', expires_in: 100 } },
    });
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', authorize: async () => ({ code: 'c' }), now: () => t });
    await session.onUnauthorized(null);
    t = 200_000;
    // Make the token endpoint start failing so the refresh rejects.
    (fn as unknown as { __break?: boolean }); // no-op marker
    const brokenRoutes = fakeFetch({ ...fullRoutes(), 'https://as.example/token': { ok: false, status: 400, body: {} } });
    (session as unknown as { opts: { fetchImpl: FetchLike } }).opts.fetchImpl = brokenRoutes.fn;
    assert.equal(await session.authorizationHeader(), null);
  });
});
