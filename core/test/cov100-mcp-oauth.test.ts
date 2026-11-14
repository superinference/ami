/**
 * cov100-mcp-oauth.test.ts — targeted BEHAVIOR tests for the uncovered edge/
 * fallback arms of core/src/mcp/oauth.ts (branch ~96.91 → closing the gaps).
 *
 * Each test asserts a concrete, observable behavior of a branch the primary
 * suite (mcp-oauth.test.ts) does not reach:
 *   - discoverProtectedResource: `resource` absent/non-string → falls back to the
 *     metadata URL as the RFC 8707 resource.
 *   - discoverAuthServer: `issuer` absent/non-string → falls back to the queried
 *     issuer argument.
 *   - McpOAuthSession: explicit opts.scope wins over PRM scopes_supported in the
 *     authorization URL (left arm of `opts.scope ?? prm.scopesSupported?.join`).
 *   - McpOAuthSession: no opts.scope AND no PRM scopes_supported → scope omitted
 *     entirely (right arm resolves to undefined → buildAuthorizationUrl skips it).
 *   - McpOAuthSession.authorizationHeader: an expired token carrying a refresh
 *     token but with no discovered authMeta yet does NOT attempt a refresh and
 *     returns null (authMeta-false arm of the `&&` guard).
 *
 * All I/O is via an injected FetchLike — pure, deterministic, no real network.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  discoverProtectedResource, discoverAuthServer,
  OAuthTokenStore, McpOAuthSession,
} from '../src/mcp/oauth';
import type { FetchLike } from '../src/mcp/oauth';

function fakeFetch(routes: Record<string, { ok?: boolean; status?: number; body: unknown }>): {
  fn: FetchLike; calls: Array<{ url: string; init?: unknown }>;
} {
  const calls: Array<{ url: string; init?: unknown }> = [];
  const fn: FetchLike = async (url, init) => {
    calls.push({ url, init });
    const r = routes[url];
    if (!r) return { ok: false, status: 404, json: async () => ({}) };
    return { ok: r.ok ?? true, status: r.status ?? 200, json: async () => r.body };
  };
  return { fn, calls };
}

describe('discoverProtectedResource — resource fallback', () => {
  it('uses the metadata URL as the resource when the doc omits `resource`', async () => {
    const METADATA_URL = 'https://rs.example/.well-known/oauth-protected-resource';
    const { fn } = fakeFetch({
      [METADATA_URL]: { body: { authorization_servers: ['https://as.example'] } }, // no `resource`
    });
    const md = await discoverProtectedResource(fn, METADATA_URL);
    assert.equal(md.resource, METADATA_URL); // fell back to the metadata URL
    assert.deepEqual(md.authorizationServers, ['https://as.example']);
    assert.equal(md.scopesSupported, undefined);
  });

  it('ignores a non-string `resource` (e.g. a number) and falls back', async () => {
    const METADATA_URL = 'https://rs.example/.well-known/oauth-protected-resource';
    const { fn } = fakeFetch({
      [METADATA_URL]: { body: { resource: 12345, authorization_servers: ['https://as.example'] } },
    });
    const md = await discoverProtectedResource(fn, METADATA_URL);
    assert.equal(md.resource, METADATA_URL);
  });
});

describe('discoverAuthServer — issuer fallback', () => {
  it('uses the queried issuer when the doc omits/mistypes `issuer`', async () => {
    const ISSUER = 'https://as.example';
    const { fn } = fakeFetch({
      'https://as.example/.well-known/oauth-authorization-server': {
        body: {
          issuer: 42, // non-string → must be ignored
          authorization_endpoint: 'https://as.example/authorize',
          token_endpoint: 'https://as.example/token',
        },
      },
    });
    const md = await discoverAuthServer(fn, ISSUER);
    assert.equal(md.issuer, ISSUER); // fell back to the argument
    assert.equal(md.authorizationEndpoint, 'https://as.example/authorize');
    assert.equal(md.tokenEndpoint, 'https://as.example/token');
  });
});

describe('McpOAuthSession — scope resolution in the authorization URL', () => {
  const SERVER = 'https://rs.example/mcp';
  const PRM_URL = 'https://rs.example/.well-known/oauth-protected-resource/mcp';
  const AS_META = 'https://as.example/.well-known/oauth-authorization-server';

  function routes(prmBody: unknown) {
    return {
      [PRM_URL]: { body: prmBody },
      [AS_META]: {
        body: {
          issuer: 'https://as.example',
          authorization_endpoint: 'https://as.example/authorize',
          token_endpoint: 'https://as.example/token',
        },
      },
      'https://as.example/token': { body: { access_token: 'AT', token_type: 'Bearer' } },
    };
  }

  it('prefers an explicit opts.scope over the PRM scopes_supported', async () => {
    const { fn } = fakeFetch(routes({ resource: SERVER, authorization_servers: ['https://as.example'], scopes_supported: ['read', 'write'] }));
    let authorizedUrl = '';
    const session = new McpOAuthSession({
      serverUrl: SERVER, fetchImpl: fn, redirectUri: 'http://127.0.0.1:9/cb', clientId: 'cid',
      scope: 'custom:scope',
      authorize: async ({ authorizationUrl }) => { authorizedUrl = authorizationUrl; return { code: 'c' }; },
      now: () => 0,
    });
    assert.equal(await session.onUnauthorized(null), true);
    assert.equal(new URL(authorizedUrl).searchParams.get('scope'), 'custom:scope');
  });

  it('omits scope when neither opts.scope nor PRM scopes_supported are present', async () => {
    const { fn } = fakeFetch(routes({ resource: SERVER, authorization_servers: ['https://as.example'] })); // no scopes_supported
    let authorizedUrl = '';
    const session = new McpOAuthSession({
      serverUrl: SERVER, fetchImpl: fn, redirectUri: 'http://127.0.0.1:9/cb', clientId: 'cid',
      authorize: async ({ authorizationUrl }) => { authorizedUrl = authorizationUrl; return { code: 'c' }; },
      now: () => 0,
    });
    assert.equal(await session.onUnauthorized(null), true);
    assert.equal(new URL(authorizedUrl).searchParams.get('scope'), null); // scope absent
  });
});

describe('McpOAuthSession.authorizationHeader — no refresh without discovered authMeta', () => {
  it('returns null for an expired, refreshable token when authMeta was never discovered', async () => {
    const SERVER = 'https://rs.example/mcp';
    // The token endpoint, if ever hit, would hand back a fresh token — we assert it is NOT hit.
    const { fn, calls } = fakeFetch({
      'https://as.example/token': { body: { access_token: 'NEW', token_type: 'Bearer' } },
    });
    const store = new OAuthTokenStore();
    // Expired at t=50 (skew-free since well before now), but carries a refresh token.
    store.set({ accessToken: 'OLD', tokenType: 'Bearer', refreshToken: 'RT', expiresAt: 50 });

    const session = new McpOAuthSession({
      serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid',
      authorize: async () => ({ code: 'c' }), now: () => 1_000_000, store,
    });

    // live header is null (expired); hasRefreshToken() is true but authMeta is null,
    // so the `&&` guard short-circuits and no refresh network call is made.
    assert.equal(await session.authorizationHeader(), null);
    assert.equal(calls.length, 0, 'no token-endpoint call should have been attempted');
    // Sanity: the store still holds the stale refresh token (it was not replaced).
    assert.equal(store.get()?.refreshToken, 'RT');
  });
});
