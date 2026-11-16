/**
 * mcp-oauth-parity-w34.test.ts — unit + integration tests for the W34/U036 OAuth
 * parity batch (core/core/src/mcp/oauth.ts):
 *   1. coerceExpiresIn — tolerate a numeric-string `expires_in` (PHP-backed IdPs).
 *   2. getJson — captive-portal / non-JSON discovery-response diagnosis.
 *   3. postForm — captive-portal / non-JSON token-response diagnosis.
 *   4. grant_types_supported + token_endpoint_auth_methods_supported parsed from
 *      AS metadata and USED (grant-type capability check + registration auth method).
 *   5. Path-aware RFC 8414 fallback when RFC 9728 PRM discovery fails.
 *
 * All network I/O is through an injected FetchLike so these are pure and fast.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  coerceExpiresIn,
  pickRegistrationAuthMethod,
  hasUrlPath,
  exchangeCode,
  refreshToken,
  discoverAuthServer,
  registerClient,
  McpOAuthSession,
  OAuthTokenStore,
} from '../src/mcp/oauth';
import type { FetchLike } from '../src/mcp/oauth';

function fakeFetch(routes: Record<string, { ok?: boolean; status?: number; body?: unknown; throwJson?: boolean }>): {
  fn: FetchLike;
  calls: Array<{ url: string; init?: { body?: string } }>;
} {
  const calls: Array<{ url: string; init?: { body?: string } }> = [];
  const fn: FetchLike = async (url, init) => {
    calls.push({ url, init: init ? { body: init.body } : undefined });
    const r = routes[url];
    if (!r) return { ok: false, status: 404, json: async () => ({}) };
    return {
      ok: r.ok ?? true,
      status: r.status ?? 200,
      json: async () => {
        if (r.throwJson) throw new SyntaxError('Unexpected token < in JSON at position 0');
        return r.body ?? {};
      },
    };
  };
  return { fn, calls };
}

const NOW = 1_000_000;

// ---------------------------------------------------------------------------
// Gap 1 — expires_in string-coercion tolerance
// ---------------------------------------------------------------------------
describe('W34 coerceExpiresIn — tolerant numeric-string expires_in', () => {
  it('passes a numeric value through', () => {
    assert.equal(coerceExpiresIn(3600), 3600);
  });
  it('coerces a numeric string (PHP-backed IdP)', () => {
    assert.equal(coerceExpiresIn('3600'), 3600);
  });
  it('drops a non-numeric / empty / missing value', () => {
    assert.equal(coerceExpiresIn('abc'), undefined);
    assert.equal(coerceExpiresIn(''), undefined);
    assert.equal(coerceExpiresIn('   '), undefined);
    assert.equal(coerceExpiresIn(undefined), undefined);
    assert.equal(coerceExpiresIn(NaN), undefined);
    assert.equal(coerceExpiresIn(Infinity), undefined);
  });
  it('a token response with a STRING expires_in is treated as expiring', async () => {
    // RED (pre-fix): typeof === "number" check dropped "120" → expiresAt fell back
    // to the 3600s default, not the server-stated 120s.
    const { fn } = fakeFetch({ u: { body: { access_token: 'AT', token_type: 'Bearer', expires_in: '120' } } });
    const tok = await exchangeCode(fn, { tokenEndpoint: 'u', code: 'c', verifier: 'v', clientId: 'c', redirectUri: 'r', resource: 'rs' }, NOW);
    assert.equal(tok.expiresAt, NOW + 120 * 1000);
  });
});

// ---------------------------------------------------------------------------
// Gap 2 — captive-portal / non-JSON discovery response
// ---------------------------------------------------------------------------
describe('W34 getJson — captive-portal non-JSON discovery diagnosis', () => {
  it('throws a captive-portal hint (not a raw SyntaxError) on a non-JSON 200', async () => {
    const url = 'https://mcp.example.com/.well-known/oauth-authorization-server';
    const { fn } = fakeFetch({ [url]: { ok: true, status: 200, throwJson: true } });
    // discoverAuthServer uses getJson under the hood.
    await assert.rejects(
      () => discoverAuthServer(fn, 'https://mcp.example.com'),
      (err: Error) => {
        assert.match(err.message, /non-JSON response/);
        assert.match(err.message, /captive portal or proxy/);
        assert.match(err.message, /https:\/\/mcp\.example\.com/);
        assert.doesNotMatch(err.name, /SyntaxError/);
        return true;
      },
    );
  });
  it('still reports an HTTP status error for a non-200 (unchanged behavior)', async () => {
    const url = 'https://mcp.example.com/.well-known/oauth-authorization-server';
    const { fn } = fakeFetch({ [url]: { ok: false, status: 503, throwJson: true } });
    await assert.rejects(
      () => discoverAuthServer(fn, 'https://mcp.example.com'),
      /OAuth discovery failed \(503\)/,
    );
  });
});

// ---------------------------------------------------------------------------
// Gap 3 — captive-portal / non-JSON token response
// ---------------------------------------------------------------------------
describe('W34 postForm — captive-portal non-JSON token-response diagnosis', () => {
  it('throws a captive-portal hint on a 2xx non-JSON token body', async () => {
    // RED (pre-fix): a 200 non-JSON body became {} → "token response missing access_token".
    const { fn } = fakeFetch({ tok: { ok: true, status: 200, throwJson: true } });
    await assert.rejects(
      () => refreshToken(fn, { tokenEndpoint: 'tok', refreshToken: 'RT', clientId: 'c', resource: 'rs' }, NOW),
      (err: Error) => {
        assert.match(err.message, /non-JSON response/);
        assert.match(err.message, /captive portal or proxy/);
        assert.match(err.message, /tok/);
        return true;
      },
    );
  });
  it('a non-2xx non-JSON body still surfaces the HTTP/token error (unchanged)', async () => {
    const { fn } = fakeFetch({ tok: { ok: false, status: 500, throwJson: true } });
    await assert.rejects(
      () => refreshToken(fn, { tokenEndpoint: 'tok', refreshToken: 'RT', clientId: 'c', resource: 'rs' }, NOW),
      /token endpoint error \(500\)/,
    );
  });
});

// ---------------------------------------------------------------------------
// Gap 4 — grant_types_supported + token_endpoint_auth_methods_supported
// ---------------------------------------------------------------------------
describe('W34 pickRegistrationAuthMethod', () => {
  it('defaults to none when nothing is advertised', () => {
    assert.equal(pickRegistrationAuthMethod(undefined), 'none');
    assert.equal(pickRegistrationAuthMethod([]), 'none');
  });
  it('prefers none when the server supports it', () => {
    assert.equal(pickRegistrationAuthMethod(['client_secret_post', 'none']), 'none');
  });
  it('falls back to client_secret_post then basic when none is unsupported', () => {
    assert.equal(pickRegistrationAuthMethod(['client_secret_post', 'client_secret_basic']), 'client_secret_post');
    assert.equal(pickRegistrationAuthMethod(['client_secret_basic']), 'client_secret_basic');
  });
  it('defaults to none for an exotic-only list', () => {
    assert.equal(pickRegistrationAuthMethod(['private_key_jwt']), 'none');
  });
});

describe('W34 discoverAuthServer — parses grant_types/auth-methods', () => {
  it('captures grant_types_supported and token_endpoint_auth_methods_supported', async () => {
    const url = 'https://as.example.com/.well-known/oauth-authorization-server';
    const { fn } = fakeFetch({
      [url]: {
        body: {
          issuer: 'https://as.example.com',
          authorization_endpoint: 'https://as.example.com/authorize',
          token_endpoint: 'https://as.example.com/token',
          grant_types_supported: ['authorization_code', 'refresh_token'],
          token_endpoint_auth_methods_supported: ['client_secret_post', 'none'],
        },
      },
    });
    const meta = await discoverAuthServer(fn, 'https://as.example.com');
    assert.deepEqual(meta.grantTypesSupported, ['authorization_code', 'refresh_token']);
    assert.deepEqual(meta.tokenEndpointAuthMethodsSupported, ['client_secret_post', 'none']);
  });
});

describe('W34 registerClient — honors advertised auth methods', () => {
  it('registers client_secret_post when the server omits none', async () => {
    const { fn, calls } = fakeFetch({ reg: { body: { client_id: 'cid' } } });
    const res = await registerClient(fn, 'reg', ['https://127.0.0.1:1234/cb'], undefined, ['client_secret_post']);
    assert.equal(res.clientId, 'cid');
    const body = JSON.parse(calls[0].init!.body!);
    assert.equal(body.token_endpoint_auth_method, 'client_secret_post');
  });
  it('registers none by default (no advertised list)', async () => {
    const { fn, calls } = fakeFetch({ reg: { body: { client_id: 'cid' } } });
    await registerClient(fn, 'reg', ['https://127.0.0.1:1234/cb']);
    const body = JSON.parse(calls[0].init!.body!);
    assert.equal(body.token_endpoint_auth_method, 'none');
  });
});

describe('W34 McpOAuthSession — rejects an AS that cannot do authorization_code', () => {
  it('onUnauthorized returns false when grant_types_supported excludes authorization_code', async () => {
    const serverUrl = 'https://mcp.example.com';
    const prmUrl = 'https://mcp.example.com/.well-known/oauth-protected-resource';
    const asUrl = 'https://as.example.com/.well-known/oauth-authorization-server';
    let authorizeCalled = false;
    const { fn } = fakeFetch({
      [prmUrl]: { body: { resource: 'https://mcp.example.com', authorization_servers: ['https://as.example.com'] } },
      [asUrl]: {
        body: {
          issuer: 'https://as.example.com',
          authorization_endpoint: 'https://as.example.com/authorize',
          token_endpoint: 'https://as.example.com/token',
          grant_types_supported: ['client_credentials'],
        },
      },
    });
    const session = new McpOAuthSession({
      serverUrl,
      fetchImpl: fn,
      redirectUri: 'https://127.0.0.1:9999/cb',
      clientId: 'cid',
      authorize: async () => { authorizeCalled = true; return { code: 'x' }; },
      now: () => NOW,
    });
    const ok = await session.onUnauthorized('Bearer');
    assert.equal(ok, false);
    assert.equal(authorizeCalled, false, 'must not drive consent against an incapable AS');
  });
});

// ---------------------------------------------------------------------------
// Gap 5 — path-aware RFC 8414 fallback when RFC 9728 discovery fails
// ---------------------------------------------------------------------------
describe('W34 McpOAuthSession — path-aware RFC 8414 fallback', () => {
  const serverUrl = 'https://mcp.example.com/tenant1';
  const prmUrl = 'https://mcp.example.com/.well-known/oauth-protected-resource/tenant1';
  const asUrl = 'https://mcp.example.com/.well-known/oauth-authorization-server/tenant1';
  const tokenUrl = 'https://mcp.example.com/token';

  it('falls back to a path-aware AS probe and completes consent when PRM 404s', async () => {
    const { fn, calls } = fakeFetch({
      // RFC 9728 protected-resource discovery is absent on this legacy server.
      [prmUrl]: { ok: false, status: 404 },
      // but RFC 8414 AS metadata is co-hosted path-aware on the server itself.
      [asUrl]: {
        body: {
          issuer: 'https://mcp.example.com/tenant1',
          authorization_endpoint: 'https://mcp.example.com/authorize',
          token_endpoint: tokenUrl,
        },
      },
      [tokenUrl]: { body: { access_token: 'AT', token_type: 'Bearer', expires_in: 3600 } },
    });
    const store = new OAuthTokenStore();
    const session = new McpOAuthSession({
      serverUrl,
      fetchImpl: fn,
      redirectUri: 'https://127.0.0.1:9999/cb',
      clientId: 'cid',
      authorize: async () => ({ code: 'authcode' }),
      now: () => NOW,
      store,
    });
    const ok = await session.onUnauthorized('Bearer');
    assert.equal(ok, true, 'fallback must authenticate a legacy RFC-8414-only server');
    assert.equal(await session.authorizationHeader(), 'Bearer AT');
    // The RFC 8707 resource indicator on the token exchange used the server URL.
    const tokenCall = calls.find((c) => c.url === tokenUrl)!;
    assert.match(tokenCall.init!.body!, /resource=https%3A%2F%2Fmcp\.example\.com%2Ftenant1/);
  });

  it('does NOT fall back for a root (pathless) server URL — onUnauthorized returns false', async () => {
    const rootServer = 'https://mcp.example.com';
    const rootPrm = 'https://mcp.example.com/.well-known/oauth-protected-resource';
    let authorizeCalled = false;
    const { fn } = fakeFetch({ [rootPrm]: { ok: false, status: 404 } });
    const session = new McpOAuthSession({
      serverUrl: rootServer,
      fetchImpl: fn,
      redirectUri: 'https://127.0.0.1:9999/cb',
      clientId: 'cid',
      authorize: async () => { authorizeCalled = true; return { code: 'x' }; },
      now: () => NOW,
    });
    const ok = await session.onUnauthorized('Bearer');
    assert.equal(ok, false);
    assert.equal(authorizeCalled, false);
  });

  it('does NOT fall back when a metadata URL is pinned — onUnauthorized returns false', async () => {
    const pinned = 'https://as.example.com/custom/metadata';
    const { fn } = fakeFetch({ [prmUrl]: { ok: false, status: 404 } });
    const session = new McpOAuthSession({
      serverUrl,
      fetchImpl: fn,
      redirectUri: 'https://127.0.0.1:9999/cb',
      clientId: 'cid',
      authServerMetadataUrl: pinned,
      authorize: async () => ({ code: 'x' }),
      now: () => NOW,
    });
    const ok = await session.onUnauthorized('Bearer');
    assert.equal(ok, false);
  });
});

describe('W34 hasUrlPath', () => {
  it('is true only for a non-empty path component', () => {
    assert.equal(hasUrlPath('https://h.example.com/tenant1'), true);
    assert.equal(hasUrlPath('https://h.example.com'), false);
    assert.equal(hasUrlPath('https://h.example.com/'), false);
    assert.equal(hasUrlPath('not a url'), false);
  });
});
