/**
 * mcp-oauth-xaa.test.ts — Cross-App Access (XAA / SEP-990) browserless enterprise
 * SSO for MCP OAuth (core/core/src/mcp/oauth.ts + manager wiring).
 *
 * Covers the gaps:
 *   - XaaConfig input shape for browserless enterprise SSO token exchange.
 *   - RFC 8693 token-exchange at the IdP (id_token → ID-JAG).
 *   - RFC 7523 jwt-bearer grant at the AS (ID-JAG → access_token).
 *   - performCrossAppAccess orchestrator (PRM → AS → exchange → jwt-bearer).
 *   - login via direct id_token injection (idpIdToken) — no browser consent.
 *   - CLAUDE_CODE_ENABLE_XAA-equivalent env gate (MCP_ENABLE_XAA).
 *   - McpOAuthSession routing through the browserless XAA path + manager wiring.
 *
 * All network I/O is through an injected FetchLike so these are pure and fast.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  TOKEN_EXCHANGE_GRANT, JWT_BEARER_GRANT, ID_JAG_TOKEN_TYPE, ID_TOKEN_TYPE,
  MCP_ENABLE_XAA_ENV, isXaaEnabled,
  requestJwtAuthorizationGrant, exchangeJwtAuthGrant, performCrossAppAccess,
  McpOAuthSession, OAuthError,
} from '../src/mcp/oauth';
import type { FetchLike, XaaConfig } from '../src/mcp/oauth';

function recordingFetch(routes: Record<string, { ok?: boolean; status?: number; body: unknown }>) {
  const calls: Array<{ url: string; init?: { method?: string; headers?: Record<string, string>; body?: string } }> = [];
  const fn: FetchLike = async (url, init) => {
    calls.push({ url, init });
    const r = routes[url];
    if (!r) return { ok: false, status: 404, json: async () => ({ error: 'not_found' }) };
    return { ok: r.ok ?? true, status: r.status ?? 200, json: async () => r.body };
  };
  return { fn, calls };
}

const XAA: XaaConfig = {
  clientId: 'as-client',
  clientSecret: 'as-secret',
  idpClientId: 'idp-client',
  idpClientSecret: 'idp-secret',
  idpIdToken: 'the-id-token',
  idpTokenEndpoint: 'https://idp.example.com/token',
};

// ---------------------------------------------------------------------------
// Grant-type + token-type constants
// ---------------------------------------------------------------------------
describe('XAA constants + env gate', () => {
  it('exposes the SEP-990 grant and token type URNs', () => {
    assert.equal(TOKEN_EXCHANGE_GRANT, 'urn:ietf:params:oauth:grant-type:token-exchange');
    assert.equal(JWT_BEARER_GRANT, 'urn:ietf:params:oauth:grant-type:jwt-bearer');
    assert.equal(ID_JAG_TOKEN_TYPE, 'urn:ietf:params:oauth:token-type:id-jag');
    assert.equal(ID_TOKEN_TYPE, 'urn:ietf:params:oauth:token-type:id_token');
  });

  it('isXaaEnabled is off by default and honours truthy env values', () => {
    assert.equal(isXaaEnabled({}), false);
    assert.equal(isXaaEnabled({ [MCP_ENABLE_XAA_ENV]: '' }), false);
    assert.equal(isXaaEnabled({ [MCP_ENABLE_XAA_ENV]: '0' }), false);
    assert.equal(isXaaEnabled({ [MCP_ENABLE_XAA_ENV]: 'false' }), false);
    assert.equal(isXaaEnabled({ [MCP_ENABLE_XAA_ENV]: '1' }), true);
    assert.equal(isXaaEnabled({ [MCP_ENABLE_XAA_ENV]: 'true' }), true);
    assert.equal(isXaaEnabled({ [MCP_ENABLE_XAA_ENV]: 'YES' }), true);
    assert.equal(isXaaEnabled({ [MCP_ENABLE_XAA_ENV]: ' On ' }), true);
  });
});

// ---------------------------------------------------------------------------
// RFC 8693 token exchange (requestJwtAuthorizationGrant)
// ---------------------------------------------------------------------------
describe('requestJwtAuthorizationGrant (RFC 8693)', () => {
  const endpoint = 'https://idp.example.com/token';

  it('POSTs the token-exchange grant and returns the ID-JAG', async () => {
    const { fn, calls } = recordingFetch({
      [endpoint]: { body: { access_token: 'the-id-jag', issued_token_type: ID_JAG_TOKEN_TYPE, expires_in: 300, scope: 'mcp:read' } },
    });
    const res = await requestJwtAuthorizationGrant(fn, {
      tokenEndpoint: endpoint, audience: 'https://as.example.com', resource: 'https://mcp.example.com',
      idToken: 'id-tok', clientId: 'idp-client', clientSecret: 'idp-secret', scope: 'mcp:read',
    });
    assert.equal(res.jwtAuthGrant, 'the-id-jag');
    assert.equal(res.expiresIn, 300);
    assert.equal(res.scope, 'mcp:read');
    const body = new URLSearchParams(calls[0].init!.body!);
    assert.equal(body.get('grant_type'), TOKEN_EXCHANGE_GRANT);
    assert.equal(body.get('requested_token_type'), ID_JAG_TOKEN_TYPE);
    assert.equal(body.get('subject_token'), 'id-tok');
    assert.equal(body.get('subject_token_type'), ID_TOKEN_TYPE);
    assert.equal(body.get('client_secret'), 'idp-secret');
    assert.equal(body.get('scope'), 'mcp:read');
  });

  it('omits client_secret/scope when not provided', async () => {
    const { fn, calls } = recordingFetch({
      [endpoint]: { body: { access_token: 'g', issued_token_type: ID_JAG_TOKEN_TYPE } },
    });
    await requestJwtAuthorizationGrant(fn, {
      tokenEndpoint: endpoint, audience: 'a', resource: 'r', idToken: 't', clientId: 'c',
    });
    const body = new URLSearchParams(calls[0].init!.body!);
    assert.equal(body.has('client_secret'), false);
    assert.equal(body.has('scope'), false);
  });

  it('rejects a plaintext (non-loopback) IdP token endpoint', async () => {
    const { fn } = recordingFetch({});
    await assert.rejects(
      () => requestJwtAuthorizationGrant(fn, { tokenEndpoint: 'http://idp.example.com/token', audience: 'a', resource: 'r', idToken: 't', clientId: 'c' }),
      /https:\/\//,
    );
  });

  it('surfaces a server error body as an OAuthError', async () => {
    const { fn } = recordingFetch({
      [endpoint]: { ok: false, status: 400, body: { error: 'invalid_grant', error_description: 'id_token expired' } },
    });
    await assert.rejects(
      () => requestJwtAuthorizationGrant(fn, { tokenEndpoint: endpoint, audience: 'a', resource: 'r', idToken: 't', clientId: 'c' }),
      (e: unknown) => e instanceof OAuthError && e.code === 'invalid_grant',
    );
  });

  it('rejects a non-JSON (captive portal) response', async () => {
    const fn: FetchLike = async () => ({ ok: true, status: 200, json: async () => { throw new Error('not json'); } });
    await assert.rejects(
      () => requestJwtAuthorizationGrant(fn, { tokenEndpoint: endpoint, audience: 'a', resource: 'r', idToken: 't', clientId: 'c' }),
      /non-JSON/,
    );
  });

  it('rejects a response missing access_token', async () => {
    const { fn } = recordingFetch({ [endpoint]: { body: { issued_token_type: ID_JAG_TOKEN_TYPE } } });
    await assert.rejects(
      () => requestJwtAuthorizationGrant(fn, { tokenEndpoint: endpoint, audience: 'a', resource: 'r', idToken: 't', clientId: 'c' }),
      /missing access_token/,
    );
  });

  it('rejects an unexpected issued_token_type', async () => {
    const { fn } = recordingFetch({ [endpoint]: { body: { access_token: 'x', issued_token_type: 'urn:something:else' } } });
    await assert.rejects(
      () => requestJwtAuthorizationGrant(fn, { tokenEndpoint: endpoint, audience: 'a', resource: 'r', idToken: 't', clientId: 'c' }),
      /unexpected issued_token_type/,
    );
  });
});

// ---------------------------------------------------------------------------
// RFC 7523 jwt-bearer grant (exchangeJwtAuthGrant)
// ---------------------------------------------------------------------------
describe('exchangeJwtAuthGrant (RFC 7523)', () => {
  const endpoint = 'https://as.example.com/token';

  it('defaults to client_secret_basic (Authorization header, no body creds)', async () => {
    const { fn, calls } = recordingFetch({
      [endpoint]: { body: { access_token: 'at', token_type: 'Bearer', expires_in: 3600 } },
    });
    const tok = await exchangeJwtAuthGrant(fn, { tokenEndpoint: endpoint, assertion: 'jag', clientId: 'c', clientSecret: 's', scope: 'mcp' }, 1000);
    assert.equal(tok.accessToken, 'at');
    assert.equal(tok.tokenType, 'Bearer');
    assert.equal(tok.expiresAt, 1000 + 3600 * 1000);
    const init = calls[0].init!;
    assert.ok(init.headers!.Authorization.startsWith('Basic '));
    const decoded = Buffer.from(init.headers!.Authorization.slice(6), 'base64').toString();
    assert.equal(decoded, 'c:s');
    const body = new URLSearchParams(init.body!);
    assert.equal(body.get('grant_type'), JWT_BEARER_GRANT);
    assert.equal(body.get('assertion'), 'jag');
    assert.equal(body.has('client_secret'), false);
    assert.equal(body.get('scope'), 'mcp');
  });

  it('uses client_secret_post when requested (creds in body, no header)', async () => {
    const { fn, calls } = recordingFetch({ [endpoint]: { body: { access_token: 'at', token_type: 'Bearer' } } });
    await exchangeJwtAuthGrant(fn, { tokenEndpoint: endpoint, assertion: 'jag', clientId: 'c', clientSecret: 's', authMethod: 'client_secret_post' }, 0);
    const init = calls[0].init!;
    assert.equal(init.headers!.Authorization, undefined);
    const body = new URLSearchParams(init.body!);
    assert.equal(body.get('client_id'), 'c');
    assert.equal(body.get('client_secret'), 's');
  });

  it('throws on a non-JSON response and on an error body', async () => {
    const fn1: FetchLike = async () => ({ ok: true, status: 200, json: async () => { throw new Error('html'); } });
    await assert.rejects(() => exchangeJwtAuthGrant(fn1, { tokenEndpoint: endpoint, assertion: 'j', clientId: 'c', clientSecret: 's' }, 0), /non-JSON/);
    const { fn: fn2 } = recordingFetch({ [endpoint]: { ok: false, status: 401, body: { error: 'invalid_client' } } });
    await assert.rejects(() => exchangeJwtAuthGrant(fn2, { tokenEndpoint: endpoint, assertion: 'j', clientId: 'c', clientSecret: 's' }, 0), (e: unknown) => e instanceof OAuthError && e.code === 'invalid_client');
  });

  it('rejects a plaintext token endpoint', async () => {
    const { fn } = recordingFetch({});
    await assert.rejects(() => exchangeJwtAuthGrant(fn, { tokenEndpoint: 'http://as.example.com/token', assertion: 'j', clientId: 'c', clientSecret: 's' }, 0), /https:\/\//);
  });
});

// ---------------------------------------------------------------------------
// performCrossAppAccess orchestrator
// ---------------------------------------------------------------------------
describe('performCrossAppAccess', () => {
  const serverUrl = 'https://mcp.example.com';
  const prmUrl = 'https://mcp.example.com/.well-known/oauth-protected-resource';
  const asWellKnown = 'https://as.example.com/.well-known/oauth-authorization-server';

  function fullRoutes(extra?: Record<string, { ok?: boolean; status?: number; body: unknown }>) {
    return {
      [prmUrl]: { body: { resource: 'https://mcp.example.com', authorization_servers: ['https://as.example.com'] } },
      [asWellKnown]: { body: { issuer: 'https://as.example.com', authorization_endpoint: 'https://as.example.com/auth', token_endpoint: 'https://as.example.com/token', grant_types_supported: ['authorization_code', JWT_BEARER_GRANT] } },
      [XAA.idpTokenEndpoint]: { body: { access_token: 'the-jag', issued_token_type: ID_JAG_TOKEN_TYPE } },
      'https://as.example.com/token': { body: { access_token: 'final-at', token_type: 'Bearer', expires_in: 3600, scope: 'mcp' } },
      ...extra,
    };
  }

  it('runs the full PRM → AS → exchange → jwt-bearer chain', async () => {
    const { fn, calls } = recordingFetch(fullRoutes());
    const res = await performCrossAppAccess(fn, serverUrl, XAA, 5000);
    assert.equal(res.tokens.accessToken, 'final-at');
    assert.equal(res.resource, 'https://mcp.example.com');
    assert.equal(res.authorizationServerUrl, 'https://as.example.com');
    assert.equal(res.authServer.tokenEndpoint, 'https://as.example.com/token');
    // The id-token was used as subject_token at the IdP.
    const idpCall = calls.find((c) => c.url === XAA.idpTokenEndpoint)!;
    assert.equal(new URLSearchParams(idpCall.init!.body!).get('subject_token'), 'the-id-token');
    // The ID-JAG became the assertion at the AS.
    const asCall = calls.find((c) => c.url === 'https://as.example.com/token')!;
    assert.equal(new URLSearchParams(asCall.init!.body!).get('assertion'), 'the-jag');
  });

  it('skips an AS that explicitly omits the jwt-bearer grant and uses the next', async () => {
    const routes = {
      [prmUrl]: { body: { resource: 'https://mcp.example.com', authorization_servers: ['https://as1.example.com', 'https://as.example.com'] } },
      'https://as1.example.com/.well-known/oauth-authorization-server': { body: { issuer: 'https://as1.example.com', authorization_endpoint: 'https://as1.example.com/auth', token_endpoint: 'https://as1.example.com/token', grant_types_supported: ['authorization_code'] } },
      [asWellKnown]: { body: { issuer: 'https://as.example.com', authorization_endpoint: 'https://as.example.com/auth', token_endpoint: 'https://as.example.com/token' } },
      [XAA.idpTokenEndpoint]: { body: { access_token: 'the-jag', issued_token_type: ID_JAG_TOKEN_TYPE } },
      'https://as.example.com/token': { body: { access_token: 'final-at', token_type: 'Bearer' } },
    };
    const { fn } = recordingFetch(routes);
    const res = await performCrossAppAccess(fn, serverUrl, XAA, 0);
    assert.equal(res.authorizationServerUrl, 'https://as.example.com');
  });

  it('throws when no advertised AS can perform jwt-bearer', async () => {
    const routes = {
      [prmUrl]: { body: { resource: 'https://mcp.example.com', authorization_servers: ['https://as1.example.com'] } },
      'https://as1.example.com/.well-known/oauth-authorization-server': { body: { issuer: 'https://as1.example.com', authorization_endpoint: 'https://as1.example.com/auth', token_endpoint: 'https://as1.example.com/token', grant_types_supported: ['authorization_code'] } },
    };
    const { fn } = recordingFetch(routes);
    await assert.rejects(() => performCrossAppAccess(fn, serverUrl, XAA, 0), /no authorization server supports jwt-bearer/);
  });

  it('skips an AS whose metadata discovery fails and records the reason', async () => {
    const routes = {
      [prmUrl]: { body: { resource: 'https://mcp.example.com', authorization_servers: ['https://dead.example.com'] } },
    };
    const { fn } = recordingFetch(routes);
    await assert.rejects(() => performCrossAppAccess(fn, serverUrl, XAA, 0), /no authorization server supports jwt-bearer.*dead\.example\.com/);
  });

  it('picks client_secret_post when the AS advertises only post', async () => {
    const { fn, calls } = recordingFetch(fullRoutes({
      [asWellKnown]: { body: { issuer: 'https://as.example.com', authorization_endpoint: 'https://as.example.com/auth', token_endpoint: 'https://as.example.com/token', token_endpoint_auth_methods_supported: ['client_secret_post'] } },
    }));
    await performCrossAppAccess(fn, serverUrl, XAA, 0);
    const asCall = calls.find((c) => c.url === 'https://as.example.com/token')!;
    assert.equal(asCall.init!.headers!.Authorization, undefined);
    assert.equal(new URLSearchParams(asCall.init!.body!).get('client_secret'), 'as-secret');
  });
});

// ---------------------------------------------------------------------------
// McpOAuthSession XAA routing (browserless path + env gate)
// ---------------------------------------------------------------------------
describe('McpOAuthSession browserless XAA routing', () => {
  const serverUrl = 'https://mcp.example.com';
  const prmUrl = 'https://mcp.example.com/.well-known/oauth-protected-resource';
  const asWellKnown = 'https://as.example.com/.well-known/oauth-authorization-server';

  const routes = {
    [prmUrl]: { body: { resource: 'https://mcp.example.com', authorization_servers: ['https://as.example.com'] } },
    [asWellKnown]: { body: { issuer: 'https://as.example.com', authorization_endpoint: 'https://as.example.com/auth', token_endpoint: 'https://as.example.com/token' } },
    [XAA.idpTokenEndpoint]: { body: { access_token: 'the-jag', issued_token_type: ID_JAG_TOKEN_TYPE } },
    'https://as.example.com/token': { body: { access_token: 'xaa-access-token', token_type: 'Bearer', expires_in: 3600 } },
  };

  it('authenticates via the token-exchange flow without calling authorize() when gated on', async () => {
    const { fn } = recordingFetch(routes);
    let authorizeCalled = false;
    const session = new McpOAuthSession({
      serverUrl, fetchImpl: fn, redirectUri: 'http://localhost:9999/callback',
      authorize: async () => { authorizeCalled = true; return { code: 'should-not-happen' }; },
      xaa: XAA, env: { [MCP_ENABLE_XAA_ENV]: '1' }, now: () => 0,
    });
    const ok = await session.onUnauthorized(null);
    assert.equal(ok, true);
    assert.equal(authorizeCalled, false, 'browser consent must be skipped on the XAA path');
    assert.equal(await session.authorizationHeader(), 'Bearer xaa-access-token');
  });

  it('falls back to the interactive flow when the env gate is OFF', async () => {
    // No XAA routes needed — authorize() should be invoked instead. Provide a minimal
    // interactive route set so the browser flow completes.
    const iroutes = {
      [prmUrl]: { body: { resource: 'https://mcp.example.com', authorization_servers: ['https://as.example.com'] } },
      [asWellKnown]: { body: { issuer: 'https://as.example.com', authorization_endpoint: 'https://as.example.com/auth', token_endpoint: 'https://as.example.com/token', registration_endpoint: 'https://as.example.com/register' } },
      'https://as.example.com/register': { body: { client_id: 'dyn-client' } },
      'https://as.example.com/token': { body: { access_token: 'interactive-token', token_type: 'Bearer', expires_in: 3600 } },
    };
    const { fn } = recordingFetch(iroutes);
    let authorizeCalled = false;
    const session = new McpOAuthSession({
      serverUrl, fetchImpl: fn, redirectUri: 'http://localhost:9999/callback',
      authorize: async ({ state }) => { authorizeCalled = true; return { code: 'authcode', state }; },
      xaa: XAA, env: {}, now: () => 0,
    });
    const ok = await session.onUnauthorized(null);
    assert.equal(ok, true);
    assert.equal(authorizeCalled, true, 'gate off → interactive consent runs');
    assert.equal(await session.authorizationHeader(), 'Bearer interactive-token');
  });

  it('returns false when the XAA exchange fails (and does not throw)', async () => {
    const badRoutes = {
      [prmUrl]: { body: { resource: 'https://mcp.example.com', authorization_servers: ['https://as.example.com'] } },
      [asWellKnown]: { body: { issuer: 'https://as.example.com', authorization_endpoint: 'https://as.example.com/auth', token_endpoint: 'https://as.example.com/token' } },
      [XAA.idpTokenEndpoint]: { ok: false, status: 400, body: { error: 'invalid_grant' } },
    };
    const { fn } = recordingFetch(badRoutes);
    const steps: string[] = [];
    const session = new McpOAuthSession({
      serverUrl, fetchImpl: fn, redirectUri: 'http://localhost:9999/callback',
      authorize: async () => ({ code: 'x' }),
      xaa: XAA, env: { [MCP_ENABLE_XAA_ENV]: 'true' }, now: () => 0,
      onLog: (e) => steps.push(`${e.step}:${e.ok ?? ''}`),
    });
    const ok = await session.onUnauthorized(null);
    assert.equal(ok, false);
    assert.ok(steps.some((s) => s.startsWith('xaa-exchange')));
  });
});
