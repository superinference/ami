/**
 * mcp-oauth-parity-c.test.ts — red-green reproducers for the MCP OAuth parity
 * batch on core/core/src/mcp/oauth.ts:
 *   - generateState 256-bit entropy (F1345)
 *   - decodeJwtExpiry + toTokens JWT-exp fallback when expires_in is absent (F0315)
 *   - normalizeOAuthUrl / assertIssuerMatch + discoverAuthServer RFC 8414 §3.3
 *     issuer mix-up protection (F1338)
 *   - McpOAuthSession CSRF state validation on the consent callback (F0181)
 *   - McpOAuthSession single-flight refresh dedup (F0315/F1317/F1875/F2019)
 *
 * Each test discriminates the fix from the prior behavior.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  generateState, decodeJwtExpiry, normalizeOAuthUrl, assertIssuerMatch,
  discoverAuthServer, exchangeCode, McpOAuthSession,
} from '../src/mcp/oauth';
import type { FetchLike } from '../src/mcp/oauth';

const b64url = (s: Buffer) => s.toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
const jwt = (payload: object) =>
  `${b64url(Buffer.from('{"alg":"none"}'))}.${b64url(Buffer.from(JSON.stringify(payload)))}.sig`;

function fakeFetch(routes: Record<string, { ok?: boolean; status?: number; body: unknown }>): { fn: FetchLike; calls: Array<{ url: string }> } {
  const calls: Array<{ url: string }> = [];
  const fn: FetchLike = async (url) => {
    calls.push({ url });
    const r = routes[url];
    if (!r) return { ok: false, status: 404, json: async () => ({}) };
    return { ok: r.ok ?? true, status: r.status ?? 200, json: async () => r.body };
  };
  return { fn, calls };
}

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

describe('generateState entropy (F1345)', () => {
  it('seeds state with 256 bits (base64url of 32 bytes = 43 chars)', () => {
    // RED: randomBytes(16) -> 22-char value (128-bit). GREEN: 43-char (256-bit).
    const s = generateState();
    assert.equal(s.length, 43);
    assert.doesNotMatch(s, /[+/=]/);
    assert.equal(Buffer.from(s.replace(/-/g, '+').replace(/_/g, '/'), 'base64').length, 32);
  });
});

describe('decodeJwtExpiry (F0315)', () => {
  it('returns exp (seconds) as absolute epoch-ms for a JWT with a numeric exp', () => {
    assert.equal(decodeJwtExpiry(jwt({ exp: 2_000_000_000 })), 2_000_000_000_000);
  });
  it('returns undefined for non-JWT, non-JSON payload, and missing/non-numeric exp', () => {
    assert.equal(decodeJwtExpiry('opaque-token'), undefined);
    assert.equal(decodeJwtExpiry('a.b'), undefined);
    assert.equal(decodeJwtExpiry(`x.${b64url(Buffer.from('not json'))}.y`), undefined);
    assert.equal(decodeJwtExpiry(jwt({ sub: 'u' })), undefined);
    assert.equal(decodeJwtExpiry(jwt({ exp: 'soon' })), undefined);
  });
});

describe('toTokens JWT-exp fallback (F0315)', () => {
  it('derives expiresAt from the access token JWT exp when expires_in is absent', async () => {
    // RED: expiresAt undefined (token treated as non-expiring). GREEN: exp*1000.
    const { fn } = fakeFetch({ u: { body: { access_token: jwt({ exp: 2_000_000_000 }), token_type: 'Bearer' } } });
    const tok = await exchangeCode(fn, { tokenEndpoint: 'u', code: 'c', verifier: 'v', clientId: 'cid', redirectUri: 'r', resource: 'rs' }, 1000);
    assert.equal(tok.expiresAt, 2_000_000_000_000);
  });
  it('still prefers expires_in over the JWT exp when both are present', async () => {
    const { fn } = fakeFetch({ u: { body: { access_token: jwt({ exp: 2_000_000_000 }), token_type: 'Bearer', expires_in: 60 } } });
    const tok = await exchangeCode(fn, { tokenEndpoint: 'u', code: 'c', verifier: 'v', clientId: 'cid', redirectUri: 'r', resource: 'rs' }, 1000);
    assert.equal(tok.expiresAt, 1000 + 60 * 1000);
  });
});

describe('normalizeOAuthUrl / assertIssuerMatch (F1338)', () => {
  it('normalizes scheme/host case, default port, and trailing slash', () => {
    assert.equal(normalizeOAuthUrl('HTTPS://AS.Example:443/Tenant/'), 'https://as.example/Tenant');
    assert.equal(normalizeOAuthUrl('https://as.example'), 'https://as.example');
  });
  it('accepts issuers that differ only cosmetically and rejects a different origin', () => {
    assert.doesNotThrow(() => assertIssuerMatch('https://AS.example/', 'https://as.example'));
    assert.throws(() => assertIssuerMatch('https://evil.example', 'https://as.example'), /issuer mismatch/);
    assert.throws(() => assertIssuerMatch('not a url', 'https://as.example'), /not a valid URL/);
  });
});

describe('discoverAuthServer RFC 8414 §3.3 issuer mix-up protection (F1338)', () => {
  it('rejects metadata whose issuer differs from the URL it was fetched from', async () => {
    // RED: returns metadata trusting evil.example endpoints. GREEN: throws.
    const { fn } = fakeFetch({
      'https://as.example/.well-known/oauth-authorization-server': {
        body: { issuer: 'https://evil.example', authorization_endpoint: 'https://evil.example/authorize', token_endpoint: 'https://evil.example/token' },
      },
    });
    await assert.rejects(() => discoverAuthServer(fn, 'https://as.example'), /issuer mismatch/);
  });
  it('accepts matching issuer (cosmetic trailing-slash difference ok)', async () => {
    const { fn } = fakeFetch({
      'https://as.example/.well-known/oauth-authorization-server': {
        body: { issuer: 'https://as.example/', authorization_endpoint: 'https://as.example/authorize', token_endpoint: 'https://as.example/token' },
      },
    });
    const md = await discoverAuthServer(fn, 'https://as.example');
    assert.equal(md.tokenEndpoint, 'https://as.example/token');
  });
});

describe('McpOAuthSession CSRF state validation (F0181)', () => {
  it('rejects the flow when the consent leg echoes a mismatched state', async () => {
    // RED: state never checked -> onUnauthorized returns true. GREEN: false.
    const { fn } = fakeFetch(fullRoutes());
    const session = new McpOAuthSession({
      serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid',
      authorize: async () => ({ code: 'c', state: 'TAMPERED' }), now: () => 0,
    });
    assert.equal(await session.onUnauthorized(null), false);
    assert.equal(await session.authorizationHeader(), null);
  });
  it('accepts the flow when the consent leg echoes the matching state', async () => {
    const { fn } = fakeFetch(fullRoutes());
    const session = new McpOAuthSession({
      serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid',
      authorize: async ({ state }) => ({ code: 'c', state }), now: () => 0,
    });
    assert.equal(await session.onUnauthorized(null), true);
    assert.equal(await session.authorizationHeader(), 'Bearer AT');
  });
  it('preserves legacy behavior when the consent leg omits state', async () => {
    const { fn } = fakeFetch(fullRoutes());
    const session = new McpOAuthSession({
      serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid',
      authorize: async () => ({ code: 'c' }), now: () => 0,
    });
    assert.equal(await session.onUnauthorized(null), true);
  });
});

describe('McpOAuthSession single-flight refresh dedup (F0315/F1317/F1875/F2019)', () => {
  it('coalesces concurrent refreshes onto a single token POST', async () => {
    let t = 0;
    const { fn, calls } = fakeFetch(fullRoutes({
      'https://as.example/token': { body: { access_token: 'AT1', token_type: 'Bearer', refresh_token: 'RT', expires_in: 100 } },
    }));
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', authorize: async () => ({ code: 'c' }), now: () => t });
    await session.onUnauthorized(null); // obtain AT1 (one token POST = code exchange)
    t = 200_000; // past expiry
    const before = calls.filter((c) => c.url === 'https://as.example/token').length;
    // Fire a burst while expired; without dedup each call POSTs the single-use RT.
    const headers = await Promise.all([
      session.authorizationHeader(), session.authorizationHeader(), session.authorizationHeader(),
    ]);
    const refreshPosts = calls.filter((c) => c.url === 'https://as.example/token').length - before;
    // RED: 3 refresh POSTs (one per caller). GREEN: exactly 1.
    assert.equal(refreshPosts, 1);
    assert.deepEqual(headers, ['Bearer AT1', 'Bearer AT1', 'Bearer AT1']);
  });
  it('clears the in-flight promise so a later refresh runs again', async () => {
    let t = 0;
    const { fn, calls } = fakeFetch(fullRoutes({
      'https://as.example/token': { body: { access_token: 'AT1', token_type: 'Bearer', refresh_token: 'RT', expires_in: 100 } },
    }));
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', authorize: async () => ({ code: 'c' }), now: () => t });
    await session.onUnauthorized(null);
    t = 200_000;
    await session.authorizationHeader(); // refresh #1
    const after1 = calls.filter((c) => c.url === 'https://as.example/token').length;
    t = 400_000;
    await session.authorizationHeader(); // refresh #2 (in-flight was cleared)
    const after2 = calls.filter((c) => c.url === 'https://as.example/token').length;
    assert.equal(after2, after1 + 1);
  });
});
