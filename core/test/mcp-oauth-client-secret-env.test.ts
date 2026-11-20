/**
 * mcp-oauth-client-secret-env.test.ts — U036 OAuth parity: confidential-client
 * secret sourced from the environment (parity with the reference's
 * `readClientSecret()` env precedence, services/mcp/auth.ts:L2362-L2397).
 *
 * AMI's OAuth path accepts a confidential `clientSecret` option but previously
 * required it to be inlined into the (plaintext) server config. This adds the
 * env-first resolver {@link resolveClientSecret} + {@link MCP_CLIENT_SECRET_ENV}
 * so a secret can be supplied via `MCP_CLIENT_SECRET` and never written to the
 * config, argv, or shell history. The resolver is wired into
 * {@link McpOAuthSession} so the env secret actually flows into the token/refresh
 * exchange (end-to-end), and is re-resolved on `invalidateCredentials('client')`.
 *
 * All network I/O is through an injected FetchLike so these are pure and fast.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  resolveClientSecret,
  MCP_CLIENT_SECRET_ENV,
  McpOAuthSession,
  OAuthTokenStore,
} from '../src/mcp/oauth';
import type {
  FetchLike, OAuthTokens, DiscoveryState, DiscoveryStateStore, AuthServerMetadata,
} from '../src/mcp/oauth';

// ---------------------------------------------------------------------------
// resolveClientSecret — precedence + blank handling
// ---------------------------------------------------------------------------
describe('resolveClientSecret', () => {
  it('exposes the MCP_CLIENT_SECRET env-var name', () => {
    assert.equal(MCP_CLIENT_SECRET_ENV, 'MCP_CLIENT_SECRET');
  });

  it('returns an explicitly-configured secret (it wins over the env)', () => {
    assert.equal(
      resolveClientSecret('CONFIG', { MCP_CLIENT_SECRET: 'ENV' }),
      'CONFIG',
    );
  });

  it('falls back to MCP_CLIENT_SECRET when no secret is configured', () => {
    assert.equal(resolveClientSecret(undefined, { MCP_CLIENT_SECRET: 'ENV' }), 'ENV');
  });

  it('treats a blank/whitespace configured value as absent and uses the env', () => {
    assert.equal(resolveClientSecret('   ', { MCP_CLIENT_SECRET: 'ENV' }), 'ENV');
  });

  it('treats a blank env value as absent (public client — undefined)', () => {
    assert.equal(resolveClientSecret(undefined, { MCP_CLIENT_SECRET: '  ' }), undefined);
  });

  it('returns undefined when neither source provides a secret', () => {
    assert.equal(resolveClientSecret(undefined, {}), undefined);
  });
});

// ---------------------------------------------------------------------------
// End-to-end: the env secret flows into the refresh token exchange
// ---------------------------------------------------------------------------
const AS_META: AuthServerMetadata = {
  issuer: 'https://as.example',
  authorizationEndpoint: 'https://as.example/authorize',
  tokenEndpoint: 'https://as.example/token',
};

function memDiscoveryStore(initial: DiscoveryState): DiscoveryStateStore {
  let value: DiscoveryState | null = initial;
  return {
    load() { return value; },
    save(s: DiscoveryState) { value = s; },
    clear() { value = null; },
  };
}

/** Captures the body posted to the token endpoint and answers with a fresh token. */
function captureToken(): { fn: FetchLike; bodies: string[] } {
  const bodies: string[] = [];
  const fn: FetchLike = async (url, init) => {
    if (url === AS_META.tokenEndpoint) {
      bodies.push(String((init as { body?: unknown } | undefined)?.body ?? ''));
      return { ok: true, status: 200, json: async () => ({ access_token: 'FRESH', token_type: 'Bearer', expires_in: 3600 }) };
    }
    return { ok: false, status: 404, json: async () => ({}) };
  };
  return { fn, bodies };
}

function expiredStore(): OAuthTokenStore {
  const store = new OAuthTokenStore();
  store.set({ accessToken: 'STALE', tokenType: 'Bearer', refreshToken: 'RT', expiresAt: -1, clientId: 'cid' } as OAuthTokens);
  return store;
}

describe('McpOAuthSession — MCP_CLIENT_SECRET env fallback (end-to-end)', () => {
  it('sends the env secret as client_secret on a silent refresh when none is configured', async () => {
    const { fn, bodies } = captureToken();
    const session = new McpOAuthSession({
      serverUrl: 'https://rs.example/mcp', fetchImpl: fn, redirectUri: 'http://127.0.0.1:9/cb',
      clientId: 'cid', store: expiredStore(), now: () => 0,
      env: { MCP_CLIENT_SECRET: 'ENVSECRET' },
      discoveryStore: memDiscoveryStore({ resource: 'https://rs.example/mcp', authServer: AS_META }),
      authorize: async () => { throw new Error('should not consent — a silent refresh must succeed'); },
    });
    const header = await session.authorizationHeader();
    assert.equal(header, 'Bearer FRESH');
    assert.equal(bodies.length, 1);
    const form = new URLSearchParams(bodies[0]);
    assert.equal(form.get('grant_type'), 'refresh_token');
    assert.equal(form.get('client_secret'), 'ENVSECRET');
  });

  it('a configured clientSecret still wins over the env var', async () => {
    const { fn, bodies } = captureToken();
    const session = new McpOAuthSession({
      serverUrl: 'https://rs.example/mcp', fetchImpl: fn, redirectUri: 'http://127.0.0.1:9/cb',
      clientId: 'cid', clientSecret: 'CONFIGSECRET', store: expiredStore(), now: () => 0,
      env: { MCP_CLIENT_SECRET: 'ENVSECRET' },
      discoveryStore: memDiscoveryStore({ resource: 'https://rs.example/mcp', authServer: AS_META }),
      authorize: async () => { throw new Error('should not consent'); },
    });
    assert.equal(await session.authorizationHeader(), 'Bearer FRESH');
    assert.equal(new URLSearchParams(bodies[0]).get('client_secret'), 'CONFIGSECRET');
  });

  it('stays a public client (no client_secret) when neither config nor env supply one', async () => {
    const { fn, bodies } = captureToken();
    const session = new McpOAuthSession({
      serverUrl: 'https://rs.example/mcp', fetchImpl: fn, redirectUri: 'http://127.0.0.1:9/cb',
      clientId: 'cid', store: expiredStore(), now: () => 0,
      env: {},
      discoveryStore: memDiscoveryStore({ resource: 'https://rs.example/mcp', authServer: AS_META }),
      authorize: async () => { throw new Error('should not consent'); },
    });
    assert.equal(await session.authorizationHeader(), 'Bearer FRESH');
    assert.equal(new URLSearchParams(bodies[0]).has('client_secret'), false);
  });

  it('re-resolves the env secret on invalidateCredentials("client")', async () => {
    const { fn, bodies } = captureToken();
    const session = new McpOAuthSession({
      serverUrl: 'https://rs.example/mcp', fetchImpl: fn, redirectUri: 'http://127.0.0.1:9/cb',
      clientId: 'cid', store: expiredStore(), now: () => 0,
      env: { MCP_CLIENT_SECRET: 'ENVSECRET' },
      discoveryStore: memDiscoveryStore({ resource: 'https://rs.example/mcp', authServer: AS_META }),
      authorize: async () => { throw new Error('should not consent'); },
    });
    await session.invalidateCredentials('client');
    // Re-seed an expired grant (invalidate('client') leaves tokens intact here) and refresh.
    assert.equal(await session.authorizationHeader(), 'Bearer FRESH');
    assert.equal(new URLSearchParams(bodies[0]).get('client_secret'), 'ENVSECRET');
  });
});
