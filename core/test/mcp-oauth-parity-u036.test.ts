/**
 * mcp-oauth-parity-u036.test.ts — unit + integration tests for the U036 OAuth
 * parity batch (core/core/src/mcp/oauth.ts + manager wiring):
 *   1. DEFAULT_TOKEN_LIFETIME_SEC fallback when expires_in + JWT exp are absent.
 *   2. login_hint OIDC parameter on the authorization URL.
 *   3. scope parameter on the refresh_token grant (RFC 6749 §6).
 *   4. richer scope extraction from AS metadata (scope / default_scope / scopes_supported).
 *   5. oauth.authServerMetadataUrl pinned-metadata knob (https:// enforced, mix-up bypass).
 *
 * All network I/O is through an injected FetchLike so these are pure and fast.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'events';

import {
  DEFAULT_TOKEN_LIFETIME_SEC,
  buildAuthorizationUrl, refreshToken, exchangeCode,
  discoverAuthServer, resolveScopeFromMetadata, McpOAuthSession,
} from '../src/mcp/oauth';
import type { FetchLike, AuthServerMetadata } from '../src/mcp/oauth';
import { McpManager } from '../src/mcp/manager';

function fakeFetch(routes: Record<string, { ok?: boolean; status?: number; body: unknown }>): { fn: FetchLike; calls: Array<{ url: string; init?: { body?: string } }> } {
  const calls: Array<{ url: string; init?: { body?: string } }> = [];
  const fn: FetchLike = async (url, init) => {
    calls.push({ url, init });
    const r = routes[url];
    if (!r) return { ok: false, status: 404, json: async () => ({}) };
    return { ok: r.ok ?? true, status: r.status ?? 200, json: async () => r.body };
  };
  return { fn, calls };
}

const b64url = (s: Buffer) => s.toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
const jwt = (payload: Record<string, unknown>) =>
  `${b64url(Buffer.from('{}'))}.${b64url(Buffer.from(JSON.stringify(payload)))}.sig`;

// ---------------------------------------------------------------------------
// Gap 1 — DEFAULT_TOKEN_LIFETIME_SEC fallback expiry
// ---------------------------------------------------------------------------
describe('U036 default token lifetime fallback', () => {
  const NOW = 1_000_000;
  it('exports a 3600s default', () => {
    assert.equal(DEFAULT_TOKEN_LIFETIME_SEC, 3600);
  });
  it('falls back to now + 3600s for an opaque token lacking expires_in and a JWT exp', async () => {
    // RED (pre-fix): expiresAt was undefined (treated as never-expiring).
    const { fn } = fakeFetch({ u: { body: { access_token: 'opaque-AT', token_type: 'Bearer' } } });
    const tok = await refreshToken(fn, { tokenEndpoint: 'u', refreshToken: 'RT', clientId: 'c', resource: 'rs' }, NOW);
    assert.equal(tok.expiresAt, NOW + 3600 * 1000);
  });
  it('still prefers expires_in over the 3600s fallback', async () => {
    const { fn } = fakeFetch({ u: { body: { access_token: 'AT', token_type: 'Bearer', expires_in: 120 } } });
    const tok = await exchangeCode(fn, { tokenEndpoint: 'u', code: 'c', verifier: 'v', clientId: 'c', redirectUri: 'r', resource: 'rs' }, NOW);
    assert.equal(tok.expiresAt, NOW + 120 * 1000);
  });
  it('still prefers the JWT exp over the 3600s fallback', async () => {
    const { fn } = fakeFetch({ u: { body: { access_token: jwt({ exp: 2_000_000_000 }), token_type: 'Bearer' } } });
    const tok = await exchangeCode(fn, { tokenEndpoint: 'u', code: 'c', verifier: 'v', clientId: 'c', redirectUri: 'r', resource: 'rs' }, NOW);
    assert.equal(tok.expiresAt, 2_000_000_000_000);
  });
});

// ---------------------------------------------------------------------------
// Gap 2 — login_hint OIDC parameter
// ---------------------------------------------------------------------------
describe('U036 login_hint authorization parameter', () => {
  it('appends login_hint when supplied', () => {
    const url = new URL(buildAuthorizationUrl({
      authorizationEndpoint: 'https://as/authorize', clientId: 'c', redirectUri: 'r',
      resource: 'rs', codeChallenge: 'x', state: 's', loginHint: 'user@example.com',
    }));
    assert.equal(url.searchParams.get('login_hint'), 'user@example.com');
  });
  it('omits login_hint when not supplied', () => {
    const url = new URL(buildAuthorizationUrl({
      authorizationEndpoint: 'https://as/authorize', clientId: 'c', redirectUri: 'r',
      resource: 'rs', codeChallenge: 'x', state: 's',
    }));
    assert.equal(url.searchParams.get('login_hint'), null);
  });
});

// ---------------------------------------------------------------------------
// Gap 3 — scope on refresh_token grant
// ---------------------------------------------------------------------------
describe('U036 scope on refresh_token grant', () => {
  const NOW = 10;
  it('includes scope in the refresh form when supplied', async () => {
    const { fn, calls } = fakeFetch({ u: { body: { access_token: 'AT', token_type: 'Bearer' } } });
    await refreshToken(fn, { tokenEndpoint: 'u', refreshToken: 'RT', clientId: 'c', resource: 'rs', scope: 'read write' }, NOW);
    const form = new URLSearchParams(calls[0].init!.body);
    assert.equal(form.get('grant_type'), 'refresh_token');
    assert.equal(form.get('scope'), 'read write');
  });
  it('omits scope from the refresh form when not supplied', async () => {
    const { fn, calls } = fakeFetch({ u: { body: { access_token: 'AT', token_type: 'Bearer' } } });
    await refreshToken(fn, { tokenEndpoint: 'u', refreshToken: 'RT', clientId: 'c', resource: 'rs' }, NOW);
    const form = new URLSearchParams(calls[0].init!.body);
    assert.equal(form.get('scope'), null);
  });
});

// ---------------------------------------------------------------------------
// Gap 4 — richer scope extraction from AS metadata
// ---------------------------------------------------------------------------
describe('U036 resolveScopeFromMetadata', () => {
  const base: AuthServerMetadata = { issuer: 'https://as', authorizationEndpoint: 'https://as/a', tokenEndpoint: 'https://as/t' };
  it('returns undefined for null/undefined/empty metadata', () => {
    assert.equal(resolveScopeFromMetadata(undefined), undefined);
    assert.equal(resolveScopeFromMetadata(null), undefined);
    assert.equal(resolveScopeFromMetadata(base), undefined);
  });
  it('prefers scope, then default_scope, then scopes_supported', () => {
    assert.equal(resolveScopeFromMetadata({ ...base, scope: 'a b', defaultScope: 'c', scopesSupported: ['d'] }), 'a b');
    assert.equal(resolveScopeFromMetadata({ ...base, defaultScope: 'c d', scopesSupported: ['e'] }), 'c d');
    assert.equal(resolveScopeFromMetadata({ ...base, scopesSupported: ['x', 'y'] }), 'x y');
  });
  it('discoverAuthServer parses scope and default_scope from the metadata doc', async () => {
    const { fn } = fakeFetch({ 'https://as/.well-known/oauth-authorization-server': { body: {
      issuer: 'https://as', authorization_endpoint: 'https://as/authorize', token_endpoint: 'https://as/token',
      scope: 'profile', default_scope: 'openid profile',
    } } });
    const md = await discoverAuthServer(fn, 'https://as');
    assert.equal(md.scope, 'profile');
    assert.equal(md.defaultScope, 'openid profile');
  });
});

// ---------------------------------------------------------------------------
// Gap 5 — oauth.authServerMetadataUrl pinned metadata knob
// ---------------------------------------------------------------------------
describe('U036 authServerMetadataUrl pinned metadata', () => {
  it('fetches the pinned URL directly (not well-known) and bypasses the issuer mix-up check', async () => {
    const pinned = 'https://as.example/custom/meta.json';
    // doc.issuer deliberately differs from the probed issuer — allowed because pinned.
    const { fn, calls } = fakeFetch({ [pinned]: { body: {
      issuer: 'https://different-issuer.example', authorization_endpoint: 'https://as.example/authorize', token_endpoint: 'https://as.example/token',
    } } });
    const md = await discoverAuthServer(fn, 'https://as.example', pinned);
    assert.equal(md.tokenEndpoint, 'https://as.example/token');
    assert.equal(calls[0].url, pinned);
    assert.ok(!calls.some((c) => c.url.includes('.well-known')));
  });
  it('rejects a non-https pinned metadata URL', async () => {
    const { fn } = fakeFetch({});
    await assert.rejects(
      () => discoverAuthServer(fn, 'https://as.example', 'http://as.example/meta.json'),
      /authServerMetadataUrl must use https:\/\//,
    );
  });
});

// ---------------------------------------------------------------------------
// Integration — McpOAuthSession end-to-end with the new knobs
// ---------------------------------------------------------------------------
describe('U036 McpOAuthSession integration', () => {
  const SERVER = 'https://rs.example/mcp';
  const PRM_URL = 'https://rs.example/.well-known/oauth-protected-resource/mcp';
  const PINNED = 'https://as.example/pinned-meta.json';

  it('uses a pinned AS metadata URL, login_hint, and AS-metadata default_scope end to end', async () => {
    const { fn, calls } = fakeFetch({
      [PRM_URL]: { body: { resource: SERVER, authorization_servers: ['https://as.example'] } }, // no scopes_supported here
      [PINNED]: { body: {
        issuer: 'https://any-issuer.example', authorization_endpoint: 'https://as.example/authorize',
        token_endpoint: 'https://as.example/token', default_scope: 'mcp:read mcp:write',
      } },
      'https://as.example/token': { body: { access_token: 'AT', token_type: 'Bearer', refresh_token: 'RT', expires_in: 3600 } },
    });
    let authUrl = '';
    const session = new McpOAuthSession({
      serverUrl: SERVER, fetchImpl: fn, redirectUri: 'http://127.0.0.1:9/cb', clientId: 'cid',
      authServerMetadataUrl: PINNED, loginHint: 'me@example.com',
      authorize: async ({ authorizationUrl }) => { authUrl = authorizationUrl; return { code: 'the-code' }; },
      now: () => 1000,
    });
    assert.equal(await session.onUnauthorized('Bearer resource_metadata="' + PRM_URL + '"'), true);
    // Pinned metadata was fetched; no well-known AS probe happened.
    assert.ok(calls.some((c) => c.url === PINNED));
    assert.ok(!calls.some((c) => c.url.includes('.well-known/oauth-authorization-server')));
    const u = new URL(authUrl);
    assert.equal(u.searchParams.get('login_hint'), 'me@example.com');
    // Scope resolved from AS metadata default_scope (no configured scope, no PRM scopes).
    assert.equal(u.searchParams.get('scope'), 'mcp:read mcp:write');
    assert.equal(await session.authorizationHeader(), 'Bearer AT');
  });

  it('sends the configured scope on a silent refresh', async () => {
    let t = 0;
    const { fn, calls } = fakeFetch({
      [PRM_URL]: { body: { resource: SERVER, authorization_servers: ['https://as.example'] } },
      [PINNED]: { body: { issuer: 'x', authorization_endpoint: 'https://as.example/authorize', token_endpoint: 'https://as.example/token' } },
      'https://as.example/token': { body: { access_token: 'AT1', token_type: 'Bearer', refresh_token: 'RT', expires_in: 100 } },
    });
    const session = new McpOAuthSession({
      serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', scope: 'read', authServerMetadataUrl: PINNED,
      authorize: async () => ({ code: 'c' }), now: () => t,
    });
    await session.onUnauthorized('Bearer resource_metadata="' + PRM_URL + '"');
    t = 200_000; // past expiry → silent refresh
    assert.equal(await session.authorizationHeader(), 'Bearer AT1');
    const refreshCall = calls.filter((c) => c.url === 'https://as.example/token')
      .map((c) => new URLSearchParams(c.init!.body))
      .find((f) => f.get('grant_type') === 'refresh_token');
    assert.ok(refreshCall, 'a refresh_token grant should have been POSTed');
    assert.equal(refreshCall!.get('scope'), 'read');
  });
});

// ---------------------------------------------------------------------------
// End-to-end wiring — McpManager threads the new config knobs into the session
// ---------------------------------------------------------------------------
class FakeClient extends EventEmitter {
  state = 'ready';
  serverInfo: unknown = { name: 'fake', version: '1' };
  instructions: string | null = null;
  uptime: number | null = 1;
  tools: unknown[] = [];
  resources: unknown[] = [];
  prompts: unknown[] = [];
  connectImpl: () => Promise<void> = async () => {};
  async connect(): Promise<void> { return this.connectImpl(); }
  async connectHTTP(): Promise<void> { return this.connectImpl(); }
  async connectSSE(): Promise<void> { return this.connectImpl(); }
  async listTools(): Promise<unknown[]> { return this.tools; }
  async listResources(): Promise<unknown[]> { return this.resources; }
  async listPrompts(): Promise<unknown[]> { return this.prompts; }
  async getPrompt(): Promise<unknown> { return { messages: [] }; }
  async readResource(): Promise<unknown> { return {}; }
  async callTool(): Promise<unknown> { return { ok: true }; }
  async reconnect(): Promise<void> {}
  disconnect(): void {}
  setHeaders(): void {}
}

describe('U036 McpManager config wiring', () => {
  const SERVER = 'https://rs.example/mcp';
  const PRM_URL = 'https://rs.example/.well-known/oauth-protected-resource/mcp';
  const PINNED = 'https://as.example/pinned-meta.json';

  it('passes oauth.authServerMetadataUrl and oauth.loginHint from server config into the live session', async () => {
    const { fn, calls } = fakeFetch({
      [PRM_URL]: { body: { resource: SERVER, authorization_servers: ['https://as.example'] } },
      [PINNED]: { body: { issuer: 'x', authorization_endpoint: 'https://as.example/authorize', token_endpoint: 'https://as.example/token' } },
      'https://as.example/token': { body: { access_token: 'AT', token_type: 'Bearer', expires_in: 3600 } },
    });
    const mgr = new McpManager();
    mgr._fetch = fn;
    let capturedOAuth: { onUnauthorized(h: string | null): Promise<boolean> } | undefined;
    mgr._createClient = ((cfg: { oauth?: { onUnauthorized(h: string | null): Promise<boolean> } }) => {
      capturedOAuth = cfg.oauth;
      return new FakeClient();
    }) as never;
    let presentedUrl = '';
    mgr.setOAuthAuthorizer(async ({ authorizationUrl }) => { presentedUrl = authorizationUrl; return { code: 'the-code' }; });
    mgr.addPendingServer('oauth-srv', {
      command: '', transport: 'http', url: SERVER,
      oauth: { clientId: 'cid', authServerMetadataUrl: PINNED, loginHint: 'wired@example.com' },
    });
    await mgr.ensureConnected('oauth-srv');
    assert.ok(capturedOAuth, 'manager should build an OAuth provider for the server');
    assert.equal(await capturedOAuth!.onUnauthorized('Bearer resource_metadata="' + PRM_URL + '"'), true);
    // The pinned metadata URL from config reached the session's discovery.
    assert.ok(calls.some((c) => c.url === PINNED));
    // The login_hint from config reached the authorization URL.
    assert.equal(new URL(presentedUrl).searchParams.get('login_hint'), 'wired@example.com');
  });
});
