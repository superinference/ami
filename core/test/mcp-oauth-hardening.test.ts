/**
 * mcp-oauth-hardening.test.ts — unit tests for the OAuth transport-security and
 * resilience helpers: HTTPS enforcement, per-request timeout, retry/backoff,
 * token-error normalization, PRM mix-up protection, and the session behaviors
 * that compose them (invalid_grant clearing, plaintext refusal).
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  assertSecureUrl,
  OAuthError,
  normalizeTokenError,
  withTimeout,
  wrapFetchWithTimeout,
  retryWithBackoff,
  isTransientOAuthError,
  assertResourceMatch,
  discoverAuthServer,
  refreshToken,
  McpOAuthSession,
  OAuthTokenStore,
  OAUTH_HTTP_TIMEOUT_MS,
} from '../src/mcp/oauth';
import type { FetchLike } from '../src/mcp/oauth';

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

describe('assertSecureUrl', () => {
  it('allows https', () => { assert.doesNotThrow(() => assertSecureUrl('https://as.example/token')); });
  it('allows http only on loopback', () => {
    assert.doesNotThrow(() => assertSecureUrl('http://localhost:8080/cb'));
    assert.doesNotThrow(() => assertSecureUrl('http://127.0.0.1/cb'));
    assert.doesNotThrow(() => assertSecureUrl('http://[::1]:9000/cb'));
  });
  it('refuses plaintext http on a non-loopback host', () => {
    assert.throws(() => assertSecureUrl('http://as.example/token', 'token endpoint'), /token endpoint must use https/);
  });
  it('refuses a malformed URL', () => {
    assert.throws(() => assertSecureUrl('not a url'), /not a valid URL/);
  });
});

describe('normalizeTokenError / OAuthError', () => {
  it('keeps a regex-stable message and extracts the code', () => {
    const e = normalizeTokenError(401, { error: 'invalid_client', error_description: 'bad' });
    assert.ok(e instanceof OAuthError);
    assert.equal(e.status, 401);
    assert.equal(e.code, 'invalid_client');
    assert.equal(e.description, 'bad');
    assert.match(e.message, /token endpoint error \(401\): invalid_client \(bad\)/);
  });
  it('normalizes non-standard invalid_grant aliases', () => {
    assert.equal(normalizeTokenError(400, { error: 'expired_token' }).code, 'invalid_grant');
    assert.equal(normalizeTokenError(400, { error: 'invalid_refresh_token' }).code, 'invalid_grant');
  });
  it('handles a bodyless error', () => {
    assert.match(normalizeTokenError(500, {}).message, /token endpoint error \(500\)$/);
  });
});

describe('withTimeout', () => {
  it('resolves a fast factory and passes a signal', async () => {
    let seen: AbortSignal | undefined;
    const v = await withTimeout(async (signal) => { seen = signal; return 7; }, 1000, 'op');
    assert.equal(v, 7);
    assert.ok(seen instanceof AbortSignal);
  });
  it('rejects and aborts when the factory exceeds the budget', async () => {
    const aborted: boolean[] = [];
    await assert.rejects(
      () => withTimeout((signal) => new Promise((_r, rej) => { signal!.addEventListener('abort', () => { aborted.push(true); rej(new Error('aborted')); }); }), 10, 'slow op'),
      /slow op timed out after 10ms/,
    );
    assert.deepEqual(aborted, [true]);
  });
  it('is a no-op passthrough when ms<=0', async () => {
    assert.equal(await withTimeout(async (s) => { assert.equal(s, undefined); return 1; }, 0, 'x'), 1);
  });
});

describe('wrapFetchWithTimeout', () => {
  it('bounds a hung fetch', async () => {
    const hung: FetchLike = () => new Promise(() => { /* never resolves */ });
    const wrapped = wrapFetchWithTimeout(hung, 10);
    await assert.rejects(() => wrapped('https://as/x'), /timed out/);
  });
  it('passes through a fast fetch', async () => {
    const { fn } = fakeFetch({ 'https://as/x': { body: { ok: 1 } } });
    const wrapped = wrapFetchWithTimeout(fn, 1000);
    const res = await wrapped('https://as/x');
    assert.equal(res.status, 200);
  });
});

describe('retryWithBackoff / isTransientOAuthError', () => {
  it('retries transient failures then succeeds', async () => {
    let n = 0;
    const out = await retryWithBackoff(async () => { n++; if (n < 3) throw new OAuthError('boom', 503); return 'ok'; }, { retries: 3, baseMs: 0, isRetryable: isTransientOAuthError, sleep: async () => {} });
    assert.equal(out, 'ok');
    assert.equal(n, 3);
  });
  it('does not retry a non-retryable error', async () => {
    let n = 0;
    await assert.rejects(() => retryWithBackoff(async () => { n++; throw new OAuthError('dead', 400, 'invalid_grant'); }, { retries: 5, baseMs: 0, isRetryable: isTransientOAuthError }), /dead/);
    assert.equal(n, 1);
  });
  it('gives up after exhausting retries', async () => {
    let n = 0;
    await assert.rejects(() => retryWithBackoff(async () => { n++; throw new Error('net'); }, { retries: 2, baseMs: 0, sleep: async () => {} }), /net/);
    assert.equal(n, 3);
  });
  it('classifies transient vs terminal correctly', () => {
    assert.equal(isTransientOAuthError(new OAuthError('x', 503)), true);
    assert.equal(isTransientOAuthError(new OAuthError('x', 429)), true);
    assert.equal(isTransientOAuthError(new OAuthError('x', 400)), false);
    assert.equal(isTransientOAuthError(new Error('network')), true);
  });
  it('uses real defaults without an explicit sleep', async () => {
    assert.equal(await retryWithBackoff(async () => 42), 42);
  });
});

describe('assertResourceMatch', () => {
  it('passes for same-origin resource', () => {
    assert.doesNotThrow(() => assertResourceMatch('https://rs.example/mcp', 'https://rs.example/other'));
  });
  it('rejects a different-origin resource (mix-up protection)', () => {
    assert.throws(() => assertResourceMatch('https://evil.example/mcp', 'https://rs.example/mcp'), /mismatch/);
  });
  it('is lenient when the advertised resource is not absolute', () => {
    assert.doesNotThrow(() => assertResourceMatch('not-a-url', 'https://rs.example/mcp'));
    assert.doesNotThrow(() => assertResourceMatch('https://rs.example/mcp', 'not-a-url'));
  });
});

describe('discoverAuthServer — transport downgrade guard', () => {
  it('refuses a plaintext token endpoint from metadata', async () => {
    const { fn } = fakeFetch({ 'https://as/.well-known/oauth-authorization-server': { body: { issuer: 'https://as', authorization_endpoint: 'https://as/authorize', token_endpoint: 'http://as/token' } } });
    await assert.rejects(() => discoverAuthServer(fn, 'https://as'), /token endpoint must use https/);
  });
});

describe('refreshToken — HTTP-200 error body', () => {
  it('treats a 200 response carrying error as a failure', async () => {
    const { fn } = fakeFetch({ 'https://as/token': { status: 200, body: { error: 'invalid_grant', error_description: 'revoked' } } });
    await assert.rejects(() => refreshToken(fn, { tokenEndpoint: 'https://as/token', refreshToken: 'RT', clientId: 'c', resource: 'rs' }, 0), /token endpoint error \(200\): invalid_grant/);
  });
});

describe('McpOAuthSession — resilience', () => {
  const SERVER = 'https://rs.example/mcp';
  const PRM_URL = 'https://rs.example/.well-known/oauth-protected-resource/mcp';
  const AS_META = 'https://as.example/.well-known/oauth-authorization-server';

  function fullRoutes(extra: Record<string, { ok?: boolean; status?: number; body: unknown }> = {}) {
    return {
      [PRM_URL]: { body: { resource: SERVER, authorization_servers: ['https://as.example'] } },
      [AS_META]: { body: { issuer: 'https://as.example', authorization_endpoint: 'https://as.example/authorize', token_endpoint: 'https://as.example/token' } },
      'https://as.example/token': { body: { access_token: 'AT1', token_type: 'Bearer', refresh_token: 'RT', expires_in: 100 } },
      ...extra,
    };
  }

  it('clears the stored tokens when a refresh fails with invalid_grant', async () => {
    let t = 0;
    const store = new OAuthTokenStore();
    const routes = fullRoutes();
    const { fn } = fakeFetch(routes);
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', store, authorize: async () => ({ code: 'c' }), now: () => t, retry: { baseMs: 0, sleep: async () => {} } });
    await session.onUnauthorized(null);
    assert.ok(store.get());
    t = 200_000;
    // The token endpoint now rejects the refresh as a dead grant.
    routes['https://as.example/token'] = { status: 400, body: { error: 'invalid_grant' } } as { status: number; body: unknown };
    assert.equal(await session.authorizationHeader(), null);
    assert.equal(store.get(), null); // cleared → next 401 drives full re-auth
  });

  it('rejects a PRM whose resource points at another origin (mix-up)', async () => {
    const { fn } = fakeFetch(fullRoutes({ [PRM_URL]: { body: { resource: 'https://evil.example/mcp', authorization_servers: ['https://as.example'] } } }));
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', authorize: async () => ({ code: 'c' }), now: () => 0 });
    assert.equal(await session.onUnauthorized(null), false);
  });

  it('exposes a sane default timeout constant', () => {
    assert.equal(OAUTH_HTTP_TIMEOUT_MS, 30_000);
  });
});
