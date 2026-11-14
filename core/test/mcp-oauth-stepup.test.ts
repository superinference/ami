/**
 * mcp-oauth-stepup.test.ts — H23: OAuth 2.1 step-up on a 403 insufficient_scope.
 * The token authenticates fine but lacks a scope the resource now requires, so we
 * must drop the stale token and drive a full re-consent requesting the wider scope
 * (never a silent refresh that would re-mint the same insufficient scope).
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { McpOAuthSession, parseWwwAuthenticate, unionScopes } from '../src/mcp/oauth';
import type { FetchLike } from '../src/mcp/oauth';

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

const SERVER = 'https://rs.example/mcp';
const PRM_URL = 'https://rs.example/.well-known/oauth-protected-resource/mcp';
const AS_META = 'https://as.example/.well-known/oauth-authorization-server';
function fullRoutes(extra: Record<string, { ok?: boolean; status?: number; body: unknown }> = {}) {
  return {
    [PRM_URL]: { body: { resource: SERVER, authorization_servers: ['https://as.example'], scopes_supported: ['read'] } },
    [AS_META]: { body: { issuer: 'https://as.example', authorization_endpoint: 'https://as.example/authorize', token_endpoint: 'https://as.example/token' } },
    'https://as.example/token': { body: { access_token: 'AT2', token_type: 'Bearer', refresh_token: 'RT2', expires_in: 3600 } },
    ...extra,
  };
}

describe('parseWwwAuthenticate scope extraction (H23)', () => {
  it('extracts a quoted scope on a 403 insufficient_scope challenge', () => {
    const r = parseWwwAuthenticate('Bearer error="insufficient_scope", scope="read write admin"');
    assert.equal(r.error, 'insufficient_scope');
    assert.equal(r.scope, 'read write admin');
  });
  it('extracts an unquoted scope token (RFC 6750)', () => {
    const r = parseWwwAuthenticate('Bearer error=insufficient_scope, scope=admin');
    assert.equal(r.error, 'insufficient_scope');
    assert.equal(r.scope, 'admin');
  });
});

describe('unionScopes (H23)', () => {
  it('unions, dedups, preserves order, drops blanks', () => {
    assert.equal(unionScopes('read write', 'write admin'), 'read write admin');
    assert.equal(unionScopes('  read   write ', undefined), 'read write');
    assert.equal(unionScopes(undefined, undefined), undefined);
    assert.equal(unionScopes('', '   '), undefined);
  });
});

describe('McpOAuthSession.onForbidden step-up (H23)', () => {
  it('re-consents for the union of configured + required scope on insufficient_scope', async () => {
    const { fn } = fakeFetch(fullRoutes());
    let authorizedUrl = '';
    const session = new McpOAuthSession({
      serverUrl: SERVER, fetchImpl: fn, redirectUri: 'http://127.0.0.1:9/cb', clientId: 'cid',
      scope: 'read',
      authorize: async ({ authorizationUrl }) => { authorizedUrl = authorizationUrl; return { code: 'c' }; },
      now: () => 0,
    });
    const ok = await session.onForbidden('Bearer error="insufficient_scope", scope="write admin"');
    assert.equal(ok, true);
    const scopeParam = new URL(authorizedUrl).searchParams.get('scope');
    assert.equal(scopeParam, 'read write admin');
    assert.equal(await session.authorizationHeader(), 'Bearer AT2');
  });

  it('drops the stale token so a silent refresh cannot mask the step-up', async () => {
    // Seed a live token via a normal 401 flow, then a 403 must NOT silently reuse it.
    const { fn } = fakeFetch({
      ...fullRoutes(),
      'https://as.example/token': { body: { access_token: 'AT1', token_type: 'Bearer', refresh_token: 'RT1', expires_in: 3600 } },
    });
    let authorizeCalls = 0;
    const session = new McpOAuthSession({
      serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', scope: 'read',
      authorize: async () => { authorizeCalls++; return { code: 'c' }; }, now: () => 0,
    });
    await session.onUnauthorized(null);
    assert.equal(await session.authorizationHeader(), 'Bearer AT1');
    assert.equal(authorizeCalls, 1);
    const ok = await session.onForbidden('Bearer error="insufficient_scope", scope="admin"');
    assert.equal(ok, true);
    // A second interactive consent was forced (not a silent refresh of the old token).
    assert.equal(authorizeCalls, 2);
  });

  it('returns false for a 403 that is NOT an insufficient_scope challenge (real denial)', async () => {
    const { fn } = fakeFetch(fullRoutes());
    let authorizeCalls = 0;
    const session = new McpOAuthSession({
      serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid',
      authorize: async () => { authorizeCalls++; return { code: 'c' }; }, now: () => 0,
    });
    assert.equal(await session.onForbidden('Bearer error="access_denied"'), false);
    assert.equal(await session.onForbidden(null), false);
    assert.equal(authorizeCalls, 0);
  });
});
