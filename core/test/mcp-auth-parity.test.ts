/**
 * Parity hardening tests for the MCP `authenticate` pseudo-tool
 * (src/tools/mcp-auth.ts), bringing it to functional parity with the reference's
 * `/mcp` OAuth flow as an additive superset.
 *
 * Each test is a RED-GREEN reproducer: it discriminates the hardened behavior
 * from the pre-fix behavior (it fails if the corresponding fix is reverted).
 *
 * Fixes covered:
 *   1. redactAuthUrl — credential-bearing query params in the surfaced
 *      authorization URL are scrubbed (output + metadata) because the shared
 *      redactSecrets misses `?access_token=`/`&client_secret=`/`&code=`.
 *   2. classifyMcpAuthError + metadata.error_kind — auth failures are
 *      classified (auth/network/config/unknown) and surfaced so the model can
 *      pick the right next step instead of treating every failure identically.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  createMcpAuthTool,
  redactAuthUrl,
  classifyMcpAuthError,
  MCP_AUTH_ERROR_KINDS,
} from '../src/tools/mcp-auth';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

// ---------------------------------------------------------------------------
// Fix 1: redactAuthUrl — the surfaced authorization URL must never leak creds.
// ---------------------------------------------------------------------------
describe('mcp-auth parity — authorization URL credential redaction', () => {
  it('redactAuthUrl scrubs access_token / client_secret / code query values', () => {
    const leaky =
      'https://as.example.com/cb?access_token=supersecrettokenvalue123456&client_id=x&state=y';
    const safe = redactAuthUrl(leaky);
    // RED on revert: redactSecrets alone leaves the query-string token intact.
    assert.ok(!safe.includes('supersecrettokenvalue123456'), 'access_token value must be redacted');
    assert.ok(safe.includes('[REDACTED-SECRET]'));
    // Non-secret params survive so the user can still complete consent.
    assert.ok(safe.includes('client_id=x'));
    assert.ok(safe.includes('state=y'));

    const leaky2 = 'https://as.example.com/cb?client_secret=hunter2abcdefghij&code=abcdefghijklmnop';
    const safe2 = redactAuthUrl(leaky2);
    assert.ok(!safe2.includes('hunter2abcdefghij'), 'client_secret value must be redacted');
    assert.ok(!safe2.includes('abcdefghijklmnop'), 'authorization code value must be redacted');
  });

  it('redactAuthUrl is a no-op on a clean OAuth 2.1 authorize URL (no happy-path breakage)', () => {
    const clean =
      'https://as.example.com/authorize?client_id=x&redirect_uri=http://localhost/cb&scope=read&state=y&code_challenge=abc&response_type=code';
    // code_challenge must NOT be redacted (it is not `code=`); response_type=code too.
    assert.equal(redactAuthUrl(clean), clean);
  });

  it('execute() surfaces a REDACTED auth URL in both output and metadata on failure', async () => {
    const tool = createMcpAuthTool('leak-server', { transport: 'http' });
    const leakyUrl = 'https://as.example.com/cb?access_token=leakedtokenvalue1234567890&state=z';
    const r = await tool.execute({}, ctx({
      _mcpManager: {
        getServerStatus: () => ({ state: 'disconnected' }),
        reconnect: async () => { throw new Error('consent required'); },
        getPendingAuthUrl: () => leakyUrl,
      },
    } as Partial<ToolContext>));
    assert.ok(r.isError);
    // RED on revert: raw URL (with token) is placed verbatim into output + metadata.
    assert.ok(!r.output.includes('leakedtokenvalue1234567890'), 'token must not leak into output');
    assert.ok(typeof r.metadata?.auth_url === 'string');
    assert.ok(!(r.metadata!.auth_url as string).includes('leakedtokenvalue1234567890'), 'token must not leak into metadata');
    assert.ok((r.metadata!.auth_url as string).includes('[REDACTED-SECRET]'));
    // Non-secret state param preserved so the model can still relay the URL.
    assert.ok((r.metadata!.auth_url as string).includes('state=z'));
  });
});

// ---------------------------------------------------------------------------
// Fix 2: classifyMcpAuthError + metadata.error_kind.
// ---------------------------------------------------------------------------
describe('mcp-auth parity — auth error classification', () => {
  it('classifyMcpAuthError distinguishes auth / network / config / unknown', () => {
    assert.equal(classifyMcpAuthError('401 Unauthorized'), 'auth');
    assert.equal(classifyMcpAuthError('invalid_grant: consent expired'), 'auth');
    assert.equal(classifyMcpAuthError('connect ECONNREFUSED 127.0.0.1:8080'), 'network');
    assert.equal(classifyMcpAuthError('request timed out after 30s'), 'network');
    assert.equal(classifyMcpAuthError('server url not configured'), 'config');
    assert.equal(classifyMcpAuthError('something entirely unexpected happened'), 'unknown');
    // Every result is a member of the exported set.
    for (const msg of ['401', 'ETIMEDOUT', 'missing url', 'weird']) {
      assert.ok(MCP_AUTH_ERROR_KINDS.includes(classifyMcpAuthError(msg)));
    }
  });

  it('execute() tags an authorization rejection as error_kind="auth" with a re-auth hint', async () => {
    const tool = createMcpAuthTool('auth-fail', { transport: 'http' });
    const r = await tool.execute({}, ctx({
      _mcpManager: {
        getServerStatus: () => ({ state: 'disconnected' }),
        reconnect: async () => { throw new Error('403 Forbidden: token rejected'); },
      },
    } as Partial<ToolContext>));
    assert.ok(r.isError);
    assert.equal(r.metadata?.status, 'error');
    // RED on revert: error_kind did not exist (undefined) and no re-auth hint.
    assert.equal(r.metadata?.error_kind, 'auth');
    assert.ok(r.output.includes('Re-authentication is required'));
  });

  it('execute() tags a connectivity failure as error_kind="network" with a retry hint', async () => {
    const tool = createMcpAuthTool('net-fail', { transport: 'sse' });
    const r = await tool.execute({}, ctx({
      _mcpManager: {
        getServerStatus: () => ({ state: 'disconnected' }),
        reconnect: async () => { throw new Error('connect ECONNREFUSED 10.0.0.1:443'); },
      },
    } as Partial<ToolContext>));
    assert.ok(r.isError);
    // RED on revert: no error_kind, so this assertion fails (undefined !== 'network').
    assert.equal(r.metadata?.error_kind, 'network');
    assert.ok(r.output.includes('connectivity'));
    // An auth-specific hint must NOT appear for a network failure.
    assert.ok(!r.output.includes('Re-authentication is required'));
  });
});
