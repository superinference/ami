/**
 * mcp-oauth-parity-w67.test.ts — unit + integration tests for the U036/w67 OAuth
 * parity batch (core/core/src/mcp/oauth.ts + McpManager wiring):
 *   1. combineAbortSignals — compose caller + timeout cancellation.
 *   2. redactOAuthUrl — scrub CSRF/PKCE params (state/nonce/code_challenge/
 *      code_verifier/code) from a URL before it is LOGGED.
 *   3. redactTokenBody — nesting-depth-agnostic scrub of token-bearing JSON keys.
 *   4. mcpCredentialKey + mcpTokenFileName — serverName + config-hash credential key.
 *   5. onLog — step-by-step diagnostic events across the multi-leg flow.
 *   6. signal — caller cancellation aborts in-flight OAuth requests (session level).
 *   7. McpManager wiring — config-hashed token file name + per-server abort on stop.
 *
 * All network I/O is through an injected FetchLike so these are pure and fast.
 */
import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'events';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  combineAbortSignals, redactOAuthUrl, redactTokenBody, SENSITIVE_OAUTH_PARAMS,
  mcpCredentialKey, mcpTokenFileName, McpOAuthSession,
} from '../src/mcp/oauth';
import type { FetchLike, OAuthLogEvent } from '../src/mcp/oauth';
import { McpManager } from '../src/mcp/manager';

function fakeFetch(routes: Record<string, { ok?: boolean; status?: number; body: unknown }>): { fn: FetchLike; calls: Array<{ url: string; init?: { body?: string; signal?: AbortSignal } }> } {
  const calls: Array<{ url: string; init?: { body?: string; signal?: AbortSignal } }> = [];
  const fn: FetchLike = async (url, init) => {
    calls.push({ url, init });
    // Honor cancellation the way a real fetch does: a composed caller/timeout
    // signal that is already aborted rejects the request.
    if (init?.signal?.aborted) throw new Error('aborted');
    const r = routes[url];
    if (!r) return { ok: false, status: 404, json: async () => ({}) };
    return { ok: r.ok ?? true, status: r.status ?? 200, json: async () => r.body };
  };
  return { fn, calls };
}

// ---------------------------------------------------------------------------
// Gap: caller abort signal composed into in-flight OAuth requests
// ---------------------------------------------------------------------------
describe('w67 combineAbortSignals', () => {
  it('returns undefined when no signal is present', () => {
    assert.equal(combineAbortSignals(undefined, undefined), undefined);
  });
  it('returns the lone signal unchanged when exactly one is present', () => {
    const c = new AbortController();
    assert.equal(combineAbortSignals(undefined, c.signal), c.signal);
  });
  it('aborts as soon as ANY input aborts', () => {
    const a = new AbortController();
    const b = new AbortController();
    const combined = combineAbortSignals(a.signal, b.signal)!;
    assert.equal(combined.aborted, false);
    b.abort();
    assert.equal(combined.aborted, true);
  });
});

// ---------------------------------------------------------------------------
// Gap: redaction of sensitive OAuth params (state/nonce/code_challenge/
//      code_verifier/code) in logged URLs
// ---------------------------------------------------------------------------
describe('w67 redactOAuthUrl', () => {
  it('exports the sensitive-param list', () => {
    assert.deepEqual([...SENSITIVE_OAUTH_PARAMS], ['state', 'nonce', 'code_challenge', 'code_verifier', 'code']);
  });
  it('redacts every CSRF/PKCE param but keeps non-sensitive ones', () => {
    const url = 'https://as.example/authorize?client_id=c&redirect_uri=http%3A%2F%2Flocalhost%2Fcb&scope=read'
      + '&state=STSECRET&nonce=NSECRET&code_challenge=CHSECRET&code_verifier=CVSECRET&code=AUTHCODE';
    const safe = redactOAuthUrl(url);
    for (const leak of ['STSECRET', 'NSECRET', 'CHSECRET', 'CVSECRET', 'AUTHCODE']) {
      assert.ok(!safe.includes(leak), `${leak} must be redacted`);
    }
    const out = new URL(safe);
    assert.equal(out.searchParams.get('state'), '[REDACTED]');
    assert.equal(out.searchParams.get('code_verifier'), '[REDACTED]');
    // Non-sensitive params survive.
    assert.equal(out.searchParams.get('client_id'), 'c');
    assert.equal(out.searchParams.get('scope'), 'read');
  });
  it('returns a non-URL string unchanged', () => {
    assert.equal(redactOAuthUrl('not a url'), 'not a url');
  });
});

// ---------------------------------------------------------------------------
// Gap: redact token-bearing keys from error bodies (nesting-depth-agnostic)
// ---------------------------------------------------------------------------
describe('w67 redactTokenBody', () => {
  it('redacts every token-bearing key at any nesting depth, from a raw string', () => {
    const raw = '{"error":"x","data":{"access_token":"AT","inner":{"refresh_token":"RT","id_token":"IT",'
      + '"assertion":"AS","subject_token":"ST","client_secret":"CS"}}}';
    const safe = redactTokenBody(raw);
    for (const leak of ['"AT"', '"RT"', '"IT"', '"AS"', '"ST"', '"CS"']) {
      assert.ok(!safe.includes(leak), `${leak} must be redacted`);
    }
    assert.ok(safe.includes('"access_token":"[REDACTED]"'));
    assert.ok(safe.includes('"client_secret":"[REDACTED]"'));
    // Non-token fields are preserved.
    assert.ok(safe.includes('"error":"x"'));
  });
  it('stringifies and redacts an object input', () => {
    const safe = redactTokenBody({ refresh_token: 'LEAK', keep: 'ok' });
    assert.ok(!safe.includes('LEAK'));
    assert.ok(safe.includes('"keep":"ok"'));
  });
});

// ---------------------------------------------------------------------------
// Gap: credential key derived from serverName + config hash (type/url/headers)
// ---------------------------------------------------------------------------
describe('w67 mcpCredentialKey', () => {
  const cfg = { type: 'http', url: 'https://rs.example/mcp', headers: { Authorization: 'x' } };
  it('is name|16-hex-hash and stable for identical config', () => {
    const k = mcpCredentialKey('srv', cfg);
    assert.match(k, /^srv\|[0-9a-f]{16}$/);
    assert.equal(mcpCredentialKey('srv', cfg), k, 'stable');
  });
  it('differs when the URL changes (no credential reuse across configs)', () => {
    assert.notEqual(
      mcpCredentialKey('srv', cfg),
      mcpCredentialKey('srv', { ...cfg, url: 'https://OTHER.example/mcp' }),
    );
  });
  it('differs when the transport/type changes', () => {
    assert.notEqual(mcpCredentialKey('srv', cfg), mcpCredentialKey('srv', { ...cfg, type: 'sse' }));
  });
  it('treats missing headers as an empty object (stable)', () => {
    assert.equal(
      mcpCredentialKey('srv', { type: 'http', url: 'u' }),
      mcpCredentialKey('srv', { type: 'http', url: 'u', headers: {} }),
    );
  });
  it('mcpTokenFileName embeds the credential key and is filesystem-safe', () => {
    const key = mcpCredentialKey('srv:prod/remote', cfg);
    const name = mcpTokenFileName(key);
    assert.match(name, /^[a-zA-Z0-9_.-]+\.json$/);
    // Two configs that differ only in URL produce distinct token files.
    assert.notEqual(name, mcpTokenFileName(mcpCredentialKey('srv:prod/remote', { ...cfg, url: 'https://x' })));
  });
});

// ---------------------------------------------------------------------------
// Gap: step-by-step progress logging + signal cancellation (session level)
// ---------------------------------------------------------------------------
const SERVER = 'https://rs.example/mcp';
const PRM_URL = 'https://rs.example/.well-known/oauth-protected-resource/mcp';
const AS_META = 'https://as.example/.well-known/oauth-authorization-server';

function routesFor(): Record<string, { ok?: boolean; status?: number; body: unknown }> {
  return {
    [PRM_URL]: { body: { resource: SERVER, authorization_servers: ['https://as.example'] } },
    [AS_META]: { body: { issuer: 'https://as.example', authorization_endpoint: 'https://as.example/authorize', token_endpoint: 'https://as.example/token' } },
    'https://as.example/token': { body: { access_token: 'AT', token_type: 'Bearer', expires_in: 3600 } },
  };
}

describe('w67 onLog diagnostic stream', () => {
  it('emits an ordered, redacted step event for each leg of a successful flow', async () => {
    const { fn } = fakeFetch(routesFor());
    const events: OAuthLogEvent[] = [];
    const session = new McpOAuthSession({
      serverUrl: SERVER,
      fetchImpl: fn,
      clientId: 'cid',
      redirectUri: 'http://localhost:9999/cb',
      authorize: async ({ authorizationUrl }) => {
        // The URL handed to onLog's 'authorize' step must already be redacted.
        assert.ok(authorizationUrl.includes('state='));
        return { code: 'THECODE' };
      },
      onLog: (ev) => events.push(ev),
    });
    const ok = await session.onUnauthorized('Bearer resource_metadata="' + PRM_URL + '"');
    assert.equal(ok, true);
    const steps = events.map((e) => e.step);
    assert.ok(steps.includes('discover-prm'), 'logs PRM discovery');
    assert.ok(steps.includes('discover-as'), 'logs AS discovery');
    assert.ok(steps.includes('authorize'), 'logs consent leg');
    assert.ok(steps.includes('exchange-code'), 'logs code exchange');
    assert.ok(steps.includes('token-stored'), 'logs token storage');
    // The 'authorize' progress line must carry a REDACTED URL (the sensitive param
    // value is replaced; URL.toString percent-encodes the marker to %5BREDACTED%5D).
    const authEv = events.find((e) => e.step === 'authorize' && e.ok === undefined)!;
    assert.ok(authEv.message.includes('REDACTED'), 'authorize log URL is redacted');
    assert.ok(decodeURIComponent(authEv.message).includes('state=[REDACTED]'), 'state value is redacted in the log');
    // Every event carries the server URL for attribution.
    assert.ok(events.every((e) => e.serverUrl === SERVER));
  });

  it('emits a cache-hit event when a live token is reused', async () => {
    const { fn } = fakeFetch(routesFor());
    const events: OAuthLogEvent[] = [];
    const session = new McpOAuthSession({
      serverUrl: SERVER, fetchImpl: fn, clientId: 'cid',
      redirectUri: 'http://localhost:9999/cb',
      authorize: async () => ({ code: 'THECODE' }),
      onLog: (ev) => events.push(ev),
    });
    await session.onUnauthorized('Bearer resource_metadata="' + PRM_URL + '"');
    events.length = 0;
    const header = await session.authorizationHeader();
    assert.ok(header?.startsWith('Bearer '));
    assert.equal(events[0]?.step, 'cache-hit');
  });

  it('an onLog that throws never breaks the flow', async () => {
    const { fn } = fakeFetch(routesFor());
    const session = new McpOAuthSession({
      serverUrl: SERVER, fetchImpl: fn, clientId: 'cid',
      redirectUri: 'http://localhost:9999/cb',
      authorize: async () => ({ code: 'THECODE' }),
      onLog: () => { throw new Error('observer boom'); },
    });
    assert.equal(await session.onUnauthorized('Bearer resource_metadata="' + PRM_URL + '"'), true);
  });
});

describe('w67 caller cancellation', () => {
  it('aborts the in-flight flow and emits a non-error "cancelled" event', async () => {
    const { fn, calls } = fakeFetch(routesFor());
    const events: OAuthLogEvent[] = [];
    const abort = new AbortController();
    abort.abort(); // pre-cancelled: the very first discovery fetch must reject.
    const session = new McpOAuthSession({
      serverUrl: SERVER, fetchImpl: fn, clientId: 'cid',
      redirectUri: 'http://localhost:9999/cb',
      signal: abort.signal,
      authorize: async () => ({ code: 'THECODE' }),
      onLog: (ev) => events.push(ev),
    });
    const ok = await session.onUnauthorized('Bearer resource_metadata="' + PRM_URL + '"');
    // RED without the fix: the caller signal was never composed into fetch, so the
    // flow would complete (ok=true) and no 'cancelled' event would be emitted.
    assert.equal(ok, false, 'a cancelled flow does not authorize');
    assert.equal(events.at(-1)?.step, 'cancelled', 'cancellation is distinguished from a real error');
    assert.equal(events.at(-1)?.ok, false);
    // The composed signal reached the fetch call.
    assert.ok(calls[0]?.init?.signal?.aborted, 'the aborted signal was composed into the request');
  });
});

// ---------------------------------------------------------------------------
// End-to-end — McpManager wires the config-hashed token file + per-server abort
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

describe('w67 McpManager OAuth wiring', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w67-'));
  const prevDataDir = process.env.SUPERINFERENCE_DATA_DIR;
  const prevConfigDir = process.env.CLAUDE_CONFIG_DIR;
  process.env.SUPERINFERENCE_DATA_DIR = tmp;
  delete process.env.CLAUDE_CONFIG_DIR;

  after(() => {
    if (prevDataDir === undefined) delete process.env.SUPERINFERENCE_DATA_DIR; else process.env.SUPERINFERENCE_DATA_DIR = prevDataDir;
    if (prevConfigDir !== undefined) process.env.CLAUDE_CONFIG_DIR = prevConfigDir;
    fs.rmSync(tmp, { recursive: true, force: true });
  });

  it('persists tokens under a config-hashed filename and cancels an in-flight flow on stopServer', async () => {
    const { fn } = fakeFetch(routesFor());
    const mgr = new McpManager();
    mgr._fetch = fn;
    let capturedOAuth: { onUnauthorized(h: string | null): Promise<boolean> } | undefined;
    mgr._createClient = ((cfg: { oauth?: { onUnauthorized(h: string | null): Promise<boolean> } }) => {
      capturedOAuth = cfg.oauth;
      return new FakeClient();
    }) as never;
    mgr.setOAuthAuthorizer(async () => ({ code: 'the-code' }));
    mgr.addPendingServer('oauth-srv', {
      command: '', transport: 'http', url: SERVER,
      oauth: { clientId: 'cid' },
    });
    await mgr.ensureConnected('oauth-srv');
    assert.ok(capturedOAuth, 'manager builds an OAuth provider');

    // A successful flow persists the token under the config-hash-derived filename.
    assert.equal(await capturedOAuth!.onUnauthorized('Bearer resource_metadata="' + PRM_URL + '"'), true);
    const expectedFile = mcpTokenFileName(mcpCredentialKey('oauth-srv', { type: 'http', url: SERVER, headers: undefined }));
    const tokenPath = path.join(tmp, 'mcp-tokens', expectedFile);
    assert.ok(fs.existsSync(tokenPath), `token persisted at config-hashed path ${expectedFile}`);

    // Stopping the server aborts its OAuth controller, which is composed into the
    // same session's fetch. A subsequent (re)authorization leg must now fail,
    // because the first discovery request is aborted. (The token response carried
    // no refresh_token, so onUnauthorized drives a full re-auth rather than a
    // silent refresh.)
    await mgr.stopServer('oauth-srv');
    const ok = await capturedOAuth!.onUnauthorized('Bearer resource_metadata="' + PRM_URL + '"');
    assert.equal(ok, false, 'stopServer aborts the in-flight OAuth flow via the composed signal');
  });
});
