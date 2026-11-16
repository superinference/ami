/**
 * mcp-oauth-parity-u036-w68.test.ts — unit + integration tests for the U036/w68
 * OAuth parity batch on core/core/src/mcp/oauth.ts (+ manager/client wiring):
 *
 *   1. CIMD (SEP-991): resolveClientMetadataUrl env/config precedence + https guard;
 *      discoverAuthServer parses client_id_metadata_document_supported; the flow uses
 *      the metadata-document URL as client_id and skips Dynamic Client Registration
 *      when the AS advertises support (else falls back to DCR).
 *   2. invalidateCredentials(scope): granular local invalidation
 *      ('tokens'|'client'|'discovery'|'verifier'|'all'); wired into the invalid_grant
 *      refresh path and the full-forget teardown.
 *   3. clientId-change invalidation: a persisted grant minted under a different
 *      client_id is dropped on construction; a matching/absent one is kept. The
 *      client_id is stamped onto exchanged/refreshed tokens.
 *   4. Cross-process refresh lockfile + re-read-after-lock dedup on FileOAuthTokenStore.
 *   5. McpManager threads oauth.clientMetadataUrl into the live session.
 *
 * All network I/O is through an injected FetchLike so these are pure and fast.
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { EventEmitter } from 'node:events';

import {
  resolveClientMetadataUrl, MCP_OAUTH_CLIENT_METADATA_URL_ENV,
  discoverAuthServer, McpOAuthSession, FileOAuthTokenStore,
} from '../src/mcp/oauth';
import type { FetchLike, OAuthTokens } from '../src/mcp/oauth';
import { McpManager } from '../src/mcp/manager';

function fakeFetch(routes: Record<string, { ok?: boolean; status?: number; body: unknown }>): { fn: FetchLike; calls: Array<{ url: string; init?: { body?: string } }> } {
  const calls: Array<{ url: string; init?: { body?: string } }> = [];
  const fn: FetchLike = async (url, init) => {
    calls.push({ url, init: init as { body?: string } });
    const r = routes[url];
    if (!r) return { ok: false, status: 404, json: async () => ({}) };
    return { ok: r.ok ?? true, status: r.status ?? 200, json: async () => r.body };
  };
  return { fn, calls };
}

const tmpDirs: string[] = [];
function tmpFile(name = 'tokens.json'): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-oauth-u036-'));
  tmpDirs.push(dir);
  return path.join(dir, name);
}
afterEach(() => { for (const d of tmpDirs.splice(0)) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } } });

// ---------------------------------------------------------------------------
// 1. CIMD (SEP-991)
// ---------------------------------------------------------------------------
describe('resolveClientMetadataUrl (CIMD / SEP-991)', () => {
  it('returns the configured URL when no env override is set', () => {
    assert.equal(resolveClientMetadataUrl('https://cfg.example/cimd.json', {}), 'https://cfg.example/cimd.json');
  });
  it('prefers the MCP_OAUTH_CLIENT_METADATA_URL env override over the configured URL', () => {
    const env = { [MCP_OAUTH_CLIENT_METADATA_URL_ENV]: 'https://env.example/cimd.json' } as NodeJS.ProcessEnv;
    assert.equal(resolveClientMetadataUrl('https://cfg.example/cimd.json', env), 'https://env.example/cimd.json');
  });
  it('returns undefined when neither is set (CIMD disabled — no hardcoded default)', () => {
    assert.equal(resolveClientMetadataUrl(undefined, {}), undefined);
  });
  it('refuses a plaintext http:// metadata document URL', () => {
    assert.throws(() => resolveClientMetadataUrl('http://insecure.example/cimd.json', {}), /must use https/);
    assert.throws(() => resolveClientMetadataUrl(undefined, { [MCP_OAUTH_CLIENT_METADATA_URL_ENV]: 'http://x/y' } as NodeJS.ProcessEnv), /must use https/);
  });
});

describe('discoverAuthServer — client_id_metadata_document_supported', () => {
  const AS_META = 'https://as.example/.well-known/oauth-authorization-server';
  it('parses the SEP-991 flag as a boolean', async () => {
    const { fn } = fakeFetch({ [AS_META]: { body: { issuer: 'https://as.example', authorization_endpoint: 'https://as.example/authorize', token_endpoint: 'https://as.example/token', client_id_metadata_document_supported: true } } });
    const meta = await discoverAuthServer(fn, 'https://as.example');
    assert.equal(meta.clientIdMetadataDocumentSupported, true);
  });
  it('defaults to false when the flag is absent', async () => {
    const { fn } = fakeFetch({ [AS_META]: { body: { issuer: 'https://as.example', authorization_endpoint: 'https://as.example/authorize', token_endpoint: 'https://as.example/token' } } });
    const meta = await discoverAuthServer(fn, 'https://as.example');
    assert.equal(meta.clientIdMetadataDocumentSupported, false);
  });
});

describe('McpOAuthSession — CIMD flow', () => {
  const SERVER = 'https://rs.example/mcp';
  const PRM_URL = 'https://rs.example/.well-known/oauth-protected-resource/mcp';
  const AS_META = 'https://as.example/.well-known/oauth-authorization-server';
  const CIMD = 'https://claude.example/client-metadata.json';

  function routes(cimdSupported: boolean) {
    return {
      [PRM_URL]: { body: { resource: SERVER, authorization_servers: ['https://as.example'] } },
      [AS_META]: { body: { issuer: 'https://as.example', authorization_endpoint: 'https://as.example/authorize', token_endpoint: 'https://as.example/token', registration_endpoint: 'https://as.example/register', client_id_metadata_document_supported: cimdSupported } },
      'https://as.example/register': { body: { client_id: 'dyn-cid' } },
      'https://as.example/token': { body: { access_token: 'AT', token_type: 'Bearer', expires_in: 3600 } },
    };
  }

  it('uses the CIMD URL as client_id and skips DCR when the AS advertises support', async () => {
    const { fn, calls } = fakeFetch(routes(true));
    let authUrl = '';
    const session = new McpOAuthSession({
      serverUrl: SERVER, fetchImpl: fn, redirectUri: 'http://127.0.0.1:9/cb',
      clientMetadataUrl: CIMD,
      authorize: async ({ authorizationUrl }) => { authUrl = authorizationUrl; return { code: 'c' }; },
      now: () => 0,
    });
    assert.equal(await session.onUnauthorized('Bearer resource_metadata="' + PRM_URL + '"'), true);
    // The CIMD URL became the client_id, and the registration endpoint was NOT hit.
    assert.equal(new URL(authUrl).searchParams.get('client_id'), CIMD);
    assert.ok(!calls.some((c) => c.url === 'https://as.example/register'), 'DCR should be skipped under CIMD');
  });

  it('falls back to Dynamic Client Registration when the AS does not advertise CIMD', async () => {
    const { fn, calls } = fakeFetch(routes(false));
    let authUrl = '';
    const session = new McpOAuthSession({
      serverUrl: SERVER, fetchImpl: fn, redirectUri: 'http://127.0.0.1:9/cb',
      clientMetadataUrl: CIMD,
      authorize: async ({ authorizationUrl }) => { authUrl = authorizationUrl; return { code: 'c' }; },
      now: () => 0,
    });
    assert.equal(await session.onUnauthorized('Bearer resource_metadata="' + PRM_URL + '"'), true);
    assert.equal(new URL(authUrl).searchParams.get('client_id'), 'dyn-cid');
    assert.ok(calls.some((c) => c.url === 'https://as.example/register'), 'DCR should run without CIMD support');
  });

  it('honors the MCP_OAUTH_CLIENT_METADATA_URL env override in the flow', async () => {
    const prev = process.env[MCP_OAUTH_CLIENT_METADATA_URL_ENV];
    process.env[MCP_OAUTH_CLIENT_METADATA_URL_ENV] = 'https://env.example/cimd.json';
    try {
      const { fn } = fakeFetch(routes(true));
      let authUrl = '';
      const session = new McpOAuthSession({
        serverUrl: SERVER, fetchImpl: fn, redirectUri: 'http://127.0.0.1:9/cb',
        clientMetadataUrl: CIMD, // overridden by env
        authorize: async ({ authorizationUrl }) => { authUrl = authorizationUrl; return { code: 'c' }; },
        now: () => 0,
      });
      assert.equal(await session.onUnauthorized(null), true);
      assert.equal(new URL(authUrl).searchParams.get('client_id'), 'https://env.example/cimd.json');
    } finally {
      if (prev === undefined) delete process.env[MCP_OAUTH_CLIENT_METADATA_URL_ENV];
      else process.env[MCP_OAUTH_CLIENT_METADATA_URL_ENV] = prev;
    }
  });
});

// ---------------------------------------------------------------------------
// 2. invalidateCredentials(scope)
// ---------------------------------------------------------------------------
describe('McpOAuthSession.invalidateCredentials', () => {
  const SERVER = 'https://rs.example/mcp';
  const PRM_URL = 'https://rs.example/.well-known/oauth-protected-resource/mcp';
  const AS_META = 'https://as.example/.well-known/oauth-authorization-server';

  function fullRoutes() {
    return {
      [PRM_URL]: { body: { resource: SERVER, authorization_servers: ['https://as.example'] } },
      [AS_META]: { body: { issuer: 'https://as.example', authorization_endpoint: 'https://as.example/authorize', token_endpoint: 'https://as.example/token', registration_endpoint: 'https://as.example/register' } },
      'https://as.example/register': { body: { client_id: 'dyn-cid' } },
      'https://as.example/token': { body: { access_token: 'AT', token_type: 'Bearer', refresh_token: 'RT', expires_in: 3600 } },
    };
  }

  it("'tokens' drops the access token (header becomes null)", async () => {
    const { fn } = fakeFetch(fullRoutes());
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', authorize: async () => ({ code: 'c' }), now: () => 0 });
    await session.onUnauthorized(null);
    assert.equal(await session.authorizationHeader(), 'Bearer AT');
    await session.invalidateCredentials('tokens');
    assert.equal(await session.authorizationHeader(), null);
  });

  it("'discovery' forces re-discovery on the next flow", async () => {
    const { fn, calls } = fakeFetch(fullRoutes());
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', authorize: async () => ({ code: 'c' }), now: () => 0 });
    await session.onUnauthorized(null);
    const prmBefore = calls.filter((c) => c.url === PRM_URL).length;
    await session.invalidateCredentials('discovery');
    await session.onUnauthorized(null);
    const prmAfter = calls.filter((c) => c.url === PRM_URL).length;
    assert.equal(prmAfter, prmBefore + 1, 'discovery should re-run after invalidation');
  });

  it("'client' forces re-registration on the next flow", async () => {
    const { fn, calls } = fakeFetch(fullRoutes());
    // No static clientId → the first flow dynamically registers.
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', authorize: async () => ({ code: 'c' }), now: () => 0 });
    await session.onUnauthorized(null);
    const regBefore = calls.filter((c) => c.url === 'https://as.example/register').length;
    assert.equal(regBefore, 1);
    await session.invalidateCredentials('client');
    await session.onUnauthorized(null);
    const regAfter = calls.filter((c) => c.url === 'https://as.example/register').length;
    assert.equal(regAfter, 2, 'a dropped dynamic client should re-register');
  });

  it("'verifier' is a no-op (nothing persisted) and leaves tokens intact", async () => {
    const { fn } = fakeFetch(fullRoutes());
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', authorize: async () => ({ code: 'c' }), now: () => 0 });
    await session.onUnauthorized(null);
    await session.invalidateCredentials('verifier');
    assert.equal(await session.authorizationHeader(), 'Bearer AT');
  });

  it("'all' clears tokens, the dynamic client, and discovery together", async () => {
    const { fn, calls } = fakeFetch(fullRoutes());
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', authorize: async () => ({ code: 'c' }), now: () => 0 });
    await session.onUnauthorized(null);
    await session.invalidateCredentials('all');
    assert.equal(await session.authorizationHeader(), null); // tokens gone
    await session.onUnauthorized(null);
    // Re-registration AND re-discovery both happened again.
    assert.equal(calls.filter((c) => c.url === 'https://as.example/register').length, 2);
    assert.equal(calls.filter((c) => c.url === PRM_URL).length, 2);
  });

  it('is wired into the invalid_grant refresh path (dead grant clears tokens)', async () => {
    let t = 1000;
    const routes = fullRoutes();
    const { fn } = fakeFetch(routes);
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', authorize: async () => ({ code: 'c' }), now: () => t });
    await session.onUnauthorized(null); // AT with RT, expires at 1000+3600_000
    // Make the token look expired and the refresh fail with invalid_grant.
    t = 5_000_000;
    routes['https://as.example/token'] = { body: { error: 'invalid_grant', error_description: 'dead' } };
    const header = await session.authorizationHeader(); // triggers silent refresh → invalid_grant
    assert.equal(header, null);
    // The dead grant was cleared (hasRefreshToken would be false) — a subsequent
    // authorizationHeader cannot silently refresh and returns null without a POST.
    assert.equal(await session.authorizationHeader(), null);
  });
});

// ---------------------------------------------------------------------------
// 3. clientId-change invalidation + client_id stamping
// ---------------------------------------------------------------------------
describe('clientId-change invalidation', () => {
  const SERVER = 'https://rs.example/mcp';

  function writeStored(file: string, tokens: Partial<OAuthTokens>) {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, JSON.stringify({ accessToken: 'AT', tokenType: 'Bearer', expiresAt: Date.now() + 1_000_000, ...tokens }));
  }

  it('drops a persisted grant minted under a DIFFERENT client_id', () => {
    const file = tmpFile();
    writeStored(file, { clientId: 'old-cid' });
    const store = new FileOAuthTokenStore(file);
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fakeFetch({}).fn, redirectUri: 'r', clientId: 'new-cid', store, authorize: async () => ({ code: 'c' }) });
    assert.equal(store.get(), null, 'stale grant should be cleared');
    void session;
  });

  it('keeps a persisted grant whose client_id MATCHES the configured one', () => {
    const file = tmpFile();
    writeStored(file, { clientId: 'cid' });
    const store = new FileOAuthTokenStore(file);
    new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fakeFetch({}).fn, redirectUri: 'r', clientId: 'cid', store, authorize: async () => ({ code: 'c' }) });
    assert.equal(store.get()?.accessToken, 'AT');
  });

  it('keeps a persisted grant that carries no client_id (legacy / absent)', () => {
    const file = tmpFile();
    writeStored(file, {}); // no clientId field
    const store = new FileOAuthTokenStore(file);
    new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fakeFetch({}).fn, redirectUri: 'r', clientId: 'cid', store, authorize: async () => ({ code: 'c' }) });
    assert.equal(store.get()?.accessToken, 'AT');
  });

  it('stamps the client_id onto exchanged tokens so a later change is detectable', async () => {
    const file = tmpFile();
    const { fn } = fakeFetch({
      'https://rs.example/.well-known/oauth-protected-resource/mcp': { body: { resource: SERVER, authorization_servers: ['https://as.example'] } },
      'https://as.example/.well-known/oauth-authorization-server': { body: { issuer: 'https://as.example', authorization_endpoint: 'https://as.example/authorize', token_endpoint: 'https://as.example/token' } },
      'https://as.example/token': { body: { access_token: 'AT', token_type: 'Bearer', expires_in: 3600 } },
    });
    const store = new FileOAuthTokenStore(file);
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', store, authorize: async () => ({ code: 'c' }), now: () => 0 });
    await session.onUnauthorized(null);
    const persisted = JSON.parse(fs.readFileSync(file, 'utf-8')) as OAuthTokens;
    assert.equal(persisted.clientId, 'cid');
  });
});

// ---------------------------------------------------------------------------
// 4. Cross-process refresh lockfile + re-read-after-lock dedup
// ---------------------------------------------------------------------------
describe('FileOAuthTokenStore.withRefreshLock', () => {
  it('runs the critical section under a lock and removes the lock dir afterward', async () => {
    const file = tmpFile();
    const store = new FileOAuthTokenStore(file);
    let lockExistedDuring = false;
    await store.withRefreshLock(async () => { lockExistedDuring = fs.existsSync(`${file}.lock`); });
    assert.equal(lockExistedDuring, true);
    assert.equal(fs.existsSync(`${file}.lock`), false);
  });

  it('proceeds WITHOUT the lock (still runs fn) when a fresh lock is held by another holder', async () => {
    const file = tmpFile();
    fs.mkdirSync(`${file}.lock`); // simulate another process holding it
    const store = new FileOAuthTokenStore(file, { lockRetries: 2, sleep: async () => {} });
    let ran = false;
    await store.withRefreshLock(async () => { ran = true; });
    assert.equal(ran, true, 'must not deadlock — proceed unlocked after retries');
    // We did not hold it, so we must not delete the other holder's lock dir.
    assert.equal(fs.existsSync(`${file}.lock`), true);
    fs.rmSync(`${file}.lock`, { recursive: true });
  });

  it('steals a STALE lock left by a crashed holder', async () => {
    const file = tmpFile();
    fs.mkdirSync(`${file}.lock`);
    // now() far in the future makes the lock dir appear older than lockStaleMs.
    const store = new FileOAuthTokenStore(file, { lockStaleMs: 1000, now: () => Date.now() + 10_000, sleep: async () => {} });
    let ran = false;
    await store.withRefreshLock(async () => { ran = true; });
    assert.equal(ran, true);
    assert.equal(fs.existsSync(`${file}.lock`), false, 'a stolen+acquired lock is released at the end');
  });

  it('proceeds unlocked on a non-contention mkdir error (missing parent dir)', async () => {
    const file = path.join(os.tmpdir(), 'ami-oauth-u036-missing', 'nope', 'tokens.json');
    const store = new FileOAuthTokenStore(file);
    let ran = false;
    await store.withRefreshLock(async () => { ran = true; });
    assert.equal(ran, true);
  });

  it('re-reads after acquiring the lock and skips the POST when a sibling already refreshed', async () => {
    let t = 1000;
    const SERVER = 'https://rs.example/mcp';
    const PRM_URL = 'https://rs.example/.well-known/oauth-protected-resource/mcp';
    const AS_META = 'https://as.example/.well-known/oauth-authorization-server';
    const routes = {
      [PRM_URL]: { body: { resource: SERVER, authorization_servers: ['https://as.example'] } },
      [AS_META]: { body: { issuer: 'https://as.example', authorization_endpoint: 'https://as.example/authorize', token_endpoint: 'https://as.example/token', registration_endpoint: 'https://as.example/register' } },
      'https://as.example/register': { body: { client_id: 'dyn-cid' } },
      'https://as.example/token': { body: { access_token: 'AT', token_type: 'Bearer', refresh_token: 'RT', expires_in: 3600 } },
    };
    const { fn, calls } = fakeFetch(routes);
    const file = tmpFile();
    const store = new FileOAuthTokenStore(file);
    const session = new McpOAuthSession({ serverUrl: SERVER, fetchImpl: fn, redirectUri: 'r', clientId: 'cid', store, authorize: async () => ({ code: 'c' }), now: () => t });
    await session.onUnauthorized(null); // one token exchange
    const tokenCallsBefore = calls.filter((c) => c.url === 'https://as.example/token').length;
    assert.equal(tokenCallsBefore, 1);
    // The in-memory token is now expired, but a SIBLING process writes a fresh token
    // to the shared file before our refresh acquires the lock.
    t = 5_000_000;
    fs.writeFileSync(file, JSON.stringify({ accessToken: 'AT-SIBLING', tokenType: 'Bearer', refreshToken: 'RT2', expiresAt: t + 1_000_000, clientId: 'cid' }));
    const header = await session.authorizationHeader(); // triggers silent refresh → reload picks up sibling token
    assert.equal(header, 'Bearer AT-SIBLING');
    // No second token POST — the sibling refresh was reused.
    assert.equal(calls.filter((c) => c.url === 'https://as.example/token').length, 1);
  });
});

// ---------------------------------------------------------------------------
// 5. McpManager threads oauth.clientMetadataUrl into the live session
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

describe('U036/w68 McpManager CIMD wiring', () => {
  const SERVER = 'https://rs.example/mcp';
  const PRM_URL = 'https://rs.example/.well-known/oauth-protected-resource/mcp';
  const CIMD = 'https://claude.example/client-metadata.json';

  it('passes oauth.clientMetadataUrl from server config into the session (used as client_id under CIMD)', async () => {
    const { fn } = fakeFetch({
      [PRM_URL]: { body: { resource: SERVER, authorization_servers: ['https://as.example'] } },
      'https://as.example/.well-known/oauth-authorization-server': { body: { issuer: 'https://as.example', authorization_endpoint: 'https://as.example/authorize', token_endpoint: 'https://as.example/token', client_id_metadata_document_supported: true } },
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
      oauth: { clientMetadataUrl: CIMD },
    });
    await mgr.ensureConnected('oauth-srv');
    assert.ok(capturedOAuth, 'manager should build an OAuth provider');
    assert.equal(await capturedOAuth!.onUnauthorized('Bearer resource_metadata="' + PRM_URL + '"'), true);
    assert.equal(new URL(presentedUrl).searchParams.get('client_id'), CIMD);
  });
});
