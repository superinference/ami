import { describe, it, after, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'events';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  McpManager,
  inferMcpTransportFromUrl,
  isValidMcpUrl,
  formatMcpConnectivitySummary,
  type McpConnectivitySummary,
  type McpServerConfig,
} from '../src/mcp/manager';
import { mcpCredentialKey, mcpTokenFileName } from '../src/mcp/oauth';
import { resolveConfigDir } from '../src/reference-journal';

// ---------------------------------------------------------------------------
// U018-w74 — additive MCP manager parity (the reference):
//   * Transport inference from URL scheme (ide.tsx:L585-593) — a `url` with no
//     explicit `transport` dispatches over the inferred remote transport, never stdio.
//   * Add-time URL well-formedness validation (xaaIdpCommand.ts:L44-58).
//   * Internal/managed server protected from the enable/disable toggle (mcp.tsx:L31-32).
//   * Credential lifecycle hygiene — purge the persisted OAuth token file on remove,
//     even when the server has no live client (cli/handlers/mcp.tsx:L79-84).
//   * Proactive connectivity partition + notification format (useMcpConnectivityStatus).
// ---------------------------------------------------------------------------

/** A fake MCP client (factory seam) that records which connect method was invoked
 *  and exposes a configurable state so status/partition paths can be exercised. */
class FakeClient extends EventEmitter {
  state = 'idle';
  serverInfo: unknown = { name: 'fake', version: '1' };
  instructions: string | null = null;
  uptime: number | null = 1;
  tools: Array<{ name: string; description?: string }> = [];
  resources: unknown[] = [];
  prompts: unknown[] = [];
  serverCapabilities: Record<string, unknown> = {};
  onElicitation: unknown;
  onSampling: unknown;
  connectCalls: string[] = [];
  async connect(): Promise<void> { this.connectCalls.push('stdio'); this.state = 'ready'; }
  async connectHTTP(): Promise<void> { this.connectCalls.push('http'); this.state = 'ready'; }
  async connectSSE(): Promise<void> { this.connectCalls.push('sse'); this.state = 'ready'; }
  async connectWebSocket(): Promise<void> { this.connectCalls.push('ws'); this.state = 'ready'; }
  async listTools(): Promise<Array<{ name: string; description?: string }>> { return this.tools; }
  async listResources(): Promise<unknown[]> { return this.resources; }
  async reconnect(): Promise<void> {}
  disconnect(): void { this.state = 'stopped'; }
  setHeaders(): void {}
  async revokeAuth(): Promise<void> {}
}

function newManagerWithFake(): { mgr: McpManager; created: FakeClient[] } {
  const mgr = new McpManager();
  const created: FakeClient[] = [];
  (mgr as unknown as { _createClient: (o: unknown) => FakeClient })._createClient = () => {
    const c = new FakeClient();
    created.push(c);
    return c;
  };
  return { mgr, created };
}

// ===========================================================================
// inferMcpTransportFromUrl
// ===========================================================================
describe('inferMcpTransportFromUrl', () => {
  it('maps ws/wss schemes to the websocket transports', () => {
    assert.equal(inferMcpTransportFromUrl('ws://host/mcp'), 'ws');
    assert.equal(inferMcpTransportFromUrl('wss://host/mcp'), 'wss');
  });
  it('maps http/https schemes to the http transport', () => {
    assert.equal(inferMcpTransportFromUrl('http://host/mcp'), 'http');
    assert.equal(inferMcpTransportFromUrl('https://host/mcp'), 'http');
  });
  it('returns undefined for an unknown scheme, a non-URL, or undefined', () => {
    assert.equal(inferMcpTransportFromUrl('ftp://host/x'), undefined);
    assert.equal(inferMcpTransportFromUrl('not a url'), undefined);
    assert.equal(inferMcpTransportFromUrl(undefined), undefined);
    assert.equal(inferMcpTransportFromUrl(''), undefined);
  });
});

// ===========================================================================
// isValidMcpUrl
// ===========================================================================
describe('isValidMcpUrl', () => {
  it('accepts well-formed http/https/ws/wss URLs', () => {
    for (const u of ['http://h/x', 'https://h/x', 'ws://h/x', 'wss://h/x']) {
      assert.equal(isValidMcpUrl(u), true, u);
    }
  });
  it('rejects malformed URLs and disallowed schemes', () => {
    for (const u of ['', '   ', 'not a url', 'owner/repo', 'ftp://h/x', 'file:///etc/passwd']) {
      assert.equal(isValidMcpUrl(u), false, u);
    }
  });
});

// ===========================================================================
// connectServer transport dispatch uses the inferred transport
// ===========================================================================
describe('connectServer — transport inference dispatch', () => {
  it('dispatches a transport-less wss URL over the websocket transport (not stdio)', async () => {
    const { mgr, created } = newManagerWithFake();
    mgr.addServer('wsonly', { command: '', url: 'wss://host/mcp' } as McpServerConfig);
    await mgr.connectServer('wsonly');
    assert.deepEqual(created[0].connectCalls, ['ws']);
  });
  it('dispatches a transport-less https URL over the http transport (not stdio)', async () => {
    const { mgr, created } = newManagerWithFake();
    mgr.addServer('httponly', { command: '', url: 'https://host/mcp' } as McpServerConfig);
    await mgr.connectServer('httponly');
    assert.deepEqual(created[0].connectCalls, ['http']);
  });
  it('still spawns stdio for a plain command server with no url', async () => {
    const { mgr, created } = newManagerWithFake();
    mgr.addServer('stdio', { command: 'node', args: ['s.js'] });
    await mgr.connectServer('stdio');
    assert.deepEqual(created[0].connectCalls, ['stdio']);
  });
  it('honors an explicit transport over inference', async () => {
    const { mgr, created } = newManagerWithFake();
    mgr.addServer('explicit', { command: '', transport: 'sse', url: 'https://host/mcp' } as McpServerConfig);
    await mgr.connectServer('explicit');
    assert.deepEqual(created[0].connectCalls, ['sse']);
  });
});

// ===========================================================================
// Internal/managed server protected from the enable/disable toggle
// ===========================================================================
describe('protected (internal/managed) servers', () => {
  it('seeds, reports, and matches protected names (normalization-insensitive)', () => {
    const { mgr } = newManagerWithFake();
    mgr.setProtectedServers(['ide', 'my.bridge']);
    assert.deepEqual(mgr.getProtectedServers().sort(), ['ide', 'my.bridge']);
    assert.equal(mgr.isServerProtected('ide'), true);
    assert.equal(mgr.isServerProtected('my_bridge'), true); // `.` <-> `_` normalization
    assert.equal(mgr.isServerProtected('other'), false);
  });
  it('refuses to DISABLE a protected server but allows a plain stop', async () => {
    const { mgr } = newManagerWithFake();
    mgr.addServer('ide', { command: 'node' });
    mgr.setProtectedServers(['ide']);
    await assert.rejects(() => mgr.stopServer('ide', true), /internal\/managed and cannot be disabled/);
    // A transient stop (no disable) is still permitted.
    await assert.doesNotReject(() => mgr.stopServer('ide'));
    assert.equal(mgr.isDisabled('ide'), false);
  });
  it('still allows disabling a non-protected server', async () => {
    const { mgr } = newManagerWithFake();
    mgr.addServer('plain', { command: 'node' });
    await mgr.stopServer('plain', true);
    assert.equal(mgr.isDisabled('plain'), true);
  });
});

// ===========================================================================
// Credential lifecycle hygiene — purge persisted OAuth token file on remove
// ===========================================================================
describe('removeServer — OAuth token purge', () => {
  let tmp: string;
  let prevDataDir: string | undefined;
  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-purge-'));
    prevDataDir = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = tmp;
  });
  afterEach(() => {
    if (prevDataDir === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
    else process.env.SUPERINFERENCE_DATA_DIR = prevDataDir;
    try { fs.rmSync(tmp, { recursive: true, force: true }); } catch {}
  });

  function tokenPathFor(name: string, cfg: McpServerConfig): string {
    const key = mcpCredentialKey(name, { type: cfg.transport, url: cfg.url, headers: cfg.headers });
    return path.join(resolveConfigDir(), 'mcp-tokens', mcpTokenFileName(key));
  }

  it('purges the persisted token file of an http oauth server with NO live client', () => {
    const { mgr } = newManagerWithFake();
    const cfg: McpServerConfig = { command: '', transport: 'http', url: 'https://host/mcp', oauth: {} };
    // A pending (never-connected) server: there is no live client, so the server-side
    // revokeAuth path cannot run — only the on-disk purge can clean it up.
    mgr.addPendingServer('remote', cfg);
    const tokenFile = tokenPathFor('remote', cfg);
    fs.mkdirSync(path.dirname(tokenFile), { recursive: true });
    fs.writeFileSync(tokenFile, JSON.stringify({ accessToken: 'x', tokenType: 'Bearer' }));
    assert.equal(fs.existsSync(tokenFile), true);
    mgr.removeServer('remote');
    assert.equal(fs.existsSync(tokenFile), false, 'token file must be purged on remove');
  });

  it('does not throw removing a stdio server that has no token file', () => {
    const { mgr } = newManagerWithFake();
    mgr.addPendingServer('local', { command: 'node' });
    assert.doesNotThrow(() => mgr.removeServer('local'));
  });
});

// ===========================================================================
// Connectivity partition + notification formatting
// ===========================================================================
describe('getConnectivitySummary / formatMcpConnectivitySummary', () => {
  it('partitions live servers into failed vs needs-auth, alphabetically', () => {
    const { mgr, created } = newManagerWithFake();
    for (const n of ['zed', 'able', 'auth1']) mgr.addServer(n, { command: 'node' });
    // Drive each fake client into a distinct state.
    created[0].state = 'error';     // zed
    created[1].state = 'ready';     // able
    created[2].state = 'needs-auth'; // auth1
    const summary = mgr.getConnectivitySummary();
    assert.deepEqual(summary.failed, ['zed']);
    assert.deepEqual(summary.needsAuth, ['auth1']);
  });

  it('formats pluralized, dedupe-keyed notifications with the /mcp pointer', () => {
    const one: McpConnectivitySummary = { failed: ['a'], needsAuth: ['b'] };
    assert.deepEqual(formatMcpConnectivitySummary(one), [
      { key: 'mcp-failed', severity: 'error', message: '1 MCP server failed · /mcp' },
      { key: 'mcp-needs-auth', severity: 'warning', message: '1 MCP server needs auth · /mcp' },
    ]);
    const many: McpConnectivitySummary = { failed: ['a', 'c'], needsAuth: ['b', 'd', 'e'] };
    assert.deepEqual(formatMcpConnectivitySummary(many), [
      { key: 'mcp-failed', severity: 'error', message: '2 MCP servers failed · /mcp' },
      { key: 'mcp-needs-auth', severity: 'warning', message: '3 MCP servers need auth · /mcp' },
    ]);
  });

  it('returns no notifications when nothing is failed / needs-auth', () => {
    assert.deepEqual(formatMcpConnectivitySummary({ failed: [], needsAuth: [] }), []);
  });
});

after(() => { /* temp dirs cleaned per-test */ });
