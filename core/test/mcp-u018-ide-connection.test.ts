import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'events';
import {
  McpManager,
  IDE_MCP_SERVER_NAME,
  canonicalMcpTransport,
  getMcpIdeName,
  ideStatusFromServerState,
  isRemoteMcpTransport,
  normalizeMcpServerEntry,
  validateMcpServerConfigShape,
  type McpServerConfig,
} from '../src/mcp/manager';

// ---------------------------------------------------------------------------
// U018 — IDE MCP connection parity (Claude Code useIdeConnectionStatus.ts +
// the `sse-ide`/`ws-ide`/`sdk` transport discriminants):
//   * McpServerConfig.ideName + the IDE/sdk transport discriminants validate.
//   * canonicalMcpTransport maps the IDE discriminants to their base transport.
//   * getMcpIdeName extracts the editor name gated on an IDE transport.
//   * ideStatusFromServerState maps a live state → the editor-badge tri-state.
//   * McpManager.getIdeConnectionStatus derives the reserved `ide` server status.
// ---------------------------------------------------------------------------

/** A minimal fake MCP client returned by the factory seam, so connect/ready is
 *  driveable without spawning a real server. */
class FakeClient extends EventEmitter {
  state = 'idle';
  serverInfo: unknown = { name: 'ide', version: '1' };
  instructions: string | null = null;
  uptime: number | null = 1;
  tools: Array<{ name: string; description?: string }> = [];
  onElicitation: unknown;
  onSampling: unknown;
  async connect(): Promise<void> { this.state = 'ready'; }
  async connectHTTP(): Promise<void> { this.state = 'ready'; }
  async connectSSE(): Promise<void> { this.state = 'ready'; }
  async connectWebSocket(): Promise<void> { this.state = 'ready'; }
  async listTools(): Promise<Array<{ name: string; description?: string }>> { return this.tools; }
  async listResources(): Promise<unknown[]> { return []; }
  async listPrompts(): Promise<unknown[]> { return []; }
  async callTool(): Promise<unknown> { return { ok: true }; }
  disconnect(): void { this.state = 'stopped'; }
  setHeaders(): void {}
  async revokeAuth(): Promise<void> {}
}

describe('canonicalMcpTransport', () => {
  it('maps the IDE discriminants to their base transport', () => {
    assert.equal(canonicalMcpTransport('sse-ide'), 'sse');
    assert.equal(canonicalMcpTransport('ws-ide'), 'ws');
  });
  it('returns base transports and sdk/undefined unchanged', () => {
    assert.equal(canonicalMcpTransport('sse'), 'sse');
    assert.equal(canonicalMcpTransport('http'), 'http');
    assert.equal(canonicalMcpTransport('ws'), 'ws');
    assert.equal(canonicalMcpTransport('wss'), 'wss');
    assert.equal(canonicalMcpTransport('stdio'), 'stdio');
    assert.equal(canonicalMcpTransport('sdk'), 'sdk');
    assert.equal(canonicalMcpTransport(undefined), undefined);
  });
});

describe('isRemoteMcpTransport (IDE discriminants)', () => {
  it('treats sse-ide/ws-ide as remote', () => {
    assert.equal(isRemoteMcpTransport('sse-ide'), true);
    assert.equal(isRemoteMcpTransport('ws-ide'), true);
  });
  it('still treats stdio/sdk/undefined as local', () => {
    assert.equal(isRemoteMcpTransport('stdio'), false);
    assert.equal(isRemoteMcpTransport('sdk'), false);
    assert.equal(isRemoteMcpTransport(undefined), false);
  });
});

describe('getMcpIdeName', () => {
  it('returns the editor name for an IDE transport', () => {
    assert.equal(getMcpIdeName({ command: '', transport: 'sse-ide', ideName: 'VS Code' }), 'VS Code');
    assert.equal(getMcpIdeName({ command: '', transport: 'ws-ide', ideName: 'JetBrains' }), 'JetBrains');
  });
  it('returns null for a non-IDE transport even when ideName is set', () => {
    assert.equal(getMcpIdeName({ command: '', transport: 'sse', ideName: 'VS Code' }), null);
    assert.equal(getMcpIdeName({ command: 'node' }), null);
  });
  it('returns null for an IDE transport with no ideName, and for undefined config', () => {
    assert.equal(getMcpIdeName({ command: '', transport: 'sse-ide' }), null);
    assert.equal(getMcpIdeName(undefined), null);
  });
});

describe('ideStatusFromServerState', () => {
  it('maps ready → connected', () => {
    assert.equal(ideStatusFromServerState('ready'), 'connected');
  });
  it('maps connecting/pending → pending', () => {
    assert.equal(ideStatusFromServerState('pending'), 'pending');
    assert.equal(ideStatusFromServerState('connecting'), 'pending');
  });
  it('maps every other state → disconnected', () => {
    assert.equal(ideStatusFromServerState('error'), 'disconnected');
    assert.equal(ideStatusFromServerState('stopped'), 'disconnected');
    assert.equal(ideStatusFromServerState('needs-auth'), 'disconnected');
    assert.equal(ideStatusFromServerState('disabled'), 'disconnected');
    assert.equal(ideStatusFromServerState(undefined), 'disconnected');
  });
});

describe('validateMcpServerConfigShape (IDE fields)', () => {
  it('accepts the IDE/sdk transport discriminants and a string ideName', () => {
    assert.deepEqual(validateMcpServerConfigShape({ transport: 'sse-ide', url: 'https://x', ideName: 'VS Code' }), []);
    assert.deepEqual(validateMcpServerConfigShape({ transport: 'ws-ide', url: 'wss://x', ideName: 'VS Code' }), []);
    assert.deepEqual(validateMcpServerConfigShape({ transport: 'sdk' }), []);
  });
  it('rejects a non-string ideName', () => {
    const issues = validateMcpServerConfigShape({ command: 'node', ideName: 42 });
    assert.equal(issues.length, 1);
    assert.match(issues[0], /ideName/);
  });
  it('lists the IDE discriminants in the unsupported-transport message', () => {
    const issues = validateMcpServerConfigShape({ transport: 'bogus' });
    assert.equal(issues.length, 1);
    assert.match(issues[0], /sse-ide/);
    assert.match(issues[0], /ws-ide/);
  });
  it('normalizeMcpServerEntry maps a reference-style type:sse-ide to transport', () => {
    const out = normalizeMcpServerEntry({ type: 'sse-ide', url: 'https://x', ideName: 'VS Code' }) as Record<string, unknown>;
    assert.equal(out.transport, 'sse-ide');
    assert.equal(out.type, undefined);
    assert.deepEqual(validateMcpServerConfigShape(out), []);
  });
});

describe('McpManager.getIdeConnectionStatus', () => {
  it('returns {status:null, ideName:null} when no ide server is wired', () => {
    const mgr = new McpManager();
    mgr.addServer('other', { command: 'node' });
    assert.deepEqual(mgr.getIdeConnectionStatus(), { status: null, ideName: null });
  });

  it('reports pending with the editor name while the ide server awaits connect', () => {
    const mgr = new McpManager();
    const cfg: McpServerConfig = { command: '', transport: 'sse-ide', url: 'https://ide', ideName: 'VS Code' };
    mgr.addPendingServer(IDE_MCP_SERVER_NAME, cfg);
    assert.deepEqual(mgr.getIdeConnectionStatus(), { status: 'pending', ideName: 'VS Code' });
  });

  it('reports connected with the editor name once the ide server is ready', async () => {
    const mgr = new McpManager();
    const client = new FakeClient();
    mgr._createClient = (() => client) as never;
    const cfg: McpServerConfig = { command: '', transport: 'ws-ide', url: 'wss://ide', ideName: 'JetBrains' };
    mgr.addServer(IDE_MCP_SERVER_NAME, cfg);
    await mgr.ensureConnected(IDE_MCP_SERVER_NAME);
    assert.deepEqual(mgr.getIdeConnectionStatus(), { status: 'connected', ideName: 'JetBrains' });
  });

  it('reports disconnected (no ideName) when a non-IDE ide server errors', async () => {
    const mgr = new McpManager();
    const client = new FakeClient();
    client.connect = async () => { client.state = 'error'; throw new Error('boom'); };
    mgr._createClient = (() => client) as never;
    mgr.addServer(IDE_MCP_SERVER_NAME, { command: 'node' });
    await assert.rejects(() => mgr.ensureConnected(IDE_MCP_SERVER_NAME));
    assert.deepEqual(mgr.getIdeConnectionStatus(), { status: 'disconnected', ideName: null });
  });
});
