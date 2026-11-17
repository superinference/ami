import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'events';
import {
  McpManager,
  IDE_MCP_SERVER_NAME,
  IDE_CONNECTION_TIMEOUT_MS,
  ideTransportForUrl,
  type DetectedIdeInfo,
} from '../src/mcp/manager';

// ---------------------------------------------------------------------------
// U018 (w88) — interactive IDE connect/disconnect state machine + current-IDE
// reconciliation (Claude-parity ide.tsx `IDECommandFlow` + `findCurrentIDE`):
//   * ideTransportForUrl maps a URL scheme → the IDE transport discriminant.
//   * McpManager.findCurrentIde reconciles the live `ide` config to a detected IDE.
//   * McpManager.connectIde registers + connects the `ide` server and resolves
//     with a connected/failed/timeout outcome + the display message.
//   * McpManager.disconnectIde tears the `ide` server down (no auto-reconnect).
// ---------------------------------------------------------------------------

/** Minimal fake MCP client driveable without a real server. `connectMode`
 *  selects whether the (SSE/WS) connect resolves, rejects, or hangs. */
class FakeClient extends EventEmitter {
  state = 'idle';
  serverInfo: unknown = { name: 'ide', version: '1' };
  instructions: string | null = null;
  uptime: number | null = 1;
  tools: Array<{ name: string; description?: string }> = [];
  onElicitation: unknown;
  onSampling: unknown;
  constructor(private connectMode: 'ok' | 'fail' | 'hang' = 'ok') { super(); }
  private async drive(): Promise<void> {
    this.state = 'connecting';
    if (this.connectMode === 'fail') throw new Error('handshake refused by editor');
    if (this.connectMode === 'hang') return new Promise<void>(() => { /* never resolves */ });
    this.state = 'ready';
  }
  async connect(): Promise<void> { return this.drive(); }
  async connectHTTP(): Promise<void> { return this.drive(); }
  async connectSSE(): Promise<void> { return this.drive(); }
  async connectWebSocket(): Promise<void> { return this.drive(); }
  async listTools(): Promise<Array<{ name: string; description?: string }>> { return this.tools; }
  async listResources(): Promise<unknown[]> { return []; }
  async listPrompts(): Promise<unknown[]> { return []; }
  async callTool(): Promise<unknown> { return { ok: true }; }
  disconnect(): void { this.state = 'stopped'; }
  setHeaders(): void {}
  async revokeAuth(): Promise<void> {}
}

const vscode: DetectedIdeInfo = { name: 'VS Code', url: 'http://127.0.0.1:4111/sse' };
const jetbrains: DetectedIdeInfo = { name: 'JetBrains', url: 'ws://127.0.0.1:5000' };

describe('ideTransportForUrl', () => {
  it('maps ws:// and wss:// to ws-ide', () => {
    assert.equal(ideTransportForUrl('ws://127.0.0.1:5000'), 'ws-ide');
    assert.equal(ideTransportForUrl('wss://editor.example/mcp'), 'ws-ide');
  });
  it('maps http(s) / everything else to sse-ide', () => {
    assert.equal(ideTransportForUrl('http://127.0.0.1:4111/sse'), 'sse-ide');
    assert.equal(ideTransportForUrl('https://editor.example/sse'), 'sse-ide');
  });
});

describe('McpManager.findCurrentIde', () => {
  it('returns null when no ide server is wired', () => {
    const mgr = new McpManager();
    assert.equal(mgr.findCurrentIde([vscode, jetbrains]), null);
  });
  it('returns null when the ide server is not an IDE transport', () => {
    const mgr = new McpManager();
    mgr.addServer(IDE_MCP_SERVER_NAME, { command: '', transport: 'sse', url: vscode.url });
    assert.equal(mgr.findCurrentIde([vscode]), null);
  });
  it('matches the wired ide config to the detected IDE by url identity', () => {
    const mgr = new McpManager();
    mgr.addServer(IDE_MCP_SERVER_NAME, { command: '', transport: 'sse-ide', url: vscode.url, ideName: 'VS Code' });
    assert.deepEqual(mgr.findCurrentIde([jetbrains, vscode]), vscode);
  });
  it('returns null when no detected IDE url matches the wired config', () => {
    const mgr = new McpManager();
    mgr.addServer(IDE_MCP_SERVER_NAME, { command: '', transport: 'ws-ide', url: 'ws://127.0.0.1:9999', ideName: 'JetBrains' });
    assert.equal(mgr.findCurrentIde([vscode, jetbrains]), null);
  });
});

describe('McpManager.connectIde', () => {
  it('registers the ide server over the url-implied transport and reports connected', async () => {
    const mgr = new McpManager();
    let lastOpts: Record<string, unknown> | undefined;
    mgr._createClient = ((opts: Record<string, unknown>) => { lastOpts = opts; return new FakeClient('ok'); }) as never;

    const res = await mgr.connectIde(jetbrains);
    assert.deepEqual(res, { outcome: 'connected', message: 'Connected to JetBrains.' });
    // The ws:// url selected the ws-ide transport and surfaced the editor badge.
    assert.deepEqual(mgr.getIdeConnectionStatus(), { status: 'connected', ideName: 'JetBrains' });
    assert.equal(lastOpts?.headers, undefined);
  });

  it('threads an authToken into a bearer Authorization header', async () => {
    const mgr = new McpManager();
    let lastOpts: Record<string, unknown> | undefined;
    mgr._createClient = ((opts: Record<string, unknown>) => { lastOpts = opts; return new FakeClient('ok'); }) as never;

    const res = await mgr.connectIde({ name: 'VS Code', url: vscode.url, authToken: 'tok-123' });
    assert.equal(res.outcome, 'connected');
    assert.deepEqual(lastOpts?.headers, { Authorization: 'Bearer tok-123' });
  });

  it('reports failed with the editor name when the handshake rejects', async () => {
    const mgr = new McpManager();
    mgr._createClient = (() => new FakeClient('fail')) as never;
    const res = await mgr.connectIde(vscode);
    assert.deepEqual(res, { outcome: 'failed', message: 'Failed to connect to VS Code.' });
  });

  it('reports timeout when no terminal state is reached before the ceiling', async () => {
    const mgr = new McpManager();
    mgr._createClient = (() => new FakeClient('hang')) as never;
    const res = await mgr.connectIde(vscode, { timeoutMs: 20 });
    assert.deepEqual(res, { outcome: 'timeout', message: 'Connection to VS Code timed out.' });
    // The hung connect leaves the server in a non-ready (pending) badge state.
    assert.equal(mgr.getIdeConnectionStatus().status, 'pending');
    mgr.disconnectIde();
  });

  it('re-points the ide server to a new editor, replacing the prior one', async () => {
    const mgr = new McpManager();
    mgr._createClient = (() => new FakeClient('ok')) as never;
    await mgr.connectIde(vscode);
    assert.equal(mgr.getIdeConnectionStatus().ideName, 'VS Code');
    // A second connect re-points the single `ide` slot (addServer would otherwise
    // throw "already registered" — proving the prior server was torn down first).
    const res = await mgr.connectIde(jetbrains);
    assert.equal(res.outcome, 'connected');
    assert.deepEqual(mgr.findCurrentIde([vscode, jetbrains]), jetbrains);
  });

  it('defaults to the Claude-parity 35s connection ceiling', () => {
    assert.equal(IDE_CONNECTION_TIMEOUT_MS, 35000);
  });
});

describe('McpManager.disconnectIde', () => {
  it('reports "No IDE selected." when no editor is wired', () => {
    const mgr = new McpManager();
    assert.deepEqual(mgr.disconnectIde(), { disconnected: false, message: 'No IDE selected.' });
  });

  it('tears down the live ide server and reports the editor name', async () => {
    const mgr = new McpManager();
    mgr._createClient = (() => new FakeClient('ok')) as never;
    await mgr.connectIde(vscode);
    assert.equal(mgr.getIdeConnectionStatus().status, 'connected');

    const res = mgr.disconnectIde();
    assert.deepEqual(res, { disconnected: true, message: 'Disconnected from VS Code.' });
    // The server is forgotten — the badge reports "no editor" (null) again.
    assert.deepEqual(mgr.getIdeConnectionStatus(), { status: null, ideName: null });
    assert.equal(mgr.findCurrentIde([vscode]), null);
  });

  it('falls back to a generic "IDE" name when the config omits ideName', () => {
    const mgr = new McpManager();
    mgr.addServer(IDE_MCP_SERVER_NAME, { command: '', transport: 'sse-ide', url: vscode.url });
    assert.deepEqual(mgr.disconnectIde(), { disconnected: true, message: 'Disconnected from IDE.' });
  });
});
