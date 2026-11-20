import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'events';
import {
  McpManager,
  IDE_MCP_SERVER_NAME,
  MCP_IDE_SSE_PORT_ENV,
  MCP_IDE_AUTOCONNECT_ENV,
  shouldAutoConnectIde,
  isSupportedIdeTerminal,
  isEnvDefinedFalsy,
  resolveIdeMcpEndpoint,
} from '../src/mcp/manager';

// ---------------------------------------------------------------------------
// U018 (w81) — IDE MCP auto-connect waterfall (the reference-parity `addIde`,
// useIDEIntegration.ts:L32-36). The reserved `ide` server is auto-registered when
// the waterfall (config knob / `--ide` flag / supported editor terminal /
// inherited SSE port / pending extension / truthy override env) is satisfied and
// NOT hard-disabled, AND the editor exported its SSE port (AMI_SSE_PORT).
// ---------------------------------------------------------------------------

/** A minimal fake MCP client (mirrors mcp-u018-ide-connection.test.ts). */
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

const PORT_ENV: NodeJS.ProcessEnv = { [MCP_IDE_SSE_PORT_ENV]: '41234' };

describe('isSupportedIdeTerminal', () => {
  it('is true for known editor terminals (case-insensitive)', () => {
    assert.equal(isSupportedIdeTerminal({ TERM_PROGRAM: 'vscode' }), true);
    assert.equal(isSupportedIdeTerminal({ TERM_PROGRAM: 'Cursor' }), true);
    assert.equal(isSupportedIdeTerminal({ TERM_PROGRAM: 'WINDSURF' }), true);
  });
  it('is false for a non-editor or missing terminal', () => {
    assert.equal(isSupportedIdeTerminal({ TERM_PROGRAM: 'iTerm.app' }), false);
    assert.equal(isSupportedIdeTerminal({}), false);
  });
});

describe('isEnvDefinedFalsy', () => {
  it('is true only for an explicit falsy value', () => {
    for (const v of ['0', 'false', 'no', 'off', ' OFF ', 'False']) {
      assert.equal(isEnvDefinedFalsy(v), true, v);
    }
  });
  it('is false for unset / empty / truthy / other values', () => {
    for (const v of [undefined, '', '1', 'true', 'yes', 'on', 'maybe']) {
      assert.equal(isEnvDefinedFalsy(v), false, String(v));
    }
  });
});

describe('resolveIdeMcpEndpoint', () => {
  it('builds a loopback SSE url from a valid port', () => {
    assert.deepEqual(resolveIdeMcpEndpoint({ [MCP_IDE_SSE_PORT_ENV]: '41234' }), { url: 'http://127.0.0.1:41234/sse' });
  });
  it('returns null for an unset / invalid / out-of-range port', () => {
    assert.equal(resolveIdeMcpEndpoint({}), null);
    assert.equal(resolveIdeMcpEndpoint({ [MCP_IDE_SSE_PORT_ENV]: 'nope' }), null);
    assert.equal(resolveIdeMcpEndpoint({ [MCP_IDE_SSE_PORT_ENV]: '0' }), null);
    assert.equal(resolveIdeMcpEndpoint({ [MCP_IDE_SSE_PORT_ENV]: '70000' }), null);
  });
});

describe('shouldAutoConnectIde waterfall', () => {
  it('enables on the config knob, flag, pending extension, supported terminal, port, or truthy override', () => {
    assert.equal(shouldAutoConnectIde({ configEnabled: true, env: {} }), true);
    assert.equal(shouldAutoConnectIde({ flag: true, env: {} }), true);
    assert.equal(shouldAutoConnectIde({ ideToInstall: true, env: {} }), true);
    assert.equal(shouldAutoConnectIde({ env: { TERM_PROGRAM: 'vscode' } }), true);
    assert.equal(shouldAutoConnectIde({ env: { [MCP_IDE_SSE_PORT_ENV]: '41234' } }), true);
    assert.equal(shouldAutoConnectIde({ env: { [MCP_IDE_AUTOCONNECT_ENV]: '1' } }), true);
  });
  it('is disabled when no input is satisfied', () => {
    assert.equal(shouldAutoConnectIde({ env: {} }), false);
  });
  it('a falsy override env is a HARD opt-out that wins over every enabling input', () => {
    assert.equal(
      shouldAutoConnectIde({ configEnabled: true, flag: true, ideToInstall: true, env: { TERM_PROGRAM: 'vscode', [MCP_IDE_SSE_PORT_ENV]: '41234', [MCP_IDE_AUTOCONNECT_ENV]: 'false' } }),
      false,
    );
  });
});

describe('McpManager.autoConnectIde', () => {
  afterEach(() => { delete process.env[MCP_IDE_SSE_PORT_ENV]; delete process.env[MCP_IDE_AUTOCONNECT_ENV]; delete process.env.TERM_PROGRAM; });

  it('registers the reserved ide server when gated and the port resolves', () => {
    const mgr = new McpManager();
    const registered = mgr.autoConnectIde({ configEnabled: true, env: { ...PORT_ENV, TERM_PROGRAM: 'vscode' } });
    assert.equal(registered, true);
    const cfg = mgr.getServerConfig(IDE_MCP_SERVER_NAME);
    assert.ok(cfg);
    assert.equal(cfg!.transport, 'sse-ide');
    assert.equal(cfg!.url, 'http://127.0.0.1:41234/sse');
    assert.equal(cfg!.ideName, 'vscode');
    // The IDE badge now reflects the registered (not-yet-connected) editor server.
    assert.deepEqual(mgr.getIdeConnectionStatus(), { status: 'disconnected', ideName: 'vscode' });
  });

  it('registers on an inherited port alone (no explicit opts) and is a no-op with an empty env', () => {
    const mgr = new McpManager();
    assert.equal(mgr.autoConnectIde({ env: PORT_ENV }), true); // the port is itself an enabling input
    const mgr2 = new McpManager();
    assert.equal(mgr2.autoConnectIde({ env: {} }), false);
    assert.equal(mgr2.getServerConfig(IDE_MCP_SERVER_NAME), undefined);
  });

  it('is a no-op when gated but no editor port is present', () => {
    const mgr = new McpManager();
    assert.equal(mgr.autoConnectIde({ configEnabled: true, env: {} }), false);
    assert.equal(mgr.getServerConfig(IDE_MCP_SERVER_NAME), undefined);
  });

  it('is a no-op when an ide server is already registered', () => {
    const mgr = new McpManager();
    mgr.addServer(IDE_MCP_SERVER_NAME, { command: '', transport: 'sse-ide', url: 'https://preexisting', ideName: 'Existing' });
    assert.equal(mgr.autoConnectIde({ configEnabled: true, env: PORT_ENV }), false);
    // The pre-existing config is untouched.
    assert.equal(mgr.getServerConfig(IDE_MCP_SERVER_NAME)!.url, 'https://preexisting');
  });

  it('end-to-end: an auto-registered ide server connects on the normal connect pass', async () => {
    const mgr = new McpManager();
    const client = new FakeClient();
    mgr._createClient = (() => client) as never;
    assert.equal(mgr.autoConnectIde({ configEnabled: true, env: { ...PORT_ENV, TERM_PROGRAM: 'cursor' } }), true);
    await mgr.connectAll();
    assert.deepEqual(mgr.getIdeConnectionStatus(), { status: 'connected', ideName: 'cursor' });
  });

  it('reads process.env + the config knob when no explicit env/opts are given', () => {
    process.env[MCP_IDE_SSE_PORT_ENV] = '41234';
    const mgr = new McpManager();
    assert.equal(mgr.autoConnectIde({ configEnabled: true }), true);
    assert.equal(mgr.getServerConfig(IDE_MCP_SERVER_NAME)!.url, 'http://127.0.0.1:41234/sse');
  });
});
