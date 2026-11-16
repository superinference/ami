import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'events';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  McpManager,
  getLoggingSafeMcpBaseUrl,
  detectWindowsNpxWrapperWarning,
} from '../src/mcp/manager';
import { McpSessionExpiredError } from '../src/mcp/client';
import { fsHooks, __resetLoggerForTest } from '../src/logger';

// A configurable fake MCP client that models the surface the manager reads for
// the resource paths: state, serverCapabilities/hasCapability, listResources,
// readResource, listTools, reconnect.
class FakeClient extends EventEmitter {
  state = 'ready';
  serverInfo: unknown = { name: 'fake', version: '1' };
  instructions: string | null = null;
  uptime: number | null = 1;
  tools: unknown[] = [];
  resources: unknown[] = [];
  prompts: unknown[] = [];
  onElicitation: unknown;
  onSampling: unknown;
  caps: Record<string, unknown> = { resources: {} };
  reconnectCalls = 0;
  listResourcesImpl: () => Promise<unknown[]> = async () => this.resources;
  readResourceImpl: () => Promise<unknown> = async () => ({ contents: [{ text: 'ok' }] });
  get serverCapabilities(): Record<string, unknown> { return { ...this.caps }; }
  hasCapability(name: string): boolean { return name in this.caps; }
  async connect(): Promise<void> {}
  async connectHTTP(): Promise<void> {}
  async connectSSE(): Promise<void> {}
  async listTools(): Promise<unknown[]> { return this.tools; }
  async listResources(): Promise<unknown[]> { return this.listResourcesImpl(); }
  async listPrompts(): Promise<unknown[]> { return this.prompts; }
  async getPrompt(): Promise<unknown> { return { messages: [] }; }
  async readResource(): Promise<unknown> { return this.readResourceImpl(); }
  async callTool(): Promise<unknown> { return { ok: true }; }
  async reconnect(): Promise<void> { this.reconnectCalls++; }
  disconnect(): void {}
  setHeaders(): void {}
}

// --------------------------------------------------------------------------
// U018-G4 — getLoggingSafeMcpBaseUrl (strip token-bearing query + trailing slash)
// --------------------------------------------------------------------------
describe('getLoggingSafeMcpBaseUrl', () => {
  it('strips a token-bearing query string before logging', () => {
    assert.equal(
      getLoggingSafeMcpBaseUrl({ command: '', transport: 'sse', url: 'https://h.example.com/mcp?token=SECRET' }),
      'https://h.example.com/mcp',
    );
  });
  it('strips a trailing slash', () => {
    assert.equal(
      getLoggingSafeMcpBaseUrl({ command: '', transport: 'http', url: 'https://h.example.com/mcp/' }),
      'https://h.example.com/mcp',
    );
  });
  it('returns undefined for a stdio server (no url)', () => {
    assert.equal(getLoggingSafeMcpBaseUrl({ command: 'node', args: ['s.js'] }), undefined);
  });
  it('returns undefined for an unparseable url and for undefined config', () => {
    assert.equal(getLoggingSafeMcpBaseUrl({ command: '', url: 'not a url' }), undefined);
    assert.equal(getLoggingSafeMcpBaseUrl(undefined), undefined);
  });
});

// --------------------------------------------------------------------------
// U018-G5 — Windows npx-without-cmd-wrapper validation warning
// --------------------------------------------------------------------------
describe('detectWindowsNpxWrapperWarning', () => {
  it('warns on win32 for a bare npx stdio command', () => {
    assert.match(
      detectWindowsNpxWrapperWarning({ command: 'npx', args: ['-y', 'pkg'] }, 'win32') ?? '',
      /cmd \/c/,
    );
  });
  it('warns for npx reached via a path (\\npx and /npx)', () => {
    assert.ok(detectWindowsNpxWrapperWarning({ command: 'C:\\n\\npx' }, 'win32'));
    assert.ok(detectWindowsNpxWrapperWarning({ command: '/usr/bin/npx' }, 'win32'));
  });
  it('does not warn for a non-npx command, a url server, or http transport', () => {
    assert.equal(detectWindowsNpxWrapperWarning({ command: 'node' }, 'win32'), null);
    assert.equal(detectWindowsNpxWrapperWarning({ command: '', url: 'https://x/mcp', transport: 'http' }, 'win32'), null);
    assert.equal(detectWindowsNpxWrapperWarning({ command: 'npx', transport: 'http', url: 'https://x' }, 'win32'), null);
  });
  it('is a no-op on every non-win32 platform', () => {
    assert.equal(detectWindowsNpxWrapperWarning({ command: 'npx' }, 'linux'), null);
    assert.equal(detectWindowsNpxWrapperWarning({ command: 'npx' }, 'darwin'), null);
  });

  it('surfaces the npx warning through getConfigWarnings when loading a win32 config', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-mcp-npx-'));
    const cfg = path.join(dir, 'mcp.json');
    fs.writeFileSync(cfg, JSON.stringify({ mcpServers: { srv: { command: 'npx', args: ['-y', 'pkg'] } } }));
    try {
      const mgr = new McpManager();
      mgr._platform = 'win32';
      const events: Array<[string, string[]]> = [];
      mgr.on('config-warning', (name: string, warns: string[]) => events.push([name, warns]));
      mgr.loadFromConfig(cfg);
      const warnings = mgr.getConfigWarnings('srv');
      assert.ok(warnings.some((w) => /cmd \/c/.test(w)), `expected npx warning, got ${JSON.stringify(warnings)}`);
      assert.equal(events.length, 1);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('does not add the npx warning on a non-win32 host', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-mcp-npx-'));
    const cfg = path.join(dir, 'mcp.json');
    fs.writeFileSync(cfg, JSON.stringify({ mcpServers: { srv: { command: 'npx', args: ['-y', 'pkg'] } } }));
    try {
      const mgr = new McpManager();
      mgr._platform = 'linux';
      mgr.loadFromConfig(cfg);
      assert.deepEqual(mgr.getConfigWarnings('srv'), []);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

// --------------------------------------------------------------------------
// U018-G1 — readResource pre-flight validation
// --------------------------------------------------------------------------
describe('readResource pre-flight validation', () => {
  let mgr: McpManager;
  beforeEach(() => {
    mgr = new McpManager();
    mgr._createClient = (() => new FakeClient()) as never;
  });
  afterEach(() => mgr.disconnectAll());

  it('distinguishes "not found" (enumerates servers) from "not connected"', async () => {
    mgr.addServer('alpha', { command: 'x' });
    mgr.addServer('beta', { command: 'y' });
    // Never-registered name → not found + available-servers list.
    await assert.rejects(
      () => mgr.readResource('gamma', 'file:///x'),
      /not found\. Available servers: alpha, beta/,
    );
    // Known (pending) but no live client → not connected (no server list).
    mgr.addPendingServer('delta', { command: 'z' });
    await assert.rejects(() => mgr.readResource('delta', 'file:///x'), /is not connected/);
  });

  it('rejects when the server declares capabilities but not resources', async () => {
    const fake = new FakeClient();
    fake.caps = { tools: {} };
    mgr._createClient = (() => fake) as never;
    mgr.addServer('srv', { command: 'x' });
    await assert.rejects(() => mgr.readResource('srv', 'file:///x'), /does not support resources/);
  });

  it('reads when the server advertises resources, and stays lenient on empty caps', async () => {
    const withCap = new FakeClient();
    withCap.caps = { resources: {} };
    mgr._createClient = (() => withCap) as never;
    mgr.addServer('a', { command: 'x' });
    assert.deepEqual(await mgr.readResource('a', 'file:///x'), { contents: [{ text: 'ok' }] });

    const emptyCap = new FakeClient();
    emptyCap.caps = {};
    mgr._createClient = (() => emptyCap) as never;
    mgr.addServer('b', { command: 'y' });
    assert.deepEqual(await mgr.readResource('b', 'file:///x'), { contents: [{ text: 'ok' }] });
  });
});

// --------------------------------------------------------------------------
// U018-G2 — listResources in-call reconnect-and-retry (session expiry)
// --------------------------------------------------------------------------
describe('listResources in-call reconnect-and-retry', () => {
  it('reconnects and re-fetches once after a session-expired error', async () => {
    const mgr = new McpManager();
    const fake = new FakeClient();
    fake.resources = [{ uri: 'file:///r', name: 'r' }];
    let calls = 0;
    fake.listResourcesImpl = async () => {
      calls++;
      if (calls === 1) throw new McpSessionExpiredError();
      return fake.resources;
    };
    mgr._createClient = (() => fake) as never;
    mgr.addServer('srv', { command: 'x' });

    const out = await mgr.listResources();
    assert.equal(out.length, 1);
    assert.equal(out[0].server, 'srv');
    assert.equal(fake.reconnectCalls, 1, 'must reconnect once in-call');
    mgr.disconnectAll();
  });
});

// --------------------------------------------------------------------------
// U018-G3 — per-server fetch-failure logging (ALL errors) via safe URL
// --------------------------------------------------------------------------
describe('listResources per-server failure logging', () => {
  afterEach(() => {
    mock.restoreAll();
    __resetLoggerForTest();
  });

  it('logs a non-session failure with the token-stripped URL and isolates it', async () => {
    const lines: string[] = [];
    mock.method(fsHooks, 'appendFileSync', (_f: unknown, line: unknown) => { lines.push(String(line)); });

    const mgr = new McpManager();
    // Failing remote server whose URL carries a secret in the query string.
    const bad = new FakeClient();
    bad.listResourcesImpl = async () => { throw new Error('resources/list boom'); };
    // Healthy server contributing a resource (isolation: one failure must not sink it).
    const good = new FakeClient();
    good.resources = [{ uri: 'file:///g', name: 'g' }];
    const clients = [bad, good];
    let i = 0;
    mgr._createClient = (() => clients[i++]) as never;
    mgr.addServer('bad', { command: '', transport: 'sse', url: 'https://h.example.com/mcp?token=SECRET' });
    mgr.addServer('good', { command: 'x' });

    const out = await mgr.listResources();
    // Isolation: the healthy server's resource still comes back.
    assert.deepEqual(out.map((r) => r.server), ['good']);

    const failLine = lines.find((l) => l.includes('listResources failed for bad'));
    assert.ok(failLine, 'non-session failure must be logged');
    assert.ok(failLine!.includes('https://h.example.com/mcp'), 'safe base URL must appear');
    assert.ok(!failLine!.includes('SECRET'), 'token query string must be stripped from the log');
    mgr.disconnectAll();
  });
});
