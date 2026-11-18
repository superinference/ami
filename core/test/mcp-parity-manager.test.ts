import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'events';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  McpManager,
  wildcardToRegExp,
  isAuthRequiredError,
  runBoundedPool,
  getMcpConnectionBatchSize,
  getMcpRemoteConnectionBatchSize,
  getConfiguredOAuthCallbackPort,
  validateMcpServerConfigShape,
  normalizeMcpServerEntry,
  normalizeMcpToolName,
  findMcpConfigPaths,
} from '../src/mcp/manager';
import { McpSessionExpiredError } from '../src/mcp/client';

// A rich fake MCP client driving the manager deterministically without a
// subprocess or socket. Exposes the getters/methods McpManager reads.
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
  connectImpl: () => Promise<void> = async () => {};
  toolImpl: (n: string, a: unknown) => Promise<unknown> = async () => ({ ok: true });
  reconnectCalls = 0;
  async connect(): Promise<void> { return this.connectImpl(); }
  async connectHTTP(): Promise<void> { return this.connectImpl(); }
  async connectSSE(): Promise<void> { return this.connectImpl(); }
  async listTools(): Promise<unknown[]> { return this.tools; }
  async listResources(): Promise<unknown[]> { return this.resources; }
  async listPrompts(): Promise<unknown[]> { return this.prompts; }
  async getPrompt(): Promise<unknown> { return { messages: [] }; }
  async readResource(): Promise<unknown> { return {}; }
  async callTool(n: string, a: unknown): Promise<unknown> { return this.toolImpl(n, a); }
  async reconnect(): Promise<void> { this.reconnectCalls++; }
  disconnect(): void {}
  setHeaders(): void {}
  supportsChannelPermissionRelay(): boolean { return false; }
}

// --------------------------------------------------------------------------
// Theme: rich allow/deny policy — command-array + URL-wildcard + name-wildcard
// --------------------------------------------------------------------------
describe('MCP policy matching (command-array + URL wildcard)', () => {
  it('wildcardToRegExp is anchored and treats * as any-run', () => {
    const re = wildcardToRegExp('https://*.example.com/*');
    assert.equal(re.test('https://a.example.com/mcp'), true);
    assert.equal(re.test('https://a.b.example.com/x/y'), true);
    assert.equal(re.test('https://evil.com/example.com/'), false); // anchored
  });

  it('denies a stdio server by its full command array regardless of name', () => {
    const mgr = new McpManager();
    mgr.setDeniedServerList([{ serverCommand: ['npx', '-y', 'evil-pkg'] }]);
    assert.equal(mgr.isServerDenied('innocent-name', { command: 'npx', args: ['-y', 'evil-pkg'] }), true);
    assert.equal(mgr.isServerDenied('innocent-name', { command: 'npx', args: ['-y', 'good-pkg'] }), false);
    assert.throws(
      () => mgr.addServer('innocent-name', { command: 'npx', args: ['-y', 'evil-pkg'] }),
      /blocked by the denied-servers list/,
    );
  });

  it('denies a whole subdomain family via a URL wildcard', () => {
    const mgr = new McpManager();
    mgr.setDeniedServerList(['https://*.evil.com/*']);
    assert.equal(mgr.isServerDenied('a', { url: 'https://api.evil.com/mcp' }), true);
    assert.equal(mgr.isServerDenied('b', { url: 'https://evil.com.good.org/mcp' }), false);
  });

  it('allowlist can match by command array (not just name)', () => {
    const mgr = new McpManager();
    mgr.setAllowedServers([{ serverCommand: ['node', 'srv.js'] }]);
    assert.equal(mgr.isServerAllowed('whatever', { command: 'node', args: ['srv.js'] }), true);
    assert.equal(mgr.isServerAllowed('whatever', { command: 'node', args: ['other.js'] }), false);
  });

  it('plain-string name/exact-URL entries still work (back-compat) and denylist stays case-insensitive', () => {
    const mgr = new McpManager();
    mgr.setDeniedServerList(['Evil-Server', 'https://e.example.com/mcp']);
    assert.equal(mgr.isServerDenied('evil-server'), true);
    assert.equal(mgr.isServerDenied('x', { url: 'https://E.example.com/mcp/' }), true);
  });
});

// --------------------------------------------------------------------------
// Theme: needs-auth state
// --------------------------------------------------------------------------
describe('needs-auth connection state', () => {
  it('classifies OAuth-consent/401 errors as auth-required', () => {
    assert.equal(isAuthRequiredError(new Error('OAuth consent requires an interactive frontend')), true);
    assert.equal(isAuthRequiredError(new Error('MCP HTTP 401 Unauthorized')), true);
    assert.equal(isAuthRequiredError(new Error('connect ECONNREFUSED')), false);
  });

  it('ensureConnected sets needs-auth (not error) when OAuth consent is unavailable', async () => {
    const mgr = new McpManager();
    mgr._createClient = (() => {
      const c = new FakeClient();
      c.connectImpl = async () => { throw new Error('OAuth consent requires an interactive frontend'); };
      return c;
    }) as never;
    mgr.addPendingServer('oauth-srv', { command: '', transport: 'http', url: 'https://srv.example.com/mcp', oauth: {} });
    await assert.rejects(() => mgr.ensureConnected('oauth-srv'));
    assert.equal(mgr.getServerState('oauth-srv'), 'needs-auth');
  });
});

// --------------------------------------------------------------------------
// Theme: bounded-concurrency connect
// --------------------------------------------------------------------------
describe('bounded-concurrency pool', () => {
  it('never exceeds the concurrency ceiling', async () => {
    const items = Array.from({ length: 20 }, (_, i) => i);
    let inFlight = 0;
    let peak = 0;
    await runBoundedPool(items, 4, async () => {
      inFlight++; peak = Math.max(peak, inFlight);
      await new Promise((r) => setTimeout(r, 2));
      inFlight--;
    });
    assert.ok(peak <= 4, `peak ${peak} exceeded limit 4`);
  });

  it('batch-size knobs read env overrides', () => {
    const prevL = process.env.SUPERINFERENCE_MCP_SERVER_CONNECTION_BATCH_SIZE;
    const prevR = process.env.SUPERINFERENCE_MCP_REMOTE_SERVER_CONNECTION_BATCH_SIZE;
    try {
      delete process.env.SUPERINFERENCE_MCP_SERVER_CONNECTION_BATCH_SIZE;
      delete process.env.SUPERINFERENCE_MCP_REMOTE_SERVER_CONNECTION_BATCH_SIZE;
      assert.equal(getMcpConnectionBatchSize(), 3);
      assert.equal(getMcpRemoteConnectionBatchSize(), 20);
      process.env.SUPERINFERENCE_MCP_SERVER_CONNECTION_BATCH_SIZE = '7';
      process.env.SUPERINFERENCE_MCP_REMOTE_SERVER_CONNECTION_BATCH_SIZE = '42';
      assert.equal(getMcpConnectionBatchSize(), 7);
      assert.equal(getMcpRemoteConnectionBatchSize(), 42);
    } finally {
      if (prevL === undefined) delete process.env.SUPERINFERENCE_MCP_SERVER_CONNECTION_BATCH_SIZE; else process.env.SUPERINFERENCE_MCP_SERVER_CONNECTION_BATCH_SIZE = prevL;
      if (prevR === undefined) delete process.env.SUPERINFERENCE_MCP_REMOTE_SERVER_CONNECTION_BATCH_SIZE; else process.env.SUPERINFERENCE_MCP_REMOTE_SERVER_CONNECTION_BATCH_SIZE = prevR;
    }
  });
});

// --------------------------------------------------------------------------
// Theme: transparent single-retry of a tool call after session expiry (bug)
//   RED on revert: without the retry, callTool rethrows McpSessionExpiredError.
// --------------------------------------------------------------------------
describe('callTool session-expiry retry', () => {
  it('reconnects and retries once after a session-expired error', async () => {
    const mgr = new McpManager();
    const fake = new FakeClient();
    let calls = 0;
    fake.toolImpl = async () => {
      calls++;
      if (calls === 1) throw new McpSessionExpiredError();
      return { ok: calls };
    };
    mgr._createClient = (() => fake) as never;
    mgr.addServer('srv', { command: 'x' });
    const result = await mgr.callTool('srv:dothing', {});
    assert.deepEqual(result, { ok: 2 });
    assert.equal(fake.reconnectCalls, 1);
  });
});

// --------------------------------------------------------------------------
// Theme: server-not-found enumerates available servers
// --------------------------------------------------------------------------
describe('server-not-found error lists available servers', () => {
  let mgr: McpManager;
  beforeEach(() => {
    mgr = new McpManager();
    mgr._createClient = (() => new FakeClient()) as never;
    mgr.addServer('alpha', { command: 'x' });
    mgr.addServer('beta', { command: 'y' });
  });

  it('readResource on an unknown server lists the valid ones', async () => {
    await assert.rejects(
      () => mgr.readResource('gamma', 'file:///x'),
      /not found\. Available servers: alpha, beta/,
    );
  });

  it('listResources with an unknown filter throws enumerating servers', async () => {
    await assert.rejects(
      () => mgr.listResources('gamma'),
      /not found\. Available servers: alpha, beta/,
    );
  });
});

// --------------------------------------------------------------------------
// Theme: status — sort, promptCount, isAuthenticated
// --------------------------------------------------------------------------
describe('server status enhancements', () => {
  it('lists servers alphabetically with promptCount and isAuthenticated', () => {
    const mgr = new McpManager();
    mgr._createClient = (() => {
      const c = new FakeClient();
      c.prompts = [{ name: 'p1' }, { name: 'p2' }];
      c.tools = [{ name: 't1' }];
      return c;
    }) as never;
    mgr.addServer('zeta', { command: 'x', transport: 'http', url: 'https://z/mcp' });
    mgr.addServer('alpha', { command: 'y', transport: 'http', url: 'https://a/mcp' });
    const names = mgr.listServers().map((s) => s.name);
    assert.deepEqual(names, ['alpha', 'zeta']);
    const alpha = mgr.getServerStatus('alpha')!;
    assert.equal(alpha.promptCount, 2);
    assert.equal(alpha.isAuthenticated, true); // remote + ready + has tools
  });

  it('isAuthenticated is undefined for stdio servers', () => {
    const mgr = new McpManager();
    mgr._createClient = (() => { const c = new FakeClient(); c.tools = [{ name: 't' }]; return c; }) as never;
    mgr.addServer('local', { command: 'x' }); // stdio (no transport)
    assert.equal(mgr.getServerStatus('local')!.isAuthenticated, undefined);
  });
});

// --------------------------------------------------------------------------
// Theme: loadFromConfig robustness — type mapping, per-server isolation, schema
// --------------------------------------------------------------------------
describe('loadFromConfig robustness', () => {
  const dirs: string[] = [];
  afterEach(() => { for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true }); });

  const writeCfg = (obj: unknown): string => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-mcp-parity-'));
    dirs.push(dir);
    const p = path.join(dir, 'mcp.json');
    fs.writeFileSync(p, JSON.stringify(obj));
    return p;
  };

  it('maps a Claude-style `type` transport onto `transport`', () => {
    const mgr = new McpManager();
    const p = writeCfg({ mcpServers: { web: { type: 'http', url: 'https://w/mcp' } } });
    mgr.loadFromConfig(p);
    assert.equal(mgr.getServerConfig('web')?.transport, 'http');
  });

  it('skips one malformed entry but loads the rest (per-server isolation)', () => {
    const mgr = new McpManager();
    const p = writeCfg({ mcpServers: {
      good: { command: 'echo' },
      broken: { command: 'echo', args: 'not-an-array' }, // invalid args
      alsogood: { command: 'echo' },
    } });
    mgr.loadFromConfig(p);
    const names = mgr.listAllServers().map((s) => s.name).sort();
    assert.deepEqual(names, ['alsogood', 'good']);
    assert.match(mgr.getConfigValidationErrors().broken, /args.*must be an array/);
  });

  it('validateMcpServerConfigShape flags wrong-typed fields and bad transports', () => {
    assert.deepEqual(validateMcpServerConfigShape({ command: 'echo' }), []);
    assert.ok(validateMcpServerConfigShape({ env: ['x'] }).length > 0);
    // ws/wss + the Claude-parity IDE/sdk discriminants are supported transports; a
    // genuinely unknown transport is flagged.
    assert.deepEqual(validateMcpServerConfigShape({ transport: 'ws', url: 'ws://x' }), []);
    assert.deepEqual(validateMcpServerConfigShape({ transport: 'sdk', url: 'x' }), []);
    assert.ok(validateMcpServerConfigShape({ transport: 'bogus', url: 'x' }).some((m) => /unsupported transport/.test(m)));
    assert.deepEqual(validateMcpServerConfigShape(null), ['server config must be a JSON object']);
  });

  it('normalizeMcpServerEntry drops `type` after mapping', () => {
    const out = normalizeMcpServerEntry({ type: 'sse', url: 'https://x/mcp' }) as Record<string, unknown>;
    assert.equal(out.transport, 'sse');
    assert.equal('type' in out, false);
  });
});

// --------------------------------------------------------------------------
// Theme: OAuth callback port env knob
// --------------------------------------------------------------------------
describe('MCP_OAUTH_CALLBACK_PORT env knob', () => {
  it('reads a positive-int port, else undefined', () => {
    const prev = process.env.MCP_OAUTH_CALLBACK_PORT;
    try {
      delete process.env.MCP_OAUTH_CALLBACK_PORT;
      assert.equal(getConfiguredOAuthCallbackPort(), undefined);
      process.env.MCP_OAUTH_CALLBACK_PORT = '45999';
      assert.equal(getConfiguredOAuthCallbackPort(), 45999);
      process.env.MCP_OAUTH_CALLBACK_PORT = 'nope';
      assert.equal(getConfiguredOAuthCallbackPort(), undefined);
    } finally {
      if (prev === undefined) delete process.env.MCP_OAUTH_CALLBACK_PORT; else process.env.MCP_OAUTH_CALLBACK_PORT = prev;
    }
  });
});

// --------------------------------------------------------------------------
// Theme: findMcpConfigPaths ascends parent directories for .mcp.json
// --------------------------------------------------------------------------
describe('findMcpConfigPaths parent traversal', () => {
  it('finds an ancestor .mcp.json when launched from a nested subdirectory', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'si-mcp-tree-'));
    try {
      const nested = path.join(root, 'a', 'b', 'c');
      fs.mkdirSync(nested, { recursive: true });
      fs.writeFileSync(path.join(root, '.mcp.json'), '{}');
      fs.writeFileSync(path.join(nested, '.mcp.json'), '{}');
      const paths = findMcpConfigPaths(nested);
      const nestedIdx = paths.indexOf(path.join(nested, '.mcp.json'));
      const rootIdx = paths.indexOf(path.join(root, '.mcp.json'));
      assert.ok(nestedIdx >= 0 && rootIdx >= 0, 'both configs found');
      assert.ok(nestedIdx < rootIdx, 'nearest config precedes ancestor (first-wins)');
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

// --------------------------------------------------------------------------
// Theme: unicode-sanitize prompt metadata
// --------------------------------------------------------------------------
describe('prompt metadata unicode sanitization', () => {
  it('strips invisible Unicode from prompt names/descriptions in listPrompts', async () => {
    const mgr = new McpManager();
    mgr._createClient = (() => {
      const c = new FakeClient();
      // U+200B zero-width space + U+E0001 Tag char smuggled into metadata.
      c.prompts = [{ name: 'safe​name', description: 'desc\u{E0001}hidden' }];
      return c;
    }) as never;
    mgr.addServer('srv', { command: 'x' });
    const prompts = await mgr.listPrompts();
    assert.equal(prompts[0].name, 'safename');
    assert.equal(prompts[0].description, 'deschidden');
  });
});

// --------------------------------------------------------------------------
// Theme: annotation classifier normalizes camelCase/kebab
// --------------------------------------------------------------------------
describe('tool annotation classification normalization', () => {
  it('normalizeMcpToolName reduces camelCase and kebab-case to snake_case', () => {
    assert.equal(normalizeMcpToolName('searchJiraIssues'), 'search_jira_issues');
    assert.equal(normalizeMcpToolName('brave-web-search'), 'brave_web_search');
  });

  it('classifies a non-leading read/search verb as read-only', () => {
    const mgr = new McpManager();
    // No schema annotations -> falls to heuristic. camelCase with trailing verb.
    assert.equal(mgr.getToolAnnotations('exa:webSearchExa').readOnlyHint, true);
    assert.equal(mgr.getToolAnnotations('notion:asanaTypeaheadSearch').readOnlyHint, true);
  });
});

// --------------------------------------------------------------------------
// Theme: auto-reconnect on remote transport close (guarded against intentional)
// --------------------------------------------------------------------------
describe('auto-reconnect on remote transport close', () => {
  const timers = (m: McpManager) => (m as unknown as { reconnectTimers: Map<string, unknown> }).reconnectTimers;

  it('schedules a reconnect when a remote (http) client closes unexpectedly', () => {
    const mgr = new McpManager();
    const fake = new FakeClient();
    mgr._createClient = (() => fake) as never;
    mgr.addServer('remote', { command: '', transport: 'http', url: 'https://r/mcp' });
    fake.emit('close', 1000, 5);
    assert.equal(timers(mgr).has('remote'), true);
    mgr.disconnectAll();
  });

  it('does NOT reconnect a stdio server on close', () => {
    const mgr = new McpManager();
    const fake = new FakeClient();
    mgr._createClient = (() => fake) as never;
    mgr.addServer('local', { command: 'x' }); // stdio
    fake.emit('close', 0, 5);
    assert.equal(timers(mgr).has('local'), false);
  });

  it('does NOT reconnect after an intentional stop', async () => {
    const mgr = new McpManager();
    const fake = new FakeClient();
    mgr._createClient = (() => fake) as never;
    mgr.addServer('remote', { command: '', transport: 'http', url: 'https://r/mcp' });
    await mgr.stopServer('remote');
    fake.emit('close', 1000, 5); // the stale client should not revive a stopped server
    assert.equal(timers(mgr).has('remote'), false);
  });
});
