import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'events';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  McpManager,
  MAX_MCP_RECONNECT_ATTEMPTS,
  type McpConfigDiagnostic,
} from '../src/mcp/manager';

// ===========================================================================
// U018 (w66) — MCP manager parity additions:
//  - unified severity-tagged, per-entry config diagnostics (McpParsingWarnings)
//  - needsAuth per-server status field (MCPAgentServerMenu)
//  - reconnect-attempt progress in status (MCPListPanel)
//  - workspace-trust boundary for config-supplied headersHelper (headersHelper.ts)
//  - actionable errors for malformed / unreadable .mcp.json (config.ts)
// ===========================================================================

// Minimal fake MCP client (mirrors the one used by mcp-headers-helper.test.ts).
class FakeClient extends EventEmitter {
  onElicitation: unknown;
  onSampling: unknown;
  applied: Record<string, string> | null = null;
  state = 'ready';
  serverInfo = { name: 'fake', version: '1' };
  tools: unknown[] = [];
  resources: unknown[] = [];
  prompts: unknown[] = [];
  uptime: number | null = 0;
  throwOnConnect: Error | null = null;
  setHeaders(h: Record<string, string>): void { this.applied = h; }
  async connect(): Promise<void> { if (this.throwOnConnect) throw this.throwOnConnect; }
  async connectHTTP(): Promise<void> { if (this.throwOnConnect) throw this.throwOnConnect; }
  async connectSSE(): Promise<void> { if (this.throwOnConnect) throw this.throwOnConnect; }
  async listTools(): Promise<unknown[]> { return []; }
  async reconnect(): Promise<void> {}
  async revokeAuth(): Promise<void> {}
  disconnect(): void {}
}

const dirs: string[] = [];
function writeConfig(contents: string): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-u018-'));
  dirs.push(d);
  const p = path.join(d, 'mcp.json');
  fs.writeFileSync(p, contents);
  return p;
}
afterEach(() => {
  for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true });
});

describe('getConfigDiagnostics — unified severity-tagged per-entry diagnostics', () => {
  it('surfaces fatal parse failures, fatal per-server validation errors, and warnings with attribution', () => {
    const mgr = new McpManager();
    // 1) malformed file → fatal parse diagnostic (whole file, no serverName).
    const badFile = writeConfig('{ "mcpServers": { "zzz": { "command": "x" }, ');
    mgr.loadFromConfig(badFile);
    // 2) valid file with: an invalid-shape entry (fatal, skipped) + a server that
    //    references an unset env var (warning, still loaded).
    delete process.env.__U018_UNSET;
    const goodFile = writeConfig(JSON.stringify({
      mcpServers: {
        broken: { command: 123 },                              // wrong-typed command
        envref: { command: 'run', args: ['${__U018_UNSET}'] }, // unset env var warning
      },
    }));
    mgr.loadFromConfig(goodFile);

    const diags = mgr.getConfigDiagnostics();
    // Every severity is one of the two parity values.
    for (const d of diags) assert.ok(d.severity === 'fatal' || d.severity === 'warning');

    // Fatal diagnostics are listed before warnings.
    const firstWarningIdx = diags.findIndex((d) => d.severity === 'warning');
    const lastFatalIdx = diags.map((d) => d.severity).lastIndexOf('fatal');
    assert.ok(lastFatalIdx < firstWarningIdx, 'fatal diagnostics must precede warnings');

    const parse = diags.find((d) => d.severity === 'fatal' && d.path === badFile && !d.serverName);
    assert.ok(parse, 'parse failure must be a fatal file-level diagnostic');

    const validation = diags.find((d) => d.severity === 'fatal' && d.serverName === 'broken');
    assert.ok(validation, 'invalid-shape entry must be a fatal per-server diagnostic');
    assert.equal(validation!.jsonPath, 'mcpServers.broken');
    assert.equal(validation!.path, goodFile);

    const warn = diags.find((d) => d.severity === 'warning' && d.serverName === 'envref');
    assert.ok(warn, 'unset-env server must be a per-server warning');
    assert.equal(warn!.jsonPath, 'mcpServers.envref');
    assert.ok(/__U018_UNSET/.test(warn!.message));
  });

  it('returns an empty list when there are no config problems', () => {
    const mgr = new McpManager();
    const p = writeConfig(JSON.stringify({ mcpServers: { ok: { command: 'run' } } }));
    mgr.loadFromConfig(p);
    const diags: McpConfigDiagnostic[] = mgr.getConfigDiagnostics();
    assert.deepEqual(diags, []);
  });
});

describe('needsAuth per-server status field', () => {
  it('sets needsAuth on a remote server parked in needs-auth after an auth-failed connect', async () => {
    const mgr = new McpManager();
    const fake = new FakeClient();
    fake.throwOnConnect = new Error('401 unauthorized');
    mgr._createClient = (() => fake) as never;
    mgr.addPendingServer('remote', { command: '', transport: 'http', url: 'https://api.example.com' });
    await assert.rejects(() => mgr.ensureConnected('remote'));

    const status = mgr.listAllServers().find((s) => s.name === 'remote');
    assert.ok(status, 'server must be listed');
    assert.equal(status!.state, 'needs-auth');
    assert.equal(status!.needsAuth, true);
  });

  it('does not set needsAuth for a healthy server', async () => {
    const mgr = new McpManager();
    const fake = new FakeClient();
    mgr._createClient = (() => fake) as never;
    mgr.addServer('ok', { command: 'run' });
    await mgr.connectServer('ok');
    const status = mgr.getServerStatus('ok');
    assert.equal(status?.needsAuth, undefined);
  });
});

describe('reconnect-attempt progress in status', () => {
  it('surfaces reconnectAttempt / maxReconnectAttempts while a reconnect is scheduled', async () => {
    const mgr = new McpManager();
    const fake = new FakeClient();
    mgr._createClient = (() => fake) as never;
    mgr.addServer('r', { command: '', transport: 'http', url: 'https://api.example.com' });
    await mgr.connectServer('r');
    // A remote transport close (not intentional) schedules a reconnect.
    fake.emit('close', 1, 100);
    const status = mgr.getServerStatus('r');
    assert.equal(status?.reconnectAttempt, 1, 'first attempt is reported as 1');
    assert.equal(status?.maxReconnectAttempts, MAX_MCP_RECONNECT_ATTEMPTS);
    mgr.removeServer('r'); // clear the pending timer
  });

  it('omits reconnect fields for a healthy server', async () => {
    const mgr = new McpManager();
    const fake = new FakeClient();
    mgr._createClient = (() => fake) as never;
    mgr.addServer('h', { command: 'run' });
    await mgr.connectServer('h');
    const status = mgr.getServerStatus('h');
    assert.equal(status?.reconnectAttempt, undefined);
    assert.equal(status?.maxReconnectAttempts, undefined);
  });
});

describe('workspace-trust boundary for headersHelper', () => {
  it('isHeadersHelperTrusted: trusted for non-project scopes, gated for project scope', () => {
    const mgr = new McpManager();
    const p = writeConfig(JSON.stringify({ mcpServers: { user: { command: 'run' } } }));
    mgr.loadFromConfig(p); // user scope
    assert.equal(mgr.isHeadersHelperTrusted('user'), true);

    // A project-scoped, un-approved server is NOT trusted to run a helper.
    const proj = writeConfig(JSON.stringify({ mcpServers: { proj: { command: 'run' } } }));
    mgr.loadFromConfig(proj, { projectScoped: true });
    assert.equal(mgr.isHeadersHelperTrusted('proj'), false);

    // Approval establishes trust.
    mgr.setApprovedServers(['proj']);
    assert.equal(mgr.isHeadersHelperTrusted('proj'), true);
  });

  it('connectServer skips the helper (and records a warning) for an untrusted project server', async () => {
    const mgr = new McpManager();
    const fake = new FakeClient();
    mgr._createClient = (() => fake) as never;
    mgr.addServer('ps', { command: '', transport: 'http', url: 'https://api.example.com', headersHelper: 'echo {}' });
    // Force the untrusted project scope on the live client.
    (mgr as unknown as { _serverScopes: Map<string, string> })._serverScopes.set('ps', 'project');
    await mgr.connectServer('ps');
    assert.equal(fake.applied, null, 'headersHelper must NOT run for an untrusted project server');
    const warnings = mgr.getConfigWarnings('ps');
    assert.ok(warnings.some((w) => /workspace-trust boundary/.test(w)), 'a trust-boundary warning is recorded');
  });

  it('connectServer runs the helper for an approved project server', async () => {
    const mgr = new McpManager();
    const fake = new FakeClient();
    mgr._createClient = (() => fake) as never;
    mgr.setEnableAllProjectServers(true); // blanket trust
    mgr.addServer('ap', { command: '', transport: 'http', url: 'https://api.example.com', headers: { 'X-Static': '1' }, headersHelper: 'exit 1' });
    (mgr as unknown as { _serverScopes: Map<string, string> })._serverScopes.set('ap', 'project');
    await mgr.connectServer('ap');
    assert.ok(fake.applied, 'headersHelper IS executed for an approved (trusted) project server');
    assert.equal(fake.applied?.['X-Static'], '1');
  });
});

describe('actionable errors for malformed / unreadable .mcp.json', () => {
  it('gives an actionable parse-error message naming common JSON mistakes', () => {
    const mgr = new McpManager();
    const p = writeConfig('{ "mcpServers": { "x": { "command": "run" }, }'); // trailing comma
    mgr.loadFromConfig(p);
    const errs = mgr.getConfigErrors();
    assert.equal(errs.length, 1);
    assert.equal(errs[0].severity, 'fatal');
    assert.ok(/trailing comma/.test(errs[0].message), 'message must guide the user to the fix');
  });

  it('distinguishes an unreadable config (a directory) from a malformed one', () => {
    const d = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-u018-dir-'));
    dirs.push(d);
    const asFile = path.join(d, 'cfg');
    fs.mkdirSync(asFile); // a directory where a file is expected → EISDIR on read
    const mgr = new McpManager();
    const events: unknown[] = [];
    mgr.on('config-error', (e) => events.push(e));
    mgr.loadFromConfig(asFile);
    const errs = mgr.getConfigErrors();
    assert.equal(errs.length, 1);
    assert.ok(/Cannot read MCP config/.test(errs[0].message));
    assert.equal(events.length, 1);
  });
});
