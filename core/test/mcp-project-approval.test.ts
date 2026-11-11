/**
 * M14 — per-server approval for PROJECT-tier MCP configs.
 *
 * Project-local MCP server configs (from a cloned repo's `.mcp.json` /
 * `.superinference/mcp.json`) must NOT auto-connect: a cloned repo could point MCP
 * at anything. These tests prove a new project server lands PENDING (awaiting
 * approval) instead of being registered as a connectable client, that approval
 * connects it, that a previously-approved server connects normally, and that
 * user/global-tier configs are unaffected.
 *
 * ESM-SAFE: no source-text reads, no namespace monkeypatching, no __dirname
 * coupling. The transport is mocked via the `_createClient` DI seam (a local
 * FakeClient) so no real process/network is spawned; configs are written to
 * fs.mkdtempSync temp dirs.
 */

import { describe, it, beforeEach, afterEach, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { EventEmitter } from 'node:events';
import { McpManager } from '../src/mcp/manager';

after(() => { setTimeout(() => process.exit(0), 200).unref(); });

const tmpDirs: string[] = [];
function tmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-m14-'));
  tmpDirs.push(d);
  return d;
}
after(() => { for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch {} } });

// Minimal in-memory MCP client — never spawns a process or opens a socket.
class FakeClient extends EventEmitter {
  state = 'ready';
  serverInfo: { name: string; version: string } | null = { name: 'fake', version: '1' };
  tools: Array<Record<string, unknown>> = [];
  resources: Array<Record<string, unknown>> = [];
  uptime: number | null = 42;
  _tools: Array<Record<string, unknown>> = [{ name: 'doThing', description: 'd', inputSchema: { type: 'object' } }];
  onElicitation?: (req: unknown) => Promise<unknown>;
  onSampling?: (req: unknown) => Promise<unknown>;
  async connect() {}
  async connectHTTP(_url: string) {}
  async connectSSE(_url: string) {}
  async listTools() { return this._tools; }
  async listResources() { return this.resources; }
  async readResource() { return {}; }
  async callTool() { return { ok: true }; }
  async listPrompts() { return []; }
  async getPrompt() { return { messages: [] }; }
  async ping() {}
  async reconnect() {}
  disconnect() {}
}

function writeConfig(dir: string, servers: Record<string, unknown>): string {
  const file = path.join(dir, 'mcp.json');
  fs.writeFileSync(file, JSON.stringify({ mcpServers: servers }));
  return file;
}

const SRV = { command: process.execPath, args: ['-e', ''] };

let mgr: McpManager;
let created: string[];
beforeEach(() => {
  mgr = new McpManager();
  created = [];
  // DI seam: observe/stub client creation so nothing real is spawned.
  mgr._createClient = (() => { created.push('x'); return new FakeClient() as never; });
});
afterEach(() => { mgr.disconnectAll(); });

describe('M14 — project-tier MCP approval gate', () => {
  it('registers a NEW project server as pending (needs approval), NOT as a connectable client', () => {
    const file = writeConfig(tmp(), { repoSrv: SRV });

    mgr.loadFromConfig(file, { projectScoped: true });

    // Not a live client → connectAll (which iterates clients) cannot bring it up.
    assert.deepEqual(mgr.listServers().map(s => s.name), []);
    assert.equal(created.length, 0, 'no MCP client should be created for an unapproved project server');
    // Surfaced as pending + awaiting approval.
    assert.equal(mgr.needsApproval('repoSrv'), true);
    assert.deepEqual(mgr.listPendingApprovals().map(p => p.name), ['repoSrv']);
    assert.ok(mgr.listAllServers().some(s => s.name === 'repoSrv' && s.state === 'pending'));
  });

  it('connectAll does NOT connect an unapproved project server', async () => {
    const file = writeConfig(tmp(), { repoSrv: SRV });
    mgr.loadFromConfig(file, { projectScoped: true });

    const results = await mgr.connectAll();
    assert.equal(results.has('repoSrv'), false);
    assert.equal(created.length, 0);
    assert.equal(mgr.needsApproval('repoSrv'), true);
  });

  it('approvePendingServer connects the server and records the approval', async () => {
    const file = writeConfig(tmp(), { repoSrv: SRV });
    mgr.loadFromConfig(file, { projectScoped: true });
    assert.equal(mgr.needsApproval('repoSrv'), true);

    await mgr.approvePendingServer('repoSrv');

    // Now a live, ready client with its tools discovered.
    assert.equal(created.length, 1);
    assert.equal(mgr.getServerState('repoSrv'), 'ready');
    assert.deepEqual(mgr.listServers().map(s => s.name), ['repoSrv']);
    assert.deepEqual(mgr.getToolsForServer('repoSrv').map(t => t.name), ['doThing']);
    // No longer pending; approval is persisted-readable.
    assert.equal(mgr.needsApproval('repoSrv'), false);
    assert.deepEqual(mgr.listPendingApprovals(), []);
    assert.deepEqual(mgr.getApprovedServers(), ['repoSrv']);
    assert.equal(mgr.isServerApproved('repoSrv'), true);
  });

  it('a previously-approved project server connects normally (no approval gate)', () => {
    const file = writeConfig(tmp(), { repoSrv: SRV });
    mgr.setApprovedServers(['repoSrv']);           // seeded from the persisted store

    mgr.loadFromConfig(file, { projectScoped: true });

    // Added as a live client straight away (connectAll would connect it).
    assert.deepEqual(mgr.listServers().map(s => s.name), ['repoSrv']);
    assert.equal(mgr.needsApproval('repoSrv'), false);
    assert.deepEqual(mgr.listPendingApprovals(), []);
  });

  it('user/global-tier configs are unaffected — every server is added directly', () => {
    const file = writeConfig(tmp(), { globalSrv: SRV });

    mgr.loadFromConfig(file);                        // no projectScoped flag

    assert.deepEqual(mgr.listServers().map(s => s.name), ['globalSrv']);
    assert.equal(mgr.needsApproval('globalSrv'), false);
    assert.deepEqual(mgr.listPendingApprovals(), []);
  });

  it('on-demand connect (e.g. the mcp-connect tool) is refused for an unapproved project server', async () => {
    const file = writeConfig(tmp(), { repoSrv: SRV });
    mgr.loadFromConfig(file, { projectScoped: true });

    await assert.rejects(
      () => mgr.ensureConnected('repoSrv'),
      /requires approval/,
    );
    assert.equal(created.length, 0, 'the connect bypass must not create a client');
    assert.equal(mgr.needsApproval('repoSrv'), true);
  });

  it('approvePendingServer throws for a server that is not pending', async () => {
    await assert.rejects(() => mgr.approvePendingServer('ghost'), /not pending approval/);
  });

  it('setApprovedServers replaces prior approvals', () => {
    mgr.setApprovedServers(['a', 'b']);
    assert.deepEqual(mgr.getApprovedServers().sort(), ['a', 'b']);
    mgr.setApprovedServers(['c']);
    assert.deepEqual(mgr.getApprovedServers(), ['c']);
  });
});
