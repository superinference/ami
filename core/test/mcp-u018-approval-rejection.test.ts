/**
 * U018 — PROJECT-tier MCP approval/rejection decision model (Claude-parity).
 *
 * AMI's M14 gate previously persisted only APPROVALS: a project `.mcp.json`
 * server the user did not approve re-surfaced as `pending` every session, and
 * there was no way to durably decline a server, bulk-trust a project, or reset
 * past choices. These tests exercise the added parity surface on McpManager:
 *
 *  - `setEnableAllProjectServers` — blanket "trust all project servers"
 *    (`enableAllProjectMcpServers`): every un-rejected project server auto-approves.
 *  - `setRejectedServers` / `rejectServer` / `rejectAllPending` — the persisted
 *    rejected set (`disabledMcpjsonServers`): a declined project server is
 *    SUPPRESSED on every load, never re-prompted.
 *  - `getProjectMcpServerStatus` — layered precedence (rejected > approved/
 *    enable-all > pending) with normalization-insensitive name matching.
 *  - `resetProjectMcpChoices` — clears approved + rejected + enable-all
 *    (`mcpResetChoicesHandler`), forcing re-approval.
 *
 * ESM-SAFE: transport is mocked via the `_createClient` DI seam (a local
 * FakeClient); configs are written to fs.mkdtempSync temp dirs.
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
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-u018-'));
  tmpDirs.push(d);
  return d;
}
after(() => { for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch {} } });

class FakeClient extends EventEmitter {
  state = 'ready';
  serverInfo: { name: string; version: string } | null = { name: 'fake', version: '1' };
  tools: Array<Record<string, unknown>> = [];
  resources: Array<Record<string, unknown>> = [];
  uptime: number | null = 1;
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
  async revokeAuth() {}
  disconnect() {}
  supportsChannelPermissionRelay() { return false; }
}

function writeConfig(dir: string, servers: Record<string, unknown>): string {
  const file = path.join(dir, 'mcp.json');
  fs.writeFileSync(file, JSON.stringify({ mcpServers: servers }));
  return file;
}

const SRV = { command: process.execPath, args: ['-e', ''] };

let mgr: McpManager;
let created: number;
beforeEach(() => {
  mgr = new McpManager();
  created = 0;
  mgr._createClient = (() => { created++; return new FakeClient() as never; });
});
afterEach(() => { mgr.disconnectAll(); });

describe('U018 — enableAllProjectServers blanket approve-all', () => {
  it('auto-approves every project server (added as live client, not pending) when set', () => {
    const file = writeConfig(tmp(), { a: SRV, b: SRV });
    mgr.setEnableAllProjectServers(true);
    assert.equal(mgr.getEnableAllProjectServers(), true);

    mgr.loadFromConfig(file, { projectScoped: true });

    assert.deepEqual(mgr.listServers().map(s => s.name).sort(), ['a', 'b']);
    assert.equal(created, 2);
    assert.deepEqual(mgr.listPendingApprovals(), []);
  });

  it('is OFF by default — project servers stay pending', () => {
    const file = writeConfig(tmp(), { a: SRV });
    assert.equal(mgr.getEnableAllProjectServers(), false);
    mgr.loadFromConfig(file, { projectScoped: true });
    assert.equal(mgr.needsApproval('a'), true);
    assert.equal(created, 0);
  });
});

describe('U018 — persisted rejected/disabled set', () => {
  it('a rejected project server is SUPPRESSED (not registered, not pending, not re-prompted)', () => {
    const file = writeConfig(tmp(), { evil: SRV, ok: SRV });
    mgr.setRejectedServers(['evil']);
    assert.deepEqual(mgr.getRejectedServers(), ['evil']);

    mgr.loadFromConfig(file, { projectScoped: true });

    // evil is gone entirely; ok is pending (unapproved, but not rejected).
    assert.equal(mgr.listAllServers().some(s => s.name === 'evil'), false);
    assert.equal(mgr.needsApproval('evil'), false);
    assert.deepEqual(mgr.listPendingApprovals().map(p => p.name), ['ok']);
  });

  it('rejectServer durably records a rejection and tears down a pending server', () => {
    const file = writeConfig(tmp(), { repoSrv: SRV });
    mgr.loadFromConfig(file, { projectScoped: true });
    assert.equal(mgr.needsApproval('repoSrv'), true);

    mgr.rejectServer('repoSrv');

    assert.deepEqual(mgr.getRejectedServers(), ['repoSrv']);
    assert.deepEqual(mgr.listPendingApprovals(), []);
    assert.equal(mgr.listAllServers().some(s => s.name === 'repoSrv'), false);

    // Reloading the same config must NOT re-surface the rejected server.
    mgr.loadFromConfig(file, { projectScoped: true });
    assert.deepEqual(mgr.listPendingApprovals(), []);
    assert.equal(created, 0);
  });

  it('rejectServer clears a prior approval (reject wins)', () => {
    mgr.setApprovedServers(['x']);
    mgr.rejectServer('x');
    assert.equal(mgr.isServerApproved('x'), false);
    assert.equal(mgr.getProjectMcpServerStatus('x'), 'rejected');
  });

  it('rejectServer tears down a LIVE approved client too', () => {
    const file = writeConfig(tmp(), { live: SRV });
    mgr.setApprovedServers(['live']);
    mgr.loadFromConfig(file, { projectScoped: true });
    assert.deepEqual(mgr.listServers().map(s => s.name), ['live']);

    mgr.rejectServer('live');
    assert.deepEqual(mgr.listServers(), []);
    assert.deepEqual(mgr.getRejectedServers(), ['live']);
  });
});

describe('U018 — getProjectMcpServerStatus precedence + name normalization', () => {
  it('rejected > approved/enable-all > pending', () => {
    mgr.setApprovedServers(['a']);
    mgr.setRejectedServers(['a']);           // both sets contain "a"
    assert.equal(mgr.getProjectMcpServerStatus('a'), 'rejected');

    mgr.setRejectedServers([]);
    assert.equal(mgr.getProjectMcpServerStatus('a'), 'approved');  // explicit approve

    assert.equal(mgr.getProjectMcpServerStatus('b'), 'pending');   // unknown
    mgr.setEnableAllProjectServers(true);
    assert.equal(mgr.getProjectMcpServerStatus('b'), 'approved');  // enable-all
    mgr.setRejectedServers(['b']);
    assert.equal(mgr.getProjectMcpServerStatus('b'), 'rejected');  // reject beats enable-all
  });

  it('matches names normalization-insensitively on both sides', () => {
    mgr.setRejectedServers(['my.server']);
    assert.equal(mgr.getProjectMcpServerStatus('my_server'), 'rejected');
    mgr.setRejectedServers([]);
    mgr.setApprovedServers(['has space']);
    assert.equal(mgr.getProjectMcpServerStatus('has_space'), 'approved');
  });
});

describe('U018 — rejectAllPending (cancel/ESC reject-all)', () => {
  it('durably rejects every server awaiting approval so none re-surface', () => {
    const file = writeConfig(tmp(), { a: SRV, b: SRV });
    mgr.loadFromConfig(file, { projectScoped: true });
    assert.deepEqual(mgr.listPendingApprovals().map(p => p.name).sort(), ['a', 'b']);

    const rejected = mgr.rejectAllPending();
    assert.deepEqual(rejected.sort(), ['a', 'b']);
    assert.deepEqual(mgr.getRejectedServers().sort(), ['a', 'b']);
    assert.deepEqual(mgr.listPendingApprovals(), []);

    mgr.loadFromConfig(file, { projectScoped: true });
    assert.deepEqual(mgr.listPendingApprovals(), []);
  });

  it('returns [] when nothing is pending', () => {
    assert.deepEqual(mgr.rejectAllPending(), []);
  });
});

describe('U018 — resetProjectMcpChoices', () => {
  it('clears approved + rejected + enable-all so servers are re-prompted', () => {
    const file = writeConfig(tmp(), { keep: SRV, drop: SRV });
    mgr.setApprovedServers(['keep']);
    mgr.setRejectedServers(['drop']);
    mgr.setEnableAllProjectServers(true);

    mgr.resetProjectMcpChoices();

    assert.deepEqual(mgr.getApprovedServers(), []);
    assert.deepEqual(mgr.getRejectedServers(), []);
    assert.equal(mgr.getEnableAllProjectServers(), false);

    // After reset, both servers gate to pending on load (re-prompt).
    mgr.loadFromConfig(file, { projectScoped: true });
    assert.deepEqual(mgr.listPendingApprovals().map(p => p.name).sort(), ['drop', 'keep']);
    assert.equal(created, 0);
  });
});
