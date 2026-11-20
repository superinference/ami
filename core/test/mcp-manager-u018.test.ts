/**
 * U018 parity tests for src/mcp/manager.ts — four additive capabilities:
 *   1. validateMcpServerName — add-time server-name character validation
 *      (the reference-parity addMcpConfig name check).
 *   2. Per-server tool-count cap (MAX_MCP_TOOLS_PER_SERVER) enforced during tool
 *      ingestion, with a surfaced config warning.
 *   3. Suppressed-duplicate surfacing — a lower-precedence config entry that
 *      duplicates an already-registered server is recorded as an INFORMATIONAL
 *      diagnostic (not a fatal "already registered" validation error).
 *   4. A machine-private `local` MCP config scope (`.ami/mcp.local.json`) with
 *      highest first-wins precedence.
 *
 * ESM-SAFE: no source-text reads, no monkeypatching of frozen namespace imports;
 * fs via mkdtempSync temp dirs; the cap test drives a REAL spawned stdio server.
 */

import { describe, it, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  McpManager,
  validateMcpServerName,
  findMcpConfigScopes,
  MAX_MCP_TOOLS_PER_SERVER,
} from '../src/mcp/manager';

after(() => { setTimeout(() => process.exit(0), 200).unref(); });

// A minimal real stdio MCP server advertising `toolCount` distinct tools.
function manyToolsServerArgs(toolCount: number): string[] {
  const script = `
    const readline = require('readline');
    const rl = readline.createInterface({ input: process.stdin });
    const send = (o) => process.stdout.write(JSON.stringify(o) + '\\n');
    const N = ${toolCount};
    rl.on('line', (line) => {
      let msg; try { msg = JSON.parse(line); } catch { return; }
      const m = msg.method;
      if (m === 'initialize') {
        send({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: '2024-11-05',
          serverInfo: { name: 'srv', version: '1.0' }, capabilities: { tools: {} } } });
      } else if (m === 'tools/list') {
        const tools = [];
        for (let i = 0; i < N; i++) tools.push({ name: 'tool_' + i, description: 'd', inputSchema: { type: 'object' } });
        send({ jsonrpc: '2.0', id: msg.id, result: { tools } });
      } else if (msg.id !== undefined) {
        send({ jsonrpc: '2.0', id: msg.id, result: {} });
      }
    });
  `;
  return ['-e', script];
}

describe('U018 — validateMcpServerName', () => {
  it('accepts names of letters, digits, hyphen, and underscore', () => {
    for (const ok of ['github', 'my-server', 'my_server', 'srv2', 'A-b_9']) {
      assert.equal(validateMcpServerName(ok), null, `expected "${ok}" to be valid`);
    }
  });

  it('rejects an empty / whitespace-only name', () => {
    assert.match(validateMcpServerName('') ?? '', /must not be empty/);
    assert.match(validateMcpServerName('   ') ?? '', /must not be empty/);
  });

  it('rejects names with namespace-corrupting characters', () => {
    for (const bad of ['my server', 'my:server', 'my.server', 'a/b', 'x@y']) {
      const err = validateMcpServerName(bad);
      assert.ok(err, `expected "${bad}" to be rejected`);
      assert.match(err!, /only letters, numbers, hyphens, and underscores/);
    }
  });
});

describe('U018 — per-server tool-count cap', () => {
  let mgr: McpManager;
  afterEach(() => { mgr?.disconnectAll(); });

  it('ingests at most MAX_MCP_TOOLS_PER_SERVER tools and records a warning', async () => {
    mgr = new McpManager();
    const over = MAX_MCP_TOOLS_PER_SERVER + 5;
    mgr.addServer('srv', { command: process.execPath, args: manyToolsServerArgs(over) });
    await mgr.ensureConnected('srv');

    assert.equal(mgr.getToolsForServer('srv').length, MAX_MCP_TOOLS_PER_SERVER);
    const warnings = mgr.getConfigWarnings('srv');
    assert.ok(
      warnings.some((w) => w.includes(`advertises ${over} tools`) && w.includes('tool cap')),
      `expected a tool-cap warning, got ${JSON.stringify(warnings)}`,
    );
    // The warning is surfaced through the unified diagnostics view too.
    const diags = mgr.getConfigDiagnostics();
    assert.ok(diags.some((d) => d.serverName === 'srv' && d.severity === 'warning' && /tool cap/.test(d.message)));
  });

  it('does not cap or warn a server under the limit', async () => {
    mgr = new McpManager();
    mgr.addServer('srv', { command: process.execPath, args: manyToolsServerArgs(3) });
    await mgr.ensureConnected('srv');
    assert.equal(mgr.getToolsForServer('srv').length, 3);
    assert.equal(mgr.getConfigWarnings('srv').length, 0);
  });
});

describe('U018 — suppressed duplicate surfacing', () => {
  let tmp: string;
  let mgr: McpManager;
  afterEach(() => { mgr?.disconnectAll(); });
  after(() => { if (tmp) fs.rmSync(tmp, { recursive: true, force: true }); });

  it('records a cross-scope name collision as informational, not a fatal error', () => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'u018-dup-'));
    const fileA = path.join(tmp, 'a.json');
    const fileB = path.join(tmp, 'b.json');
    fs.writeFileSync(fileA, JSON.stringify({ mcpServers: { dup: { command: 'echo', args: ['A'] } } }));
    fs.writeFileSync(fileB, JSON.stringify({ mcpServers: { dup: { command: 'echo', args: ['B'] } } }));

    mgr = new McpManager();
    const events: Array<[string, string]> = [];
    mgr.on('duplicate-suppressed', (n: string, d: string) => events.push([n, d]));

    // Higher-precedence scope loads first (first-wins), then the duplicate.
    mgr.loadFromConfig(fileA, { scope: 'local' });
    mgr.loadFromConfig(fileB, { scope: 'user' });

    // The winning server keeps the name + its original (local) config/args.
    assert.equal(mgr.getServerConfig('dup')?.args?.[0], 'A');
    assert.equal(mgr.getServerScope('dup'), 'local');

    // The duplicate is surfaced informationally, NOT as a fatal validation error.
    const suppressed = mgr.getSuppressedDuplicates();
    assert.deepEqual(suppressed, [{ name: 'dup', duplicateOf: 'dup' }]);
    assert.deepEqual(events, [['dup', 'dup']]);
    assert.equal(mgr.getConfigValidationErrors().dup, undefined);

    const diags = mgr.getConfigDiagnostics();
    assert.ok(diags.some((d) => d.serverName === 'dup' && d.severity === 'warning' && /suppressed/.test(d.message)));

    // disconnectAll clears the suppressed set (install-scoped list, not history).
    mgr.disconnectAll();
    assert.deepEqual(mgr.getSuppressedDuplicates(), []);
  });
});

describe('U018 — local MCP config scope', () => {
  let tmp: string;
  after(() => { if (tmp) fs.rmSync(tmp, { recursive: true, force: true }); });

  it('discovers .ami/mcp.local.json as the highest-precedence local scope', () => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'u018-local-'));
    const amiDir = path.join(tmp, '.ami');
    fs.mkdirSync(amiDir, { recursive: true });
    const localPath = path.join(amiDir, 'mcp.local.json');
    const projectPath = path.join(amiDir, 'mcp.json');
    fs.writeFileSync(localPath, JSON.stringify({ mcpServers: {} }));
    fs.writeFileSync(projectPath, JSON.stringify({ mcpServers: {} }));

    const scopes = findMcpConfigScopes(tmp);
    const local = scopes.find((s) => s.scope === 'local');
    assert.ok(local, 'expected a local-scope entry');
    assert.equal(local!.path, localPath);
    // First-wins precedence: local is discovered before the committed project file.
    const localIdx = scopes.findIndex((s) => s.path === localPath);
    const projIdx = scopes.findIndex((s) => s.path === projectPath);
    assert.ok(localIdx >= 0 && projIdx >= 0 && localIdx < projIdx, 'local must precede project');
  });

  it('omits the local scope when no .ami/mcp.local.json exists', () => {
    const empty = fs.mkdtempSync(path.join(os.tmpdir(), 'u018-nolocal-'));
    try {
      assert.ok(!findMcpConfigScopes(empty).some((s) => s.scope === 'local'));
    } finally {
      fs.rmSync(empty, { recursive: true, force: true });
    }
  });
});
