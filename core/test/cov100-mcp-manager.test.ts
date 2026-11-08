/**
 * Coverage-to-100 behavior test for src/mcp/manager.ts.
 *
 * Target: the defensive arm in McpManager.callTool() for an UNQUALIFIED tool
 * name (no `server:` prefix) whose cached tool-catalog entry points at a server
 * that no longer has a live client. This "stale tool-cache entry" guard
 * (`if (!client) throw ... 'not connected'`) is the one branch left uncovered by
 * the existing suite — every other callTool path (unqualified+found+live,
 * unqualified+not-found, qualified+missing-server) already has a test.
 *
 * ESM-SAFE: no source-text reads, no monkeypatching of frozen namespace imports.
 * The server is a REAL spawned stdio MCP process; the stale-cache precondition is
 * reconstructed by tearing the live client down out-of-band (the exact race the
 * guard defends against) and is observed purely through public behavior.
 */

import { describe, it, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { McpManager } from '../src/mcp/manager';

// Safety net: force-exit shortly after the suite so any stray child process
// (should be none — we disconnect explicitly) cannot keep the runner alive.
after(() => { setTimeout(() => process.exit(0), 200).unref(); });

// A minimal, real stdio MCP server run as `node -e <script>`: it initializes,
// advertises a single tool, and answers tools/call.
function stdioServerArgs(toolName: string): string[] {
  const script = `
    const readline = require('readline');
    const rl = readline.createInterface({ input: process.stdin });
    const send = (o) => process.stdout.write(JSON.stringify(o) + '\\n');
    rl.on('line', (line) => {
      let msg; try { msg = JSON.parse(line); } catch { return; }
      const m = msg.method;
      if (m === 'initialize') {
        send({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: '2024-11-05',
          serverInfo: { name: 'srv', version: '1.0' }, capabilities: { tools: {} } } });
      } else if (m === 'tools/list') {
        send({ jsonrpc: '2.0', id: msg.id, result: { tools: [
          { name: ${JSON.stringify(toolName)}, description: 'A tool', inputSchema: { type: 'object' } },
        ] } });
      } else if (m === 'tools/call') {
        send({ jsonrpc: '2.0', id: msg.id, result: { content: [{ type: 'text', text: 'called' }] } });
      } else if (msg.id !== undefined) {
        send({ jsonrpc: '2.0', id: msg.id, result: {} });
      }
    });
  `;
  return ['-e', script];
}

describe('McpManager.callTool — unqualified name, stale tool-cache entry', () => {
  let mgr: McpManager;
  afterEach(() => { mgr?.disconnectAll(); });

  it('resolves and calls an unqualified tool while the client is live', async () => {
    mgr = new McpManager();
    mgr.addServer('srv', { command: process.execPath, args: stdioServerArgs('getThing') });
    await mgr.ensureConnected('srv');

    // Baseline: the tool is cached, and an unqualified call reaches the server.
    assert.equal(mgr.getToolsForServer('srv').length, 1);
    const res = await mgr.callTool('getThing') as { content: Array<{ text: string }> };
    assert.equal(res.content[0].text, 'called');
  });

  it('throws "not connected" naming the server when the cached tool\'s client is gone', async () => {
    mgr = new McpManager();
    mgr.addServer('srv', { command: process.execPath, args: stdioServerArgs('getThing') });
    await mgr.ensureConnected('srv');
    assert.equal(mgr.getToolsForServer('srv').length, 1);

    // Reconstruct the guarded race: the live client is torn down out-of-band
    // (crash / external removal) but the _allTools catalog entry for its tool
    // still lingers, pointing at a server that no longer has a client.
    const internal = mgr as unknown as { clients: Map<string, { disconnect(): void }> };
    internal.clients.get('srv')!.disconnect();
    internal.clients.delete('srv');

    // The stale catalog entry survives (no invalidation happened).
    assert.equal(mgr.getToolsForServer('srv').length, 1);

    // Start from a clean last-used slate so we can prove the attempt is recorded.
    mgr._lastUsedAt.delete('srv');

    // The unqualified call resolves via the tool catalog to 'srv', then hits the
    // missing-client guard and surfaces a clear, server-named error (not a crash
    // on an undefined client).
    await assert.rejects(
      () => mgr.callTool('getThing'),
      (err: unknown) =>
        err instanceof Error && /MCP server 'srv' not connected/.test(err.message),
    );

    // The guard records the per-server last-used timestamp BEFORE discovering the
    // client is absent — the attempt is counted even though it fails.
    assert.ok(mgr._lastUsedAt.get('srv') !== undefined);
  });
});
