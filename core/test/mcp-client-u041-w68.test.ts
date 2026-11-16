import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { McpClient } from '../src/mcp/client';
import { McpManager } from '../src/mcp/manager';

// ---------------------------------------------------------------------------
// U041 (w68) — two additive MCP client capabilities:
//
// 1. On-demand cancellation of an in-flight reconnect BACKOFF sleep. A
//    disconnect()/stopServer that lands while the reconnect loop is mid-backoff
//    must wake the sleep at once (parity with Claude Code's wakePollLoop
//    cancelling an at-capacity sleep, bridge/bridgeDebug.ts) instead of burning
//    the full (up to 30s) delay before the loop notices the abort.
//    RED (pre-fix): reconnect() `await setTimeout(delay)` then checks the abort
//    flag, so an external disconnect waits out the whole first backoff (~1s).
//    GREEN: disconnect wakes the sleep and reconnect returns promptly.
//
// 2. Live per-tool-call progress. callTool attaches a unique `_meta.progressToken`
//    so a server can push `notifications/progress` for THAT call; the client
//    routes them to a `tool-progress` event (determinate percent + message), and
//    the manager re-emits it on its public bus (parity with Claude Code's
//    per-tool-call onProgress / progressToken).
//    RED (pre-fix): callTool sends no progressToken, so server progress is never
//    tied to the call and no `tool-progress` event is ever emitted.
// ---------------------------------------------------------------------------

describe('U041 w68 — reconnect backoff sleep is cancellable on disconnect', () => {
  it('wakes the in-flight backoff immediately when disconnect lands mid-sleep', async () => {
    // A command that exits non-zero so every connect attempt fails and the loop
    // stays in its backoff phase; we abort during the very first backoff sleep.
    const client = new McpClient({ command: 'node', args: ['-e', 'process.exit(1)'] });
    client.on('error', () => { /* swallow spawn/exit noise */ });

    const start = Date.now();
    const reconnecting = client.reconnect();
    // Disconnect well inside the first backoff (reconnectDelay(0) ≈ 750–1250ms).
    setTimeout(() => client.disconnect(), 50);
    await reconnecting; // resolves (aborted) — it must NOT reject here
    const elapsed = Date.now() - start;

    assert.ok(
      elapsed < 500,
      `reconnect should wake its backoff sleep on disconnect (took ${elapsed}ms; pre-fix waits out the full ~1s backoff)`,
    );
    assert.equal(client.state, 'disconnected');
  });
});

/**
 * A stdio MCP server that, on tools/call, echoes the received `_meta.progressToken`
 * and emits one determinate `notifications/progress` (progress 1 / total 2 → 50%)
 * BEFORE the result, so the client's per-call progress routing is exercised.
 */
const PROGRESS_SERVER = `
  const readline = require('readline');
  const rl = readline.createInterface({ input: process.stdin });
  rl.on('line', (line) => {
    let msg; try { msg = JSON.parse(line); } catch { return; }
    if (msg.method === 'initialize') {
      process.stdout.write(JSON.stringify({ jsonrpc:'2.0', id: msg.id, result: {
        protocolVersion: '2025-06-18',
        serverInfo: { name: 'progress-server', version: '1.0.0' },
        capabilities: {},
      }}) + '\\n');
    } else if (msg.method === 'tools/list') {
      process.stdout.write(JSON.stringify({ jsonrpc:'2.0', id: msg.id, result: {
        tools: [{ name: 'progressTool', description: 'runs with progress', inputSchema: { type:'object', properties:{} } }],
      }}) + '\\n');
    } else if (msg.method === 'tools/call') {
      const token = msg.params && msg.params._meta && msg.params._meta.progressToken;
      if (token) {
        process.stdout.write(JSON.stringify({ jsonrpc:'2.0', method:'notifications/progress', params:{
          progressToken: token, progress: 1, total: 2, message: 'half',
        }}) + '\\n');
      }
      process.stdout.write(JSON.stringify({ jsonrpc:'2.0', id: msg.id, result: {
        content: [{ type:'text', text: 'token=' + String(token) }],
      }}) + '\\n');
    }
  });
`;

describe('U041 w68 — live per-tool-call progress (progressToken → tool-progress)', () => {
  it('sends a _meta.progressToken and emits tool-progress with a determinate percent', async () => {
    const client = new McpClient({ command: 'node', args: ['-e', PROGRESS_SERVER], requestTimeout: 5000 });
    const progress: Array<{ name: string; percent?: number; message?: string }> = [];
    client.on('tool-progress', (p) => progress.push(p));
    try {
      await client.connect();
      await client.listTools();
      const result = await client.callTool('progressTool', {}) as { content: Array<{ text: string }> };
      // The server echoed the token it received, proving callTool attached _meta.progressToken.
      assert.match(result.content[0].text, /^token=mcp-\d+$/);
      assert.equal(progress.length, 1);
      assert.equal(progress[0].name, 'progressTool');
      assert.equal(progress[0].percent, 50);
      assert.equal(progress[0].message, 'half');
    } finally {
      client.disconnect();
    }
  });

  it('cleans up the progress callback after the call settles (no leak across calls)', async () => {
    const client = new McpClient({ command: 'node', args: ['-e', PROGRESS_SERVER], requestTimeout: 5000 });
    try {
      await client.connect();
      await client.listTools();
      await client.callTool('progressTool', {});
      await client.callTool('progressTool', {});
      // Each call registers + deletes exactly one token; the internal map must be empty.
      const cbs = (client as unknown as { progressCallbacks: Map<string, unknown> }).progressCallbacks;
      assert.equal(cbs.size, 0);
    } finally {
      client.disconnect();
    }
  });

  it('is re-emitted on the manager bus as tool-progress(serverName, payload)', async () => {
    const mgr = new McpManager();
    mgr.addServer('srv', { command: 'node', args: ['-e', PROGRESS_SERVER], requestTimeout: 5000 });
    const forwarded: Array<{ server: string; percent?: number }> = [];
    mgr.on('tool-progress', (server: string, p: { percent?: number }) => forwarded.push({ server, percent: p.percent }));
    try {
      await mgr.connectServer('srv');
      await mgr.callTool('srv:progressTool', {});
      assert.ok(
        forwarded.some((e) => e.server === 'srv' && e.percent === 50),
        `manager should forward tool-progress; got ${JSON.stringify(forwarded)}`,
      );
    } finally {
      mgr.disconnectAll();
    }
  });
});
