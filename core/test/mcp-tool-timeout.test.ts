import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { McpClient } from '../src/mcp/client';
import { McpManager } from '../src/mcp/manager';

// ---------------------------------------------------------------------------
// H25/H30: a tool call must use the long tool-call timeout at the transport
// layer, so a tool that is slower than the short request/connect timeout is NOT
// killed by it. The server here answers tools/call after a delay LONGER than the
// client's requestTimeout but SHORTER than the tool timeout.
// ---------------------------------------------------------------------------

const slowServer = `
  const readline = require('readline');
  const rl = readline.createInterface({ input: process.stdin });
  rl.on('line', (line) => {
    try {
      const msg = JSON.parse(line);
      const send = (result) => process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result }) + '\\n');
      if (msg.method === 'initialize') {
        send({ protocolVersion: '2024-11-05', serverInfo: { name: 's', version: '1' }, capabilities: {} });
      } else if (msg.method === 'tools/list') {
        send({ tools: [{ name: 'slow', description: 'slow tool', inputSchema: { type: 'object', properties: {} } }] });
      } else if (msg.method === 'tools/call') {
        // Answer AFTER the (short) requestTimeout window but within the tool timeout.
        setTimeout(() => send({ content: [{ type: 'text', text: 'slow-ok' }] }), 250);
      }
    } catch {}
  });
`;

afterEach(() => { delete process.env.SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS; });

describe('MCP tool-call timeout vs request timeout (H25/H30)', () => {
  it('a slow tool call survives the short request timeout when given a long tool timeout', async () => {
    process.env.SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS = '5000';
    const mgr = new McpManager();
    // requestTimeout is short (100ms) — a naive call would be killed before 250ms.
    mgr.addServer('s', { command: 'node', args: ['-e', slowServer], requestTimeout: 100 });
    try {
      await mgr.connectServer('s');
      const res = await mgr.callTool('s:slow') as any;
      assert.equal(res.content[0].text, 'slow-ok');
    } finally {
      mgr.disconnectAll();
    }
  });

  it('a direct client call with the short request timeout still times out (no override)', async () => {
    const client = new McpClient({ command: 'node', args: ['-e', slowServer], requestTimeout: 100 });
    try {
      await client.connect();
      // No timeoutMs override → the short requestTimeout applies and the slow call fails.
      await assert.rejects(() => client.callTool('slow'), /timed out/);
    } finally {
      client.disconnect();
    }
  });
});
