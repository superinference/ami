import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { McpClient } from '../src/mcp/client';
import type { McpElicitationRequest, McpElicitationResult } from '../src/mcp/client';

// ---------------------------------------------------------------------------
// URL-mode elicitation pass-through: a server can send a `mode:'url'`
// elicitation/create carrying `url` / `actionLabel` / `showCancel` / `completed`
// as siblings of message+requestedSchema. handleElicitation MUST forward those
// fields to the frontend handler (NOT strip them), so the frontend can branch
// into the open-browser + waiting lifecycle. The server records the result it
// receives back over the real stdio transport.
// ---------------------------------------------------------------------------

const serverScript = `
  const readline = require('readline');
  const rl = readline.createInterface({ input: process.stdin });
  let elicitResult = null;
  function send(o) { process.stdout.write(JSON.stringify(o) + '\\n'); }
  rl.on('line', (line) => {
    let msg; try { msg = JSON.parse(line); } catch { return; }
    if (msg.method === 'initialize') {
      send({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: '2025-06-18', serverInfo: { name: 's', version: '1' }, capabilities: {} } });
    } else if (msg.method === 'tools/list') {
      send({ jsonrpc: '2.0', id: msg.id, result: { tools: [{ name: 'go', description: '', inputSchema: { type: 'object', properties: {} } }] } });
    } else if (msg.method === 'tools/call') {
      // Inbound server->client URL-mode elicitation.
      send({ jsonrpc: '2.0', id: 888, method: 'elicitation/create', params: {
        message: 'Authorize the app', requestedSchema: {},
        mode: 'url', url: 'https://auth.example.com/go',
        actionLabel: 'Skip confirmation', showCancel: true, completed: true,
      } });
      // Report what the client answered a moment later.
      setTimeout(() => send({ jsonrpc: '2.0', id: msg.id, result: { content: [{ type: 'text', text: JSON.stringify(elicitResult) }] } }), 150);
    } else if (msg.id === 888 && msg.result) {
      elicitResult = msg.result;
    }
  });
`;

describe('URL-mode elicitation pass-through', () => {
  it('forwards mode/url/actionLabel/showCancel/completed to the handler', async () => {
    let seen: McpElicitationRequest | undefined;
    const onElicitation = (req: McpElicitationRequest): Promise<McpElicitationResult> => {
      seen = req;
      return Promise.resolve({ action: 'accept' });
    };
    const client = new McpClient({ command: 'node', args: ['-e', serverScript], requestTimeout: 5000 });
    client.onElicitation = onElicitation;
    try {
      await client.connect();
      const res = (await client.callTool('go', {})) as { content: Array<{ text: string }> };
      assert.ok(seen, 'the handler was invoked');
      assert.equal(seen!.mode, 'url');
      assert.equal(seen!.url, 'https://auth.example.com/go');
      assert.equal(seen!.actionLabel, 'Skip confirmation');
      assert.equal(seen!.showCancel, true);
      assert.equal(seen!.completed, true);
      // The server received exactly the accept result the handler produced.
      assert.deepEqual(JSON.parse(res.content[0].text), { action: 'accept' });
    } finally {
      client.disconnect();
    }
  });
});
