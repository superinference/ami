import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { McpClient } from '../src/mcp/client';
import type { McpElicitationRequest, McpElicitationResult } from '../src/mcp/client';

// ---------------------------------------------------------------------------
// H08: a server that abandons a pending elicitation by sending
// notifications/cancelled must abort the client-side handler so a frontend
// prompt resolves as cancel, and the client must NOT answer the cancelled
// request.
// ---------------------------------------------------------------------------

// Server: on tools/call it sends an elicitation/create request (id 777), then a
// moment later sends notifications/cancelled for that id, then completes the call.
// It records whether it ever received an elicitation RESULT response from us.
const serverScript = `
  const readline = require('readline');
  const rl = readline.createInterface({ input: process.stdin });
  let gotElicitResponse = false;
  function send(o) { process.stdout.write(JSON.stringify(o) + '\\n'); }
  rl.on('line', (line) => {
    let msg; try { msg = JSON.parse(line); } catch { return; }
    if (msg.method === 'initialize') {
      send({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: '2024-11-05', serverInfo: { name: 's', version: '1' }, capabilities: {} } });
    } else if (msg.method === 'tools/list') {
      send({ jsonrpc: '2.0', id: msg.id, result: { tools: [{ name: 'go', description: '', inputSchema: { type: 'object', properties: {} } }] } });
    } else if (msg.method === 'tools/call') {
      // Inbound server->client elicitation request.
      send({ jsonrpc: '2.0', id: 777, method: 'elicitation/create', params: { message: 'need input', requestedSchema: { type: 'object', properties: { x: { type: 'string' } } } } });
      // Cancel it shortly after.
      setTimeout(() => send({ jsonrpc: '2.0', method: 'notifications/cancelled', params: { requestId: 777, reason: 'abandoned' } }), 60);
      // Finish the tool call a bit later.
      setTimeout(() => send({ jsonrpc: '2.0', id: msg.id, result: { content: [{ type: 'text', text: 'done, elicitResponse=' + gotElicitResponse }] } }), 200);
    } else if (msg.id === 777 && msg.result) {
      // We received an answer to the (cancelled) elicitation — should NOT happen.
      gotElicitResponse = true;
    }
  });
`;

describe('server-driven elicitation cancellation (H08)', () => {
  it('aborts the pending handler and does not answer the cancelled request', async () => {
    let sawSignal = false;
    let signalAborted = false;
    const onElicitation = (req: McpElicitationRequest): Promise<McpElicitationResult> =>
      new Promise((resolve) => {
        sawSignal = !!req.signal;
        // A handler that waits for user input; it resolves cancel when aborted.
        req.signal?.addEventListener('abort', () => { signalAborted = true; resolve({ action: 'cancel' }); }, { once: true });
        // Otherwise never resolves on its own (simulating a waiting prompt).
      });

    const client = new McpClient({ command: 'node', args: ['-e', serverScript], requestTimeout: 5000 });
    client.onElicitation = onElicitation;
    try {
      await client.connect();
      const res = await client.callTool('go', {}) as any;
      assert.equal(sawSignal, true, 'request carried an abort signal');
      assert.equal(signalAborted, true, 'the signal aborted on server cancellation');
      // The server reports whether it got an elicitation response; it must not have.
      assert.match(res.content[0].text, /elicitResponse=false/);
    } finally {
      client.disconnect();
    }
  });
});
