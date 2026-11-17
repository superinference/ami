/**
 * mcp-client-u041-in-process.test.ts — in-process (linked) MCP transport (U041).
 *
 * Parity with Claude Code's services/mcp/InProcessTransport.ts + SdkControlTransport.ts:
 * two linked transports let an MCP server and client run in the SAME process with no
 * subprocess/socket. Covers the three algorithmic guarantees (microtask delivery,
 * symmetric+idempotent close, send-after-close guard) AND a full end-to-end round-trip
 * through a real McpClient.connectInProcess driving an in-process MCP server object.
 *
 * ESM-SAFE: pure in-memory transports + a real McpClient (no spawn, no network, no
 * source-text reads, no namespace monkeypatching).
 */

import { describe, it, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { McpClient, MCP_TRANSPORTS } from '../src/mcp/client';
import { createLinkedTransportPair, type InProcessTransport } from '../src/mcp/in-process-transport';

after(() => { setTimeout(() => process.exit(0), 200); });

const liveClients: McpClient[] = [];
afterEach(() => {
  while (liveClients.length) {
    try { liveClients.pop()!.disconnect(); } catch { /* ignore */ }
  }
});
function track(c: McpClient): McpClient { liveClients.push(c); c.on('error', () => {}); return c; }

const nextTick = () => new Promise<void>((r) => setTimeout(r, 0));

describe('createLinkedTransportPair — primitive guarantees', () => {
  it("'inprocess' is a registered transport", () => {
    assert.ok((MCP_TRANSPORTS as readonly string[]).includes('inprocess'));
  });

  it('delivers send() to the peer onmessage on a later microtask (not synchronously)', async () => {
    const [a, b] = createLinkedTransportPair();
    const received: unknown[] = [];
    b.onmessage = (m) => received.push(m);
    const p = a.send({ hello: 1 });
    // Not delivered synchronously within the send() call (queueMicrotask breaks the stack).
    assert.deepEqual(received, []);
    await p;
    await nextTick();
    assert.deepEqual(received, [{ hello: 1 }]);
  });

  it('routes each end to the OTHER end (bidirectional)', async () => {
    const [a, b] = createLinkedTransportPair();
    const toA: unknown[] = [];
    const toB: unknown[] = [];
    a.onmessage = (m) => toA.push(m);
    b.onmessage = (m) => toB.push(m);
    await a.send('for-b');
    await b.send('for-a');
    await nextTick();
    assert.deepEqual(toB, ['for-b']);
    assert.deepEqual(toA, ['for-a']);
  });

  it('throws "Transport is closed" on send after close', async () => {
    const [a] = createLinkedTransportPair();
    await a.close();
    await assert.rejects(() => a.send({}), /Transport is closed/);
  });

  it('close() tears down BOTH ends exactly once, symmetrically', async () => {
    const [a, b] = createLinkedTransportPair();
    let aClosed = 0;
    let bClosed = 0;
    a.onclose = () => aClosed++;
    b.onclose = () => bClosed++;
    await a.close();
    assert.equal(aClosed, 1);
    assert.equal(bClosed, 1, 'peer onclose fired');
    assert.equal(a.closed, true);
    assert.equal(b.closed, true);
    // Idempotent: a second close (either end) fires nothing more.
    await a.close();
    await b.close();
    assert.equal(aClosed, 1);
    assert.equal(bClosed, 1);
  });

  it('drops a send whose peer closed between send() and delivery', async () => {
    const [a, b] = createLinkedTransportPair();
    const received: unknown[] = [];
    b.onmessage = (m) => received.push(m);
    void a.send({ late: true }); // queue delivery WITHOUT draining the microtask
    await b.close(); // peer closes synchronously before the delivery microtask runs
    await nextTick();
    assert.deepEqual(received, [], 'message to a closed peer is dropped');
  });

  it('surfaces a throwing peer handler via onerror instead of crashing', async () => {
    const [a, b] = createLinkedTransportPair();
    const errs: Error[] = [];
    b.onmessage = () => { throw new Error('handler boom'); };
    b.onerror = (e) => errs.push(e);
    await a.send({});
    await nextTick();
    assert.equal(errs.length, 1);
    assert.match(errs[0].message, /handler boom/);
  });
});

/**
 * A minimal in-process MCP server wired to the SERVER end of a linked pair. Answers
 * initialize / tools/list / tools/call over the transport, mirroring a real server's
 * JSON-RPC responses so a McpClient can drive it with zero subprocess/socket.
 */
function startInProcessEchoServer(server: InProcessTransport): void {
  server.onmessage = (raw) => {
    const msg = raw as { id?: number; method?: string; params?: Record<string, unknown> };
    if (msg.method === 'initialize') {
      void server.send({
        jsonrpc: '2.0',
        id: msg.id,
        result: { protocolVersion: '2025-06-18', serverInfo: { name: 'inproc-echo', version: '1.0.0' }, capabilities: { tools: {} } },
      });
    } else if (msg.method === 'notifications/initialized') {
      // no reply
    } else if (msg.method === 'tools/list') {
      void server.send({
        jsonrpc: '2.0',
        id: msg.id,
        result: { tools: [{ name: 'echo', description: 'echo back', inputSchema: { type: 'object' } }] },
      });
    } else if (msg.method === 'tools/call') {
      const args = (msg.params?.arguments ?? {}) as { text?: string };
      void server.send({
        jsonrpc: '2.0',
        id: msg.id,
        result: { content: [{ type: 'text', text: `echo: ${args.text ?? ''}` }] },
      });
    }
  };
  void server.start();
}

describe('McpClient.connectInProcess — end-to-end', () => {
  it('connects, lists tools and round-trips a tool call through the linked pair', async () => {
    const [clientEnd, serverEnd] = createLinkedTransportPair();
    startInProcessEchoServer(serverEnd);
    const client = track(new McpClient({ command: '', args: [] }));

    await client.connectInProcess(clientEnd);
    assert.equal(client.state, 'ready');
    assert.equal(client.serverInfo?.name, 'inproc-echo');

    const tools = await client.listTools();
    assert.equal(tools.length, 1);
    assert.equal(tools[0].name, 'echo');

    const result = await client.callTool('echo', { text: 'hi' }) as { content: Array<{ text: string }> };
    assert.equal(result.content[0].text, 'echo: hi');
  });

  it('disconnect() tears down the linked pair (server end sees onclose)', async () => {
    const [clientEnd, serverEnd] = createLinkedTransportPair();
    startInProcessEchoServer(serverEnd);
    let serverClosed = false;
    serverEnd.onclose = () => { serverClosed = true; };
    const client = new McpClient({ command: '', args: [] });
    client.on('error', () => {});

    await client.connectInProcess(clientEnd);
    client.disconnect();
    await nextTick();
    assert.equal(serverClosed, true, 'closing the client closes the server end');
    assert.equal(clientEnd.closed, true);
  });

  it('a peer close surfaces as the client close event', async () => {
    const [clientEnd, serverEnd] = createLinkedTransportPair();
    startInProcessEchoServer(serverEnd);
    const client = track(new McpClient({ command: '', args: [] }));
    const closed = new Promise<void>((resolve) => client.on('close', () => resolve()));
    await client.connectInProcess(clientEnd);
    await serverEnd.close(); // server tears down the pair
    await Promise.race([
      closed,
      new Promise<never>((_, rej) => setTimeout(() => rej(new Error('client close never fired')), 1000)),
    ]);
    assert.equal(client.state, 'disconnected');
  });
});
