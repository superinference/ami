/**
 * U041 — WebSocket MCP transport (`ws://` / `wss://`) + connect() late-rejection
 * swallow.
 *
 * Covers the additive WebSocket transport (parity with the reference's
 * `WebSocketTransport` in `utils/mcpWebSocketTransport.ts` and the `ws`/`wss`
 * transport selection in `services/mcp/{utils,types}.ts`): a selectable network
 * transport whose frames route through the SAME JSON-RPC dispatcher the stdio/http
 * transports use. The client transport is driven two ways:
 *   1. a fake, injectable WebSocket (deterministic unit coverage of every branch),
 *   2. a REAL hand-rolled RFC-6455 server + the runtime global `WebSocket`
 *      (end-to-end proof the wired path actually speaks WebSocket).
 *
 * Also validates the connect() late-rejection swallow (parity with the reference's
 * racing-request cleanup, `utils/computerUse/mcpServer.ts`): when the connect
 * timeout wins the race, the losing initialize promise's LATE rejection must not
 * surface as an unhandledRejection (RED without the `void connectPromise.catch`).
 *
 * ESM-SAFE: no source-text reads, no namespace monkeypatching of frozen imports,
 * no __dirname coupling. The WebSocket is injected via the documented
 * `webSocketFactory` seam; the e2e server binds `http.createServer` on port 0.
 */

import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import http from 'node:http';
import crypto from 'node:crypto';
import {
  McpClient,
  MCP_TRANSPORTS,
  MCP_WS_OPEN,
  type McpWebSocketLike,
} from '../src/mcp/client';
import {
  McpManager,
  isRemoteMcpTransport,
  validateMcpServerConfigShape,
} from '../src/mcp/manager';

const delay = (ms = 5) => new Promise((r) => setTimeout(r, ms));

/** Minimal in-memory WebSocket implementing {@link McpWebSocketLike} for tests. */
class FakeWebSocket implements McpWebSocketLike {
  readyState = 0; // CONNECTING
  sent: string[] = [];
  private listeners = new Map<string, Set<(ev: unknown) => void>>();
  constructor(public url: string) {}
  addEventListener(type: string, listener: (ev: unknown) => void): void {
    let set = this.listeners.get(type);
    if (!set) { set = new Set(); this.listeners.set(type, set); }
    set.add(listener);
  }
  removeEventListener(type: string, listener: (ev: unknown) => void): void {
    this.listeners.get(type)?.delete(listener);
  }
  private fire(type: string, ev?: unknown): void {
    for (const l of [...(this.listeners.get(type) ?? [])]) l(ev);
  }
  send(data: string): void { this.sent.push(data); }
  close(): void { if (this.readyState === 3) return; this.readyState = 3; this.fire('close'); }
  // --- test drivers ---
  open(): void { this.readyState = MCP_WS_OPEN; this.fire('open'); }
  errorOut(): void { this.fire('error', new Error('boom')); }
  serverSend(obj: unknown): void {
    this.fire('message', { data: typeof obj === 'string' ? obj : JSON.stringify(obj) });
  }
  /** Parse the nth JSON-RPC message the client sent. */
  parseSent(pred: (m: Record<string, unknown>) => boolean): Record<string, unknown> | undefined {
    for (const s of this.sent) {
      const m = JSON.parse(s) as Record<string, unknown>;
      if (pred(m)) return m;
    }
    return undefined;
  }
}

/** Open a ws client against a fake socket and complete the initialize handshake. */
async function connectFake(opts?: { url?: string }): Promise<{ client: McpClient; fake: FakeWebSocket }> {
  const url = opts?.url ?? 'ws://127.0.0.1:9/mcp';
  const fake = new FakeWebSocket(url);
  const client = new McpClient({ command: '', webSocketFactory: () => fake });
  const p = client.connectWebSocket(url);
  await delay();
  fake.open();
  await delay();
  const init = fake.parseSent((m) => m.method === 'initialize')!;
  fake.serverSend({
    jsonrpc: '2.0',
    id: init.id,
    result: { serverInfo: { name: 't', version: '1' }, capabilities: { tools: {} }, protocolVersion: '2025-06-18' },
  });
  await p;
  return { client, fake };
}

describe('U041 — WebSocket MCP transport', () => {
  const clients: McpClient[] = [];
  const track = (c: McpClient) => { clients.push(c); return c; };
  afterEach(() => { for (const c of clients.splice(0)) { try { c.disconnect(); } catch { /* noop */ } } });

  it('registers ws + wss as supported transports', () => {
    assert.ok(MCP_TRANSPORTS.includes('ws'));
    assert.ok(MCP_TRANSPORTS.includes('wss'));
  });

  it('completes the initialize handshake and reaches ready over a WebSocket', async () => {
    const { client, fake } = await connectFake();
    track(client);
    assert.equal(client.state, 'ready');
    assert.deepEqual(client.serverInfo, { name: 't', version: '1' });
    // The handshake must have been sent as discrete frames (initialize + initialized).
    assert.ok(fake.parseSent((m) => m.method === 'initialize'));
    assert.ok(fake.parseSent((m) => m.method === 'notifications/initialized'));
  });

  it('sets the wss transport for a wss:// url', async () => {
    const { client } = await connectFake({ url: 'wss://example.test/mcp' });
    track(client);
    assert.equal(client.state, 'ready');
  });

  it('round-trips a tools/call over the WebSocket and sanitizes the result', async () => {
    const { client, fake } = await connectFake();
    track(client);
    const callP = client.callTool('do', { x: 1 });
    await delay();
    const call = fake.parseSent((m) => m.method === 'tools/call')!;
    assert.equal((call.params as Record<string, unknown>).name, 'do');
    fake.serverSend({ jsonrpc: '2.0', id: call.id, result: { content: [{ type: 'text', text: 'ok' }] } });
    const result = await callP as { content: Array<{ text: string }> };
    assert.equal(result.content[0].text, 'ok');
  });

  it('answers a server→client ping request on the same socket', async () => {
    const { client, fake } = await connectFake();
    track(client);
    const before = fake.sent.length;
    fake.serverSend({ jsonrpc: '2.0', id: 77, method: 'ping' });
    await delay();
    const reply = fake.sent.slice(before).map((s) => JSON.parse(s)).find((m) => m.id === 77);
    assert.ok(reply, 'client must reply to the server ping over the socket');
    assert.deepEqual(reply.result, {});
  });

  it('emits a redacted parse-error that never echoes a malformed frame payload', async () => {
    const { client, fake } = await connectFake();
    track(client);
    const errors: string[] = [];
    client.on('parse-error', (p: string) => errors.push(p));
    fake.serverSend('{ not json — secret=hunter2');
    await delay();
    assert.equal(errors.length, 1);
    assert.ok(!errors[0].includes('hunter2'), 'raw untrusted payload must not be echoed');
    assert.match(errors[0], /websocket frame parse error/);
  });

  it('handles an unexpected close: disconnected state, close event, pending rejected', async () => {
    const { client, fake } = await connectFake();
    track(client);
    let closed = false;
    client.on('close', () => { closed = true; });
    const pending = client.callTool('slow', {});
    await delay();
    fake.close();
    await assert.rejects(pending, /WebSocket closed/);
    assert.equal(client.state, 'disconnected');
    assert.ok(closed);
  });

  it('rejects when the socket never opens before the connect timeout', async () => {
    const fake = new FakeWebSocket('ws://127.0.0.1:9/mcp');
    const client = track(new McpClient({ command: '', connectTimeout: 20, webSocketFactory: () => fake }));
    await assert.rejects(client.connectWebSocket('ws://127.0.0.1:9/mcp'), /timed out/);
    // connect()/connectWebSocket() tears down on failure (disconnect() wins the
    // final state), matching the stdio connect() failure path.
    assert.equal(client.state, 'disconnected');
  });

  it('rejects when the socket errors during connect', async () => {
    const fake = new FakeWebSocket('ws://127.0.0.1:9/mcp');
    const client = track(new McpClient({ command: '', connectTimeout: 1000, webSocketFactory: () => fake }));
    const p = client.connectWebSocket('ws://127.0.0.1:9/mcp');
    await delay();
    fake.errorOut();
    await assert.rejects(p, /WebSocket connection error/);
    assert.equal(client.state, 'disconnected');
  });

  it('throws a clear error when no WebSocket implementation is available', async () => {
    const g = globalThis as { WebSocket?: unknown };
    const saved = g.WebSocket;
    delete g.WebSocket;
    try {
      const client = track(new McpClient({ command: '' })); // no factory, no global
      await assert.rejects(client.connectWebSocket('ws://x/mcp'), /no global WebSocket/);
    } finally {
      if (saved !== undefined) g.WebSocket = saved;
    }
  });

  it('sending after the socket closed surfaces a not-open error', async () => {
    const { client, fake } = await connectFake();
    track(client);
    fake.readyState = 3; // CLOSED without firing the close handler
    await assert.rejects(client.callTool('x', {}), /not open/);
  });

  it('reconnect() re-establishes via the WebSocket branch', async () => {
    const sockets: FakeWebSocket[] = [];
    let url = 'ws://127.0.0.1:9/mcp';
    const client = track(new McpClient({
      command: '',
      connectTimeout: 1000,
      webSocketFactory: () => { const s = new FakeWebSocket(url); sockets.push(s); return s; },
    }));
    const p1 = client.connectWebSocket(url);
    await delay();
    sockets[0].open();
    await delay();
    const init1 = sockets[0].parseSent((m) => m.method === 'initialize')!;
    sockets[0].serverSend({ jsonrpc: '2.0', id: init1.id, result: { serverInfo: { name: 't', version: '1' }, capabilities: {}, protocolVersion: '2025-06-18' } });
    await p1;
    assert.equal(client.state, 'ready');

    // Drive a reconnect: it must open a NEW socket via connectWebSocket.
    const rp = client.reconnect();
    // Let the backoff (~1s with jitter) + new connect run; drive the second socket.
    for (let i = 0; i < 300 && sockets.length < 2; i++) await delay(10);
    assert.ok(sockets.length >= 2, 'reconnect must open a new WebSocket');
    sockets[1].open();
    await delay();
    const init2 = sockets[1].parseSent((m) => m.method === 'initialize')!;
    sockets[1].serverSend({ jsonrpc: '2.0', id: init2.id, result: { serverInfo: { name: 't', version: '1' }, capabilities: {}, protocolVersion: '2025-06-18' } });
    await rp;
    assert.equal(client.state, 'ready');
  });
});

describe('U041 — manager wiring for ws/wss', () => {
  let mgr: McpManager;
  afterEach(() => { try { mgr?.disconnectAll(); } catch { /* noop */ } });

  it('isRemoteMcpTransport treats http/sse/ws/wss as remote, stdio/undefined as local', () => {
    for (const t of ['http', 'sse', 'ws', 'wss']) assert.equal(isRemoteMcpTransport(t), true, t);
    for (const t of ['stdio', undefined, 'bogus']) assert.equal(isRemoteMcpTransport(t), false, String(t));
  });

  it('validateMcpServerConfigShape accepts ws/wss and rejects an unknown transport', () => {
    assert.deepEqual(validateMcpServerConfigShape({ transport: 'ws', url: 'ws://x/mcp' }), []);
    assert.deepEqual(validateMcpServerConfigShape({ transport: 'wss', url: 'wss://x/mcp' }), []);
    const bad = validateMcpServerConfigShape({ transport: 'carrier-pigeon' });
    assert.equal(bad.length, 1);
    // The valid-transport enumeration grew (U018 added the IDE/SDK connection
    // transports sse-ide/ws-ide/sdk); assert ws + wss are still named among them.
    assert.match(bad[0], /ws, wss/);
  });

  it('connectServer dispatches a ws server to the WebSocket transport', async () => {
    mgr = new McpManager();
    let wsUrl = '';
    class StubClient extends (await import('node:events')).EventEmitter {
      state = 'ready'; serverInfo = { name: 's', version: '1' }; tools: unknown[] = []; _tools: unknown[] = [];
      async connect() { throw new Error('should not use stdio'); }
      async connectWebSocket(u: string) { wsUrl = u; }
      async listTools() { return []; }
      disconnect() {}
    }
    mgr._createClient = (() => new StubClient() as never);
    mgr.addServer('wssrv', { command: '', transport: 'ws', url: 'ws://localhost:3/mcp' });
    await mgr.connectServer('wssrv');
    assert.equal(wsUrl, 'ws://localhost:3/mcp');
  });
});

// ---------------------------------------------------------------------------
// End-to-end: a REAL RFC-6455 server + the runtime global WebSocket.
// ---------------------------------------------------------------------------

const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
function wsAccept(key: string): string {
  return crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
}
/** Encode a server→client text frame (unmasked; supports 16-bit extended length). */
function encodeFrame(str: string): Buffer {
  const payload = Buffer.from(str, 'utf8');
  if (payload.length < 126) return Buffer.concat([Buffer.from([0x81, payload.length]), payload]);
  const head = Buffer.from([0x81, 126, (payload.length >> 8) & 0xff, payload.length & 0xff]);
  return Buffer.concat([head, payload]);
}
/** Decode complete client→server frames from a buffer; returns texts + the leftover. */
function decodeFrames(buf: Buffer): { texts: string[]; closed: boolean; rest: Buffer } {
  const texts: string[] = [];
  let closed = false;
  let off = 0;
  while (off + 2 <= buf.length) {
    const opcode = buf[off] & 0x0f;
    const masked = (buf[off + 1] & 0x80) !== 0;
    let len = buf[off + 1] & 0x7f;
    let hdr = 2;
    if (len === 126) { if (off + 4 > buf.length) break; len = (buf[off + 2] << 8) | buf[off + 3]; hdr = 4; }
    const need = off + hdr + (masked ? 4 : 0) + len;
    if (need > buf.length) break;
    let p = off + hdr;
    let mask: Buffer | null = null;
    if (masked) { mask = buf.subarray(p, p + 4); p += 4; }
    const raw = buf.subarray(p, p + len);
    const out = Buffer.alloc(len);
    for (let i = 0; i < len; i++) out[i] = mask ? raw[i] ^ mask[i % 4] : raw[i];
    if (opcode === 0x8) closed = true;
    else if (opcode === 0x1) texts.push(out.toString('utf8'));
    off = need;
  }
  return { texts, closed, rest: buf.subarray(off) };
}

describe('U041 — WebSocket transport end-to-end (real socket)', () => {
  it('connects, lists tools, and calls a tool over a real ws:// server', async () => {
    const server = http.createServer();
    server.on('upgrade', (req, socket) => {
      const key = req.headers['sec-websocket-key'] as string;
      socket.write(
        'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n' +
        `Sec-WebSocket-Accept: ${wsAccept(key)}\r\n\r\n`,
      );
      let acc = Buffer.alloc(0);
      socket.on('data', (chunk: Buffer) => {
        acc = Buffer.concat([acc, chunk]);
        const { texts, closed, rest } = decodeFrames(acc);
        acc = rest;
        if (closed) { try { socket.end(); } catch { /* noop */ } return; }
        for (const text of texts) {
          const msg = JSON.parse(text) as { id?: number; method?: string };
          if (msg.method === 'initialize') {
            socket.write(encodeFrame(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { serverInfo: { name: 'e2e', version: '9' }, capabilities: { tools: {} }, protocolVersion: '2025-06-18' } })));
          } else if (msg.method === 'tools/list') {
            socket.write(encodeFrame(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { tools: [{ name: 'echo', description: 'e', inputSchema: {} }] } })));
          } else if (msg.method === 'tools/call') {
            socket.write(encodeFrame(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { content: [{ type: 'text', text: 'pong' }] } })));
          }
          // notifications/initialized has no id → no reply needed.
        }
      });
      socket.on('error', () => { /* client teardown */ });
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const port = (server.address() as { port: number }).port;
    const url = `ws://127.0.0.1:${port}/mcp`;

    const client = new McpClient({ command: '', connectTimeout: 3000, requestTimeout: 3000 });
    try {
      await client.connectWebSocket(url);
      assert.equal(client.state, 'ready');
      assert.deepEqual(client.serverInfo, { name: 'e2e', version: '9' });
      const tools = await client.listTools();
      assert.equal(tools.length, 1);
      assert.equal(tools[0].name, 'echo');
      const res = await client.callTool('echo', { text: 'ping' }) as { content: Array<{ text: string }> };
      assert.equal(res.content[0].text, 'pong');
    } finally {
      client.disconnect();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });
});
