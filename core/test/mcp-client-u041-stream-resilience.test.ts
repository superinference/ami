import { describe, it, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';
import { McpClient } from '../src/mcp/client';

after(() => { setTimeout(() => process.exit(0), 200); });

function listenOnRandom(server: http.Server): Promise<number> {
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const addr = server.address();
      resolve(typeof addr === 'object' ? addr!.port : 0);
    });
  });
}

function closeServer(server: http.Server): Promise<void> {
  return new Promise((resolve) => server.close(() => resolve()));
}

/** Handshake over POST; GET behavior is supplied per-test via onGet. */
function makeServer(onGet: (req: http.IncomingMessage, res: http.ServerResponse, getCount: number) => void): http.Server {
  let getCount = 0;
  return http.createServer((req, res) => {
    if (req.method === 'GET') { getCount++; onGet(req, res, getCount); return; }
    let body = '';
    req.on('data', (c) => { body += c; });
    req.on('end', () => {
      const msg = body ? JSON.parse(body) : {};
      if (msg.method === 'initialize') {
        res.writeHead(200, { 'Content-Type': 'application/json', 'Mcp-Session-Id': 'sess-u041' });
        res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: '2025-06-18', serverInfo: { name: 'stream-mcp', version: '1.0.0' }, capabilities: { tools: {} } } }));
        return;
      }
      if (typeof msg.method === 'string' && msg.method.startsWith('notifications/')) { res.writeHead(202); res.end(); return; }
      if (msg.method && msg.id != null) { res.writeHead(200, { 'Content-Type': 'application/json' }); res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: {} })); return; }
      res.writeHead(202); res.end();
    });
  });
}

describe('MCP client U041 — GET-stream resilience (liveness / resumption / permanent-codes / budget)', () => {
  let server: http.Server;
  let port: number;
  let client: McpClient;
  const openStreams: http.ServerResponse[] = [];

  afterEach(async () => {
    for (const s of openStreams) { try { s.end(); } catch { /* ignore */ } }
    openStreams.length = 0;
    if (client) client.disconnect();
    if (server) await closeServer(server);
  });

  it('reconnects the GET stream after a liveness/idle timeout (half-open detection)', { timeout: 6000 }, async () => {
    server = makeServer((_req, res) => {
      // Establish the stream (flush headers so the client sees 200) but then send
      // NOTHING — a silent/half-open connection the liveness watchdog must detect.
      res.writeHead(200, { 'Content-Type': 'text/event-stream', Connection: 'keep-alive' });
      res.flushHeaders();
      openStreams.push(res);
    });
    port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000, httpStreamLivenessTimeout: 150 });

    const timedOut = new Promise<void>((resolve) => client.on('http-stream-liveness-timeout', () => resolve()));
    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    await timedOut; // the silent stream was detected and torn down
  });

  it('keeps the stream alive while keepalive comment frames arrive (timer reset per chunk)', { timeout: 6000 }, async () => {
    server = makeServer((_req, res) => {
      res.writeHead(200, { 'Content-Type': 'text/event-stream', Connection: 'keep-alive' });
      openStreams.push(res);
      const iv = setInterval(() => { try { res.write(':keepalive\n\n'); } catch { /* ignore */ } }, 60);
      res.on('close', () => clearInterval(iv));
    });
    port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000, httpStreamLivenessTimeout: 180 });

    let timedOut = false;
    client.on('http-stream-liveness-timeout', () => { timedOut = true; });
    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    await new Promise((r) => setTimeout(r, 600)); // > 3× the idle budget, but kept alive by keepalives
    assert.equal(timedOut, false, 'keepalive frames must keep the liveness timer from firing');
  });

  it('resumes after reconnect with from_sequence_num + Last-Event-ID', { timeout: 6000 }, async () => {
    const secondGet = new Promise<{ url: string; lastEventId?: string }>((resolve) => {
      server = makeServer((req, res, getCount) => {
        res.writeHead(200, { 'Content-Type': 'text/event-stream', Connection: 'keep-alive' });
        if (getCount === 1) {
          // Push a sequenced event, then end the stream so the client reconnects.
          res.write(`id: 5\nevent: message\ndata: ${JSON.stringify({ jsonrpc: '2.0', method: 'notifications/message', params: { level: 'info', data: 'x' } })}\n\n`);
          res.end();
        } else {
          resolve({ url: req.url ?? '', lastEventId: req.headers['last-event-id'] as string | undefined });
          openStreams.push(res);
        }
      });
    });
    port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });

    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    const { url, lastEventId } = await secondGet;
    assert.ok(url.includes('from_sequence_num=5'), `reconnect URL must carry from_sequence_num=5, got ${url}`);
    assert.equal(lastEventId, '5', 'reconnect must send Last-Event-ID header');
  });

  it('dedupes a replayed SSE frame by sequence id (routes it only once)', { timeout: 6000 }, async () => {
    server = makeServer((_req, res) => {
      res.writeHead(200, { 'Content-Type': 'text/event-stream', Connection: 'keep-alive' });
      openStreams.push(res);
      const frame = (data: string) => `id: 7\nevent: message\ndata: ${data}\n\n`;
      const notif = JSON.stringify({ jsonrpc: '2.0', method: 'notifications/message', params: { level: 'info', data: 'dup' } });
      res.write(frame(notif));
      res.write(frame(notif)); // same id → must be dropped as a duplicate
    });
    port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });

    let routed = 0;
    let duplicates = 0;
    client.on('notification', (method: string) => { if (method === 'notifications/message') routed++; });
    client.on('http-stream-duplicate', () => { duplicates++; });
    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    await new Promise((r) => setTimeout(r, 200));
    assert.equal(routed, 1, 'a duplicate sequence must not be routed twice');
    assert.equal(duplicates, 1, 'the duplicate frame must be reported');
  });

  it('treats a 401 on the GET stream as permanent — closes without reconnecting', { timeout: 6000 }, async () => {
    let getCount = 0;
    server = makeServer((_req, res) => { getCount++; res.writeHead(401); res.end(); });
    port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });

    const closed = new Promise<number | undefined>((resolve) => client.on('http-stream-closed', (code?: number) => resolve(code)));
    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    const code = await closed;
    assert.equal(code, 401, 'permanent 401 should close the stream with the status');
    await new Promise((r) => setTimeout(r, 300));
    assert.equal(getCount, 1, 'a permanent 401 must NOT be retried');
    assert.equal((client as unknown as { httpStreamClosed: boolean }).httpStreamClosed, true);
  });

  it('403 on the GET stream is also permanent', { timeout: 6000 }, async () => {
    server = makeServer((_req, res) => { res.writeHead(403); res.end(); });
    port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });
    const closed = new Promise<number | undefined>((resolve) => client.on('http-stream-closed', (code?: number) => resolve(code)));
    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    assert.equal(await closed, 403);
  });

  it('wall-clock budget keeps retrying far past a fixed attempt count', { timeout: 6000 }, async () => {
    server = makeServer((_req, res) => { res.writeHead(200, { 'Content-Type': 'text/event-stream' }); openStreams.push(res); });
    port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });
    await client.connectHTTP(`http://127.0.0.1:${port}/`);

    let gaveUp = false;
    client.on('http-stream-closed', () => { gaveUp = true; });
    const c = client as unknown as { httpStreamReconnectAttempts: number; httpStreamReconnectStartTime: number | null; scheduleHttpStreamReconnect: () => void };
    // Far more than the old fixed cap of 5, but well within the 10-minute budget.
    c.httpStreamReconnectAttempts = 50;
    c.httpStreamReconnectStartTime = Date.now() - 1000;
    c.scheduleHttpStreamReconnect();
    assert.equal(gaveUp, false, '50 attempts within the budget must NOT give up');
  });
});
