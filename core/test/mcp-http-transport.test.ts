import { describe, it, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';
import { McpClient } from '../src/mcp/client';

after(() => { setTimeout(() => process.exit(0), 200); });

function createMockMcpHttpServer(opts?: {
  sseFraming?: boolean;
  rejectNotificationsWithId?: boolean;
}): http.Server {
  const sseFraming = opts?.sseFraming ?? false;
  const rejectNotifWithId = opts?.rejectNotificationsWithId ?? false;

  return http.createServer((req, res) => {
    if (req.method === 'GET') { res.writeHead(405); res.end(); return; }
    let body = '';
    req.on('data', (chunk) => { body += chunk; });
    req.on('end', () => {
      const msg = JSON.parse(body);

      if (rejectNotifWithId && msg.method?.startsWith('notifications/') && msg.id != null) {
        const err = { jsonrpc: '2.0', id: msg.id, error: { code: -32601, message: 'Method not found' } };
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(err));
        return;
      }

      if (msg.method === 'notifications/initialized') {
        res.writeHead(202);
        res.end();
        return;
      }

      let result: unknown;

      if (msg.method === 'initialize') {
        result = {
          protocolVersion: '2024-11-05',
          serverInfo: { name: 'mock-http-mcp', version: '1.0.0' },
          capabilities: { tools: {} },
        };
      } else if (msg.method === 'tools/list') {
        result = {
          tools: [
            { name: 'list-items', description: 'List all items', inputSchema: { type: 'object', properties: {} } },
            { name: 'get-item', description: 'Get one item', inputSchema: { type: 'object', properties: { id: { type: 'string' } }, required: ['id'] } },
            { name: 'create-item', description: 'Create item', inputSchema: { type: 'object', properties: { name: { type: 'string' } }, required: ['name'] } },
          ],
        };
      } else if (msg.method === 'tools/call') {
        const toolName = msg.params?.name;
        if (toolName === 'list-items') {
          result = { content: [{ type: 'text', text: JSON.stringify({ items: ['a', 'b', 'c'] }) }] };
        } else if (toolName === 'get-item') {
          result = { content: [{ type: 'text', text: JSON.stringify({ id: msg.params?.arguments?.id, name: 'test' }) }] };
        } else {
          result = { content: [{ type: 'text', text: 'ok' }] };
        }
      } else {
        result = {};
      }

      const response = { jsonrpc: '2.0', id: msg.id, result };

      if (sseFraming) {
        res.writeHead(200, { 'Content-Type': 'text/event-stream' });
        res.end(`event: message\ndata: ${JSON.stringify(response)}\n\n`);
      } else {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(response));
      }
    });
  });
}

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

// ---------------------------------------------------------------------------
// Bug 1: SSE-framed responses (text/event-stream with "data:" prefix)
// ---------------------------------------------------------------------------

describe('MCP HTTP transport — SSE-framed responses', () => {
  let server: http.Server;
  let port: number;
  let client: McpClient;

  afterEach(async () => {
    if (client) client.disconnect();
    if (server) await closeServer(server);
  });

  it('connects when server returns SSE-framed JSON', async () => {
    server = createMockMcpHttpServer({ sseFraming: true });
    port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });

    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    assert.equal(client.state, 'ready');
    assert.equal(client.serverInfo!.name, 'mock-http-mcp');
  });

  it('lists tools from SSE-framed response', async () => {
    server = createMockMcpHttpServer({ sseFraming: true });
    port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });

    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    const tools = await client.listTools();
    assert.equal(tools.length, 3);
    assert.ok(tools.some(t => t.name === 'list-items'));
    assert.ok(tools.some(t => t.name === 'get-item'));
    assert.ok(tools.some(t => t.name === 'create-item'));
  });

  it('calls tool and parses SSE-framed result', async () => {
    server = createMockMcpHttpServer({ sseFraming: true });
    port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });

    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    const result = await client.callTool('list-items', {}) as any;
    assert.ok(result);
    const content = Array.isArray(result) ? result : result.content;
    assert.ok(content);
    const text = content.find((c: any) => c.type === 'text');
    const parsed = JSON.parse(text.text);
    assert.deepEqual(parsed.items, ['a', 'b', 'c']);
  });

  it('also works with plain JSON responses (non-SSE)', async () => {
    server = createMockMcpHttpServer({ sseFraming: false });
    port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });

    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    assert.equal(client.state, 'ready');
    const tools = await client.listTools();
    assert.equal(tools.length, 3);
  });
});

// ---------------------------------------------------------------------------
// Bug 2: JSON-RPC notifications must not include 'id'
// ---------------------------------------------------------------------------

describe('MCP HTTP transport — notification handling', () => {
  let server: http.Server;
  let port: number;
  let client: McpClient;

  afterEach(async () => {
    if (client) client.disconnect();
    if (server) await closeServer(server);
  });

  it('connects to server that rejects notifications with id', async () => {
    server = createMockMcpHttpServer({ rejectNotificationsWithId: true });
    port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });

    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    assert.equal(client.state, 'ready');
  });

  it('notifications/initialized sent without id field', async () => {
    let capturedNotification: Record<string, unknown> | null = null;
    server = http.createServer((req, res) => {
      if (req.method === 'GET') { res.writeHead(405); res.end(); return; }
      let body = '';
      req.on('data', (chunk) => { body += chunk; });
      req.on('end', () => {
        const msg = JSON.parse(body);
        if (msg.method === 'notifications/initialized') {
          capturedNotification = msg;
          res.writeHead(202);
          res.end();
          return;
        }
        if (msg.method === 'initialize') {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: {
              protocolVersion: '2024-11-05',
              serverInfo: { name: 'capture-server', version: '1.0.0' },
              capabilities: {},
            },
          }));
          return;
        }
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: {} }));
      });
    });
    port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });

    await client.connectHTTP(`http://127.0.0.1:${port}/`);

    assert.ok(capturedNotification, 'server should have received notifications/initialized');
    assert.equal(capturedNotification!.id, undefined, 'notification must NOT have an id field');
    assert.equal(capturedNotification!.method, 'notifications/initialized');
    assert.equal(capturedNotification!.jsonrpc, '2.0');
  });

  it('regular requests still include id field', async () => {
    let capturedInitialize: Record<string, unknown> | null = null;
    server = http.createServer((req, res) => {
      if (req.method === 'GET') { res.writeHead(405); res.end(); return; }
      let body = '';
      req.on('data', (chunk) => { body += chunk; });
      req.on('end', () => {
        const msg = JSON.parse(body);
        if (msg.method === 'initialize') {
          capturedInitialize = msg;
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: {
              protocolVersion: '2024-11-05',
              serverInfo: { name: 'capture-server', version: '1.0.0' },
              capabilities: {},
            },
          }));
          return;
        }
        if (msg.method?.startsWith('notifications/')) {
          res.writeHead(202);
          res.end();
          return;
        }
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: {} }));
      });
    });
    port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });

    await client.connectHTTP(`http://127.0.0.1:${port}/`);

    assert.ok(capturedInitialize, 'server should have received initialize');
    assert.ok(capturedInitialize!.id != null, 'initialize request MUST have an id field');
  });
});

// ---------------------------------------------------------------------------
// HTTP transport — custom headers
// ---------------------------------------------------------------------------

describe('MCP HTTP transport — custom headers', () => {
  let server: http.Server;
  let port: number;
  let client: McpClient;

  afterEach(async () => {
    if (client) client.disconnect();
    if (server) await closeServer(server);
  });

  it('sends custom Authorization header', async () => {
    let capturedAuthHeader: string | undefined;
    server = http.createServer((req, res) => {
      if (req.method === 'GET') { res.writeHead(405); res.end(); return; }
      capturedAuthHeader = req.headers.authorization as string | undefined;
      let body = '';
      req.on('data', (chunk) => { body += chunk; });
      req.on('end', () => {
        const msg = JSON.parse(body);
        if (msg.method === 'initialize') {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: {
              protocolVersion: '2024-11-05',
              serverInfo: { name: 'auth-server', version: '1.0.0' },
              capabilities: {},
            },
          }));
        } else if (msg.method?.startsWith('notifications/')) {
          res.writeHead(202);
          res.end();
        } else {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: {} }));
        }
      });
    });
    port = await listenOnRandom(server);
    client = new McpClient({
      command: '',
      headers: { Authorization: 'Bearer test-api-key-123' },
      requestTimeout: 5000,
      connectTimeout: 5000,
    });

    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    assert.equal(capturedAuthHeader, 'Bearer test-api-key-123');
  });

  it('sends multiple custom headers', async () => {
    let capturedHeaders: Record<string, string> = {};
    server = http.createServer((req, res) => {
      if (req.method === 'GET') { res.writeHead(405); res.end(); return; }
      capturedHeaders = {
        authorization: req.headers.authorization as string || '',
        'x-custom': req.headers['x-custom'] as string || '',
        'x-request-id': req.headers['x-request-id'] as string || '',
      };
      let body = '';
      req.on('data', (chunk) => { body += chunk; });
      req.on('end', () => {
        const msg = JSON.parse(body);
        if (msg.method === 'initialize') {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: {
              protocolVersion: '2024-11-05',
              serverInfo: { name: 'multi-header-server', version: '1.0.0' },
              capabilities: {},
            },
          }));
        } else if (msg.method?.startsWith('notifications/')) {
          res.writeHead(202);
          res.end();
        } else {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: {} }));
        }
      });
    });
    port = await listenOnRandom(server);
    client = new McpClient({
      command: '',
      headers: {
        Authorization: 'Bearer key-abc',
        'X-Custom': 'custom-value',
        'X-Request-ID': 'req-42',
      },
      requestTimeout: 5000,
      connectTimeout: 5000,
    });

    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    assert.equal(capturedHeaders.authorization, 'Bearer key-abc');
    assert.equal(capturedHeaders['x-custom'], 'custom-value');
    assert.equal(capturedHeaders['x-request-id'], 'req-42');
  });

  it('sends headers on every request, not just initialize', async () => {
    const seenAuths: string[] = [];
    server = http.createServer((req, res) => {
      if (req.method === 'GET') { res.writeHead(405); res.end(); return; }
      seenAuths.push(req.headers.authorization as string || '');
      let body = '';
      req.on('data', (chunk) => { body += chunk; });
      req.on('end', () => {
        const msg = JSON.parse(body);
        if (msg.method === 'initialize') {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: {
              protocolVersion: '2024-11-05',
              serverInfo: { name: 'multi-req-server', version: '1.0.0' },
              capabilities: { tools: {} },
            },
          }));
        } else if (msg.method?.startsWith('notifications/')) {
          res.writeHead(202);
          res.end();
        } else if (msg.method === 'tools/list') {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({
            jsonrpc: '2.0', id: msg.id,
            result: { tools: [{ name: 'test', description: 'test', inputSchema: { type: 'object' } }] },
          }));
        } else {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: {} }));
        }
      });
    });
    port = await listenOnRandom(server);
    client = new McpClient({
      command: '',
      headers: { Authorization: 'Bearer persistent-key' },
      requestTimeout: 5000,
      connectTimeout: 5000,
    });

    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    await client.listTools();

    assert.ok(seenAuths.length >= 3, `expected at least 3 requests (init + notif + tools/list), got ${seenAuths.length}`);
    for (const auth of seenAuths) {
      assert.equal(auth, 'Bearer persistent-key', 'every request must include the auth header');
    }
  });
});

// ---------------------------------------------------------------------------
// M13: Streamable-HTTP server→client GET SSE stream
// ---------------------------------------------------------------------------

/**
 * Mock Streamable-HTTP MCP server with BOTH halves of the transport:
 *  - POST: ordinary JSON-RPC (returns an Mcp-Session-Id on initialize).
 *  - GET : the server→client SSE stream. `getStatus` controls whether it is
 *          accepted (200, held open — `onStream` gets the response to push on)
 *          or declined (e.g. 405, to assert graceful POST-only degradation).
 */
function createStreamingMcpServer(opts: {
  getStatus?: number;
  onStream?: (res: http.ServerResponse) => void;
  onPost?: (msg: Record<string, unknown>) => void;
}): http.Server {
  const getStatus = opts.getStatus ?? 200;
  return http.createServer((req, res) => {
    if (req.method === 'GET') {
      if (getStatus !== 200) { res.writeHead(getStatus); res.end(); return; }
      res.writeHead(200, { 'Content-Type': 'text/event-stream', Connection: 'keep-alive' });
      opts.onStream?.(res);
      return; // held open for server→client pushes
    }
    let body = '';
    req.on('data', (chunk) => { body += chunk; });
    req.on('end', () => {
      const msg = body ? JSON.parse(body) : {};
      opts.onPost?.(msg);
      if (msg.method === 'initialize') {
        res.writeHead(200, { 'Content-Type': 'application/json', 'Mcp-Session-Id': 'sess-123' });
        res.end(JSON.stringify({
          jsonrpc: '2.0', id: msg.id,
          result: { protocolVersion: '2025-06-18', serverInfo: { name: 'stream-mcp', version: '1.0.0' }, capabilities: { tools: {} } },
        }));
        return;
      }
      if (typeof msg.method === 'string' && msg.method.startsWith('notifications/')) { res.writeHead(202); res.end(); return; }
      // A plain JSON-RPC request from the client (e.g. tools/list).
      if (msg.method && msg.id != null) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: {} }));
        return;
      }
      // A client→server RESPONSE to one of OUR server→client requests: just ack.
      res.writeHead(202); res.end();
    });
  });
}

describe('MCP HTTP transport — server→client GET SSE stream (M13)', () => {
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

  it('routes a server-pushed SSE notification to the message handler', { timeout: 5000 }, async () => {
    server = createStreamingMcpServer({
      onStream: (res) => {
        openStreams.push(res);
        // Push a server→client notification down the GET stream.
        res.write(`event: message\ndata: ${JSON.stringify({ jsonrpc: '2.0', method: 'notifications/message', params: { level: 'info', data: 'pushed' } })}\n\n`);
      },
    });
    port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });

    const got = new Promise<{ method: string; params: unknown }>((resolve) => {
      client.on('notification', (method: string, params: unknown) => {
        if (method === 'notifications/message') resolve({ method, params });
      });
    });

    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    let timer: ReturnType<typeof setTimeout>;
    const timeout = new Promise<never>((_, rej) => {
      timer = setTimeout(() => rej(new Error('no server-pushed notification received')), 3000);
    });
    const evt = await Promise.race([got, timeout]).finally(() => clearTimeout(timer));
    assert.equal(evt.method, 'notifications/message');
    assert.deepEqual(evt.params, { level: 'info', data: 'pushed' });
  });

  it('answers a server→client request pushed on the stream (POSTs the response back)', async () => {
    const posted: Array<Record<string, unknown>> = [];
    server = createStreamingMcpServer({
      onPost: (msg) => { posted.push(msg); },
      onStream: (res) => {
        openStreams.push(res);
        // Server asks the client to ping — the client must POST a response back.
        res.write(`event: message\ndata: ${JSON.stringify({ jsonrpc: '2.0', id: 7, method: 'ping' })}\n\n`);
      },
    });
    port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });

    await client.connectHTTP(`http://127.0.0.1:${port}/`);

    // Wait for the client's POSTed JSON-RPC response (id 7, result present).
    const deadline = Date.now() + 3000;
    let reply: Record<string, unknown> | undefined;
    while (Date.now() < deadline && !reply) {
      reply = posted.find((m) => m.id === 7 && m.result !== undefined && m.method === undefined);
      if (!reply) await new Promise((r) => setTimeout(r, 20));
    }
    assert.ok(reply, 'client should POST a JSON-RPC response for the server ping');
    assert.equal(reply!.jsonrpc, '2.0');
    assert.deepEqual(reply!.result, {});
  });

  it('sends the session id and protocol version on the GET stream', async () => {
    let streamHeaders: http.IncomingHttpHeaders | undefined;
    server = http.createServer((req, res) => {
      if (req.method === 'GET') {
        streamHeaders = req.headers;
        res.writeHead(200, { 'Content-Type': 'text/event-stream' });
        openStreams.push(res);
        return;
      }
      let body = '';
      req.on('data', (c) => { body += c; });
      req.on('end', () => {
        const msg = JSON.parse(body);
        if (msg.method === 'initialize') {
          res.writeHead(200, { 'Content-Type': 'application/json', 'Mcp-Session-Id': 'abc-789' });
          res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: '2025-06-18', serverInfo: { name: 's', version: '1' }, capabilities: {} } }));
          return;
        }
        res.writeHead(202); res.end();
      });
    });
    port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });

    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    const deadline = Date.now() + 3000;
    while (Date.now() < deadline && !streamHeaders) await new Promise((r) => setTimeout(r, 20));
    assert.ok(streamHeaders, 'server should receive the GET stream request');
    assert.equal(streamHeaders!.accept, 'text/event-stream');
    assert.equal(streamHeaders!['mcp-session-id'], 'abc-789');
    assert.equal(streamHeaders!['mcp-protocol-version'], '2025-06-18');
  });

  it('degrades to POST-only when the server declines the GET stream (405)', async () => {
    let unsupported = 0;
    server = createStreamingMcpServer({ getStatus: 405 });
    port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });
    client.on('http-stream-unsupported', () => { unsupported++; });

    // Must NOT throw, and ordinary POST operations must still work.
    await assert.doesNotReject(() => client.connectHTTP(`http://127.0.0.1:${port}/`));
    assert.equal(client.state, 'ready');
    await assert.doesNotReject(() => client.ping());

    // Give the GET's 405 response a moment to land.
    await new Promise((r) => setTimeout(r, 50));
    assert.equal(unsupported, 1, 'client should emit http-stream-unsupported on a 405');
  });

  it('reconnects the stream after an unexpected close', async () => {
    let getCount = 0;
    server = http.createServer((req, res) => {
      if (req.method === 'GET') {
        getCount++;
        res.writeHead(200, { 'Content-Type': 'text/event-stream' });
        if (getCount === 1) {
          // Drop the first stream immediately → client must re-open it.
          res.end();
        } else {
          openStreams.push(res);
        }
        return;
      }
      let body = '';
      req.on('data', (c) => { body += c; });
      req.on('end', () => {
        const msg = JSON.parse(body);
        if (msg.method === 'initialize') {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: '2025-06-18', serverInfo: { name: 's', version: '1' }, capabilities: {} } }));
          return;
        }
        res.writeHead(202); res.end();
      });
    });
    port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });

    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    // First GET closes immediately; the bounded backoff re-opens it (~1s).
    const deadline = Date.now() + 4000;
    while (Date.now() < deadline && getCount < 2) await new Promise((r) => setTimeout(r, 50));
    assert.ok(getCount >= 2, `expected the stream to re-open after close, saw ${getCount} GET(s)`);
  });
});

// ---------------------------------------------------------------------------
// M13 defensive branches: batch payloads, OAuth on the stream, malformed SSE,
// non-200 reconnect, reconnect exhaustion, and disconnect teardown.
// ---------------------------------------------------------------------------

/** Minimal POST handler shared by the inline servers below (init + ack POSTs). */
function respondInitOrAck(
  msg: Record<string, unknown>,
  res: http.ServerResponse,
  extraInitHeaders: Record<string, string> = {},
): void {
  if (msg.method === 'initialize') {
    res.writeHead(200, { 'Content-Type': 'application/json', ...extraInitHeaders });
    res.end(JSON.stringify({
      jsonrpc: '2.0', id: msg.id,
      result: { protocolVersion: '2025-06-18', serverInfo: { name: 's', version: '1' }, capabilities: {} },
    }));
    return;
  }
  res.writeHead(202); res.end();
}

describe('MCP HTTP transport — server→client stream defensive branches (M13)', () => {
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

  it('routes a JSON-RPC BATCH (array) pushed on the stream', { timeout: 5000 }, async () => {
    server = createStreamingMcpServer({
      onStream: (res) => {
        openStreams.push(res);
        res.write(`event: message\ndata: ${JSON.stringify([
          { jsonrpc: '2.0', method: 'notifications/message', params: { level: 'info', data: 'one' } },
          { jsonrpc: '2.0', method: 'notifications/message', params: { level: 'info', data: 'two' } },
        ])}\n\n`);
      },
    });
    port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });

    const datas: string[] = [];
    const got = new Promise<void>((resolve) => {
      client.on('notification', (m: string, p: { data?: string } | undefined) => {
        if (m === 'notifications/message') { datas.push(p?.data ?? ''); if (datas.length === 2) resolve(); }
      });
    });
    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    let timer: ReturnType<typeof setTimeout>;
    const to = new Promise<never>((_, rej) => { timer = setTimeout(() => rej(new Error('batch not fully routed')), 3000); });
    await Promise.race([got, to]).finally(() => clearTimeout(timer));
    assert.deepEqual([...datas].sort(), ['one', 'two']);
  });

  it('POSTs a server-request response carrying the OAuth Authorization header', { timeout: 5000 }, async () => {
    const postAuths: Array<string | undefined> = [];
    server = http.createServer((req, res) => {
      if (req.method === 'GET') {
        res.writeHead(200, { 'Content-Type': 'text/event-stream' });
        openStreams.push(res);
        res.write(`event: message\ndata: ${JSON.stringify({ jsonrpc: '2.0', id: 9, method: 'ping' })}\n\n`);
        return;
      }
      let body = ''; req.on('data', (c) => { body += c; });
      req.on('end', () => {
        const msg = body ? JSON.parse(body) : {};
        if (typeof msg.method === 'string' && msg.method.startsWith('notifications/')) { res.writeHead(202); res.end(); return; }
        // The client's POSTed response to our ping (id 9, result, no method).
        if (msg.id === 9 && msg.result !== undefined && msg.method === undefined) {
          postAuths.push(req.headers.authorization as string | undefined);
        }
        respondInitOrAck(msg, res, { 'Mcp-Session-Id': 'sess-oauth' });
      });
    });
    port = await listenOnRandom(server);
    const oauth = { authorizationHeader: async () => 'Bearer stream-tok', onUnauthorized: async () => false };
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000, oauth });

    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    const deadline = Date.now() + 3000;
    while (Date.now() < deadline && postAuths.length === 0) await new Promise((r) => setTimeout(r, 20));
    assert.equal(postAuths.length, 1, 'client should POST a response to the server ping');
    assert.equal(postAuths[0], 'Bearer stream-tok', 'POSTed response must carry the OAuth bearer token');
  });

  it('keeps a custom Authorization header on the GET stream even when OAuth is present', { timeout: 5000 }, async () => {
    let getAuth: string | undefined;
    server = http.createServer((req, res) => {
      if (req.method === 'GET') {
        getAuth = req.headers.authorization as string | undefined;
        res.writeHead(200, { 'Content-Type': 'text/event-stream' });
        openStreams.push(res);
        return;
      }
      let body = ''; req.on('data', (c) => { body += c; });
      req.on('end', () => { respondInitOrAck(body ? JSON.parse(body) : {}, res); });
    });
    port = await listenOnRandom(server);
    const oauth = { authorizationHeader: async () => 'Bearer oauth-tok', onUnauthorized: async () => false };
    client = new McpClient({ command: '', headers: { Authorization: 'Bearer custom-static' }, requestTimeout: 5000, connectTimeout: 5000, oauth });

    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    const deadline = Date.now() + 3000;
    while (Date.now() < deadline && !getAuth) await new Promise((r) => setTimeout(r, 20));
    assert.equal(getAuth, 'Bearer custom-static', 'a preset Authorization header must not be overwritten by OAuth');
  });

  it('emits parse-error for a malformed SSE data payload', { timeout: 5000 }, async () => {
    server = createStreamingMcpServer({ onStream: (res) => { openStreams.push(res); res.write('event: message\ndata: {not valid json\n\n'); } });
    port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });

    const got = new Promise<string>((resolve) => client.on('parse-error', (p: string) => resolve(p)));
    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    let timer: ReturnType<typeof setTimeout>;
    const to = new Promise<never>((_, rej) => { timer = setTimeout(() => rej(new Error('no parse-error emitted')), 3000); });
    const payload = await Promise.race([got, to]).finally(() => clearTimeout(timer));
    assert.match(payload, /not valid json/);
  });

  it('ignores SSE events without a data line and parses "data:" without a space', { timeout: 5000 }, async () => {
    server = createStreamingMcpServer({
      onStream: (res) => {
        openStreams.push(res);
        res.write(': keep-alive comment\n\n');        // no data: line → dataParts empty, ignored
        res.write('event: ping\n\n');                  // still no data: line, ignored
        res.write('data:' + JSON.stringify({ jsonrpc: '2.0', method: 'notifications/message', params: { level: 'info', data: 'nospace' } }) + '\n\n');
      },
    });
    port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });

    const got = new Promise<{ data?: string }>((resolve) => client.on('notification', (m: string, p: { data?: string } | undefined) => { if (m === 'notifications/message') resolve(p ?? {}); }));
    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    let timer: ReturnType<typeof setTimeout>;
    const to = new Promise<never>((_, rej) => { timer = setTimeout(() => rej(new Error('no-space data not routed')), 3000); });
    const params = await Promise.race([got, to]).finally(() => clearTimeout(timer));
    assert.equal(params.data, 'nospace');
  });

  it('reconnects the stream after a non-200/405 GET status', { timeout: 6000 }, async () => {
    let getCount = 0;
    server = http.createServer((req, res) => {
      if (req.method === 'GET') {
        getCount++;
        if (getCount === 1) { res.writeHead(500); res.end(); return; } // unexpected status → reconnect
        res.writeHead(200, { 'Content-Type': 'text/event-stream' });
        openStreams.push(res);
        return;
      }
      let body = ''; req.on('data', (c) => { body += c; });
      req.on('end', () => { respondInitOrAck(body ? JSON.parse(body) : {}, res); });
    });
    port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });
    client.on('error', () => { /* ignore transient */ });

    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    const deadline = Date.now() + 5000;
    while (Date.now() < deadline && getCount < 2) await new Promise((r) => setTimeout(r, 50));
    assert.ok(getCount >= 2, `expected a reconnect after HTTP 500, saw ${getCount} GET(s)`);
  });

  it('emits http-stream-closed when reconnect attempts are exhausted', { timeout: 5000 }, async () => {
    server = createStreamingMcpServer({ getStatus: 405 }); // degrade to POST-only, stays ready + open
    port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });

    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    const closed = new Promise<void>((resolve) => client.on('http-stream-closed', () => resolve()));
    // Push the attempt counter to the ceiling so the next schedule gives up.
    const c = client as unknown as { httpStreamReconnectAttempts: number; maxReconnectAttempts: number; scheduleHttpStreamReconnect: () => void };
    c.httpStreamReconnectAttempts = c.maxReconnectAttempts;
    c.scheduleHttpStreamReconnect();
    let timer: ReturnType<typeof setTimeout>;
    const to = new Promise<never>((_, rej) => { timer = setTimeout(() => rej(new Error('http-stream-closed not emitted')), 2000); });
    await Promise.race([closed, to]).finally(() => clearTimeout(timer));
  });

  it('tears down the GET stream on disconnect and does not reconnect after', { timeout: 5000 }, async () => {
    let getCount = 0;
    server = http.createServer((req, res) => {
      if (req.method === 'GET') { getCount++; res.writeHead(200, { 'Content-Type': 'text/event-stream' }); openStreams.push(res); return; }
      let body = ''; req.on('data', (c) => { body += c; });
      req.on('end', () => { respondInitOrAck(body ? JSON.parse(body) : {}, res); });
    });
    port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });
    client.on('error', () => { /* ignore */ });

    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    const deadline = Date.now() + 3000;
    while (Date.now() < deadline && getCount < 1) await new Promise((r) => setTimeout(r, 20));
    assert.ok(getCount >= 1, 'the GET stream should have opened');

    client.disconnect();
    assert.equal((client as unknown as { httpStreamReq: unknown }).httpStreamReq, null, 'disconnect must null the stream request');
    const afterDisconnect = getCount;
    await new Promise((r) => setTimeout(r, 300));
    assert.equal(getCount, afterDisconnect, 'no reconnect should occur after disconnect');
  });

  it('openHttpStream is a no-op once the stream is closed', { timeout: 5000 }, async () => {
    server = createStreamingMcpServer({});
    port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });

    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    client.disconnect(); // sets httpStreamClosed = true
    await assert.doesNotReject(() => (client as unknown as { openHttpStream: () => Promise<void> }).openHttpStream());
  });

  it('aborts opening the GET stream if disconnected while building headers', { timeout: 5000 }, async () => {
    let getCount = 0;
    server = http.createServer((req, res) => {
      if (req.method === 'GET') { getCount++; res.writeHead(200, { 'Content-Type': 'text/event-stream' }); openStreams.push(res); return; }
      let body = ''; req.on('data', (c) => { body += c; });
      req.on('end', () => { respondInitOrAck(body ? JSON.parse(body) : {}, res); });
    });
    port = await listenOnRandom(server);
    // A slow OAuth header keeps openHttpStream suspended in buildStreamHeaders
    // until after we disconnect — exercising the post-await httpStreamClosed guard.
    const oauth = { authorizationHeader: async () => { await new Promise((r) => setTimeout(r, 40)); return 'Bearer slow'; }, onUnauthorized: async () => false };
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000, oauth });

    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    client.disconnect(); // flips httpStreamClosed before the slow header resolves
    await new Promise((r) => setTimeout(r, 150));
    assert.equal(getCount, 0, 'the GET stream must not open once disconnected mid-build');
  });
});
