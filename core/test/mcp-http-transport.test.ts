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
