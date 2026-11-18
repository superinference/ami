import { describe, it, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';
import {
  McpClient,
  getMcpConnectTimeout,
  getMcpUserAgent,
  DEFAULT_MCP_CONNECT_TIMEOUT_MS,
} from '../src/mcp/client';
import { McpManager } from '../src/mcp/manager';
import { AMI_CLIENT_NAME, AMI_VERSION } from '../src/version';

after(() => { setTimeout(() => process.exit(0), 200); });

// A stdio MCP server that completes the handshake and lists a 'slow' tool but
// never answers tools/call or resources/read — so a call times out.
const stallServer = `
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
      }
      // tools/call and resources/read are intentionally never answered.
    } catch {}
  });
`;

// A stdio MCP server that never answers anything (including initialize) — so
// connect() hits the connection timeout.
const silentServer = `
  const readline = require('readline');
  readline.createInterface({ input: process.stdin }).on('line', () => {});
`;

// ---------------------------------------------------------------------------
// GAP U041-85: MCP_TIMEOUT connection-timeout env override (parity with Claude's
// getConnectionTimeoutMs).
// ---------------------------------------------------------------------------
describe('getMcpConnectTimeout (MCP_TIMEOUT env override)', () => {
  afterEach(() => { delete process.env.MCP_TIMEOUT; });

  it('defaults to DEFAULT_MCP_CONNECT_TIMEOUT_MS when MCP_TIMEOUT is unset', () => {
    delete process.env.MCP_TIMEOUT;
    assert.equal(getMcpConnectTimeout(), DEFAULT_MCP_CONNECT_TIMEOUT_MS);
    assert.equal(DEFAULT_MCP_CONNECT_TIMEOUT_MS, 10000);
  });

  it('reads a positive integer MCP_TIMEOUT', () => {
    process.env.MCP_TIMEOUT = '45000';
    assert.equal(getMcpConnectTimeout(), 45000);
  });

  it('falls back on non-numeric / zero / negative values', () => {
    process.env.MCP_TIMEOUT = 'nope';
    assert.equal(getMcpConnectTimeout(), DEFAULT_MCP_CONNECT_TIMEOUT_MS);
    process.env.MCP_TIMEOUT = '0';
    assert.equal(getMcpConnectTimeout(), DEFAULT_MCP_CONNECT_TIMEOUT_MS);
    process.env.MCP_TIMEOUT = '-5';
    assert.equal(getMcpConnectTimeout(), DEFAULT_MCP_CONNECT_TIMEOUT_MS);
  });

  it('threads MCP_TIMEOUT into the client connect timeout end-to-end', async () => {
    process.env.MCP_TIMEOUT = '60';
    const client = new McpClient({ command: 'node', args: ['-e', silentServer] });
    try {
      await assert.rejects(
        () => client.connect(),
        /MCP connection timed out after 60ms/,
      );
    } finally {
      client.disconnect();
    }
  });

  it('an explicit connectTimeout option still overrides MCP_TIMEOUT', async () => {
    process.env.MCP_TIMEOUT = '60000';
    const client = new McpClient({ command: 'node', args: ['-e', silentServer], connectTimeout: 70 });
    try {
      await assert.rejects(
        () => client.connect(),
        /MCP connection timed out after 70ms/,
      );
    } finally {
      client.disconnect();
    }
  });

  it('is honored through the McpManager default (end-to-end)', async () => {
    process.env.MCP_TIMEOUT = '70';
    const mgr = new McpManager();
    mgr.addServer('silent', { command: 'node', args: ['-e', silentServer] });
    try {
      await assert.rejects(
        () => mgr.connectServer('silent'),
        /MCP connection timed out after 70ms/,
      );
    } finally {
      await mgr.disconnectAll();
    }
  });
});

// ---------------------------------------------------------------------------
// GAP U041-100/101: self-identifying User-Agent on MCP HTTP/SSE transport.
// ---------------------------------------------------------------------------
describe('getMcpUserAgent + HTTP transport User-Agent header', () => {
  const servers: http.Server[] = [];
  after(() => { for (const s of servers) s.close(); });

  function listen(server: http.Server): Promise<number> {
    servers.push(server);
    return new Promise((resolve) => {
      server.listen(0, '127.0.0.1', () => {
        resolve((server.address() as import('net').AddressInfo).port);
      });
    });
  }

  it('returns a self-identifying ami/<version> UA string', () => {
    // getMcpUserAgent is surface-aware (appends the originating entrypoint / SDK
    // tags from the ambient env — see version.ts amiUserAgent); it always begins
    // with the single-source ami/<version> leaf regardless of any surface suffix.
    const ua = getMcpUserAgent();
    assert.ok(
      ua === `${AMI_CLIENT_NAME}/${AMI_VERSION}` ||
        ua.startsWith(`${AMI_CLIENT_NAME}/${AMI_VERSION} (`),
      `unexpected MCP UA: ${ua}`,
    );
    assert.match(ua, /^ami\/\d+\.\d+\.\d+/);
  });

  it('sends the default User-Agent on the initialize POST', async () => {
    let seenUA: string | undefined;
    const server = http.createServer((req, res) => {
      if (req.method === 'GET') { res.writeHead(405); res.end(); return; }
      seenUA = req.headers['user-agent'];
      let body = '';
      req.on('data', (c) => { body += c; });
      req.on('end', () => {
        const msg = JSON.parse(body);
        if (msg.method === 'notifications/initialized') { res.writeHead(202); res.end(); return; }
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: '2024-11-05', serverInfo: { name: 'h', version: '1' }, capabilities: {} } }));
      });
    });
    const port = await listen(server);
    const client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });
    try {
      await client.connectHTTP(`http://127.0.0.1:${port}/mcp`);
      assert.equal(seenUA, getMcpUserAgent());
    } finally {
      client.disconnect();
    }
  });

  it('lets a caller-supplied User-Agent header override the default', async () => {
    let seenUA: string | undefined;
    const server = http.createServer((req, res) => {
      if (req.method === 'GET') { res.writeHead(405); res.end(); return; }
      seenUA = req.headers['user-agent'];
      let body = '';
      req.on('data', (c) => { body += c; });
      req.on('end', () => {
        const msg = JSON.parse(body);
        if (msg.method === 'notifications/initialized') { res.writeHead(202); res.end(); return; }
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: '2024-11-05', serverInfo: { name: 'h', version: '1' }, capabilities: {} } }));
      });
    });
    const port = await listen(server);
    const client = new McpClient({ command: '', headers: { 'User-Agent': 'custom-agent/9' }, requestTimeout: 5000, connectTimeout: 5000 });
    try {
      await client.connectHTTP(`http://127.0.0.1:${port}/mcp`);
      assert.equal(seenUA, 'custom-agent/9');
    } finally {
      client.disconnect();
    }
  });
});

// ---------------------------------------------------------------------------
// GAP U041-28: tool-call timeout error names the server + tool and uses seconds.
// ---------------------------------------------------------------------------
describe('tool-call timeout message (server + tool + seconds)', () => {
  it('names the server and tool and uses whole seconds for a tools/call timeout', async () => {
    const client = new McpClient({ command: 'node', args: ['-e', stallServer], requestTimeout: 5000 });
    try {
      await client.connect();
      await client.listTools();
      await assert.rejects(
        () => client.callTool('slow', {}, { timeoutMs: 200 }),
        (err: Error) => {
          assert.match(err.message, /^MCP server "s" tool "slow" timed out after \d+s$/);
          return true;
        },
      );
    } finally {
      client.disconnect();
    }
  });

  it('keeps the generic method/ms form for a non-tool request timeout', async () => {
    const client = new McpClient({ command: 'node', args: ['-e', stallServer], requestTimeout: 150, connectTimeout: 5000 });
    try {
      await client.connect();
      await assert.rejects(
        () => client.readResource('res://x'),
        /MCP request 'resources\/read' timed out after 150ms/,
      );
    } finally {
      client.disconnect();
    }
  });
});
