import { describe, it, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { McpClient, McpSessionExpiredError } from '../src/mcp/client';

after(() => { setTimeout(() => process.exit(0), 200); });

// ---------------------------------------------------------------------------
// SUPERINFERENCE_MCP_SHELL_PREFIX — a stdio MCP server can be launched THROUGH a
// wrapper command so a sandbox/shim mediates it. When the env var is set, the
// client must spawn the PREFIX as the executable and pass `command arg1 arg2 …`
// as a single argument (parity with the reference's CLAUDE_CODE_SHELL_PREFIX).
// ---------------------------------------------------------------------------

// A wrapper that behaves as a minimal MCP stdio server and echoes back the single
// argument it received (the joined real command line) plus its argv length, so
// the test can prove the prefix was used and the cmdline was a single arg.
const wrapperScript = `#!/usr/bin/env node
const readline = require('readline');
const rl = readline.createInterface({ input: process.stdin });
const cmdline = process.argv[2] === undefined ? null : process.argv[2];
rl.on('line', (line) => {
  try {
    const msg = JSON.parse(line);
    const send = (result) => process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result }) + '\\n');
    if (msg.method === 'initialize') {
      send({ protocolVersion: '2024-11-05', serverInfo: { name: 'wrap', version: '1' }, capabilities: {} });
    } else if (msg.method === 'tools/call') {
      send({ content: [{ type: 'text', text: JSON.stringify({ cmdline, argc: process.argv.length }) }] });
    }
  } catch { /* ignore */ }
});
`;

describe('SUPERINFERENCE_MCP_SHELL_PREFIX stdio launch wrapper', () => {
  const saved = process.env.SUPERINFERENCE_MCP_SHELL_PREFIX;
  let tmpDir: string | null = null;

  afterEach(() => {
    if (saved === undefined) delete process.env.SUPERINFERENCE_MCP_SHELL_PREFIX;
    else process.env.SUPERINFERENCE_MCP_SHELL_PREFIX = saved;
    if (tmpDir) { try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* ignore */ } tmpDir = null; }
  });

  it('runs the server through the prefix with the command line as one argument', async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-prefix-'));
    const wrapper = path.join(tmpDir, 'wrapper.js');
    fs.writeFileSync(wrapper, wrapperScript, { mode: 0o755 });
    process.env.SUPERINFERENCE_MCP_SHELL_PREFIX = wrapper;

    // `nonexistent-mcp-cmd` would ENOENT if spawned directly; the prefix wrapper
    // intercepts it, so connect succeeds only when the wrapper is actually used.
    const client = new McpClient({
      command: 'nonexistent-mcp-cmd',
      args: ['alpha', 'beta'],
      requestTimeout: 5000,
      connectTimeout: 5000,
    });
    try {
      await client.connect();
      const res = await client.callTool('echo', {}) as { content: Array<{ type: string; text: string }> };
      const seen = JSON.parse(res.content[0].text) as { cmdline: string; argc: number };
      assert.equal(seen.cmdline, 'nonexistent-mcp-cmd alpha beta', 'the full command line is passed as one argument');
      assert.equal(seen.argc, 3, 'node, wrapper, single cmdline arg');
    } finally {
      client.disconnect();
    }
  });

  it('spawns the command verbatim when no prefix is set', async () => {
    delete process.env.SUPERINFERENCE_MCP_SHELL_PREFIX;
    const serverScript = `
      const readline = require('readline');
      const rl = readline.createInterface({ input: process.stdin });
      rl.on('line', (line) => {
        try {
          const msg = JSON.parse(line);
          const send = (result) => process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result }) + '\\n');
          if (msg.method === 'initialize') send({ protocolVersion: '2024-11-05', serverInfo: { name: 's', version: '1' }, capabilities: {} });
          else if (msg.method === 'tools/call') send({ content: [{ type: 'text', text: JSON.stringify({ argc: process.argv.length }) }] });
        } catch { /* ignore */ }
      });
    `;
    const client = new McpClient({ command: 'node', args: ['-e', serverScript], requestTimeout: 5000, connectTimeout: 5000 });
    try {
      await client.connect();
      const res = await client.callTool('echo', {}) as { content: Array<{ type: string; text: string }> };
      const seen = JSON.parse(res.content[0].text) as { argc: number };
      // `node -e <script>` leaves argv as [execPath] only (no script-file / wrapper
      // arg layer) — proving the command was spawned verbatim, not through a prefix.
      assert.equal(seen.argc, 1, 'direct spawn: no prefix wrapper arg');
    } finally {
      client.disconnect();
    }
  });
});

// ---------------------------------------------------------------------------
// -32000 'Connection closed' on the HTTP transport must be classified as a
// session-expiry (typed McpSessionExpiredError + 'session-expired' event) so the
// manager's transparent single-retry recovers a routine Streamable-HTTP session
// rotation instead of surfacing a hard failure.
// ---------------------------------------------------------------------------

function mockHttpServer(toolError: { code: number; message: string }): http.Server {
  return http.createServer((req, res) => {
    if (req.method === 'GET') { res.writeHead(405); res.end(); return; }
    let body = '';
    req.on('data', (c) => { body += c; });
    req.on('end', () => {
      const msg = JSON.parse(body);
      if (msg.method === 'notifications/initialized') { res.writeHead(202); res.end(); return; }
      if (msg.method === 'initialize') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: '2024-11-05', serverInfo: { name: 'm', version: '1' }, capabilities: { tools: {} } } }));
        return;
      }
      // tools/call → reply with the configured JSON-RPC error
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, error: toolError }));
    });
  });
}

function listen(server: http.Server): Promise<number> {
  return new Promise((resolve) => server.listen(0, '127.0.0.1', () => {
    const addr = server.address();
    resolve(typeof addr === 'object' ? addr!.port : 0);
  }));
}

describe('MCP HTTP -32000 "Connection closed" → session expired', () => {
  let server: http.Server;
  let client: McpClient;

  afterEach(async () => {
    if (client) client.disconnect();
    if (server) await new Promise<void>((r) => server.close(() => r()));
  });

  it('maps -32000 "Connection closed" to McpSessionExpiredError and emits session-expired', async () => {
    server = mockHttpServer({ code: -32000, message: 'Connection closed' });
    const port = await listen(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });
    await client.connectHTTP(`http://127.0.0.1:${port}/`);

    let expired = false;
    client.on('session-expired', () => { expired = true; });
    await assert.rejects(() => client.callTool('x', {}), (e) => e instanceof McpSessionExpiredError);
    assert.ok(expired, 'session-expired event fired so the manager invalidates cache + retries');
  });

  it('is case-insensitive on the message', async () => {
    server = mockHttpServer({ code: -32000, message: 'connection CLOSED by peer' });
    const port = await listen(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });
    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    await assert.rejects(() => client.callTool('x', {}), (e) => e instanceof McpSessionExpiredError);
  });

  it('does NOT misclassify an unrelated -32000 error', async () => {
    server = mockHttpServer({ code: -32000, message: 'Internal server error' });
    const port = await listen(server);
    client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });
    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    await assert.rejects(
      () => client.callTool('x', {}),
      (e) => e instanceof Error && !(e instanceof McpSessionExpiredError) && /MCP error -32000/.test(e.message),
    );
  });
});
