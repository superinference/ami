import { describe, it, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';
import {
  McpClient,
  boundMcpResponse,
  getMaxMcpOutputTokens,
  getMaxMcpOutputChars,
  getMcpToolHeartbeatIntervalMs,
  DEFAULT_MAX_MCP_OUTPUT_TOKENS,
  DEFAULT_MCP_TOOL_HEARTBEAT_INTERVAL_MS,
  MCP_CHARS_PER_TOKEN,
} from '../src/mcp/client';
import { findExecutable, whichSync } from '../src/utils/which';

after(() => { setTimeout(() => process.exit(0), 200); });

// ---------------------------------------------------------------------------
// GAP U041-36: token-based MCP output cap (MAX_MCP_OUTPUT_TOKENS + 25000 default)
// ---------------------------------------------------------------------------
describe('getMaxMcpOutputTokens / getMaxMcpOutputChars', () => {
  afterEach(() => { delete process.env.MAX_MCP_OUTPUT_TOKENS; });

  it('defaults to 25000 tokens when the env var is unset', () => {
    delete process.env.MAX_MCP_OUTPUT_TOKENS;
    assert.equal(getMaxMcpOutputTokens(), DEFAULT_MAX_MCP_OUTPUT_TOKENS);
    assert.equal(DEFAULT_MAX_MCP_OUTPUT_TOKENS, 25000);
  });

  it('honors a positive MAX_MCP_OUTPUT_TOKENS override', () => {
    process.env.MAX_MCP_OUTPUT_TOKENS = '1000';
    assert.equal(getMaxMcpOutputTokens(), 1000);
  });

  it('falls back to the default on a non-positive/invalid override', () => {
    process.env.MAX_MCP_OUTPUT_TOKENS = '0';
    assert.equal(getMaxMcpOutputTokens(), DEFAULT_MAX_MCP_OUTPUT_TOKENS);
    process.env.MAX_MCP_OUTPUT_TOKENS = '-5';
    assert.equal(getMaxMcpOutputTokens(), DEFAULT_MAX_MCP_OUTPUT_TOKENS);
    process.env.MAX_MCP_OUTPUT_TOKENS = 'abc';
    assert.equal(getMaxMcpOutputTokens(), DEFAULT_MAX_MCP_OUTPUT_TOKENS);
  });

  it('derives the char budget as tokens x chars-per-token', () => {
    delete process.env.MAX_MCP_OUTPUT_TOKENS;
    assert.equal(getMaxMcpOutputChars(), DEFAULT_MAX_MCP_OUTPUT_TOKENS * MCP_CHARS_PER_TOKEN);
    process.env.MAX_MCP_OUTPUT_TOKENS = '10';
    assert.equal(getMaxMcpOutputChars(), 10 * MCP_CHARS_PER_TOKEN);
  });
});

// ---------------------------------------------------------------------------
// GAP U041-37/78/79: truncation message carries a pagination/filtering hint
// ---------------------------------------------------------------------------
describe('boundMcpResponse truncation message', () => {
  it('appends a pagination/filtering recovery hint when truncating', () => {
    const big = 'z'.repeat(200);
    const r = boundMcpResponse(big, 50);
    assert.equal(r.truncated, true);
    // Keeps the existing byte-budget line (back-compat for existing assertions)...
    assert.match(r.content, /Response truncated: 200 bytes/);
    // ...and adds the actionable pagination/filtering guidance.
    assert.match(r.content, /pagination or filtering tools/);
    assert.match(r.content, /results may be incomplete/);
  });

  it('adds no hint when the content fits under the cap', () => {
    const r = boundMcpResponse('small', 1024);
    assert.equal(r.truncated, false);
    assert.doesNotMatch(r.content, /pagination/);
  });
});

// ---------------------------------------------------------------------------
// GAP U041-31: findExecutable(exe, args) -> { cmd, args }
// ---------------------------------------------------------------------------
describe('findExecutable', () => {
  it('resolves a real executable name to its full PATH path', () => {
    const resolved = whichSync('node');
    const out = findExecutable('node', ['--version']);
    // When node resolves, cmd is its absolute path; else the original name.
    assert.equal(out.cmd, resolved ?? 'node');
    if (resolved) assert.notEqual(out.cmd, 'node');
  });

  it('falls back to the original name when the executable is unresolved', () => {
    const name = 'definitely-not-a-real-binary-xyz-123';
    const out = findExecutable(name, ['a', 'b']);
    assert.equal(out.cmd, name);
  });

  it('passes args through unchanged', () => {
    const args = ['--flag', 'value', ''];
    const out = findExecutable('node', args);
    assert.deepEqual(out.args, args);
  });
});

// ---------------------------------------------------------------------------
// GAP U041-61: self-generated "still running" heartbeat for long tool calls
// ---------------------------------------------------------------------------
describe('getMcpToolHeartbeatIntervalMs', () => {
  afterEach(() => { delete process.env.MCP_TOOL_HEARTBEAT_INTERVAL_MS; });

  it('defaults to 30s and honors a positive override', () => {
    delete process.env.MCP_TOOL_HEARTBEAT_INTERVAL_MS;
    assert.equal(getMcpToolHeartbeatIntervalMs(), DEFAULT_MCP_TOOL_HEARTBEAT_INTERVAL_MS);
    assert.equal(DEFAULT_MCP_TOOL_HEARTBEAT_INTERVAL_MS, 30000);
    process.env.MCP_TOOL_HEARTBEAT_INTERVAL_MS = '25';
    assert.equal(getMcpToolHeartbeatIntervalMs(), 25);
    process.env.MCP_TOOL_HEARTBEAT_INTERVAL_MS = 'bad';
    assert.equal(getMcpToolHeartbeatIntervalMs(), DEFAULT_MCP_TOOL_HEARTBEAT_INTERVAL_MS);
  });
});

describe('tool-heartbeat event during a slow callTool (HTTP transport)', () => {
  const servers: http.Server[] = [];
  after(() => { for (const s of servers) s.close(); delete process.env.MCP_TOOL_HEARTBEAT_INTERVAL_MS; });

  function listen(server: http.Server): Promise<number> {
    servers.push(server);
    return new Promise((resolve) => {
      server.listen(0, '127.0.0.1', () => resolve((server.address() as import('net').AddressInfo).port));
    });
  }

  it('emits tool-heartbeat with elapsed time while the server response is pending', async () => {
    process.env.MCP_TOOL_HEARTBEAT_INTERVAL_MS = '20';
    const server = http.createServer((req, res) => {
      if (req.method === 'GET') { res.writeHead(405); res.end(); return; }
      let body = '';
      req.on('data', (c) => { body += c; });
      req.on('end', () => {
        const msg = JSON.parse(body);
        if (msg.method === 'notifications/initialized') { res.writeHead(202); res.end(); return; }
        if (msg.method === 'tools/call') {
          // Delay the response well past several heartbeat intervals.
          setTimeout(() => {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { content: [{ type: 'text', text: 'ok' }] } }));
          }, 120);
          return;
        }
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: '2024-11-05', serverInfo: { name: 'h', version: '1' }, capabilities: {} } }));
      });
    });
    const port = await listen(server);
    const client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });
    const beats: Array<{ name: string; elapsedMs: number; elapsedSeconds: number }> = [];
    client.on('tool-heartbeat', (b) => beats.push(b));
    try {
      await client.connectHTTP(`http://127.0.0.1:${port}/mcp`);
      const result = await client.callTool('slow', {}, { timeoutMs: 5000 });
      assert.ok(result, 'the call still resolves normally');
      assert.ok(beats.length >= 1, `expected at least one heartbeat, got ${beats.length}`);
      assert.equal(beats[0].name, 'slow');
      assert.ok(beats[0].elapsedMs >= 0);
      assert.equal(typeof beats[0].elapsedSeconds, 'number');
    } finally {
      client.disconnect();
    }
  });

  it('stops heartbeats once the call settles', async () => {
    process.env.MCP_TOOL_HEARTBEAT_INTERVAL_MS = '15';
    const server = http.createServer((req, res) => {
      if (req.method === 'GET') { res.writeHead(405); res.end(); return; }
      let body = '';
      req.on('data', (c) => { body += c; });
      req.on('end', () => {
        const msg = JSON.parse(body);
        if (msg.method === 'notifications/initialized') { res.writeHead(202); res.end(); return; }
        if (msg.method === 'tools/call') {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { content: [] } }));
          return;
        }
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: '2024-11-05', serverInfo: { name: 'h', version: '1' }, capabilities: {} } }));
      });
    });
    const port = await listen(server);
    const client = new McpClient({ command: '', requestTimeout: 5000, connectTimeout: 5000 });
    let beats = 0;
    client.on('tool-heartbeat', () => { beats++; });
    try {
      await client.connectHTTP(`http://127.0.0.1:${port}/mcp`);
      await client.callTool('fast', {}, { timeoutMs: 5000 });
      const settled = beats;
      await new Promise((r) => setTimeout(r, 60));
      assert.equal(beats, settled, 'no heartbeat should fire after the call settled');
    } finally {
      client.disconnect();
    }
  });
});
