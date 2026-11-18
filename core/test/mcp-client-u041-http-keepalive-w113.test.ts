/**
 * U041 (wave w113) — additive MCP client parity: PROACTIVE Streamable-HTTP
 * keep-alive DATA frames to defeat proxy idle-timeout RSTs.
 *
 * Gap: Claude Code's WebSocketTransport.startKeepaliveInterval() sends a real DATA
 * frame ({"type":"keep_alive"}) every ~5min because ping/pong control frames (and
 * SSE comment frames) do NOT reset an intermediary proxy's idle timer (e.g.
 * Cloudflare's 5-min window). AMI's persistent MCP Streamable-HTTP transport only
 * RECOVERED from such a drop reactively (GET-stream liveness watchdog +
 * scheduleHttpStreamReconnect). This adds a proactive, activity-gated keepalive:
 * an MCP `ping` REQUEST (a data frame over the POST leg) on a sub-idle-timeout
 * interval, suppressible via AMI_REMOTE when an outer host heartbeat already runs.
 *
 * ESM-SAFE: real http.createServer on port 0; no source-text reads, no namespace
 * monkeypatching. The keepalive interval is injected via the documented
 * `httpStreamKeepaliveInterval` constructor option.
 */

import { describe, it, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';
import {
  McpClient,
  getMcpHttpKeepaliveIntervalMs,
  DEFAULT_MCP_HTTP_KEEPALIVE_INTERVAL_MS,
} from '../src/mcp/client';
import { isSafeEnvVar } from '../src/env-protection';

after(() => { setTimeout(() => process.exit(0), 200); });

const delay = (ms: number) => new Promise((r) => setTimeout(r, ms));

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

/**
 * Handshake over POST; counts `ping` POSTs (the keepalive data frame). The GET
 * stream is accepted (200) and then left SILENT — a long-idle connection the
 * keepalive must warm without the inbound leg resetting the activity clock.
 */
function makeServer(pingMethods: string[], openStreams: http.ServerResponse[]): http.Server {
  return http.createServer((req, res) => {
    if (req.method === 'GET') {
      res.writeHead(200, { 'Content-Type': 'text/event-stream', Connection: 'keep-alive' });
      res.flushHeaders();
      openStreams.push(res);
      return;
    }
    let body = '';
    req.on('data', (c) => { body += c; });
    req.on('end', () => {
      const msg = body ? JSON.parse(body) : {};
      if (msg.method) pingMethods.push(msg.method);
      if (msg.method === 'initialize') {
        res.writeHead(200, { 'Content-Type': 'application/json', 'Mcp-Session-Id': 'sess-w113' });
        res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: '2025-06-18', serverInfo: { name: 'ka-mcp', version: '1.0.0' }, capabilities: { tools: {} } } }));
        return;
      }
      if (typeof msg.method === 'string' && msg.method.startsWith('notifications/')) { res.writeHead(202); res.end(); return; }
      if (msg.method && msg.id != null) { res.writeHead(200, { 'Content-Type': 'application/json' }); res.end(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: {} })); return; }
      res.writeHead(202); res.end();
    });
  });
}

describe('U041 w113 — Streamable-HTTP proactive keep-alive data frames', () => {
  let server: http.Server | undefined;
  let client: McpClient | undefined;
  const openStreams: http.ServerResponse[] = [];

  afterEach(async () => {
    delete process.env.MCP_HTTP_KEEPALIVE_INTERVAL_MS;
    delete process.env.AMI_REMOTE;
    for (const s of openStreams) { try { s.end(); } catch { /* ignore */ } }
    openStreams.length = 0;
    if (client) { client.disconnect(); client = undefined; }
    if (server) { await closeServer(server); server = undefined; }
  });

  it('getMcpHttpKeepaliveIntervalMs defaults, honors a positive override, rejects invalid', () => {
    delete process.env.MCP_HTTP_KEEPALIVE_INTERVAL_MS;
    assert.equal(getMcpHttpKeepaliveIntervalMs(), DEFAULT_MCP_HTTP_KEEPALIVE_INTERVAL_MS);
    assert.equal(DEFAULT_MCP_HTTP_KEEPALIVE_INTERVAL_MS, 300000);
    process.env.MCP_HTTP_KEEPALIVE_INTERVAL_MS = '50';
    assert.equal(getMcpHttpKeepaliveIntervalMs(), 50);
    process.env.MCP_HTTP_KEEPALIVE_INTERVAL_MS = '0';
    assert.equal(getMcpHttpKeepaliveIntervalMs(), DEFAULT_MCP_HTTP_KEEPALIVE_INTERVAL_MS);
    process.env.MCP_HTTP_KEEPALIVE_INTERVAL_MS = 'bad';
    assert.equal(getMcpHttpKeepaliveIntervalMs(), DEFAULT_MCP_HTTP_KEEPALIVE_INTERVAL_MS);
  });

  it('MCP_HTTP_KEEPALIVE_INTERVAL_MS is an allow-listed (safe) env tuning knob', () => {
    assert.equal(isSafeEnvVar('MCP_HTTP_KEEPALIVE_INTERVAL_MS'), true);
  });

  it('sends a periodic keep-alive ping DATA frame over an idle HTTP transport + emits http-stream-keepalive', async () => {
    const seen: string[] = [];
    server = makeServer(seen, openStreams);
    const port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 4000, connectTimeout: 4000, httpStreamLivenessTimeout: 5000, httpStreamKeepaliveInterval: 40 });
    const kaIdle: number[] = [];
    client.on('http-stream-keepalive', (idleMs: number) => kaIdle.push(idleMs));
    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    await delay(160);
    assert.ok(seen.includes('ping'), 'at least one keep-alive ping data frame must have been POSTed');
    assert.ok(kaIdle.length >= 1, 'http-stream-keepalive event must fire');
    assert.ok(kaIdle[0] >= 40, 'the reported idle time must be at least the keepalive interval');
  });

  it('activity-gating: a long interval sends NO keepalive while idle (idle < interval → no-op)', async () => {
    const seen: string[] = [];
    server = makeServer(seen, openStreams);
    const port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 4000, connectTimeout: 4000, httpStreamLivenessTimeout: 5000, httpStreamKeepaliveInterval: 10000 });
    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    await delay(150);
    assert.equal(seen.filter((m) => m === 'ping').length, 0, 'no keepalive ping should fire before the interval elapses');
  });

  it('suppresses the keep-alive entirely when AMI_REMOTE is set (outer host heartbeat)', async () => {
    process.env.AMI_REMOTE = '1';
    const seen: string[] = [];
    server = makeServer(seen, openStreams);
    const port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 4000, connectTimeout: 4000, httpStreamLivenessTimeout: 5000, httpStreamKeepaliveInterval: 40 });
    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    await delay(160);
    assert.equal(seen.filter((m) => m === 'ping').length, 0, 'no keepalive ping when AMI_REMOTE suppresses it');
  });

  it('stops the keep-alive on disconnect (no pings after teardown)', async () => {
    const seen: string[] = [];
    server = makeServer(seen, openStreams);
    const port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 4000, connectTimeout: 4000, httpStreamLivenessTimeout: 5000, httpStreamKeepaliveInterval: 40 });
    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    await delay(100); // let at least one keepalive fire
    client.disconnect();
    const afterDisconnect = seen.filter((m) => m === 'ping').length;
    await delay(160);
    assert.equal(seen.filter((m) => m === 'ping').length, afterDisconnect, 'no further keepalive pings after disconnect');
    client = undefined;
  });

  it('env override tunes the interval when no explicit option is passed', async () => {
    process.env.MCP_HTTP_KEEPALIVE_INTERVAL_MS = '40';
    const seen: string[] = [];
    server = makeServer(seen, openStreams);
    const port = await listenOnRandom(server);
    client = new McpClient({ command: '', requestTimeout: 4000, connectTimeout: 4000, httpStreamLivenessTimeout: 5000 });
    await client.connectHTTP(`http://127.0.0.1:${port}/`);
    await delay(160);
    assert.ok(seen.includes('ping'), 'the env-tuned interval must drive keepalive pings');
  });
});
