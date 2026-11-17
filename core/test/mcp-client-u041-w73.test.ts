/**
 * U041 (wave w73) — additive MCP client parity: autoReconnect opt-out, WebSocket
 * keep-alive ping (configurable interval + env suppression gate), WebSocket TLS
 * connect-options for `wss://`, and the `notifications/elicitation/complete`
 * out-of-band completion notification.
 *
 * Gaps implemented:
 *  - autoReconnect option flag (parity: WebSocketTransportOptions.autoReconnect) —
 *    reconnect() fails fast when a caller owns recovery.
 *  - Configurable WS keep-alive + AMI_REMOTE suppression gate (parity:
 *    DEFAULT_KEEPALIVE_INTERVAL + isEnvTruthy(CLAUDE_CODE_REMOTE)).
 *  - WebSocket TLS ConnectionOptions for wss:// (parity: getWebSocketTLSOptions)
 *    reusing AMI's single mTLS source.
 *  - Elicitation completion notification (parity:
 *    ElicitationCompleteNotificationSchema) → `elicitation-complete` event.
 *
 * ESM-SAFE: no source-text reads, no namespace monkeypatching; the WebSocket is
 * injected via the documented `webSocketFactory` seam.
 */

import { describe, it, afterEach, after } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  McpClient,
  MCP_WS_OPEN,
  resolveMcpWebSocketTlsOptions,
  getMcpWsKeepaliveIntervalMs,
  isMcpWsKeepaliveSuppressed,
  validateMcpNotificationParams,
  DEFAULT_MCP_WS_KEEPALIVE_INTERVAL_MS,
  type McpWebSocketLike,
  type McpWebSocketTlsOptions,
} from '../src/mcp/client';

after(() => { setTimeout(() => process.exit(0), 200); });

const delay = (ms = 5) => new Promise((r) => setTimeout(r, ms));

/** Minimal in-memory WebSocket implementing {@link McpWebSocketLike} for tests. */
class FakeWebSocket implements McpWebSocketLike {
  readyState = 0; // CONNECTING
  sent: string[] = [];
  /** When set, auto-reply to a sent `ping` request with a JSON-RPC error. */
  errorOnPing = false;
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
  send(data: string): void {
    this.sent.push(data);
    if (this.errorOnPing) {
      const m = JSON.parse(data) as { id?: number; method?: string };
      if (m.method === 'ping' && typeof m.id === 'number') {
        setTimeout(() => this.serverSend({ jsonrpc: '2.0', id: m.id, error: { code: -32000, message: 'dead' } }), 1);
      }
    }
  }
  close(): void { if (this.readyState === 3) return; this.readyState = 3; this.fire('close'); }
  open(): void { this.readyState = MCP_WS_OPEN; this.fire('open'); }
  serverSend(obj: unknown): void {
    this.fire('message', { data: typeof obj === 'string' ? obj : JSON.stringify(obj) });
  }
  parseSent(pred: (m: Record<string, unknown>) => boolean): Record<string, unknown> | undefined {
    for (const s of this.sent) { const m = JSON.parse(s) as Record<string, unknown>; if (pred(m)) return m; }
    return undefined;
  }
}

/** Open a ws client against a fake socket and complete the initialize handshake. */
async function connectFake(opts?: {
  url?: string;
  factory?: (url: string, tls?: McpWebSocketTlsOptions) => McpWebSocketLike;
}): Promise<{ client: McpClient; fake: FakeWebSocket }> {
  const url = opts?.url ?? 'ws://127.0.0.1:9/mcp';
  let fake!: FakeWebSocket;
  const inner = opts?.factory ?? ((u: string) => new FakeWebSocket(u));
  const factory = (u: string, tls?: McpWebSocketTlsOptions) => {
    const sock = inner(u, tls) as FakeWebSocket;
    fake = sock;
    return sock;
  };
  const client = new McpClient({ command: '', webSocketFactory: factory });
  const p = client.connectWebSocket(url);
  await delay();
  fake.open();
  await delay();
  const init = fake.parseSent((m) => m.method === 'initialize')!;
  fake.serverSend({
    jsonrpc: '2.0', id: init.id,
    result: { serverInfo: { name: 't', version: '1' }, capabilities: {}, protocolVersion: '2025-06-18' },
  });
  await p;
  return { client, fake };
}

// ---------------------------------------------------------------------------
// GAP: autoReconnect opt-out flag
// ---------------------------------------------------------------------------
describe('U041 w73 — autoReconnect opt-out', () => {
  it('defaults to true', () => {
    const c = new McpClient({ command: '' });
    assert.equal(c.autoReconnect, true);
  });

  it('reflects an explicit false and makes reconnect() fail fast without retrying', async () => {
    const c = new McpClient({ command: '', autoReconnect: false });
    assert.equal(c.autoReconnect, false);
    const before = Date.now();
    await assert.rejects(c.reconnect(), /automatic reconnection is disabled/);
    // Fails immediately — it never entered the (>=1s) backoff loop.
    assert.ok(Date.now() - before < 500, 'reconnect must not sleep when disabled');
  });

  it('keeps retrying (does not fast-fail) when enabled', async () => {
    // A stdio client with an empty command: reconnect loops + eventually gives up on
    // attempts, which (unlike the disabled path) is NOT the "disabled" error.
    const c = new McpClient({ command: '', connectTimeout: 10 });
    await assert.rejects(c.reconnect(), (e: Error) => !/disabled/.test(e.message));
    c.disconnect();
  });
});

// ---------------------------------------------------------------------------
// GAP: WebSocket TLS ConnectionOptions for wss://
// ---------------------------------------------------------------------------
describe('U041 w73 — resolveMcpWebSocketTlsOptions', () => {
  it('returns undefined for a plaintext ws:// url', () => {
    assert.equal(resolveMcpWebSocketTlsOptions('ws://host/mcp', {}), undefined);
  });

  it('returns a secure protocol floor for a wss:// url with no mTLS material', () => {
    const opts = resolveMcpWebSocketTlsOptions('wss://host/mcp', {})!;
    assert.ok(opts);
    assert.equal(opts.minVersion, 'TLSv1.2');
    assert.equal(opts.cert, undefined);
  });

  it('threads inline mTLS material into the wss:// options', () => {
    const env = {
      AMI_CLIENT_CERT: '-----BEGIN CERTIFICATE-----\nc\n-----END CERTIFICATE-----',
      AMI_CLIENT_KEY: '-----BEGIN PRIVATE KEY-----\nk\n-----END PRIVATE KEY-----',
    } as NodeJS.ProcessEnv;
    const opts = resolveMcpWebSocketTlsOptions('wss://host/mcp', env)!;
    assert.match(opts.cert!, /BEGIN CERTIFICATE/);
    assert.match(opts.key!, /BEGIN PRIVATE KEY/);
    assert.equal(opts.minVersion, 'TLSv1.2');
  });

  it('passes the resolved TLS options to the WebSocket factory for a wss:// connection', async () => {
    let seen: McpWebSocketTlsOptions | undefined;
    const { client } = await connectFake({
      url: 'wss://example.test/mcp',
      factory: (u, tls) => { seen = tls; return new FakeWebSocket(u); },
    });
    try {
      assert.ok(seen, 'factory must receive TLS options for wss://');
      assert.equal(seen!.minVersion, 'TLSv1.2');
    } finally { client.disconnect(); }
  });

  it('passes undefined TLS options to the factory for a plaintext ws:// connection', async () => {
    let seen: McpWebSocketTlsOptions | undefined | 'unset' = 'unset';
    const { client } = await connectFake({
      url: 'ws://example.test/mcp',
      factory: (u, tls) => { seen = tls; return new FakeWebSocket(u); },
    });
    try { assert.equal(seen, undefined); } finally { client.disconnect(); }
  });
});

// ---------------------------------------------------------------------------
// GAP: WebSocket keep-alive ping — interval knob + AMI_REMOTE suppression
// ---------------------------------------------------------------------------
describe('U041 w73 — WebSocket keep-alive knobs', () => {
  afterEach(() => { delete process.env.MCP_WS_KEEPALIVE_INTERVAL_MS; delete process.env.AMI_REMOTE; });

  it('getMcpWsKeepaliveIntervalMs defaults, honors a positive override, rejects invalid', () => {
    delete process.env.MCP_WS_KEEPALIVE_INTERVAL_MS;
    assert.equal(getMcpWsKeepaliveIntervalMs(), DEFAULT_MCP_WS_KEEPALIVE_INTERVAL_MS);
    assert.equal(DEFAULT_MCP_WS_KEEPALIVE_INTERVAL_MS, 300000);
    process.env.MCP_WS_KEEPALIVE_INTERVAL_MS = '50';
    assert.equal(getMcpWsKeepaliveIntervalMs(), 50);
    process.env.MCP_WS_KEEPALIVE_INTERVAL_MS = '0';
    assert.equal(getMcpWsKeepaliveIntervalMs(), DEFAULT_MCP_WS_KEEPALIVE_INTERVAL_MS);
    process.env.MCP_WS_KEEPALIVE_INTERVAL_MS = 'bad';
    assert.equal(getMcpWsKeepaliveIntervalMs(), DEFAULT_MCP_WS_KEEPALIVE_INTERVAL_MS);
  });

  it('isMcpWsKeepaliveSuppressed recognizes truthy/falsy AMI_REMOTE', () => {
    for (const v of ['1', 'true', 'YES', 'On']) assert.equal(isMcpWsKeepaliveSuppressed({ AMI_REMOTE: v } as NodeJS.ProcessEnv), true, v);
    for (const v of ['0', 'false', '', 'nope']) assert.equal(isMcpWsKeepaliveSuppressed({ AMI_REMOTE: v } as NodeJS.ProcessEnv), false, v);
    assert.equal(isMcpWsKeepaliveSuppressed({}), false);
  });

  it('sends periodic keep-alive pings over a live WebSocket', async () => {
    process.env.MCP_WS_KEEPALIVE_INTERVAL_MS = '15';
    const { client, fake } = await connectFake();
    try {
      await delay(60);
      const ping = fake.parseSent((m) => m.method === 'ping');
      assert.ok(ping, 'at least one keep-alive ping must have been sent');
    } finally { client.disconnect(); }
  });

  it('suppresses the keep-alive ping when AMI_REMOTE is set', async () => {
    process.env.MCP_WS_KEEPALIVE_INTERVAL_MS = '15';
    process.env.AMI_REMOTE = '1';
    const { client, fake } = await connectFake();
    try {
      await delay(60);
      assert.equal(fake.parseSent((m) => m.method === 'ping'), undefined, 'no ping when suppressed');
    } finally { client.disconnect(); }
  });

  it('tears down the socket when a keep-alive ping fails', async () => {
    process.env.MCP_WS_KEEPALIVE_INTERVAL_MS = '15';
    const { client, fake } = await connectFake();
    fake.errorOnPing = true;
    let closed = false;
    client.on('close', () => { closed = true; });
    try {
      await delay(80);
      assert.ok(closed, 'a failed keep-alive ping must close the socket → close event');
      assert.equal(client.state, 'disconnected');
    } finally { client.disconnect(); }
  });
});

// ---------------------------------------------------------------------------
// GAP: notifications/elicitation/complete → elicitation-complete event
// ---------------------------------------------------------------------------
describe('U041 w73 — elicitation completion notification', () => {
  it('validateMcpNotificationParams enforces a non-empty string elicitationId', () => {
    assert.deepEqual(validateMcpNotificationParams('notifications/elicitation/complete', { elicitationId: 'e1' }), { ok: true });
    const bad1 = validateMcpNotificationParams('notifications/elicitation/complete', { elicitationId: '' });
    assert.equal(bad1.ok, false);
    const bad2 = validateMcpNotificationParams('notifications/elicitation/complete', { elicitationId: 7 });
    assert.equal(bad2.ok, false);
    const bad3 = validateMcpNotificationParams('notifications/elicitation/complete', null);
    assert.equal(bad3.ok, false);
  });

  it('emits elicitation-complete with the id when the server confirms completion', async () => {
    const { client, fake } = await connectFake();
    const ids: string[] = [];
    client.on('elicitation-complete', (id: string) => ids.push(id));
    try {
      fake.serverSend({ jsonrpc: '2.0', method: 'notifications/elicitation/complete', params: { elicitationId: 'abc-123' } });
      await delay();
      assert.deepEqual(ids, ['abc-123']);
    } finally { client.disconnect(); }
  });

  it('drops a malformed completion notification as a notification-error (no event)', async () => {
    const { client, fake } = await connectFake();
    const ids: string[] = [];
    const errs: string[] = [];
    client.on('elicitation-complete', (id: string) => ids.push(id));
    client.on('notification-error', (method: string) => errs.push(method));
    try {
      fake.serverSend({ jsonrpc: '2.0', method: 'notifications/elicitation/complete', params: { elicitationId: '' } });
      await delay();
      assert.equal(ids.length, 0, 'no event for a malformed notification');
      assert.deepEqual(errs, ['notifications/elicitation/complete']);
    } finally { client.disconnect(); }
  });
});
