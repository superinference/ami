/**
 * mcp-client-bounded-call-wiring.test.ts — end-to-end proof that the MCP client's
 * initialize handshake is bounded by the shared `boundedCall` guard (UNS004).
 *
 * The in-process connect path builds the handshake promise via
 * `boundedCall(() => this.performInitialize(), { timeoutMs, message })`. Driving a
 * transport that NEVER answers the initialize request exercises the timeout arm of
 * that guard end-to-end — connectInProcess rejects with the client's descriptive
 * message and tears the client down. This is the wiring/integration test for the
 * `boundedCall` helper (its unit-level guarantees live in bounded-call.test.ts).
 *
 * RED-GREEN: with the handshake bounded by boundedCall this rejects promptly with the
 * timeout message; without any bound (the un-wired state) connectInProcess would hang
 * forever and the test would time out instead of asserting the message.
 *
 * ESM-SAFE: a pure in-memory silent transport + a real McpClient (no spawn, no network).
 */

import { describe, it, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { McpClient } from '../src/mcp/client';
import type { InProcessTransport } from '../src/mcp/in-process-transport';

after(() => { setTimeout(() => process.exit(0), 200); });

const liveClients: McpClient[] = [];
afterEach(() => {
  while (liveClients.length) {
    try { liveClients.pop()!.disconnect(); } catch { /* ignore */ }
  }
});

/**
 * A transport that starts cleanly but never routes a response — the initialize
 * request issued by performInitialize() stays pending, so only the boundedCall
 * timeout can settle connectInProcess.
 */
function silentTransport(): InProcessTransport {
  let closed = false;
  return {
    onmessage: undefined,
    onclose: undefined,
    onerror: undefined,
    get closed() { return closed; },
    async start() { /* ready immediately */ },
    async send() { /* swallow the initialize request; never reply */ },
    async close() { closed = true; this.onclose?.(); },
  };
}

describe('McpClient.connectInProcess — bounded by boundedCall', () => {
  it('rejects with the timeout message when the handshake never completes', async () => {
    const client = new McpClient({ command: '', args: [], connectTimeout: 40 });
    client.on('error', () => { /* ignore */ });
    liveClients.push(client);
    const started = Date.now();
    await assert.rejects(
      () => client.connectInProcess(silentTransport()),
      /MCP connection timed out after 40ms/,
    );
    // Proves the bound actually fired (did not hang) and did so around the deadline.
    assert.ok(Date.now() - started < 5000, 'connect must reject at the bound, not hang');
    // The timed-out handshake flips to error then tears the client down (disconnect),
    // so the client ends in a terminal non-ready state rather than left connecting.
    assert.ok(['error', 'disconnected'].includes(client.state), `expected terminal state, got ${client.state}`);
  });
});
