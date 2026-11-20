/**
 * mcp-client-u041-reconnect-cache.test.ts — U041 parity additions for
 * src/mcp/client.ts, covering four confirmed gaps:
 *
 *   1. connect() race neutralizes the losing performInitialize promise so a late
 *      rejection cannot surface as an unhandledRejection (parity with the reference's
 *      `void enumP.catch(() => {})`).
 *   2. reconnect() gains a wall-clock give-up budget alongside the attempt cap.
 *   3. reconnect() loop is abortable mid-retry: a disconnect()/stopServer while a
 *      prior attempt is sleeping exits the loop gracefully instead of burning the
 *      whole attempt budget.
 *   4. listResources() serves a warm cache (invalidated on resources/list_changed
 *      and on disconnect) instead of re-fetching on every call.
 *
 * ESM-SAFE by construction: real spawned `node -e` servers, real http servers on
 * port 0, no source-text reads, no namespace monkeypatching. All assertions are
 * on observable behavior (return values, counts, thrown/not-thrown).
 */

import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { McpClient } from '../src/mcp/client';

const liveClients: McpClient[] = [];
afterEach(() => {
  while (liveClients.length) {
    try { liveClients.pop()!.disconnect(); } catch { /* ignore */ }
  }
});
function track(c: McpClient): McpClient { liveClients.push(c); c.on('error', () => {}); return c; }

// A stdio MCP server whose resources/list returns a DIFFERENT uri on each call
// (file:///a then file:///b …) so a test can tell a cache hit (no fetch) from a
// re-fetch. It also emits a resources/list_changed notification whenever it gets
// a `ping`, letting a test drive cache invalidation deterministically.
function resourceServerArgs(): string[] {
  const script = `
    const readline = require('readline');
    const rl = readline.createInterface({ input: process.stdin });
    let rcount = 0;
    rl.on('line', (line) => {
      try {
        const msg = JSON.parse(line);
        if (msg.method === 'initialize') {
          process.stdout.write(JSON.stringify({ jsonrpc:'2.0', id:msg.id, result:{
            protocolVersion:'2024-11-05',
            serverInfo:{ name:'res-server', version:'1.0' },
            capabilities:{ resources:{ listChanged:true } } } }) + '\\n');
        } else if (msg.method === 'resources/list') {
          rcount++;
          const uri = 'file:///' + (rcount === 1 ? 'a' : 'b');
          process.stdout.write(JSON.stringify({ jsonrpc:'2.0', id:msg.id, result:{ resources:[{ uri, name: uri }] } }) + '\\n');
        } else if (msg.method === 'ping') {
          process.stdout.write(JSON.stringify({ jsonrpc:'2.0', id:msg.id, result:{} }) + '\\n');
          process.stdout.write(JSON.stringify({ jsonrpc:'2.0', method:'notifications/resources/list_changed' }) + '\\n');
        }
      } catch {}
    });
    setInterval(() => {}, 60000);
  `;
  return ['-e', script];
}

// NOTE: connect() races performInitialize against a connectTimeout via
// Promise.race, which attaches a rejection reaction to EVERY input promise. A
// late rejection of the losing performInitialize promise is therefore delivered
// to that no-op reaction and never surfaces as an unhandledRejection — safe by
// construction, so no explicit `void connectPromise.catch()` is needed. This test
// locks that safety: it fails if connect() is ever refactored off Promise.race
// without adding an equivalent guard.
describe('U041 connect() race — losing performInitialize promise never leaks', () => {
  it('a late connect-promise rejection does not become an unhandledRejection', async () => {
    const captured: unknown[] = [];
    const onUnhandled = (reason: unknown) => {
      const msg = reason instanceof Error ? reason.message : String(reason);
      if (msg.includes('Client disconnected')) captured.push(reason);
    };
    process.on('unhandledRejection', onUnhandled);
    try {
      // A server that spawns but never answers `initialize` → connect() loses the
      // race to its own connectTimeout, then disconnect() rejects the still-pending
      // initialize request, rejecting the backgrounded performInitialize promise.
      const c = track(new McpClient({
        command: 'node', args: ['-e', 'setInterval(() => {}, 60000)'],
        connectTimeout: 60, requestTimeout: 5000,
      }));
      await assert.rejects(() => c.connect(), /timed out/);
      // Give any late microtask/macrotask rejection time to surface.
      await new Promise((r) => setTimeout(r, 80));
    } finally {
      process.off('unhandledRejection', onUnhandled);
    }
    assert.deepEqual(captured, [], 'losing performInitialize rejection must be swallowed');
  });
});

describe('U041 reconnect() — wall-clock give-up budget', () => {
  it('gives up on the wall-clock budget before exhausting the attempt cap', async () => {
    const c = track(new McpClient({
      command: 'node', args: ['-e', 'process.exit(1)'],
      connectTimeout: 150, requestTimeout: 150,
      reconnectGiveUpMs: 1,
    }));
    // High attempt cap so the ONLY way it can stop is the wall-clock budget.
    (c as unknown as { maxReconnectAttempts: number }).maxReconnectAttempts = 50;
    (c as unknown as { reconnectDelay: (n: number) => number }).reconnectDelay = () => 5;
    await assert.rejects(() => c.reconnect(), /gave up after 1ms/);
  });
});

describe('U041 reconnect() — abortable mid-retry', () => {
  it('a disconnect() while a retry is sleeping exits the loop without exhausting attempts', async () => {
    const c = track(new McpClient({
      command: 'node', args: ['-e', 'process.exit(1)'],
      connectTimeout: 150, requestTimeout: 150,
    }));
    (c as unknown as { maxReconnectAttempts: number }).maxReconnectAttempts = 50;
    (c as unknown as { reconnectDelay: (n: number) => number }).reconnectDelay = () => 40;
    const p = c.reconnect();
    // Land a disconnect during the first backoff sleep.
    setTimeout(() => c.disconnect(), 15);
    await assert.doesNotReject(p, 'an aborted reconnect must resolve, not reject');
    const attempts = (c as unknown as { reconnectAttempts: number }).reconnectAttempts;
    assert.ok(attempts < 50, `loop must abort early, saw ${attempts} attempts`);
  });
});

describe('U041 listResources() — warm cache with invalidation', () => {
  it('serves a cache hit, re-fetches after list_changed, and clears on disconnect', async () => {
    const c = track(new McpClient({ command: 'node', args: resourceServerArgs(), connectTimeout: 5000, requestTimeout: 5000 }));
    await c.connect();

    const r1 = await c.listResources();
    assert.equal(r1[0].uri, 'file:///a');

    // Second call must be a cache hit (no fetch) → still the first uri, not 'b'.
    const r2 = await c.listResources();
    assert.equal(r2[0].uri, 'file:///a', 'second call must hit the cache, not re-fetch');

    // Drive a resources/list_changed notification; it invalidates the cache.
    await c.ping();
    await new Promise((r) => setTimeout(r, 60));

    const r3 = await c.listResources();
    assert.equal(r3[0].uri, 'file:///b', 'list_changed must invalidate the cache → re-fetch');

    // Disconnect clears the cache so a reconnected client re-fetches.
    c.disconnect();
    assert.deepEqual(c.resources, [], 'disconnect must clear the resource cache');
  });
});
