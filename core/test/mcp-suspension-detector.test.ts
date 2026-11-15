/**
 * Process-suspension detector in the MCP health-check loop.
 *
 * `setInterval` coalesces the ticks it missed while the host process was paused
 * (laptop sleep / VM suspend / container freeze) into a single late fire. When a
 * health-check tick arrives far later than its interval, the manager concludes the
 * process was suspended and force-reconnects every ready REMOTE (http/sse) client
 * immediately, WITHOUT trusting a ping (which can falsely succeed on a dead-NAT
 * socket). Local (stdio/container) servers resume with us and are pinged normally.
 *
 * ESM-SAFE: no source-text reads, no namespace monkeypatching, no __dirname
 * coupling. The tick body (`_runHealthCheckTick`) is clock-injectable, so the
 * suspension gap is driven deterministically without real timers. Every assertion
 * is on observable behavior (scheduled reconnect timers, ping calls, the emitted
 * `suspension-detected` event), so each test is red-green meaningful.
 */

import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { McpManager, MCP_SUSPENSION_TICK_FACTOR } from '../src/mcp/manager';

interface FakeClient {
  state: string;
  pings: number;
  ping(): Promise<void>;
  reconnect(): Promise<void>;
}

function fakeClient(state = 'ready'): FakeClient {
  return {
    state,
    pings: 0,
    async ping() { this.pings++; },
    async reconnect() { /* no-op: scheduleReconnect sets a timer we just inspect */ },
  };
}

/** Register a fake client + its transport config directly (no real connect). */
function seed(mgr: McpManager, name: string, transport: 'stdio' | 'http' | 'sse', state = 'ready'): FakeClient {
  const client = fakeClient(state);
  (mgr as any).clients.set(name, client);
  (mgr as any).configs.set(name, { command: 'x', transport });
  return client;
}

function reconnectTimers(mgr: McpManager): Map<string, unknown> {
  return (mgr as any).reconnectTimers;
}

describe('MCP health-check process-suspension detector', () => {
  const managers: McpManager[] = [];
  const track = (m: McpManager) => { managers.push(m); return m; };
  afterEach(() => {
    for (const m of managers.splice(0)) {
      try { m.disconnectAll(); } catch { /* ignore */ }
    }
  });

  it('a normal-cadence tick pings remote clients and schedules no reconnect', async () => {
    const mgr = track(new McpManager());
    const remote = seed(mgr, 'remote', 'http');
    const interval = 1000;
    const t0 = 10_000;
    await mgr._runHealthCheckTick(interval, t0);                 // seeds last tick
    await mgr._runHealthCheckTick(interval, t0 + interval);      // on-cadence gap
    assert.equal(remote.pings, 2, 'remote client is pinged on normal ticks');
    assert.equal(reconnectTimers(mgr).size, 0, 'no reconnect scheduled on a normal tick');
  });

  it('a coalesced (suspension) tick force-reconnects remote clients WITHOUT pinging', async () => {
    const mgr = track(new McpManager());
    const remote = seed(mgr, 'remote', 'http');
    const sse = seed(mgr, 'streamer', 'sse');
    const interval = 1000;
    const t0 = 10_000;
    const emitted: Array<[number, string[]]> = [];
    mgr.on('suspension-detected', (gap: number, names: string[]) => emitted.push([gap, names]));

    await mgr._runHealthCheckTick(interval, t0);
    const pingsBefore = remote.pings + sse.pings;
    // A tick arriving far later than interval × factor => suspension.
    const gap = interval * MCP_SUSPENSION_TICK_FACTOR + interval + 5_000;
    await mgr._runHealthCheckTick(interval, t0 + gap);

    assert.equal(remote.pings + sse.pings, pingsBefore,
      'remote clients are NOT pinged on a suspension tick (a dead-NAT ping can falsely succeed)');
    assert.ok(reconnectTimers(mgr).has('remote'), 'http client force-reconnected');
    assert.ok(reconnectTimers(mgr).has('streamer'), 'sse client force-reconnected');
    assert.equal(emitted.length, 1, 'suspension-detected emitted exactly once');
    assert.equal(emitted[0][0], gap, 'event carries the measured tick gap');
    assert.deepEqual(emitted[0][1].sort(), ['remote', 'streamer'], 'event lists the reconnected servers');
  });

  it('on a suspension tick, local (stdio) servers are pinged, not force-reconnected', async () => {
    const mgr = track(new McpManager());
    const local = seed(mgr, 'local', 'stdio');
    const interval = 1000;
    const t0 = 10_000;
    await mgr._runHealthCheckTick(interval, t0);
    await mgr._runHealthCheckTick(interval, t0 + interval * MCP_SUSPENSION_TICK_FACTOR + 10_000);
    assert.equal(local.pings, 2, 'stdio server is pinged on every tick (it was suspended with us)');
    assert.ok(!reconnectTimers(mgr).has('local'), 'stdio server is NOT force-reconnected on suspension');
  });

  it('non-ready clients are skipped entirely on a suspension tick', async () => {
    const mgr = track(new McpManager());
    const connecting = seed(mgr, 'warming', 'http', 'connecting');
    const interval = 1000;
    const t0 = 10_000;
    await mgr._runHealthCheckTick(interval, t0);
    await mgr._runHealthCheckTick(interval, t0 + interval * MCP_SUSPENSION_TICK_FACTOR + 10_000);
    assert.equal(connecting.pings, 0, 'a non-ready client is never pinged');
    assert.ok(!reconnectTimers(mgr).has('warming'), 'a non-ready client is not force-reconnected');
  });

  it('a failing ping on a normal tick still schedules a reconnect', async () => {
    const mgr = track(new McpManager());
    const bad = seed(mgr, 'bad', 'http');
    bad.ping = async () => { throw new Error('ping timed out'); };
    const interval = 1000;
    const t0 = 10_000;
    await mgr._runHealthCheckTick(interval, t0);
    await mgr._runHealthCheckTick(interval, t0 + interval);
    assert.ok(reconnectTimers(mgr).has('bad'), 'ping failure on a normal tick triggers reconnect');
  });

  it('startHealthChecks seeds the tick clock so the first on-cadence tick is not a false suspension', async () => {
    const mgr = track(new McpManager());
    const interval = 1000;
    mgr.startHealthChecks(interval);
    const seeded = (mgr as any)._lastHealthTick as number | null;
    assert.notEqual(seeded, null, 'tick clock seeded on start');
    let suspended = false;
    mgr.on('suspension-detected', () => { suspended = true; });
    // Simulate the first real tick arriving on cadence after the seed.
    await mgr._runHealthCheckTick(interval, (seeded as number) + interval);
    mgr.stopHealthChecks();
    assert.equal(suspended, false, 'an on-cadence first tick after the seed is not flagged as suspension');
  });
});
