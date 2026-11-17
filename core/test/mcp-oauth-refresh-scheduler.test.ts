/**
 * mcp-oauth-refresh-scheduler.test.ts — proactive pre-expiry token-refresh
 * scheduler (core/core/src/mcp/oauth.ts: OAuthRefreshScheduler) + its wiring into
 * McpOAuthSession (autoRefresh).
 *
 * Covers the gap: proactive pre-expiry token-refresh scheduler with
 *   - generation-counter staleness guard,
 *   - failure-count backoff,
 *   - fallback refresh interval.
 *
 * Uses injected timer seams so timing is deterministic (no real clocks).
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  OAuthRefreshScheduler, McpOAuthSession,
  TOKEN_REFRESH_BUFFER_MS, FALLBACK_REFRESH_INTERVAL_MS, REFRESH_RETRY_DELAY_MS, MAX_REFRESH_FAILURES,
} from '../src/mcp/oauth';
import type { FetchLike } from '../src/mcp/oauth';

/** A controllable fake timer: schedule fns and fire them on demand. */
function fakeTimers() {
  let seq = 0;
  const pending = new Map<number, { fn: () => void; ms: number }>();
  const timers = {
    setTimer: (fn: () => void, ms: number) => { const id = ++seq; pending.set(id, { fn, ms }); return id; },
    clearTimer: (h: unknown) => { pending.delete(h as number); },
    now: () => 10_000,
  };
  return {
    timers,
    pendingCount: () => pending.size,
    lastDelay: () => [...pending.values()].at(-1)?.ms,
    /** Fire the most-recently-scheduled timer (and remove it first, mirroring setTimeout). */
    async fireLatest() {
      const entries = [...pending.entries()];
      const [id, entry] = entries.at(-1)!;
      pending.delete(id);
      entry.fn();
      // Let the async fire() chain settle.
      await new Promise((r) => setImmediate(r));
    },
  };
}

describe('OAuthRefreshScheduler', () => {
  it('schedules a refresh bufferMs before expiry', () => {
    const ft = fakeTimers();
    const sched = new OAuthRefreshScheduler(async () => undefined, 1000, ft.timers);
    sched.schedule(10_000 + 5000); // expiry 5s out, buffer 1s → delay 4000
    assert.equal(ft.lastDelay(), 4000);
    assert.equal(ft.pendingCount(), 1);
  });

  it('clamps a near/past expiry to the floor rather than tight-looping', () => {
    const ft = fakeTimers();
    const sched = new OAuthRefreshScheduler(async () => undefined, 5000, ft.timers);
    sched.schedule(10_000); // already within buffer → floored
    assert.equal(ft.lastDelay(), 1000);
  });

  it('uses the fallback interval when expiry is unknown', () => {
    const ft = fakeTimers();
    const sched = new OAuthRefreshScheduler(async () => undefined, 1000, ft.timers);
    sched.schedule(undefined);
    assert.equal(ft.lastDelay(), FALLBACK_REFRESH_INTERVAL_MS);
  });

  it('re-arms from the expiry returned by a successful refresh', async () => {
    const ft = fakeTimers();
    let calls = 0;
    const sched = new OAuthRefreshScheduler(async () => { calls++; return 10_000 + 20_000; }, 1000, ft.timers);
    sched.schedule(10_000 + 5000);
    await ft.fireLatest();
    assert.equal(calls, 1);
    // Follow-up armed from the new expiry (20s out, 1s buffer → 19000).
    assert.equal(ft.lastDelay(), 19_000);
  });

  it('applies failure-count backoff and gives up after MAX_REFRESH_FAILURES', async () => {
    const ft = fakeTimers();
    let calls = 0;
    const sched = new OAuthRefreshScheduler(async () => { calls++; return undefined; }, 1000, ft.timers);
    sched.schedule(10_000 + 5000);
    // Each failed fire schedules a retry at REFRESH_RETRY_DELAY_MS, up to the cap.
    for (let i = 1; i < MAX_REFRESH_FAILURES; i++) {
      await ft.fireLatest();
      assert.equal(ft.lastDelay(), REFRESH_RETRY_DELAY_MS);
      assert.equal(ft.pendingCount(), 1);
    }
    // The final failure crosses the cap → no further retry armed.
    await ft.fireLatest();
    assert.equal(calls, MAX_REFRESH_FAILURES);
    assert.equal(ft.pendingCount(), 0);
  });

  it('resets the failure count after a success', async () => {
    const ft = fakeTimers();
    const results: Array<number | undefined> = [undefined, 10_000 + 9000, undefined];
    let i = 0;
    const sched = new OAuthRefreshScheduler(async () => results[i++], 1000, ft.timers);
    sched.schedule(10_000 + 5000);
    await ft.fireLatest(); // fail (failures=1) → retry armed
    await ft.fireLatest(); // success → failures reset, re-armed from 9s expiry
    assert.equal(ft.lastDelay(), 8000);
    await ft.fireLatest(); // fail again → failures back to 1 (retry armed), not at the cap
    assert.equal(ft.lastDelay(), REFRESH_RETRY_DELAY_MS);
    assert.equal(ft.pendingCount(), 1);
  });

  it('generation guard: a cancel during an in-flight refresh suppresses the follow-up', async () => {
    const ft = fakeTimers();
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    const sched = new OAuthRefreshScheduler(async () => { await gate; return 10_000 + 5000; }, 1000, ft.timers);
    sched.schedule(10_000 + 5000);
    // Fire the timer but do not await settle yet — the onRefresh is awaiting the gate.
    const fire = ft.fireLatest();
    sched.cancel();          // bumps generation while refresh is in flight
    release();               // let onRefresh resolve
    await fire;
    await new Promise((r) => setImmediate(r));
    await new Promise((r) => setImmediate(r));
    // No follow-up timer armed because the fire's generation is now stale.
    assert.equal(ft.pendingCount(), 0);
  });

  it('cancel clears a pending timer', () => {
    const ft = fakeTimers();
    const sched = new OAuthRefreshScheduler(async () => undefined, 1000, ft.timers);
    sched.schedule(10_000 + 5000);
    assert.equal(ft.pendingCount(), 1);
    sched.cancel();
    assert.equal(ft.pendingCount(), 0);
  });

  it('swallows an onRefresh that throws (treated as a failure)', async () => {
    const ft = fakeTimers();
    const sched = new OAuthRefreshScheduler(async () => { throw new Error('boom'); }, 1000, ft.timers);
    sched.schedule(10_000 + 5000);
    await ft.fireLatest();
    // Treated as a failure → a retry is armed, no unhandled rejection.
    assert.equal(ft.lastDelay(), REFRESH_RETRY_DELAY_MS);
  });

  it('defaults bufferMs to TOKEN_REFRESH_BUFFER_MS', () => {
    const ft = fakeTimers();
    const sched = new OAuthRefreshScheduler(async () => undefined, undefined, ft.timers);
    sched.schedule(10_000 + TOKEN_REFRESH_BUFFER_MS + 3000);
    assert.equal(ft.lastDelay(), 3000);
  });
});

// ---------------------------------------------------------------------------
// McpOAuthSession autoRefresh wiring
// ---------------------------------------------------------------------------
describe('McpOAuthSession autoRefresh wiring', () => {
  const serverUrl = 'https://mcp.example.com';
  const prmUrl = 'https://mcp.example.com/.well-known/oauth-protected-resource';
  const asWellKnown = 'https://as.example.com/.well-known/oauth-authorization-server';

  function routes(tokenBody: Record<string, unknown>) {
    return {
      [prmUrl]: { body: { resource: 'https://mcp.example.com', authorization_servers: ['https://as.example.com'] } },
      [asWellKnown]: { body: { issuer: 'https://as.example.com', authorization_endpoint: 'https://as.example.com/auth', token_endpoint: 'https://as.example.com/token', registration_endpoint: 'https://as.example.com/register' } },
      'https://as.example.com/register': { body: { client_id: 'dyn-client' } },
      'https://as.example.com/token': { body: tokenBody },
    };
  }

  function recordingFetch(map: Record<string, { ok?: boolean; status?: number; body: unknown }>): FetchLike {
    return async (url) => {
      const r = map[url];
      if (!r) return { ok: false, status: 404, json: async () => ({}) };
      return { ok: r.ok ?? true, status: r.status ?? 200, json: async () => r.body };
    };
  }

  it('arms a proactive refresh after an interactive auth that yields a refresh token', async () => {
    const ft = fakeTimers();
    const fn = recordingFetch(routes({ access_token: 'at', token_type: 'Bearer', refresh_token: 'rt', expires_in: 3600 }));
    const session = new McpOAuthSession({
      serverUrl, fetchImpl: fn, redirectUri: 'http://localhost:1/callback',
      authorize: async ({ state }) => ({ code: 'c', state }),
      now: () => 10_000, autoRefresh: true, refreshTimers: ft.timers,
    });
    const ok = await session.onUnauthorized(null);
    assert.equal(ok, true);
    // A proactive refresh timer should now be armed (expiry 3600s out).
    assert.equal(ft.pendingCount(), 1);
    assert.equal(ft.lastDelay(), 3600_000 - TOKEN_REFRESH_BUFFER_MS);
  });

  it('does NOT arm a timer when the grant has no refresh token (silent refresh impossible)', async () => {
    const ft = fakeTimers();
    const fn = recordingFetch(routes({ access_token: 'at', token_type: 'Bearer', expires_in: 3600 }));
    const session = new McpOAuthSession({
      serverUrl, fetchImpl: fn, redirectUri: 'http://localhost:1/callback',
      authorize: async ({ state }) => ({ code: 'c', state }),
      now: () => 10_000, autoRefresh: true, refreshTimers: ft.timers,
    });
    await session.onUnauthorized(null);
    assert.equal(ft.pendingCount(), 0);
  });

  it('cancels the scheduler on revoke', async () => {
    const ft = fakeTimers();
    const fn = recordingFetch({
      ...routes({ access_token: 'at', token_type: 'Bearer', refresh_token: 'rt', expires_in: 3600 }),
    });
    const session = new McpOAuthSession({
      serverUrl, fetchImpl: fn, redirectUri: 'http://localhost:1/callback',
      authorize: async ({ state }) => ({ code: 'c', state }),
      now: () => 10_000, autoRefresh: true, refreshTimers: ft.timers,
    });
    await session.onUnauthorized(null);
    assert.equal(ft.pendingCount(), 1);
    await session.revoke();
    assert.equal(ft.pendingCount(), 0);
  });

  it('cancels the scheduler when the caller signal aborts', async () => {
    const ft = fakeTimers();
    const controller = new AbortController();
    const fn = recordingFetch(routes({ access_token: 'at', token_type: 'Bearer', refresh_token: 'rt', expires_in: 3600 }));
    const session = new McpOAuthSession({
      serverUrl, fetchImpl: fn, redirectUri: 'http://localhost:1/callback',
      authorize: async ({ state }) => ({ code: 'c', state }),
      now: () => 10_000, autoRefresh: true, refreshTimers: ft.timers, signal: controller.signal,
    });
    await session.onUnauthorized(null);
    assert.equal(ft.pendingCount(), 1);
    controller.abort();
    assert.equal(ft.pendingCount(), 0);
  });
});
