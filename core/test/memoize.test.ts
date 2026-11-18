import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  memoizeWithTTL,
  memoizeWithTTLAsync,
  memoizeWithLRU,
  memoizeAsyncWithLRU,
} from '../src/utils/memoize';

const tick = () => new Promise<void>((r) => setTimeout(r, 0));

describe('memoizeWithTTL (sync, stale-while-revalidate)', () => {
  it('cold miss computes once and serves the cached value within the TTL', () => {
    let calls = 0;
    const m = memoizeWithTTL((n: number) => {
      calls++;
      return n * 2;
    }, 60_000);
    assert.equal(m(21), 42);
    assert.equal(m(21), 42);
    assert.equal(calls, 1, 'fresh hit must not recompute');
    // Distinct args are distinct keys.
    assert.equal(m(1), 2);
    assert.equal(calls, 2);
  });

  it('serves the stale value immediately and refreshes in the background', async () => {
    let value = 1;
    let calls = 0;
    const m = memoizeWithTTL(() => {
      calls++;
      return value;
    }, -1); // negative lifetime => every existing entry is immediately stale
    assert.equal(m(), 1); // cold miss
    assert.equal(calls, 1);
    value = 2;
    // Stale: returns the OLD value synchronously, schedules a refresh (the sync
    // variant defers the recompute to a microtask, so `calls` is unchanged here).
    assert.equal(m(), 1);
    assert.equal(calls, 1, 'the recompute is deferred, not run synchronously');
    await tick();
    assert.equal(calls, 2, 'exactly one background refresh ran');
    // After the background refresh lands, the fresh value is served.
    assert.equal(m(), 2);
  });

  it('the refreshing flag collapses concurrent stale reads into one refresh', async () => {
    let calls = 0;
    const m = memoizeWithTTL(() => {
      calls++;
      return calls;
    }, -1);
    m(); // cold miss -> calls=1
    // Three stale reads before the microtask runs: only the FIRST sets the
    // refreshing flag and schedules a refresh; the other two short-circuit.
    m();
    m();
    m();
    assert.equal(calls, 1, 'no recompute runs synchronously');
    await tick();
    assert.equal(calls, 2, 'exactly one background refresh ran despite three stale reads');
  });

  it('a throwing background refresh deletes the entry (self-correcting)', async () => {
    let n = 0;
    const m = memoizeWithTTL(() => {
      n++;
      if (n === 2) throw new Error('refresh boom');
      return n;
    }, -1);
    assert.equal(m(), 1); // cold miss, n=1
    assert.equal(m(), 1); // stale read -> schedules refresh (n=2 throws)
    await tick();
    // Entry was deleted by the failed refresh; next call is a cold miss (n=3).
    assert.equal(m(), 3);
  });

  it('cache.clear() drops all entries', () => {
    let calls = 0;
    const m = memoizeWithTTL((x: number) => {
      calls++;
      return x;
    }, 60_000);
    m(5);
    m(5);
    assert.equal(calls, 1);
    m.cache.clear();
    m(5);
    assert.equal(calls, 2, 'after clear the next call recomputes');
  });

  it('defaults the lifetime to 5 minutes when not supplied', () => {
    let calls = 0;
    const m = memoizeWithTTL(() => {
      calls++;
      return calls;
    });
    assert.equal(m(), 1);
    assert.equal(m(), 1); // well within the default 5-minute TTL
    assert.equal(calls, 1);
  });
});

describe('memoizeWithTTLAsync (async stale-while-revalidate + single-flight)', () => {
  it('coalesces concurrent cold-miss callers onto one invocation', async () => {
    let calls = 0;
    const m = memoizeWithTTLAsync(async (n: number) => {
      calls++;
      await tick();
      return n + 1;
    }, 60_000);
    const [a, b, c] = await Promise.all([m(10), m(10), m(10)]);
    assert.deepEqual([a, b, c], [11, 11, 11]);
    assert.equal(calls, 1, 'in-flight dedup: three concurrent callers share one f()');
    // A later call within the TTL is served from cache (still no new call).
    assert.equal(await m(10), 11);
    assert.equal(calls, 1);
  });

  it('serves the stale value immediately and refreshes in the background', async () => {
    let value = 'a';
    let calls = 0;
    const m = memoizeWithTTLAsync(async () => {
      calls++;
      return value;
    }, -1);
    assert.equal(await m(), 'a'); // cold miss
    assert.equal(calls, 1);
    value = 'b';
    assert.equal(await m(), 'a', 'stale read returns the previous value');
    assert.equal(calls, 2, 'one background refresh scheduled');
    await tick();
    assert.equal(await m(), 'b');
  });

  it('does not cache when the cold-miss invocation rejects', async () => {
    let calls = 0;
    const m = memoizeWithTTLAsync(async () => {
      calls++;
      throw new Error('cold boom');
    }, 60_000);
    await assert.rejects(m(), /cold boom/);
    await assert.rejects(m(), /cold boom/);
    assert.equal(calls, 2, 'a rejected cold miss is not cached and retries');
  });

  it('a rejecting background refresh deletes the entry', async () => {
    let n = 0;
    const m = memoizeWithTTLAsync(async () => {
      n++;
      if (n === 2) throw new Error('refresh boom');
      return n;
    }, -1);
    assert.equal(await m(), 1); // cold miss
    assert.equal(await m(), 1); // stale -> schedules refresh (n=2 rejects)
    await tick();
    await tick();
    assert.equal(await m(), 3, 'entry deleted by the failed refresh; cold miss recomputes');
  });

  it('cache.clear() during an in-flight cold miss discards the result (identity guard)', async () => {
    let calls = 0;
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    const m = memoizeWithTTLAsync(async () => {
      calls++;
      await gate;
      return calls;
    }, 60_000);
    const inflight = m();
    // Invalidate while the compute is still awaiting.
    m.cache.clear();
    release();
    assert.equal(await inflight, 1);
    // The result was NOT stored (clear intent), so the next call recomputes.
    const next = m();
    // This cold miss computes again.
    assert.equal(await next, 2);
    assert.equal(calls, 2);
  });

  it('cache.clear() wipes the cache so later reads recompute', async () => {
    let calls = 0;
    const m = memoizeWithTTLAsync(async (x: number) => {
      calls++;
      return x;
    }, 60_000);
    assert.equal(await m(7), 7);
    assert.equal(await m(7), 7);
    assert.equal(calls, 1);
    m.cache.clear();
    assert.equal(await m(7), 7);
    assert.equal(calls, 2);
  });
});

describe('memoizeWithLRU (bounded LRU with pluggable key + peek)', () => {
  it('uses the pluggable cacheFn for the key and memoizes', () => {
    let calls = 0;
    const m = memoizeWithLRU(
      (a: number, b: number) => {
        calls++;
        return { sum: a + b };
      },
      (a, b) => `${a}:${b}`,
      10,
    );
    const r1 = m(1, 2);
    const r2 = m(1, 2);
    assert.equal(r1, r2, 'same key returns the identical cached object');
    assert.equal(calls, 1);
    m(2, 1); // different key
    assert.equal(calls, 2);
  });

  it('evicts the oldest entry once maxCacheSize is exceeded', () => {
    const m = memoizeWithLRU(
      (k: string) => ({ k }),
      (k) => k,
      2,
    );
    m('a');
    m('b');
    m('c'); // exceeds 2 -> 'a' (oldest) evicted
    assert.equal(m.cache.has('a'), false);
    assert.equal(m.cache.has('b'), true);
    assert.equal(m.cache.has('c'), true);
    assert.equal(m.cache.size(), 2);
  });

  it('an internal cache hit promotes recency so the key survives eviction', () => {
    const m = memoizeWithLRU(
      (k: string) => ({ k }),
      (k) => k,
      2,
    );
    m('a');
    m('b');
    m('a'); // HIT -> promotes 'a' to most-recent; 'b' is now oldest
    m('c'); // evicts the oldest -> 'b'
    assert.equal(m.cache.has('a'), true, 'promoted key survives');
    assert.equal(m.cache.has('b'), false, 'un-promoted key is evicted');
    assert.equal(m.cache.has('c'), true);
  });

  it('management get() is a non-promoting peek', () => {
    const m = memoizeWithLRU(
      (k: string) => ({ k }),
      (k) => k,
      2,
    );
    m('a');
    m('b');
    // peek 'a' must NOT promote recency.
    assert.deepEqual(m.cache.get('a'), { k: 'a' });
    m('c'); // 'a' is still the oldest (peek did not promote) -> evicted
    assert.equal(m.cache.has('a'), false, 'peek did not save a from eviction');
    assert.equal(m.cache.get('missing'), undefined);
  });

  it('exposes delete, clear, and size on the management surface', () => {
    let calls = 0;
    const m = memoizeWithLRU(
      (k: string) => {
        calls++;
        return { k };
      },
      (k) => k,
      10,
    );
    m('x');
    assert.equal(m.cache.size(), 1);
    assert.equal(m.cache.delete('x'), true);
    assert.equal(m.cache.delete('x'), false);
    assert.equal(m.cache.size(), 0);
    m('x'); // recomputes (was deleted)
    m.cache.clear();
    assert.equal(m.cache.size(), 0);
    m('x'); // recomputes again
    assert.equal(calls, 3);
  });

  it('defaults maxCacheSize to 100', () => {
    const m = memoizeWithLRU(
      (k: number) => ({ k }),
      (k) => String(k),
    );
    for (let i = 0; i < 150; i++) m(i);
    assert.equal(m.cache.size(), 100, 'bounded to the default of 100');
    assert.equal(m.cache.has('0'), false, 'the earliest keys were evicted');
    assert.equal(m.cache.has('149'), true);
  });
});

describe('memoizeAsyncWithLRU (async bounded LRU + evict-on-rejection)', () => {
  it('coalesces concurrent callers onto one cached promise', async () => {
    let calls = 0;
    const m = memoizeAsyncWithLRU(
      async (n: number) => {
        calls++;
        await tick();
        return n + 1;
      },
      (n) => String(n),
    );
    const p1 = m(10);
    const p2 = m(10);
    assert.strictEqual(p1, p2, 'repeat lookup returns the identical cached promise');
    assert.deepEqual(await Promise.all([p1, p2]), [11, 11]);
    assert.equal(calls, 1, 'both callers shared one invocation');
    // A later call is served from the cached (already-settled) promise.
    assert.equal(await m(10), 11);
    assert.equal(calls, 1, 'settled result stays cached (no TTL expiry)');
  });

  it('caches a promise resolving to null (null-safe, unlike a value LRU)', async () => {
    let calls = 0;
    const m = memoizeAsyncWithLRU(
      async (_k: string): Promise<string | null> => {
        calls++;
        return null;
      },
      (k) => k,
    );
    assert.equal(await m('x'), null);
    assert.equal(await m('x'), null);
    assert.equal(calls, 1, 'the null-resolving promise was cached');
  });

  it('evicts the cached promise on rejection so the next call retries', async () => {
    let calls = 0;
    const m = memoizeAsyncWithLRU(
      async (_k: string) => {
        calls++;
        throw new Error('boom ' + calls);
      },
      (k) => k,
    );
    await assert.rejects(m('x'), /boom 1/);
    await tick(); // let the identity-guarded .catch eviction run
    assert.equal(m.cache.has('x'), false, 'the rejected entry was evicted');
    await assert.rejects(m('x'), /boom 2/, 'a fresh invocation retried');
    assert.equal(calls, 2);
  });

  it('identity-guards eviction: a stale rejection does not delete a newer entry', async () => {
    const deferreds: Array<{ promise: Promise<number>; reject: (e: unknown) => void }> = [];
    const m = memoizeAsyncWithLRU(
      (_k: string): Promise<number> => {
        let reject!: (e: unknown) => void;
        const promise = new Promise<number>((_res, rej) => { reject = rej; });
        deferreds.push({ promise, reject });
        return promise;
      },
      (k) => k,
    );

    const p1 = m('k'); // stores deferreds[0]
    assert.strictEqual(m.cache.get('k'), p1);
    // Simulate an LRU eviction of the key, then a fresh cold miss repopulating it
    // with a NEWER promise before the first one has settled.
    m.cache.delete('k');
    const p2 = m('k'); // stores deferreds[1]
    assert.strictEqual(m.cache.get('k'), p2);

    // The original (now stale) promise rejects: its identity-guarded .catch must
    // NOT delete the newer entry, since cache.get('k') is no longer that promise.
    deferreds[0].reject(new Error('stale'));
    await assert.rejects(p1, /stale/);
    await tick();
    assert.equal(m.cache.has('k'), true, 'the newer entry survived the stale rejection');
    assert.strictEqual(m.cache.get('k'), p2, 'the cached promise is still the newer one');

    // Cleanup: settle the live promise (and let its own eviction run).
    deferreds[1].reject(new Error('cleanup'));
    await assert.rejects(p2, /cleanup/);
    await tick();
  });

  it('bounds the cache to maxCacheSize and evicts the oldest key', async () => {
    const m = memoizeAsyncWithLRU(
      async (k: string) => k,
      (k) => k,
      2,
    );
    await m('a');
    await m('b');
    await m('c'); // exceeds 2 -> 'a' evicted
    assert.equal(m.cache.has('a'), false);
    assert.equal(m.cache.has('b'), true);
    assert.equal(m.cache.has('c'), true);
    assert.equal(m.cache.size(), 2);
  });

  it('cache.get is a non-promoting peek that resolves to the result', async () => {
    const m = memoizeAsyncWithLRU(
      async (k: string) => k.toUpperCase(),
      (k) => k,
      2,
    );
    await m('a');
    await m('b');
    const peeked = m.cache.get('a');
    assert.ok(peeked);
    assert.equal(await peeked, 'A');
    await m('c'); // peek did not promote 'a' -> it is the oldest and evicted
    assert.equal(m.cache.has('a'), false, 'peek did not save a from eviction');
    assert.equal(m.cache.get('missing'), undefined);
  });

  it('exposes clear/delete/size and defaults the bound to 100', async () => {
    let calls = 0;
    const m = memoizeAsyncWithLRU(
      async (k: number) => {
        calls++;
        return k;
      },
      (k) => String(k),
    );
    for (let i = 0; i < 150; i++) await m(i);
    assert.equal(m.cache.size(), 100, 'bounded to the default of 100');
    assert.equal(m.cache.has('0'), false, 'the earliest keys were evicted');
    assert.equal(m.cache.has('149'), true);
    assert.equal(m.cache.delete('149'), true);
    assert.equal(m.cache.delete('149'), false);
    m.cache.clear();
    assert.equal(m.cache.size(), 0);
    assert.ok(calls > 0);
  });
});
