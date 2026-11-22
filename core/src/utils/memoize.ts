import { logError } from '../logger';
import { errorMessage } from '../credential-guard';


/**
 * A single reusable memoization utility family — the canonical home for caching
 * behavior that was previously re-implemented ad hoc at every call site (bash
 * result cache, provider model/HF caches, permission pattern cache, highlight
 * cache). Three policies are offered so a caller never has to hand-roll eviction,
 * TTL, in-flight dedup, or identity-guarding again:
 *
 *   - {@link memoizeWithTTL}       — sync, stale-while-revalidate (write-through).
 *   - {@link memoizeWithTTLAsync}  — async, stale-while-revalidate + in-flight
 *                                    cold-miss dedup (single-flight).
 *   - {@link memoizeWithLRU}       — bounded LRU with a pluggable key function and
 *                                    a non-promoting observe accessor.
 *   - {@link memoizeAsyncWithLRU}  — async bounded LRU that caches the in-flight
 *                                    promise and EVICTS it on rejection (identity-
 *                                    guarded), so an aborted/failed lookup can't
 *                                    poison future turns.
 *
 * Parity with the reference's `utils/memoize.ts`. The LRU is backed by an
 * insertion-ordered Map (no external `lru-cache` dependency): a hit re-inserts
 * the key to mark it most-recent, and the oldest key is evicted first once the
 * bound is exceeded — the same behavior lru-cache provides.
 */

const DEFAULT_CACHE_LIFETIME_MS = 5 * 60 * 1000; // 5 minutes

type CacheEntry<T> = {
  value: T;
  timestamp: number;
  refreshing: boolean;
};

export type MemoizedFunction<Args extends unknown[], Result> = {
  (...args: Args): Result;
  cache: {
    clear: () => void;
  };
};

export type AsyncMemoizedFunction<Args extends unknown[], Result> = {
  (...args: Args): Promise<Result>;
  cache: {
    clear: () => void;
  };
};

export type LRUMemoizedFunction<Args extends unknown[], Result> = {
  (...args: Args): Result;
  cache: {
    clear: () => void;
    size: () => number;
    delete: (key: string) => boolean;
    /** Observe without promoting recency (peek). */
    get: (key: string) => Result | undefined;
    has: (key: string) => boolean;
  };
};

export type AsyncLRUMemoizedFunction<Args extends unknown[], Result> = {
  (...args: Args): Promise<Result>;
  cache: {
    clear: () => void;
    size: () => number;
    delete: (key: string) => boolean;
    /** Observe the cached promise without promoting recency (peek). */
    get: (key: string) => Promise<Result> | undefined;
    has: (key: string) => boolean;
  };
};

/** Stable cache key from a call's argument tuple. */
function keyFor(args: unknown[]): string {
  return JSON.stringify(args);
}

/**
 * Memoize a synchronous function with a stale-while-revalidate (write-through)
 * policy:
 *   - fresh entry  → return it immediately;
 *   - stale entry  → return the stale value AND recompute in the background
 *                    (guarded by a per-entry `refreshing` flag so N concurrent
 *                    callers trigger only ONE refresh);
 *   - cold miss    → block, compute, cache, return.
 *
 * The background refresh is identity-guarded: it only writes/deletes its own
 * entry (`cache.get(key) === cached`), so a concurrent `cache.clear()` + cold
 * miss that stored a newer entry is never clobbered by the in-flight stale
 * refresh. A refresh that throws deletes the entry (self-correcting on the next
 * call) rather than persisting wrong data for a full TTL.
 */
export function memoizeWithTTL<Args extends unknown[], Result>(
  f: (...args: Args) => Result,
  cacheLifetimeMs: number = DEFAULT_CACHE_LIFETIME_MS,
): MemoizedFunction<Args, Result> {
  const cache = new Map<string, CacheEntry<Result>>();

  const memoized = (...args: Args): Result => {
    const key = keyFor(args);
    const cached = cache.get(key);
    const now = Date.now();

    // Cold miss: block + compute.
    if (!cached) {
      const value = f(...args);
      cache.set(key, { value, timestamp: now, refreshing: false });
      return value;
    }

    // Stale and not already refreshing: serve stale, refresh in the background.
    if (now - cached.timestamp > cacheLifetimeMs && !cached.refreshing) {
      cached.refreshing = true;
      // Defer to a microtask so the refresh never blocks the caller. Both arms
      // are identity-guarded against a concurrent clear()+cold-miss.
      Promise.resolve()
        .then(() => {
          const newValue = f(...args);
          if (cache.get(key) === cached) {
            cache.set(key, { value: newValue, timestamp: Date.now(), refreshing: false });
          }
        })
        .catch((e) => {
          logError('memoize', errorMessage(e));
          if (cache.get(key) === cached) cache.delete(key);
        });
      return cached.value;
    }

    return cache.get(key)!.value;
  };

  memoized.cache = { clear: () => cache.clear() };
  return memoized;
}

/**
 * Memoize an async function with the same stale-while-revalidate policy as
 * {@link memoizeWithTTL}, plus in-flight cold-miss dedup (single-flight): a
 * per-key `inFlight` Promise map so N concurrent cold-miss callers for the same
 * key share ONE `f()` invocation instead of each spawning their own expensive /
 * side-effecting operation (e.g. N parallel credential refreshes).
 *
 * Identity-guards throughout defend against a `cache.clear()` racing an
 * in-flight compute/refresh: the cold-miss result is only stored while still
 * in-flight (`inFlight.get(key) === promise`), the finally only deletes its own
 * in-flight entry, the background refresh only writes/deletes its own stale
 * entry, and `clear()` wipes `inFlight` too so a stale in-flight promise is never
 * handed to the next caller after an invalidation.
 *
 * If `f` throws on a cold miss, nothing is cached — the error propagates and the
 * next call retries.
 */
export function memoizeWithTTLAsync<Args extends unknown[], Result>(
  f: (...args: Args) => Promise<Result>,
  cacheLifetimeMs: number = DEFAULT_CACHE_LIFETIME_MS,
): AsyncMemoizedFunction<Args, Result> {
  const cache = new Map<string, CacheEntry<Result>>();
  const inFlight = new Map<string, Promise<Result>>();

  const memoized = async (...args: Args): Promise<Result> => {
    const key = keyFor(args);
    const cached = cache.get(key);
    const now = Date.now();

    // Cold miss: coalesce concurrent callers onto one invocation.
    if (!cached) {
      const pending = inFlight.get(key);
      if (pending) return pending;
      const promise = f(...args);
      inFlight.set(key, promise);
      try {
        const result = await promise;
        // Only store if a concurrent clear() did not invalidate us mid-await.
        if (inFlight.get(key) === promise) {
          cache.set(key, { value: result, timestamp: now, refreshing: false });
        }
        return result;
      } finally {
        if (inFlight.get(key) === promise) inFlight.delete(key);
      }
    }

    // Stale and not already refreshing: serve stale, refresh in the background.
    if (now - cached.timestamp > cacheLifetimeMs && !cached.refreshing) {
      cached.refreshing = true;
      const staleEntry = cached;
      f(...args)
        .then((newValue) => {
          if (cache.get(key) === staleEntry) {
            cache.set(key, { value: newValue, timestamp: Date.now(), refreshing: false });
          }
        })
        .catch((e) => {
          logError('memoize', errorMessage(e));
          if (cache.get(key) === staleEntry) cache.delete(key);
        });
      return cached.value;
    }

    return cache.get(key)!.value;
  };

  memoized.cache = {
    clear: () => {
      cache.clear();
      inFlight.clear();
    },
  };
  return memoized;
}

/**
 * Memoize a function with a bounded LRU eviction policy to cap memory growth. A
 * pluggable `cacheFn` builds the cache key from the arguments, and `maxCacheSize`
 * bounds the entry count (oldest-used evicted first). The management surface
 * exposes `{clear, size, delete, get, has}`; `get` is a NON-promoting peek (it
 * observes without updating recency), while an internal cache hit DOES promote
 * the key to most-recent.
 *
 * Backed by an insertion-ordered Map rather than an external dependency: a hit
 * deletes + re-inserts the key (moving it to the end = most-recent), and eviction
 * removes `keys().next()` (the oldest).
 */
export function memoizeWithLRU<
  Args extends unknown[],
  Result extends NonNullable<unknown>,
>(
  f: (...args: Args) => Result,
  cacheFn: (...args: Args) => string,
  maxCacheSize: number = 100,
): LRUMemoizedFunction<Args, Result> {
  const cache = new Map<string, Result>();

  const memoized = (...args: Args): Result => {
    const key = cacheFn(...args);
    if (cache.has(key)) {
      const hit = cache.get(key)!;
      // Promote to most-recent.
      cache.delete(key);
      cache.set(key, hit);
      return hit;
    }

    const result = f(...args);
    cache.set(key, result);
    while (cache.size > maxCacheSize) {
      const oldest = cache.keys().next().value as string;
      cache.delete(oldest);
    }
    return result;
  };

  memoized.cache = {
    clear: () => cache.clear(),
    size: () => cache.size,
    delete: (key: string) => cache.delete(key),
    // Observe without promoting recency (parity with lru-cache peek()).
    get: (key: string) => cache.get(key),
    has: (key: string) => cache.has(key),
  };

  return memoized;
}

/**
 * Memoize an ASYNC function with a bounded LRU that caches the in-flight PROMISE
 * (so concurrent callers for the same key share one invocation) and EVICTS the
 * cached entry if that promise rejects — an aborted or failed lookup can never
 * poison future turns. Mirrors the reference's `utils/shell/prefix.ts` two-layer
 * pattern (L111-L118): the outer memoizer stores the promise, and a `.catch`
 * handler removes it on rejection.
 *
 * The eviction is **identity-guarded**: it deletes the key only while the stored
 * promise is still the one that rejected (`cache.get(key) === promise`). After an
 * LRU eviction a NEWER promise may already occupy the key, and a stale rejection
 * must not delete that newer entry — exactly the edge case prefix.ts guards. This
 * consolidates the hand-rolled "wrap a promise in `memoizeWithLRU` + bolt a
 * `.catch` eviction on at the call site" idiom into one reusable policy so a
 * caller never re-implements the identity guard.
 *
 * A successful result stays cached until evicted by LRU bound (there is no TTL —
 * use {@link memoizeWithTTLAsync} when a result must expire). A rejected cold miss
 * is evicted, so the next call retries.
 */
export function memoizeAsyncWithLRU<Args extends unknown[], Result>(
  f: (...args: Args) => Promise<Result>,
  cacheFn: (...args: Args) => string,
  maxCacheSize: number = 100,
): AsyncLRUMemoizedFunction<Args, Result> {
  // Reuse the sync LRU as the backing store, caching the promise itself. The
  // promise is non-nullable, so it satisfies the LRU's value constraint even when
  // `Result` includes null/undefined.
  const lru = memoizeWithLRU<Args, Promise<Result>>(
    (...args: Args): Promise<Result> => {
      const key = cacheFn(...args);
      const promise = f(...args);
      // Evict on rejection so aborted/failed calls don't poison future turns.
      // Identity guard: after LRU eviction a newer promise may occupy this key;
      // a stale rejection must not delete it.
      promise.catch(() => {
        if (lru.cache.get(key) === promise) lru.cache.delete(key);
      });
      return promise;
    },
    cacheFn,
    maxCacheSize,
  );

  return lru;
}
