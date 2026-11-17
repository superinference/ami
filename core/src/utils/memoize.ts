import { logError } from '../logger';

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
 *
 * Parity with Claude Code's `utils/memoize.ts`. The LRU is backed by an
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
          logError('memoize', e instanceof Error ? e.message : String(e));
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
          logError('memoize', e instanceof Error ? e.message : String(e));
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
