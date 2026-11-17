/**
 * Combined AbortSignal helper — parity with Claude Code's
 * `createCombinedAbortSignal` (utils/combinedAbortSignal.ts).
 *
 * Combines a primary `signal` with an optional second signal (`signalB`) and/or
 * an optional `timeoutMs` into ONE derived signal, and returns a `cleanup()` that
 * detaches the source listeners AND clears the timer. The whole point of the
 * `timeoutMs` option is to avoid `AbortSignal.timeout(ms)`: that native helper's
 * timers are finalized lazily and accumulate in native memory (~2.4KB/call) held
 * for the FULL timeout duration even after the awaited work has already settled.
 * This implementation uses `setTimeout` + `clearTimeout`, so the moment a caller
 * calls `cleanup()` (e.g. in a `finally` after a `fetch` resolves) the timer is
 * freed immediately instead of lingering until it would have fired.
 *
 * This is a DISTINCT capability from {@link ../../../../common/src/child-abort-signal}
 * — Claude itself ships the two as separate files. The child helper derives a
 * NON-propagating, WeakRef-GC-safe child controller (`abort()` + `cleanup()`);
 * this helper derives a plain combined signal that simply mirrors its sources,
 * with no back-propagation guarantee and no explicit `abort()`. Core call sites
 * (web-fetch, provider) need exactly the latter and cannot import the common
 * layer, so the primitive lives here in core (the lowest layer, No-Duplication).
 *
 * The derived signal (and any input signals) get the shared
 * {@link raiseAbortListenerCap} treatment so a wide fan-out attaching many
 * listeners never trips Node's default-10 `MaxListenersExceededWarning`.
 */
import {
  createBoundedAbortController,
  raiseAbortListenerCap,
  DEFAULT_ABORT_MAX_LISTENERS,
} from './abort';

/** Options for {@link createCombinedAbortSignal}. */
export interface CombinedAbortOptions {
  /** A second source signal; the combined signal also aborts when it aborts. */
  signalB?: AbortSignal;
  /**
   * Abort the combined signal after this many ms, using `setTimeout` +
   * `clearTimeout` (NOT `AbortSignal.timeout`) so `cleanup()` frees the timer
   * immediately instead of leaking it for the full duration.
   */
  timeoutMs?: number;
  /** Listener-cap forwarded to {@link raiseAbortListenerCap} (default 50). */
  maxListeners?: number;
}

/** A combined abort signal plus the teardown that frees its timer + listeners. */
export interface CombinedAbortSignal {
  /** The derived signal; aborts when `signal`, `signalB`, or `timeoutMs` fires. */
  signal: AbortSignal;
  /** Detach the source listeners and clear the timeout timer. Idempotent. */
  cleanup: () => void;
}

/**
 * Combine `signal` with an optional `signalB` and/or `timeoutMs` into one derived
 * signal. Always call the returned `cleanup()` once the awaited work settles so
 * the timeout timer is cleared immediately. See the module doc for the full
 * memory-safety rationale.
 */
export function createCombinedAbortSignal(
  signal: AbortSignal | undefined,
  opts?: CombinedAbortOptions,
): CombinedAbortSignal {
  const { signalB, timeoutMs, maxListeners = DEFAULT_ABORT_MAX_LISTENERS } = opts ?? {};
  const combined = createBoundedAbortController(maxListeners);

  // Already-aborted short-circuit: abort synchronously carrying the source reason
  // and return a no-op cleanup — never attach a listener/timer that cannot fire.
  if (signal?.aborted || signalB?.aborted) {
    combined.abort(signal?.aborted ? signal.reason : signalB?.reason);
    return { signal: combined.signal, cleanup: () => {} };
  }

  if (signal) raiseAbortListenerCap(signal, maxListeners);
  if (signalB) raiseAbortListenerCap(signalB, maxListeners);

  let timer: ReturnType<typeof setTimeout> | undefined;

  const onSignalAbort = (): void => combined.abort(signal?.reason);
  const onSignalBAbort = (): void => combined.abort(signalB?.reason);

  if (timeoutMs !== undefined) {
    timer = setTimeout(() => combined.abort(new Error('timed out')), timeoutMs);
    // Don't keep the event loop alive for a background timeout.
    timer.unref?.();
  }
  if (signal) signal.addEventListener('abort', onSignalAbort, { once: true });
  if (signalB) signalB.addEventListener('abort', onSignalBAbort, { once: true });

  const cleanup = (): void => {
    if (timer !== undefined) {
      clearTimeout(timer);
      timer = undefined;
    }
    if (signal) signal.removeEventListener('abort', onSignalAbort);
    if (signalB) signalB.removeEventListener('abort', onSignalBAbort);
  };

  return { signal: combined.signal, cleanup };
}
