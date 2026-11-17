/**
 * Canonical time primitives — parity with Claude Code's `utils/sleep.ts`.
 *
 * This is the ONE shared home for the two timing building blocks every backoff /
 * retry / poll / timeout-guard loop needs, so none of them re-implement the
 * pattern (and drift) at the call site:
 *
 *  - {@link sleep}: an abort-responsive delay with selectable abort semantics.
 *  - {@link withTimeout}: a race-against-a-deadline guard for any awaited work.
 *
 * It lives in `core/` (the lowest layer) so BOTH core modules (e.g. `provider.ts`
 * backoff, `mcp/client.ts` connect timeout) and upper layers (common's engine /
 * tool-executor / provision control plane) import the SAME implementation instead
 * of keeping divergent private copies.
 */

/**
 * Options bag selecting how {@link sleep} reacts to an abort. Mirrors Claude
 * Code's `utils/sleep.ts` selectable abort semantics so each caller picks exactly
 * the behavior it needs instead of being forced into one.
 */
export interface SleepOptions {
  /** Reject on abort (so the rejection bubbles up and cancels a surrounding retry loop). */
  throwOnAbort?: boolean;
  /** Reject with this specific error on abort (implies {@link throwOnAbort}) — e.g. a loop catching exactly that class. */
  abortError?: () => Error;
  /** `unref()` the pending timer so a still-sleeping backoff never keeps the process alive. */
  unref?: boolean;
}

/**
 * Abort-responsive sleep. Resolves after `ms`, or settles early when `signal`
 * aborts. By DEFAULT an abort SILENTLY RESOLVES (the caller checks
 * `signal.aborted` after the await — no try/catch needed), matching Claude Code's
 * `sleep`. Pass `{ throwOnAbort: true }` to reject instead so a deep retry loop is
 * cancelled, or `{ abortError }` to reject with a specific error class (implies
 * `throwOnAbort`). The default rejection is a DOMException `AbortError`, which
 * `isAbort` recognizes, so existing abort detection is preserved. The timer is
 * always cleared on abort so no stray timer survives.
 */
export function sleep(ms: number, signal?: AbortSignal, opts?: SleepOptions): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const settleAborted = (): void => {
      if (opts?.throwOnAbort || opts?.abortError) {
        reject(opts?.abortError?.() ?? new DOMException('Request aborted', 'AbortError'));
      } else {
        resolve();
      }
    };
    // Check aborted state BEFORE arming the timer so an already-aborted signal
    // never schedules a timer that must then be torn down.
    if (signal?.aborted) {
      settleAborted();
      return;
    }
    const onAbort = (): void => {
      clearTimeout(timer);
      settleAborted();
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    signal?.addEventListener('abort', onAbort, { once: true });
    if (opts?.unref) (timer as { unref?: () => void }).unref?.();
  });
}

/**
 * Race `promise` against a timeout. Rejects with `Error(message)` if the promise
 * does not settle within `ms`; otherwise resolves/rejects with the promise's own
 * outcome. The timeout timer is cleared the instant the promise settles (no
 * dangling timer) and is `unref`'d so a pending guard can never keep the process
 * alive past exit.
 *
 * This is a BOUNDARY, not a canceller: it does NOT abort the underlying work — a
 * runaway async step keeps running — it only returns control to the caller so a
 * hung dependency (provider SDK call, MCP connect handshake, disk / model-load
 * wait) cannot wedge a loop forever. A non-finite / non-positive `ms` means "no
 * bound" and the original promise is returned as-is.
 */
export function withTimeout<T>(promise: Promise<T>, ms: number, message: string): Promise<T> {
  // A non-positive / non-finite bound means "unbounded" — hand back the promise
  // untouched rather than arming a timer that fires immediately or never.
  if (!(ms > 0 && ms < Number.POSITIVE_INFINITY)) return promise;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(message)), ms);
    // Never keep the process alive purely for a timeout guard.
    if (typeof timer === 'object' && typeof (timer as { unref?: () => void }).unref === 'function') {
      (timer as { unref: () => void }).unref();
    }
  });
  return Promise.race([promise, timeout]).finally(() => {
    if (timer !== undefined) clearTimeout(timer);
  });
}
