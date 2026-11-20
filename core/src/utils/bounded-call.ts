/**
 * `boundedCall` — bound an awaited call (a native/FFI pump, an MCP handshake, any
 * `() => Promise<T>` work factory) with a timeout race that is SAFE against the two
 * failure modes the plain `Promise.race` / `withTimeout(promise, …)` pattern leaves
 * open:
 *
 *   1. **Orphan late rejection.** When the timeout wins the race, the work promise
 *      is orphaned; a later rejection from it (e.g. a native layer finally failing,
 *      or a handshake erroring after the deadline) could surface as an
 *      `unhandledRejection`. `boundedCall` attaches a no-op `.catch()` to the work
 *      promise so the late rejection is swallowed and the timeout error is what the
 *      caller sees. (`Promise.race` already attaches a reaction to the losing
 *      promise, so this is belt-and-suspenders that matches the reference wrapper
 *      exactly and keeps the guarantee independent of race internals.)
 *   2. **Synchronous throw + leaked resource.** The work factory `fn()` is invoked
 *      INSIDE the `try`, so a synchronous throw (e.g. NAPI argument validation, or a
 *      factory that eagerly builds its promise) still reaches the `finally`, which
 *      runs `onRelease` and clears the timer — an acquired resource (pump refcount,
 *      lock, handle) never leaks even on a construction throw.
 *
 * This is the generalization of the reference's computer-use `drainRunLoop(fn)`
 * wrapper (`utils/computerUse/drainRunLoop.ts`): its refcounted CFRunLoop pump is
 * expressed as the optional {@link BoundedCallOptions.onAcquire}/`onRelease`
 * lifecycle hooks, so ANY caller that must hold a resource for exactly the duration
 * of a bounded call gets the same guarantees without re-implementing the race.
 *
 * Relationship to {@link withTimeout} (utils/sleep.ts): `withTimeout(promise, …)`
 * takes an ALREADY-created promise and therefore can neither catch a synchronous
 * construction throw, swallow the orphaned promise's late rejection, nor own an
 * acquire/release pairing. Use `withTimeout` for a promise you already hold; use
 * `boundedCall` when you need orphan-safety, sync-throw-safety, or a guaranteed
 * acquire/release around the bounded call.
 */

/** Options controlling {@link boundedCall}. */
export interface BoundedCallOptions {
  /**
   * Upper bound in milliseconds. A non-finite / non-positive value means "no
   * bound": `fn()` is still orphan-safe, sync-throw-safe, and acquire/release
   * paired — it is simply never timed out (no timer is armed).
   */
  timeoutMs: number;
  /**
   * Error message (or a lazy factory) used when the timeout wins the race.
   * Defaults to a generic `Bounded call exceeded <ms>ms`.
   */
  message?: string | (() => string);
  /**
   * Called BEFORE `fn()` to retain a refcounted resource (e.g. a drain pump).
   * Paired 1:1 with {@link onRelease}. If it throws, `fn` is never invoked and
   * `onRelease` never runs (nothing was acquired).
   */
  onAcquire?: () => void;
  /**
   * Called in `finally` to release the resource retained by {@link onAcquire}.
   * ALWAYS runs once the `try` is entered — on success, on timeout, and on a
   * synchronous throw from `fn()`.
   */
  onRelease?: () => void;
  /**
   * `unref()` the timeout timer so a pending guard can never keep the process
   * alive past exit. Default `true`.
   */
  unref?: boolean;
}

function unrefTimer(timer: unknown, unref: boolean): void {
  if (unref && typeof timer === 'object' && timer !== null
    && typeof (timer as { unref?: () => void }).unref === 'function') {
    (timer as { unref: () => void }).unref();
  }
}

/**
 * Await `fn()` bounded by a timeout, with orphan-rejection swallowing,
 * synchronous-throw-safe release, and an optional acquire/release lifecycle.
 * See the module header for the guarantees.
 */
export async function boundedCall<T>(fn: () => Promise<T>, opts: BoundedCallOptions): Promise<T> {
  const { timeoutMs, message, onAcquire, onRelease, unref = true } = opts;
  // Acquire BEFORE the try so a throwing onAcquire does not trigger a release of
  // something that was never acquired (parity with drainRunLoop's retain()).
  onAcquire?.();
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    // `fn()` sits inside the try so a SYNCHRONOUS throw still reaches the finally
    // (release + clearTimeout) — the pump/lock/handle never leaks.
    const work = fn();
    // Orphan guard: if the timeout wins, `work` is abandoned; swallow any late
    // rejection so it never becomes an unhandledRejection. The real outcome
    // (work's own value/error, or the timeout error) still flows through the race.
    work.catch(() => {});
    // Unbounded: no timer, just await the work (still orphan/release safe).
    if (!(timeoutMs > 0 && timeoutMs < Number.POSITIVE_INFINITY)) {
      return await work;
    }
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        const msg = typeof message === 'function'
          ? message()
          : message ?? `Bounded call exceeded ${timeoutMs}ms`;
        reject(new Error(msg));
      }, timeoutMs);
      unrefTimer(timer, unref);
    });
    return await Promise.race([work, timeout]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
    onRelease?.();
  }
}
