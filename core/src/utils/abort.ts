/**
 * Abort-signal listener-cap helpers — parity with Claude Code's
 * `createAbortController` (utils/abortController.ts), which calls
 * `setMaxListeners(DEFAULT_MAX_LISTENERS)` on each AbortSignal so many abort
 * listeners can attach without Node emitting a `MaxListenersExceededWarning`
 * (default cap 10).
 *
 * Any place that attaches many `'abort'` listeners to ONE shared signal needs
 * this: the tool executor fans a child abort signal over every runnable entry in
 * a batch (one listener per parallel tool on the shared engine signal), and a
 * sub-agent spawn links its child controller to the parent turn signal AND
 * forwards that signal to nested grandchildren. A turn with more than ~10
 * concurrent attachments would trip `MaxListenersExceededWarning` on stderr,
 * polluting detached/TUI output. Raising the cap on the signal before attaching
 * avoids that without masking a genuine leak (the cap is a generous fixed bound,
 * not unlimited).
 *
 * This is the ONE canonical home for the primitive (No-Duplication rule); the
 * common layer re-exports it so cli/common consumers share this exact impl rather
 * than hand-rolling a second `setMaxListeners` call.
 */
import { setMaxListeners } from 'node:events';

/** Matches Claude's DEFAULT_MAX_LISTENERS. */
export const DEFAULT_ABORT_MAX_LISTENERS = 50;

/**
 * Raise the max-listeners cap on an AbortSignal so many `'abort'` listeners can
 * attach without a warning. Idempotent and best-effort: if the runtime does not
 * support `setMaxListeners` on an `EventTarget`, it silently no-ops.
 */
export function raiseAbortListenerCap(signal: AbortSignal, max = DEFAULT_ABORT_MAX_LISTENERS): void {
  try {
    setMaxListeners(max, signal);
  } catch {
    /* best-effort: older runtimes may not accept an EventTarget here */
  }
}

/**
 * Create an AbortController whose signal has an already-raised listener cap, so a
 * controller that will have many listeners attached (e.g. a sub-agent's signal
 * forwarded to nested grandchildren) never trips `MaxListenersExceededWarning`.
 */
export function createBoundedAbortController(max = DEFAULT_ABORT_MAX_LISTENERS): AbortController {
  const controller = new AbortController();
  raiseAbortListenerCap(controller.signal, max);
  return controller;
}
