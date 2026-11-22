/**
 * Generic async serialization wrapper.
 *
 * Parity with the reference's `utils/sequential.ts`. {@link sequential} wraps an
 * arbitrary async function so concurrent invocations run strictly ONE AT A TIME
 * in FIFO order, while every caller still gets its OWN resolved value back (or
 * its own rejection). It preserves the caller's `this` context, guards against
 * re-entrancy (a single drain loop), and re-checks the queue after the loop
 * drains so items enqueued during the idle transition are not stranded.
 *
 * Use it to protect any read-then-write critical section that would corrupt or
 * lose data if two calls interleaved — e.g. the memory-index append/remove path
 * ({@link MemoryManager.addToMemoryIndex}), where two concurrent appends could
 * each read the pre-write content and the second clobber the first's entry.
 *
 * This is the ONE canonical in-process async serializer (No-Duplication rule):
 * domain code that needs "run this async fn exclusive and get its result" should
 * wrap it here rather than hand-rolling a mutex/queue.
 */
type QueueItem<T extends unknown[], R> = {
  args: T;
  resolve: (value: R) => void;
  reject: (reason?: unknown) => void;
  context: unknown;
};

/**
 * Wrap `fn` so concurrent calls are serialized FIFO. The returned function has
 * the same signature as `fn`; each call resolves/rejects with that call's own
 * result once it reaches the front of the queue.
 *
 * @param fn - The async function to serialize.
 * @returns A wrapped function that executes calls one at a time, in order.
 */
export function sequential<T extends unknown[], R>(
  fn: (...args: T) => Promise<R>,
): (...args: T) => Promise<R> {
  const queue: QueueItem<T, R>[] = [];
  let processing = false;

  async function processQueue(): Promise<void> {
    // Re-entrancy guard: only one drain loop runs at a time.
    // Callers always push before invoking processQueue, so an empty-queue
    // early-return is unreachable here (the while below is the sole drain gate).
    if (processing) return;

    processing = true;

    while (queue.length > 0) {
      const { args, resolve, reject, context } = queue.shift()!;
      try {
        // Preserve the original `this` context the caller invoked with.
        const result = await fn.apply(context, args);
        resolve(result);
      } catch (error) {
        // A rejection is isolated to its own caller; the queue keeps draining.
        reject(error);
      }
    }

    processing = false;

    // Items enqueued during the last `await` are already drained by the while
    // loop above (the condition re-checks after each item). A post-drain
    // re-entry would only matter if work could appear between the empty-queue
    // observation and clearing `processing` — which cannot happen on the JS
    // event loop without an await between those statements. Keep the flag
    // clear so a subsequent caller starts a fresh drain.
  }

  return function (this: unknown, ...args: T): Promise<R> {
    return new Promise<R>((resolve, reject) => {
      queue.push({ args, resolve, reject, context: this });
      void processQueue();
    });
  };
}
