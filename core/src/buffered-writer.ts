/**
 * Generic batched/buffered writer (Claude-parity `createBufferedWriter`,
 * utils/bufferedWriter.ts).
 *
 * A write sink whose `writeFn` may BLOCK (e.g. a synchronous `appendFileSync` to
 * a log file) is expensive to call once per record on a hot path. This writer
 * coalesces records into a single `writeFn` call, flushing either when a timer
 * fires (`flushIntervalMs`) or when the buffer crosses a size bound
 * (`maxBufferSize` records / `maxBufferBytes` bytes). Overflow flushes are
 * DEFERRED: the buffer is detached synchronously and the actual (possibly
 * blocking) write happens on the next `setImmediate`, so the caller's tick —
 * which may be mid-render or mid-keystroke — never waits on the sink. A
 * `pendingOverflow` batch that hasn't drained yet is COALESCED into (not
 * reordered around) so records always land in order, and `flush()`/`dispose()`
 * drain any pending overflow SYNCHRONOUSLY so nothing is lost if the process
 * exits before the deferred write fires.
 *
 * `immediateMode` turns off all buffering (every `write` goes straight to
 * `writeFn`) — used so a debug/verbose session gets its records on disk right
 * away rather than up to `flushIntervalMs` later.
 */

/** The underlying (possibly blocking) write function the buffer feeds. */
export type WriteFn = (content: string) => void;

/** Handle returned by {@link createBufferedWriter}. */
export interface BufferedWriter {
  /** Queue a record (or write it immediately in `immediateMode`). */
  write: (content: string) => void;
  /** Synchronously drain pending overflow + buffer to `writeFn`. */
  flush: () => void;
  /** Flush and release the flush timer; safe to call repeatedly. */
  dispose: () => void;
}

/** Tunables for {@link createBufferedWriter}. */
export interface BufferedWriterOptions {
  /** The underlying write sink. */
  writeFn: WriteFn;
  /** Idle interval (ms) after which a non-empty buffer auto-flushes. Default 1000. */
  flushIntervalMs?: number;
  /** Record-count bound that triggers a deferred overflow flush. Default 100. */
  maxBufferSize?: number;
  /** Byte bound (sum of record lengths) that triggers a deferred overflow flush. Default Infinity. */
  maxBufferBytes?: number;
  /** When true, bypass all buffering and write every record immediately. Default false. */
  immediateMode?: boolean;
}

/**
 * Create a {@link BufferedWriter}. See the module docstring for the batching /
 * deferred-overflow / ordering semantics.
 */
export function createBufferedWriter({
  writeFn,
  flushIntervalMs = 1000,
  maxBufferSize = 100,
  maxBufferBytes = Infinity,
  immediateMode = false,
}: BufferedWriterOptions): BufferedWriter {
  let buffer: string[] = [];
  let bufferBytes = 0;
  let flushTimer: ReturnType<typeof setTimeout> | null = null;
  // A batch that overflow detached but that the deferred write has not consumed
  // yet. Tracked so flush()/dispose() can drain it synchronously if the process
  // exits before the setImmediate fires.
  let pendingOverflow: string[] | null = null;

  function clearTimer(): void {
    if (flushTimer) {
      clearTimeout(flushTimer);
      flushTimer = null;
    }
  }

  function flush(): void {
    // Drain any deferred-overflow batch first so ordering is preserved.
    if (pendingOverflow) {
      writeFn(pendingOverflow.join(''));
      pendingOverflow = null;
    }
    if (buffer.length === 0) {
      // Still clear a dangling timer even when the buffer emptied via overflow.
      clearTimer();
      return;
    }
    writeFn(buffer.join(''));
    buffer = [];
    bufferBytes = 0;
    clearTimer();
  }

  function scheduleFlush(): void {
    if (!flushTimer) {
      flushTimer = setTimeout(flush, flushIntervalMs);
      // Don't keep the event loop alive just to flush a log buffer.
      flushTimer.unref();
    }
  }

  // Detach the buffer synchronously so the caller never waits on writeFn; perform
  // the (possibly blocking) write on the next setImmediate.
  function flushDeferred(): void {
    if (pendingOverflow) {
      // A previous overflow write is still queued — coalesce into it so writes
      // land in a single setImmediate-ordered batch and never reorder.
      pendingOverflow.push(...buffer);
      buffer = [];
      bufferBytes = 0;
      clearTimer();
      return;
    }
    const detached = buffer;
    buffer = [];
    bufferBytes = 0;
    clearTimer();
    pendingOverflow = detached;
    setImmediate(() => {
      const toWrite = pendingOverflow;
      pendingOverflow = null;
      if (toWrite) writeFn(toWrite.join(''));
    });
  }

  return {
    write(content: string): void {
      if (immediateMode) {
        writeFn(content);
        return;
      }
      buffer.push(content);
      bufferBytes += content.length;
      scheduleFlush();
      if (buffer.length >= maxBufferSize || bufferBytes >= maxBufferBytes) {
        flushDeferred();
      }
    },
    flush,
    dispose(): void {
      flush();
    },
  };
}
