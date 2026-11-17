/**
 * Auto heap-dump guard.
 *
 * PARITY (additive): Claude Code's `heapDumpService` captures a V8 heap snapshot the
 * first time process RSS crosses a high-water threshold (its `auto-1.5GB` trigger) so a
 * leaking long-running session leaves a post-mortem artifact to analyse — not just a log
 * line. AMI previously had NO memory-warning path at all; this is the building block that
 * the engine turn-loop invokes when `heapDumpOnMemoryWarning` is enabled.
 *
 * The guard is a self-contained core primitive (no engine/UI dependency): every side
 * effect (reading RSS, writing the snapshot, naming the file, the clock) is injectable so
 * it is fully deterministic under test, and it fires AT MOST ONCE per instance (matching
 * Claude's one-shot capture — repeatedly snapshotting a process that is already over the
 * line would only make the pressure worse).
 */
import { writeHeapSnapshot } from 'node:v8';
import * as path from 'node:path';
import * as os from 'node:os';

/** Default RSS high-water mark that triggers a heap dump: 1.5 GB (parity: heapDumpService auto-1.5GB). */
export const HEAP_DUMP_RSS_THRESHOLD_BYTES = 1_500 * 1024 * 1024;

/** Injectable side effects for {@link HeapDumpGuard} (deterministic tests). */
export interface HeapDumpDeps {
  /** Current resident-set-size in bytes. Default: `process.memoryUsage().rss`. */
  rss?: () => number;
  /** Write a heap snapshot to `file` and return the path actually written.
   *  Default: V8's `writeHeapSnapshot`. */
  writeSnapshot?: (file: string) => string;
  /** Directory the snapshot is written to. Default: `os.tmpdir()`. */
  dir?: () => string;
  /** Monotonic-ish timestamp used in the snapshot filename. Default: `Date.now`. */
  now?: () => number;
}

/**
 * Fires a one-shot V8 heap snapshot when RSS first crosses {@link threshold}. A reader or
 * writer that throws is swallowed (best-effort: a failed dump must never crash the turn
 * loop). Once a dump is taken, {@link hasFired} stays true and subsequent {@link check}s
 * are no-ops.
 */
export class HeapDumpGuard {
  private _fired = false;
  private readonly _threshold: number;
  private readonly _rss: () => number;
  private readonly _write: (file: string) => string;
  private readonly _dir: () => string;
  private readonly _now: () => number;

  /**
   * @param threshold RSS bytes that trigger a dump; a non-positive / non-finite value
   *   falls back to {@link HEAP_DUMP_RSS_THRESHOLD_BYTES}.
   */
  constructor(threshold?: number, deps: HeapDumpDeps = {}) {
    this._threshold =
      typeof threshold === 'number' && Number.isFinite(threshold) && threshold > 0
        ? threshold
        : HEAP_DUMP_RSS_THRESHOLD_BYTES;
    this._rss = deps.rss ?? (() => process.memoryUsage().rss);
    this._write = deps.writeSnapshot ?? ((file: string) => writeHeapSnapshot(file));
    this._dir = deps.dir ?? (() => os.tmpdir());
    this._now = deps.now ?? Date.now;
  }

  /** Whether a dump has already been captured by this guard. */
  get hasFired(): boolean {
    return this._fired;
  }

  /** The RSS threshold (bytes) this guard fires at. */
  get threshold(): number {
    return this._threshold;
  }

  /**
   * Take a heap snapshot if RSS is at/over the threshold and none has been taken yet.
   * @returns the written snapshot path, or `null` if nothing was captured (below
   *   threshold, already fired, or a reader/writer error).
   */
  check(): string | null {
    if (this._fired) return null;
    let rss: number;
    try {
      rss = this._rss();
    } catch {
      return null;
    }
    if (!(rss >= this._threshold)) return null;
    // Mark fired BEFORE writing so a slow/throwing writer cannot be retried on every turn.
    this._fired = true;
    const file = path.join(this._dir(), `ami-heap-${this._now()}.heapsnapshot`);
    try {
      return this._write(file);
    } catch {
      return null;
    }
  }
}
