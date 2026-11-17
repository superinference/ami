import * as fs from 'fs';
import * as fsp from 'fs/promises';

/**
 * Canonical cross-process file lock for the whole AMI tree.
 *
 * Multiple processes (e.g. several teammates in a swarm, or a detached run plus
 * an interactive one) mutate the same on-disk state — task files, team files,
 * mailboxes. A naïve read-modify-write races and loses updates. This helper
 * serializes a critical section across processes using an exclusive-create lock
 * file (`wx` flag — only one creator wins) with bounded retry/backoff, so late
 * callers wait for the lock instead of failing immediately, plus stale-lock
 * stealing so a crashed holder cannot wedge the resource forever.
 *
 * Staleness is resolved two ways, strongest first: the lock records its holder's
 * PID, so if that PID is no longer alive the lock is reclaimed IMMEDIATELY (a
 * crashed holder can never release); failing that (PID unreadable, or a reused
 * PID that happens to be live) the mtime age acts as a hard max-age fallback.
 * While a critical section runs, a heartbeat refreshes the lock mtime so a
 * long-running but still-alive holder is never falsely judged stale and stolen,
 * and an optional `onCompromised` callback fires if the lock is lost or taken
 * over underneath the holder.
 *
 * This is the ONE lock primitive; do not hand-roll another (dedup rule). The
 * retry budget mirrors Claude Code's swarm lock sizing (retries×backoff tuned
 * for ~10 concurrent writers each doing a short readdir+read+write section).
 */
export interface FileLockOptions {
  /** Max acquisition attempts before giving up (default 30). */
  retries?: number;
  /** Initial backoff in ms, doubled each retry up to maxTimeout (default 5). */
  minTimeout?: number;
  /** Backoff ceiling in ms (default 100). */
  maxTimeout?: number;
  /** A lock older than this (ms) is considered abandoned and stolen (default 30000). */
  staleMs?: number;
  /**
   * While the critical section runs, the lock's mtime is refreshed on this
   * interval (ms) so a long-running holder proves liveness and is never falsely
   * judged stale by a waiter. Defaults to half of `staleMs` (clamped ≥ 1ms).
   * Pass `0` to disable the heartbeat.
   */
  refreshMs?: number;
  /**
   * Fired (at most once) if the heartbeat detects the lock was lost or taken
   * over by another process (file deleted, or now records a different PID) while
   * `fn` was still running. The critical section is no longer mutually exclusive;
   * the callback lets the caller abort or compensate. Callback errors are
   * swallowed so a faulty handler cannot crash the heartbeat timer.
   */
  onCompromised?: (err: Error) => void;
}

export const DEFAULT_LOCK_OPTIONS: Required<
  Pick<FileLockOptions, 'retries' | 'minTimeout' | 'maxTimeout' | 'staleMs'>
> = {
  retries: 30,
  minTimeout: 5,
  maxTimeout: 100,
  staleMs: 30_000,
};

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/**
 * Block the current thread for `ms` without spinning the CPU. Used by the
 * SYNCHRONOUS lock path ({@link withFileLockSync}) where the critical section is
 * itself synchronous (e.g. a config read-modify-write) and cannot `await`. Uses a
 * throwaway `SharedArrayBuffer` + `Atomics.wait`, which parks the thread for the
 * timeout and returns `'timed-out'` — a real sleep, not a busy loop. A non-positive
 * duration is a no-op.
 */
function syncSleep(ms: number): void {
  if (ms <= 0) return;
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

/**
 * Whether `pid` refers to a live process — the ONE canonical PID-liveness probe
 * for the whole AMI tree (consolidates the formerly hand-rolled copies in
 * `session.ts` `isPidAlive` and `cron.ts` `isProcessRunning`; dedup rule). It is
 * the strongest staleness signal every file lock shares: a crashed holder's PID
 * goes dead, so its lock is reclaimed immediately rather than wedging the resource.
 *
 * `process.kill(pid, 0)` sends no signal but validates the target: it succeeds for
 * a live process, raises ESRCH when no such process exists (dead), and EPERM when
 * the process exists but is owned by another user (still ALIVE — never steal a lock
 * from a holder merely unowned by us). A non-integer, zero, negative, or sentinel
 * PID (`<= 1`: 0 targets the caller's own process group, 1 is init/systemd) can
 * never be a legitimate holder and is treated as dead so a corrupt/planted value
 * cannot make the lock un-reclaimable (parity: Claude's pidLock `pid <= 1` guard).
 */
export function isPidAlive(pid: number): boolean {
  if (!Number.isInteger(pid) || pid <= 1) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code === 'ESRCH') return false;
    if (code === 'EPERM') return true;
    return false;
  }
}

/** Read the holder PID recorded in the lock file, or null if absent/unreadable. */
function readLockPid(lockFile: string): number | null {
  try {
    const raw = fs.readFileSync(lockFile, 'utf-8').trim();
    if (!raw) return null;
    const pid = Number(raw);
    return Number.isInteger(pid) && pid > 0 ? pid : null;
  } catch {
    return null;
  }
}

function createLockFile(lockFile: string): void {
  const fd = fs.openSync(lockFile, 'wx');
  try {
    fs.writeSync(fd, String(process.pid));
  } finally {
    fs.closeSync(fd);
  }
}

/**
 * Try to atomically create the lock file. Returns true on success. If the file
 * already exists but is abandoned — its holder PID is dead, or (fallback) its
 * mtime is older than staleMs — steals it and retries once.
 */
function tryAcquire(lockFile: string, staleMs: number): boolean {
  try {
    createLockFile(lockFile);
    return true;
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== 'EEXIST') throw err;
    // Lock exists — steal it if the holder is provably gone: either its recorded
    // PID is dead (immediate reclaim — a crashed holder can never release), or
    // (PID unreadable / reused-but-live) its mtime exceeds the staleness cap.
    try {
      const pid = readLockPid(lockFile);
      const deadHolder = pid !== null && !isPidAlive(pid);
      const age = Date.now() - fs.statSync(lockFile).mtimeMs;
      if (deadHolder || age > staleMs) {
        fs.unlinkSync(lockFile);
        try {
          createLockFile(lockFile);
          return true;
        } catch {
          return false;
        }
      }
    } catch {
      // stat/unlink raced with another cleaner — treat as still held.
    }
    return false;
  }
}

/**
 * A held lock returned by {@link acquireFileLock} — the acquire-and-HOLD handle
 * form of the lock primitive (parity: proper-lockfile's `lock()` returning a
 * release fn, as opposed to the scoped `withFileLock(fn)` wrapper). Use it when
 * the critical section is NOT a single callback — the lock is acquired at one
 * point and released much later (a different call / lifecycle stage), e.g. a
 * session or scheduler that owns a resource for its whole run.
 */
export interface FileLockHandle {
  /** Release the lock and stop its heartbeat. Idempotent — calling it more than
   *  once is a no-op (parity: proper-lockfile's release fn). */
  release(): Promise<void>;
  /** True once the heartbeat has detected the lock was lost or taken over by
   *  another process while still held (the same condition that fires
   *  `onCompromised`). Lets a long-lived holder poll liveness without a callback. */
  readonly compromised: boolean;
}

/**
 * Acquire `${target}.lock` and RETURN A HANDLE to release it later, instead of
 * wrapping a callback. This is the acquire-and-hold form of the one lock
 * primitive (parity: proper-lockfile `lock()`): same exclusive-create, bounded
 * retry/backoff, PID + mtime stale-stealing, mtime-refresh heartbeat, and
 * `onCompromised` surfacing as {@link withFileLock} — {@link withFileLock} is now
 * a thin scoped wrapper over this (acquire → run fn → release), so there is ONE
 * acquire/heartbeat/release implementation, not two (dedup rule).
 *
 * Throws if the lock cannot be acquired within the retry budget. Always pair a
 * successful acquire with {@link FileLockHandle.release} (use try/finally) so the
 * lock and its heartbeat timer are cleaned up.
 */
export async function acquireFileLock(
  target: string,
  options: FileLockOptions = {},
): Promise<FileLockHandle> {
  const opts = { ...DEFAULT_LOCK_OPTIONS, ...options };
  const lockFile = `${target}.lock`;
  let backoff = opts.minTimeout;
  let acquired = false;
  for (let attempt = 0; attempt <= opts.retries; attempt++) {
    if (tryAcquire(lockFile, opts.staleMs)) { acquired = true; break; }
    await sleep(backoff);
    backoff = Math.min(backoff * 2, opts.maxTimeout);
  }
  if (!acquired) {
    throw new Error(`Could not acquire lock ${lockFile} after ${opts.retries} retries`);
  }

  // Heartbeat: keep the lock's mtime fresh while the lock is held so a concurrent
  // waiter never judges this still-alive holder stale and steals the lock. If the
  // lock is removed or re-created by another process underneath us, surface it.
  const refreshMs = options.refreshMs ?? Math.max(1, Math.floor(opts.staleMs / 2));
  let compromised = false;
  const fireCompromised = (msg: string) => {
    if (compromised) return;
    compromised = true;
    try { options.onCompromised?.(new Error(msg)); } catch { /* ignore handler errors */ }
  };
  let timer: NodeJS.Timeout | undefined;
  if (refreshMs > 0) {
    timer = setInterval(() => {
      try {
        const pid = readLockPid(lockFile);
        if (pid === null || pid !== process.pid) {
          fireCompromised(`Lock ${lockFile} was lost or taken over by another process`);
          return;
        }
        const now = new Date();
        fs.utimesSync(lockFile, now, now);
      } catch {
        fireCompromised(`Lock ${lockFile} disappeared during the critical section`);
      }
    }, refreshMs);
    timer.unref();
  }

  let released = false;
  return {
    get compromised() { return compromised; },
    async release() {
      if (released) return;
      released = true;
      if (timer) clearInterval(timer);
      try { await fsp.unlink(lockFile); } catch { /* already gone */ }
    },
  };
}

/**
 * Acquire `${target}.lock`, run `fn`, and release the lock no matter how `fn`
 * settles. The lock file lives next to the target so callers never collide on a
 * shared global lock. Returns whatever `fn` resolves to. A thin scoped wrapper
 * over {@link acquireFileLock}.
 */
export async function withFileLock<T>(
  target: string,
  fn: () => Promise<T> | T,
  options: FileLockOptions = {},
): Promise<T> {
  const handle = await acquireFileLock(target, options);
  try {
    return await fn();
  } finally {
    await handle.release();
  }
}

/** Options for {@link withFileLockSync} (the synchronous sibling of {@link withFileLock}). */
export interface FileLockSyncOptions {
  /** Max acquisition attempts before giving up (default 30). */
  retries?: number;
  /** Initial backoff in ms, doubled each retry up to maxTimeout (default 5). */
  minTimeout?: number;
  /** Backoff ceiling in ms (default 100). */
  maxTimeout?: number;
  /** A lock older than this (ms) is considered abandoned and stolen (default 30000). */
  staleMs?: number;
  /**
   * Fired (at most once) when, on release, the lock is found to have been lost or
   * taken over by another process (deleted, or now records a different PID) while
   * the synchronous critical section ran — i.e. the section exceeded `staleMs` and a
   * waiter judged this holder stale and stole the lock, so mutual exclusion was no
   * longer guaranteed. Parity: Claude Code's `saveConfigWithLock` installs an
   * `onCompromised` handler that DOWNGRADES a stolen lock to a log instead of letting
   * the default handler throw an unhandled exception. Callback errors are swallowed.
   */
  onCompromised?: (err: Error) => void;
}

/**
 * Synchronous cross-process advisory lock — the sibling of {@link withFileLock} for
 * critical sections that are themselves synchronous and cannot `await` (e.g. a config
 * read-modify-write built on the sync `fs` helpers). Shares the SAME lockfile format,
 * PID-based stale-stealing, and `${target}.lock` naming as the async path, so a sync
 * and an async holder of the same target serialize against each other — this is the
 * ONE lock primitive, not a parallel implementation (dedup rule).
 *
 * Because the section is synchronous the thread is blocked throughout, so there is no
 * mid-section heartbeat (nothing else runs on this thread to refresh the mtime); a
 * section longer than `staleMs` can therefore be judged stale by a waiter and have its
 * lock stolen. That exact case is detected on release and reported via `onCompromised`
 * (parity: Claude's stolen-lock → log, not throw). Returns whatever `fn` returns.
 */
/**
 * A held synchronous lock returned by {@link acquireFileLockSync} — the
 * acquire-and-HOLD handle form of the sync lock (parity: proper-lockfile
 * `lockSync()` returning a release fn). Like its async sibling but with no
 * mid-section heartbeat (the sync path blocks the thread), so a hold that outlives
 * `staleMs` can be stolen; that is detected on {@link release} and reported via
 * `onCompromised`.
 */
export interface FileLockHandleSync {
  /** Release the lock. Idempotent. If the lock was stolen while held (its PID no
   *  longer matches ours, or it vanished) it is NOT deleted — it belongs to the
   *  thief now — and `onCompromised` fires instead. */
  release(): void;
  /** True once {@link release} found the lock had been lost or taken over. */
  readonly compromised: boolean;
}

/**
 * Acquire `${target}.lock` synchronously and RETURN A HANDLE to release it later
 * (parity: proper-lockfile `lockSync()`). The acquire-and-hold form of
 * {@link withFileLockSync}, which is now a thin scoped wrapper over this — ONE
 * sync acquire/release implementation (dedup rule). Shares the exact lockfile
 * format, PID-based stale-stealing, and `${target}.lock` naming of the async path,
 * so sync and async holders of the same target serialize against each other.
 *
 * Because the thread is not blocked between acquire and release, this handle form
 * (unlike {@link withFileLockSync}) may be held across work that yields to the
 * event loop — but with NO heartbeat the hold must stay under `staleMs` or a
 * waiter may judge it stale and steal it (surfaced via `onCompromised` on release).
 */
export function acquireFileLockSync(
  target: string,
  options: FileLockSyncOptions = {},
): FileLockHandleSync {
  const opts = { ...DEFAULT_LOCK_OPTIONS, ...options };
  const lockFile = `${target}.lock`;
  let backoff = opts.minTimeout;
  let acquired = false;
  for (let attempt = 0; attempt <= opts.retries; attempt++) {
    if (tryAcquire(lockFile, opts.staleMs)) { acquired = true; break; }
    syncSleep(backoff);
    backoff = Math.min(backoff * 2, opts.maxTimeout);
  }
  if (!acquired) {
    throw new Error(`Could not acquire lock ${lockFile} after ${opts.retries} retries`);
  }

  let released = false;
  let compromised = false;
  return {
    get compromised() { return compromised; },
    release() {
      if (released) return;
      released = true;
      // Stale-write detection: if the lock no longer records our PID (stolen by a
      // waiter after we overran staleMs, or vanished), the section was not mutually
      // exclusive. Report it and do NOT delete the file — it now belongs to the thief.
      let comp = false;
      try {
        const pid = readLockPid(lockFile);
        if (pid === null || pid !== process.pid) comp = true;
      } catch {
        comp = true;
      }
      if (comp) {
        compromised = true;
        try { options.onCompromised?.(new Error(`Lock ${lockFile} was lost or taken over during the critical section`)); } catch { /* ignore handler errors */ }
      } else {
        try { fs.unlinkSync(lockFile); } catch { /* already gone */ }
      }
    },
  };
}

export function withFileLockSync<T>(
  target: string,
  fn: () => T,
  options: FileLockSyncOptions = {},
): T {
  const handle = acquireFileLockSync(target, options);
  try {
    return fn();
  } finally {
    handle.release();
  }
}
