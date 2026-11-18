import * as fs from 'fs';
import * as fsp from 'fs/promises';
import * as path from 'path';

/**
 * Canonical cross-process file lock for the whole AMI tree.
 *
 * Multiple processes (e.g. several teammates in a swarm, or a detached run plus
 * an interactive one) mutate the same on-disk state — task files, team files,
 * mailboxes. A naïve read-modify-write races and loses updates. This helper
 * serializes a critical section across processes using an atomic `mkdir` of a
 * lock DIRECTORY (`${target}.lock`) with bounded retry/backoff, so late callers
 * wait for the lock instead of failing immediately, plus stale-lock stealing so
 * a crashed holder cannot wedge the resource forever.
 *
 * Why `mkdir` and not `open(…, 'wx')` (O_EXCL)? Directory creation is atomic on
 * every POSIX filesystem AND over NFS/network mounts, whereas `O_EXCL` file
 * create is NOT reliably atomic over NFS — two sessions sharing a networked
 * state dir could both believe they hold an O_EXCL lock. `mkdir` is the same
 * NFS-safe primitive `proper-lockfile` relies on. The holder PID is recorded in
 * a `pid` entry INSIDE the lock directory (best-effort metadata for liveness
 * staleness); the directory itself is the atomic token.
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
 * Whether `pid` refers to a live process. `process.kill(pid, 0)` sends no signal
 * but validates the target: it succeeds for a live process, raises ESRCH when no
 * such process exists (dead), and EPERM when the process exists but is owned by
 * another user (still alive). Anything that is not an explicit ESRCH is treated
 * as alive so we never steal a lock from a holder that is merely unowned by us.
 */
function isPidAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    return (err as NodeJS.ErrnoException).code !== 'ESRCH';
  }
}

/** Name of the PID metadata entry written inside the lock directory. */
const PID_ENTRY = 'pid';

/** Absolute path to the holder-PID metadata file inside a lock directory. */
function lockPidPath(lockDir: string): string {
  return path.join(lockDir, PID_ENTRY);
}

/** Read the holder PID recorded in the lock directory, or null if absent/unreadable. */
function readLockPid(lockDir: string): number | null {
  try {
    const raw = fs.readFileSync(lockPidPath(lockDir), 'utf-8').trim();
    if (!raw) return null;
    const pid = Number(raw);
    return Number.isInteger(pid) && pid > 0 ? pid : null;
  } catch {
    return null;
  }
}

/**
 * Atomically create the lock DIRECTORY (NFS-safe), then record the holder PID in
 * a metadata entry inside it. `mkdir` is the atomic token; the PID is best-effort
 * liveness metadata read back by {@link readLockPid} for stale-lock stealing.
 */
function createLockDir(lockDir: string): void {
  fs.mkdirSync(lockDir);
  fs.writeFileSync(lockPidPath(lockDir), String(process.pid));
}

/**
 * Release a lock: drop the PID metadata entry (best-effort, `force` so an already
 * absent entry never throws) then `rmdir` the now-empty lock directory. Splitting
 * it this way keeps release to two cheap syscalls (no recursive `readdir` scan) and
 * makes the final `rmdir` the single atomic release step. The `rmdir` is NOT forced:
 * a missing dir throws ENOENT so callers can detect a lock already removed/stolen
 * underneath them (same contract the old `unlink`-based release relied on).
 */
function removeLockDirSync(lockDir: string): void {
  fs.rmSync(lockPidPath(lockDir), { force: true });
  fs.rmdirSync(lockDir);
}

/** Async sibling of {@link removeLockDirSync} — used by the async release path. */
async function removeLockDir(lockDir: string): Promise<void> {
  await fsp.rm(lockPidPath(lockDir), { force: true });
  await fsp.rmdir(lockDir);
}

/**
 * Try to atomically create the lock directory. Returns true on success. If the
 * directory already exists but is abandoned — its holder PID is dead, or
 * (fallback) its mtime is older than staleMs — steals it and retries once.
 */
function tryAcquire(lockDir: string, staleMs: number): boolean {
  try {
    createLockDir(lockDir);
    return true;
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== 'EEXIST') throw err;
    // Lock exists — steal it if the holder is provably gone: either its recorded
    // PID is dead (immediate reclaim — a crashed holder can never release), or
    // (PID unreadable / reused-but-live) its mtime exceeds the staleness cap.
    try {
      const pid = readLockPid(lockDir);
      const deadHolder = pid !== null && !isPidAlive(pid);
      const age = Date.now() - fs.statSync(lockDir).mtimeMs;
      if (deadHolder || age > staleMs) {
        removeLockDirSync(lockDir);
        try {
          createLockDir(lockDir);
          return true;
        } catch {
          return false;
        }
      }
    } catch {
      // stat/remove raced with another cleaner — treat as still held.
    }
    return false;
  }
}

/**
 * Acquire `${target}.lock` (an atomic mkdir'd directory), run `fn`, and release
 * the lock no matter how `fn` settles. The lock dir lives next to the target so
 * callers never collide on a shared global lock. Returns whatever `fn` resolves to.
 */
export async function withFileLock<T>(
  target: string,
  fn: () => Promise<T> | T,
  options: FileLockOptions = {},
): Promise<T> {
  const opts = { ...DEFAULT_LOCK_OPTIONS, ...options };
  const lockDir = `${target}.lock`;
  let backoff = opts.minTimeout;
  let acquired = false;
  for (let attempt = 0; attempt <= opts.retries; attempt++) {
    if (tryAcquire(lockDir, opts.staleMs)) { acquired = true; break; }
    await sleep(backoff);
    backoff = Math.min(backoff * 2, opts.maxTimeout);
  }
  if (!acquired) {
    throw new Error(`Could not acquire lock ${lockDir} after ${opts.retries} retries`);
  }

  // Heartbeat: keep the lock's mtime fresh while fn runs so a concurrent waiter
  // never judges this still-alive holder stale and steals the lock. If the lock
  // is removed or re-created by another process underneath us, surface it.
  const refreshMs = options.refreshMs ?? Math.max(1, Math.floor(opts.staleMs / 2));
  let compromisedFired = false;
  const fireCompromised = (msg: string) => {
    if (compromisedFired) return;
    compromisedFired = true;
    try { options.onCompromised?.(new Error(msg)); } catch { /* ignore handler errors */ }
  };
  let timer: NodeJS.Timeout | undefined;
  if (refreshMs > 0) {
    timer = setInterval(() => {
      try {
        const pid = readLockPid(lockDir);
        if (pid === null || pid !== process.pid) {
          fireCompromised(`Lock ${lockDir} was lost or taken over by another process`);
          return;
        }
        const now = new Date();
        fs.utimesSync(lockDir, now, now);
      } catch {
        fireCompromised(`Lock ${lockDir} disappeared during the critical section`);
      }
    }, refreshMs);
    timer.unref();
  }

  try {
    return await fn();
  } finally {
    if (timer) clearInterval(timer);
    try { await removeLockDir(lockDir); } catch { /* already gone */ }
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
 * read-modify-write built on the sync `fs` helpers). Shares the SAME mkdir'd-directory
 * lock format, PID-based stale-stealing, and `${target}.lock` naming as the async path, so a sync
 * and an async holder of the same target serialize against each other — this is the
 * ONE lock primitive, not a parallel implementation (dedup rule).
 *
 * Because the section is synchronous the thread is blocked throughout, so there is no
 * mid-section heartbeat (nothing else runs on this thread to refresh the mtime); a
 * section longer than `staleMs` can therefore be judged stale by a waiter and have its
 * lock stolen. That exact case is detected on release and reported via `onCompromised`
 * (parity: Claude's stolen-lock → log, not throw). Returns whatever `fn` returns.
 */
export function withFileLockSync<T>(
  target: string,
  fn: () => T,
  options: FileLockSyncOptions = {},
): T {
  const opts = { ...DEFAULT_LOCK_OPTIONS, ...options };
  const lockDir = `${target}.lock`;
  let backoff = opts.minTimeout;
  let acquired = false;
  for (let attempt = 0; attempt <= opts.retries; attempt++) {
    if (tryAcquire(lockDir, opts.staleMs)) { acquired = true; break; }
    syncSleep(backoff);
    backoff = Math.min(backoff * 2, opts.maxTimeout);
  }
  if (!acquired) {
    throw new Error(`Could not acquire lock ${lockDir} after ${opts.retries} retries`);
  }

  try {
    return fn();
  } finally {
    // Stale-write detection: if the lock no longer records our PID (stolen by a
    // waiter after we overran staleMs, or vanished), the section was not mutually
    // exclusive. Report it and do NOT delete the dir — it now belongs to the thief.
    let compromised = false;
    try {
      const pid = readLockPid(lockDir);
      if (pid === null || pid !== process.pid) compromised = true;
    } catch {
      compromised = true;
    }
    if (compromised) {
      try { options.onCompromised?.(new Error(`Lock ${lockDir} was lost or taken over during the critical section`)); } catch { /* ignore handler errors */ }
    } else {
      try { removeLockDirSync(lockDir); } catch { /* already gone */ }
    }
  }
}
