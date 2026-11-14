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
}

export const DEFAULT_LOCK_OPTIONS: Required<FileLockOptions> = {
  retries: 30,
  minTimeout: 5,
  maxTimeout: 100,
  staleMs: 30_000,
};

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/**
 * Try to atomically create the lock file. Returns true on success. If the file
 * already exists but is stale (older than staleMs), steals it and retries once.
 */
function tryAcquire(lockFile: string, staleMs: number): boolean {
  try {
    const fd = fs.openSync(lockFile, 'wx');
    try {
      fs.writeSync(fd, String(process.pid));
    } finally {
      fs.closeSync(fd);
    }
    return true;
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== 'EEXIST') throw err;
    // Lock exists — steal it if abandoned (holder crashed without releasing).
    try {
      const age = Date.now() - fs.statSync(lockFile).mtimeMs;
      if (age > staleMs) {
        fs.unlinkSync(lockFile);
        try {
          const fd = fs.openSync(lockFile, 'wx');
          try { fs.writeSync(fd, String(process.pid)); } finally { fs.closeSync(fd); }
          return true;
        } catch { return false; }
      }
    } catch {
      // stat/unlink raced with another cleaner — treat as still held.
    }
    return false;
  }
}

/**
 * Acquire `${target}.lock`, run `fn`, and release the lock no matter how `fn`
 * settles. The lock file lives next to the target so callers never collide on a
 * shared global lock. Returns whatever `fn` resolves to.
 */
export async function withFileLock<T>(
  target: string,
  fn: () => Promise<T> | T,
  options: FileLockOptions = {},
): Promise<T> {
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
  try {
    return await fn();
  } finally {
    try { await fsp.unlink(lockFile); } catch { /* already gone */ }
  }
}
