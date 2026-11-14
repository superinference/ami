import * as fs from 'fs';
import * as path from 'path';

/**
 * Shared crash-safe, owner-only filesystem writers.
 *
 * This is the ONE canonical atomic/secure write helper for the whole codebase.
 * Session persistence (core/session.ts) and the FRITO quota/savings/config
 * stores (common/frito) both import from here instead of re-implementing a
 * temp+rename or a 0o600 chmod dance per site. Keep it dependency-free so core
 * and common can share it.
 */

/** Default owner-only file permission for persisted artifacts that may hold
 *  secrets, transcripts, or PII (0o600 → rw for owner only). */
export const OWNER_ONLY_FILE_MODE = 0o600;
/** Default owner-only directory permission (0o700 → rwx for owner only). */
export const OWNER_ONLY_DIR_MODE = 0o700;

/**
 * Mockable filesystem seam (ESM-safe). The ESM `* as fs` namespace is frozen and
 * cannot be patched with `mock.method`, so production code routes its fs calls
 * through this object (defaulting to the real `fs`). Tests exercise the atomic
 * fallback / crash paths with `mock.method(fsHooks, 'renameSync', …)`.
 */
export const fsHooks = {
  mkdirSync: fs.mkdirSync as typeof fs.mkdirSync,
  chmodSync: fs.chmodSync as typeof fs.chmodSync,
  writeFileSync: fs.writeFileSync as typeof fs.writeFileSync,
  readFileSync: fs.readFileSync as typeof fs.readFileSync,
  renameSync: fs.renameSync as typeof fs.renameSync,
  unlinkSync: fs.unlinkSync as typeof fs.unlinkSync,
};

export interface AtomicWriteOptions {
  /** File permission bits. Default {@link OWNER_ONLY_FILE_MODE} (0o600). */
  mode?: number;
  /** Permission bits for directories created along the way. Default 0o700. */
  dirMode?: number;
  encoding?: BufferEncoding;
}

/**
 * Create a directory (recursively) with owner-only permissions. `mkdir`'s `mode`
 * is ignored when the directory already exists and is subject to the process
 * umask on creation, so the mode is re-asserted with an explicit chmod. The
 * chmod is best-effort: a directory we do not own (or a platform without POSIX
 * permissions) may reject it, which must not abort the caller.
 */
export function secureMkdirSync(dir: string, mode: number = OWNER_ONLY_DIR_MODE): void {
  fsHooks.mkdirSync(dir, { recursive: true, mode });
  try {
    fsHooks.chmodSync(dir, mode);
  } catch {
    /* not owned by us / no POSIX perms — ignore */
  }
}

/**
 * Crash-safe, owner-only file write. Writes to a per-process temp file with
 * `flush: true` (fsync the data to disk), applies the mode, then atomically
 * renames it over the target — atomic on POSIX for same-filesystem renames, so
 * a crash / power loss / ENOSPC mid-write can never leave a truncated or
 * corrupt target. On any failure the temp file is cleaned up and a direct
 * (non-atomic) write is attempted as a last resort so a save is never silently
 * lost. The parent directory is created 0o700 and the file written 0o600 by
 * default (override via `opts`).
 */
export function atomicWriteFileSync(
  target: string,
  data: string,
  opts: AtomicWriteOptions = {},
): void {
  const mode = opts.mode ?? OWNER_ONLY_FILE_MODE;
  const dirMode = opts.dirMode ?? OWNER_ONLY_DIR_MODE;
  const encoding = opts.encoding ?? 'utf-8';
  secureMkdirSync(path.dirname(target), dirMode);
  const tmp = `${target}.tmp.${process.pid}.${Date.now()}`;
  try {
    fsHooks.writeFileSync(tmp, data, { encoding, mode, flush: true });
    // writeFileSync's mode is ignored if the temp file already existed — enforce it.
    fsHooks.chmodSync(tmp, mode);
    fsHooks.renameSync(tmp, target);
    // rename preserves the temp file's mode, but re-assert on all filesystems.
    try {
      fsHooks.chmodSync(target, mode);
    } catch {
      /* best effort */
    }
  } catch {
    // Clean up the temp file, then fall back to a non-atomic write.
    try {
      fsHooks.unlinkSync(tmp);
    } catch {
      /* already gone */
    }
    fsHooks.writeFileSync(target, data, { encoding, mode, flush: true });
    try {
      fsHooks.chmodSync(target, mode);
    } catch {
      /* best effort */
    }
  }
}
