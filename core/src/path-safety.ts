import * as path from 'path';
import * as fs from 'fs';

/**
 * Why a path was rejected by {@link assertPathWithin}. Lets callers branch on the
 * specific attack vector (null-byte injection, lexical escape, or symlink escape)
 * rather than string-matching a single generic message.
 */
export type PathTraversalReason = 'null-byte' | 'out-of-base' | 'symlink-escape';

/**
 * Error thrown by {@link assertPathWithin} when a path fails containment. Extends
 * `Error` (so existing `instanceof Error` / `/Path traversal/`-message consumers keep
 * working) and adds a machine-readable {@link PathTraversalReason} for granular,
 * per-vector handling.
 */
export class PathTraversalError extends Error {
  readonly reason: PathTraversalReason;
  constructor(message: string, reason: PathTraversalReason) {
    super(message);
    this.name = 'PathTraversalError';
    this.reason = reason;
  }
}

/** True if `child` is the same as, or nested inside, `parent` (both already resolved). */
function isWithin(child: string, parent: string): boolean {
  return child === parent || child.startsWith(parent + path.sep);
}

/**
 * Canonicalize an absolute path, resolving symlinks in its existing prefix.
 *
 * `fs.realpathSync` fails on paths that do not exist yet (write targets), so we realpath
 * the deepest ancestor that DOES exist and re-append the not-yet-created tail lexically.
 * This mirrors the behaviour needed for new-file writes while still following any symlink
 * in the existing portion — the part an attacker can point outside the base dir.
 *
 * Fail-closed: a symlink cycle (`ELOOP`) in the existing prefix cannot be verified, so it
 * is treated as an escape. Other, unrelated errors (e.g. `EACCES`) fall back to the lexical
 * path so this layer never introduces a false rejection for a legitimately-contained path.
 *
 * `realpath` is injectable purely so tests can drive the error branches deterministically;
 * production always uses `fs.realpathSync`.
 */
export function canonicalizeExisting(
  resolved: string,
  realpath: (p: string) => string = fs.realpathSync,
): string {
  let current = resolved;
  const tail: string[] = [];
  for (;;) {
    try {
      const real = realpath(current);
      const full = tail.length ? path.join(real, ...tail.slice().reverse()) : real;
      return full.normalize('NFC');
    } catch (err) {
      const code = (err as NodeJS.ErrnoException).code;
      if (code === 'ELOOP') {
        throw new PathTraversalError(
          `Path traversal blocked: symlink cycle while resolving ${resolved}`,
          'symlink-escape',
        );
      }
      if (code !== 'ENOENT' && code !== 'ENOTDIR') {
        // Unverifiable for an unrelated reason — do not add a false rejection here;
        // the lexical check in assertPathWithin has already vouched for containment.
        return resolved;
      }
      const parent = path.dirname(current);
      if (parent === current) {
        // Walked to the filesystem root without finding an existing ancestor.
        return resolved;
      }
      tail.push(path.basename(current));
      current = parent;
    }
  }
}

/**
 * Throw if `filePath` is not contained within `baseDir` (path-traversal guard). Shared by the
 * session store and the reference journal; lives in its own module so neither has to import the
 * other (avoids a session ↔ reference-journal import cycle).
 *
 * Layers (all fail with a {@link PathTraversalError}):
 *  1. Null-byte rejection — a `\0` in either argument is an injection attempt (and would
 *     otherwise be silently truncated by the OS) and is rejected before any resolution.
 *  2. Lexical containment — resolved, Unicode-NFC-normalized `startsWith` check. NFC
 *     normalization makes the comparison robust to macOS NFD vs NFC encodings of the same
 *     name (it only touches combining marks; path separators and `..` are ASCII, so it can
 *     never turn an escape into a pass).
 *  3. Symlink-aware containment — canonicalizes the existing prefix of both paths so a
 *     symlink inside `baseDir` that points outside it is caught even when the lexical form
 *     looks contained.
 */
export function assertPathWithin(filePath: string, baseDir: string): void {
  if (filePath.includes('\0') || baseDir.includes('\0')) {
    throw new PathTraversalError(
      `Path traversal blocked: null byte in path ${JSON.stringify(filePath)}`,
      'null-byte',
    );
  }

  const resolved = path.resolve(filePath).normalize('NFC');
  const resolvedBase = path.resolve(baseDir).normalize('NFC');
  if (!isWithin(resolved, resolvedBase)) {
    throw new PathTraversalError(
      `Path traversal blocked: ${filePath} escapes ${baseDir}`,
      'out-of-base',
    );
  }

  const canonical = canonicalizeExisting(resolved);
  const canonicalBase = canonicalizeExisting(resolvedBase);
  if (!isWithin(canonical, canonicalBase)) {
    throw new PathTraversalError(
      `Path traversal blocked: ${filePath} escapes ${baseDir} via symlink`,
      'symlink-escape',
    );
  }
}
