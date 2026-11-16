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

/**
 * Normalize a resolved path for case-/symlink-tolerant containment comparison.
 *
 *  - macOS routes `/var` → `/private/var` and `/tmp` → `/private/tmp` through
 *    well-known symlinks; rewriting the `/private` form back to the short form
 *    keeps a base dir expressed either way comparing equal.
 *  - Lowercasing defeats case-only bypasses on case-insensitive filesystems
 *    (macOS/Windows) such as `.CLaUde/Settings.json`. It is applied on every
 *    platform for consistent security and can never turn an escape into a pass:
 *    the separator-anchored prefix check still rejects sibling dirs.
 */
function normalizeForComparison(resolved: string): string {
  return resolved
    .replace(/^\/private\/var\//, '/var/')
    .replace(/^\/private\/tmp(\/|$)/, '/tmp$1')
    .toLowerCase();
}

/** True if `child` is the same as, or nested inside, `parent` (both already resolved). */
function isWithin(child: string, parent: string): boolean {
  const c = normalizeForComparison(child);
  const p = normalizeForComparison(parent);
  return c === p || c.startsWith(p + path.sep);
}

/**
 * Canonicalize an absolute path, resolving symlinks in its existing prefix.
 *
 * `fs.realpathSync` fails on paths that do not exist yet (write targets), so we realpath
 * the deepest ancestor that DOES exist and re-append the not-yet-created tail lexically.
 * This mirrors the behaviour needed for new-file writes while still following any symlink
 * in the existing portion — the part an attacker can point outside the base dir.
 *
 * Fail-CLOSED on anything that makes containment unverifiable, so a hostile or broken
 * filesystem can never be used to smuggle a write outside the base:
 *  - a symlink cycle (`ELOOP`) is treated as an escape;
 *  - a dangling symlink (the final component `lstat`s as a symlink but `realpath` returns
 *    `ENOENT` because its target is missing) is an escape — `writeFile` would follow the
 *    link and create the target outside the base;
 *  - any unexpected error (`EACCES`, `EIO`, …) is an escape — unlike `ENOENT`/`ENOTDIR`/
 *    `ENAMETOOLONG`, which merely mean "this component doesn't exist yet" and are safe to
 *    walk past to realpath the deeper existing ancestor.
 *
 * UNC paths (`//host/share`, `\\host\share`) and special files (FIFO/socket/device) bypass
 * `realpath` entirely: realpath'ing a UNC path triggers a network (DNS/SMB) lookup, and
 * `realpathSync` can block on a FIFO waiting for a writer. Their lexical form is returned
 * so the lexical containment check in {@link assertPathWithin} still governs them.
 *
 * `realpath`/`lstat` are injectable purely so tests can drive the error branches
 * deterministically; production always uses the `fs.*Sync` defaults.
 */
export function canonicalizeExisting(
  resolved: string,
  realpath: (p: string) => string = fs.realpathSync,
  lstat: (p: string) => fs.Stats = fs.lstatSync,
): string {
  // UNC paths: never touch the filesystem (would trigger a DNS/SMB lookup).
  if (resolved.startsWith('//') || resolved.startsWith('\\\\')) {
    return resolved.normalize('NFC');
  }

  // Special files (FIFO/socket/char/block device): realpathSync can block on a
  // FIFO, so skip canonicalization and let the lexical check govern them.
  try {
    const st = lstat(resolved);
    if (st.isFIFO() || st.isSocket() || st.isCharacterDevice() || st.isBlockDevice()) {
      return resolved.normalize('NFC');
    }
  } catch {
    // Doesn't exist / inaccessible — fall through to the walk, which decides.
  }

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
      if (code === 'ENOENT') {
        // `realpath` ENOENT is ambiguous: either the component is truly missing
        // (safe to walk up) OR it is a DANGLING symlink whose target is gone.
        // A dangling symlink is an escape vector — writeFile would follow it and
        // create the target wherever the link points. `lstat` distinguishes: it
        // succeeds (as a symlink) for a dangling link, throws ENOENT for a truly
        // missing path.
        try {
          if (lstat(current).isSymbolicLink()) {
            throw new PathTraversalError(
              `Path traversal blocked: dangling symlink ${current} while resolving ${resolved}`,
              'symlink-escape',
            );
          }
        } catch (lerr) {
          if (lerr instanceof PathTraversalError) throw lerr;
          // lstat also failed — the component is genuinely absent; walk up.
        }
      } else if (code !== 'ENOTDIR' && code !== 'ENAMETOOLONG') {
        // EACCES, EIO, … — containment cannot be verified. Fail closed rather than
        // trusting the lexical form (a symlink we could not read may escape).
        throw new PathTraversalError(
          `Path traversal blocked: cannot verify ${resolved} (${code ?? 'unknown'})`,
          'symlink-escape',
        );
      }
      const parent = path.dirname(current);
      if (parent === current) {
        // Walked to the filesystem root without finding an existing ancestor.
        return resolved.normalize('NFC');
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
 *  2. Lexical containment — resolved, Unicode-NFC-normalized `startsWith` check, compared
 *     case-/`/private`-insensitively so a case-only or macOS `/private` alias cannot bypass
 *     it. NFC normalization makes the comparison robust to macOS NFD vs NFC encodings of the
 *     same name (it only touches combining marks; path separators and `..` are ASCII, so it
 *     can never turn an escape into a pass).
 *  3. Symlink-aware containment — canonicalizes the existing prefix of both paths (fail-closed
 *     on unverifiable/dangling/looping symlinks) so a symlink inside `baseDir` that points
 *     outside it is caught even when the lexical form looks contained.
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
