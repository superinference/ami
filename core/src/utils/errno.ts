/**
 * Typed Node.js errno accessors and the single configured "filesystem is
 * missing/inaccessible/unreachable" code set.
 *
 * Catch blocks all over the tree used to re-implement the
 * `(err as NodeJS.ErrnoException).code` / `.path` cast and then string-compare a
 * single code (almost always only `'ENOENT'`), so the set of codes that mean
 * "the path could not be reached" was never defined in ONE place and sites
 * disagreed (most missed `ENOTDIR`/`ELOOP`/`EACCES`). This module is that single
 * place — parity with the reference's `src/utils/errors.ts`
 * (`getErrnoCode`/`getErrnoPath`/`isENOENT`/`isFsInaccessible`). Every fs catch
 * block classifies errors through these helpers instead of an ad-hoc cast.
 */

/**
 * The grouped errno set that means "path missing, inaccessible, or structurally
 * unreachable" — the single configured knob {@link isFsInaccessible} tests
 * against. Frozen so no caller can mutate the shared set.
 *
 *  - `ENOENT`  — path does not exist
 *  - `EACCES`  — permission denied
 *  - `EPERM`   — operation not permitted
 *  - `ENOTDIR` — a path component is not a directory (e.g. a file exists where a
 *                directory is expected)
 *  - `ELOOP`   — too many symlink levels (circular symlinks)
 */
export const FS_INACCESSIBLE_CODES = Object.freeze([
  'ENOENT', 'EACCES', 'EPERM', 'ENOTDIR', 'ELOOP',
] as const);

/** The literal union of codes in {@link FS_INACCESSIBLE_CODES}. */
export type FsInaccessibleCode = (typeof FS_INACCESSIBLE_CODES)[number];

/**
 * Extract a Node.js errno string (e.g. 'ENOENT', 'EACCES', 'ELOOP', 'EIO') from
 * an unknown thrown value, or undefined when none is present. Single source for
 * classifying filesystem errors so call sites can distinguish "absent" (ENOENT)
 * from "could-not-be-checked" without re-implementing the `err.code` cast.
 */
export function getErrnoCode(err: unknown): string | undefined {
  if (err && typeof err === 'object' && 'code' in err) {
    const code = (err as { code?: unknown }).code;
    if (typeof code === 'string') return code;
  }
  return undefined;
}

/**
 * Extract the filesystem path that triggered an errno error (the `.path` the
 * kernel attached, e.g. the exact file/dir the fs call failed on), or undefined
 * when none is present. Lets an error message name the OFFENDING path even when
 * it differs from the one the caller passed (a parent component of a deep path,
 * a symlink target, …). Replaces the ad-hoc `(err as NodeJS.ErrnoException).path`
 * cast (parity: the reference's getErrnoPath).
 */
export function getErrnoPath(err: unknown): string | undefined {
  if (err && typeof err === 'object' && 'path' in err) {
    const p = (err as { path?: unknown }).path;
    if (typeof p === 'string') return p;
  }
  return undefined;
}

/**
 * True when `err` is a Node.js ENOENT (the path does not exist). Replaces the
 * `getErrnoCode(err) === 'ENOENT'` comparison scattered across catch blocks.
 */
export function isENOENT(err: unknown): boolean {
  return getErrnoCode(err) === 'ENOENT';
}

/**
 * True when `err` means the path is missing, inaccessible, or structurally
 * unreachable — i.e. its errno is one of {@link FS_INACCESSIBLE_CODES}. Use in a
 * catch block after an fs operation to distinguish an expected "nothing there /
 * no access" from an unexpected error (EIO, EBUSY, …) that should surface
 * differently (parity: the reference's isFsInaccessible).
 */
export function isFsInaccessible(err: unknown): err is NodeJS.ErrnoException {
  const code = getErrnoCode(err);
  return code !== undefined && (FS_INACCESSIBLE_CODES as readonly string[]).includes(code);
}
