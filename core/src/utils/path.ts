import * as path from 'path';
import * as os from 'os';
import * as fs from 'fs';
import { canonicalizeExisting } from '../path-safety';
import { maybePosixToWindows } from './windows-paths';

/**
 * Resolves `~` to the user's home directory, resolves relative paths against
 * the provided cwd, and normalizes the result.
 *
 * Hardening (Claude Code path-utility parity, all additive):
 *   - `cwd` is optional and defaults to `process.cwd()`.
 *   - Non-string arguments are rejected with a descriptive `TypeError`.
 *   - NUL bytes in either argument are rejected (a `\0` can truncate a path at
 *     the C/syscall boundary and slip past the lexical safety check).
 *   - Empty / whitespace-only input short-circuits to the normalized cwd.
 *   - The returned path is Unicode NFC-normalized so the containment boundary
 *     is canonical (visually-identical paths compare equal).
 *
 * @throws {TypeError} if `filePath` or `cwd` is not a string
 * @throws {Error} if `filePath` or `cwd` contains a NUL byte
 */
export function expandPath(filePath: string, cwd: string = process.cwd()): string {
  // Input validation: reject non-string arguments with a descriptive TypeError.
  if (typeof filePath !== 'string') {
    throw new TypeError(`Path must be a string, received ${typeof filePath}`);
  }
  if (typeof cwd !== 'string') {
    throw new TypeError(`cwd must be a string, received ${typeof cwd}`);
  }

  // Security: reject NUL bytes before any resolution. A NUL truncates the path
  // at the syscall boundary, so "/safe\0/../../etc/passwd" would pass a lexical
  // descendant check while the kernel opens "/safe".
  if (filePath.includes('\0') || cwd.includes('\0')) {
    throw new Error('Path contains null bytes');
  }

  // Empty / whitespace-only paths resolve to the normalized cwd.
  const trimmed = filePath.trim();
  if (trimmed === '') {
    return path.normalize(cwd).normalize('NFC');
  }

  let resolved = trimmed;

  // Expand leading ~ to the home directory
  if (resolved === '~' || resolved.startsWith('~/') || resolved.startsWith('~\\')) {
    resolved = path.join(os.homedir(), resolved.slice(1));
  }

  // On Windows, convert POSIX-style drive paths (e.g. `/c/Users/...` from Git
  // Bash / MSYS2 / Cygwin) to native form (`C:\Users\...`) so the absolute
  // check below and all downstream fs calls see a real native path. No-op on
  // other platforms and on non-drive paths.
  resolved = maybePosixToWindows(resolved);

  // Resolve relative paths against cwd
  if (!path.isAbsolute(resolved)) {
    resolved = path.resolve(cwd, resolved);
  }

  // NFC-normalize so the containment boundary is Unicode-canonical.
  return path.normalize(resolved).normalize('NFC');
}

/**
 * Returns true if the resolved path is within the cwd or the user's home
 * directory. Prevents path traversal attacks by ensuring the final resolved
 * path stays inside a safe boundary.
 */
export function isPathSafe(filePath: string, cwd: string): boolean {
  const resolved = expandPath(filePath, cwd);
  // NFC-normalize the boundaries so comparison matches the NFC-normalized
  // `resolved` value (expandPath returns NFC) — a Unicode-canonical boundary.
  const normalizedCwd = path.normalize(cwd).normalize('NFC');
  const homeDir = path.normalize(os.homedir()).normalize('NFC');

  // Lexical boundary first — a cheap reject for the common "obviously outside
  // the tree" case before any filesystem access.
  if (!isDescendant(resolved, normalizedCwd) && !isDescendant(resolved, homeDir)) {
    return false;
  }

  // Symlink-aware boundary: a symlink that LIVES inside cwd/home but POINTS
  // outside it (e.g. `<cwd>/link -> /etc`) passes the lexical check above yet
  // defeats the traversal guard this function exists to provide. Canonicalize
  // the existing prefix of the path and both boundaries so the decision is made
  // against the REAL resolved location, not the lexical one. Reuses the
  // fail-closed resolver in path-safety.ts (visited-set + depth cap + dangling/
  // looping handling) — no second symlink-resolution implementation.
  try {
    const canonical = canonicalizeExisting(resolved);
    const canonicalCwd = canonicalizeExisting(normalizedCwd);
    const canonicalHome = canonicalizeExisting(homeDir);
    return isDescendant(canonical, canonicalCwd) || isDescendant(canonical, canonicalHome);
  } catch {
    // Unverifiable / dangling / looping symlink => treat as unsafe (fail closed).
    return false;
  }
}

/**
 * Resolve an already-expanded path to its real (symlink-canonical) location and
 * return it Unicode NFC-normalized, so a permission/boundary comparison is made
 * against a canonical form.
 *
 * On an NFD-storing filesystem (e.g. macOS) `fs.realpathSync` reports the
 * decomposed on-disk spelling of a name; without re-normalizing to NFC the same
 * file has a different byte string than an NFC-stored cwd/rule, so an
 * in-workspace file is judged outside the workspace and an NFD-spelled path can
 * evade an NFC deny/allow rule. Mirrors Claude Code's
 * `fs.realpathSync(path).normalize('NFC')` in getPathsForPermissionCheck.
 *
 * A path that does not yet exist (a write target) cannot be realpath-resolved, so
 * the already-expanded input (which `expandPath` returns NFC-normalized) is
 * returned unchanged.
 *
 * [FUNCTIONAL CHANGE] New exported helper — NFC canonicalization of realpath
 * output for permission checks. The `realpath` parameter is a dependency-
 * injection seam for tests; it defaults to `fs.realpathSync`.
 */
export function realpathNFC(
  expandedPath: string,
  realpath: (p: string) => string = fs.realpathSync,
): string {
  try {
    return realpath(expandedPath).normalize('NFC');
  } catch {
    // Not-yet-existing path (write target) — keep the already-NFC input.
    return expandedPath;
  }
}

/**
 * Converts an absolute path to a relative path if it is within cwd.
 * Otherwise returns the absolute path unchanged.
 */
export function toRelativePath(filePath: string, cwd: string): string {
  const resolved = expandPath(filePath, cwd);
  const normalizedCwd = path.normalize(cwd).normalize('NFC');

  if (isDescendant(resolved, normalizedCwd)) {
    return path.relative(normalizedCwd, resolved);
  }

  return resolved;
}

// ---------------------------------------------------------------------------
// FUNCTIONAL PARITY ADDITIONS (additive superset — Claude Code path display /
// containment parity). None of the above behavior is removed; the helpers below
// are new public API plus a hardening of the shared containment check.
// ---------------------------------------------------------------------------

/**
 * Strip a single trailing path separator (POSIX `/` or Windows `\`) from a
 * path, but never strip the separator off a filesystem root (`/`, `C:\`).
 * Used to normalize boundary comparisons so a caller-supplied `cwd` of
 * `/a/b/` behaves identically to `/a/b`.
 *
 * [FUNCTIONAL CHANGE] New exported helper.
 */
export function stripTrailingSep(p: string): string {
  if (p.length <= 1) return p;
  // Do not strip the separator that belongs to a root like "/" or "C:\".
  const parsed = path.parse(p);
  if (p === parsed.root) return p;
  if ((p.endsWith('/') || p.endsWith('\\')) && p.slice(0, -1) !== parsed.root.replace(/[/\\]$/, '')) {
    return p.slice(0, -1);
  }
  return p;
}

/**
 * Checks whether `child` is the same as or a descendant of `parent`.
 * Both paths are expected to be normalized absolute paths.
 *
 * [FUNCTIONAL CHANGE] Hardened: a `parent` carrying a trailing separator
 * (e.g. a cwd of `/home/user/project/`) is now treated the same as one
 * without it, so an otherwise-equal child is correctly recognized as inside
 * the boundary. Previously `isDescendant('/a/b', '/a/b/')` returned false.
 */
function isDescendant(child: string, parent: string): boolean {
  const normalizedParent = stripTrailingSep(parent);
  const normalizedChild = stripTrailingSep(child);
  // Ensure trailing separator so "/home/user2" is not matched by "/home/user"
  const parentPrefix = normalizedParent.endsWith(path.sep)
    ? normalizedParent
    : normalizedParent + path.sep;
  return normalizedChild === normalizedParent || normalizedChild.startsWith(parentPrefix);
}

/**
 * Public containment check: returns true when `childPath` is the same as, or
 * nested inside, `parentPath`. Both arguments are expanded (so `~` and
 * relative inputs are resolved against `cwd`) before comparison, making this
 * safe to call on raw user-supplied paths.
 *
 * [FUNCTIONAL CHANGE] New exported helper (exposes the previously-private
 * containment primitive and tolerates trailing separators / tilde / relative
 * inputs). Mirrors Claude Code's workspace-containment checks.
 */
export function isPathWithin(childPath: string, parentPath: string, cwd: string = process.cwd()): boolean {
  const resolvedChild = expandPath(childPath, cwd);
  const resolvedParent = expandPath(parentPath, cwd);
  return isDescendant(resolvedChild, resolvedParent);
}

/**
 * Abbreviates an absolute path that lives under the user's home directory to
 * tilde form for display: `/home/user/x` -> `~/x`, and the home directory
 * itself -> `~`. Paths outside home are returned unchanged. A trailing
 * separator on the input is preserved.
 *
 * [FUNCTIONAL CHANGE] New exported helper — tilde-display abbreviation parity
 * with Claude Code's path rendering.
 */
export function abbreviateHome(filePath: string, home: string = os.homedir()): string {
  const normalizedHome = stripTrailingSep(path.normalize(home).normalize('NFC'));
  const hadTrailing = /[/\\]$/.test(filePath);
  const normalized = path.normalize(filePath).normalize('NFC');
  const bare = stripTrailingSep(normalized);

  if (bare === normalizedHome) {
    return hadTrailing && normalized !== bare ? '~' + path.sep : '~';
  }

  const homePrefix = normalizedHome.endsWith(path.sep) ? normalizedHome : normalizedHome + path.sep;
  if (bare.startsWith(homePrefix)) {
    const rest = bare.slice(homePrefix.length);
    const abbreviated = '~' + path.sep + rest;
    return hadTrailing && normalized !== bare ? abbreviated + path.sep : abbreviated;
  }

  return filePath;
}

/**
 * Produces the shortest human-friendly display form of an absolute path by
 * computing BOTH representations and returning whichever string is shorter:
 *
 *   - home-relative: `~` for the home dir itself, otherwise `~` + the remainder
 *                    beneath home (e.g. `~/notes.md`)
 *   - cwd-relative:  `./` for the cwd itself, otherwise `./` + the remainder
 *                    beneath cwd (e.g. `./src/app.ts`)
 *
 * When both forms apply the shorter one wins, ties going to the home form
 * (`<=`); when only one applies that one is used; otherwise the absolute,
 * normalized path is returned. Input is expanded first, so `~`, relative, and
 * trailing-separator inputs are all accepted.
 *
 * This is a distinct display POLICY from {@link displayPath}, which always
 * prefers the cwd-relative form (and renders cwd as `.`); this helper picks the
 * more compact of the two spellings.
 *
 * [FUNCTIONAL CHANGE] New exported helper — "shortest of ~home vs ./cwd"
 * display parity with Claude Code's `getRelativeMemoryPath`
 * (components/memory/MemoryUpdateNotification.tsx). Superset improvement over
 * the reference: boundary membership uses the separator-aware containment check
 * (so a sibling prefix dir like `/home/user2` is not treated as inside
 * `/home/user`), where Claude uses a raw `startsWith`.
 */
export function shortestDisplayPath(
  filePath: string,
  cwd: string = process.cwd(),
  home: string = os.homedir(),
): string {
  const resolved = expandPath(filePath, cwd);
  const normalizedCwd = stripTrailingSep(path.normalize(cwd).normalize('NFC'));
  const normalizedHome = stripTrailingSep(path.normalize(home).normalize('NFC'));

  const relativeToHome = isDescendant(resolved, normalizedHome)
    ? resolved === normalizedHome
      ? '~'
      : '~' + path.sep + path.relative(normalizedHome, resolved)
    : null;

  const relativeToCwd = isDescendant(resolved, normalizedCwd)
    ? './' + path.relative(normalizedCwd, resolved)
    : null;

  if (relativeToHome !== null && relativeToCwd !== null) {
    return relativeToHome.length <= relativeToCwd.length ? relativeToHome : relativeToCwd;
  }
  return relativeToHome ?? relativeToCwd ?? resolved;
}

/**
 * Produces a short, human-friendly path for display relative to a workspace:
 *
 *   - inside `cwd`           -> a cwd-relative path (`src/app.ts`); the cwd
 *                              itself renders as `.`
 *   - outside cwd but inside home -> tilde-abbreviated (`~/other/file`)
 *   - anywhere else          -> the absolute, normalized path
 *
 * Input is expanded first, so `~`, relative, and trailing-separator inputs are
 * all accepted.
 *
 * [FUNCTIONAL CHANGE] New exported helper — workspace-relative display parity
 * with Claude Code.
 */
export function displayPath(filePath: string, cwd: string, home: string = os.homedir()): string {
  const resolved = expandPath(filePath, cwd);
  const normalizedCwd = path.normalize(cwd).normalize('NFC');

  if (isDescendant(resolved, normalizedCwd)) {
    const rel = path.relative(stripTrailingSep(normalizedCwd), resolved);
    return rel === '' ? '.' : rel;
  }

  return abbreviateHome(resolved, home);
}
