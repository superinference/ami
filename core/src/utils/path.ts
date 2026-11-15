import * as path from 'path';
import * as os from 'os';

/**
 * Resolves `~` to the user's home directory, resolves relative paths against
 * the provided cwd, and normalizes the result.
 */
export function expandPath(filePath: string, cwd: string): string {
  let resolved = filePath;

  // Expand leading ~ to the home directory
  if (resolved === '~' || resolved.startsWith('~/') || resolved.startsWith('~\\')) {
    resolved = path.join(os.homedir(), resolved.slice(1));
  }

  // Resolve relative paths against cwd
  if (!path.isAbsolute(resolved)) {
    resolved = path.resolve(cwd, resolved);
  }

  return path.normalize(resolved);
}

/**
 * Returns true if the resolved path is within the cwd or the user's home
 * directory. Prevents path traversal attacks by ensuring the final resolved
 * path stays inside a safe boundary.
 */
export function isPathSafe(filePath: string, cwd: string): boolean {
  const resolved = expandPath(filePath, cwd);
  const normalizedCwd = path.normalize(cwd);
  const homeDir = path.normalize(os.homedir());

  // The resolved path must be inside cwd or inside the home directory
  return isDescendant(resolved, normalizedCwd) || isDescendant(resolved, homeDir);
}

/**
 * Converts an absolute path to a relative path if it is within cwd.
 * Otherwise returns the absolute path unchanged.
 */
export function toRelativePath(filePath: string, cwd: string): string {
  const resolved = expandPath(filePath, cwd);
  const normalizedCwd = path.normalize(cwd);

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
  const normalizedHome = stripTrailingSep(path.normalize(home));
  const hadTrailing = /[/\\]$/.test(filePath);
  const normalized = path.normalize(filePath);
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
  const normalizedCwd = path.normalize(cwd);

  if (isDescendant(resolved, normalizedCwd)) {
    const rel = path.relative(stripTrailingSep(normalizedCwd), resolved);
    return rel === '' ? '.' : rel;
  }

  return abbreviateHome(resolved, home);
}
