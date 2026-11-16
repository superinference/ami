/**
 * POSIX-to-native Windows path conversion with LRU-memoized caching.
 *
 * Git Bash / MSYS2 / Cygwin surface Windows drives as POSIX-style paths
 * (`/c/Users/...`, `/cygdrive/c/...`). When AMI runs on Windows those must be
 * converted to the native drive-letter form (`C:\Users\...`) before any
 * filesystem access, otherwise `path.isAbsolute`/`fs.*` treat them as relative.
 *
 * The conversion is pure and hot (called on every path expansion), so results
 * are memoized behind a bounded LRU cache — matching Claude Code's
 * `memoizeWithLRU(..., 500)` in `utils/windowsPaths.ts`.
 *
 * [FUNCTIONAL CHANGE] New module — Windows POSIX-path parity with Claude Code.
 */

/** Default ceiling on cached entries, mirroring Claude Code's 500. */
const LRU_MAX = 500;

/**
 * Bounded LRU memoizer for single-string-argument pure functions. A `Map`
 * preserves insertion order, so a cache hit is moved to the most-recently-used
 * end (delete + re-set) and, once the map exceeds `max`, the least-recently-used
 * key (first in iteration order) is evicted. Shared helper so each converter
 * does not re-implement its own cache.
 */
export function memoizeWithLRU<T>(
  fn: (arg: string) => T,
  max: number = LRU_MAX,
): (arg: string) => T {
  const cache = new Map<string, T>();
  return (arg: string): T => {
    if (cache.has(arg)) {
      const hit = cache.get(arg) as T;
      // Refresh recency: re-insert so it becomes most-recently-used.
      cache.delete(arg);
      cache.set(arg, hit);
      return hit;
    }
    const value = fn(arg);
    cache.set(arg, value);
    if (cache.size > max) {
      // Evict the least-recently-used entry (first in insertion order).
      const oldest = cache.keys().next().value as string;
      cache.delete(oldest);
    }
    return value;
  };
}

/**
 * Convert a POSIX-style path to a native Windows path. Handles UNC
 * (`//server/share` → `\\server\share`), Cygwin (`/cygdrive/c/...` →
 * `C:\...`), and MSYS2/Git-Bash drive (`/c/...` → `C:\...`) forms; anything
 * else just has its slashes flipped. Memoized (500-entry LRU).
 */
export const posixPathToWindowsPath = memoizeWithLRU((posixPath: string): string => {
  // UNC: //server/share -> \\server\share
  if (posixPath.startsWith('//')) {
    return posixPath.replace(/\//g, '\\');
  }
  // Cygwin: /cygdrive/c/Users/... -> C:\Users\...
  const cygdriveMatch = posixPath.match(/^\/cygdrive\/([A-Za-z])(\/|$)/);
  if (cygdriveMatch) {
    const driveLetter = cygdriveMatch[1].toUpperCase();
    const rest = posixPath.slice(('/cygdrive/' + cygdriveMatch[1]).length);
    return driveLetter + ':' + (rest || '\\').replace(/\//g, '\\');
  }
  // MSYS2 / Git Bash: /c/Users/... -> C:\Users\...
  const driveMatch = posixPath.match(/^\/([A-Za-z])(\/|$)/);
  if (driveMatch) {
    const driveLetter = driveMatch[1].toUpperCase();
    const rest = posixPath.slice(2);
    return driveLetter + ':' + (rest || '\\').replace(/\//g, '\\');
  }
  // Already Windows or relative — just flip slashes.
  return posixPath.replace(/\//g, '\\');
});

/**
 * On Windows, convert a POSIX-style drive path (`/c/Users/...`) to its native
 * form; on any other platform, or for a path that is not a POSIX drive path,
 * return the input unchanged. `platform` is injectable so the Windows branch is
 * exercisable (and testable) off-Windows.
 */
export function maybePosixToWindows(
  p: string,
  platform: NodeJS.Platform = process.platform,
): string {
  if (platform === 'win32' && /^\/[a-z]\//i.test(p)) {
    return posixPathToWindowsPath(p);
  }
  return p;
}
