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

/**
 * Checks whether `child` is the same as or a descendant of `parent`.
 * Both paths are expected to be normalized absolute paths.
 */
function isDescendant(child: string, parent: string): boolean {
  // Ensure trailing separator so "/home/user2" is not matched by "/home/user"
  const parentPrefix = parent.endsWith(path.sep) ? parent : parent + path.sep;
  return child === parent || child.startsWith(parentPrefix);
}
