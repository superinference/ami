import { execFile } from 'child_process';
import { promisify } from 'util';
import { uriToPath } from '../lsp/diagnostic-tracking';

const execFileAsync = promisify(execFile);

/**
 * Dependency seam so tests can script `git check-ignore` deterministically
 * (ignored set, "not a repo" / exit-128, timeout) without a real git tree.
 * Production calls the real `child_process.execFile`.
 */
export const _gitIgnoreFilterHooks: { execFileAsync: typeof execFileAsync } = {
  execFileAsync,
};

/** How many paths to pass to a single `git check-ignore` invocation. */
const BATCH_SIZE = 50;
/** Bound on each `git check-ignore` subprocess so a hung git can't block a tool
 *  call (mirrors the reference's 5s check-ignore timeout). */
const CHECK_IGNORE_TIMEOUT_MS = 5_000;

/**
 * Run `git check-ignore` over a batch of paths and return the set of paths git
 * reported as ignored. `git check-ignore` exits 0 (some ignored — it echoes the
 * ignored paths on stdout), 1 (none ignored), or 128 (not a git repo); the
 * non-zero exits reject under promisify, so the stdout is read off the thrown
 * error. Any other failure (timeout, git missing) is treated as "nothing
 * ignored" so result filtering degrades to a pass-through rather than throwing.
 */
async function checkIgnoreBatch(batch: string[], cwd: string): Promise<Set<string>> {
  const ignored = new Set<string>();
  let stdout = '';
  try {
    const res = await _gitIgnoreFilterHooks.execFileAsync('git', ['check-ignore', ...batch], {
      cwd,
      timeout: CHECK_IGNORE_TIMEOUT_MS,
    });
    stdout = res.stdout ?? '';
  } catch (err) {
    // Exit 1 (none ignored) and 128 (not a repo) land here with empty/absent
    // stdout; an exit-0-with-stdout can also surface as a thrown error that still
    // carries stdout on some platforms, so harvest it defensively.
    stdout = (err as { stdout?: string } | null)?.stdout ?? '';
  }
  for (const line of stdout.split('\n')) {
    const trimmed = line.trim();
    if (trimmed) ignored.add(trimmed);
  }
  return ignored;
}

/**
 * Drop items whose backing file is gitignored, so build-artifact / vendored /
 * generated locations never pollute LSP definition/reference/symbol results.
 *
 * De-dupes URIs → paths, batches `git check-ignore` in groups of {@link
 * BATCH_SIZE} (each bounded by {@link CHECK_IGNORE_TIMEOUT_MS}), and removes any
 * item whose path git reported as ignored. Mirrors the reference's
 * `filterGitIgnoredLocations`. Returns the input unchanged when there is nothing
 * to check or nothing is ignored.
 */
export async function filterGitIgnoredLocations<T>(
  items: T[],
  uriOf: (item: T) => string | undefined,
  cwd: string,
): Promise<T[]> {
  if (items.length === 0) return items;

  // Map each unique URI to its filesystem path once.
  const uriPath = new Map<string, string>();
  for (const item of items) {
    const uri = uriOf(item);
    if (uri && !uriPath.has(uri)) uriPath.set(uri, uriToPath(uri));
  }
  const uniquePaths = Array.from(new Set(uriPath.values()));
  if (uniquePaths.length === 0) return items;

  const ignoredPaths = new Set<string>();
  for (let i = 0; i < uniquePaths.length; i += BATCH_SIZE) {
    const batch = uniquePaths.slice(i, i + BATCH_SIZE);
    const batchIgnored = await checkIgnoreBatch(batch, cwd);
    for (const p of batchIgnored) ignoredPaths.add(p);
  }
  if (ignoredPaths.size === 0) return items;

  return items.filter(item => {
    const uri = uriOf(item);
    const p = uri !== undefined ? uriPath.get(uri) : undefined;
    return p === undefined || !ignoredPaths.has(p);
  });
}
