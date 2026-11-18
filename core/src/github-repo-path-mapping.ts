import { access } from 'fs/promises';

import { loadGlobalConfig, saveGlobalConfig } from './config';
import { detectCurrentRepository, parseGitHubRepository } from './detect-repository';
import { log } from './logger';
import { canonicalGitRoot } from './reference-journal';
import { getRemoteOriginUrl } from './utils/git';

/**
 * Persistent GitHub-repo -> local-clone-path registry with MRU ordering.
 *
 * Parity with Claude Code's `githubRepoPathMapping.ts`: AMI maintains a global-config
 * map (`githubRepoPaths`) keyed by the lowercased `owner/repo` -> a most-recently-used
 * list of that repo's known local clone roots. The clone a repo was most recently used
 * from is always first, so a repo-scoped feature can resolve "where on disk is a clone
 * of owner/repo?" without re-scanning the filesystem.
 *
 * The stored path is canonicalized so the same clone always maps to one entry — reusing
 * AMI's established project-identity canonicalizer {@link canonicalGitRoot}, which already
 * performs the steps Claude's helper does inline (git-root resolution, symlink realpath,
 * NFC Unicode normalization) PLUS AMI's superset of mapping every linked worktree of a
 * repo to its single canonical main-worktree root. The repo KEY is lowercased for
 * case-insensitive matching.
 *
 * Divergence from the reference (intentional, §0c): Claude wraps each helper in a
 * try/catch because its `findGitRoot`/`realpath`/`getRemoteUrlForDir` THROW. AMI's
 * dependencies are all total/fail-soft (they return null/defaults on any error), so no
 * redundant outer catch is needed — the fire-and-forget startup contract is upheld by
 * those callees, not by a wrapper that could never fire.
 */

/** Lowercase the `owner/repo` repo key so matching is case-insensitive (parity). */
function repoKeyFor(repo: string): string {
  return repo.toLowerCase();
}

/** Read the whole repo->paths map from the global config, or `{}` when none exists. */
function readRegistry(): Record<string, string[]> {
  return loadGlobalConfig()?.githubRepoPaths ?? {};
}

/**
 * Update the GitHub-repo path mapping for `cwd` (default `process.cwd()`), promoting the
 * current canonical clone root to the FRONT of its repo's MRU list. Intended as
 * fire-and-forget startup work: it detects the current github.com repository and stores
 * its canonical git root.
 *
 * Algorithm (parity with Claude Code's `updateGithubRepoPathMapping`):
 *   - no github.com repo at `cwd` → no-op;
 *   - the canonical path is already at index 0 → short-circuit (no write);
 *   - otherwise remove any existing occurrence and prepend, so the most-recently-used
 *     clone is always first.
 */
export async function updateGithubRepoPathMapping(cwd: string = process.cwd()): Promise<void> {
  const repo = detectCurrentRepository(cwd);
  if (!repo) {
    log('github-repo-path-mapping', 'Not in a GitHub repository, skipping path mapping update');
    return;
  }

  // Canonical clone root: git-root (not an arbitrary subdirectory) + realpath + NFC,
  // via the shared project-identity canonicalizer (no duplicate git-root logic).
  const currentPath = canonicalGitRoot(cwd);
  const repoKey = repoKeyFor(repo);

  const existingPaths = readRegistry()[repoKey] ?? [];
  if (existingPaths[0] === currentPath) {
    // Already at the front — nothing to do.
    log('github-repo-path-mapping', `Path ${currentPath} already tracked for repo ${repoKey}`);
    return;
  }

  // Remove if present elsewhere (to promote to front), then prepend.
  const withoutCurrent = existingPaths.filter(p => p !== currentPath);
  const updatedPaths = [currentPath, ...withoutCurrent];

  saveGlobalConfig(current => ({
    ...current,
    githubRepoPaths: {
      ...current.githubRepoPaths,
      [repoKey]: updatedPaths,
    },
  }));

  log('github-repo-path-mapping', `Added ${currentPath} to tracked paths for repo ${repoKey}`);
}

/**
 * Known local clone paths for `repo` (an `owner/repo` string), most-recently-used first,
 * or an empty array when the repo has never been recorded. Case-insensitive on the key.
 */
export function getKnownPathsForRepo(repo: string): string[] {
  return readRegistry()[repoKeyFor(repo)] ?? [];
}

/**
 * Filter `paths` to only those that currently exist on the filesystem, PRESERVING the
 * input order. Existence is checked in parallel (`Promise.all`), then the original array
 * is index-filtered against the results — so a stale clone path that was moved/deleted is
 * pruned without reordering the surviving MRU entries. Parity with Claude Code's
 * `filterExistingPaths`.
 */
export async function filterExistingPaths(paths: string[]): Promise<string[]> {
  const results = await Promise.all(
    paths.map(p =>
      access(p).then(
        () => true,
        () => false,
      ),
    ),
  );
  return paths.filter((_, i) => results[i]);
}

/**
 * Validate that `path` still hosts the `expectedRepo` (`owner/repo`) clone by reading its
 * git `origin` remote URL, parsing the GitHub `owner/repo` out of it, and comparing
 * case-insensitively. Returns `false` on any missing remote, unparseable URL, or mismatch.
 * Parity with Claude Code's `validateRepoAtPath` (AMI's remote reader is fail-soft, so no
 * throwing path to guard).
 */
export async function validateRepoAtPath(path: string, expectedRepo: string): Promise<boolean> {
  const remoteUrl = getRemoteOriginUrl(path);
  if (!remoteUrl) return false;

  const actualRepo = parseGitHubRepository(remoteUrl);
  if (!actualRepo) return false;

  return actualRepo.toLowerCase() === expectedRepo.toLowerCase();
}

/**
 * Remove `pathToRemove` from `repo`'s tracked clone paths (used when a stored path is
 * found invalid during selection). Self-pruning, parity with Claude Code's
 * `removePathFromRepo`:
 *   - the path is not in the list → no-op (no write);
 *   - the list becomes empty → the whole repo KEY is deleted (the map stays compact)
 *     rather than left as an empty array;
 *   - otherwise the shortened list is persisted.
 */
export function removePathFromRepo(repo: string, pathToRemove: string): void {
  const registry = readRegistry();
  const repoKey = repoKeyFor(repo);
  const existingPaths = registry[repoKey] ?? [];

  const updatedPaths = existingPaths.filter(p => p !== pathToRemove);
  if (updatedPaths.length === existingPaths.length) {
    // Path wasn't in the list — nothing to do.
    return;
  }

  const updatedMapping = { ...registry };
  if (updatedPaths.length === 0) {
    // Drop the repo key entirely when no paths remain — keep the map compact.
    delete updatedMapping[repoKey];
  } else {
    updatedMapping[repoKey] = updatedPaths;
  }

  saveGlobalConfig(current => ({ ...current, githubRepoPaths: updatedMapping }));

  log('github-repo-path-mapping', `Removed ${pathToRemove} from tracked paths for repo ${repoKey}`);
}
