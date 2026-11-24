// Split from worktree-manager.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as path from 'path';
import { isWithin } from './worktree-manager-parse-pr-reference';

/** Extract a Node errno string (e.g. 'EACCES') from a thrown fs error, or ''. */
export function errnoCodeOf(err: unknown): string {
  return (err && typeof err === 'object' && typeof (err as { code?: unknown }).code === 'string')
    ? (err as { code: string }).code
    : '';
}

/**
 * Symlink large shared directories (default `node_modules`) from the source repo
 * into a new worktree so each worktree reuses one install.
 *
 * Returns the list of human-readable WARNINGS for dirs that failed with an
 * UNEXPECTED error, so the caller can surface them instead of leaving a silent
 * failure (parity with the reference's symlinkDirectories, utils/worktree.ts:L109-L136).
 * Benign cases are NOT warnings: ENOENT (source missing) and EEXIST (target
 * already present) are expected races and stay silent; any other errno
 * (EACCES/EPERM permission denied, ENOSYS on a filesystem that forbids symlinks)
 * is collected so a genuine failure is debuggable. Each dir is still guarded by
 * the path-traversal containment check on both roots.
 */
export function symlinkLargeDirectories(sourceCwd: string, worktreePath: string, dirs: string[] = ['node_modules']): string[] {
  const warnings: string[] = [];
  for (const dir of dirs) {
    const source = path.join(sourceCwd, dir);
    const target = path.join(worktreePath, dir);
    // Path-traversal guard: a poisoned dir name must not escape either root.
    if (!isWithin(sourceCwd, source) || !isWithin(worktreePath, target)) continue;
    try {
      if (fs.existsSync(source) && !fs.existsSync(target)) {
        fs.symlinkSync(source, target, 'dir');
      }
    } catch (err: unknown) {
      const code = errnoCodeOf(err);
      // ENOENT/EEXIST are benign races; surface anything else so an EACCES/EPERM/
      // ENOSYS failure is not indistinguishable from the benign case.
      if (code !== 'ENOENT' && code !== 'EEXIST') {
        warnings.push(`could not symlink ${dir}: ${code || (err instanceof Error ? err.message : String(err))}`);
      }
    }
  }
  return warnings;
}

export function validateWorktreeGitFile(worktreePath: string): { valid: boolean; error?: string } {
  const gitPath = path.join(worktreePath, '.git');
  try {
    const stat = fs.lstatSync(gitPath);
    if (stat.isDirectory()) {
      return { valid: true };
    }
    if (!stat.isFile()) {
      return { valid: false, error: 'not a file or directory' };
    }
  } catch {
    return { valid: false, error: 'missing .git' };
  }

  let content: string;
  try {
    content = fs.readFileSync(gitPath, 'utf-8').trim();
  } catch {
    return { valid: false, error: 'unreadable .git file' };
  }

  if (!content.startsWith('gitdir:')) {
    return { valid: false, error: 'invalid .git file: missing gitdir: prefix' };
  }

  const gitdir = content.slice('gitdir:'.length).trim();
  if (!gitdir) {
    return { valid: false, error: 'invalid .git file: empty gitdir path' };
  }

  const resolvedGitdir = path.isAbsolute(gitdir)
    ? gitdir
    : path.resolve(worktreePath, gitdir);

  if (!fs.existsSync(resolvedGitdir)) {
    return { valid: false, error: 'stale worktree: gitdir target does not exist' };
  }

  return validateWorktreeCommondirTrust(worktreePath, resolvedGitdir);
}

/**
 * Anti-hijack trust-chain validation for a linked worktree, mirroring git's own
 * `.git` file → `gitdir:` → `commondir` structure and the reference's
 * resolveCanonicalRoot hardening (utils/git.ts).
 *
 * The `.git` file, its admin-dir `commondir`, and the `gitdir` back-link are all
 * attacker-controlled in a cloned/downloaded repo. A malicious repo can point
 * `commondir` at any path the victim has trusted to borrow its identity, or
 * reuse a victim's existing worktree entry. We validate the exact structure
 * `git worktree add` creates before trusting the worktree:
 *   1. `<worktreeGitDir>` is a direct child of `<commonDir>/worktrees/`
 *   2. `<worktreeGitDir>/gitdir` back-link resolves to `<worktreePath>/.git`
 *
 * When the `commondir`/`gitdir` files are absent (a submodule, or any `.git`
 * pointer that is not a linked worktree) the check cannot run and we fall
 * through to `valid` — the hardening only REJECTS a definitively-mismatched
 * structure, never a legitimate worktree it cannot verify.
 */
export function validateWorktreeCommondirTrust(
  worktreePath: string,
  worktreeGitDir: string,
): { valid: boolean; error?: string } {
  let commonDirRaw: string;
  try {
    commonDirRaw = fs.readFileSync(path.join(worktreeGitDir, 'commondir'), 'utf-8').trim();
  } catch {
    return { valid: true }; // no commondir → not a linked worktree we can validate
  }
  if (!commonDirRaw) return { valid: true };

  const commonDir = path.resolve(worktreeGitDir, commonDirRaw);
  // (1) The admin gitdir must live directly under <commonDir>/worktrees/.
  if (path.resolve(path.dirname(worktreeGitDir)) !== path.join(commonDir, 'worktrees')) {
    return { valid: false, error: 'untrusted worktree: gitdir is not under commondir/worktrees' };
  }

  // (2) The back-link must point at THIS worktree's .git, so an attacker cannot
  // borrow a victim's existing worktree entry by guessing its path.
  let backlinkRaw: string;
  try {
    backlinkRaw = fs.readFileSync(path.join(worktreeGitDir, 'gitdir'), 'utf-8').trim();
  } catch {
    return { valid: false, error: 'untrusted worktree: missing gitdir back-link' };
  }
  try {
    const backlink = fs.realpathSync(backlinkRaw);
    const expected = path.join(fs.realpathSync(worktreePath), '.git');
    if (backlink !== expected) {
      return { valid: false, error: 'untrusted worktree: gitdir back-link mismatch' };
    }
  } catch {
    return { valid: false, error: 'untrusted worktree: unresolvable gitdir back-link' };
  }

  return { valid: true };
}

export function findStaleWorktrees(repoPath: string): string[] {
  const worktreeDir = path.join(repoPath, '.ami', 'worktrees');
  if (!fs.existsSync(worktreeDir)) return [];

  const stale: string[] = [];
  let entries: string[];
  try {
    entries = fs.readdirSync(worktreeDir);
  } catch {
    return [];
  }

  for (const entry of entries) {
    const fullPath = path.join(worktreeDir, entry);
    try {
      const stat = fs.statSync(fullPath);
      if (!stat.isDirectory()) continue;
    } catch {
      continue;
    }
    const result = validateWorktreeGitFile(fullPath);
    if (!result.valid) {
      stale.push(fullPath);
    }
  }

  return stale;
}

export function copyDirRecursive(src: string, dest: string): void {
  fs.mkdirSync(dest, { recursive: true });
  for (const entry of fs.readdirSync(src, { withFileTypes: true })) {
    const srcPath = path.join(src, entry.name);
    const destPath = path.join(dest, entry.name);
    if (entry.isDirectory()) {
      copyDirRecursive(srcPath, destPath);
    } else {
      fs.copyFileSync(srcPath, destPath);
    }
  }
}
