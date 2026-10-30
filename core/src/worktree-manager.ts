import * as fs from 'fs';
import * as path from 'path';
import { execSync } from 'child_process';

export const GIT_NO_PROMPT_ENV = {
  GIT_TERMINAL_PROMPT: '0',
  GIT_ASKPASS: '',
  GIT_NO_REPLACE_OBJECTS: '1',
};

// Config flags for unattended git: never fire user hooks (a post-checkout or
// pre-commit hook can block forever on prompts), never start the fsmonitor
// daemon (hangs on `status`), never attempt GPG signing (blocks on a passphrase
// prompt). Placed before the subcommand (`git -c k=v <cmd>`).
const GIT_HARDENING_FLAGS =
  '-c core.hooksPath=/dev/null -c core.fsmonitor=false -c commit.gpgsign=false';

// Hard ceiling so a wedged git invocation can never hang an unattended run.
const GIT_CMD_TIMEOUT_MS = 15_000;

/** Prefix a git subcommand with the unattended hardening config flags. */
function gitCmd(args: string): string {
  return `git ${GIT_HARDENING_FLAGS} ${args}`;
}

export const MAX_WORKTREES = 10;

const EPHEMERAL_WORKTREE_PATTERNS = [
  /^agent-[0-9a-f]{7,}$/,
  /^wf_[0-9a-f]{8}-[0-9a-f]{3}-\d+$/,
  /^wt-[0-9a-f]{8}$/,
];

interface WorktreeSession {
  originalCwd: string;
  worktreePath: string;
  worktreeName: string;
  worktreeBranch: string;
  originalBranch?: string;
  originalHeadCommit?: string;
  sessionId?: string;
  creationDurationMs?: number;
  hookBased?: boolean;
  createdAt: string;
}

let currentSession: WorktreeSession | null = null;

export function getCurrentWorktreeSession(): WorktreeSession | null {
  return currentSession;
}

export function setWorktreeSession(session: WorktreeSession | null): void {
  currentSession = session;
}

export function validateWorktreeSlug(slug: string): string | null {
  if (slug.length > 64) return 'Slug exceeds 64 characters';
  if (/^[.\/]|\.\./.test(slug)) return 'Slug cannot start with . or / or contain ..';
  if (!/^[a-zA-Z0-9][a-zA-Z0-9._\/-]*$/.test(slug)) return 'Slug contains invalid characters';
  for (const segment of slug.split('/')) {
    if (!segment || /^\./.test(segment)) return `Invalid slug segment: "${segment}"`;
  }
  return null;
}

export function flattenSlug(slug: string): string {
  return slug.replace(/\//g, '+');
}

export function worktreeBranchName(slug: string): string {
  return `worktree-${flattenSlug(slug)}`;
}

export function createWorktreeSession(cwd: string, name: string): WorktreeSession {
  const startMs = Date.now();
  const worktreeDir = path.join(cwd, '.superinference', 'worktrees', name);
  const branch = worktreeBranchName(name);

  let originalBranch: string | undefined;
  let originalHead: string | undefined;
  const gitEnv = { ...process.env, ...GIT_NO_PROMPT_ENV };
  try {
    originalBranch = execSync(gitCmd('rev-parse --abbrev-ref HEAD'), { cwd, encoding: 'utf-8', env: gitEnv, timeout: GIT_CMD_TIMEOUT_MS }).trim();
    originalHead = execSync(gitCmd('rev-parse HEAD'), { cwd, encoding: 'utf-8', env: gitEnv, timeout: GIT_CMD_TIMEOUT_MS }).trim();
  } catch {}

  // Create the worktree on disk if it doesn't already exist.
  // enter-worktree.ts creates the worktree itself (with base_ref support) then
  // calls createWorktreeSession for metadata; task.ts relies on this function
  // to create the worktree as well.
  if (!fs.existsSync(worktreeDir)) {
    fs.mkdirSync(path.dirname(worktreeDir), { recursive: true });
    execSync(gitCmd(`worktree add -B "${branch}" "${worktreeDir}" HEAD`), { cwd, stdio: 'ignore', env: gitEnv, timeout: GIT_CMD_TIMEOUT_MS });
  }

  const session: WorktreeSession = {
    originalCwd: cwd,
    worktreePath: worktreeDir,
    worktreeName: name,
    worktreeBranch: branch,
    originalBranch,
    originalHeadCommit: originalHead,
    creationDurationMs: Date.now() - startMs,
    createdAt: new Date().toISOString(),
  };

  return session;
}

/**
 * Count changes in a worktree (uncommitted files + unpushed commits).
 * Returns null on git failure (fail-closed — never removes on error).
 */
export function countWorktreeChanges(worktreePath: string, cwd: string): { uncommitted: number; unpushed: number } | null {
  const gitEnv = { ...process.env, ...GIT_NO_PROMPT_ENV };
  try {
    const status = execSync(gitCmd('status --porcelain'), { cwd: worktreePath, encoding: 'utf-8', env: gitEnv, timeout: GIT_CMD_TIMEOUT_MS }).trim();
    const uncommitted = status ? status.split('\n').length : 0;
    let unpushed = 0;
    try {
      // Check if HEAD is detached
      let isDetached = false;
      try {
        execSync(gitCmd('symbolic-ref -q HEAD'), { cwd: worktreePath, encoding: 'utf-8', env: gitEnv, timeout: GIT_CMD_TIMEOUT_MS });
      } catch {
        isDetached = true;
      }

      // Use --not --remotes to count commits not on any remote branch.
      // This works regardless of whether the branch has an upstream configured
      // (worktree branches created by enter_worktree never have upstream set).
      // For detached HEAD, also exclude local branches so we only count
      // truly orphaned commits.
      const notFlags = isDetached ? '--not --remotes --branches' : '--not --remotes';
      const revList = execSync(gitCmd(`rev-list HEAD ${notFlags}`), { cwd: worktreePath, encoding: 'utf-8', env: gitEnv, timeout: GIT_CMD_TIMEOUT_MS }).trim();
      unpushed = revList ? revList.split('\n').length : 0;
    } catch {}
    return { uncommitted, unpushed };
  } catch {
    return null;
  }
}

export function cleanupStaleWorktrees(cwd: string, maxAgeDays: number = 30): string[] {
  const worktreeDir = path.join(cwd, '.superinference', 'worktrees');
  if (!fs.existsSync(worktreeDir)) return [];

  const cleaned: string[] = [];
  const now = Date.now();
  const maxAgeMs = maxAgeDays * 24 * 60 * 60 * 1000;
  const gitEnv = { ...process.env, ...GIT_NO_PROMPT_ENV };

  for (const entry of fs.readdirSync(worktreeDir)) {
    const fullPath = path.join(worktreeDir, entry);
    try {
      const stat = fs.statSync(fullPath);
      if (!stat.isDirectory()) continue;

      const isEphemeral = EPHEMERAL_WORKTREE_PATTERNS.some(p => p.test(entry));
      const ageThreshold = isEphemeral ? maxAgeMs / 2 : maxAgeMs;

      if ((now - stat.mtimeMs) > ageThreshold) {
        const changes = countWorktreeChanges(fullPath, cwd);
        if (changes === null) continue;
        if (changes.uncommitted > 0 || changes.unpushed > 0) continue;

        try {
          execSync(gitCmd(`worktree remove --force "${fullPath}"`), { cwd, timeout: 10000, env: gitEnv });
          cleaned.push(entry);
        } catch {}
      }
    } catch {}
  }

  // Prune any stale worktree metadata
  try {
    execSync(gitCmd('worktree prune'), { cwd, timeout: 5000, env: gitEnv, stdio: 'ignore' });
  } catch {}

  return cleaned;
}

export function symlinkLargeDirectories(sourceCwd: string, worktreePath: string, dirs: string[] = ['node_modules']): void {
  for (const dir of dirs) {
    const source = path.join(sourceCwd, dir);
    const target = path.join(worktreePath, dir);
    try {
      if (fs.existsSync(source) && !fs.existsSync(target)) {
        fs.symlinkSync(source, target, 'dir');
      }
    } catch {}
  }
}

/**
 * Copy files listed in `.worktreeinclude` from source to worktree.
 * Each line in the file is a relative path or glob pattern (simple globs only).
 * Lines starting with # are comments; empty lines are skipped.
 */
export function copyWorktreeIncludes(sourceCwd: string, worktreePath: string): string[] {
  const includeFile = path.join(sourceCwd, '.worktreeinclude');
  const copied: string[] = [];

  let lines: string[];
  try {
    lines = fs.readFileSync(includeFile, 'utf-8')
      .split('\n')
      .map(l => l.trim())
      .filter(l => l && !l.startsWith('#'));
  } catch {
    return copied;
  }

  for (const entry of lines) {
    const sourcePath = path.join(sourceCwd, entry);
    const targetPath = path.join(worktreePath, entry);
    try {
      if (!fs.existsSync(sourcePath)) continue;
      const stat = fs.statSync(sourcePath);
      fs.mkdirSync(path.dirname(targetPath), { recursive: true });
      if (stat.isDirectory()) {
        copyDirRecursive(sourcePath, targetPath);
      } else {
        fs.copyFileSync(sourcePath, targetPath);
      }
      copied.push(entry);
    } catch {}
  }

  return copied;
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

  return { valid: true };
}

export function findStaleWorktrees(repoPath: string): string[] {
  const worktreeDir = path.join(repoPath, '.superinference', 'worktrees');
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

function copyDirRecursive(src: string, dest: string): void {
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
