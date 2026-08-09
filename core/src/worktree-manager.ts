import * as fs from 'fs';
import * as path from 'path';
import { execSync } from 'child_process';

const GIT_NO_PROMPT_ENV = {
  GIT_TERMINAL_PROMPT: '0',
  GIT_ASKPASS: '',
};

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
    originalBranch = execSync('git rev-parse --abbrev-ref HEAD', { cwd, encoding: 'utf-8', env: gitEnv }).trim();
    originalHead = execSync('git rev-parse HEAD', { cwd, encoding: 'utf-8', env: gitEnv }).trim();
  } catch {}

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
    const status = execSync('git status --porcelain', { cwd: worktreePath, encoding: 'utf-8', env: gitEnv }).trim();
    const uncommitted = status ? status.split('\n').length : 0;
    let unpushed = 0;
    try {
      const revList = execSync('git rev-list @{upstream}..HEAD 2>/dev/null', { cwd: worktreePath, encoding: 'utf-8', env: gitEnv }).trim();
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
          execSync(`git worktree remove --force "${fullPath}"`, { cwd, timeout: 10000, env: gitEnv });
          cleaned.push(entry);
        } catch {}
      }
    } catch {}
  }

  // Prune any stale worktree metadata
  try {
    execSync('git worktree prune', { cwd, timeout: 5000, env: gitEnv, stdio: 'ignore' });
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
