import * as fs from 'fs';
import * as path from 'path';
import * as child_process from 'child_process';
import * as crypto from 'crypto';
import { ToolDefinition, ToolResult } from '../types';

const SLUG_SEGMENT_RE = /^[a-zA-Z0-9._-]+$/;

function validateSlug(name: string): boolean {
  if (name.length > 64) return false;
  if (path.isAbsolute(name)) return false;
  const segments = name.split('/');
  for (const seg of segments) {
    if (!seg || seg === '.' || seg === '..') return false;
    if (!SLUG_SEGMENT_RE.test(seg)) return false;
  }
  return true;
}

function getDefaultBranch(cwd: string): string {
  try {
    const remote = child_process.execSync('git remote show origin 2>/dev/null | grep "HEAD branch"', { cwd, encoding: 'utf-8' });
    const match = remote.match(/HEAD branch:\s*(\S+)/);
    if (match) return match[1];
  } catch { /* fallback */ }
  try {
    child_process.execSync('git rev-parse --verify refs/heads/main', { cwd, stdio: 'ignore' });
    return 'main';
  } catch { /* fallback */ }
  return 'master';
}

export const enterWorktreeTool: ToolDefinition = {
  name: 'enter_worktree',
  description:
    'Create or enter a git worktree for isolated file modifications. ' +
    'Creates a new branch under .superinference/worktrees/. ' +
    'Use exit_worktree to leave.',
  inputSchema: {
    type: 'object',
    properties: {
      name: {
        type: 'string',
        description: 'Name for the new worktree. Letters, digits, dots, underscores, dashes; max 64 chars. A random name is generated if omitted.',
      },
      path: {
        type: 'string',
        description: 'Path to an existing worktree to enter instead of creating a new one. Mutually exclusive with name.',
      },
    },
  },
  isReadOnly: false,

  async execute(input, context): Promise<ToolResult> {
    const name = input.name as string | undefined;
    const existingPath = input.path as string | undefined;

    if (name && existingPath) {
      return { output: 'Error: name and path are mutually exclusive.', isError: true };
    }

    try {
      child_process.execSync('git rev-parse --is-inside-work-tree', { cwd: context.cwd, stdio: 'ignore' });
    } catch {
      return { output: 'Error: not inside a git repository.', isError: true };
    }

    if (existingPath) {
      if (!fs.existsSync(existingPath)) {
        return { output: `Error: path "${existingPath}" does not exist.`, isError: true };
      }
      try {
        const list = child_process.execSync('git worktree list --porcelain', { cwd: context.cwd, encoding: 'utf-8' });
        const worktrees = list.split('\n').filter(l => l.startsWith('worktree ')).map(l => l.slice(9));
        const resolved = path.resolve(existingPath);
        if (!worktrees.includes(resolved)) {
          return { output: `Error: "${existingPath}" is not a registered worktree.`, isError: true };
        }
      } catch (e) {
        return { output: `Error listing worktrees: ${e instanceof Error ? e.message : String(e)}`, isError: true };
      }
      return {
        output: `Entered existing worktree at: ${existingPath}`,
      };
    }

    const slug = name || `wt-${crypto.randomBytes(4).toString('hex')}`;
    if (!validateSlug(slug)) {
      return { output: `Error: invalid worktree name "${slug}". Use letters, digits, dots, underscores, dashes (max 64 chars per segment).`, isError: true };
    }

    const { validateWorktreeSlug } = require('../worktree-manager');
    const validationError = validateWorktreeSlug(slug);
    if (validationError) return { output: `Error: ${validationError}`, isError: true };

    // Opportunistically clean up stale worktrees (> 30 days, no uncommitted changes)
    const worktreesBase = path.join(context.cwd, '.superinference', 'worktrees');
    if (fs.existsSync(worktreesBase)) {
      const { cleanupStaleWorktrees } = require('../worktree-manager');
      try { cleanupStaleWorktrees(context.cwd); } catch { /* non-critical */ }
    }

    const worktreeDir = path.join(context.cwd, '.superinference', 'worktrees', slug);
    if (fs.existsSync(worktreeDir)) {
      return { output: `Error: worktree "${slug}" already exists at ${worktreeDir}. Use path to enter an existing worktree.`, isError: true };
    }

    const defaultBranch = getDefaultBranch(context.cwd);
    const branchName = `worktree/${slug}`;

    const gitEnv = { ...process.env, GIT_TERMINAL_PROMPT: '0', GIT_ASKPASS: '' };

    try {
      const baseRef = `origin/${defaultBranch}`;
      try {
        child_process.execSync(`git rev-parse --verify ${baseRef}`, { cwd: context.cwd, stdio: 'ignore', env: gitEnv });
        child_process.execSync(`git worktree add -B "${branchName}" "${worktreeDir}" "${baseRef}"`, { cwd: context.cwd, stdio: 'ignore', env: gitEnv });
      } catch {
        child_process.execSync(`git worktree add -B "${branchName}" "${worktreeDir}" HEAD`, { cwd: context.cwd, stdio: 'ignore', env: gitEnv });
      }
    } catch (e) {
      return { output: `Error creating worktree: ${e instanceof Error ? e.message : String(e)}`, isError: true };
    }

    let commitHash: string;
    try {
      commitHash = child_process.execSync('git rev-parse --short HEAD', { cwd: worktreeDir, encoding: 'utf-8' }).trim();
    } catch {
      commitHash = 'unknown';
    }

    const { createWorktreeSession, setWorktreeSession, symlinkLargeDirectories } = require('../worktree-manager');
    const session = createWorktreeSession(context.cwd, slug);
    setWorktreeSession(session);
    symlinkLargeDirectories(context.cwd, session.worktreePath);

    if (context.cwd) {
      (context as any)._originalCwd = context.cwd;
      (context as any).cwd = worktreeDir;
    }

    if (context._hookManager) {
      context._hookManager.executeWorktreeCreate?.({ name: slug }).catch(() => {});
      context._hookManager.executeCwdChanged?.({ oldCwd: (context as any)._originalCwd ?? context.cwd, newCwd: worktreeDir }).catch(() => {});
    }

    return {
      output: `Worktree created:\n  Path: ${worktreeDir}\n  Branch: ${branchName}\n  Base commit: ${commitHash}`,
    };
  },
};
