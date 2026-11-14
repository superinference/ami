import * as fs from 'fs';
import * as path from 'path';
import * as child_process from 'child_process';
import * as crypto from 'crypto';
import { ToolDefinition, ToolResult } from '../types';

const SLUG_SEGMENT_RE = /^[a-zA-Z0-9._-]+$/;

/**
 * Run git with NO SHELL (execFile with an argv array) so a branch name, path, or
 * ref can never be interpreted as a shell metacharacter (command-injection
 * immune — parity with worktree-manager's gitExec and Claude's
 * execFileNoThrowWithCwd). Returns trimmed stdout.
 */
function runGit(
  args: string[],
  opts: { cwd: string; stdio?: 'ignore' | 'pipe'; timeout?: number; env?: NodeJS.ProcessEnv } = { cwd: process.cwd() },
): string {
  const out = child_process.execFileSync('git', args, {
    cwd: opts.cwd,
    encoding: 'utf-8',
    stdio: opts.stdio === 'ignore' ? 'ignore' : ['ignore', 'pipe', 'pipe'],
    timeout: opts.timeout,
    env: opts.env,
  });
  return typeof out === 'string' ? out.trim() : '';
}

/**
 * Dependency seam for tests (mirrors the _createClient / _lspDiagHooks pattern).
 * All git subprocess calls route through `exec` (an argv array, NO shell) so
 * tests can inject failures (e.g. a corrupted `git worktree list`, an
 * unresolvable HEAD) via `mock.method(_enterWorktreeHooks, 'exec', ...)`. The
 * default implementation is the real no-shell git runner, so production
 * behaviour is unchanged.
 */
export const _enterWorktreeHooks = {
  exec: runGit,
};

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
  // Offline-friendly, NO shell pipe: read the origin/HEAD symref directly
  // instead of `git remote show origin | grep` (a network + shell-pipe call).
  try {
    const sym = _enterWorktreeHooks.exec(['symbolic-ref', '--short', 'refs/remotes/origin/HEAD'], { cwd });
    const match = sym.match(/^origin\/(.+)$/);
    if (match) return match[1];
  } catch { /* fallback */ }
  try {
    _enterWorktreeHooks.exec(['rev-parse', '--verify', 'refs/heads/main'], { cwd, stdio: 'ignore' });
    return 'main';
  } catch { /* fallback */ }
  return 'master';
}

export const enterWorktreeTool: ToolDefinition = {
  name: 'enter_worktree',
  description:
    'Create or enter a git worktree for isolated file modifications, on a new branch under .ami/worktrees/. ' +
    'WHEN TO USE: isolating risky or parallel changes from the main working tree (a large refactor you may want to throw away, work you want on its own branch). ' +
    'WHEN NOT TO USE: ordinary edits in the current tree, or when the user did not ask for isolation — a worktree adds branch/cwd bookkeeping overhead. ' +
    'Use exit_worktree (action "keep" or "remove") to leave. ' +
    'Requires a git repository; outside one, configure WorktreeCreate/WorktreeRemove hooks for VCS-agnostic isolation.',
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
      base_ref: {
        type: 'string',
        description: 'Git ref (branch/tag/commit) to base the new worktree on. Defaults to origin/<default-branch>, then HEAD.',
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
      _enterWorktreeHooks.exec(['rev-parse', '--is-inside-work-tree'], { cwd: context.cwd, stdio: 'ignore' });
    } catch {
      return {
        output: 'Error: not inside a git repository. To use worktree isolation with another VCS, configure WorktreeCreate/WorktreeRemove hooks in settings.',
        isError: true,
      };
    }

    // Nested-isolation guard: refuse to create a NEW worktree while the current
    // working directory is already inside a worktree (entering an existing one is
    // still allowed). Detected from the cwd path so it is not sensitive to stale
    // global session state.
    if (!existingPath && (context.cwd || '').includes('.ami/worktrees/')) {
      return { output: 'Error: already inside a worktree. Use exit_worktree before creating another.', isError: true };
    }

    if (existingPath) {
      if (!fs.existsSync(existingPath)) {
        return { output: `Error: path "${existingPath}" does not exist.`, isError: true };
      }
      try {
        const list = _enterWorktreeHooks.exec(['worktree', 'list', '--porcelain'], { cwd: context.cwd, timeout: 5000 });
        // NFC-normalize parsed worktree paths before the identity comparison.
        const worktrees = list.split('\n').filter(l => l.startsWith('worktree ')).map(l => l.slice(9).normalize('NFC'));
        const resolved = path.resolve(existingPath).normalize('NFC');
        const entered = worktrees.find(w => path.resolve(w).normalize('NFC') === resolved);
        if (!entered) {
          return { output: `Error: "${existingPath}" is not a registered worktree.`, isError: true };
        }
      } catch (e) {
        return { output: `Error listing worktrees: ${e instanceof Error ? e.message : String(e)}`, isError: true };
      }
      return {
        output: `Entered existing worktree at: ${existingPath}. Run exit_worktree when done.`,
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
    const worktreesBase = path.join(context.cwd, '.ami', 'worktrees');
    if (fs.existsSync(worktreesBase)) {
      const { cleanupStaleWorktrees } = require('../worktree-manager');
      try { cleanupStaleWorktrees(context.cwd); } catch { /* non-critical */ }
    }

    const worktreeDir = path.join(context.cwd, '.ami', 'worktrees', slug);
    if (fs.existsSync(worktreeDir)) {
      return { output: `Error: worktree "${slug}" already exists at ${worktreeDir}. Use path to enter an existing worktree.`, isError: true };
    }

    const defaultBranch = getDefaultBranch(context.cwd);
    const branchName = `worktree/${slug}`;

    const gitEnv = { ...process.env, GIT_TERMINAL_PROMPT: '0', GIT_ASKPASS: '' };

    const requestedBaseRef = typeof input.base_ref === 'string' ? input.base_ref.trim() : '';
    if (requestedBaseRef) {
      // Explicit base ref: verify it resolves to a commit, else fail loud
      // (do not silently fall back to HEAD and branch from the wrong place).
      try {
        _enterWorktreeHooks.exec(['rev-parse', '--verify', `${requestedBaseRef}^{commit}`], { cwd: context.cwd, stdio: 'ignore', env: gitEnv });
      } catch {
        return { output: `Error: base_ref "${requestedBaseRef}" is not a valid git ref (branch/tag/commit).`, isError: true };
      }
    }
    try {
      const baseRef = requestedBaseRef || `origin/${defaultBranch}`;
      try {
        _enterWorktreeHooks.exec(['rev-parse', '--verify', baseRef], { cwd: context.cwd, stdio: 'ignore', env: gitEnv });
        _enterWorktreeHooks.exec(['worktree', 'add', '-B', branchName, worktreeDir, baseRef], { cwd: context.cwd, stdio: 'ignore', env: gitEnv });
      } catch {
        if (requestedBaseRef) throw new Error(`could not create worktree from base_ref "${requestedBaseRef}"`);
        _enterWorktreeHooks.exec(['worktree', 'add', '-B', branchName, worktreeDir, 'HEAD'], { cwd: context.cwd, stdio: 'ignore', env: gitEnv });
      }
    } catch (e) {
      return { output: `Error creating worktree: ${e instanceof Error ? e.message : String(e)}`, isError: true };
    }

    let commitHash: string;
    try {
      commitHash = _enterWorktreeHooks.exec(['rev-parse', '--short', 'HEAD'], { cwd: worktreeDir });
    } catch {
      commitHash = 'unknown';
    }

    const { createWorktreeSession, setWorktreeSession, symlinkLargeDirectories } = require('../worktree-manager');
    const session = createWorktreeSession(context.cwd, slug);
    setWorktreeSession(session);
    symlinkLargeDirectories(context.cwd, session.worktreePath);

    // Capture the pre-reassignment cwd so the cwd-changed hook reports the
    // correct oldCwd whether or not context.cwd was set (equivalent to the old
    // `_originalCwd ?? context.cwd`, but without an unreachable fallback arm).
    const originalCwd = context.cwd;
    if (context.cwd) {
      (context as any)._originalCwd = context.cwd;
      (context as any).cwd = worktreeDir;
    }

    if (context._hookManager) {
      context._hookManager.executeWorktreeCreate?.({ name: slug }).catch(() => {});
      context._hookManager.executeCwdChanged?.({ oldCwd: originalCwd, newCwd: worktreeDir }).catch(() => {});
    }

    const baseLabel = requestedBaseRef || `origin/${defaultBranch}`;
    return {
      output:
        `Worktree created (session switched to it):\n` +
        `  Path: ${worktreeDir}\n  Branch: ${branchName} (based on ${baseLabel})\n  Base commit: ${commitHash}\n` +
        '  Leave with exit_worktree (action:"keep" to preserve, "remove" to delete).',
    };
  },
};
