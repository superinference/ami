import * as fs from 'fs';
import * as child_process from 'child_process';
import { ToolDefinition, ToolResult } from '../types';

export const exitWorktreeTool: ToolDefinition = {
  name: 'exit_worktree',
  description:
    'Exit a worktree session created by enter_worktree. ' +
    'Use action "keep" to preserve the worktree on disk, or "remove" to delete it.',
  inputSchema: {
    type: 'object',
    properties: {
      action: {
        type: 'string',
        description: '"keep" leaves the worktree and branch on disk; "remove" deletes both.',
        enum: ['keep', 'remove'],
      },
      discard_changes: {
        type: 'boolean',
        description: 'Required true when action is "remove" and the worktree has uncommitted changes. The tool refuses otherwise.',
      },
    },
    required: ['action'],
  },
  isReadOnly: false,

  async execute(input, context): Promise<ToolResult> {
    const action = input.action as string;
    const discardChanges = input.discard_changes === true;

    if (action !== 'keep' && action !== 'remove') {
      return { output: 'Error: action must be "keep" or "remove".', isError: true };
    }

    try {
      child_process.execSync('git rev-parse --is-inside-work-tree', { cwd: context.cwd, stdio: 'ignore' });
    } catch {
      return { output: 'Error: not inside a git repository.', isError: true };
    }

    let worktreePath: string;
    try {
      worktreePath = child_process.execSync('git rev-parse --show-toplevel', { cwd: context.cwd, encoding: 'utf-8' }).trim();
    } catch {
      return { output: 'Error: could not determine worktree path.', isError: true };
    }

    if (!worktreePath.includes('.superinference/worktrees/')) {
      return { output: 'No active worktree session (not inside a .superinference/worktrees/ path).', isError: false };
    }

    if (action === 'keep') {
      const { setWorktreeSession, getCurrentWorktreeSession } = require('../worktree-manager');
      const session = getCurrentWorktreeSession();
      if (session) {
        // Restore original CWD before clearing session
        if (session.originalCwd) {
          context.cwd = session.originalCwd;
          process.chdir(session.originalCwd);
        }
        setWorktreeSession(null);
      }
      // Do NOT fire executeWorktreeRemove hook — we are keeping the worktree
      return { output: `Worktree kept at: ${worktreePath}` };
    }

    if (!discardChanges) {
      const { countWorktreeChanges } = require('../worktree-manager');
      const changes = countWorktreeChanges(worktreePath, worktreePath);
      if (changes === null) {
        return {
          output: 'Error: could not determine worktree status (git check failed). Set discard_changes=true to force removal.',
          isError: true,
        };
      }
      if (changes.uncommitted > 0) {
        const status = child_process.execSync('git status --porcelain', { cwd: worktreePath, encoding: 'utf-8' }).trim();
        return {
          output: `Error: worktree has ${changes.uncommitted} uncommitted file(s):\n${status}\n\nSet discard_changes=true to force removal.`,
          isError: true,
        };
      }
      if (changes.unpushed > 0) {
        return {
          output: `Error: worktree branch has ${changes.unpushed} unpushed commit(s).\n\nSet discard_changes=true to force removal.`,
          isError: true,
        };
      }
    }

    try {
      child_process.execSync(`git worktree remove --force "${worktreePath}"`, { cwd: worktreePath, encoding: 'utf-8', stdio: 'pipe' });
    } catch (e) {
      try {
        if (fs.existsSync(worktreePath)) {
          fs.rmSync(worktreePath, { recursive: true, force: true });
        }
        const repoRoot = worktreePath.split('.superinference/worktrees/')[0];
        child_process.execSync('git worktree prune', { cwd: repoRoot, stdio: 'ignore' });
      } catch {
        return { output: `Error removing worktree: ${e instanceof Error ? e.message : String(e)}`, isError: true };
      }
    }

    const { setWorktreeSession, getCurrentWorktreeSession } = require('../worktree-manager');
    const activeSession = getCurrentWorktreeSession();
    if (activeSession) {
      if (activeSession.originalCwd) {
        context.cwd = activeSession.originalCwd;
      }
      setWorktreeSession(null);
    }
    if (context._hookManager) {
      context._hookManager.executeWorktreeRemove?.({ worktreePath }).catch(() => {});
    }

    return { output: `Worktree removed: ${worktreePath}` };
  },
};
