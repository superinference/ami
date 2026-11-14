import * as fs from 'fs';
import * as path from 'path';
import * as child_process from 'child_process';
import { ToolDefinition, ToolResult } from '../types';

/**
 * Dependency seam for tests (mirrors `_enterWorktreeHooks`). All git subprocess
 * calls route through this object so tests can inject a `git worktree remove`
 * failure to exercise the fs.rmSync + prune fallback deterministically, without
 * depending on the host git version's removal behaviour. The defaults are the
 * real child_process.execSync / fs ops, so production behaviour is unchanged.
 */
export const _exitWorktreeHooks = {
  execSync: child_process.execSync as typeof child_process.execSync,
  existsSync: fs.existsSync as typeof fs.existsSync,
  rmSync: fs.rmSync as typeof fs.rmSync,
};

const ALLOWED_KEYS = new Set(['action', 'discard_changes']);
const FILE_PREVIEW_THRESHOLD = 8;
const MAX_RESULT_CHARS = 100_000;

function pluralize(n: number, noun: string): string {
  return `${n} ${noun}${n === 1 ? '' : 's'}`;
}

function cap(text: string): string {
  return text.length > MAX_RESULT_CHARS ? text.slice(0, MAX_RESULT_CHARS) + '\n[... truncated ...]' : text;
}

export const exitWorktreeTool: ToolDefinition = {
  name: 'exit_worktree',
  description:
    'Exit a worktree session created by enter_worktree.\n' +
    'Scope: only acts on worktrees under .ami/worktrees/; elsewhere it is an informational no-op (no filesystem change). ' +
    'When to use: on explicit user request to finish or discard isolated work — not proactively.\n' +
    'action "keep" preserves the worktree and its branch on disk (your work stays saved there); ' +
    'action "remove" deletes the worktree AND its branch. ' +
    'discard_changes (default false) must be true to remove a worktree that has uncommitted files or unpushed commits — ' +
    'otherwise the tool refuses and tells you to confirm with the user or use action:"keep". ' +
    'On exit the session returns to the original working directory.',
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
        description: 'Required true when action is "remove" and the worktree has uncommitted files OR unpushed commits. The tool refuses otherwise. Default false.',
        default: false,
      },
    },
    required: ['action'],
  },
  isReadOnly: false,

  async execute(input, context): Promise<ToolResult> {
    // Strict input: reject unknown keys.
    const unknown = Object.keys(input).filter(k => !ALLOWED_KEYS.has(k));
    if (unknown.length > 0) {
      return { output: `Error: unknown parameter(s): ${unknown.join(', ')}.`, isError: true };
    }
    const action = input.action as string;
    const discardChanges = input.discard_changes === true;

    if (action !== 'keep' && action !== 'remove') {
      return { output: 'Error: action must be "keep" or "remove".', isError: true };
    }

    try {
      child_process.execSync('git rev-parse --is-inside-work-tree', { cwd: context.cwd, stdio: 'ignore' });
    } catch {
      return {
        output: 'Error: not inside a git repository. For non-git VCS, configure WorktreeCreate/WorktreeRemove hooks.',
        isError: true,
      };
    }

    let worktreePath: string;
    try {
      worktreePath = child_process.execSync('git rev-parse --show-toplevel', { cwd: context.cwd, encoding: 'utf-8' }).trim().normalize('NFC');
    } catch {
      return { output: 'Error: could not determine worktree path.', isError: true };
    }

    if (!worktreePath.includes('.ami/worktrees/')) {
      return { output: 'No active worktree session (not inside a .ami/worktrees/ path). No filesystem changes made.', isError: false };
    }

    const { setWorktreeSession, getCurrentWorktreeSession, countWorktreeChanges } = require('../worktree-manager');

    // Session-identity scope guard: if a session exists and points at a DIFFERENT
    // worktree, refuse to touch this (foreign) one.
    const existing = getCurrentWorktreeSession();
    if (existing?.worktreePath && path.resolve(existing.worktreePath).normalize('NFC') !== path.resolve(worktreePath).normalize('NFC')) {
      return {
        output: `No-op: this worktree was not created by the active session. Active session owns ${existing.worktreePath}. No filesystem changes made.`,
        isError: false,
      };
    }

    // Resolve the worktree's branch now (used for keep messaging + remove teardown).
    let branch: string | undefined = existing?.worktreeBranch;
    if (!branch) {
      try { branch = child_process.execSync('git rev-parse --abbrev-ref HEAD', { cwd: worktreePath, encoding: 'utf-8' }).trim(); } catch { /* detached */ }
    }
    const branchLabel = branch ? ` (branch ${branch})` : '';

    if (action === 'keep') {
      const session = getCurrentWorktreeSession();
      let returned = '';
      if (session) {
        if (session.originalCwd) {
          context.cwd = session.originalCwd;
          process.chdir(session.originalCwd);
          returned = `\nReturned to ${session.originalCwd}`;
        }
        setWorktreeSession(null);
      }
      // Do NOT fire executeWorktreeRemove hook — we are keeping the worktree.
      return {
        output: cap(`Worktree kept${branchLabel}: your work is saved at ${worktreePath}. Resume with: cd ${worktreePath}${returned}`),
      };
    }

    // TOCTOU: re-count change state at execution time.
    const changes = countWorktreeChanges(worktreePath, worktreePath);
    if (!discardChanges) {
      if (changes === null) {
        return {
          output: 'Error: could not determine worktree status (git check failed). Set discard_changes=true to force removal, or use action:"keep".',
          isError: true,
        };
      }
      if (changes.uncommitted > 0) {
        let status = '';
        try { status = child_process.execSync('git status --porcelain', { cwd: worktreePath, encoding: 'utf-8' }).trim(); } catch { /* best effort */ }
        const files = status ? status.split('\n') : [];
        const preview = files.length > FILE_PREVIEW_THRESHOLD
          ? `${files.slice(0, FILE_PREVIEW_THRESHOLD).join('\n')}\n… and ${files.length - FILE_PREVIEW_THRESHOLD} more`
          : files.join('\n');
        return {
          output: cap(`Error: worktree${branchLabel} has ${pluralize(changes.uncommitted, 'uncommitted file')}:\n${preview}\n\nSet discard_changes=true to force removal (the branch will be deleted), or use action:"keep". Confirm with the user before discarding.`),
          isError: true,
        };
      }
      if (changes.unpushed > 0) {
        return {
          output: `Error: worktree${branchLabel} has ${pluralize(changes.unpushed, 'unpushed commit')}.\n\nSet discard_changes=true to force removal (the branch will be deleted), or use action:"keep". Confirm with the user before discarding.`,
          isError: true,
        };
      }
    }

    // Snapshot what will be discarded for the success summary.
    const discardedFiles = changes?.uncommitted ?? 0;
    const discardedCommits = changes?.unpushed ?? 0;

    const repoRoot = worktreePath.split('.ami/worktrees/')[0];
    try {
      _exitWorktreeHooks.execSync(`git worktree remove --force "${worktreePath}"`, { cwd: worktreePath, encoding: 'utf-8', stdio: 'pipe' });
    } catch (e) {
      try {
        if (_exitWorktreeHooks.existsSync(worktreePath)) {
          _exitWorktreeHooks.rmSync(worktreePath, { recursive: true, force: true });
        }
        _exitWorktreeHooks.execSync('git worktree prune', { cwd: repoRoot, stdio: 'ignore' });
      } catch {
        return { output: `Error removing worktree: ${e instanceof Error ? e.message : String(e)}`, isError: true };
      }
    }

    // Delete the (now-unused) worktree branch as part of teardown.
    if (branch) {
      try { _exitWorktreeHooks.execSync(`git branch -D "${branch}"`, { cwd: repoRoot, stdio: 'ignore' }); } catch { /* branch may be checked out elsewhere / gone */ }
    }

    const activeSession = getCurrentWorktreeSession();
    let returned = '';
    if (activeSession) {
      if (activeSession.originalCwd) {
        context.cwd = activeSession.originalCwd;
        returned = `\nReturned to ${activeSession.originalCwd}`;
      }
      setWorktreeSession(null);
    }
    if (context._hookManager) {
      context._hookManager.executeWorktreeRemove?.({ worktreePath }).catch(() => {});
    }

    const discardSummary = (discardedFiles > 0 || discardedCommits > 0)
      ? ` (discarded ${pluralize(discardedCommits, 'commit')} and ${pluralize(discardedFiles, 'uncommitted file')})`
      : ' (no changes)';
    return { output: cap(`Worktree removed${branchLabel}${discardSummary}.${returned}`) };
  },
};
