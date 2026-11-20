import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolResult } from '../types';
import { gitExec, stashToCleanState } from '../worktree-manager';
import { isPathWithin } from '../utils/path';

/**
 * Every git call in the exit path — reads AND the destructive teardown — runs
 * through worktree-manager's hardened `gitExec`: NO SHELL (execFile with an argv
 * array, so a worktree path / branch name can never be a shell metacharacter),
 * a hard command timeout, GIT_NO_PROMPT_ENV, and core.hooksPath/fsmonitor/gpg
 * disabled. A wedged git (credential prompt, hanging hook, fsmonitor daemon)
 * can therefore never hang the exit. This is the single canonical hardened git
 * runner for the whole worktree subsystem (no parallel, unbounded execSync).
 */

/**
 * Dependency seam for tests (mirrors `_enterWorktreeHooks`). The destructive
 * teardown git calls (worktree remove / prune / branch -D) route through this
 * object so tests can inject a `git worktree remove` failure to exercise the
 * fs.rmSync + prune fallback deterministically, without depending on the host
 * git version's removal behaviour. `exec` is the hardened no-shell git runner,
 * so production behaviour is unchanged.
 */
export const _exitWorktreeHooks = {
  exec: gitExec,
  existsSync: fs.existsSync as typeof fs.existsSync,
  rmSync: fs.rmSync as typeof fs.rmSync,
  // Lock-release delay (ms) awaited AFTER `git worktree remove` and BEFORE
  // `git branch -D <worktree-branch>` (parity: the reference sleeps ~100ms so git has
  // released the worktree's locks, avoiding a "cannot delete branch, locked"
  // race). Exposed on the hook object so tests can set it to 0 (fast) or assert
  // it was awaited, without a real 100ms wait in every removal test.
  branchDeleteDelayMs: 100,
  sleep: (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms)),
  // Recoverable stash-to-clean-state used by the non-destructive `stash` action
  // (parity: the reference's stashToCleanState). Routed through the hook object so a test
  // can simulate a stash failure deterministically without a real dirty tree.
  stash: stashToCleanState,
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
    'When to use: on explicit user request to finish, merge, or discard isolated work — not proactively.\n' +
    'action "keep" preserves the worktree and its branch on disk (your work stays saved there); ' +
    'action "merge" merges the worktree branch back into the original branch, then deletes the worktree AND its branch; ' +
    'action "remove" deletes the worktree AND its branch WITHOUT merging (discards the isolated work); ' +
    'action "stash" is the NON-DESTRUCTIVE alternative to "remove": it saves the worktree\'s uncommitted files ' +
    '(tracked and untracked) into a recoverable git stash, then deletes the worktree and its branch — the uncommitted ' +
    'work is NOT lost (recover it from the main checkout with git stash apply). "stash" refuses if the worktree has ' +
    'unpushed commits (a stash cannot preserve commits), telling you to use "merge"/"keep"/"remove" instead. ' +
    'discard_changes (default false) must be true to remove a worktree that has uncommitted files or unpushed commits, ' +
    'and to merge a worktree that still has uncommitted files (uncommitted work is never part of a merge and would be lost) — ' +
    'otherwise the tool refuses and tells you to confirm with the user or use action:"keep". ' +
    'A merge that would conflict is aborted and the worktree is preserved (never silently discarded). ' +
    'Behavior: on exit the session returns to the original working directory and CWD-dependent caches ' +
    '(system-prompt environment sections, the session-environment cache) are cleared so they reflect the ' +
    'restored directory; the tool is re-callable — you can start a fresh isolated worktree again afterwards with enter_worktree.',
  inputSchema: {
    type: 'object',
    properties: {
      action: {
        type: 'string',
        description: '"keep" leaves the worktree and branch on disk; "merge" merges the branch back into the original branch then deletes both; "remove" deletes both without merging; "stash" saves uncommitted files to a recoverable git stash then deletes both (the non-destructive alternative to "remove").',
        enum: ['keep', 'merge', 'remove', 'stash'],
      },
      discard_changes: {
        type: 'boolean',
        description: 'Required true when action is "remove" and the worktree has uncommitted files OR unpushed commits. The tool refuses otherwise. To preserve uncommitted work instead of discarding it, use action:"stash" (no discard_changes needed). Default false.',
        default: false,
      },
    },
    required: ['action'],
  },
  // Structured result shape (parity: the reference's ExitWorktreeTool outputSchema). A
  // frontend / resume path can validate the machine-readable fields it carries
  // alongside the human text before rendering. Object-root (ToolInputSchema) so it
  // passes the provider's object-root-only exposure guard.
  outputSchema: {
    type: 'object',
    properties: {
      action: { type: 'string', enum: ['keep', 'merge', 'remove', 'stash'], description: 'The action that was performed.' },
      originalCwd: { type: 'string', description: 'The directory the session returned to.' },
      worktreePath: { type: 'string', description: 'Absolute path of the worktree that was acted on.' },
      worktreeBranch: { type: 'string', description: 'Branch the worktree was on, when known.' },
      discardedFiles: { type: 'number', description: 'Count of uncommitted files discarded (remove action).' },
      discardedCommits: { type: 'number', description: 'Count of unpushed commits discarded (remove action).' },
      stashedFiles: { type: 'number', description: 'Count of uncommitted files saved to a recoverable git stash (stash action).' },
      tmuxSessionName: { type: 'string', description: 'Name of the worktree\'s tmux session, when one was attached (kept and still running on "keep"; killed on "remove").' },
      message: { type: 'string', description: 'Human-readable summary of what happened.' },
    },
    required: ['action', 'originalCwd', 'worktreePath', 'message'],
  },
  isReadOnly: false,
  // Human-friendly display name for renderers / discovery surfaces (parity: the reference's
  // ExitWorktreeTool `userFacingName() => 'Exiting worktree'`, UI.tsx L7-9). The snake_case
  // tool id `exit_worktree` is otherwise what a reader sees; this relabels it to the readable
  // activity phrase the model's turn is performing. Surfaced end-to-end via `ami --introspect`
  // (the feature-discovery manifest maps `ToolDefinition.userFacingName` → the tool entry), so
  // the affordance is discoverable without reading source. Static (input-independent), matching
  // the reference. [FUNCTIONAL CHANGE: additive affordance metadata, no behavior change.]
  userFacingName: () => 'Exiting worktree',
  // Per-input irreversible-operation classification (parity: the reference's
  // ExitWorktreeTool.isDestructive(input) — `input.action === 'remove'`). Only
  // `remove` destroys work (deletes the worktree AND its branch WITHOUT merging);
  // `keep` preserves everything and `merge` integrates committed work first. The
  // permission layer consults this to downgrade a wildcard allow to an explicit
  // prompt in `ask` mode so an irreversible discard is never silently auto-approved.
  isDestructive: (input) => input.action === 'remove',

  // Compact projection fed to the auto-mode security classifier (parity: the reference's
  // ExitWorktreeTool `toAutoClassifierInput(input) => input.action`). The
  // auto-approval decision keys on the keep/merge/remove/stash action rather than
  // the full argument blob; a non-string/omitted action projects to '' (skip).
  toAutoClassifierInput: (input) => (typeof input.action === 'string' ? input.action : ''),

  async execute(input, context): Promise<ToolResult> {
    // Strict input: reject unknown keys.
    const unknown = Object.keys(input).filter(k => !ALLOWED_KEYS.has(k));
    if (unknown.length > 0) {
      return { output: `Error: unknown parameter(s): ${unknown.join(', ')}.`, isError: true };
    }
    const action = input.action as string;
    const discardChanges = input.discard_changes === true;

    if (action !== 'keep' && action !== 'remove' && action !== 'merge' && action !== 'stash') {
      return { output: 'Error: action must be "keep", "merge", "remove", or "stash".', isError: true };
    }
    const wantMerge = action === 'merge';
    const wantStash = action === 'stash';

    try {
      gitExec(['rev-parse', '--is-inside-work-tree'], { cwd: context.cwd, stdio: 'ignore' });
    } catch {
      return {
        output: 'Error: not inside a git repository. For non-git VCS, configure WorktreeCreate/WorktreeRemove hooks.',
        isError: true,
      };
    }

    let worktreePath: string;
    try {
      worktreePath = gitExec(['rev-parse', '--show-toplevel'], { cwd: context.cwd }).normalize('NFC');
    } catch {
      return { output: 'Error: could not determine worktree path.', isError: true };
    }

    if (!worktreePath.includes('.ami/worktrees/')) {
      return { output: 'No active worktree session (not inside a .ami/worktrees/ path). No filesystem changes made.', isError: false };
    }

    // Concurrency-safe cwd restore (parity: the reference only restores the working dir it
    // actually changed — restoreSessionToOriginalCwd's `projectRootIsWorktree`
    // guard, utils/cwd.ts). AMI runs subagents concurrently in ONE process
    // (task.ts), so an UNCONDITIONAL `process.chdir(originalCwd)` here would yank
    // the single GLOBAL process cwd out from under every other concurrently-running
    // agent whenever ANY agent exits a worktree — any path/permission resolution
    // that falls back to `process.cwd()` would then resolve against the wrong dir.
    // Only touch the global process cwd when it is actually stranded INSIDE this
    // worktree (the foreground/owning session that cd'd in); a sibling agent whose
    // global cwd is elsewhere is left untouched. The per-call `context.cwd` is
    // restored regardless, so the owning session always returns correctly. Captured
    // BEFORE any destructive teardown so `process.cwd()` cannot throw ENOENT after
    // the worktree dir is deleted. [FUNCTIONAL CHANGE: additive concurrency guard.]
    let processCwdAtStart: string | null;
    try { processCwdAtStart = process.cwd(); } catch { processCwdAtStart = null; }
    const globalCwdInWorktree = processCwdAtStart !== null && isPathWithin(processCwdAtStart, worktreePath);

    const { setWorktreeSession, getCurrentWorktreeSession, countWorktreeChanges, getWorktreeFileStatus } = require('../worktree-manager');

    // Session-identity scope guard: if a session exists and points at a DIFFERENT
    // worktree, refuse to touch this (foreign) one.
    const existing = getCurrentWorktreeSession();
    if (existing?.worktreePath && path.resolve(existing.worktreePath).normalize('NFC') !== path.resolve(worktreePath).normalize('NFC')) {
      return {
        output: `No-op: this worktree was not created by the active session. Active session owns ${existing.worktreePath}. No filesystem changes made.`,
        isError: false,
      };
    }

    // Session-ownership scope guard for DESTRUCTIVE actions (parity: the reference's
    // ExitWorktree only acts on a worktree created by enter_worktree IN THIS
    // SESSION; with no active session it is a clean no-op, filesystem unchanged).
    // AMI previously gated the destructive `remove`/`merge` purely on the
    // `.ami/worktrees/` path pattern, so cd'ing into a leftover worktree from a
    // previous session — or one created manually under that dir — and running
    // exit_worktree would force-remove it even though no active session owns it.
    // Require an owning session before any destructive teardown; otherwise return
    // an informational no-op. "keep" stays session-null tolerant (non-destructive).
    if (action !== 'keep' && !existing) {
      return {
        output: 'No active worktree session created by enter_worktree in this session; nothing to remove. No filesystem changes made.',
        isError: false,
      };
    }

    // Resolve the worktree's branch now (used for keep messaging + merge/remove
    // teardown). The worktree's own checked-out HEAD is the GROUND TRUTH for the
    // real branch name — session metadata (worktreeBranch) can diverge from the
    // branch git actually created (enter_worktree names it "worktree/<slug>" while
    // the session records "worktree-<slug>"), so trusting the session value alone
    // would merge/delete the wrong ref and silently leak the real branch. Prefer
    // the live HEAD; fall back to session metadata only when detached.
    let branch: string | undefined;
    try {
      const head = gitExec(['rev-parse', '--abbrev-ref', 'HEAD'], { cwd: worktreePath });
      branch = head === 'HEAD' ? undefined : head; // "HEAD" literal == detached
    } catch { /* rev-parse failed */ }
    if (!branch) branch = existing?.worktreeBranch;
    const branchLabel = branch ? ` (branch ${branch})` : '';

    if (action === 'keep') {
      const session = getCurrentWorktreeSession();
      let returned = '';
      if (session) {
        if (session.originalCwd) {
          context.cwd = session.originalCwd;
          // Only restore the GLOBAL process cwd when it was actually inside this
          // worktree (owning session); never move it for a concurrent agent.
          if (globalCwdInWorktree) { try { process.chdir(session.originalCwd); } catch { /* best effort */ } }
          returned = `\nReturned to ${session.originalCwd}`;
          // Mirror enter_worktree: fire executeCwdChanged so CWD-dependent caches
          // (system-prompt env sections, session-env cache) are invalidated for the
          // restored directory. Without this the exit path is asymmetric and caches
          // populated inside the worktree can leak into the restored session.
          context._hookManager?.executeCwdChanged?.({ oldCwd: worktreePath, newCwd: session.originalCwd })?.catch(() => {});
        }
        setWorktreeSession(null);
      }
      // Do NOT fire executeWorktreeRemove hook — we are keeping the worktree.
      // Surface the tmux reattach command when the kept worktree owns a tmux
      // session (parity: the reference's ExitWorktreeTool keep branch — "Tmux session <n>
      // is still running; reattach with: tmux attach -t <n>"). The session is LEFT
      // running on keep; only a destructive remove tears it down.
      const tmuxName = session?.tmuxSessionName;
      const tmuxNote = tmuxName
        ? ` Tmux session ${tmuxName} is still running; reattach with: tmux attach -t ${tmuxName}`
        : '';
      const keptMsg = `Worktree kept${branchLabel}: your work is saved at ${worktreePath}. Resume with: cd ${worktreePath}${returned}${tmuxNote}`;
      return {
        output: cap(keptMsg),
        // Structured result (parity: the reference's ExitWorktreeTool outputSchema) carried
        // on metadata alongside the human text so a resume/frontend path can read the
        // machine-readable fields without parsing prose. "keep" discards nothing.
        metadata: {
          action,
          originalCwd: session?.originalCwd ?? worktreePath,
          worktreePath,
          worktreeBranch: branch,
          discardedFiles: 0,
          discardedCommits: 0,
          ...(tmuxName ? { tmuxSessionName: tmuxName } : {}),
          message: keptMsg,
        },
      };
    }

    // TOCTOU: re-count change state at execution time. Pass the session's base
    // commit so unpushed commits are counted as the precise session delta
    // (originalHeadCommit..HEAD) rather than the remote-based count, which would
    // over-count pre-existing local-only base commits and wrongly refuse remove.
    const changes = countWorktreeChanges(worktreePath, worktreePath, { baseCommit: existing?.originalHeadCommit });

    // Non-destructive "stash" path (parity: the reference's stashToCleanState / the
    // "Stash changes and continue" affordance): instead of discarding the
    // worktree's uncommitted work, save it to a recoverable git stash (which
    // survives the worktree's removal — refs/stash lives in the shared git dir),
    // then fall through to the SAME teardown as remove. Uncommitted work is never
    // lost. A stash cannot preserve COMMITS, so a worktree with unpushed commits
    // is refused here (directing the user to merge/keep/remove) — keeping "stash"
    // strictly non-destructive.
    let stashedFiles = 0;
    if (wantStash) {
      if (changes === null) {
        return {
          output: 'Error: could not determine worktree status (git check failed). Use action:"keep" to preserve it, or action:"remove" with discard_changes=true.',
          isError: true,
        };
      }
      if (changes.unpushed > 0) {
        return {
          output: cap(`Error: worktree${branchLabel} has ${pluralize(changes.unpushed, 'unpushed commit')} that a stash cannot preserve (stashing only saves uncommitted files; the commits would be lost when the branch is deleted). Use action:"merge" to integrate them into the original branch, action:"keep" to preserve everything on disk, or action:"remove" with discard_changes=true to discard them.`),
          isError: true,
        };
      }
      if (changes.uncommitted > 0) {
        const stashed = _exitWorktreeHooks.stash(worktreePath, `exit_worktree auto-stash${branch ? ` (${branch})` : ''}`);
        if (!stashed) {
          // Fail-closed (parity: TeleportStash refuses to continue on a failed
          // stash): do NOT proceed to a destructive teardown — the uncommitted
          // work is still only in the worktree, so leave it fully intact.
          return {
            output: cap(`Error: failed to stash the worktree's uncommitted changes — the worktree is preserved at ${worktreePath} and nothing was removed. Resolve the git state manually, or use action:"keep".`),
            isError: true,
          };
        }
        stashedFiles = changes.uncommitted;
      }
    }

    // What the user will lose if we proceed. For "merge", the committed/unpushed
    // work is NOT a loss — it is exactly what gets merged — so only uncommitted
    // files are at risk and require discard_changes. For "remove", both
    // uncommitted files and unpushed commits are discarded, so both are guarded.
    // "stash" has its own guard above (it preserves the work), so skip here.
    if (!discardChanges && !wantStash) {
      if (changes === null) {
        return {
          output: `Error: could not determine worktree status (git check failed). Set discard_changes=true to force ${wantMerge ? 'merge' : 'removal'}, or use action:"keep".`,
          isError: true,
        };
      }
      if (changes.uncommitted > 0 || (!wantMerge && changes.unpushed > 0)) {
        // Single consequence-explaining summary up front (parity with the reference's
        // WorktreeExitDialog): classify EVERY at-risk state together — "N
        // uncommitted files and M commits on <branch>" — so the model/user sees
        // the full cost of removal before confirming, instead of a sequence of
        // one-state-at-a-time refusals that hide the second loss. For "merge",
        // committed work is NOT a loss (it is exactly what gets merged), so only
        // uncommitted files are counted and commits are excluded.
        const parts: string[] = [];
        let preview = '';
        if (changes.uncommitted > 0) {
          parts.push(pluralize(changes.uncommitted, 'uncommitted file'));
          // Enumerate the concrete at-risk paths, split tracked vs untracked
          // (parity: the reference merges getFileStatus().tracked/.untracked before a
          // destructive confirm) so the user sees exactly what — and which kind —
          // would be lost, not just a count.
          const fileStatus = getWorktreeFileStatus(worktreePath) as { tracked: string[]; untracked: string[] } | null;
          const files = fileStatus
            ? [
                ...fileStatus.tracked.map((f: string) => `  modified: ${f}`),
                ...fileStatus.untracked.map((f: string) => `  untracked: ${f}`),
              ]
            : [];
          preview = '\n' + (files.length > FILE_PREVIEW_THRESHOLD
            ? `${files.slice(0, FILE_PREVIEW_THRESHOLD).join('\n')}\n… and ${files.length - FILE_PREVIEW_THRESHOLD} more`
            : files.join('\n'));
        }
        if (!wantMerge && changes.unpushed > 0) {
          parts.push(`${pluralize(changes.unpushed, 'unpushed commit')}${branch ? ` on ${branch}` : ''}`);
        }
        const consequence = wantMerge
          ? 'These uncommitted changes are not part of a merge and will be lost.'
          : 'Removing will discard this work permanently (the branch will be deleted too).';
        // Offer the non-destructive stash path only when it would actually help:
        // "stash" preserves uncommitted files but refuses on unpushed commits, so
        // surface it as the recoverable alternative only when there are none.
        const stashOffer = (!wantMerge && changes.unpushed === 0)
          ? ' Or use action:"stash" to save the uncommitted work to a recoverable git stash and remove the worktree (nothing lost).'
          : '';
        const hint = wantMerge
          ? 'Commit them first so they are included in the merge, set discard_changes=true to discard them and merge only committed work, or use action:"keep".'
          : `Set discard_changes=true to force removal, use action:"merge" to merge committed work into the original branch, or use action:"keep". Confirm with the user before discarding.${stashOffer}`;
        return {
          output: cap(`Error: worktree${branchLabel} has ${parts.join(' and ')}. ${consequence}${preview}\n\n${hint}`),
          isError: true,
        };
      }
    }

    // Snapshot what will be discarded for the success summary. For "stash" the
    // uncommitted files were SAVED to a stash (counted as stashedFiles), not
    // discarded, and unpushed commits were already refused — so nothing is lost.
    const discardedFiles = wantStash ? 0 : (changes?.uncommitted ?? 0);
    const discardedCommits = wantStash ? 0 : (changes?.unpushed ?? 0);

    const repoRoot = worktreePath.split('.ami/worktrees/')[0];

    // Merge-back: fold the worktree branch into the original branch BEFORE any
    // destructive teardown. A failed/conflicting merge is aborted and the
    // worktree is left fully intact so no committed work is ever lost.
    let mergedInto: string | undefined;
    if (wantMerge) {
      if (!branch) {
        return {
          output: 'Error: cannot merge — the worktree is in detached HEAD (no branch to merge). Commit to a branch, or use action:"remove"/"keep".',
          isError: true,
        };
      }
      // The branch the worktree was based on (recorded at creation).
      const targetBranch = existing?.originalBranch;
      if (!targetBranch) {
        return {
          output: `Error: cannot merge — the original branch is unknown (the base was detached or the session metadata is missing). Merge ${branch} manually, or use action:"remove"/"keep".`,
          isError: true,
        };
      }
      if (targetBranch === branch) {
        return {
          output: `Error: cannot merge — the worktree branch and the original branch are both "${branch}". Nothing to merge; use action:"remove" or action:"keep".`,
          isError: true,
        };
      }
      // The target branch must be checked out at the main repo root and its
      // working tree clean, or an in-place merge would be unsafe. Refuse rather
      // than touch the user's main checkout unexpectedly.
      let repoBranch = '';
      try { repoBranch = gitExec(['rev-parse', '--abbrev-ref', 'HEAD'], { cwd: repoRoot }); } catch { /* detached main */ }
      if (repoBranch !== targetBranch) {
        return {
          output: `Error: cannot merge — the main checkout at ${repoRoot} is on "${repoBranch || 'a detached HEAD'}", not the original branch "${targetBranch}". Switch it there first (git checkout ${targetBranch}), or use action:"remove" to discard the worktree.`,
          isError: true,
        };
      }
      // Only tracked modifications block a safe merge; untracked files (notably
      // the .ami/worktrees/ container itself) are expected and benign.
      const repoChanges = countWorktreeChanges(repoRoot, repoRoot, { ignoreUntracked: true });
      if (repoChanges === null || repoChanges.uncommitted > 0) {
        return {
          output: `Error: cannot merge — the main checkout at ${repoRoot} has uncommitted changes. Commit or stash them first, or use action:"remove" to discard the worktree.`,
          isError: true,
        };
      }
      try {
        _exitWorktreeHooks.exec(['merge', '--no-edit', branch], { cwd: repoRoot, stdio: 'pipe' });
      } catch (e) {
        // Abort any in-progress merge so the main checkout is left clean.
        try { _exitWorktreeHooks.exec(['merge', '--abort'], { cwd: repoRoot, stdio: 'ignore' }); } catch { /* no merge in progress */ }
        return {
          output: cap(`Error: merge of ${branch} into ${targetBranch} failed (likely a conflict) and was aborted — the worktree is preserved at ${worktreePath}. Resolve the conflict manually, or use action:"remove" to discard it.\n${e instanceof Error ? e.message : String(e)}`),
          isError: true,
        };
      }
      mergedInto = targetBranch;
    }

    // Tear down the worktree's tmux session (if any) BEFORE deleting the worktree
    // dir (parity: the reference's ExitWorktreeTool remove branch — `if (tmuxSessionName)
    // await killTmuxSession(...)` before cleanupWorktree). A destructive exit
    // (remove / merge / stash all delete the worktree) must never orphan the
    // background tmux session rooted at the now-deleted path. Best-effort: a
    // missing session / absent tmux is a no-op.
    if (existing?.tmuxSessionName) {
      const { killTmuxSession } = require('../worktree-manager');
      try { killTmuxSession(existing.tmuxSessionName); } catch { /* best-effort teardown */ }
    }

    // Run the destructive removal from the REPO ROOT, never from inside the
    // worktree we are about to delete (parity: the reference runs `git worktree remove`
    // with cwd = gitRoot — "Run from the main repo root, not the worktree (which
    // we're about to delete)"). Deleting the process/child cwd out from under git
    // is unreliable on some platforms and is the reason the rmSync+prune fallback
    // exists at all; running from repoRoot avoids that failure mode entirely.
    let cleanupFailed = false;
    try {
      _exitWorktreeHooks.exec(['worktree', 'remove', '--force', worktreePath], { cwd: repoRoot, stdio: 'pipe' });
    } catch (e) {
      try {
        if (_exitWorktreeHooks.existsSync(worktreePath)) {
          _exitWorktreeHooks.rmSync(worktreePath, { recursive: true, force: true });
        }
        _exitWorktreeHooks.exec(['worktree', 'prune'], { cwd: repoRoot, stdio: 'ignore' });
      } catch {
        // Soft-failure (parity: the reference's WorktreeExitDialog "Worktree cleanup
        // failed, exiting anyway"): an unrecoverable teardown error must not trap
        // the session inside the worktree it asked to leave. Record it, then still
        // restore the original cwd and clear the session below, and return a
        // non-error "exiting anyway" result that tells the user how to clean up.
        cleanupFailed = true;
        void e;
      }
    }

    // Delete the (now-unused) worktree branch as part of teardown. Wait a short
    // lock-release window first (parity with the reference): `git worktree remove` can
    // leave the worktree's locks briefly held, and `git branch -D` of the branch
    // that worktree had checked out would then fail with "cannot delete, locked".
    // Skip when the worktree could not be removed — its branch is still checked out.
    if (branch && !cleanupFailed) {
      if (_exitWorktreeHooks.branchDeleteDelayMs > 0) {
        await _exitWorktreeHooks.sleep(_exitWorktreeHooks.branchDeleteDelayMs);
      }
      try { _exitWorktreeHooks.exec(['branch', '-D', branch], { cwd: repoRoot, stdio: 'ignore' }); } catch { /* branch may be checked out elsewhere / gone */ }
    }

    const activeSession = getCurrentWorktreeSession();
    let returned = '';
    if (activeSession) {
      if (activeSession.originalCwd) {
        context.cwd = activeSession.originalCwd;
        // Restore the PROCESS working directory too (parity + symmetry with the
        // keep branch). After `git worktree remove` deletes the worktree dir,
        // leaving process.cwd() pointing at the now-deleted path breaks any
        // subsequent relative-path git/fs operation. The keep branch already
        // chdir'd back; the remove branch previously only reassigned context.cwd.
        // Skip when cleanup failed (the dir may still exist, and we never want to
        // throw here) OR when the global cwd was not inside this worktree (a
        // concurrent sibling agent — moving the shared process cwd would strand it).
        if (!cleanupFailed && globalCwdInWorktree) { try { process.chdir(activeSession.originalCwd); } catch { /* best effort */ } }
        returned = `\nReturned to ${activeSession.originalCwd}`;
        // Mirror enter_worktree + the keep branch: invalidate CWD-dependent caches
        // for the restored directory on exit (system-prompt env sections,
        // session-env cache) so state populated inside the worktree cannot leak
        // into the restored session.
        context._hookManager?.executeCwdChanged?.({ oldCwd: worktreePath, newCwd: activeSession.originalCwd })?.catch(() => {});
      }
      setWorktreeSession(null);
    }
    // Only signal removal when the worktree was actually removed.
    if (context._hookManager && !cleanupFailed) {
      context._hookManager.executeWorktreeRemove?.({ worktreePath }).catch(() => {});
    }

    const originalCwd = activeSession?.originalCwd ?? worktreePath;

    if (cleanupFailed) {
      // Graceful degraded exit (parity): the session leaves cleanly even though
      // the worktree dir could not be deleted; tell the user how to finish it.
      const failMsg = `Worktree cleanup failed, exiting anyway${branchLabel}: ${worktreePath} could not be removed — remove it manually with: git worktree remove --force ${worktreePath}${returned}`;
      return {
        output: cap(failMsg),
        metadata: { action, originalCwd, worktreePath, worktreeBranch: branch, discardedFiles, discardedCommits, message: failMsg, cleanupFailed: true },
      };
    }

    if (mergedInto) {
      // On a merge, the commits are NOT discarded — they were folded into the
      // original branch. Only forcibly-discarded uncommitted files are a loss.
      const mergedCommits = discardedCommits > 0 ? ` (${pluralize(discardedCommits, 'commit')} merged)` : '';
      const lost = discardedFiles > 0 ? ` (discarded ${pluralize(discardedFiles, 'uncommitted file')})` : '';
      const mergeMsg = `Worktree merged into ${mergedInto}${branchLabel}${mergedCommits}${lost} and removed.${returned}`;
      return {
        output: cap(mergeMsg),
        metadata: { action, originalCwd, worktreePath, worktreeBranch: branch, mergedInto, discardedFiles, discardedCommits, message: mergeMsg },
      };
    }
    if (wantStash) {
      // Non-destructive removal: uncommitted work was preserved in a recoverable
      // stash (or there was nothing to stash). Tell the user exactly how to get it
      // back from the main checkout.
      const stashNote = stashedFiles > 0
        ? ` (${pluralize(stashedFiles, 'uncommitted file')} saved to a git stash — recover from ${repoRoot} with: git stash apply)`
        : ' (no uncommitted changes to stash)';
      const stashMsg = `Worktree removed${branchLabel}${stashNote}.${returned}`;
      return {
        output: cap(stashMsg),
        metadata: { action, originalCwd, worktreePath, worktreeBranch: branch, discardedFiles: 0, discardedCommits: 0, stashedFiles, message: stashMsg },
      };
    }
    const discardSummary = (discardedFiles > 0 || discardedCommits > 0)
      ? ` (discarded ${pluralize(discardedCommits, 'commit')} and ${pluralize(discardedFiles, 'uncommitted file')})`
      : ' (no changes)';
    const removedMsg = `Worktree removed${branchLabel}${discardSummary}.${returned}`;
    return {
      output: cap(removedMsg),
      metadata: { action, originalCwd, worktreePath, worktreeBranch: branch, discardedFiles, discardedCommits, message: removedMsg },
    };
  },
};
