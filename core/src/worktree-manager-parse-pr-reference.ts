// Split from worktree-manager.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as path from 'path';
import { execFileSync } from 'child_process';
import { validateConfigJson, stripJsoncComments } from './config';

/**
 * True when the project config at `cwd` exists but fails JSON validation. A
 * corrupt config means its retention/cleanup settings (maxAgeDays etc.) cannot
 * be trusted, so destructive worktree cleanup must be skipped — mirroring
 * the reference's settings-error short-circuit before a reap (utils/cleanup.ts). When
 * no config exists, cleanup proceeds with the built-in defaults.
 */
export function isProjectConfigInvalid(cwd: string): boolean {
  const configPath = path.join(cwd, '.ami', 'config.json');
  let raw: string;
  try {
    raw = fs.readFileSync(configPath, 'utf-8');
  } catch {
    return false; // absent config → use defaults, not an error
  }
  return !validateConfigJson(stripJsoncComments(raw)).valid;
}

export const GIT_NO_PROMPT_ENV = {
  GIT_TERMINAL_PROMPT: '0',
  GIT_ASKPASS: '',
  GIT_NO_REPLACE_OBJECTS: '1',
};

// Config flags for unattended git: never fire user hooks (a post-checkout or
// pre-commit hook can block forever on prompts), never start the fsmonitor
// daemon (hangs on `status`), never attempt GPG signing (blocks on a passphrase
// prompt). Passed as argv pairs (`git -c k=v <cmd>`), never a shell string.
export const GIT_HARDENING_FLAGS = [
  '-c', 'core.hooksPath=/dev/null',
  '-c', 'core.fsmonitor=false',
  '-c', 'commit.gpgsign=false',
];

// Hard ceiling so a wedged git invocation can never hang an unattended run.
// Default for WRITE/other commands (worktree add/remove/prune, commit, …).
export const GIT_CMD_TIMEOUT_MS = 15_000;

/**
 * Ceiling for a READ command (status/rev-list/log/…). A flat 15s cap is too tight
 * for a status/diff over a very large or cold working tree, yet a read must still
 * be bounded so a wedged reader can never hang an unattended run. 120s mirrors
 * Orca's read-class ceiling (git-command-classification.ts) — generous enough for
 * a legitimate large read, still bounded.
 */
export const GIT_READ_CMD_TIMEOUT_MS = 120_000;

export const GIT_COMMAND_CLASS = ['network', 'read', 'other'] as const;

export type GitCommandClass = (typeof GIT_COMMAND_CLASS)[number];

/** Effective timeout (ms) for a git class: network uncapped (0 = no timeout). */
export function gitTimeoutForClass(cls: GitCommandClass): number {
  if (cls === 'network') return 0; // uncapped — execFileSync treats 0 as no timeout
  if (cls === 'read') return GIT_READ_CMD_TIMEOUT_MS;
  return GIT_CMD_TIMEOUT_MS;
}

// Only "another git process holds the lock" is transient and worth retrying; a
// CAS ref mismatch (`cannot lock ref …: is at X but expected Y`) is NOT transient.
export const GIT_LOCK_CONTENTION_PATTERNS = [
  /Unable to create '[^'\n]*\.lock': File exists/i,
  /could not lock config file [^\n]*: File exists/i,
  /cannot lock ref '[^'\n]*': Unable to create '[^'\n]*\.lock': File exists/i,
];

export const GIT_LOCK_CONTENTION_RETRY_DELAYS_MS: readonly number[] = [50, 150, 400];

/** Read the combined stderr/stdout/message text from a thrown execFileSync error. */
export function gitFailureText(error: unknown): string {
  if (!error || typeof error !== 'object') return String(error ?? '');
  const e = error as { stderr?: unknown; stdout?: unknown; message?: unknown };
  const parts = [e.stderr, e.stdout, e.message]
    .map(p => (typeof p === 'string' ? p : Buffer.isBuffer(p) ? p.toString('utf-8') : ''))
    .filter(Boolean);
  return parts.join('\n');
}

/**
 * True when `error` is a TRANSIENT git lock-contention failure (another git
 * process briefly held a `*.lock`), the only failure class safe to retry
 * (parity: Orca git-lock-contention.ts). A ref CAS mismatch or any other failure
 * returns false so a genuine error is never masked by a retry.
 */
export function isGitLockContentionFailure(error: unknown): boolean {
  const text = gitFailureText(error);
  return GIT_LOCK_CONTENTION_PATTERNS.some(p => p.test(text));
}

/** Synchronous sleep (ms) — the runner is sync, so backoff must block in-thread. */
export function sleepSync(ms: number): void {
  if (ms <= 0) return;
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

/**
 * Re-run `attempt` after each TRANSIENT git lock-contention failure, sleeping the
 * next backoff delay between tries; ANY other failure (or exhausting the delays)
 * is thrown so a genuine error is never masked (parity: Orca retryOnGitLockContention,
 * adapted to the synchronous runner). A `shouldAbort` hook lets a caller bail out
 * of the backoff on cancellation.
 */
export function retryOnGitLockContention<T>(
  attempt: () => T,
  delaysMs: readonly number[] = GIT_LOCK_CONTENTION_RETRY_DELAYS_MS,
  shouldAbort?: () => boolean,
): T {
  for (let retry = 0; ; retry += 1) {
    try {
      return attempt();
    } catch (err) {
      const delay = delaysMs[retry];
      if (delay === undefined || !isGitLockContentionFailure(err)) throw err;
      if (shouldAbort?.()) throw new Error('git operation aborted');
      sleepSync(delay);
    }
  }
}

export const FETCH_HEAD_LOCK_POLL_MS = 100;

/** 40-hex (SHA-1) or 64-hex (SHA-256) git object id. */
export function isValidGitSha(sha: string): boolean {
  return /^[0-9a-f]{40}$/.test(sha) || /^[0-9a-f]{64}$/.test(sha);
}

/** Allow-list validator for a ref/branch name used in git argv. Rejects the
 *  characters git itself forbids plus anything that could confuse tooling. */
export function isSafeRefName(ref: string): boolean {
  if (!ref || ref.length > 255) return false;
  if (/[\x00-\x20~^:?*[\\\s]/.test(ref)) return false; // control, space, glob, git-forbidden
  if (ref.includes('..') || ref.includes('@{')) return false;
  if (ref.startsWith('/') || ref.endsWith('/') || ref.startsWith('-')) return false;
  if (ref.endsWith('.lock') || ref.endsWith('.')) return false;
  return true;
}

export function computeGitDir(startPath: string): string | null {
  const gitPath = path.join(startPath, '.git');
  let stat: fs.Stats;
  try {
    stat = fs.statSync(gitPath);
  } catch {
    return null; // no `.git` here (not a repo / unreadable)
  }
  // Regular repository: `.git` is a directory and IS the git dir.
  if (stat.isDirectory()) return gitPath;
  // A linked worktree / submodule stores `.git` as a FILE: `gitdir: <path>\n`.
  if (!stat.isFile()) return null;
  let content: string;
  try {
    content = fs.readFileSync(gitPath, 'utf-8');
  } catch {
    return null;
  }
  // `.trim()` strips the trailing newline/CR (git writes `gitdir: <path>\n`,
  // mirroring git's own setup.c which trims the pointer).
  const trimmed = content.trim();
  const prefix = 'gitdir:';
  if (!trimmed.startsWith(prefix)) return null;
  const rel = trimmed.slice(prefix.length).trim();
  if (!rel) return null;
  // The pointer is resolved relative to the worktree root (absolute pointers
  // are used verbatim), then confirmed to exist so a stale pointer reads as
  // "no git dir" rather than a path that cannot be read.
  const resolved = path.isAbsolute(rel) ? rel : path.resolve(startPath, rel);
  return fs.existsSync(resolved) ? resolved : null;
}

/**
 * Structured state of the current git HEAD read directly off disk (no subprocess):
 * on a branch (`.git/HEAD` is `ref: refs/heads/<name>`), detached (`.git/HEAD`
 * holds a bare commit SHA — during a rebase/bisect, `checkout --detach`, or a
 * CI checkout of a tag/commit), or none (not a repo / unreadable / tampered).
 *
 * Parity: the reference's `readGitHead` distinguishes branch vs detached and returns the
 * raw SHA for a detached HEAD (`{type:'detached', sha}`), so git context is not
 * lost for detached sessions; AMI's {@link readGitBranchFromDisk} alone collapses
 * every non-branch HEAD to '' (gitFilesystem.ts L149-183).
 */
export type GitHeadInfo =
  | { type: 'branch'; branch: string }
  | { type: 'detached'; sha: string }
  | { type: 'none' };

export const MAX_WORKTREES = 10;

export const EPHEMERAL_WORKTREE_PATTERNS = [
  /^agent-[0-9a-f]{7,}$/,
  /^wf_[0-9a-f]{8}-[0-9a-f]{3}-\d+$/,
  /^wt-[0-9a-f]{8}$/,
];

export interface WorktreeSession {
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
  /**
   * Name of the detached tmux session bound to this worktree, when the worktree
   * was created with the tmux affordance enabled (parity: the reference's
   * WorktreeSession.tmuxSessionName, utils/worktree.ts:L149). Lets exit_worktree
   * surface the `tmux attach -t <name>` reattach command on keep and tear the
   * session down (killTmuxSession) on a destructive remove so a removed/kept
   * worktree never orphans its background terminal multiplexer session.
   */
  tmuxSessionName?: string;
  /**
   * True when git sparse-checkout was applied to this worktree via
   * `settings.worktree.sparsePaths` (parity: the reference's
   * WorktreeSession.usedSparsePaths, utils/worktree.ts:L152-153). Persisted so a
   * resume/introspection path knows the working tree is intentionally a partial
   * checkout rather than a broken/empty one.
   */
  usedSparsePaths?: boolean;
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

/**
 * Derive a deterministic tmux session name for a worktree from the repo root and
 * the worktree branch (parity: the reference's `generateTmuxSessionName`,
 * utils/worktree.ts:L171-179). The repo path is basename'd (only the final dir
 * component is used, never the full path) and every `/` or `.` in `<repo>_<branch>`
 * is replaced with `_`, because tmux rejects `.` and `:` in session names and a
 * `/` would be ambiguous. Pure/synchronous — no subprocess — so it is safe to call
 * before a session is created.
 */
export function generateTmuxSessionName(repoPath: string, branch: string): string {
  const repoName = path.basename(repoPath);
  return `${repoName}_${branch}`.replace(/[/.]/g, '_');
}

/**
 * Dependency seam for the tmux subprocess calls (mirrors `_exitWorktreeHooks`):
 * tests inject a stub `run` so the tmux lifecycle can be exercised WITHOUT tmux
 * installed. The default runs the real `tmux` binary with NO SHELL (execFile with
 * an argv array) so a session name / worktree path can never be a shell
 * metacharacter. Returns `{ ok }` plus captured stderr on failure.
 */
export const _tmuxHooks = {
  run(args: string[]): { ok: boolean; error?: string } {
    try {
      execFileSync('tmux', args, {
        encoding: 'utf-8',
        stdio: ['ignore', 'ignore', 'pipe'],
        timeout: GIT_CMD_TIMEOUT_MS,
      });
      return { ok: true };
    } catch (err: unknown) {
      const stderr = (err && typeof err === 'object' && 'stderr' in err && typeof (err as { stderr?: unknown }).stderr === 'string')
        ? (err as { stderr: string }).stderr.trim()
        : '';
      return { ok: false, error: stderr || (err instanceof Error ? err.message : String(err)) };
    }
  },
};

/**
 * Spawn a DETACHED tmux session named `sessionName`, rooted at `worktreePath`, so
 * a worktree owns a reattachable background terminal session (parity: the reference's
 * `createTmuxSessionForWorktree`, utils/worktree.ts:L673-690). Returns
 * `{ created: true }` on success, or `{ created: false, error }` when tmux is not
 * installed or the session could not be started — the caller surfaces the failure
 * as a non-fatal warning rather than aborting worktree creation.
 */
export function createTmuxSessionForWorktree(sessionName: string, worktreePath: string): { created: boolean; error?: string } {
  const res = _tmuxHooks.run(['new-session', '-d', '-s', sessionName, '-c', worktreePath]);
  return res.ok ? { created: true } : { created: false, error: res.error };
}

/**
 * Kill the tmux session `sessionName` (parity: the reference's `killTmuxSession`,
 * utils/worktree.ts:L693-700). Returns true when the session was killed. Used on a
 * destructive worktree exit so removing a worktree never orphans its background
 * tmux session. Best-effort: a missing session / absent tmux returns false
 * without throwing.
 */
export function killTmuxSession(sessionName: string): boolean {
  return _tmuxHooks.run(['kill-session', '-t', sessionName]).ok;
}

/**
 * Parse a pull-request reference into its PR number, or `null` when the string is
 * not a recognized PR reference (parity with the reference's `parsePRReference`,
 * utils/worktree.ts). Accepts two shapes:
 *   - a GitHub-style PR URL `https://<host>/owner/repo/pull/<N>` (host-agnostic,
 *     with an optional trailing slash / query / hash) — the `/pull/N` path shape is
 *     specific to GitHub & GHE (GitLab uses `/-/merge_requests/N`, Bitbucket
 *     `/pull-requests/N`), so matching any host here is safe; and
 *   - the shorthand `#N`.
 * Lets a worktree be based on a pull request: the caller fetches
 * `origin pull/<N>/head` and bases the new worktree on FETCH_HEAD, naming it
 * `pr-<N>` when no explicit name is given.
 */
export function parsePRReference(input: string): number | null {
  // `#N` shorthand.
  const hashMatch = /^#(\d+)$/.exec(input);
  if (hashMatch) return parseInt(hashMatch[1], 10);
  // PR URL `https://<host>/owner/repo/pull/<N>` (host-agnostic, optional trailing
  // slash / query / hash). Parsed by splitting on `/` — each segment is bounded, so
  // there is no backtracking (ReDoS-free), unlike a single catch-all regex.
  const scheme = /^https?:\/\//i.exec(input);
  if (!scheme) return null;
  const pathPart = input.slice(scheme[0].length).split(/[?#]/, 1)[0];
  const segs = pathPart.split('/');
  // Exactly host / owner / repo / "pull" / N, allowing a single trailing slash.
  const len = segs.length === 6 && segs[5] === '' ? 5 : segs.length;
  if (len !== 5) return null;
  if (!segs[0] || !segs[1] || !segs[2] || segs[3] !== 'pull') return null;
  if (!/^\d+$/.test(segs[4])) return null;
  return parseInt(segs[4], 10);
}

/**
 * Prefix of the default stash message used when {@link stashToCleanState} is
 * called with no explicit `message`. Parity with the reference's timestamped auto-stash
 * default (`the reference auto-stash - <ISO>`), re-branded to AMI; exported so a
 * caller / test can recognise an auto-generated stash entry by prefix.
 */
export const AUTO_STASH_MESSAGE_PREFIX = 'AMI auto-stash';

/**
 * Outcome of a stale-worktree sweep. `cleaned` lists the worktrees removed;
 * `failed` accounts for every removal that was ATTEMPTED (age/change gates
 * passed) but errored — the name plus the error message — instead of swallowing
 * it into a bare `catch {}`. Parity with the reference's structured CleanupResult
 * (utils/cleanup.ts) so a repeatedly-failing removal (permission/lock) is
 * observable rather than silently invisible.
 */
export interface WorktreeCleanupResult {
  cleaned: string[];
  failed: Array<{ name: string; error: string }>;
}

/** Marker file (inside the worktrees dir) recording the last throttled sweep. */
export const CLEANUP_MARKER = '.last-cleanup';

/**
 * Options for the recurring stale-worktree sweep.
 *   - `throttle` gates the sweep to at most once per {@link ONE_DAY_MS} via a
 *     marker file, so a long-lived / repeatedly-shutdown session does not
 *     re-walk + re-prune every worktree on every invocation.
 *   - `retentionCount` keeps the N most-recently-modified worktrees regardless
 *     of age (parity: the reference's keep-newest-N retention knob,
 *     `NPM_CACHE_RETENTION_COUNT`), so the freshest worktrees are never reclaimed
 *     by a pure age cutoff even once they cross the threshold.
 */
export interface WorktreeCleanupOptions {
  throttle?: boolean;
  retentionCount?: number;
}

/**
 * Bump a worktree directory's mtime so an actively-reused worktree is not reaped
 * by the stale-cleanup sweep on resume.
 */
export function bumpWorktreeMtime(worktreePath: string): void {
  try {
    const now = new Date();
    fs.utimesSync(worktreePath, now, now);
  } catch { /* best effort */ }
}

/** mtime (ms) of a path, or undefined when it cannot be stat'd. */
export function mtimeOrUndefined(p: string): number | undefined {
  try {
    return fs.statSync(p).mtimeMs;
  } catch {
    return undefined;
  }
}

/** True when `child` is contained within `parent` (no path escape). */
export function isWithin(parent: string, child: string): boolean {
  const rel = path.relative(path.resolve(parent), path.resolve(child));
  return rel === '' || (!rel.startsWith('..') && !path.isAbsolute(rel));
}
