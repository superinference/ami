import * as fs from 'fs';
import * as path from 'path';
import { execFileSync } from 'child_process';
import { validateConfigJson, stripJsoncComments } from './config';

/**
 * True when the project config at `cwd` exists but fails JSON validation. A
 * corrupt config means its retention/cleanup settings (maxAgeDays etc.) cannot
 * be trusted, so destructive worktree cleanup must be skipped — mirroring
 * Claude's settings-error short-circuit before a reap (utils/cleanup.ts). When
 * no config exists, cleanup proceeds with the built-in defaults.
 */
function isProjectConfigInvalid(cwd: string): boolean {
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
const GIT_HARDENING_FLAGS = [
  '-c', 'core.hooksPath=/dev/null',
  '-c', 'core.fsmonitor=false',
  '-c', 'commit.gpgsign=false',
];

// Hard ceiling so a wedged git invocation can never hang an unattended run.
const GIT_CMD_TIMEOUT_MS = 15_000;

/**
 * Run git with the hardening flags, NO SHELL (execFile with an argv array), so
 * branch names / paths / refs can never be interpreted as shell metacharacters
 * (command-injection immune). Returns trimmed stdout.
 *
 * Exported so the worktree teardown path (exit_worktree) runs its git reads and
 * destructive calls through the SAME hardened runner (timeout + GIT_NO_PROMPT_ENV
 * + hooks/fsmonitor/gpg disabled) instead of a raw, unbounded child_process call
 * — one canonical hardened git invocation for the whole worktree subsystem.
 */
export function gitExec(args: string[], opts: { cwd: string; timeout?: number; stdio?: 'ignore' | 'pipe'; signal?: AbortSignal } = { cwd: process.cwd() }): string {
  // Honor cancellation for the SYNCHRONOUS runner: a blocked event loop means
  // execFileSync's own `signal` option can never fire mid-call, so the effective
  // cancellation point is BEFORE each git subprocess — refuse to launch another
  // one once the turn/engine was aborted (parity with the abort-threading pattern
  // git-commit.ts / runShellCommand / enter-worktree already follow; this module
  // previously honored no signal at all, so a worktree add/remove/prune ran to
  // completion even on a cancelled turn). `signal` is still forwarded to
  // execFileSync so a future async migration keeps the same contract.
  if (opts.signal?.aborted) {
    throw new Error('git operation aborted');
  }
  const env = { ...process.env, ...GIT_NO_PROMPT_ENV };
  const out = execFileSync('git', [...GIT_HARDENING_FLAGS, ...args], {
    cwd: opts.cwd,
    encoding: 'utf-8',
    env,
    timeout: opts.timeout ?? GIT_CMD_TIMEOUT_MS,
    stdio: opts.stdio === 'ignore' ? 'ignore' : ['ignore', 'pipe', 'pipe'],
    ...(opts.signal ? { signal: opts.signal } : {}),
  } as Parameters<typeof execFileSync>[2]);
  return typeof out === 'string' ? out.trim() : '';
}

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

/**
 * Memoization cache for {@link resolveGitDir}, keyed by the absolute start path
 * (parity: Claude's `resolveGitDirCache`, utils/git/gitFilesystem.ts). Every
 * downstream subprocess-free git reader (branch/HEAD) goes through the one
 * memoized resolver so a worktree/submodule `.git`-file is parsed at most once
 * per path.
 */
const resolveGitDirCache = new Map<string, string | null>();

function computeGitDir(startPath: string): string | null {
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
 * Resolve the REAL git directory for `startPath`, transparently handling the
 * worktree/submodule case where `.git` is a FILE containing `gitdir: <path>`
 * rather than a directory (parity: Claude's `resolveGitDir`,
 * utils/git/gitFilesystem.ts:L28-L76). Returns the directory a reader should
 * join `HEAD`/refs onto, or `null` when `startPath` is not a git repo / the
 * pointer is stale/unreadable. Memoized per absolute start path.
 *
 * This is the LENIENT resolver used by subprocess-free readers (e.g. the
 * journal's git-branch reader). {@link validateWorktreeGitFile} is the STRICT
 * validator that additionally enforces the commondir trust chain and reports a
 * granular error; this helper only answers "where is the git dir".
 */
export function resolveGitDir(startPath: string): string | null {
  const key = path.resolve(startPath);
  const cached = resolveGitDirCache.get(key);
  if (cached !== undefined) return cached;
  const result = computeGitDir(key);
  resolveGitDirCache.set(key, result);
  return result;
}

/**
 * Read the current branch name for the repo at `cwd` WITHOUT a git subprocess,
 * correctly in a linked worktree where `.git` is a `gitdir:` pointer file
 * (parity: Claude's filesystem git-HEAD reader). Resolves the real git dir via
 * {@link resolveGitDir} before reading `HEAD`, so an agent/task running inside a
 * `.ami/worktrees/` worktree records its actual branch instead of an empty
 * string. Returns `''` on a detached HEAD, a non-repo, or any read failure.
 */
export function readGitBranchFromDisk(cwd: string): string {
  const gitDir = resolveGitDir(cwd);
  if (!gitDir) return '';
  try {
    const head = fs.readFileSync(path.join(gitDir, 'HEAD'), 'utf-8').trim();
    const m = /^ref:\s*refs\/heads\/(.+)$/.exec(head);
    return m ? m[1] : '';
  } catch {
    return '';
  }
}

/**
 * Resolve the canonical MAIN repository root, even when invoked from inside an
 * existing worktree (so nested-worktree creation targets the real repo, not a
 * linked worktree). Falls back to cwd when git can't answer.
 */
export function resolveMainRepoRoot(cwd: string): string {
  try {
    // --git-common-dir points at the main repo's .git even from a linked worktree.
    const commonDir = gitExec(['rev-parse', '--path-format=absolute', '--git-common-dir'], { cwd });
    if (commonDir) {
      const norm = commonDir.normalize('NFC');
      // The main worktree root is the parent of the common .git dir.
      const root = path.basename(norm) === '.git' ? path.dirname(norm) : norm;
      if (fs.existsSync(root)) return root;
    }
  } catch { /* fall through */ }
  try {
    const top = gitExec(['rev-parse', '--show-toplevel'], { cwd });
    if (top) return top.normalize('NFC');
  } catch { /* fall through */ }
  return cwd;
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
  /**
   * Name of the detached tmux session bound to this worktree, when the worktree
   * was created with the tmux affordance enabled (parity: Claude's
   * WorktreeSession.tmuxSessionName, utils/worktree.ts:L149). Lets exit_worktree
   * surface the `tmux attach -t <name>` reattach command on keep and tear the
   * session down (killTmuxSession) on a destructive remove so a removed/kept
   * worktree never orphans its background terminal multiplexer session.
   */
  tmuxSessionName?: string;
  /**
   * True when git sparse-checkout was applied to this worktree via
   * `settings.worktree.sparsePaths` (parity: Claude's
   * WorktreeSession.usedSparsePaths, utils/worktree.ts:L152-153). Persisted so a
   * resume/introspection path knows the working tree is intentionally a partial
   * checkout rather than a broken/empty one.
   */
  usedSparsePaths?: boolean;
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

/**
 * Derive a deterministic tmux session name for a worktree from the repo root and
 * the worktree branch (parity: Claude's `generateTmuxSessionName`,
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
 * a worktree owns a reattachable background terminal session (parity: Claude's
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
 * Kill the tmux session `sessionName` (parity: Claude's `killTmuxSession`,
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
 * not a recognized PR reference (parity with Claude's `parsePRReference`,
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

export function createWorktreeSession(cwd: string, name: string, opts: { signal?: AbortSignal } = {}): WorktreeSession {
  const startMs = Date.now();
  const signal = opts.signal;
  // Resolve to the canonical main repo root so creation works even when invoked
  // from inside an existing (linked) worktree.
  const repoRoot = resolveMainRepoRoot(cwd);
  const worktreeDir = path.join(repoRoot, '.ami', 'worktrees', name);
  const branch = worktreeBranchName(name);
  if (!isSafeRefName(branch)) {
    throw new Error(`Refusing to create worktree: unsafe branch name "${branch}".`);
  }

  let originalBranch: string | undefined;
  let originalHead: string | undefined;
  try {
    originalBranch = gitExec(['rev-parse', '--abbrev-ref', 'HEAD'], { cwd: repoRoot, signal });
    originalHead = gitExec(['rev-parse', 'HEAD'], { cwd: repoRoot, signal });
  } catch { /* detached / fresh repo */ }

  // Create the worktree on disk if it doesn't already exist.
  // enter-worktree.ts creates the worktree itself (with base_ref support) then
  // calls createWorktreeSession for metadata; task.ts relies on this function
  // to create the worktree as well. The optional abort signal is threaded into
  // the long-running `worktree add` (and its teardown) so a cancelled turn kills
  // the in-flight git process instead of blocking up to GIT_CMD_TIMEOUT_MS.
  if (!fs.existsSync(worktreeDir)) {
    fs.mkdirSync(path.dirname(worktreeDir), { recursive: true });
    try {
      gitExec(['worktree', 'add', '-B', branch, worktreeDir, 'HEAD'], { cwd: repoRoot, stdio: 'ignore', signal });
    } catch (err) {
      // Tear down a half-created worktree so a failed add leaves no debris.
      try { gitExec(['worktree', 'remove', '--force', worktreeDir], { cwd: repoRoot, stdio: 'ignore' }); } catch { /* best effort */ }
      try { if (fs.existsSync(worktreeDir)) fs.rmSync(worktreeDir, { recursive: true, force: true }); } catch { /* best effort */ }
      throw err;
    }
  } else {
    // Fast-resume path: the worktree already exists and is being re-entered.
    // Bump its mtime so the stale-worktree sweep (cleanupStaleWorktrees keys
    // removal off stat.mtimeMs) can never reap a worktree that is actively
    // being reused — a long-lived worktree created past the age cutoff but
    // resumed here must count as fresh.
    bumpWorktreeMtime(worktreeDir);
  }

  const session: WorktreeSession = {
    originalCwd: repoRoot,
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
 * True when the working tree at `worktreePath` has no changes (parity: Claude's
 * central `getIsClean`, utils/git.ts:L356-L367). The ONE canonical cleanliness
 * helper so every callsite shares the same hardened read instead of re-spelling
 * `git status --porcelain`:
 *   - `--no-optional-locks` — a read-only status never takes git's optional index
 *     lock, so it cannot contend with a concurrent git operation in the tree.
 *   - `ignoreUntracked` appends `--untracked-files=no` (git's `-uno`) so a tree
 *     whose ONLY changes are untracked files reads as clean — the knob Claude's
 *     `getIsClean({ ignoreUntracked })` exposes, which AMI previously lacked.
 * Fail-closed: a git failure returns `false` (an unverifiable tree is treated as
 * dirty), matching the never-act-on-uncertainty contract used across this module.
 */
export function isClean(worktreePath: string, opts: { ignoreUntracked?: boolean } = {}): boolean {
  try {
    const args = ['--no-optional-locks', 'status', '--porcelain'];
    if (opts.ignoreUntracked) args.push('--untracked-files=no');
    return gitExec(args, { cwd: worktreePath }) === '';
  } catch {
    return false;
  }
}

/**
 * Count changes in a worktree (uncommitted files + unpushed commits).
 * Returns null on git failure (fail-closed — never removes on error).
 */
export function countWorktreeChanges(
  worktreePath: string,
  cwd: string,
  opts: { ignoreUntracked?: boolean; baseCommit?: string } = {},
): { uncommitted: number; unpushed: number } | null {
  try {
    // --no-optional-locks: a read-only status must not take the index lock
    // (avoids contending with a concurrent git in the same worktree).
    const statusArgs = ['--no-optional-locks', 'status', '--porcelain'];
    if (opts.ignoreUntracked) statusArgs.push('--untracked-files=no');
    const status = gitExec(statusArgs, { cwd: worktreePath });
    const uncommitted = status ? status.split('\n').length : 0;

    // Precise session-delta counting: when the caller knows the exact commit the
    // worktree was branched from (session.originalHeadCommit), count ONLY the
    // commits created in the session via `<base>..HEAD`. The remote-based count
    // below over-counts whenever the base branch itself has unpushed/local-only
    // commits — it attributes those pre-existing base commits to the worktree,
    // so exit_worktree would refuse `remove` on a worktree that contains no new
    // work. Claude parity: WorktreeExitDialog counts
    // `git rev-list --count <originalHeadCommit>..HEAD`. A malformed or
    // unreachable base is ignored (fall through to the remote-based count).
    if (opts.baseCommit && isValidGitSha(opts.baseCommit)) {
      try {
        const count = gitExec(['rev-list', '--count', `${opts.baseCommit}..HEAD`], { cwd: worktreePath });
        const n = parseInt(count, 10);
        if (Number.isFinite(n)) return { uncommitted, unpushed: n };
      } catch { /* base unreachable → fall back to the remote-based count */ }
    }

    // Fail-closed: if we cannot definitively count unpushed commits, return null
    // so the caller never treats an unverifiable worktree as safe to delete.
    let isDetached = false;
    try {
      gitExec(['symbolic-ref', '-q', 'HEAD'], { cwd: worktreePath });
    } catch {
      isDetached = true;
    }
    const notFlags = isDetached ? ['--not', '--remotes', '--branches'] : ['--not', '--remotes'];
    let unpushed: number;
    try {
      const revList = gitExec(['rev-list', 'HEAD', ...notFlags], { cwd: worktreePath });
      unpushed = revList ? revList.split('\n').length : 0;
    } catch {
      return null; // rev-list unverifiable → fail closed
    }
    return { uncommitted, unpushed };
  } catch {
    return null;
  }
}

/**
 * Enumerate the dirty files in a worktree, split into tracked modifications and
 * untracked files, so a destructive-op warning can show the user the CONCRETE
 * paths at risk before they confirm (parity with Claude's merge of
 * getFileStatus().tracked and .untracked, components/TeleportStash.tsx:L18-L19).
 *
 * Parses `git --no-optional-locks status --porcelain`: a `??` XY status code
 * marks an untracked path; every other code is a tracked change. A rename is
 * reported `R  old -> new` — the NEW path is surfaced (what now exists on disk).
 * Returns null on git failure (the same fail-closed contract as
 * {@link countWorktreeChanges}), so a caller never renders an unverifiable empty
 * list as "nothing at risk".
 */
export function getWorktreeFileStatus(
  worktreePath: string,
): { tracked: string[]; untracked: string[] } | null {
  let status: string;
  try {
    status = gitExec(['--no-optional-locks', 'status', '--porcelain'], { cwd: worktreePath });
  } catch {
    return null;
  }
  const tracked: string[] = [];
  const untracked: string[] = [];
  if (!status) return { tracked, untracked };
  for (const line of status.split('\n')) {
    if (!line) continue;
    const code = line.slice(0, 2);
    // The path follows the 2-char XY status code after its separator space(s);
    // strip the leading whitespace rather than assuming a fixed offset.
    const rest = line.slice(2).replace(/^\s+/, '').trim();
    if (!rest) continue;
    // A rename/copy entry is `old -> new`; surface the destination path.
    const arrow = rest.indexOf(' -> ');
    const file = arrow >= 0 ? rest.slice(arrow + 4).trim() : rest;
    if (code === '??') untracked.push(file);
    else tracked.push(file);
  }
  return { tracked, untracked };
}

/**
 * Stash a worktree's uncommitted changes (tracked modifications AND untracked
 * files, via `git stash push -u`) to a clean working state, preserving them
 * RECOVERABLY instead of discarding them. Mirrors Claude's `stashToCleanState`
 * (components/TeleportStash.tsx): the non-destructive alternative offered before a
 * git operation that would otherwise clobber a dirty tree.
 *
 * The stash is written to `refs/stash`, which lives in the repository's SHARED
 * (common) git dir — so a stash created inside a linked worktree survives the
 * worktree's removal and can be recovered from the main checkout with
 * `git stash list` / `git stash apply`. Verified behavior, relied on by the
 * exit_worktree `stash` action which stashes then removes the worktree.
 *
 * Returns true only when a stash was actually created AND the tree is now clean;
 * false when there was nothing to stash or the stash failed (fail-closed — the
 * caller must NOT proceed with a destructive teardown on a false result, so no
 * uncommitted work is ever silently lost). Runs through the hardened `gitExec`
 * (no shell, bounded timeout, prompts/hooks disabled).
 */
export function stashToCleanState(worktreePath: string, message: string): boolean {
  try {
    if (isClean(worktreePath)) return false; // nothing to stash
    gitExec(['stash', 'push', '-u', '-m', message], { cwd: worktreePath, stdio: 'pipe' });
    // Confirm the stash actually cleaned the tree (git stash can no-op in edge
    // cases); only report success when the working tree is genuinely clean.
    return isClean(worktreePath);
  } catch {
    return false;
  }
}

/**
 * Outcome of a stale-worktree sweep. `cleaned` lists the worktrees removed;
 * `failed` accounts for every removal that was ATTEMPTED (age/change gates
 * passed) but errored — the name plus the error message — instead of swallowing
 * it into a bare `catch {}`. Parity with Claude's structured CleanupResult
 * (utils/cleanup.ts) so a repeatedly-failing removal (permission/lock) is
 * observable rather than silently invisible.
 */
export interface WorktreeCleanupResult {
  cleaned: string[];
  failed: Array<{ name: string; error: string }>;
}

/**
 * One day in milliseconds — the default throttle interval for recurring
 * background cleanup (parity: Claude's `ONE_DAY_MS` marker-file throttle,
 * utils/cleanup.ts). Exported so a caller can reason about the same window.
 */
export const ONE_DAY_MS = 24 * 60 * 60 * 1000;

/** Marker file (inside the worktrees dir) recording the last throttled sweep. */
const CLEANUP_MARKER = '.last-cleanup';

/**
 * Options for the recurring stale-worktree sweep.
 *   - `throttle` gates the sweep to at most once per {@link ONE_DAY_MS} via a
 *     marker file, so a long-lived / repeatedly-shutdown session does not
 *     re-walk + re-prune every worktree on every invocation.
 *   - `retentionCount` keeps the N most-recently-modified worktrees regardless
 *     of age (parity: Claude's keep-newest-N retention knob,
 *     `NPM_CACHE_RETENTION_COUNT`), so the freshest worktrees are never reclaimed
 *     by a pure age cutoff even once they cross the threshold.
 */
export interface WorktreeCleanupOptions {
  throttle?: boolean;
  retentionCount?: number;
}

export function cleanupStaleWorktrees(
  cwd: string,
  maxAgeDays: number = 30,
  opts: WorktreeCleanupOptions = {},
): WorktreeCleanupResult {
  const worktreeDir = path.join(cwd, '.ami', 'worktrees');
  if (!fs.existsSync(worktreeDir)) return { cleaned: [], failed: [] };

  // Config-validity gate: never reap worktrees based on a config we cannot parse.
  // A corrupt .ami/config.json could mean the retention window is wrong, so we
  // fail safe and skip all destructive removal until the config is fixed.
  if (isProjectConfigInvalid(cwd)) return { cleaned: [], failed: [] };

  const markerPath = path.join(worktreeDir, CLEANUP_MARKER);
  // Once-per-interval throttle: when enabled, skip entirely if the previous
  // sweep's marker is newer than ONE_DAY_MS (recurring background cleanup should
  // not re-run on every shutdown of a long session).
  if (opts.throttle) {
    const last = mtimeOrUndefined(markerPath);
    if (last !== undefined && (Date.now() - last) < ONE_DAY_MS) {
      return { cleaned: [], failed: [] };
    }
  }

  const cleaned: string[] = [];
  const failed: Array<{ name: string; error: string }> = [];
  const now = Date.now();
  const maxAgeMs = maxAgeDays * 24 * 60 * 60 * 1000;
  // Never reap the worktree the active session is currently using.
  const activePath = currentSession?.worktreePath ? path.resolve(currentSession.worktreePath) : null;

  // Keep-newest-N retention: the `retentionCount` most-recently-modified worktree
  // directories are protected from removal regardless of age.
  let protectedNames: Set<string> | null = null;
  if (typeof opts.retentionCount === 'number' && opts.retentionCount > 0) {
    const dirs: Array<{ name: string; mtimeMs: number }> = [];
    for (const entry of fs.readdirSync(worktreeDir)) {
      try {
        const st = fs.statSync(path.join(worktreeDir, entry));
        if (st.isDirectory()) dirs.push({ name: entry, mtimeMs: st.mtimeMs });
      } catch { /* unreadable — not a retention candidate */ }
    }
    dirs.sort((a, b) => b.mtimeMs - a.mtimeMs);
    protectedNames = new Set(dirs.slice(0, opts.retentionCount).map(d => d.name));
  }

  for (const entry of fs.readdirSync(worktreeDir)) {
    const fullPath = path.join(worktreeDir, entry);
    try {
      if (activePath && path.resolve(fullPath) === activePath) continue;
      if (protectedNames?.has(entry)) continue; // keep newest-N regardless of age
      const stat = fs.statSync(fullPath);
      if (!stat.isDirectory()) continue;

      const isEphemeral = EPHEMERAL_WORKTREE_PATTERNS.some(p => p.test(entry));
      const ageThreshold = isEphemeral ? maxAgeMs / 2 : maxAgeMs;

      if ((now - stat.mtimeMs) > ageThreshold) {
        const changes = countWorktreeChanges(fullPath, cwd);
        if (changes === null) continue; // fail-closed: never remove on uncertainty
        if (changes.uncommitted > 0 || changes.unpushed > 0) continue;

        try {
          gitExec(['worktree', 'remove', '--force', fullPath], { cwd, timeout: 10000 });
          cleaned.push(entry);
        } catch (err) {
          // Account for the failure (so a persistently-failing removal is
          // observable) and leave the worktree for the next sweep.
          failed.push({ name: entry, error: err instanceof Error ? err.message : String(err) });
        }
      }
    } catch { /* unreadable entry — skip */ }
  }

  // Prune any stale worktree metadata
  try {
    gitExec(['worktree', 'prune'], { cwd, timeout: 5000, stdio: 'ignore' });
  } catch { /* best effort */ }

  // Record this sweep so the once-per-interval throttle can skip the next call.
  if (opts.throttle) {
    try { fs.writeFileSync(markerPath, String(now)); } catch { /* best effort */ }
  }

  return { cleaned, failed };
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

/** Default staleness window for a clone's last fetch — 7 days, matching Claude's
 *  STALE_FETCH_WARN_MS (utils/deepLink/banner.ts:L22). */
export const STALE_FETCH_WARN_MS = 7 * 24 * 60 * 60 * 1000;

/** mtime (ms) of a path, or undefined when it cannot be stat'd. */
function mtimeOrUndefined(p: string): number | undefined {
  try {
    return fs.statSync(p).mtimeMs;
  } catch {
    return undefined;
  }
}

/**
 * Last-fetch timestamp (ms) for the repo at `cwd`, read from the mtime of
 * `FETCH_HEAD`. Because `FETCH_HEAD` is PER-WORKTREE, this reads it from BOTH the
 * worktree's own git dir and the resolved common (main repo) git dir and returns
 * whichever is NEWER — so a fetch run in the main checkout is still seen from a
 * linked worktree and vice-versa (parity with Claude's readLastFetchTime
 * worktree/common-dir newer-of-two fallback, utils/deepLink/banner.ts:L94-L101).
 * Returns undefined when the repo has NEVER fetched (no `FETCH_HEAD` in either
 * location) or `cwd` is not a git repo.
 */
export function readLastFetchTime(cwd: string): number | undefined {
  const dirs: string[] = [];
  try {
    const gitDir = gitExec(['rev-parse', '--path-format=absolute', '--git-dir'], { cwd });
    if (gitDir) dirs.push(gitDir);
  } catch { /* not a git repo */ }
  try {
    const commonDir = gitExec(['rev-parse', '--path-format=absolute', '--git-common-dir'], { cwd });
    if (commonDir) dirs.push(commonDir);
  } catch { /* not a git repo */ }

  let newest: number | undefined;
  for (const d of dirs) {
    const t = mtimeOrUndefined(path.join(d, 'FETCH_HEAD'));
    if (t !== undefined && (newest === undefined || t > newest)) newest = t;
  }
  return newest;
}

/**
 * True when the clone at `cwd` is STALE — it has not fetched within `maxAgeMs`
 * (default {@link STALE_FETCH_WARN_MS}), OR it has never fetched at all (a missing
 * `FETCH_HEAD` is treated as stale, mirroring buildDeepLinkBanner's
 * missing-timestamp branch). Lets the worktree path warn that a worktree based on
 * `origin/<default>` may be built on an out-of-date ref.
 */
export function isCloneStale(cwd: string, maxAgeMs: number = STALE_FETCH_WARN_MS): boolean {
  const last = readLastFetchTime(cwd);
  if (last === undefined) return true;
  return (Date.now() - last) > maxAgeMs;
}

/**
 * Enumerate all git worktrees (bounded, NFC-normalized, current-first then
 * alphabetical). Returns [] on any failure so callers never block on git.
 */
export function listWorktrees(cwd: string): Array<{ path: string; branch?: string; isCurrent: boolean }> {
  let raw: string;
  try {
    raw = gitExec(['worktree', 'list', '--porcelain'], { cwd, timeout: 5000 });
  } catch {
    return [];
  }
  const current = path.resolve(cwd);
  const out: Array<{ path: string; branch?: string; isCurrent: boolean }> = [];
  let cur: { path: string; branch?: string } | null = null;
  for (const line of raw.split('\n')) {
    if (line.startsWith('worktree ')) {
      if (cur) out.push({ ...cur, isCurrent: false });
      cur = { path: line.slice('worktree '.length).trim().normalize('NFC') };
    } else if (line.startsWith('branch ') && cur) {
      cur.branch = line.slice('branch '.length).trim();
    }
  }
  if (cur) out.push({ ...cur, isCurrent: false });
  for (const w of out) {
    w.isCurrent = path.resolve(w.path) === current || current.startsWith(path.resolve(w.path) + path.sep);
  }
  out.sort((a, b) => {
    if (a.isCurrent !== b.isCurrent) return a.isCurrent ? -1 : 1;
    return a.path.localeCompare(b.path);
  });
  return out;
}

/** True when `child` is contained within `parent` (no path escape). */
function isWithin(parent: string, child: string): boolean {
  const rel = path.relative(path.resolve(parent), path.resolve(child));
  return rel === '' || (!rel.startsWith('..') && !path.isAbsolute(rel));
}

/** Extract a Node errno string (e.g. 'EACCES') from a thrown fs error, or ''. */
function errnoCodeOf(err: unknown): string {
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
 * failure (parity with Claude's symlinkDirectories, utils/worktree.ts:L109-L136).
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

/**
 * Copy files listed in `.worktreeinclude` from source to worktree.
 * Each line in the file is a relative path (a file or a directory). Lines
 * starting with `#` are comments; empty lines are skipped.
 *
 * Parity with Claude's copyWorktreeIncludeFiles (utils/worktree.ts): a file is
 * only carried over when it is BOTH named in `.worktreeinclude` AND GITIGNORED
 * (untracked-and-ignored). `.worktreeinclude` is for local, gitignored artifacts
 * (`.env`, caches, build outputs) — a git-TRACKED file is already present in the
 * worktree checkout, and a plain untracked file that is NOT ignored is almost
 * always a stray source file the user would not expect to be duplicated. The
 * eligible set is computed once from
 * `git ls-files --others --ignored --exclude-standard`. A directory entry copies
 * exactly the gitignored files beneath it (so a collapsed `node_modules/` or
 * `config/secrets/` carries its ignored contents, not tracked siblings).
 *
 * When `sourceCwd` is not a git repo (the lister fails), there is no gitignore
 * information, so we fall back to a best-effort literal copy of each entry.
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

  // The gitignored (untracked + ignored) file set, or null when `sourceCwd` is
  // not a git repo / git is unavailable.
  const gitignored = listGitignoredFiles(sourceCwd);

  const copyOne = (rel: string): void => {
    const sourcePath = path.join(sourceCwd, rel);
    const targetPath = path.join(worktreePath, rel);
    if (!isWithin(sourceCwd, sourcePath) || !isWithin(worktreePath, targetPath)) return;
    try {
      if (!fs.existsSync(sourcePath)) return;
      fs.mkdirSync(path.dirname(targetPath), { recursive: true });
      fs.copyFileSync(sourcePath, targetPath);
      copied.push(rel);
    } catch { /* best effort per file */ }
  };

  for (const entry of lines) {
    const normEntry = entry.replace(/^\.\//, '').replace(/\/$/, '').normalize('NFC');
    const sourcePath = path.join(sourceCwd, normEntry);
    const targetPath = path.join(worktreePath, normEntry);
    // Path-escape guard: an entry must stay inside BOTH the source repo and the
    // worktree — never copy an arbitrary absolute/`..` path.
    if (!isWithin(sourceCwd, sourcePath) || !isWithin(worktreePath, targetPath)) continue;

    if (gitignored) {
      // Gitignore-gated copy. For a directory entry, expand to the gitignored
      // files beneath it; for a file entry, require it to be gitignored.
      let stat: fs.Stats | undefined;
      try { stat = fs.statSync(sourcePath); } catch { stat = undefined; }
      if (stat?.isDirectory()) {
        const prefix = normEntry + '/';
        for (const rel of gitignored) {
          if (rel === normEntry || rel.startsWith(prefix)) copyOne(rel);
        }
      } else if (gitignored.has(normEntry)) {
        copyOne(normEntry);
      }
      continue;
    }

    // Non-git fallback: best-effort literal copy (file or directory tree).
    try {
      if (!fs.existsSync(sourcePath)) continue;
      const stat = fs.statSync(sourcePath);
      fs.mkdirSync(path.dirname(targetPath), { recursive: true });
      if (stat.isDirectory()) {
        copyDirRecursive(sourcePath, targetPath);
      } else {
        fs.copyFileSync(sourcePath, targetPath);
      }
      copied.push(normEntry);
    } catch { /* best effort per entry */ }
  }

  return copied;
}

/**
 * List the repo's gitignored (untracked AND ignored) files, relative to `cwd`,
 * NFC-normalized. Returns null when `cwd` is not a git repo / git is
 * unavailable, so the caller can fall back to a non-gitignore code path.
 */
function listGitignoredFiles(cwd: string): Set<string> | null {
  try {
    const out = gitExec(
      ['ls-files', '--others', '--ignored', '--exclude-standard'],
      { cwd, stdio: 'pipe' },
    );
    return new Set(
      out
        ? out.split('\n').map(l => l.trim()).filter(Boolean).map(l => l.normalize('NFC'))
        : [],
    );
  } catch {
    return null;
  }
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
 * `.git` file → `gitdir:` → `commondir` structure and Claude's
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
function validateWorktreeCommondirTrust(
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
