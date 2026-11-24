import * as fs from 'fs';
import * as path from 'path';
import { execFileSync } from 'child_process';
import './config';
import { AUTO_STASH_MESSAGE_PREFIX, CLEANUP_MARKER, EPHEMERAL_WORKTREE_PATTERNS, FETCH_HEAD_LOCK_POLL_MS, GIT_HARDENING_FLAGS, GIT_LOCK_CONTENTION_RETRY_DELAYS_MS, GIT_NO_PROMPT_ENV, bumpWorktreeMtime, computeGitDir, gitTimeoutForClass, isProjectConfigInvalid, isSafeRefName, isValidGitSha, isWithin, killTmuxSession, mtimeOrUndefined, retryOnGitLockContention, sleepSync, worktreeBranchName } from './worktree-manager-parse-pr-reference';
import type { GitCommandClass, GitHeadInfo, WorktreeCleanupOptions, WorktreeCleanupResult, WorktreeSession } from './worktree-manager-parse-pr-reference';
import { copyDirRecursive, errnoCodeOf } from './worktree-manager-validate-worktree-git-file';
export { AUTO_STASH_MESSAGE_PREFIX, GIT_COMMAND_CLASS, GIT_LOCK_CONTENTION_RETRY_DELAYS_MS, GIT_NO_PROMPT_ENV, GIT_READ_CMD_TIMEOUT_MS, MAX_WORKTREES, _tmuxHooks, bumpWorktreeMtime, createTmuxSessionForWorktree, flattenSlug, generateTmuxSessionName, gitTimeoutForClass, isGitLockContentionFailure, isSafeRefName, isValidGitSha, killTmuxSession, parsePRReference, retryOnGitLockContention, validateWorktreeSlug, worktreeBranchName } from './worktree-manager-parse-pr-reference';
export type { GitCommandClass, GitHeadInfo, WorktreeCleanupOptions, WorktreeCleanupResult } from './worktree-manager-parse-pr-reference';
export { findStaleWorktrees, symlinkLargeDirectories, validateWorktreeGitFile } from './worktree-manager-validate-worktree-git-file';

// Global options that take a VALUE and so precede the subcommand (their value must
// be skipped when scanning for the subcommand): `git -C <dir> status`, `git -c k=v …`.
const GIT_GLOBAL_OPTIONS_WITH_VALUE: ReadonlySet<string> = new Set([
  '-c', '-C', '--git-dir', '--work-tree', '--namespace', '--super-prefix',
  '--config-env', '--exec-path',
]);

// Subcommands whose dominant phase is a remote transfer — these MUST NOT be killed
// by the read/other timeout (a real clone/fetch/push over a slow link legitimately
// runs for minutes). Classified 'network' → run UNCAPPED.
const GIT_NETWORK_SUBCOMMANDS = new Set(['fetch', 'pull', 'push', 'clone', 'ls-remote']);

// Subcommands that are always a read (no working-tree mutation, no network).
const GIT_ALWAYS_READ_SUBCOMMANDS = new Set([
  'blame', 'cat-file', 'check-attr', 'check-ref-format', 'check-ignore',
  'describe', 'diff', 'diff-tree', 'for-each-ref', 'log', 'ls-files', 'ls-tree',
  'merge-base', 'name-rev', 'rev-list', 'rev-parse', 'show', 'show-ref', 'status', 'var',
]);

// `branch`/`config` are reads only with a listing/query flag anywhere in the args.
const GIT_READ_FLAG_SUBCOMMANDS: Record<string, ReadonlySet<string>> = {
  branch: new Set(['--list', '-l', '--show-current', '--contains', '--points-at', '--all', '-a', '--remotes', '-r']),
  config: new Set(['--get', '--get-all', '--get-regexp', '--get-urlmatch', '--list', '-l']),
};

// `remote`/`submodule`/`worktree` are reads only in their listing action form.
const GIT_READ_ACTION_SUBCOMMANDS: Record<string, ReadonlySet<string>> = {
  remote: new Set(['get-url', 'show']),
  submodule: new Set(['status']),
  worktree: new Set(['list']),
};
const GIT_BARE_FORM_IS_READ = new Set(['remote', 'submodule']);

/** Index of the git subcommand token, skipping leading global options + their values. */
export function findGitSubcommandIndex(args: readonly string[]): number {
  for (let i = 0; i < args.length; i += 1) {
    const a = args[i];
    if (GIT_GLOBAL_OPTIONS_WITH_VALUE.has(a)) { i += 1; continue; }
    if (a.startsWith('-')) continue;
    return i;
  }
  return -1;
}

function isGitReadCommand(args: readonly string[], subIndex: number): boolean {
  const sub = args[subIndex];
  if (GIT_ALWAYS_READ_SUBCOMMANDS.has(sub)) return true;
  const rest = args.slice(subIndex + 1);
  if (sub === 'symbolic-ref') {
    if (rest.some(a => a === '-d' || a === '--delete' || a === '-m')) return false;
    // Reading takes one ref; a second positional is a value being written.
    return rest.filter(a => a !== '--' && !a.startsWith('-')).length <= 1;
  }
  const readActions = GIT_READ_ACTION_SUBCOMMANDS[sub];
  if (readActions) {
    const action = rest.find(a => !a.startsWith('-'));
    if (!action) return GIT_BARE_FORM_IS_READ.has(sub);
    return readActions.has(action);
  }
  const readFlags = GIT_READ_FLAG_SUBCOMMANDS[sub];
  return Boolean(readFlags && rest.some(a => readFlags.has(a.split('=')[0])));
}

/**
 * Classify a git argv by its dominant phase so a single flat timeout can be
 * replaced by a command-class ceiling (parity: Orca git-command-classification.ts).
 * 'network' (fetch/pull/push/clone/ls-remote, submodule/remote update) must run
 * UNCAPPED — a flat 15s cap would kill any real network op. 'read' gets the larger
 * read ceiling. Everything else (write/other) keeps the tight default.
 */
export function classifyGitCommand(args: readonly string[]): GitCommandClass {
  const subIndex = findGitSubcommandIndex(args);
  if (subIndex === -1) return 'other';
  const sub = args[subIndex];
  if (GIT_NETWORK_SUBCOMMANDS.has(sub)) return 'network';
  const action = args.slice(subIndex + 1).find(a => !a.startsWith('-'));
  if ((sub === 'submodule' && action === 'update') || (sub === 'remote' && action === 'update')) {
    return 'network';
  }
  return isGitReadCommand(args, subIndex) ? 'read' : 'other';
}

/**
 * Resolve the per-common-dir FETCH_HEAD lock key for a git argv whose write of a
 * SHARED FETCH_HEAD / remote-tracking ref races sibling-worktree fetches
 * (parity: Orca git-fetch-head-lock.ts resolveGitFetchHeadCommand). Returns the
 * absolute `<commonGitDir>/FETCH_HEAD` path to serialize on, or null when the
 * command does not write a shared FETCH_HEAD (so no lane is taken). fs-only — no
 * subprocess — so it is safe inside the hardened sync runner.
 */
export function resolveFetchHeadLockKey(args: readonly string[], cwd: string): string | null {
  const subIndex = findGitSubcommandIndex(args);
  if (subIndex === -1) return null;
  const sub = args[subIndex];
  if (sub !== 'fetch' && sub !== 'pull') return null;

  // `-C <dir>` / `--git-dir` before the subcommand relocate the repo we resolve.
  let base = cwd;
  let explicitGitDir: string | undefined;
  for (let i = 0; i < subIndex; i += 1) {
    const a = args[i];
    if (a === '-C' && args[i + 1]) { base = path.resolve(base, args[i + 1]); i += 1; continue; }
    if (a.startsWith('-C') && a.length > 2) { base = path.resolve(base, a.slice(2)); continue; }
    if (a === '--git-dir' && args[i + 1]) { explicitGitDir = path.resolve(base, args[i + 1]); i += 1; continue; }
    if (a.startsWith('--git-dir=')) { explicitGitDir = path.resolve(base, a.slice('--git-dir='.length)); continue; }
    if (GIT_GLOBAL_OPTIONS_WITH_VALUE.has(a)) { i += 1; }
  }

  if (sub === 'fetch') {
    // A fetch that does not write FETCH_HEAD and touches no tracking ref needs no lane.
    let writesFetchHead = true;
    let updatesTrackingRef = false;
    for (const a of args.slice(subIndex + 1)) {
      if (a === '--no-write-fetch-head') writesFetchHead = false;
      else if (a === '--write-fetch-head') writesFetchHead = true;
      else if (a.includes(':refs/remotes/')) updatesTrackingRef = true;
    }
    if (!writesFetchHead && !updatesTrackingRef) return null;
  }

  const commonGitDir = explicitGitDir ?? resolveCommonGitDir(base);
  if (!commonGitDir) return null;
  return path.join(commonGitDir, 'FETCH_HEAD');
}

/**
 * Resolve the SHARED (common) git dir for `startPath` using ONLY the filesystem
 * (no subprocess): the worktree's git dir, then its `commondir` pointer. Returns
 * an absolute path, or null when `startPath` is not a repo.
 */
export function resolveCommonGitDir(startPath: string): string | null {
  const gitDir = resolveGitDir(startPath);
  if (!gitDir) return null;
  try {
    const commondir = fs.readFileSync(path.join(gitDir, 'commondir'), 'utf-8').trim();
    if (commondir) return path.resolve(gitDir, commondir);
  } catch { /* no commondir → gitDir IS the common dir (main checkout) */ }
  return gitDir;
}

// Bound the wait for a sibling process's FETCH_HEAD lane so a crashed holder can
// never wedge a fetch forever; a lockfile older than this is treated as abandoned.
const FETCH_HEAD_LOCK_STALE_MS = 2 * 60_000;

/**
 * Dependency seam for the FETCH_HEAD advisory lock's open/close/unlink calls.
 * Production defaults to `fs`; tests replace methods to force the best-effort
 * catch arms (stale-steal race, finally cleanup failures) without depending on
 * host filesystem races. Mirrors `_tmuxHooks` / `_exitWorktreeHooks`.
 */
export const _fetchHeadLockHooks = {
  openSync: fs.openSync as typeof fs.openSync,
  closeSync: fs.closeSync as typeof fs.closeSync,
  unlinkSync: fs.unlinkSync as typeof fs.unlinkSync,
};

/**
 * Run `fn` while holding a cross-process advisory lock on `<commonDir>/FETCH_HEAD`,
 * so two processes in the SAME repo cannot write FETCH_HEAD concurrently and
 * corrupt a sibling-worktree fetch transaction (parity: Orca runWithGitFetchHeadLock,
 * keyed by the resolved common dir rather than the worktree path). An abandoned
 * lockfile (holder crashed) older than {@link FETCH_HEAD_LOCK_STALE_MS} is stolen.
 * Within a single process the sync runner already serializes, so this lane is the
 * CROSS-process guard the ~20-lane fleet needs.
 */
export function withFetchHeadLock<T>(lockKey: string, fn: () => T): T {
  const lockPath = `${lockKey}.ami-lock`;
  const deadline = Date.now() + FETCH_HEAD_LOCK_STALE_MS;
  let fd: number | null = null;
  for (;;) {
    try {
      fd = _fetchHeadLockHooks.openSync(lockPath, 'wx');
      break;
    } catch (err) {
      if (errnoCodeOf(err) !== 'EEXIST') {
        // Cannot even create the lock file (ENOENT parent, EACCES) → run
        // unlocked rather than fail the fetch outright.
        return fn();
      }
      const held = mtimeOrUndefined(lockPath);
      if (held !== undefined && (Date.now() - held) > FETCH_HEAD_LOCK_STALE_MS) {
        try { _fetchHeadLockHooks.unlinkSync(lockPath); } catch { /* raced another stealer */ }
        continue;
      }
      if (Date.now() > deadline) {
        // Waited out the stale window without acquiring — proceed unlocked so a
        // persistently-wedged holder cannot block the fetch indefinitely.
        return fn();
      }
      sleepSync(FETCH_HEAD_LOCK_POLL_MS);
    }
  }
  try {
    return fn();
  } finally {
    try { _fetchHeadLockHooks.closeSync(fd!); } catch { /* best effort */ }
    try { _fetchHeadLockHooks.unlinkSync(lockPath); } catch { /* best effort */ }
  }
}

/**
 * Run git with the hardening flags, NO SHELL (execFile with an argv array), so
 * branch names / paths / refs can never be interpreted as shell metacharacters
 * (command-injection immune). Returns trimmed stdout.
 *
 * Exported so the worktree teardown path (exit_worktree) runs its git reads and
 * destructive calls through the SAME hardened runner (timeout + GIT_NO_PROMPT_ENV
 * + hooks/fsmonitor/gpg disabled) instead of a raw, unbounded child_process call
 * — one canonical hardened git invocation for the whole worktree subsystem.
 *
 * Concurrent-git hardening (parity: Orca shared/git):
 *   - command-class timeout: an explicit `opts.timeout` always wins; otherwise the
 *     ceiling is derived from {@link classifyGitCommand} — a NETWORK op (fetch/
 *     pull/push/clone) runs UNCAPPED instead of being killed by the flat 15s cap,
 *     a READ gets the larger {@link GIT_READ_CMD_TIMEOUT_MS}, and a write/other op
 *     keeps the tight default.
 *   - lock-contention retry: a TRANSIENT `*.lock: File exists` failure (a sibling
 *     git briefly held the lock) is retried with backoff
 *     ({@link GIT_LOCK_CONTENTION_RETRY_DELAYS_MS}); any other failure is thrown at
 *     once so a genuine error is never masked.
 *   - FETCH_HEAD serialization: a fetch/pull that writes the SHARED FETCH_HEAD is
 *     run under a per-common-dir cross-process lane ({@link withFetchHeadLock}) so
 *     sibling-worktree fetches in the fleet cannot corrupt each other.
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
  const cls = classifyGitCommand(args);
  const timeout = opts.timeout ?? gitTimeoutForClass(cls);
  const env = { ...process.env, ...GIT_NO_PROMPT_ENV };

  const runOnce = (): string => {
    const out = execFileSync('git', [...GIT_HARDENING_FLAGS, ...args], {
      cwd: opts.cwd,
      encoding: 'utf-8',
      env,
      timeout,
      stdio: opts.stdio === 'ignore' ? 'ignore' : ['ignore', 'pipe', 'pipe'],
      ...(opts.signal ? { signal: opts.signal } : {}),
    } as Parameters<typeof execFileSync>[2]);
    return typeof out === 'string' ? out.trim() : '';
  };

  // Retry ONLY transient lock contention; everything else propagates immediately.
  const runWithRetry = (): string =>
    retryOnGitLockContention(runOnce, GIT_LOCK_CONTENTION_RETRY_DELAYS_MS, () => opts.signal?.aborted === true);

  // Serialize a shared-FETCH_HEAD write across processes in the same repo.
  if (cls === 'network') {
    let lockKey: string | null = null;
    try { lockKey = resolveFetchHeadLockKey(args, opts.cwd); } catch { lockKey = null; }
    if (lockKey) return withFetchHeadLock(lockKey, runWithRetry);
  }
  return runWithRetry();
}

/**
 * Injectable safety check used by {@link createWorktreeSession} so tests can
 * force the unsafe-branch refuse arm without inventing a slug that both passes
 * {@link validateWorktreeSlug} and fails {@link isSafeRefName}.
 */
export const _worktreeSafetyHooks = {
  isSafeRefName,
};

/**
 * Memoization cache for {@link resolveGitDir}, keyed by the absolute start path
 * (parity: the reference's `resolveGitDirCache`, utils/git/gitFilesystem.ts). Every
 * downstream subprocess-free git reader (branch/HEAD) goes through the one
 * memoized resolver so a worktree/submodule `.git`-file is parsed at most once
 * per path.
 */
const resolveGitDirCache = new Map<string, string | null>();

/**
 * Resolve the REAL git directory for `startPath`, transparently handling the
 * worktree/submodule case where `.git` is a FILE containing `gitdir: <path>`
 * rather than a directory (parity: the reference's `resolveGitDir`,
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
 * (parity: the reference's filesystem git-HEAD reader). Resolves the real git dir via
 * {@link resolveGitDir} before reading `HEAD`, so an agent/task running inside a
 * `.ami/worktrees/` worktree records its actual branch instead of an empty
 * string. Returns `''` on a detached HEAD, a non-repo, or any read failure.
 *
 * Defense-in-depth: the branch name is read directly off disk (`.git/HEAD`),
 * which is attacker-controlled in a cloned/downloaded repo, and then flows into
 * the journal's `gitBranch` field and the cross-agent spend-by-branch view. A
 * tampered HEAD (`ref: refs/heads/$(…)`, control chars, `..` traversal) must not
 * propagate, so the extracted ref is run through {@link isSafeRefName} and a
 * name that fails the allowlist reads as `''` — parity with the reference's filesystem
 * git reader, which validates every ref it reads from disk (gitFilesystem.ts).
 */
export function readGitBranchFromDisk(cwd: string): string {
  const head = readGitHeadFromDisk(cwd);
  return head.type === 'branch' ? head.branch : '';
}

/**
 * Read the current git HEAD at `cwd` WITHOUT a git subprocess, distinguishing a
 * branch from a detached HEAD (parity: the reference's filesystem `readGitHead`). The
 * branch name is validated through {@link isSafeRefName} (defense-in-depth: `.git/HEAD`
 * is attacker-controlled in a cloned repo); a bare commit SHA (sha1 = 40, sha256 = 64
 * lowercase-hex) is recognized as a detached HEAD and returned verbatim. Anything
 * else — an unsafe ref, a non-repo, or a read failure — resolves to `{type:'none'}`.
 */
export function readGitHeadFromDisk(cwd: string): GitHeadInfo {
  const gitDir = resolveGitDir(cwd);
  if (!gitDir) return { type: 'none' };
  try {
    const head = fs.readFileSync(path.join(gitDir, 'HEAD'), 'utf-8').trim();
    const m = /^ref:\s*refs\/heads\/(.+)$/.exec(head);
    if (m) {
      const branch = m[1];
      // Reject a tampered/unsafe ref read off disk before it reaches any consumer.
      return isSafeRefName(branch) ? { type: 'branch', branch } : { type: 'none' };
    }
    // Detached HEAD: `.git/HEAD` holds a bare object id (sha1 = 40 hex, sha256 = 64 hex).
    if (isValidGitSha(head)) {
      return { type: 'detached', sha: head };
    }
    return { type: 'none' };
  } catch {
    return { type: 'none' };
  }
}

/**
 * A short, journal/diagnostics-friendly label for the current git HEAD at `cwd`:
 * the branch name when on a branch, `HEAD@<short-sha>` (12-char prefix) when the
 * HEAD is detached, or '' when the path is not a repo / unreadable. Lets the
 * reference journal and the cross-agent spend-by-branch view record real git
 * context for a detached session instead of a blank branch (parity: the reference's
 * computeBranch/computeHead degrading to the SHA rather than the empty string).
 */
export function readGitHeadLabelFromDisk(cwd: string): string {
  const head = readGitHeadFromDisk(cwd);
  if (head.type === 'branch') return head.branch;
  if (head.type === 'detached') return `HEAD@${head.sha.slice(0, 12)}`;
  return '';
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

let currentSession: WorktreeSession | null = null;

export function getCurrentWorktreeSession(): WorktreeSession | null {
  return currentSession;
}

export function setWorktreeSession(session: WorktreeSession | null): void {
  currentSession = session;
}

export function createWorktreeSession(cwd: string, name: string, opts: { signal?: AbortSignal } = {}): WorktreeSession {
  const startMs = Date.now();
  const signal = opts.signal;
  // Resolve to the canonical main repo root so creation works even when invoked
  // from inside an existing (linked) worktree.
  const repoRoot = resolveMainRepoRoot(cwd);
  const worktreeDir = path.join(repoRoot, '.ami', 'worktrees', name);
  const branch = worktreeBranchName(name);
  if (!_worktreeSafetyHooks.isSafeRefName(branch)) {
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
 * True when the working tree at `worktreePath` has no changes (parity: the reference's
 * central `getIsClean`, utils/git.ts:L356-L367). The ONE canonical cleanliness
 * helper so every callsite shares the same hardened read instead of re-spelling
 * `git status --porcelain`:
 *   - `--no-optional-locks` — a read-only status never takes git's optional index
 *     lock, so it cannot contend with a concurrent git operation in the tree.
 *   - `ignoreUntracked` appends `--untracked-files=no` (git's `-uno`) so a tree
 *     whose ONLY changes are untracked files reads as clean — the knob the reference's
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
    // work. the reference parity: WorktreeExitDialog counts
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
 * paths at risk before they confirm (parity with the reference's merge of
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
 * RECOVERABLY instead of discarding them. Mirrors the reference's `stashToCleanState`
 * (components/TeleportStash.tsx): the non-destructive alternative offered before a
 * git operation that would otherwise clobber a dirty tree.
 *
 * `message` is OPTIONAL (parity with the reference's `stashToCleanState(message?)`): when
 * omitted (or empty), a timestamped default `${AUTO_STASH_MESSAGE_PREFIX} - <ISO>`
 * is used so an auto-stash always lands with an identifiable, time-ordered label a
 * user can find later in `git stash list`.
 *
 * Untracked-file safety: `git stash push -u` includes untracked files IN the stash,
 * so they are preserved and recoverable — equivalent protection to the reference's
 * "stage untracked files before stashing" step, without leaving them staged in the
 * index after a later `git stash pop`.
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
export function stashToCleanState(worktreePath: string, message?: string): boolean {
  try {
    if (isClean(worktreePath)) return false; // nothing to stash
    const stashMessage = message && message.trim() !== ''
      ? message
      : `${AUTO_STASH_MESSAGE_PREFIX} - ${new Date().toISOString()}`;
    gitExec(['stash', 'push', '-u', '-m', stashMessage], { cwd: worktreePath, stdio: 'pipe' });
    // Confirm the stash actually cleaned the tree (git stash can no-op in edge
    // cases); only report success when the working tree is genuinely clean.
    return isClean(worktreePath);
  } catch {
    return false;
  }
}

/**
 * One day in milliseconds — the default throttle interval for recurring
 * background cleanup (parity: the reference's `ONE_DAY_MS` marker-file throttle,
 * utils/cleanup.ts). Exported so a caller can reason about the same window.
 */
export const ONE_DAY_MS = 24 * 60 * 60 * 1000;

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
 * Tear down the CURRENT session's worktree at interrupt/shutdown, NON-age-gated,
 * when (and only when) it is change-free — the missing eager cleanup path.
 *
 * {@link cleanupStaleWorktrees} is the recurring, age-gated + throttled sweep:
 * it never reclaims a same-session worktree created via `enter_worktree` because
 * that worktree is always fresh (mtime < the age cutoff) and a throttled sweep
 * may skip entirely. So a worktree entered in a session that is then interrupted
 * (SIGTERM/SIGHUP/orphan-TTY) leaks until it is 30 days old. the reference destroys each
 * session-created worktree immediately on shutdown regardless of age
 * (utils/swarm/teamHelpers.ts cleanupSession → destroyWorktree); this is AMI's
 * equivalent for the interactive `enter_worktree` session.
 *
 * SAFETY (never lose work): removal happens ONLY when {@link countWorktreeChanges}
 * reports zero uncommitted AND zero unpushed commits — a dirty or unverifiable
 * (`null`) worktree is KEPT so uncommitted work survives for review, matching
 * AMI's "cleaned up if no changes, preserved for review otherwise" contract. On a
 * successful removal the module's `currentSession` is cleared so a later sweep /
 * repeated shutdown does not try to re-remove it.
 *
 * Returns true only when a worktree was actually removed. Best-effort and fully
 * guarded, mirroring the sibling teardown hooks drained from engine.shutdown().
 */
export function removeSessionWorktreeIfClean(session: WorktreeSession | null = currentSession): boolean {
  if (!session?.worktreePath) return false;
  const worktreePath = session.worktreePath;
  if (!fs.existsSync(worktreePath)) {
    // Already gone — clear the stale reference if it was the active session.
    if (session === currentSession) currentSession = null;
    return false;
  }
  // Fail-closed: a dirty / unverifiable worktree is NEVER removed here. Count the
  // session's own commits precisely via the recorded base commit so a base branch
  // that itself has unpushed commits does not block cleanup of a work-free worktree.
  const changes = countWorktreeChanges(worktreePath, session.originalCwd, {
    baseCommit: session.originalHeadCommit,
  });
  if (changes === null || changes.uncommitted > 0 || changes.unpushed > 0) {
    return false;
  }
  try {
    gitExec(['worktree', 'remove', '--force', worktreePath], { cwd: session.originalCwd, timeout: 10000, stdio: 'ignore' });
  } catch {
    return false; // leave the worktree for the age-gated sweep rather than error
  }
  // Tear down the bound tmux session, if any, so a removed worktree never orphans
  // its background multiplexer session (parity with the exit_worktree teardown).
  if (session.tmuxSessionName) {
    try { killTmuxSession(session.tmuxSessionName); } catch { /* best effort */ }
  }
  if (session === currentSession) currentSession = null;
  return true;
}

/** Default staleness window for a clone's last fetch — 7 days, matching the reference's
 *  STALE_FETCH_WARN_MS (utils/deepLink/banner.ts:L22). */
export const STALE_FETCH_WARN_MS = 7 * 24 * 60 * 60 * 1000;

/**
 * Last-fetch timestamp (ms) for the repo at `cwd`, read from the mtime of
 * `FETCH_HEAD`. Because `FETCH_HEAD` is PER-WORKTREE, this reads it from BOTH the
 * worktree's own git dir and the resolved common (main repo) git dir and returns
 * whichever is NEWER — so a fetch run in the main checkout is still seen from a
 * linked worktree and vice-versa (parity with the reference's readLastFetchTime
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

/**
 * Copy files listed in `.worktreeinclude` from source to worktree.
 * Each line in the file is a relative path (a file or a directory). Lines
 * starting with `#` are comments; empty lines are skipped.
 *
 * Parity with the reference's copyWorktreeIncludeFiles (utils/worktree.ts): a file is
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
