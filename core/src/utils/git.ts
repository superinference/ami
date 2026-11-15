import { execSync, spawnSync } from 'child_process';
import * as crypto from 'crypto';

/** Hard ceiling so a wedged git invocation can never hang a caller. */
const GIT_BRANCH_TIMEOUT_MS = 2_000;

/** Non-interactive git env: never block on credential / terminal prompts. */
const GIT_NO_PROMPT_ENV = {
  GIT_TERMINAL_PROMPT: '0',
  GIT_ASKPASS: '',
} as const;

/**
 * Return the current git branch name for `cwd`, or `undefined` when `cwd` is not
 * a git working tree (or git is unavailable). Shared helper so callers that need
 * to stamp the branch (e.g. persisted sessions) don't each re-spawn git with
 * slightly different flags. A detached HEAD yields `undefined` (abbrev-ref prints
 * "HEAD", which is not a branch name).
 */
export function getGitBranch(cwd: string): string | undefined {
  try {
    const out = execSync('git rev-parse --abbrev-ref HEAD', {
      cwd,
      encoding: 'utf-8',
      timeout: GIT_BRANCH_TIMEOUT_MS,
      stdio: ['ignore', 'pipe', 'ignore'],
      env: { ...process.env, ...GIT_NO_PROMPT_ENV },
    }).trim();
    if (!out || out === 'HEAD') return undefined;
    return out;
  } catch {
    return undefined;
  }
}

/**
 * Run `git <args>` in `cwd` with no shell (argv form — injection-safe), a hard
 * timeout, and a non-interactive environment. Returns the exit code and trimmed
 * stdout, or `null` when git could not be spawned at all (not on PATH, timeout,
 * etc.). Read-only callers pass `--no-optional-locks` in `args` so they never
 * take or disturb the index lock during concurrent git work.
 */
function runGit(cwd: string, args: string[]): { code: number; stdout: string } | null {
  const res = spawnSync('git', args, {
    cwd,
    encoding: 'utf-8',
    timeout: GIT_BRANCH_TIMEOUT_MS,
    stdio: ['ignore', 'pipe', 'ignore'],
    env: { ...process.env, ...GIT_NO_PROMPT_ENV },
  });
  // `error` is set when the process could not be spawned or was killed (ENOENT,
  // timeout). `status` is null in that case; treat the whole call as unavailable.
  if (res.error || res.status === null) return null;
  // With `encoding` set and stdout piped, spawnSync always yields a string here.
  return { code: res.status, stdout: res.stdout.trim() };
}

/** `127.0.0.1` / `::1` / `localhost` (optionally with a `:port` suffix). */
function isLocalHost(host: string): boolean {
  const bare = host.replace(/:\d+$/, '');
  return bare === '127.0.0.1' || bare === 'localhost' || bare === '::1';
}

/**
 * Normalize a git remote URL to one canonical, clone-protocol-independent
 * `host/owner/repo` form (lowercase, no `.git` suffix), or `null` when the input
 * is empty or unrecognized. This yields a stable repo identity regardless of
 * whether the remote was cloned over SSH or HTTPS.
 *
 * Examples:
 * - `git@github.com:owner/repo.git`              -> `github.com/owner/repo`
 * - `https://github.com/owner/repo.git`          -> `github.com/owner/repo`
 * - `ssh://git@github.com/owner/repo`            -> `github.com/owner/repo`
 * - `http://user@127.0.0.1:16583/git/owner/repo` -> `github.com/owner/repo`
 * - `http://127.0.0.1:1/git/ghe.host/owner/repo` -> `ghe.host/owner/repo`
 *
 * Local git-proxy URLs (`127.0.0.1`/`localhost` with a `/git/...` path) are
 * special-cased: a first path segment containing a dot is treated as an
 * enterprise host, otherwise `github.com` is assumed.
 */
export function normalizeGitRemoteUrl(url: string): string | null {
  const trimmed = url.trim();
  if (!trimmed) return null;

  // SSH scp-like form: git@host:owner/repo(.git)
  const sshMatch = trimmed.match(/^git@([^:]+):(.+?)(?:\.git)?$/);
  if (sshMatch) {
    return `${sshMatch[1]}/${sshMatch[2]}`.toLowerCase();
  }

  // URL form: https://host/owner/repo(.git) or ssh://git@host/owner/repo
  const urlMatch = trimmed.match(/^(?:https?|ssh):\/\/(?:[^@/]+@)?([^/]+)\/(.+?)(?:\.git)?$/);
  if (urlMatch) {
    const host = urlMatch[1];
    const path = urlMatch[2];

    if (isLocalHost(host) && path.startsWith('git/')) {
      const proxyPath = path.slice(4); // strip the "git/" prefix
      const segments = proxyPath.split('/');
      // 3+ segments whose first contains a dot → host/owner/repo (enterprise).
      if (segments.length >= 3 && segments[0].includes('.')) {
        return proxyPath.toLowerCase();
      }
      // Otherwise owner/repo on github.com (legacy proxy form).
      return `github.com/${proxyPath}`.toLowerCase();
    }

    return `${host}/${path}`.toLowerCase();
  }

  return null;
}

/**
 * Return a stable, PII-free repository identity for `cwd`: the first 16 chars of
 * the SHA-256 of the {@link normalizeGitRemoteUrl normalized} `origin` remote
 * URL, or `null` when `cwd` has no git `origin` remote (or git is unavailable).
 *
 * The hash is identical across SSH and HTTPS clones of the same repo and never
 * exposes the real owner/repo name, so it is safe to use as a per-repo state key
 * or to log in place of the raw remote URL.
 */
export function getRepoRemoteHash(cwd: string): string | null {
  const res = runGit(cwd, ['config', '--get', 'remote.origin.url']);
  if (!res || res.code !== 0) return null;
  // An empty / unparseable remote URL falls through to the null return below.
  const normalized = normalizeGitRemoteUrl(res.stdout);
  if (!normalized) return null;
  return crypto.createHash('sha256').update(normalized).digest('hex').slice(0, 16);
}

/** Common default-branch names, in the order git itself tends to prefer them. */
const DEFAULT_REMOTE_BASE_CANDIDATES = ['origin/main', 'origin/staging', 'origin/master'] as const;

/**
 * Pick the best remote ref to diff a worktree against, or `null` when none can be
 * resolved. Priority, using only LOCAL refs (never contacts the network):
 *
 * 1. the current branch's upstream tracking ref (`@{u}`), when set;
 * 2. origin's recorded default branch (`refs/remotes/origin/HEAD`, set at clone);
 * 3. the first existing ref from `candidates` (defaults to
 *    `origin/main`, `origin/staging`, `origin/master`).
 */
export function findRemoteBase(
  cwd: string,
  candidates: readonly string[] = DEFAULT_REMOTE_BASE_CANDIDATES,
): string | null {
  const tracking = runGit(cwd, ['rev-parse', '--abbrev-ref', '--symbolic-full-name', '@{u}']);
  if (tracking && tracking.code === 0 && tracking.stdout) {
    return tracking.stdout;
  }

  const head = runGit(cwd, ['symbolic-ref', '--short', 'refs/remotes/origin/HEAD']);
  if (head && head.code === 0 && head.stdout) {
    return head.stdout;
  }

  for (const candidate of candidates) {
    const verify = runGit(cwd, ['rev-parse', '--verify', '--quiet', candidate]);
    if (verify && verify.code === 0) {
      return candidate;
    }
  }

  return null;
}

/**
 * Return `true` when `cwd` is inside a shallow git clone (one with a truncated
 * history), `false` otherwise or when git is unavailable. Callers use this to
 * fall back to a `diff HEAD` base when a merge-base cannot be computed.
 */
export function isShallowClone(cwd: string): boolean {
  const res = runGit(cwd, ['rev-parse', '--is-shallow-repository']);
  return !!res && res.code === 0 && res.stdout === 'true';
}

/**
 * Return `true` when the `cwd` working tree has no changes, `false` when it is
 * dirty or git is unavailable. Pass `{ ignoreUntracked: true }` to ignore
 * untracked files (appends `-uno`). Read-only: uses `--no-optional-locks` so it
 * never disturbs the index lock during concurrent git work.
 */
export function getIsClean(cwd: string, options?: { ignoreUntracked?: boolean }): boolean {
  const args = ['--no-optional-locks', 'status', '--porcelain'];
  if (options?.ignoreUntracked) args.push('-uno');
  const res = runGit(cwd, args);
  return !!res && res.code === 0 && res.stdout.length === 0;
}
