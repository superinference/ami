// Split from git.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { execSync, spawnSync } from 'child_process';
import * as crypto from 'crypto';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { parseGitConfigValue } from '../git-config-parser';
import { logForDiagnosticsNoPII } from '../logger';

/** Hard ceiling so a wedged git invocation can never hang a caller. */
export const GIT_BRANCH_TIMEOUT_MS = 2_000;

/** Non-interactive git env: never block on credential / terminal prompts. */
export const GIT_NO_PROMPT_ENV = {
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
  // PII-free timing+outcome diagnostic around the git-discovery probe (parity with
  // the reference's find_git_root_* / is_git_check_* instrumentation): lets a caller
  // introspect "why is branch detection slow / did it find a branch" via the
  // AMI_DIAGNOSTICS_FILE channel without ever logging the branch name itself.
  const startTime = Date.now();
  logForDiagnosticsNoPII('info', 'get_git_branch_started');
  try {
    const out = execSync('git rev-parse --abbrev-ref HEAD', {
      cwd,
      encoding: 'utf-8',
      timeout: GIT_BRANCH_TIMEOUT_MS,
      stdio: ['ignore', 'pipe', 'ignore'],
      env: { ...process.env, ...GIT_NO_PROMPT_ENV },
    }).trim();
    const branch = !out || out === 'HEAD' ? undefined : out;
    logForDiagnosticsNoPII('info', 'get_git_branch_completed', {
      duration_ms: Date.now() - startTime,
      found: branch !== undefined,
    });
    return branch;
  } catch {
    logForDiagnosticsNoPII('info', 'get_git_branch_completed', {
      duration_ms: Date.now() - startTime,
      found: false,
    });
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
export function runGit(cwd: string, args: string[]): { code: number; stdout: string } | null {
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

/**
 * Authoritative "is this path ignored by git?" predicate — delegates the whole
 * question to `git check-ignore` so ALL ignore sources resolve with git's real
 * precedence: nested `.gitignore` files, `.git/info/exclude`, and the GLOBAL
 * gitignore (`core.excludesFile`) — none of which the in-process glob/ignore
 * matchers can see. `filePath` may be absolute or relative to `cwd`.
 *
 * Exit-code branching mirrors the reference's `isPathGitignored`:
 *   0   → path is ignored        → true
 *   1   → path is not ignored    → false
 *   128 → `cwd` is not a git repo → false (fail OPEN — never spuriously hide a
 *          path just because there is no repo)
 * A spawn failure (git missing / timeout → `runGit` returns null) also fails
 * open. Uses the shared no-shell {@link runGit} runner (injection-safe, hard
 * timeout, `--no-optional-locks` so it never disturbs a concurrent index lock).
 */
export function isPathGitignored(filePath: string, cwd: string): boolean {
  const res = runGit(cwd, ['--no-optional-locks', 'check-ignore', '--quiet', '--', filePath]);
  return res?.code === 0;
}

/**
 * Resolve git's GLOBAL ignore file (`core.excludesFile`): `$XDG_CONFIG_HOME/git/ignore`
 * when `XDG_CONFIG_HOME` is set, else `~/.config/git/ignore`. Pure (home dir + env
 * injected) so the resolution is unit-testable without touching the real `$HOME`.
 * Parity with the reference's `getGlobalGitignorePath()` (`utils/git/gitignore.ts`),
 * extended with the XDG_CONFIG_HOME precedence git itself honours. This is the ONE
 * canonical resolver: the `@`-file picker (common/src/autocomplete-provider.ts)
 * re-exports it rather than defining its own, so the global-ignore WRITE path
 * ({@link addFileGlobRuleToGitignore}) and the picker's READ path agree on the file.
 */
export function getGlobalGitignorePath(
  homeDir: string = os.homedir(),
  xdgConfigHome?: string | undefined,
): string {
  // Only fall back to process.env when the caller OMITTED the arg. Passing
  // `undefined` explicitly means "no XDG override" (so tests and callers that
  // inject homeDir get a deterministic ~/.config under that home). A default
  // parameter of `= process.env.XDG_CONFIG_HOME` would replace an explicit
  // `undefined` with the ambient env and break that contract on CI runners
  // that set XDG_CONFIG_HOME.
  const xdg = arguments.length >= 2 ? xdgConfigHome : process.env.XDG_CONFIG_HOME;
  const base = xdg && xdg.trim() !== ''
    ? xdg
    : path.join(homeDir, '.config');
  return path.join(base, 'git', 'ignore');
}

/** `127.0.0.1` / `::1` / `localhost` (optionally with a `:port` suffix). */
export function isLocalHost(host: string): boolean {
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
  const sshMatch = trimmed.match(/^git@([^:]+):(.+)$/);
  if (sshMatch) {
    return `${sshMatch[1]}/${sshMatch[2].replace(/\.git$/, "")}`.toLowerCase();
  }

  // URL form: https://host/owner/repo(.git) or ssh://git@host/owner/repo
  const urlMatch = trimmed.match(/^(?:https?|ssh):\/\/([^/]+)\/(.+)$/);
  if (urlMatch) {
    // authority may be "user@host"; strip optional userinfo in code to avoid a
    // nested-quantifier regex (ReDoS-flagged).
    const authority = urlMatch[1];
    const at = authority.lastIndexOf('@');
    const host = at >= 0 ? authority.slice(at + 1) : authority;
    const path = urlMatch[2].replace(/\.git$/, "");

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
 * Read the raw `origin` remote URL for `cwd`, or `null` when `cwd` has no
 * `origin` remote or the value is empty. Single point for "what is this
 * checkout's origin URL" so every repo-identity helper ({@link getRepoRemoteHash},
 * {@link classifyRepoRemote}, and the `detect-repository` resolver) shares one
 * reader instead of re-spawning git with slightly different flags.
 *
 * Reads `.git/config` DIRECTLY off disk FIRST, with no git subprocess
 * ({@link parseGitConfigValue}). This is both safer and more robust than
 * `git config --get`: it never processes a malicious repo's `include.path` /
 * `includeIf.*.path` directives (arbitrary file read / command execution via
 * `core.fsmonitor`, `core.sshCommand`, aliases, …), and it still resolves the
 * URL when the git binary is absent, broken, or would hang. The common case
 * (a plain `[remote "origin"] url = …`) is satisfied without spawning anything.
 *
 * Falls back to a no-shell `git config --get` subprocess only when the direct
 * read yields nothing — e.g. the rare case where the remote is contributed by
 * an `includeIf`-included file the direct parse cannot see. That subprocess is
 * injection-safe and hard-timeout bounded.
 */
export function getRemoteOriginUrl(cwd: string): string | null {
  const gitDir = resolveGitDir(cwd);
  if (gitDir) {
    const fsUrl = parseGitConfigValue(gitDir, 'remote', 'origin', 'url');
    if (fsUrl) return fsUrl;
  }
  const res = runGit(cwd, ['config', '--get', 'remote.origin.url']);
  if (!res || res.code !== 0) return null;
  return res.stdout || null;
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
  const url = getRemoteOriginUrl(cwd);
  if (url === null) return null;
  // An empty / unparseable remote URL falls through to the null return below.
  const normalized = normalizeGitRemoteUrl(url);
  if (!normalized) return null;
  return crypto.createHash('sha256').update(normalized).digest('hex').slice(0, 16);
}

/**
 * Trust classification of a repo's `origin` remote, used to pick a safe default
 * for identity-leaking behavior (commit/PR attribution). Parity with the reference
 * Code's repo-class gating (`getRepoClassCached()` →
 * `internal`/`external`/`none`): the classification is keyed to whether the push
 * target is a remote the user positively owns.
 *
 * - `owned`    — the normalized `origin` remote matches one of `ownedPatterns`.
 * - `external` — a remote exists but matches none of the patterns (public/
 *                upstream repo the user is merely contributing to).
 * - `none`     — no `origin` remote (or git unavailable / unparseable URL).
 *
 * Callers that want a fail-safe-ON policy (tighten when ownership is NOT
 * confirmed) should treat anything other than `owned` as untrusted — `external`
 * and `none` both fall to the strict side, matching the reference's "only a confirmed
 * internal repo relaxes the guard" rule.
 */
export const REPO_REMOTE_CLASS = ['owned', 'external', 'none'] as const;

export type RepoRemoteClass = (typeof REPO_REMOTE_CLASS)[number];

export function classifyRepoRemote(
  cwd: string,
  ownedPatterns: readonly string[],
): RepoRemoteClass {
  const url = getRemoteOriginUrl(cwd);
  if (url === null) return 'none';
  const normalized = normalizeGitRemoteUrl(url);
  if (!normalized) return 'none';
  const owned = ownedPatterns.some(p => {
    const pat = p.trim().toLowerCase();
    return pat.length > 0 && normalized.includes(pat);
  });
  return owned ? 'owned' : 'external';
}

/**
 * Resolve the `.git` directory that holds the ref store for `cwd`, following a
 * worktree/submodule `.git` FILE (`gitdir: <path>`) and its `commondir` pointer
 * (linked worktrees keep `refs/remotes/*` in the common dir, not the per-worktree
 * gitdir). Returns `null` when `cwd` is not a git working tree. Pure fs — no git
 * subprocess.
 */
export function resolveGitDir(cwd: string): string | null {
  const dotGit = path.join(cwd, '.git');
  let stat: fs.Stats;
  try {
    stat = fs.statSync(dotGit);
  } catch {
    return null;
  }
  let gitDir: string;
  if (stat.isDirectory()) {
    gitDir = dotGit;
  } else {
    // A linked worktree / submodule `.git` is a file: `gitdir: <path>`.
    let pointer: string;
    try {
      pointer = fs.readFileSync(dotGit, 'utf-8').trim();
    } catch {
      return null;
    }
    const m = pointer.match(/^gitdir:\s*(.+)$/);
    if (!m) return null;
    gitDir = path.resolve(cwd, m[1].trim());
  }
  // Honor `commondir` so ref reads land in the shared store for a linked worktree.
  try {
    const commonPtr = fs.readFileSync(path.join(gitDir, 'commondir'), 'utf-8').trim();
    if (commonPtr) return path.resolve(gitDir, commonPtr);
  } catch {
    /* no commondir → gitDir is already the common dir */
  }
  return gitDir;
}

/** Read one ref from `gitDir`, following a single `ref: <target>` level. */
export function readRefFrom(gitDir: string, ref: string, depth: number): string | null {
  // Guard a symbolic-ref cycle so a malformed store can't spin forever.
  if (depth > 5) return null;
  // Loose ref file.
  let loose: string | null = null;
  try {
    loose = fs.readFileSync(path.join(gitDir, ref), 'utf-8').trim();
  } catch {
    loose = null;
  }
  if (loose) {
    const sym = loose.match(/^ref:\s*(.+)$/);
    if (sym) return readRefFrom(gitDir, sym[1].trim(), depth + 1);
    return /^[0-9a-f]{40}$/i.test(loose) ? loose : null;
  }
  // packed-refs fallback.
  try {
    const packed = fs.readFileSync(path.join(gitDir, 'packed-refs'), 'utf-8');
    for (const line of packed.split('\n')) {
      // Skip the header (`# ...`), peeled-tag lines (`^<sha>`), and blanks.
      if (!line || line.startsWith('#') || line.startsWith('^')) continue;
      const sp = line.indexOf(' ');
      if (sp < 0) continue;
      const sha = line.slice(0, sp);
      if (line.slice(sp + 1).trim() === ref && /^[0-9a-f]{40}$/i.test(sha)) return sha;
    }
  } catch {
    /* no packed-refs */
  }
  return null;
}

/**
 * Resolve a fully-qualified git ref (e.g. `refs/remotes/origin/main`) to its
 * commit SHA by reading the ref store DIRECTLY off the filesystem — no git
 * subprocess. Reads the loose ref first, then `packed-refs`, following a single
 * level of symbolic ref (`ref: <target>`). Returns the 40-char SHA, or `null`
 * when the ref is absent/unreadable, the ref string is unsafe (contains a `..`
 * or empty segment), or `cwd` is not a git working tree.
 *
 * Mirrors the reference's `resolveRef` fast-path (utils/worktree.ts): when
 * `origin/<default>` already exists locally the base SHA is obtained without
 * spawning `git fetch`/`git rev-parse`, skipping a multi-second commit-graph scan
 * on very large clones while still giving the caller the base commit.
 */
export function resolveRef(cwd: string, ref: string): string | null {
  // Path-traversal guard: a ref is joined onto the git dir, so reject `..`/empty
  // segments that could escape the ref store.
  if (ref.split('/').some(seg => seg === '' || seg === '..' || seg === '.')) return null;
  const gitDir = resolveGitDir(cwd);
  if (!gitDir) return null;
  return readRefFrom(gitDir, ref, 0);
}
