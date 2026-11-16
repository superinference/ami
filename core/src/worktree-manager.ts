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
export function gitExec(args: string[], opts: { cwd: string; timeout?: number; stdio?: 'ignore' | 'pipe' } = { cwd: process.cwd() }): string {
  const env = { ...process.env, ...GIT_NO_PROMPT_ENV };
  const out = execFileSync('git', [...GIT_HARDENING_FLAGS, ...args], {
    cwd: opts.cwd,
    encoding: 'utf-8',
    env,
    timeout: opts.timeout ?? GIT_CMD_TIMEOUT_MS,
    stdio: opts.stdio === 'ignore' ? 'ignore' : ['ignore', 'pipe', 'pipe'],
  });
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

export function createWorktreeSession(cwd: string, name: string): WorktreeSession {
  const startMs = Date.now();
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
    originalBranch = gitExec(['rev-parse', '--abbrev-ref', 'HEAD'], { cwd: repoRoot });
    originalHead = gitExec(['rev-parse', 'HEAD'], { cwd: repoRoot });
  } catch { /* detached / fresh repo */ }

  // Create the worktree on disk if it doesn't already exist.
  // enter-worktree.ts creates the worktree itself (with base_ref support) then
  // calls createWorktreeSession for metadata; task.ts relies on this function
  // to create the worktree as well.
  if (!fs.existsSync(worktreeDir)) {
    fs.mkdirSync(path.dirname(worktreeDir), { recursive: true });
    try {
      gitExec(['worktree', 'add', '-B', branch, worktreeDir, 'HEAD'], { cwd: repoRoot, stdio: 'ignore' });
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

export function cleanupStaleWorktrees(cwd: string, maxAgeDays: number = 30): string[] {
  const worktreeDir = path.join(cwd, '.ami', 'worktrees');
  if (!fs.existsSync(worktreeDir)) return [];

  // Config-validity gate: never reap worktrees based on a config we cannot parse.
  // A corrupt .ami/config.json could mean the retention window is wrong, so we
  // fail safe and skip all destructive removal until the config is fixed.
  if (isProjectConfigInvalid(cwd)) return [];

  const cleaned: string[] = [];
  const now = Date.now();
  const maxAgeMs = maxAgeDays * 24 * 60 * 60 * 1000;
  // Never reap the worktree the active session is currently using.
  const activePath = currentSession?.worktreePath ? path.resolve(currentSession.worktreePath) : null;

  for (const entry of fs.readdirSync(worktreeDir)) {
    const fullPath = path.join(worktreeDir, entry);
    try {
      if (activePath && path.resolve(fullPath) === activePath) continue;
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
        } catch { /* leave it for the next sweep */ }
      }
    } catch { /* unreadable entry — skip */ }
  }

  // Prune any stale worktree metadata
  try {
    gitExec(['worktree', 'prune'], { cwd, timeout: 5000, stdio: 'ignore' });
  } catch { /* best effort */ }

  return cleaned;
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

export function symlinkLargeDirectories(sourceCwd: string, worktreePath: string, dirs: string[] = ['node_modules']): void {
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
      // ENOENT/EEXIST are benign races; anything else is swallowed to keep
      // worktree setup resilient.
      void err;
    }
  }
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
