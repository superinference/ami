import 'child_process';
import 'crypto';
import * as fs from 'fs';
import 'os';
import * as path from 'path';

import '../git-config-parser';
import { logForDiagnosticsNoPII } from '../logger';
import { hasBinaryExtension, isBinaryContent } from './binary';
import { getGlobalGitignorePath, isPathGitignored, readRefFrom, resolveGitDir, resolveRef, runGit } from './git-normalize-git-remote-url';
export { REPO_REMOTE_CLASS, classifyRepoRemote, getGitBranch, getGlobalGitignorePath, getRemoteOriginUrl, getRepoRemoteHash, isPathGitignored, normalizeGitRemoteUrl, resolveRef } from './git-normalize-git-remote-url';
export type { RepoRemoteClass } from './git-normalize-git-remote-url';

/**
 * Add a recursive glob rule (`**` + `/` + filename) to the user's GLOBAL
 * gitignore so a locally-generated, never-shared file (e.g.
 * `.ami/permissions.local.json`) is
 * actually kept out of version control — making good on the "gitignored" promise
 * AMI's project-local save destination already advertises. Mirrors the reference's
 * `addFileGlobRuleToGitignore` (`utils/git/gitignore.ts` L53-99), SYNC to fit
 * AMI's synchronous git helpers and the synchronous permission-save path:
 *   1. no-op outside a git repo — nothing to ignore ({@link isGitRepo});
 *   2. directory patterns (trailing `/`) are probed with a synthetic child path
 *      because `git check-ignore` on a bare directory pattern behaves differently;
 *   3. two-layer dedup — first ask git whether the pattern is ALREADY covered by
 *      any gitignore source ({@link isPathGitignored}, local OR global), then
 *      re-check the global file's own text before appending;
 *   4. ENOENT → create `~/.config/git/` (recursive) and write the file fresh;
 *   5. every failure is swallowed (best-effort) — a gitignore write must never
 *      break the operation that triggered it.
 * Returns `true` only when it actually wrote the entry (appended or created),
 * `false` otherwise (not a repo / already ignored / already present / error) so a
 * caller can trace the side effect.
 */
export function addFileGlobRuleToGitignore(filename: string, cwd: string): boolean {
  try {
    if (!isGitRepo(cwd)) return false;

    const gitignoreEntry = `**/${filename}`;
    // A bare directory pattern (trailing `/`) is probed via a synthetic child file
    // because `git check-ignore` on a directory pattern alone behaves differently.
    const testPath = filename.endsWith('/') ? `${filename}sample-file.txt` : filename;
    if (isPathGitignored(testPath, cwd)) return false;

    const globalGitignorePath = getGlobalGitignorePath();
    fs.mkdirSync(path.dirname(globalGitignorePath), { recursive: true });

    let content: string;
    try {
      content = fs.readFileSync(globalGitignorePath, 'utf-8');
    } catch (e) {
      if ((e as NodeJS.ErrnoException)?.code === 'ENOENT') {
        fs.writeFileSync(globalGitignorePath, `${gitignoreEntry}\n`, 'utf-8');
        return true;
      }
      throw e;
    }
    if (content.includes(gitignoreEntry)) return false;
    fs.appendFileSync(globalGitignorePath, `\n${gitignoreEntry}\n`);
    return true;
  } catch {
    return false;
  }
}

/**
 * Read the committed HEAD SHA of a LINKED git worktree directory by reading its
 * `.git` pointer file DIRECTLY — no git subprocess, no upward `findGitRoot` walk.
 * A `rev-parse HEAD` spawn burns ~15-55ms of process overhead even for a 2ms task,
 * so a fast-resume probe that may run on every `enter_worktree` reads the ref store
 * off disk instead (parity with the reference's `readWorktreeHeadSha`).
 *
 * `<worktreePath>/.git` for a linked worktree is a FILE containing `gitdir: <path>`;
 * HEAD lives in that per-worktree gitdir, while a branch HEAD's `refs/heads/<name>`
 * resolves through the shared common dir (followed by {@link resolveRef}). Returns:
 *   - the 40-char SHA when `worktreePath` is a valid, registered linked worktree;
 *   - `null` when the directory is absent, is not a linked worktree (`.git` is a
 *     directory, or not a `gitdir:` pointer), or its HEAD is missing/malformed.
 * A `null` return is the caller's signal that the path is NOT a resumable worktree.
 */
export function readWorktreeHeadSha(worktreePath: string): string | null {
  let pointer: string;
  try {
    pointer = fs.readFileSync(path.join(worktreePath, '.git'), 'utf-8').trim();
  } catch {
    return null;
  }
  const m = pointer.match(/^gitdir:\s*(.+)$/);
  if (!m) return null;
  const perWorktreeGitDir = path.resolve(worktreePath, m[1].trim());
  let head: string;
  try {
    head = fs.readFileSync(path.join(perWorktreeGitDir, 'HEAD'), 'utf-8').trim();
  } catch {
    return null;
  }
  const sym = head.match(/^ref:\s*(.+)$/);
  if (sym) {
    // Branch HEAD: refs/heads/<name> lives in the shared common dir, which
    // resolveGitDir reaches by following the worktree's `commondir` pointer.
    const gitDir = resolveGitDir(worktreePath);
    if (!gitDir) return null;
    return readRefFrom(gitDir, sym[1].trim(), 0);
  }
  return /^[0-9a-f]{40}$/i.test(head) ? head : null;
}

/**
 * In-progress (transient) multi-step git operations, keyed by the on-disk marker
 * the operation leaves in the git dir, in the priority order git itself resolves
 * them. Mirrors the reference's `isGitTransientState` indicator set
 * (utils/commitAttribution.ts) plus `REVERT_HEAD` for an in-progress revert.
 */
const GIT_TRANSIENT_MARKERS: ReadonlyArray<readonly [marker: string, label: string]> = [
  ['rebase-merge', 'rebase'],
  ['rebase-apply', 'rebase'],
  ['MERGE_HEAD', 'merge'],
  ['CHERRY_PICK_HEAD', 'cherry-pick'],
  ['REVERT_HEAD', 'revert'],
  ['BISECT_LOG', 'bisect'],
];

/**
 * Detect whether the repository at `cwd` is mid-way through a transient multi-step
 * git operation — an in-progress rebase, merge, cherry-pick, revert, or bisect.
 * Returns the operation label (e.g. `'rebase'`, `'merge'`) or `null` when the tree
 * is in a normal state (or `cwd` is not a git working tree). Pure fs — reads the
 * marker files git drops in the git dir; no git subprocess. Resolves the real git
 * dir first (follows a worktree/submodule `.git` file + `commondir`) so a linked
 * worktree's in-progress operation is detected correctly.
 */
export function detectGitTransientState(cwd: string): string | null {
  const gitDir = resolveGitDir(cwd);
  if (!gitDir) return null;
  for (const [marker, label] of GIT_TRANSIENT_MARKERS) {
    if (fs.existsSync(path.join(gitDir, marker))) return label;
  }
  return null;
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
    // fs-level ref read first (no subprocess) — parity with the reference's resolveRef
    // fast-path: when the remote-tracking ref already exists locally we skip
    // spawning `git rev-parse`. `runGit` stays the authority for anything the fs
    // read can't settle (e.g. a non-`origin/` candidate).
    if (candidate.startsWith('origin/') && resolveRef(cwd, `refs/remotes/${candidate}`)) {
      return candidate;
    }
    const verify = runGit(cwd, ['rev-parse', '--verify', '--quiet', candidate]);
    if (verify && verify.code === 0) {
      return candidate;
    }
  }

  return null;
}

/**
 * Return `true` when `cwd` is inside a git working tree, `false` otherwise (not a
 * repo, or git unavailable — fails CLOSED so a precondition guard treats "can't tell"
 * as "not a repo"). Delegates to `git rev-parse --is-inside-work-tree` via the shared
 * no-shell {@link runGit} runner (injection-safe, hard timeout). Parity with the reference
 * Code's `getIsGit()` precondition check (used by worktree-spawning commands like
 * `/batch`). Shared helper so worktree-dependent callers don't each re-spawn git.
 */
export function isGitRepo(cwd: string): boolean {
  // PII-free timing+outcome diagnostic (parity with the reference's
  // is_git_check_started / is_git_check_completed): records how long the repo probe
  // took and whether `cwd` is a work tree, with no path/identity data.
  const startTime = Date.now();
  logForDiagnosticsNoPII('info', 'is_git_check_started');
  const res = runGit(cwd, ['rev-parse', '--is-inside-work-tree']);
  const isGit = !!res && res.code === 0 && res.stdout === 'true';
  logForDiagnosticsNoPII('info', 'is_git_check_completed', {
    duration_ms: Date.now() - startTime,
    is_git: isGit,
  });
  return isGit;
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

/**
 * Average characters per changed line — the heuristic used to turn a
 * `git diff --stat` line count into an approximate character volume. Parity with
 * the reference's `getGitDiffSize` (utils/commitAttribution.ts), which multiplies
 * `insertions + deletions` by this constant as a cheap proxy for change size.
 */
export const AVG_CHARS_PER_DIFF_LINE = 40;

/**
 * Approximate the character volume of the STAGED changes in `cwd` (optionally
 * restricted to `filePath`) from the summary line of `git diff --cached --stat`,
 * using the {@link AVG_CHARS_PER_DIFF_LINE} chars-per-line heuristic:
 * `(insertions + deletions) * 40`. A cheap "how much changed" proxy that never
 * reads a single hunk. Returns `0` when there are no staged changes, the path has
 * none, or git is unavailable. Parity with the reference's `getGitDiffSize`.
 *
 * Read-only: uses the shared no-shell {@link runGit} runner with
 * `--no-optional-locks` so it never disturbs a concurrent index lock.
 */
export function getStagedDiffSize(cwd: string, filePath?: string): number {
  const args = ['--no-optional-locks', 'diff', '--cached', '--stat'];
  if (filePath !== undefined) args.push('--', filePath);
  const res = runGit(cwd, args);
  if (!res || res.code !== 0 || !res.stdout) return 0;
  let total = 0;
  for (const line of res.stdout.split('\n')) {
    // Only the trailing summary line ("N file(s) changed, …") carries the
    // insertion/deletion counts; per-file rows ("file | 5 ++---") are skipped.
    if (!line.includes('file changed') && !line.includes('files changed')) continue;
    const insertMatch = line.match(/(\d+) insertions?/);
    const deleteMatch = line.match(/(\d+) deletions?/);
    const insertions = insertMatch ? parseInt(insertMatch[1], 10) : 0;
    const deletions = deleteMatch ? parseInt(deleteMatch[1], 10) : 0;
    total += (insertions + deletions) * AVG_CHARS_PER_DIFF_LINE;
  }
  return total;
}

/**
 * Return `true` when `filePath` is staged for DELETION in `cwd` — the staged
 * `git diff --cached --name-status` reports a `D` status for it — and `false`
 * otherwise (modified/added, no staged change, or git unavailable). Parity with
 * the reference's `isFileDeleted` (utils/commitAttribution.ts): lets attribution /
 * change-size logic honestly flag a file whose content no longer exists on disk.
 *
 * Read-only: uses the shared no-shell {@link runGit} runner with
 * `--no-optional-locks` so it never disturbs a concurrent index lock.
 */
export function isFileStagedForDeletion(cwd: string, filePath: string): boolean {
  const res = runGit(cwd, ['--no-optional-locks', 'diff', '--cached', '--name-status', '--', filePath]);
  if (!res || res.code !== 0 || !res.stdout) return false;
  // Format: "D\t<path>" for a staged deletion.
  return res.stdout.trim().startsWith('D\t');
}

/**
 * Injectable no-shell git runner for {@link computeDefaultBranch}: takes an argv
 * array, returns trimmed stdout, and THROWS when the invocation fails. The seam
 * lets each call site supply its own runner (e.g. the mockable `enter_worktree`
 * exec hook, or a slash-command's `execFileSync` wrapper) while the default-branch
 * algorithm itself lives in exactly one place.
 */
export type GitRefExec = (args: string[], opts: { cwd: string; stdio?: 'ignore' | 'pipe' }) => string;

/**
 * Resolve a repository's default branch, offline-friendly and with NO network
 * call (parity with the reference's `computeDefaultBranch`). One canonical
 * algorithm, consumed by both `enter_worktree` (worktree base selection) and the
 * `/commit-push-pr` branch-creation guard so the two never drift:
 *   1. read the `origin/HEAD` symref directly (`git symbolic-ref` — no `git remote
 *      show` network/shell-pipe call);
 *   2. otherwise probe the conventional candidates `main` then `master` and return
 *      the first that exists as a local ref;
 *   3. fall back to `main` (never a bare `master` guess for a ref that is absent).
 */
export function computeDefaultBranch(cwd: string, exec: GitRefExec): string {
  try {
    const sym = exec(['symbolic-ref', '--short', 'refs/remotes/origin/HEAD'], { cwd });
    const match = sym.match(/^origin\/(.+)$/);
    if (match) return match[1];
  } catch { /* fall through to the candidate probe */ }
  for (const candidate of ['main', 'master']) {
    try {
      exec(['rev-parse', '--verify', `refs/heads/${candidate}`], { cwd, stdio: 'ignore' });
      return candidate;
    } catch { /* try the next candidate */ }
  }
  return 'main';
}

/**
 * Upper bound on a SINGLE untracked file that {@link captureUntrackedFiles} will
 * read (500 MB). A larger file is skipped — never slurped into memory. Parity with
 * the reference's `MAX_FILE_SIZE_BYTES` (utils/git.ts).
 */
export const UNTRACKED_MAX_FILE_SIZE_BYTES = 500 * 1024 * 1024;

/**
 * Aggregate ceiling across all captured files (5 GB). Once adding the next file
 * would cross this, the scan stops. Parity with the reference's
 * `MAX_TOTAL_SIZE_BYTES`.
 */
export const UNTRACKED_MAX_TOTAL_SIZE_BYTES = 5 * 1024 * 1024 * 1024;

/**
 * Hard cap on how many untracked files are captured (20000), so a pathological
 * working tree can never produce an unbounded result. Parity with the reference's
 * `MAX_FILE_COUNT`.
 */
export const UNTRACKED_MAX_FILE_COUNT = 20000;

/**
 * Initial read buffer for binary detection + content reuse (64 KB). Covers most
 * source files in a single read; {@link isBinaryContent} scans only its first 8 KB
 * for the binary heuristic, so the extra bytes exist purely to avoid a second read
 * when the file turns out to be text. Parity with the reference's `SNIFF_BUFFER_SIZE`.
 */
export const UNTRACKED_SNIFF_BUFFER_SIZE = 64 * 1024;

/** One captured untracked file: its working-tree-relative path + decoded text. */
export interface CapturedUntrackedFile {
  path: string;
  content: string;
}

/**
 * Tunable bounds for {@link captureUntrackedFiles}. Every field defaults to the
 * exported `UNTRACKED_*` constant (the reference-parity defaults); a caller that needs a
 * tighter budget (e.g. a bug-report attachment) can override any of them.
 */
export interface CaptureUntrackedOptions {
  maxFileSizeBytes?: number;
  maxTotalSizeBytes?: number;
  maxFileCount?: number;
  sniffBufferSize?: number;
}

/**
 * Capture the content of every untracked file in `cwd` (`git ls-files --others
 * --exclude-standard`) as a bounded, binary-safe, memory-safe snapshot — a reusable
 * building block for checkpoint / share-state / issue-replay capture. Parity with
 * the reference's `captureUntrackedFiles` (utils/git.ts L616-713), tuned identically:
 *
 *  - zero-I/O skip by {@link hasBinaryExtension binary extension};
 *  - per-file ({@link UNTRACKED_MAX_FILE_SIZE_BYTES}), total
 *    ({@link UNTRACKED_MAX_TOTAL_SIZE_BYTES}), and count
 *    ({@link UNTRACKED_MAX_FILE_COUNT}) gates;
 *  - a single {@link UNTRACKED_SNIFF_BUFFER_SIZE}-byte read used BOTH for the
 *    {@link isBinaryContent} sniff AND, when the file fits in the buffer, reused as
 *    the content (no second read); a larger text file falls back to an
 *    encoding-aware `readFile` so the runtime decodes straight to a string without
 *    a full-size Buffer living alongside it (avoids doubling peak memory);
 *  - an empty-file fast path (no open at all).
 *
 * Binary files and files that cannot be read are silently skipped. Returns `[]`
 * when `cwd` is not a git repo, there are no untracked files, or git is
 * unavailable.
 */
export async function captureUntrackedFiles(
  cwd: string,
  opts: CaptureUntrackedOptions = {},
): Promise<CapturedUntrackedFile[]> {
  const maxFileSize = opts.maxFileSizeBytes ?? UNTRACKED_MAX_FILE_SIZE_BYTES;
  const maxTotalSize = opts.maxTotalSizeBytes ?? UNTRACKED_MAX_TOTAL_SIZE_BYTES;
  const maxFileCount = opts.maxFileCount ?? UNTRACKED_MAX_FILE_COUNT;
  const sniffBufferSize = opts.sniffBufferSize ?? UNTRACKED_SNIFF_BUFFER_SIZE;

  const listed = runGit(cwd, ['--no-optional-locks', 'ls-files', '--others', '--exclude-standard']);
  if (!listed || listed.code !== 0 || !listed.stdout) return [];

  const files = listed.stdout.split('\n').filter(Boolean);
  const result: CapturedUntrackedFile[] = [];
  let totalSize = 0;

  for (const relPath of files) {
    // File-count gate.
    if (result.length >= maxFileCount) break;

    // Zero-I/O skip by extension.
    if (hasBinaryExtension(relPath)) continue;

    const absPath = path.resolve(cwd, relPath);
    try {
      const stats = await fs.promises.stat(absPath);
      if (!stats.isFile()) continue;
      const fileSize = stats.size;

      // Per-file size gate.
      if (fileSize > maxFileSize) continue;
      // Aggregate size gate — stop once the next file would cross the ceiling.
      if (totalSize + fileSize > maxTotalSize) break;

      // Empty-file fast path — nothing to open.
      if (fileSize === 0) {
        result.push({ path: relPath, content: '' });
        continue;
      }

      const sniffSize = Math.min(sniffBufferSize, fileSize);
      const fd = await fs.promises.open(absPath, 'r');
      try {
        const sniffBuf = Buffer.alloc(sniffSize);
        const { bytesRead } = await fd.read(sniffBuf, 0, sniffSize, 0);
        const sniff = sniffBuf.subarray(0, bytesRead);
        if (isBinaryContent(sniff)) continue;

        // Reuse the sniff bytes as content when they cover the whole file; otherwise
        // decode the full file directly to a string (no full-size Buffer alongside).
        const content = fileSize <= sniffSize
          ? sniff.toString('utf-8')
          : await fs.promises.readFile(absPath, 'utf-8');
        result.push({ path: relPath, content });
        totalSize += fileSize;
      } finally {
        await fd.close();
      }
    } catch {
      // Skip files we can't stat/read.
    }
  }

  return result;
}
