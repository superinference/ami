// Split from tool-utils.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as path from 'path';
import * as fs from 'fs';
import type { ToolResult } from '../types';
import { validatePathExpansionSafety, normalizeCaseForComparison } from '../utils/path';
import { getErrnoCode } from '../utils/errno';

/**
 * Dependency seam so tests can drive branches that are otherwise shadowed by
 * earlier guards or never thrown by the real fs:
 *  - `existsSync` lets buildToolDescriptionContext's package.json defensive catch arm run.
 *  - `realpathSync` lets safeReadFile's post-resolveFilePath symlink re-check
 *    (the TOCTOU swap guard) run even though resolveFilePath already resolved
 *    the same path inside the workspace.
 * Production uses the real fs functions.
 */
export const _toolUtilsHooks: {
  existsSync: (p: string) => boolean;
  realpathSync: (p: string) => string;
  readlinkSync: (p: string) => string;
} = {
  existsSync: (p) => fs.existsSync(p),
  realpathSync: (p) => fs.realpathSync(p),
  readlinkSync: (p) => fs.readlinkSync(p),
};

// ---------------------------------------------------------------------------
// Device / special-file blocklist — shared by file_read and file_edit
// ---------------------------------------------------------------------------

export const BLOCKED_PATHS = [
  '/dev/zero', '/dev/null', '/dev/random', '/dev/urandom',
  '/dev/stdin', '/dev/stdout', '/dev/stderr',
  '/dev/fd/', '/dev/tcp/', '/dev/udp/',
  '/proc/self/fd/', '/proc/self/mem',
  // PARITY ADD: physical/kernel memory + I/O-port devices. Reading these can
  // dump raw RAM (leaking any process's secrets) or hard-hang the reader;
  // /proc/kcore exposes the live kernel core image for the same reason.
  '/dev/mem', '/dev/kmem', '/dev/port', '/proc/kcore',
  // PARITY ADD: complete the hang-inducing device set (the reference's
  // BLOCKED_DEVICE_PATHS). `/dev/full` always reports ENOSPC and produces
  // infinite zero bytes on read; `/dev/tty` and `/dev/console` block forever
  // waiting on terminal input. Reading any of these hangs or floods the tool.
  '/dev/full', '/dev/tty', '/dev/console',
];

export function isBlockedPath(filePath: string): boolean {
  const normalized = filePath.replace(/\\/g, '/');
  return BLOCKED_PATHS.some(bp => normalized === bp || normalized.startsWith(bp));
}

/**
 * A UNC path (`\\server\share` or `//server/share`). Merely touching one on
 * Windows can open an outbound SMB connection that leaks NTLM credentials, so the
 * file/search tools must short-circuit on it BEFORE any filesystem call. Shared by
 * every tool that accepts a path. Note: a single leading `/` (ordinary absolute
 * POSIX path) is NOT a UNC path — only a doubled separator is.
 */
export function isUNCPath(filePath: string): boolean {
  return filePath.startsWith('\\\\') || filePath.startsWith('//');
}

/**
 * Reject file paths containing NUL bytes — prevents silent truncation
 * attacks that could bypass path checks or crash fs operations.
 */
export function validateFilePath(filePath: string): ToolResult | null {
  if (filePath.includes('\x00')) {
    return { output: 'Error: file path contains a null byte.', isError: true };
  }
  // Reject TOCTOU-prone path inputs that are literal at validation time but
  // re-expanded by the shell at execution time (unresolved tilde variants,
  // $/%/leading-= shell-expansion syntax) — see validatePathExpansionSafety.
  const expansionError = validatePathExpansionSafety(filePath);
  if (expansionError) {
    return { output: `Error: ${expansionError}`, isError: true };
  }
  return null;
}

/**
 * Escape control characters in a file path for safe display in error messages.
 * Prevents newlines/tabs from breaking message formatting.
 *
 * PARITY FIX: previously only `\n`/`\r`/`\t` were escaped, so an attacker-named
 * path containing an ESC (`\x1b`), BEL (`\x07`), backspace, or other C0/C1/DEL
 * control byte flowed verbatim into a terminal error message — a classic ANSI
 * terminal-injection vector (cursor moves, title rewrites, hidden-text tricks).
 * We keep the readable `\n`/`\r`/`\t` forms and escape every other control
 * character as a `\xHH` sequence.
 */
export function sanitizePathForDisplay(filePath: string): string {
  return filePath
    .replace(/\n/g, '\\n')
    .replace(/\r/g, '\\r')
    .replace(/\t/g, '\\t')
    // Remaining C0 controls (excl. the three handled above), DEL, and C1 range.
    .replace(/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]/g, (c) =>
      '\\x' + c.charCodeAt(0).toString(16).padStart(2, '0'),
    );
}

/**
 * Validate that a required string input field is present and non-empty.
 * Returns a ToolResult error if invalid, or null if valid.
 */
export function validateRequiredString(
  value: unknown,
  fieldName: string,
): ToolResult | null {
  if (!value || (typeof value === 'string' && value.trim().length === 0)) {
    return { output: `Error: ${fieldName} must not be empty.`, isError: true };
  }
  return null;
}

/**
 * LOGICAL workspace containment for the core path guards: true when the (already
 * absolute-resolved) `resolved` is the launch cwd, under it, or under any permission-granted
 * read root. This is a pure string/prefix test — it does NOT follow symlinks, so callers
 * keep their explicit realpath step to detect symlink escapes (and to emit the distinct
 * "resolves outside … via symlink" message). The granted roots are what the permission layer
 * (PermissionManager.getAllowedReadRoots) has already approved for this call — `/add-dir`,
 * `--context-file`/`--image`, an approved out-of-workspace prompt, or auto-allow/yolo — which
 * is what lets a skill loaded from an external folder read its own resources while an
 * ungranted sibling/parent/other-$HOME path stays blocked.
 */
export function isWithinWorkspaceRoots(
  resolved: string,
  cwd: string,
  allowedRoots: readonly string[] = [],
): boolean {
  // NFC-normalize both sides so a Unicode-equivalent spelling of the same path
  // cannot slip a containment check (see normalizePathForComparison).
  const strip = (p: string): string => { const n = path.resolve(p).normalize('NFC'); return n.replace(/[/\\]+$/, '') || n; };
  const target = strip(resolved);
  for (const root of [cwd, ...allowedRoots].filter(Boolean)) {
    const r = strip(root);
    if (target === r || target.startsWith(r + path.sep)) return true;
  }
  return false;
}

/** realpath a path for symlink-aware containment; falls back to the normalized path when it
 *  does not exist yet (so not-yet-created targets still get a logical check). */
export function realpathOrResolve(p: string): string {
  try { return fs.realpathSync(path.resolve(p)); } catch { return path.resolve(p); }
}

/**
 * Resolve a search path relative to cwd.
 * If inputPath is provided, resolves it absolutely; otherwise returns cwd.
 * `allowedRoots` are permission-granted dirs outside cwd that count as in-bounds.
 */
export function resolveSearchPath(
  inputPath: string | undefined,
  cwd: string,
  allowedRoots: readonly string[] = [],
  operation?: string,
): { resolved: string; error?: string } {
  // UNC guard — BEFORE any filesystem access, mirroring resolveFilePath. Merely
  // realpath'ing a UNC path on Windows opens an outbound SMB connection that can
  // leak NTLM credentials, so grep/glob/list refuse it too (the reference blocks
  // UNC in both safeResolvePath AND getPathsForPermissionCheck, not just reads).
  if (inputPath && isUNCPath(inputPath)) {
    return { resolved: inputPath, error: `Error: UNC paths are not allowed (potential NTLM credential leak): ${inputPath}` };
  }
  const resolved = inputPath
    ? path.isAbsolute(inputPath)
      ? inputPath
      : path.resolve(cwd, inputPath)
    : cwd;
  // Resolve symlinks on BOTH sides before the containment check: a symlink living inside
  // cwd that points outside (e.g. cwd/link -> /etc) would otherwise pass a pure
  // string-prefix check and let a search escape the workspace. Granted roots are
  // realpath-resolved too so a symlinked grant still matches its real target.
  const abs = realpathOrResolve(resolved);
  const realCwd = realpathOrResolve(cwd);
  const realRoots = allowedRoots.map(realpathOrResolve);
  if (!isWithinWorkspaceRoots(abs, realCwd, realRoots)) {
    return { resolved, error: outsideWorkspaceMessage(inputPath ?? cwd, cwd, allowedRoots, operation) };
  }
  return { resolved };
}

/**
 * Reject a glob/search pattern that would traverse ABOVE the search root. The search root
 * itself is confined by resolveSearchPath, but a pattern like "../../**" passed to the
 * globber still walks up out of the workspace. Patterns must stay relative-DOWN from the
 * root (e.g. "**\/*.ts", "src/**"). Returns an error ToolResult, or null if the pattern is safe.
 */
export function rejectEscapingPattern(pattern: string): ToolResult | null {
  const normalized = pattern.replace(/\\/g, '/');
  if (normalized.split('/').some(seg => seg === '..')) {
    return {
      output: `Error: glob pattern "${pattern}" must not contain ".." path segments — it would escape the workspace. Use a pattern relative to the working directory (e.g. "**/*.ts").`,
      isError: true,
    };
  }
  return null;
}

export interface ToolDescriptionContext {
  cwd: string;
  projectName?: string;
  hasGit?: boolean;
  hasPackageJson?: boolean;
}

/**
 * Resolve a file path relative to cwd and validate it's inside the workspace.
 * Returns the resolved path or a ToolResult error.
 */

export const SENSITIVE_PATH_PREFIXES = [
  '/etc/shadow', '/etc/passwd', '/etc/sudoers',
  '/proc/', '/sys/',
];

export const SENSITIVE_HOME_DIRS = [
  '.ssh',
  '.aws',
  '.gnupg',
  '.config/gh',
  '.claude/backups',
];

export const SENSITIVE_HOME_FILES = [
  '.git-credentials',
  '.config/git/credentials',
  '.npmrc',
  '.netrc',
  '_netrc',
  '.docker/config.json',
  '.claude/.credentials.json',
  '.claude.json',
  '.kube/config',
];

export function isSensitivePath(filePath: string): boolean {
  // Case-fold the WHOLE path for the comparison on case-insensitive filesystems
  // (Windows NTFS, macOS APFS/HFS+). Without this, `~/.SSH`, `~/.Aws`, or
  // `/ETC/shadow` would slip past the sensitive-path guard on those platforms
  // and read a credential store the check is meant to protect. On case-sensitive
  // Linux the path is returned unchanged, so behavior there is unaffected. Shares
  // the single canonical fold helper used by the containment checks (utils/path).
  const normalized = normalizeCaseForComparison(path.resolve(filePath));
  if (SENSITIVE_PATH_PREFIXES.some(p => {
    const fp = normalizeCaseForComparison(p);
    return normalized === fp || normalized.startsWith(fp);
  })) return true;
  const home = process.env.HOME || process.env.USERPROFILE || '';
  if (!home) return false;
  for (const dir of SENSITIVE_HOME_DIRS) {
    const dirPath = normalizeCaseForComparison(path.join(home, dir));
    if (normalized === dirPath || normalized.startsWith(dirPath + path.sep)) return true;
  }
  for (const file of SENSITIVE_HOME_FILES) {
    if (normalized === normalizeCaseForComparison(path.join(home, file))) return true;
  }
  return false;
}

/** Cap on how many allowed roots an access-denied message enumerates before
 *  summarizing the remainder as "and N more" (keeps the hint readable). */
export const MAX_DIRS_TO_LIST = 5;

/**
 * Hidden hold directory where the plugin installer relocates SUPERSEDED plugin
 * versions during the deferred-deletion grace window (see common's
 * `plugin-orphan-cache.ts` → `ORPHAN_HOLD_DIRNAME`). A relocated version keeps
 * its full file tree (including `SKILL.md`s and source) under this dir until it
 * is reclaimed, so a raw glob/grep over a plugin install tree would otherwise
 * surface STALE files from an orphaned version. The search tools exclude it by
 * name — exactly as they exclude `node_modules`/`.git` — so results never mix
 * live and orphaned plugin content. Parity: the reference's
 * `getGlobExclusionsForPluginCache` (orphanedPluginFilter.ts), adapted to AMI's
 * relocate-to-hold-dir layout. The literal is duplicated across the core↔common
 * layer boundary (core must not import from common); keep the two in sync. */
export const ORPHANED_PLUGIN_HOLD_DIRNAME = '.orphaned';

/**
 * Render a quoted, truncated list of directories for an access-denied hint, e.g.
 * `'a', 'b', 'c', 'd', 'e', and 3 more`. De-duplicates and drops empties.
 * Returns '' when there is nothing to list so callers can omit the hint.
 */
export function formatDirectoryList(dirs: readonly string[], max: number = MAX_DIRS_TO_LIST): string {
  const unique = [...new Set(dirs.filter(d => typeof d === 'string' && d.length > 0))];
  if (unique.length === 0) return '';
  const shown = unique.slice(0, max).map(d => `'${d}'`);
  const extra = unique.length - shown.length;
  return extra > 0 ? `${shown.join(', ')}, and ${extra} more` : shown.join(', ');
}

/**
 * Human-readable phrasing for the operation named in an access-denied message, so
 * a blocked path tells the reader WHAT was attempted — "read files in", "search
 * for patterns in files in", … — not just that it failed. Mirrors the reference's
 * `ACTION_VERBS` (BashTool/pathValidation.ts). Keyed by the tool operation; an
 * unknown/absent operation falls back to a generic verb via {@link describeAction}.
 */
export const ACTION_VERBS: Readonly<Record<string, string>> = {
  read: 'read files in',
  write: 'write files in',
  edit: 'edit files in',
  list: 'list files in',
  search: 'search for patterns in files in',
  glob: 'find files in',
};

/**
 * The verb phrase for a tool operation (from {@link ACTION_VERBS}), or a generic
 * `'access'` fallback for an unknown/absent operation — so every denial can name
 * the action in human terms without each call site re-deriving the phrasing.
 */
export function describeAction(operation?: string): string {
  return (operation !== undefined && ACTION_VERBS[operation]) || 'access';
}

/**
 * Build the canonical out-of-workspace denial string — ONE source shared by the
 * read/write path resolver and the search-path resolver so the wording never
 * drifts. It (1) names the working directory as context so the model can rebase a
 * relative path (the reference's FILE_NOT_FOUND_CWD_NOTE convention), (2) names the
 * OPERATION in human terms via {@link describeAction} (the reference's ACTION_VERBS),
 * and (3) enumerates the session's accessible roots so the reader learns WHERE
 * access is permitted and can retry correctly.
 */
export function outsideWorkspaceMessage(
  filePath: string,
  cwd: string,
  allowedRoots: readonly string[] = [],
  operation?: string,
): string {
  const roots = formatDirectoryList([cwd, ...allowedRoots]);
  const rootHint = roots ? ` Accessible roots for this session: ${roots}.` : '';
  return `Error: path "${filePath}" is outside the workspace directory (cwd: ${cwd}). ` +
    `For security, AMI may only ${describeAction(operation)} the allowed working directories this session.${rootHint}`;
}

/** {@link outsideWorkspaceMessage} wrapped as an error ToolResult for the file tools. */
export function outsideWorkspaceError(
  filePath: string,
  cwd: string,
  allowedRoots: readonly string[],
  operation?: string,
): ToolResult {
  return { output: outsideWorkspaceMessage(filePath, cwd, allowedRoots, operation), isError: true };
}

/**
 * The outcome of walking a (possibly not-yet-existing) absolute path UP to its
 * deepest existing ancestor to learn where a NEW-file write would ACTUALLY land
 * once the OS follows the parent symlinks:
 *  - `resolved`     — the realpath of the deepest existing ancestor with the
 *                     non-existent tail segments rejoined.
 *  - `dangling`     — a component is a symlink whose target does not exist (an
 *                     attack vector: a write would follow it and create a file
 *                     outside the workspace).
 *  - `loop`         — a symlink loop (ELOOP).
 *  - `unverifiable` — containment could not be checked (EACCES/EIO/…); fail closed.
 */
export type AncestorResolution =
  | { kind: 'resolved'; path: string }
  | { kind: 'dangling'; at: string }
  | { kind: 'loop'; at: string }
  | { kind: 'unverifiable'; at: string; code: string };

/**
 * Resolve the deepest EXISTING ancestor of an absolute path, following parent
 * symlinks, so callers can containment-check where a not-yet-created target
 * would really be written. Walks up with `realpathSync`; an ENOENT at a
 * component is distinguished — via `lstat` — into a dangling symlink (link entry
 * present, target gone → blocked) vs a genuinely absent segment (safe to walk
 * past). ELOOP and EACCES/EIO fail closed. Mirrors the reference's
 * `resolveDeepestExistingAncestorSync` / teamMemPaths `realpathDeepestExisting`.
 */
export function resolveDeepestExistingAncestor(absolutePath: string): AncestorResolution {
  const tail: string[] = [];
  let current = absolutePath;
  for (let parent = path.dirname(current); current !== parent; parent = path.dirname(current)) {
    try {
      const real = _toolUtilsHooks.realpathSync(current);
      return { kind: 'resolved', path: tail.length === 0 ? real : path.join(real, ...[...tail].reverse()) };
    } catch (err) {
      const code = getErrnoCode(err);
      if (code === 'ELOOP') return { kind: 'loop', at: current };
      if (code === 'ENOENT') {
        // lstat succeeds for a dangling symlink's own entry but fails (ENOENT)
        // for a truly absent path — the two need different handling.
        try {
          const st = fs.lstatSync(current);
          if (st.isSymbolicLink()) return { kind: 'dangling', at: current };
          // Non-symlink that realpath could not resolve ⇒ a dangling symlink sits
          // in an ancestor; keep walking up to reach/clear it.
        } catch { /* truly absent — safe to walk up */ }
      } else if (code !== 'ENOTDIR' && code !== 'ENAMETOOLONG') {
        // EACCES / EIO / etc. — cannot verify containment; fail closed.
        return { kind: 'unverifiable', at: current, code: code ?? 'UNKNOWN' };
      }
      tail.push(path.basename(current));
      current = parent;
    }
  }
  // Reached the filesystem root without hitting a symlink — the logical path is
  // the landing; the caller's containment check decides.
  return { kind: 'resolved', path: absolutePath };
}
