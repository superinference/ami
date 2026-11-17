import * as path from 'path';
import * as fs from 'fs';
import type { ToolResult, DiffHunk } from '../types';
import { isCredentialPath } from './bash-security';
import { validatePathExpansionSafety, normalizeCaseForComparison, expandPath } from '../utils/path';
import { isSpecialFileStat } from '../path-safety';

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
// Case-insensitive-filesystem-aware path comparison
// ---------------------------------------------------------------------------

/**
 * Platform seam for case-insensitive-filesystem detection. Default is the real
 * `process.platform`; tests override it to exercise the Windows/macOS case-folding
 * branch deterministically on a case-sensitive CI host.
 */
export const _pathCompareHooks: { platform: NodeJS.Platform } = { platform: process.platform };

/**
 * True when the host filesystem is case-INSENSITIVE by default (Windows, macOS/APFS).
 * On these platforms two spellings of a path that differ only by letter-case name the
 * SAME file, so path-equality/dedup keys must fold case to match the OS.
 */
export function isCaseInsensitiveFs(platform: NodeJS.Platform = _pathCompareHooks.platform): boolean {
  return platform === 'win32' || platform === 'darwin';
}

/**
 * Canonicalize a path for COMPARISON/dedup: always collapse `.`/`..`/duplicate
 * separators via `path.normalize`, and — only on a case-insensitive filesystem —
 * unify separators to `/` and lowercase, so the same file reached via two letter-cases
 * (or mixed `\`/`/`) resolves to one key. On a case-sensitive FS (Linux) this is just
 * `path.normalize` (case preserved). Mirrors Claude Code's `normalizePathForComparison`.
 */
export function normalizePathForComparison(filePath: string): string {
  // NFC-normalize so the two Unicode spellings of the same filename (e.g. a
  // precomposed "é" U+00E9 vs the decomposed "e" + U+0301) collapse to ONE
  // comparison key — otherwise a path reached via a decomposed spelling would
  // dodge a dedup/containment check that stored the composed form. Mirrors Claude
  // Code's NFC normalization of resolved paths used for containment checks.
  const normalized = path.normalize(filePath).normalize('NFC');
  if (isCaseInsensitiveFs()) {
    return normalized.replace(/\\/g, '/').toLowerCase();
  }
  return normalized;
}

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
  // PARITY ADD: complete the hang-inducing device set (Claude Code's
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
function realpathOrResolve(p: string): string {
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
  // leak NTLM credentials, so grep/glob/list refuse it too (Claude Code blocks
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

const SENSITIVE_PATH_PREFIXES = [
  '/etc/shadow', '/etc/passwd', '/etc/sudoers',
  '/proc/', '/sys/',
];

const SENSITIVE_HOME_DIRS = [
  '.ssh',
  '.aws',
  '.gnupg',
  '.config/gh',
  '.claude/backups',
];

const SENSITIVE_HOME_FILES = [
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
 * live and orphaned plugin content. Parity: Claude Code's
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
 * for patterns in files in", … — not just that it failed. Mirrors Claude Code's
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
 * relative path (Claude Code's FILE_NOT_FOUND_CWD_NOTE convention), (2) names the
 * OPERATION in human terms via {@link describeAction} (Claude Code's ACTION_VERBS),
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
function outsideWorkspaceError(
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
 * past). ELOOP and EACCES/EIO fail closed. Mirrors Claude Code's
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

export function resolveFilePath(
  filePath: string,
  cwd: string,
  allowedRoots: readonly string[] = [],
  operation?: string,
): { resolved: string; error?: ToolResult } {
  // UNC guard — BEFORE any filesystem access. Merely touching a UNC path on
  // Windows can open an outbound SMB connection that leaks NTLM credentials, so
  // the shared resolver refuses it for EVERY caller (file_edit, index.ts, …), not
  // just the ones that happen to pre-check. Claude Code blocks UNC in safeResolvePath.
  if (isUNCPath(filePath)) {
    return { resolved: filePath, error: { output: `Error: UNC paths are not allowed (potential NTLM credential leak): ${filePath}`, isError: true } };
  }
  const resolved = path.isAbsolute(filePath)
    ? filePath
    : path.resolve(cwd, filePath);
  if (isSensitivePath(resolved)) {
    return { resolved, error: { output: `Error: access to "${filePath}" is blocked — sensitive system path.`, isError: true } };
  }
  // Logical (pre-symlink) containment: cwd OR a permission-granted root.
  if (!isWithinWorkspaceRoots(resolved, cwd, allowedRoots)) {
    return { resolved, error: outsideWorkspaceError(filePath, cwd, allowedRoots, operation) };
  }
  // Special-file (FIFO/socket/char/block device) guard — BEFORE realpathSync,
  // which can block INDEFINITELY on a FIFO waiting for a writer (a DoS a crafted
  // named pipe inside the workspace could trigger just by being validated), and
  // before any readFileSync which would likewise hang. lstat does not follow the
  // final symlink and never blocks. A non-existent target throws ENOENT here and
  // is skipped — the realpath/ancestor logic below governs new-file writes. Reuses
  // the single shared isSpecialFileStat predicate (path-safety), no re-impl.
  try {
    if (isSpecialFileStat(fs.lstatSync(resolved))) {
      return { resolved, error: { output: `Error: path "${filePath}" is a special file (FIFO/socket/device) — refused.`, isError: true } };
    }
  } catch { /* target does not exist yet — governed by the ancestor walk below */ }
  try {
    const real = fs.realpathSync(resolved);
    if (!isWithinWorkspaceRoots(real, cwd, allowedRoots)) {
      return { resolved, error: { output: `Error: path "${filePath}" resolves outside the workspace via symlink.`, isError: true } };
    }
    // Special-file guard on the SYMLINK TARGET. The lstat pre-guard above only
    // inspects the link entry itself (lstat does not follow the final symlink),
    // so a symlink (or symlink chain) that points at an in-workspace FIFO/socket/
    // char/block device slips past it — and a later readFileSync would follow the
    // link and block INDEFINITELY on the FIFO (a DoS a crafted link could trigger
    // just by being validated). statSync follows the WHOLE symlink chain to its
    // terminal target but — unlike open()/read() — never blocks on a FIFO, so it
    // is safe to classify the target here and refuse before any consumer reads.
    // Reuses the single shared isSpecialFileStat predicate (path-safety), no re-impl.
    try {
      if (isSpecialFileStat(fs.statSync(resolved))) {
        return { resolved, error: { output: `Error: path "${filePath}" is a symlink targeting a special file (FIFO/socket/device) — refused.`, isError: true } };
      }
    } catch { /* target vanished between realpath and stat — treat as absent */ }
  } catch (err) {
    const code = getErrnoCode(err);
    if (code === 'ELOOP') {
      return { resolved, error: { output: `Error: path "${filePath}" contains a symlink loop — refused.`, isError: true } };
    }
    if (code === 'EACCES' || code === 'EIO') {
      return { resolved, error: { output: `Error: cannot verify containment of "${filePath}" (${code}) — refused.`, isError: true } };
    }
    // ENOENT/ENOTDIR — the target does not exist yet. Resolve where a NEW-file
    // write would ACTUALLY land by walking up to the deepest existing ancestor
    // (following parent symlinks), so a dangling-or-live parent symlink that
    // escapes the workspace is still caught (the old code only checked the
    // IMMEDIATE parent — a grandparent symlink slipped through).
    const landing = resolveDeepestExistingAncestor(resolved);
    if (landing.kind === 'dangling') {
      return { resolved, error: { output: `Error: path "${filePath}" is a dangling symlink — refused.`, isError: true } };
    }
    if (landing.kind === 'loop') {
      return { resolved, error: { output: `Error: path "${filePath}" contains a symlink loop — refused.`, isError: true } };
    }
    if (landing.kind === 'unverifiable') {
      return { resolved, error: { output: `Error: cannot verify containment of "${filePath}" (${landing.code}) — refused.`, isError: true } };
    }
    if (!isWithinWorkspaceRoots(landing.path, cwd, allowedRoots)) {
      return { resolved, error: { output: `Error: path "${filePath}" resolves outside the workspace via symlink.`, isError: true } };
    }
  }
  return { resolved };
}

/**
 * Read a file with lstat-based symlink verification to close the TOCTOU gap.
 * Validates the path is inside the workspace AND that the final target (after
 * any symlink chain) resolves within the workspace. Uses lstat right before
 * reading to reduce the window for a race.
 */
export function safeReadFile(
  filePath: string,
  cwd: string,
  allowedRoots: readonly string[] = [],
): { content: string; encoding: FileEncoding; error?: undefined } | { error: ToolResult; content?: undefined; encoding?: undefined } {
  const { resolved, error } = resolveFilePath(filePath, cwd, allowedRoots);
  if (error) return { error };

  try {
    const stat = fs.lstatSync(resolved);
    if (stat.isSymbolicLink()) {
      const target = _toolUtilsHooks.realpathSync(resolved);
      if (isCredentialPath(target)) {
        return { error: { output: `Error: "${filePath}" is a symlink targeting a sensitive file: ${target}`, isError: true } };
      }
      if (!isWithinWorkspaceRoots(target, cwd, allowedRoots)) {
        return { error: { output: `Error: "${filePath}" is a symlink resolving outside the workspace.`, isError: true } };
      }
    }

    const buf = fs.readFileSync(resolved);
    const { text, encoding } = decodeBuffer(buf);
    return { content: text, encoding };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { error: { output: `Error: cannot read "${filePath}": ${msg}`, isError: true } };
  }
}

/**
 * Get file metadata using lstat (does NOT follow symlinks).
 * Returns the raw lstat result so callers can distinguish regular files
 * from symlinks and detect symlink-escape attacks.
 */
export function safeLstat(
  filePath: string,
  cwd: string,
  allowedRoots: readonly string[] = [],
): { stat: fs.Stats; error?: undefined } | { error: ToolResult; stat?: undefined } {
  const resolved = path.isAbsolute(filePath) ? filePath : path.resolve(cwd, filePath);
  if (!isWithinWorkspaceRoots(resolved, cwd, allowedRoots)) {
    return { error: { output: `Error: path "${filePath}" is outside the workspace directory (cwd: ${cwd}).`, isError: true } };
  }

  try {
    const stat = fs.lstatSync(resolved);
    return { stat };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { error: { output: `Error: cannot stat "${filePath}": ${msg}`, isError: true } };
  }
}

/**
 * Informed-consent disclosure of a symlinked write target, surfaced at the
 * permission prompt so the user/model is told the REAL path a mutating file op
 * will affect before granting approval. Mirrors Claude Code's FilePermissionDialog
 * symlink branch (FilePermissionDialog.tsx:L75-89,L162-167): it computes the
 * resolved symlink target for non-read ops and shows one of two consent lines —
 * an explicit "outside working directory" warning when the target escapes cwd, or
 * a plain "Symlink target:" note when an in-cwd symlink redirects to another
 * in-cwd file.
 *
 * AMI already HARD-BLOCKS out-of-workspace symlink writes at execution
 * (resolveFilePath/safeReadFile) — that safety stays. This helper is additive: it
 * surfaces the real target as disclosure at approval time (including for in-cwd
 * symlink redirection, which previously executed silently with no indication the
 * path was a symlink). It never throws and returns `null` whenever there is
 * nothing to disclose (missing path, non-existent target, or not a symlink).
 */
export interface SymlinkDisclosure {
  /** The real path the symlink resolves to (one hop for a dangling link). */
  resolvedPath: string;
  /** True when the resolved target is outside cwd (`relative(cwd, target)` escapes). */
  isOutsideCwd: boolean;
  /** The informed-consent line to render at the permission prompt. */
  message: string;
}

export function describeSymlinkTarget(
  filePath: string,
  cwd: string = process.cwd(),
): SymlinkDisclosure | null {
  if (!filePath) return null;
  const expanded = expandPath(filePath, cwd);
  const absolute = path.isAbsolute(expanded) ? expanded : path.resolve(cwd, expanded);
  let stat: fs.Stats;
  try {
    stat = fs.lstatSync(absolute);
  } catch {
    // Target does not exist yet → nothing to disclose (a NEW-file write is not
    // through a symlink; parent-symlink escapes are still blocked by resolveFilePath).
    return null;
  }
  if (!stat.isSymbolicLink()) return null;
  let resolvedPath: string;
  try {
    resolvedPath = _toolUtilsHooks.realpathSync(absolute);
  } catch {
    // Dangling symlink — resolve a single hop so the user still sees the intended
    // target they are about to create/modify through the link.
    try {
      const link = _toolUtilsHooks.readlinkSync(absolute);
      resolvedPath = path.isAbsolute(link) ? link : path.resolve(path.dirname(absolute), link);
    } catch {
      return null;
    }
  }
  const isOutsideCwd = path.relative(cwd, resolvedPath).startsWith('..');
  const message = isOutsideCwd
    ? `This will modify ${resolvedPath} (outside working directory) via a symlink`
    : `Symlink target: ${resolvedPath}`;
  return { resolvedPath, isOutsideCwd, message };
}

export const FILE_ENCODINGS = ['utf-8', 'utf-8-bom', 'utf-16le', 'utf-16be'] as const;
export type FileEncoding = (typeof FILE_ENCODINGS)[number];

/** Detect a text file's encoding from its leading BOM bytes (UTF-8 otherwise). */
export function detectEncoding(buf: Buffer): FileEncoding {
  if (buf.length >= 3 && buf[0] === 0xEF && buf[1] === 0xBB && buf[2] === 0xBF) return 'utf-8-bom';
  if (buf.length >= 2 && buf[0] === 0xFF && buf[1] === 0xFE) return 'utf-16le';
  if (buf.length >= 2 && buf[0] === 0xFE && buf[1] === 0xFF) return 'utf-16be';
  return 'utf-8';
}

/**
 * Detect an EXISTING file's encoding by reading ONLY its leading BOM bytes via a
 * bounded `readSync`, instead of slurping the whole file just to inspect 4 bytes.
 * For a write-back that re-encodes content (file_edit's {@link writeFileAtomicSync}
 * path), re-reading a multi-megabyte file to recover its BOM is pure waste — this
 * opens the fd, reads at most {@link BOM_PROBE_BYTES} bytes, and closes it.
 * Returns the default `'utf-8'` for an empty/short/absent/unreadable file so the
 * caller behaves exactly as a BOM-less UTF-8 file. Mirrors Claude Code's
 * `readFileSyncWithMetadata` leading-4096-byte encoding probe (scaled to the 4
 * BOM bytes we actually need). Single source for cheap on-disk encoding probing.
 */
export const BOM_PROBE_BYTES = 4;

export function detectFileEncoding(resolvedPath: string): FileEncoding {
  let fd: number | undefined;
  try {
    fd = fs.openSync(resolvedPath, 'r');
    const buf = Buffer.alloc(BOM_PROBE_BYTES);
    const bytesRead = fs.readSync(fd, buf, 0, BOM_PROBE_BYTES, 0);
    return detectEncoding(buf.subarray(0, bytesRead));
  } catch {
    return 'utf-8';
  } finally {
    if (fd !== undefined) {
      try { fs.closeSync(fd); } catch { /* fd already gone */ }
    }
  }
}

/** Decode a buffer to text, stripping any BOM and honoring UTF-16 byte order. */
export function decodeBuffer(buf: Buffer): { text: string; encoding: FileEncoding } {
  const encoding = detectEncoding(buf);
  if (encoding === 'utf-8-bom') return { text: buf.subarray(3).toString('utf-8'), encoding };
  if (encoding === 'utf-16le') return { text: buf.subarray(2).toString('utf16le'), encoding };
  if (encoding === 'utf-16be') {
    const body = Buffer.from(buf.subarray(2));
    body.swap16();
    return { text: body.toString('utf16le'), encoding };
  }
  return { text: buf.toString('utf-8'), encoding };
}

/**
 * Check whether a buffer contains non-UTF-8 high bytes that indicate
 * a legacy encoding (Shift-JIS, GBK, Windows-1252, ISO-8859-x).
 * Editing such files as UTF-8 would silently corrupt them.
 */
export function isNonUtfEncoding(buf: Buffer): boolean {
  const enc = detectEncoding(buf);
  if (enc !== 'utf-8') return false;
  let invalidUtf8Runs = 0;
  for (let i = 0; i < Math.min(buf.length, 8192); i++) {
    const b = buf[i];
    if (b < 0x80) continue;
    if ((b & 0xE0) === 0xC0) {
      if (i + 1 >= buf.length || (buf[i + 1] & 0xC0) !== 0x80) { invalidUtf8Runs++; continue; }
      i += 1;
    } else if ((b & 0xF0) === 0xE0) {
      if (i + 2 >= buf.length || (buf[i + 1] & 0xC0) !== 0x80 || (buf[i + 2] & 0xC0) !== 0x80) { invalidUtf8Runs++; continue; }
      i += 2;
    } else if ((b & 0xF8) === 0xF0) {
      if (i + 3 >= buf.length || (buf[i + 1] & 0xC0) !== 0x80 || (buf[i + 2] & 0xC0) !== 0x80 || (buf[i + 3] & 0xC0) !== 0x80) { invalidUtf8Runs++; continue; }
      i += 3;
    } else {
      invalidUtf8Runs++;
    }
  }
  return invalidUtf8Runs >= 3;
}

/** Re-encode text to a buffer in the given encoding, re-prepending any BOM. */
export function encodeText(text: string, encoding: FileEncoding): Buffer {
  if (encoding === 'utf-8-bom') return Buffer.concat([Buffer.from([0xEF, 0xBB, 0xBF]), Buffer.from(text, 'utf-8')]);
  if (encoding === 'utf-16le') return Buffer.concat([Buffer.from([0xFF, 0xFE]), Buffer.from(text, 'utf16le')]);
  if (encoding === 'utf-16be') {
    const body = Buffer.from(text, 'utf16le');
    body.swap16();
    return Buffer.concat([Buffer.from([0xFE, 0xFF]), body]);
  }
  return Buffer.from(text, 'utf-8');
}

/**
 * Fail-loud guard for search tools: confirm the resolved target is readable.
 * Returns an isError ToolResult when the path is missing or unreadable, else
 * null. This lets grep/glob distinguish "genuinely zero matches" from "the
 * search could not run" — otherwise an agent that greps a mistyped or
 * permission-denied path gets "No matches found" and wrongly concludes the
 * symbol is absent.
 */
export function assertReadableTarget(resolved: string): ToolResult | null {
  try {
    fs.accessSync(resolved, fs.constants.R_OK);
    return null;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { output: `Error: search failed — cannot read "${resolved}": ${msg}`, isError: true };
  }
}

/**
 * Validate a required pattern string and resolve a search path in one call.
 * Returns { pattern, resolved } on success, or { error } on validation failure.
 */
export function validatePatternAndPath(
  pattern: unknown,
  searchPath: string | undefined,
  cwd: string,
  allowedRoots: readonly string[] = [],
  operation?: string,
): { pattern: string; resolved: string; error?: undefined } | { error: ToolResult; pattern?: undefined; resolved?: undefined } {
  const invalid = validateRequiredString(pattern, 'pattern');
  if (invalid) return { error: invalid };
  const { resolved, error: pathError } = resolveSearchPath(searchPath, cwd, allowedRoots, operation);
  if (pathError) return { error: { output: pathError, isError: true } };
  return { pattern: pattern as string, resolved };
}

/**
 * Pluralize `word` for a count (mirrors Claude Code's `plural()` helper in
 * `utils/stringUtils.ts`). The single canonical implementation shared by the
 * code-intelligence tools (`search_symbols`, `lsp`) so count headers read
 * `1 symbol` / `2 symbols` without each tool re-declaring the same helper.
 */
export function plural(n: number, word: string, pluralWord = `${word}s`): string {
  return n === 1 ? word : pluralWord;
}

/**
 * Parse an optional non-negative integer tool-input knob (e.g. `limit`,
 * `offset`). Returns `{ value }` with the parsed integer (or `fallback` when the
 * key is absent/undefined), or `{ error }` — mirroring extractQuery's shape —
 * when present but not a finite non-negative integer, so a bad paging knob
 * surfaces a validation error instead of being silently coerced.
 */
export function extractNonNegativeInt(
  raw: unknown,
  name: string,
  fallback: number,
): { value: number; error?: undefined } | { error: ToolResult; value?: undefined } {
  if (raw === undefined || raw === null) return { value: fallback };
  if (typeof raw !== 'number' || !Number.isInteger(raw) || raw < 0) {
    return {
      error: {
        output: `Invalid "${name}": expected a non-negative integer, got ${JSON.stringify(raw)}.`,
        isError: true,
      },
    };
  }
  return { value: raw };
}

/**
 * Extract and validate a required 'query' string from tool input.
 * Returns { query } on success, or { error } on validation failure.
 */
export function extractQuery(input: Record<string, unknown>): { query: string; error?: undefined } | { error: ToolResult; query?: undefined } {
  const invalid = validateRequiredString(input.query, 'query');
  if (invalid) return { error: invalid };
  return { query: input.query as string };
}

/**
 * VCS working-copy markers. Git is handled separately (it is detected by walking
 * ANCESTORS, since a subdirectory of a repo is still "in" the repo); the rest are
 * checked in the given directory only. Mirrors Claude Code's `VCS_MARKERS`.
 */
const VCS_MARKERS: ReadonlyArray<readonly [marker: string, name: string]> = [
  ['.hg', 'mercurial'],
  ['.svn', 'svn'],
  ['.p4config', 'perforce'],
  ['.jj', 'jujutsu'],
  ['.sl', 'sapling'],
];

/**
 * Detect the version-control system(s) governing `dir` (default cwd). Git is
 * found by walking ancestors synchronously (so being inside any subdirectory of a
 * repo still reports `git`, mirroring Claude Code's `findGitRoot`); Mercurial/
 * SVN/Perforce/Jujutsu/Sapling are detected by their marker in `dir`; Perforce is
 * additionally inferred from the `P4PORT` env knob. Returns the detected VCS
 * names (possibly several, e.g. a git + perforce hybrid). Mirrors Claude Code's
 * `detectVcs(dir?)`.
 */
export function detectVcs(dir: string = process.cwd()): string[] {
  const detected = new Set<string>();
  if (process.env.P4PORT) detected.add('perforce');
  // Git: walk ancestors until a `.git` dir/file is found (worktrees/submodules
  // use a `.git` FILE), stopping at the filesystem root.
  let current = path.resolve(dir);
  for (;;) {
    try {
      const st = fs.statSync(path.join(current, '.git'));
      if (st.isDirectory() || st.isFile()) { detected.add('git'); break; }
    } catch { /* no .git at this level — keep walking up */ }
    const parent = path.dirname(current);
    if (parent === current) break;
    current = parent;
  }
  for (const [marker, name] of VCS_MARKERS) {
    try { if (fs.existsSync(path.join(dir, marker))) detected.add(name); } catch { /* unreadable */ }
  }
  return [...detected];
}

export function buildToolDescriptionContext(cwd: string): ToolDescriptionContext {
  const ctx: ToolDescriptionContext = { cwd, projectName: path.basename(cwd) };
  // Walk ancestors for git so a subdirectory of a repo still reports hasGit (the
  // old cwd-only existsSync check missed that). detectVcs swallows its own fs
  // errors, so no extra guard is needed here.
  ctx.hasGit = detectVcs(cwd).includes('git');
  try { ctx.hasPackageJson = _toolUtilsHooks.existsSync(path.join(cwd, 'package.json')); } catch { ctx.hasPackageJson = false; }
  return ctx;
}

export function renderToolDescription(
  template: string,
  context: ToolDescriptionContext,
): string {
  return template
    .replace(/\{\{cwd\}\}/g, context.cwd)
    .replace(/\{\{projectName\}\}/g, context.projectName ?? '')
    .replace(/\{\{#hasGit\}\}([\s\S]*?)\{\{\/hasGit\}\}/g, context.hasGit ? '$1' : '')
    .replace(/\{\{#hasPackageJson\}\}([\s\S]*?)\{\{\/hasPackageJson\}\}/g, context.hasPackageJson ? '$1' : '');
}

// ---------------------------------------------------------------------------
// Secret scanner — checks content for leaked credentials before writes.
//
// Detection (scanForSecrets) and redaction (redactSecrets) derive from ONE
// curated rule source (SECRET_RULES) so they can never drift: a pattern added
// for detection is automatically redacted, and vice-versa. Detection results
// (SecretMatch) carry only a stable rule id + human label and NEVER the matched
// bytes, so warnings and logs can name what leaked without echoing the secret.
// ---------------------------------------------------------------------------

/**
 * A curated secret-detection rule. Capture group 1 is the secret token itself;
 * any surrounding boundary characters stay OUTSIDE the group so redaction can
 * replace the secret while preserving the surrounding text.
 */
interface SecretRule {
  /** Stable kebab-case id, surfaced in warnings/labels (never the secret bytes). */
  id: string;
  /** Regex source; capture group 1 = the secret token to redact. */
  source: string;
  /** Optional JS regex flags (defaults to case-sensitive). */
  flags?: string;
}

/**
 * A secret detection result. Intentionally carries only a rule id + human label
 * and NEVER the matched bytes, so callers can name what leaked without echoing
 * the credential itself.
 */
export interface SecretMatch {
  /** The rule that fired, e.g. `aws-access-token`. */
  ruleId: string;
  /** Human-readable label, e.g. `AWS Access Token`. */
  label: string;
}

const SECRET_RULES: SecretRule[] = [
  // OpenAI project / service-account / admin keys carry a distinctive `T3BlbkFJ`
  // infix — match BEFORE the generic `sk-` rule so they label precisely.
  // PARITY ADD: sk-proj-/sk-svcacct-/sk-admin- scoped keys.
  { id: 'openai-project-key', source: '(?:^|[^a-zA-Z0-9])(sk-(?:proj|svcacct|admin)-[A-Za-z0-9_-]{20,}T3BlbkFJ[A-Za-z0-9_-]{20,})' },
  // OpenAI standard (non-scoped) user API key — the `sk-<20>T3BlbkFJ<20>` form
  // whose mandatory `T3BlbkFJ` infix is the structural anchor Claude Code's
  // secretScanner uses for near-zero-false-positive OpenAI detection (the second
  // alternative of its `openai-api-key` rule). PARITY ADD: matched BEFORE the
  // generic `sk-` rule so a modern user key is identified by its structure, not
  // only by the broad prefix heuristic. The generic rule below is kept as AMI's
  // wider defensive net (superset) for legacy/opaque `sk-` tokens.
  { id: 'openai-user-key', source: '(?:^|[^a-zA-Z0-9])(sk-[a-zA-Z0-9]{20}T3BlbkFJ[a-zA-Z0-9]{20})' },
  // Anthropic `sk-ant-…` — body contains `-`/`_`, so must precede the generic sk- rule.
  { id: 'anthropic-api-key', source: '(?:^|[^a-zA-Z0-9])(sk-ant-[a-zA-Z0-9_-]{20,})' },
  { id: 'openai-api-key', source: '(?:^|[^a-zA-Z0-9])(sk-[a-zA-Z0-9]{20,})' },
  { id: 'github-pat', source: '(?:^|[^a-zA-Z0-9])(ghp_[a-zA-Z0-9]{36})' },
  // GitHub fine-grained PAT (82-char underscore-bearing body) — before the generic gh* rule.
  { id: 'github-fine-grained-pat', source: '(?:^|[^a-zA-Z0-9])(github_pat_[A-Za-z0-9_]{70,})' },
  { id: 'github-app-token', source: '(?:^|[^a-zA-Z0-9])(gh[osur]_[a-zA-Z0-9]{36,})' },
  { id: 'groq-api-key', source: '(?:^|[^a-zA-Z0-9])(gsk_[a-zA-Z0-9]{20,})' },
  // AWS access key IDs — PARITY ADD: cover session/temporary creds (ASIA = STS
  // temp credentials) and the A3T…/ABIA/ACCA prefixes, not just long-lived AKIA.
  { id: 'aws-access-token', source: '(?:^|[^a-zA-Z0-9])((?:A3T[A-Z0-9]|AKIA|ASIA|ABIA|ACCA)[A-Z2-7]{16})' },
  { id: 'gcp-api-key', source: '(?:^|[^a-zA-Z0-9])(AIza[a-zA-Z0-9_-]{35})' },
  // Hugging Face user access tokens (`hf_` + 34 letters). PARITY ADD — matches
  // Claude Code's huggingface-access-token rule; AI-model credentials leak here too.
  { id: 'huggingface-access-token', source: '(?:^|[^a-zA-Z0-9])(hf_[a-zA-Z]{34})' },
  // DigitalOcean personal-access / OAuth tokens (`dop_v1_`/`doo_v1_` + 64 hex).
  // PARITY ADD — curated-provider breadth (Claude Code SECRET_RULES).
  { id: 'digitalocean-pat', source: '(?:^|[^a-zA-Z0-9])(dop_v1_[a-f0-9]{64})' },
  { id: 'digitalocean-access-token', source: '(?:^|[^a-zA-Z0-9])(doo_v1_[a-f0-9]{64})' },
  // GCP / Vertex AI service-account identities (`<name>@<project>.iam.gserviceaccount.com`).
  // These name a privileged service identity; redacting + detecting them keeps a
  // leaked service-account email out of shared transcripts (Claude Code parity).
  { id: 'gcp-service-account', source: '(?:^|[^a-zA-Z0-9])([a-z0-9-]+@[a-z0-9-]+\\.iam\\.gserviceaccount\\.com)' },
  // Google OAuth access/refresh tokens (`ya29.<token>`).
  { id: 'google-oauth-token', source: '(?:^|[^a-zA-Z0-9])(ya29\\.[0-9A-Za-z_-]{20,})' },
  // Stripe secret / restricted keys (`sk_live_`, `rk_test_`, …) — underscore-delimited.
  { id: 'stripe-access-token', source: '(?:^|[^a-zA-Z0-9])([rs]k_(?:live|test)_[0-9a-zA-Z]{20,})' },
  // SendGrid API keys (`SG.<22>.<43>`).
  { id: 'sendgrid-api-token', source: '(?:^|[^a-zA-Z0-9])(SG\\.[A-Za-z0-9_-]{22}\\.[A-Za-z0-9_-]{43})' },
  { id: 'gitlab-token', source: '(?:^|[^a-zA-Z0-9])(gl(?:pat|dt|rt|oas|ptt|agent|imt|soat|cbt|ft|ffct)-[a-zA-Z0-9_-]{20,})' },
  { id: 'slack-token', source: '(?:^|[^a-zA-Z0-9])(xox[bpsar]-[a-zA-Z0-9-]{10,})' },
  // JWT — the signature (3rd) segment is optional so both scan + redact catch it.
  { id: 'jwt', source: '(?:^|[^a-zA-Z0-9])(eyJ[a-zA-Z0-9_-]{20,}\\.eyJ[a-zA-Z0-9_-]{20,}(?:\\.[a-zA-Z0-9_-]+)?)' },
  { id: 'npm-access-token', source: '(?:^|[^a-zA-Z0-9])(npm_[a-zA-Z0-9]{36,})' },
  { id: 'pypi-upload-token', source: '(?:^|[^a-zA-Z0-9])(pypi-[a-zA-Z0-9]{50,})' },
  // PARITY ADD (curated-provider breadth, Claude Code SECRET_RULES). Each new rule
  // keeps the secret token in capture group 1 (so redactSecrets replaces ONLY the
  // token, preserving boundary chars) and adopts Claude's trailing boundary
  // alternation `(?:[\x60'"\s;]|\\[nr]|$)` — which treats a literal escaped newline
  // (the two-char `\n`/`\r` inside a serialized JSON/quoted string) as a valid
  // terminator, catching secrets embedded in minified/escaped content.
  // Azure AD client secret — distinctive `\dQ~` infix (very low false-positive).
  { id: 'azure-ad-client-secret', source: '(?:^|[\\\\\'"\\x60\\s>=:(,)])([a-zA-Z0-9_~.]{3}\\dQ~[a-zA-Z0-9_~.-]{31,34})(?:$|[\\\\\'"\\x60\\s<),])' },
  // Twilio API key — `SK` + 32 hex; negative lookahead bounds a longer hex run.
  { id: 'twilio-api-key', source: '(?:^|[^a-zA-Z0-9])(SK[0-9a-fA-F]{32})(?![0-9a-fA-F])' },
  // Databricks personal access token — `dapi` + 32 hex (optional `-N` shard).
  { id: 'databricks-api-token', source: '(?:^|[^a-zA-Z0-9])(dapi[a-f0-9]{32}(?:-[0-9])?)(?:[\\x60\'"\\s;]|\\\\[nr]|$)' },
  // HashiCorp Terraform Cloud API token — `<14>.atlasv1.<60-70>`.
  { id: 'hashicorp-tf-api-token', source: '(?:^|[^a-zA-Z0-9])([a-zA-Z0-9]{14}\\.atlasv1\\.[a-zA-Z0-9\\-_=]{60,70})' },
  // Pulumi access token — `pul-` + 40 hex.
  { id: 'pulumi-api-token', source: '(?:^|[^a-zA-Z0-9])(pul-[a-f0-9]{40})(?:[\\x60\'"\\s;]|\\\\[nr]|$)' },
  // Postman API key — `PMAK-<24hex>-<34hex>`.
  { id: 'postman-api-token', source: '(?:^|[^a-zA-Z0-9])(PMAK-[a-fA-F0-9]{24}-[a-fA-F0-9]{34})(?:[\\x60\'"\\s;]|\\\\[nr]|$)' },
  // Grafana Cloud API token — `glc_` + base64-ish body.
  { id: 'grafana-cloud-api-token', source: '(?:^|[^a-zA-Z0-9])(glc_[A-Za-z0-9+/]{32,400}={0,3})(?:[\\x60\'"\\s;]|\\\\[nr]|$)' },
  // Grafana API key — legacy `eyJrIjoi…` base64 blob. A single-segment base64
  // run (no `.eyJ`), so it never collides with the two-segment `jwt` rule.
  { id: 'grafana-api-key', source: '(?:^|[^a-zA-Z0-9])(eyJrIjoi[A-Za-z0-9+/]{70,400}={0,3})(?:[\\x60\'"\\s;]|\\\\[nr]|$)' },
  // Grafana service-account token — `glsa_<32>_<8 hex>`.
  { id: 'grafana-service-account-token', source: '(?:^|[^a-zA-Z0-9])(glsa_[A-Za-z0-9]{32}_[A-Fa-f0-9]{8})(?:[\\x60\'"\\s;]|\\\\[nr]|$)' },
  // Sentry user auth token — `sntryu_` + 64 hex.
  { id: 'sentry-user-token', source: '(?:^|[^a-zA-Z0-9])(sntryu_[a-f0-9]{64})(?:[\\x60\'"\\s;]|\\\\[nr]|$)' },
  // Sentry org auth token — `sntrys_` + an embedded base64 JSON header/region claim.
  { id: 'sentry-org-token', source: '(?:^|[^a-zA-Z0-9])(sntrys_eyJpYXQiO[a-zA-Z0-9+/]{10,200}(?:LCJyZWdpb25fdXJs|InJlZ2lvbl91cmwi|cmVnaW9uX3VybCI6)[a-zA-Z0-9+/]{10,200}={0,2}_[a-zA-Z0-9+/]{43})' },
  // Slack app-level token — `xapp-<ver>-<id>-<ts>-<secret>` (case-insensitive).
  { id: 'slack-app-token', source: '(?:^|[^a-zA-Z0-9])(xapp-\\d-[A-Za-z0-9]+-\\d+-[a-z0-9]+)', flags: 'i' },
  // Shopify access token — `shpat_` + 32 hex.
  { id: 'shopify-access-token', source: '(?:^|[^a-zA-Z0-9])(shpat_[a-fA-F0-9]{32})(?:[\\x60\'"\\s;]|\\\\[nr]|$)' },
  // Shopify shared secret — `shpss_` + 32 hex.
  { id: 'shopify-shared-secret', source: '(?:^|[^a-zA-Z0-9])(shpss_[a-fA-F0-9]{32})(?:[\\x60\'"\\s;]|\\\\[nr]|$)' },
  // Quoted credential assignment: password/secret/token/api_key : = "value".
  { id: 'credential-assignment', source: '(?:password|secret|token|api_key)\\s*[:=]\\s*[\'"]([^\'"]{8,})[\'"]', flags: 'i' },
];

const MAX_SCAN_LINE_LENGTH = 500;

// eslint-disable-next-line security/detect-bidi-characters -- matches invisible chars to strip
const INVISIBLE_CHARS_RE = /[­͏؜᠎​-‏‪-‮⁠-⁤⁦-⁯﻿￹-￻]/g;

// Lazily compiled rule caches (compiled once on first use). Scan needs
// stateless, non-global `test` regexes; redact needs global `replace` regexes.
// Both derive from the SAME SECRET_RULES, keeping detection + redaction in lockstep.
let scanCompiled: Array<{ id: string; re: RegExp }> | null = null;
let redactCompiled: Array<{ re: RegExp }> | null = null;

function getScanRules(): Array<{ id: string; re: RegExp }> {
  if (scanCompiled === null) {
    scanCompiled = SECRET_RULES.map(r => ({
      id: r.id,
      // eslint-disable-next-line security/detect-non-literal-regexp -- sources are internal constants, never user input
      re: new RegExp(r.source, (r.flags ?? '').replace('g', '')),
    }));
  }
  return scanCompiled;
}

function getRedactRules(): Array<{ re: RegExp }> {
  if (redactCompiled === null) {
    redactCompiled = SECRET_RULES.map(r => ({
      // eslint-disable-next-line security/detect-non-literal-regexp -- sources are internal constants, never user input
      re: new RegExp(r.source, (r.flags ?? '').replace('g', '') + 'g'),
    }));
  }
  return redactCompiled;
}

// Rule-id words whose canonical capitalization differs from plain title case.
const RULE_LABEL_SPECIAL: Record<string, string> = {
  aws: 'AWS', gcp: 'GCP', api: 'API', pat: 'PAT', jwt: 'JWT', npm: 'NPM',
  pypi: 'PyPI', oauth: 'OAuth', github: 'GitHub', gitlab: 'GitLab',
  openai: 'OpenAI', sendgrid: 'SendGrid', groq: 'Groq',
  huggingface: 'HuggingFace', digitalocean: 'DigitalOcean',
  ad: 'AD', tf: 'TF', hashicorp: 'HashiCorp',
};

/** Convert a kebab-case rule id to a human-readable label (never a secret). */
function ruleIdToLabel(ruleId: string): string {
  return ruleId
    .split('-')
    .map(part => RULE_LABEL_SPECIAL[part] ?? (part.length === 0 ? part : part[0].toUpperCase() + part.slice(1)))
    .join(' ');
}

/**
 * Scan content for leaked credentials. Returns one {@link SecretMatch} per rule
 * that fired (deduplicated by rule id). The matched bytes are DELIBERATELY never
 * returned — callers name the rule/label and must never echo the secret value.
 */
export function scanForSecrets(content: string): SecretMatch[] {
  // Scan the FULL content — NOT a per-line 500-char slice. A single-line .env
  // export, a minified JSON/JS config, a base64 blob, or a one-line connection
  // string can carry a credential past column 500; truncating each line before
  // matching let those slip through the write-time guard entirely. Every rule is
  // prefix-anchored (`sk-`, `AIza`, `ghp_`, …) so a full-content `test` does not
  // backtrack on long non-matching input (freeze-protection test still holds).
  // Mirrors Claude Code's secretScanner, which tests `rule.re.test(content)`.
  // One entry per rule that fires (rule ids are unique, so results are naturally
  // deduplicated). The matched bytes are never captured into the result.
  const matches: SecretMatch[] = [];
  for (const rule of getScanRules()) {
    if (rule.re.test(content)) {
      matches.push({ ruleId: rule.id, label: ruleIdToLabel(rule.id) });
    }
  }
  return matches;
}

/**
 * Replace detected secret tokens with a stable marker, preserving surrounding
 * boundary characters. Used before writing shared transcripts (/export) so API
 * keys and credentials never land in a file the user might paste or publish.
 */
export function redactSecrets(content: string): string {
  const lines = content.split('\n');
  let out = lines.map(line => {
    const trimmed = line.length > MAX_SCAN_LINE_LENGTH * 2 ? line.slice(0, MAX_SCAN_LINE_LENGTH * 2) + '…' : line;
    return trimmed.replace(INVISIBLE_CHARS_RE, '');
  }).join('\n');

  // 1. Private key blocks — must run before token patterns to avoid partial matches.
  out = out.replace(
    /-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----/g,
    '[REDACTED-PRIVATE-KEY]',
  );

  // 2. Shared token/credential rules (SAME SECRET_RULES source as scanForSecrets).
  // Every rule captures the secret in group 1; replace ONLY that captured run so
  // the surrounding boundary characters / quotes survive.
  for (const { re } of getRedactRules()) {
    out = out.replace(re, (match: string, secret: string) => match.replace(secret, '[REDACTED-SECRET]'));
  }

  // 3. Bearer / Basic auth headers — redact long token values (>= 20 chars).
  // Handles standard "Bearer <token>", percent-encoded "Bearer%20<token>",
  // and positional variants like "Bearer was <token>" or "Bearer: <token>".
  out = out.replace(
    /((?:Bearer|Basic)(?:\s+|%20))[A-Za-z0-9+/=_.~%-]{20,}/gi,
    '$1[REDACTED-SECRET]',
  );
  out = out.replace(
    /((?:Bearer|Basic)\s+(?:was|is|token|value|=)\s*:?\s*)[A-Za-z0-9+/=_.~%-]{20,}/gi,
    '$1[REDACTED-SECRET]',
  );

  // 4. URL credentials — https://user:password@host (password >= 4 chars).
  // Passwords may contain @, so match greedily up to the last @ before a host.
  out = out.replace(
    /(https?:\/\/[^:@/\s]+:)(.{4,}?)(@[^@/\s]+)/g,
    '$1[REDACTED-SECRET]$3',
  );

  // 5. Database / messaging connection strings — same pattern for known schemes.
  out = out.replace(
    /((?:mysql|postgres|postgresql|mongodb|redis|amqp):\/\/[^:@/\s]+:)(.{4,}?)(@[^@/\s]+)/g,
    '$1[REDACTED-SECRET]$3',
  );

  // 6. JSON-shaped credential: "api_key": "value" (quoted key), covering common aliases.
  // (The simpler `key: "value"` form is covered by the shared credential-assignment rule above.)
  // PARITY ADD (Claude Code SECRET_FIELD_NAMES — bridge/debugUtils.ts): redact by JSON
  // FIELD NAME regardless of the value's shape, so an opaque custom token under
  // "access_token"/"session_ingress_token"/"environment_secret" (values that match no
  // provider prefix) is scrubbed on the export/log path, not just known-shape tokens.
  out = out.replace(/("(?:password|secret|token|api_?key|access_?token|access_?key|secret_?key|client_secret|auth|session_ingress_token|environment_secret)"\s*:\s*")[^"]{6,}(")/gi, '$1[REDACTED-SECRET]$2');
  // 7. Unquoted KEY=value assignments (e.g. PASSWORD=mysecret, export API_KEY=abc123).
  // eslint-disable-next-line security/detect-unsafe-regex -- anchored (^|\s), fixed keyword alternation, no overlap
  out = out.replace(/((?:^|\s)(?:export\s+)?(?:PASSWORD|SECRET|TOKEN|API_?KEY|ACCESS_?KEY|SECRET_?KEY|CLIENT_SECRET|AUTH|PRIVATE_?KEY|GITHUB_?TOKEN|OPENAI_?KEY|ANTHROPIC_?KEY|DATABASE_?URL|DB_?PASS(?:WORD)?)\s*=\s*)\S{6,}/gim, '$1[REDACTED-SECRET]');
  return out;
}

/**
 * Scan only lines that this edit introduces. A file that already contains
 * `Password = "placeholder"` must stay editable; a newly added secret must not.
 */
export function scanAddedSecrets(before: string, after: string): SecretMatch[] {
  const beforeCounts = new Map<string, number>();
  for (const line of normalizeToLf(before).split('\n')) {
    beforeCounts.set(line, (beforeCounts.get(line) ?? 0) + 1);
  }
  const added: string[] = [];
  for (const line of normalizeToLf(after).split('\n')) {
    const remaining = beforeCounts.get(line) ?? 0;
    if (remaining > 0) beforeCounts.set(line, remaining - 1);
    else added.push(line);
  }
  if (added.length === 0) return [];
  return scanForSecrets(added.join('\n'));
}

/**
 * Build the canonical write-block message for detected secrets — ONE source of
 * truth for every edit mode (single replace, replace_all, multi-edit, full
 * overwrite) so the wording never drifts between paths. Parity with Claude Code's
 * secret guard, which both NAMES the matched secret labels AND states the
 * sharing-risk rationale (a committed secret is exposed to everyone with
 * repository access), not just the type list. `where` distinguishes a fresh
 * write ("content") from an in-place edit ("edited content"). Labels only — the
 * matched secret bytes are never echoed.
 */
export function secretWarning(
  secrets: SecretMatch[],
  where: 'content' | 'edited content',
  opts: { teamMemory?: boolean } = {},
): string {
  const labels = secrets.map(s => s.label).join(', ');
  // Team-memory scope: a file under the shared team-memory tree is replicated to
  // every repository collaborator, so writing a secret there is strictly worse
  // than committing one to this checkout. Surface a distinct, collaborator-aware
  // block message for that path (parity with Claude Code's teamMemSecretGuard,
  // which hard-blocks with "Team memory is shared with all repository
  // collaborators"). Any other path keeps the generic repository-scoped warning.
  if (opts.teamMemory) {
    return `Error: Potential secrets detected in ${where}: ${labels}. ` +
      'This file is in the shared TEAM MEMORY, which is synced to and readable by ALL repository collaborators — ' +
      'never write a secret there. Remove the secret (use an env var or secret manager) before writing.';
  }
  return `Warning: Potential secrets detected in ${where}: ${labels}. ` +
    'A committed secret is exposed to everyone with access to this repository and its history — ' +
    'remove the secret (use an env var or secret manager) before writing.';
}

// ---------------------------------------------------------------------------
// CRLF-aware line ending helpers (shared by file-edit)
// ---------------------------------------------------------------------------

export function detectLineEnding(content: string): '\r\n' | '\n' {
  const crlf = (content.match(/\r\n/g) || []).length;
  const lf = (content.match(/(?<!\r)\n/g) || []).length;
  return crlf > lf ? '\r\n' : '\n';
}

import type { ToolCallContent } from '../types';

export function toolCallFingerprint(toolName: string, input: Record<string, unknown>): string {
  const sortedKeys = Object.keys(input).sort();
  const parts = sortedKeys.map(k => `${k}=${JSON.stringify(input[k])}`);
  return `${toolName}:${parts.join('&')}`;
}

export function deduplicateToolCalls(
  calls: ToolCallContent[],
): { deduplicated: ToolCallContent[]; removedCount: number; removedIds: string[] } {
  const seen = new Set<string>();
  const deduplicated: ToolCallContent[] = [];
  const removedIds: string[] = [];

  for (const call of calls) {
    let parsed: Record<string, unknown>;
    try {
      parsed = JSON.parse(call.function.arguments);
    } catch {
      parsed = { _raw: call.function.arguments };
    }
    const fp = toolCallFingerprint(call.function.name, parsed);
    if (seen.has(fp)) {
      removedIds.push(call.id);
    } else {
      seen.add(fp);
      deduplicated.push(call);
    }
  }

  return { deduplicated, removedCount: removedIds.length, removedIds };
}

/**
 * True when a tool name is an MCP tool — the canonical `mcp__<server>__<tool>`
 * prefix every MCP-backed tool is registered under. Single source for the
 * built-in-vs-MCP partition so no call site re-derives the prefix test.
 */
export function isMcpToolName(name: string): boolean {
  return name.startsWith('mcp__');
}

/**
 * Deterministically order a tool list for PROMPT-CACHE STABILITY: partition into
 * built-ins vs MCP tools (by {@link isMcpToolName}), sort EACH group
 * alphabetically by name, and return `builtIn.concat(mcp)`. Built-ins stay a
 * contiguous, stably-ordered prefix and MCP tools follow in a stable order, so
 * the provider's tool-schema cache prefix does not shift between turns/runs as
 * MCP servers connect on demand. De-duplicates by name (first occurrence wins),
 * mirroring Claude Code's `mergeAndFilterTools` partition-sort. Pure — returns a
 * new array and never mutates the input. Generic over any `{ name }` shape so it
 * can order either `ToolDefinition`s or already-serialized tool schemas.
 */
export function partitionAndSortTools<T extends { name: string }>(tools: readonly T[]): T[] {
  const seen = new Set<string>();
  const builtIn: T[] = [];
  const mcp: T[] = [];
  for (const tool of tools) {
    if (seen.has(tool.name)) continue;
    seen.add(tool.name);
    (isMcpToolName(tool.name) ? mcp : builtIn).push(tool);
  }
  const byName = (a: T, b: T): number => a.name.localeCompare(b.name);
  builtIn.sort(byName);
  mcp.sort(byName);
  return builtIn.concat(mcp);
}

/**
 * Validate a file path before writing: rejects NUL bytes and symlinks.
 * Symlinks at the target path could redirect writes to arbitrary files.
 * If the file doesn't exist yet, that's fine — allow writes to new files.
 */
/** Glob metacharacters that must never appear in a NEW write/create target.
 *  A create path like `src/*.ts` or `out/{a,b}` would pass base-directory
 *  validation but resolve to an unintended literal path (the write tool never
 *  expands globs), so a non-existent target containing `* ? { }` is hard-denied
 *  (Claude Code `GLOB_PATTERN_REGEX`). SUPERSET over Claude: brackets `[ ]` are
 *  DELIBERATELY NOT rejected — framework dynamic-route filenames (e.g. a Next.js
 *  `[id].tsx` or SvelteKit `[slug]/+page.svelte`) are legitimate literal paths,
 *  and the write tool uses the path verbatim (no shell glob expansion), so there
 *  is no base-dir bypass risk from brackets. */
const GLOB_PATTERN_REGEX = /[*?{}]/;

export function validateFileWritePath(filePath: string): { valid: boolean; error?: string } {
  const nullCheck = validateFilePath(filePath);
  if (nullCheck) return { valid: false, error: nullCheck.output };

  try {
    const stat = fs.lstatSync(filePath);
    if (stat.isSymbolicLink()) {
      return { valid: false, error: `Error: "${filePath}" is a symlink — write blocked to prevent symlink redirect attack.` };
    }
  } catch {
    // File doesn't exist yet — a create. A glob metacharacter in a create target
    // is a base-dir-validation bypass, so reject it (write tools use the path
    // literally and never expand globs).
    if (GLOB_PATTERN_REGEX.test(filePath)) {
      return {
        valid: false,
        error: `Error: "${filePath}" contains glob metacharacters (*?{}). Provide a single literal file path for write/create operations.`,
      };
    }
  }

  return { valid: true };
}

/**
 * Environment overrides that make `git` fail fast instead of blocking on an
 * interactive credential/askpass prompt. A non-TTY stdin alone is not enough:
 * git can still try an askpass helper or a terminal prompt and hang until the
 * caller's timeout. Spread this over `process.env` for any `git` subprocess
 * (clone/fetch/etc.) spawned by a tool so a private/unauthenticated repo errors
 * out immediately with a clear auth failure. Shared by every git-spawning tool.
 */
export const GIT_NO_PROMPT_ENV: Readonly<Record<string, string>> = {
  GIT_TERMINAL_PROMPT: '0',
  GIT_ASKPASS: '',
  SSH_ASKPASS: '',
  GCM_INTERACTIVE: 'never',
};

/**
 * Crash-safe, flushed, permission-preserving synchronous file write — the single
 * canonical durable-write primitive for the file tools. It:
 *  1. captures the existing file's mode (so the executable bit / permissions survive),
 *  2. writes to a sibling `${target}.tmp.${pid}.${now}` temp file with `flush: true`
 *     (fsync to disk — a crash/ENOSPC mid-write never leaves a truncated target),
 *  3. chmods the temp back to the captured mode,
 *  4. atomically `renameSync`s temp→target (the target is never partially written),
 *  5. on ANY atomic-path failure, unlinks the temp and falls back to a direct
 *     `writeFileSync` so the write still lands on filesystems where rename across
 *     the same dir is unavailable.
 * `data` may be a string or Buffer (callers that re-encode pass a Buffer).
 */
/**
 * Parse an explicit file-permission-bits value from tool input into a numeric
 * mode. Accepts a number (used as-is) or an octal string (`'600'`, `'0600'`,
 * `'0o755'`). Returns undefined for absent/invalid input so the caller falls back
 * to the default (umask / preserved) permissions instead of applying a bogus
 * mode. Bounded to the standard permission range (<= 0o7777) so a wild value can
 * never request an unexpected setuid/sticky combination. Parity with Claude
 * Code's `{ mode }` option on its fs write helpers (create-with-exact-bits).
 */
export function parseFileMode(raw: unknown): number | undefined {
  if (typeof raw === 'number') {
    return Number.isInteger(raw) && raw >= 0 && raw <= 0o7777 ? raw : undefined;
  }
  if (typeof raw === 'string') {
    const s = raw.trim().replace(/^0o/i, '');
    if (!/^[0-7]{3,4}$/.test(s)) return undefined;
    const n = parseInt(s, 8);
    return n >= 0 && n <= 0o7777 ? n : undefined;
  }
  return undefined;
}

export function writeFileAtomicSync(target: string, data: string | Buffer, opts?: { mode?: number }): void {
  // An explicit mode (create-with-exact-bits) overrides preservation; otherwise
  // preserve the existing file's permissions across the atomic replace.
  let mode: number | undefined = opts?.mode;
  if (mode === undefined) {
    try { mode = fs.statSync(target).mode; } catch { /* new file — no mode to preserve */ }
  }

  const tmp = `${target}.tmp.${process.pid}.${Date.now()}`;
  try {
    fs.writeFileSync(tmp, data, { flush: true });
    if (mode !== undefined) {
      try { fs.chmodSync(tmp, mode); } catch { /* best-effort permission preservation */ }
    }
    fs.renameSync(tmp, target);
  } catch {
    try { fs.unlinkSync(tmp); } catch { /* temp may not exist */ }
    fs.writeFileSync(target, data);
    if (mode !== undefined) {
      try { fs.chmodSync(target, mode); } catch { /* best-effort permission application */ }
    }
  }
}

export function normalizeToLf(text: string): string {
  return text.replace(/\r\n/g, '\n');
}

export function convertToLineEnding(text: string, ending: '\r\n' | '\n'): string {
  const normalized = normalizeToLf(text);
  if (ending === '\r\n') return normalized.replace(/\n/g, '\r\n');
  return normalized;
}

/**
 * Strip trailing spaces/tabs from each line while PRESERVING the exact line
 * endings (CRLF / LF / CR). Splitting on a capturing EOL regex keeps the
 * terminators as odd-indexed segments so only the even-indexed line CONTENT is
 * trimmed; a blank/whitespace-only line collapses to empty but its newline is
 * kept. Single source for the edit/write content normalization so no call site
 * re-implements it. Markdown is handled by the caller via {@link isMarkdownPath}
 * (two trailing spaces are a hard line break there, so stripping is skipped).
 */
export function stripTrailingWhitespace(text: string): string {
  const parts = text.split(/(\r\n|\n|\r)/);
  let result = '';
  for (let i = 0; i < parts.length; i++) {
    const part = parts[i];
    if (part === undefined) continue;
    result += i % 2 === 0 ? part.replace(/[ \t]+$/, '') : part;
  }
  return result;
}

/**
 * Markdown files (.md/.mdx) use two trailing spaces as a hard line break, so
 * trailing-whitespace stripping must be skipped for them. Case-insensitive.
 */
export function isMarkdownPath(filePath: string): boolean {
  return /\.(md|mdx)$/i.test(filePath);
}

/**
 * Extract a Node.js errno string (e.g. 'ENOENT', 'EACCES', 'ELOOP', 'EIO') from
 * an unknown thrown value, or undefined when none is present. Single source for
 * classifying filesystem errors so call sites can distinguish "absent" (ENOENT)
 * from "could-not-be-checked" without re-implementing the `err.code` cast.
 */
export function getErrnoCode(err: unknown): string | undefined {
  const code = (err as { code?: unknown })?.code;
  return typeof code === 'string' ? code : undefined;
}

/** Lines of unchanged context shown around a changed region in a unified diff. */
export const DIFF_CONTEXT_LINES = 3;

/**
 * Context width that forces the WHOLE file into a single hunk (mirrors Claude
 * Code's `singleHunk` → `context: 100_000`). Any realistic file has fewer than
 * this many unchanged lines between two edits, so every change folds into one
 * contiguous hunk.
 */
export const DIFF_SINGLE_HUNK_CONTEXT = 100_000;

/**
 * Upper bound (old-line-count × new-line-count) for running the exact O(n·m)
 * line LCS. Past this the diff would be too slow/memory-hungry to compute on the
 * request path, so we fall back to the cheap O(n) prefix/suffix single-region
 * differ — mirroring Claude Code's `DIFF_TIMEOUT_MS` guard where `structuredPatch`
 * bails and the caller degrades gracefully rather than hanging.
 */
const MAX_DIFF_CELLS = 4_000_000;

/**
 * Options for {@link buildUnifiedDiff}. All optional; the defaults reproduce the
 * previous single-purpose behavior (3 context lines, whitespace-sensitive, no
 * cap) so existing call sites are unaffected.
 */
export interface UnifiedDiffOptions {
  /** Unchanged lines shown around each change region. Default {@link DIFF_CONTEXT_LINES}. */
  contextLines?: number;
  /** Compare lines with whitespace collapsed/trimmed when deciding equality. */
  ignoreWhitespace?: boolean;
  /** Emit everything as one hunk (sets context to {@link DIFF_SINGLE_HUNK_CONTEXT}). */
  singleHunk?: boolean;
  /** Byte cap on the rendered diff; past it, truncate at a line boundary with a marker. */
  maxBytes?: number;
}

type DiffOpType = ' ' | '-' | '+';
interface DiffOp { type: DiffOpType; line: string }

/** Normalizer used for equality when {@link UnifiedDiffOptions.ignoreWhitespace} is set. */
function diffEq(a: string, b: string, ignoreWhitespace: boolean): boolean {
  if (a === b) return true;
  if (!ignoreWhitespace) return false;
  return a.replace(/\s+/g, ' ').trim() === b.replace(/\s+/g, ' ').trim();
}

/**
 * Exact line-level LCS → a flat op sequence (equal/remove/add). O(n·m); callers
 * must gate on {@link MAX_DIFF_CELLS} before invoking it.
 */
function lineDiffOps(oldLines: string[], newLines: string[], ignoreWhitespace: boolean): DiffOp[] {
  const n = oldLines.length;
  const m = newLines.length;
  const dp: Uint32Array[] = Array.from({ length: n + 1 }, () => new Uint32Array(m + 1));
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      dp[i][j] = diffEq(oldLines[i], newLines[j], ignoreWhitespace)
        ? dp[i + 1][j + 1] + 1
        : Math.max(dp[i + 1][j], dp[i][j + 1]);
    }
  }
  const ops: DiffOp[] = [];
  let i = 0;
  let j = 0;
  while (i < n && j < m) {
    if (diffEq(oldLines[i], newLines[j], ignoreWhitespace)) {
      ops.push({ type: ' ', line: oldLines[i] });
      i++; j++;
    } else if (dp[i + 1][j] >= dp[i][j + 1]) {
      ops.push({ type: '-', line: oldLines[i] });
      i++;
    } else {
      ops.push({ type: '+', line: newLines[j] });
      j++;
    }
  }
  while (i < n) ops.push({ type: '-', line: oldLines[i++] });
  while (j < m) ops.push({ type: '+', line: newLines[j++] });
  return ops;
}

/** Render a flat op sequence into one-or-more `@@` hunks with `context` lines around each change. */
/**
 * Group a line-diff op stream into {@link DiffHunk} objects — the ONE canonical
 * hunk-grouping used by BOTH the text renderer ({@link renderHunks}) and the
 * machine-readable {@link buildStructuredPatch}, so the two can never diverge.
 * Every op within `context` positions of a change is kept; contiguous kept runs
 * become one hunk (changes separated by <= 2·context unchanged lines merge, like
 * a unified diff). Each hunk's `lines` carry the leading ` `/`+`/`-` prefix,
 * matching Claude Code's `structuredPatch` hunk shape. Returns `[]` when there
 * is no change.
 */
function groupHunks(ops: DiffOp[], context: number): DiffHunk[] {
  const changed = ops.map(o => o.type !== ' ');
  if (!changed.some(Boolean)) return [];
  const keep = new Array(ops.length).fill(false);
  for (let k = 0; k < ops.length; k++) {
    if (!changed[k]) continue;
    for (let d = -context; d <= context; d++) {
      const idx = k + d;
      if (idx >= 0 && idx < ops.length) keep[idx] = true;
    }
  }

  const hunks: DiffHunk[] = [];
  let oldNo = 0; // old lines consumed before the current op
  let newNo = 0; // new lines consumed before the current op
  let k = 0;
  while (k < ops.length) {
    if (!keep[k]) {
      if (ops[k].type !== '+') oldNo++;
      if (ops[k].type !== '-') newNo++;
      k++;
      continue;
    }
    const hunkStartOld = oldNo;
    const hunkStartNew = newNo;
    const lines: string[] = [];
    let oldLen = 0;
    let newLen = 0;
    while (k < ops.length && keep[k]) {
      const op = ops[k];
      lines.push(`${op.type}${op.line}`);
      if (op.type !== '+') { oldNo++; oldLen++; }
      if (op.type !== '-') { newNo++; newLen++; }
      k++;
    }
    hunks.push({
      oldStart: oldLen > 0 ? hunkStartOld + 1 : hunkStartOld,
      oldLines: oldLen,
      newStart: newLen > 0 ? hunkStartNew + 1 : hunkStartNew,
      newLines: newLen,
      lines,
    });
  }
  return hunks;
}

function renderHunks(ops: DiffOp[], label: string, context: number): string {
  const hunks = groupHunks(ops, context);
  if (hunks.length === 0) return '(no changes detected)';
  const out: string[] = [`--- ${label}`, `+++ ${label}`];
  for (const h of hunks) {
    out.push(`@@ -${h.oldStart},${h.oldLines} +${h.newStart},${h.newLines} @@`);
    out.push(...h.lines);
  }
  return out.join('\n');
}

/** Cheap O(n) single-region differ used as the degenerate-input fallback. */
function fastSingleRegionDiff(oldLines: string[], newLines: string[], label: string, context: number): string {
  let firstDiff = 0;
  while (firstDiff < oldLines.length && firstDiff < newLines.length && oldLines[firstDiff] === newLines[firstDiff]) {
    firstDiff++;
  }
  let oldEnd = oldLines.length - 1;
  let newEnd = newLines.length - 1;
  while (oldEnd > firstDiff && newEnd > firstDiff && oldLines[oldEnd] === newLines[newEnd]) {
    oldEnd--;
    newEnd--;
  }
  if (firstDiff > oldEnd && firstDiff > newEnd) return '(no changes detected)';

  const ctxStart = Math.max(0, firstDiff - context);
  const result: string[] = [`--- ${label}`, `+++ ${label}`];
  const oldWindowEnd = Math.min(oldLines.length - 1, oldEnd + context);
  const newWindowEnd = Math.min(newLines.length - 1, newEnd + context);
  result.push(`@@ -${ctxStart + 1},${oldWindowEnd - ctxStart + 1} +${ctxStart + 1},${newWindowEnd - ctxStart + 1} @@`);
  for (let i = ctxStart; i < firstDiff; i++) result.push(` ${oldLines[i]}`);
  for (let i = firstDiff; i <= oldEnd; i++) result.push(`-${oldLines[i]}`);
  for (let i = firstDiff; i <= newEnd; i++) result.push(`+${newLines[i]}`);
  for (let i = oldEnd + 1; i <= oldWindowEnd; i++) result.push(` ${oldLines[i]}`);
  return result.join('\n');
}

/**
 * Apply the {@link UnifiedDiffOptions.maxBytes} cap: truncate at the last line
 * boundary within the cap and append a `... [N lines truncated] ...` marker
 * (matching Claude Code's snippet-truncation convention), so a huge diff can
 * never blow the per-turn token budget.
 */
function capDiffBytes(diff: string, maxBytes: number): string {
  if (Buffer.byteLength(diff, 'utf8') <= maxBytes) return diff;
  // Operate on characters bounded by the byte cap (a byte cap is an upper bound
  // on the char count for UTF-8), finding the last newline that fits.
  let cutoff = diff.lastIndexOf('\n', maxBytes);
  if (cutoff <= 0) cutoff = Math.min(maxBytes, diff.length);
  const kept = diff.slice(0, cutoff);
  const remaining = diff.slice(cutoff).split('\n').length;
  return `${kept}\n\n... [${remaining} lines truncated] ...`;
}

/**
 * Build a unified diff between two text blobs. Produces SEPARATE `@@` hunks for
 * each non-contiguous change region (with `contextLines` of context around each),
 * so scattered edits no longer collapse into one unbounded hunk. Single source of
 * truth for tool diff previews (full-file writes, replace edits, notebook cell
 * edits) so the diff format is never re-implemented per tool. `label` is the
 * path/identifier shown on the ---/+++ header.
 *
 * Options (all optional, defaults preserve prior behavior):
 *  - `contextLines`  — unchanged lines around each change (default 3).
 *  - `ignoreWhitespace` — treat whitespace-only differences as equal.
 *  - `singleHunk` — fold everything into one hunk (context 100 000).
 *  - `maxBytes` — cap the rendered diff, truncating with a `[N lines truncated]` marker.
 */
export function buildUnifiedDiff(
  oldContent: string,
  newContent: string,
  label: string,
  options: UnifiedDiffOptions = {},
): string {
  const oldLines = oldContent.split('\n');
  const newLines = newContent.split('\n');
  const context = options.singleHunk
    ? DIFF_SINGLE_HUNK_CONTEXT
    : (options.contextLines ?? DIFF_CONTEXT_LINES);

  let diff: string;
  if (oldLines.length * newLines.length > MAX_DIFF_CELLS) {
    // Degenerate input: skip the exact LCS and degrade to the bounded O(n) differ.
    diff = fastSingleRegionDiff(oldLines, newLines, label, Math.min(context, DIFF_CONTEXT_LINES));
  } else {
    const ops = lineDiffOps(oldLines, newLines, options.ignoreWhitespace ?? false);
    diff = renderHunks(ops, label, context);
  }

  if (diff === '(no changes detected)') return diff;
  return options.maxBytes !== undefined ? capDiffBytes(diff, options.maxBytes) : diff;
}

/**
 * Build the machine-readable {@link DiffHunk}[] for a file mutation — parity with
 * Claude Code's FileEdit `structuredPatch` output field, which lets a frontend
 * render/fold a diff per-hunk programmatically instead of re-parsing the text
 * `output`. Shares the EXACT hunk-grouping of {@link buildUnifiedDiff} (via
 * {@link groupHunks}) so the structured patch and the text diff are always
 * consistent. Returns `[]` when there is no change. The degenerate-input guard
 * ({@link MAX_DIFF_CELLS}) returns `[]` rather than an approximate single-region
 * hunk: callers already carry the (capped) text diff for display, and a huge
 * machine patch would blow the per-turn budget it is meant to economize.
 */
export function buildStructuredPatch(
  oldContent: string,
  newContent: string,
  options: { contextLines?: number; ignoreWhitespace?: boolean } = {},
): DiffHunk[] {
  const oldLines = oldContent.split('\n');
  const newLines = newContent.split('\n');
  if (oldLines.length * newLines.length > MAX_DIFF_CELLS) return [];
  const ops = lineDiffOps(oldLines, newLines, options.ignoreWhitespace ?? false);
  return groupHunks(ops, options.contextLines ?? DIFF_CONTEXT_LINES);
}

/**
 * Count the lines added vs removed between two blobs. Mirrors Claude Code's
 * `countLinesChanged`: a brand-new file (empty old content) counts every new
 * line as an addition; otherwise additions/removals are tallied independently
 * from the line LCS so a modified line shows as one removal + one addition.
 */
export function countLinesChanged(
  oldContent: string,
  newContent: string,
  options: { ignoreWhitespace?: boolean } = {},
): { additions: number; removals: number } {
  if (oldContent.length === 0) {
    return { additions: newContent.length === 0 ? 0 : newContent.split('\n').length, removals: 0 };
  }
  const oldLines = oldContent.split('\n');
  const newLines = newContent.split('\n');
  if (oldLines.length * newLines.length > MAX_DIFF_CELLS) {
    // Bounded fallback: approximate via the net line-count delta rather than an
    // exact LCS, consistent with buildUnifiedDiff's degenerate-input guard.
    const delta = newLines.length - oldLines.length;
    return { additions: Math.max(0, delta), removals: Math.max(0, -delta) };
  }
  const ops = lineDiffOps(oldLines, newLines, options.ignoreWhitespace ?? false);
  let additions = 0;
  let removals = 0;
  for (const op of ops) {
    if (op.type === '+') additions++;
    else if (op.type === '-') removals++;
  }
  return { additions, removals };
}

/**
 * Count the CHARACTERS the agent authored between two revisions — the summed
 * length of the ADDED lines in the line-level diff (parity intent: Claude Code's
 * per-edit character contribution, `claudeContribution`). Reuses the SAME exact
 * line LCS as {@link countLinesChanged} (so the two can never disagree about what
 * changed) with the identical degenerate-input guard, counting only `+` ops.
 * A brand-new file (empty old content) counts its full length; a pure deletion,
 * or no change, counts zero. Line-separator characters are not tallied — the
 * metric is "characters of content the agent introduced".
 */
export function countCharsAdded(
  oldContent: string,
  newContent: string,
  options: { ignoreWhitespace?: boolean } = {},
): number {
  if (newContent.length === 0) return 0;
  if (oldContent.length === 0) return newContent.length;
  const oldLines = oldContent.split('\n');
  const newLines = newContent.split('\n');
  if (oldLines.length * newLines.length > MAX_DIFF_CELLS) {
    // Bounded fallback: approximate by the net positive length delta, consistent
    // with countLinesChanged's degenerate-input fallback.
    return Math.max(0, newContent.length - oldContent.length);
  }
  const ops = lineDiffOps(oldLines, newLines, options.ignoreWhitespace ?? false);
  let chars = 0;
  for (const op of ops) {
    if (op.type === '+') chars += op.line.length;
  }
  return chars;
}

/**
 * Count the lines in a blob the way an editor numbers them: a trailing newline
 * is a line TERMINATOR, not the start of a new empty line. `"a\nb\n"` is 2 lines,
 * `"a\nb"` is 2 lines, `""` is 0 lines. Mirrors Claude Code's `countLines`
 * (`content.endsWith(EOL) ? parts.length - 1 : parts.length`) so a "wrote N lines"
 * display matches editor line numbering instead of over-counting a normal
 * newline-terminated file by one.
 */
export function countContentLines(content: string): number {
  if (content.length === 0) return 0;
  const parts = content.split('\n');
  return content.endsWith('\n') ? parts.length - 1 : parts.length;
}

/**
 * Marker included in file-not-found error messages that carry a working-directory
 * note, so the model can self-correct relative-vs-absolute path confusion and a
 * renderer can detect the "file not found" shape. Parity: Claude Code's
 * `FILE_NOT_FOUND_CWD_NOTE`.
 */
export const FILE_NOT_FOUND_CWD_NOTE = 'Note: your current working directory is';
