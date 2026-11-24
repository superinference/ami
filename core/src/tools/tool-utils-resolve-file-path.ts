// Split from tool-utils.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as path from 'path';
import * as fs from 'fs';
import type { ToolResult } from '../types';
import { isCredentialPath } from './bash-security';
import { expandPath } from '../utils/path';
import { isSpecialFileStat } from '../path-safety';
import { getErrnoCode } from '../utils/errno';
import { _toolUtilsHooks, isSensitivePath, isUNCPath, isWithinWorkspaceRoots, outsideWorkspaceError, resolveDeepestExistingAncestor, resolveSearchPath, validateRequiredString } from './tool-utils-resolve-deepest-existing-ancestor';

export function resolveFilePath(
  filePath: string,
  cwd: string,
  allowedRoots: readonly string[] = [],
  operation?: string,
): { resolved: string; error?: ToolResult } {
  // UNC guard — BEFORE any filesystem access. Merely touching a UNC path on
  // Windows can open an outbound SMB connection that leaks NTLM credentials, so
  // the shared resolver refuses it for EVERY caller (file_edit, index.ts, …), not
  // just the ones that happen to pre-check. the reference blocks UNC in safeResolvePath.
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
 * will affect before granting approval. Mirrors the reference's FilePermissionDialog
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
 * caller behaves exactly as a BOM-less UTF-8 file. Mirrors the reference's
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
 * Pluralize `word` for a count (mirrors the reference's `plural()` helper in
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
 * checked in the given directory only. Mirrors the reference's `VCS_MARKERS`.
 */
export const VCS_MARKERS: ReadonlyArray<readonly [marker: string, name: string]> = [
  ['.hg', 'mercurial'],
  ['.svn', 'svn'],
  ['.p4config', 'perforce'],
  ['.jj', 'jujutsu'],
  ['.sl', 'sapling'],
];
